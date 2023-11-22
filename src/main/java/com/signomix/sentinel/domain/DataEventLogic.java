package com.signomix.sentinel.domain;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.db.SentinelDaoIface;
import com.signomix.common.db.SignalDaoIface;
import com.signomix.common.event.IotEvent;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.sentinel.AlarmCondition;
import com.signomix.common.iot.sentinel.SentinelConfig;
import com.signomix.common.iot.sentinel.Signal;

import io.agroal.api.AgroalDataSource;
import io.quarkus.agroal.DataSource;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
public class DataEventLogic {

    @Inject
    Logger logger;

    @Inject
    @DataSource("oltp")
    AgroalDataSource tsDs;

    @Inject
    @DataSource("olap")
    AgroalDataSource olapDs;

    SentinelDaoIface sentinelDao;
    IotDatabaseIface olapDao;
    SignalDaoIface signalDao;
    IotDatabaseIface oltpDao;

    @Inject
    SignalLogic sentinelLogic;

    @Inject
    @Channel("alerts")
    Emitter<String> alertEmitter;

    void onStart(@Observes StartupEvent ev) {
        sentinelDao = new com.signomix.common.tsdb.SentinelDao();
        sentinelDao.setDatasource(tsDs);
        olapDao = new com.signomix.common.tsdb.IotDatabaseDao();
        olapDao.setDatasource(olapDs);
        signalDao = new com.signomix.common.tsdb.SignalDao();
        signalDao.setDatasource(tsDs);
        oltpDao = new com.signomix.common.tsdb.IotDatabaseDao();
        oltpDao.setDatasource(tsDs);
    }

    /**
     * Handles the event of data being received from a device.
     * Finds all sentinel definitions related to the device and checks alert
     * conditions for each one.
     * 
     * @param deviceEui the EUI of the device that sent the data
     */
    public void handleDataReceivedEvent(String deviceEui) {
        logger.info("Handling data received event: " + deviceEui);
        List<SentinelConfig> configs;
        // find all sentinel definitions related to the device
        try {
            configs = sentinelDao.getConfigsByDevice(deviceEui, 1000, 0);
        } catch (IotDatabaseException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            // TODO: inform user/admin about error
            return;
        }
        // check alert conditions
        for (int i = 0; i < configs.size(); i++) {
            // skip inactive sentinels
            if (!configs.get(i).active) {
                continue;
            }
            runSentinelCheck((SentinelConfig) configs.get(i), deviceEui);
        }
    }

    private void runSentinelCheck(SentinelConfig config, String deviceEui) {
        logger.info("Running sentinel check for config: " + config.id);
        // find all devices related to the sentinel
        Map<String, Map<String, String>> deviceChannelMap = null; // key: deviceEui, value: mapa (channel:nr_kolumny)
        try {
            deviceChannelMap = sentinelDao.getDevicesByConfigId(config.id, 1000, 0);
        } catch (IotDatabaseException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            return;
        }

        checkSentinelRelatedData(config, deviceChannelMap, deviceEui);
    }

    private void checkSentinelRelatedData(SentinelConfig config, Map deviceChannelMap, String eui) {
        List<List> values;
        try {
            values = sentinelDao.getLastValuesByConfigId(config.id);
            for (List deviceParamsAndValues : values) {
                String deviceEui = (String) deviceParamsAndValues.get(0);
                Timestamp timestamp = (Timestamp) deviceParamsAndValues.get(1);
                // od indeksu 2 do 26 sa wartosci (d1,d2,...,d24)
                Map<String, String> channelMap = (Map<String, String>) deviceChannelMap.get(deviceEui);
                HashMap<String, Double> valuesMap = new HashMap<>(); // key: channel, value: value
                // TODO: fill valuesMap
                channelMap.forEach((channel, column) -> {
                    logger.info("column: " + column + ", value: "
                            + (Double) deviceParamsAndValues.get(1 + Integer.parseInt(channel.substring(1))));
                    valuesMap.put(column,
                            (Double) deviceParamsAndValues.get(1 + Integer.parseInt(channel.substring(1))));
                });
                boolean conditionsMet = runQuery(config, valuesMap);
                int status = sentinelDao.getSentinelStatus(config.id);

                if (!conditionsMet) {
                    logger.info("Conditions not met for sentinel: " + config.id);
                    if (status != 0) {
                        // status changed to 0
                        saveResetEvent(config, deviceEui);
                    }
                    continue;
                }
                logger.info("Conditions met for sentinel: " + config.id);
                if (config.everyTime) {
                    saveEvent(config, deviceEui);
                    continue;
                } else {
                    // check if the event was already saved
                    if (status == 0) {
                        saveEvent(config, deviceEui);
                    }
                }
            }
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        }
    }

    /**
     * Runs a query on the given SentinelConfig and values map to check if the
     * conditions are met.
     * 
     * @param config the SentinelConfig to use for the query
     * @param values the map of measurement values to use for the query
     * @return true if the conditions are met, false otherwise
     */
    private boolean runQuery(SentinelConfig config, Map<String, Double> values) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            try {
                logger.info(mapper.writeValueAsString(config));
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            AlarmCondition condition;
            boolean conditionsMet = false;
            Double value;
            List conditions = config.conditions;
            LinkedHashMap<String, Object> conditionMap = new LinkedHashMap<>();
            if (conditions != null) {
                for (int i = 0; i < conditions.size(); i++) {
                    if (i > 1) {
                        break;
                    }
                    logger.info("conditions class: " + conditions.getClass().getName());
                    logger.info("conditions element class: " + conditions.get(i).getClass().getName());
                    conditionMap = (LinkedHashMap<String, Object>) conditions.get(i);
                    condition = new AlarmCondition();
                    condition.measurement = (String) conditionMap.get("measurement");
                    condition.condition1 = (Integer) conditionMap.get("condition1");
                    condition.value1 = (Double) conditionMap.get("value1");
                    condition.condition2 = (Integer) conditionMap.get("condition2");
                    condition.value2 = (Double) conditionMap.get("value2");
                    condition.orOperator = (Boolean) conditionMap.get("orOperator");
                    condition.conditionOperator = (Integer) conditionMap.get("conditionOperator");
                    logger.info("condition:  " + condition.conditionOperator + " " + condition.measurement + ", "
                            + condition.condition1 + " " + condition.value1 + " " + condition.orOperator + " "
                            + condition.condition2 + " " + condition.value2);
                    boolean ok = false;
                    value = values.get(condition.measurement);
                    if (value == null) {
                        logger.info(i + " value for " + condition.measurement + " not found");
                        continue;
                    }

                    if (condition.condition1 == AlarmCondition.CONDITION_GREATER) {
                        ok = value.compareTo(condition.value1) > 0;
                    } else if (condition.condition1 == AlarmCondition.CONDITION_LESS) {
                        ok = value.compareTo(condition.value1) < 0;
                    }
                    if (condition.orOperator) {
                        if (condition.condition2 == AlarmCondition.CONDITION_GREATER) {
                            ok = ok || value.compareTo(condition.value2) > 0;
                        } else if (condition.condition2 == AlarmCondition.CONDITION_LESS) {
                            ok = ok || value.compareTo(condition.value2) < 0;
                        }
                    }
                    if (i == 0) {
                        conditionsMet = ok;
                    } else {
                        if (null != condition.conditionOperator
                                && condition.conditionOperator == AlarmCondition.CONDITION_OPERATOR_AND) {
                            conditionsMet = conditionsMet && ok;
                        } else if (null != condition.conditionOperator
                                && condition.conditionOperator == AlarmCondition.CONDITION_OPERATOR_OR) {
                            conditionsMet = conditionsMet || ok;
                        }
                    }
                }
            }
            return conditionsMet;
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    private void saveResetEvent(SentinelConfig config, String deviceEui) {
        try {
            sentinelDao.addSentinelEvent(config.id, deviceEui, (-1 * config.alertLevel), config.conditionOkMessage,
                    config.conditionOkMessage);
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (!config.conditionOk) {
            logger.info("Condition OK not set for sentinel: " + config.id);
            return;
        }
        long createdAt = System.currentTimeMillis();
        String alertType = getAlertType(config.alertLevel);
        // alert won't be sent to its creator (owner) - only to team members and admins
        if (config.team != null && !config.team.isEmpty()) {
            String[] teamMembers = config.team.split(",");
            for (int i = 0; i < teamMembers.length; i++) {
                if (teamMembers[i].isEmpty()) {
                    continue;
                }
                saveSignal(-1 * config.alertLevel, config.id, config.organizationId, teamMembers[i], deviceEui,
                        config.conditionOkMessage, createdAt);
                sendAlert(alertType, teamMembers[i], deviceEui, config.conditionOkMessage, createdAt);
            }
        }
        if (config.administrators != null && !config.administrators.isEmpty()) {
            String[] admins = config.administrators.split(",");
            for (int i = 0; i < admins.length; i++) {
                if (admins[i].isEmpty()) {
                    continue;
                }
                saveSignal(-1 * config.alertLevel, config.id, config.organizationId, admins[i], deviceEui,
                        config.conditionOkMessage, createdAt);
                sendAlert(alertType, admins[i], deviceEui, config.conditionOkMessage, createdAt);
            }
        }
    }

    private void saveEvent(SentinelConfig config, String deviceEui) {
        logger.info("Saving event for sentinel: " + config.id);
        String alertType = getAlertType(config.alertLevel);
        long createdAt = System.currentTimeMillis();
        try {
            sentinelDao.addSentinelEvent(config.id, deviceEui, config.alertLevel, config.alertMessage,
                    config.alertMessage);
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // alert won't be sent to its creator (owner) - only to team members and admins
        if (config.team != null && !config.team.isEmpty()) {
            String[] teamMembers = config.team.split(",");
            for (int i = 0; i < teamMembers.length; i++) {
                if (teamMembers[i].isEmpty()) {
                    continue;
                }
                saveSignal(config.alertLevel, config.id, config.organizationId, teamMembers[i], deviceEui,
                        config.alertMessage, createdAt);
                sendAlert(alertType, teamMembers[i], deviceEui, config.alertMessage, createdAt);
            }
        }
        if (config.administrators != null && !config.administrators.isEmpty()) {
            String[] admins = config.administrators.split(",");
            for (int i = 0; i < admins.length; i++) {
                if (admins[i].isEmpty()) {
                    continue;
                }
                saveSignal(config.alertLevel, config.id, config.organizationId, admins[i], deviceEui,
                        config.alertMessage, createdAt);
                sendAlert(alertType, admins[i], deviceEui, config.alertMessage, createdAt);
            }
        }
    }

    private void sendAlert(String alertType, String userId, String deviceEui, String alertMessage, long createdAt) {
        try {
            oltpDao.addAlert(alertType, deviceEui, userId, alertMessage, createdAt);
        } catch (IotDatabaseException e) {
            e.printStackTrace();
        }
        alertEmitter.send(userId + "\t" + deviceEui + "\t" + alertType + "\t" + alertMessage);
    }

    private void saveSignal(int alertLevel, long configId, long organizationId, String userId, String deviceEui,
            String alertMessage, long createdAt) {
        try {
            Signal signal = new Signal();
            signal.deviceEui = deviceEui;
            signal.level = alertLevel;
            signal.messageEn = alertMessage;
            signal.messagePl = alertMessage;
            signal.sentinelConfigId = configId;
            signal.userId = userId;
            signal.organizationId = organizationId;
            signalDao.saveSignal(signal);
        } catch (IotDatabaseException e) {
            e.printStackTrace();
        }
    }

    private String getAlertType(int alertLevel) {
        String alertType;
        switch (alertLevel) {
            case 0:
                alertType = "GENERAL";
                break;
            case 1:
                alertType = "INFO";
                break;
            case 2:
                alertType = "WARNING";
                break;
            case 3:
                alertType = "ALERT";
                break;
            default:
                alertType = "ALERT";
        }
        return alertType;
    }

}
