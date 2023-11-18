package com.signomix.sentinel.domain;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.db.SentinelDaoIface;
import com.signomix.common.db.SignalDaoIface;
import com.signomix.common.iot.sentinel.AlarmCondition;
import com.signomix.common.iot.sentinel.SentinelConfig;

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

    @Inject
    SignalLogic sentinelLogic;

    void onStart(@Observes StartupEvent ev) {
        sentinelDao = new com.signomix.common.tsdb.SentinelDao();
        sentinelDao.setDatasource(tsDs);
        olapDao = new com.signomix.common.tsdb.IotDatabaseDao();
        olapDao.setDatasource(olapDs);
        signalDao = new com.signomix.common.tsdb.SignalDao();
        signalDao.setDatasource(tsDs);
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
        for (int i=0; i<configs.size(); i++) {
            runSentinelCheck((SentinelConfig)configs.get(i), deviceEui);
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

        // create and save alert object
        //Alert alert = new Alert(null, 0, deviceEui, "after receiving data from " + deviceEui + " sentinel fire alarm");
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
                    valuesMap.put(column, (Double) deviceParamsAndValues.get(1+Integer.parseInt(channel.substring(1))));
                });
                boolean conditionsMet = runQuery(config, valuesMap);

                if (conditionsMet) {
                    logger.info("Conditions met for sentinel: " + config.id);
                    /* Signal signal= new Signal();
                    signal.deviceEui = deviceEui;
                    signal.level = config.alertLevel;
                    signal.messageEn = config.alertMessage;
                    signal.messagePl = config.alertMessage;
                    signal.sentinelConfigId = config.id;
                    signal.userId = null;
                    signal.organizationId = null;

                    logger.info("Signal fired: " + signal.toString());
                    try {
                        signalDao.saveSignal(signal);
                    } catch (IotDatabaseException e) {
                        logger.error(e.getMessage());
                        e.printStackTrace();
                    } */
                    sentinelDao.addSentinelEvent(config.id, deviceEui, config.alertLevel, config.alertMessage, config.alertMessage);
                }else{
                    logger.info("Conditions not met for sentinel: " + config.id);
                }
            }
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        }
    }

    /**
     * Runs a query on the given SentinelConfig and values map to check if the conditions are met.
     * @param config the SentinelConfig to use for the query
     * @param values the map of measurement values to use for the query
     * @return true if the conditions are met, false otherwise
     */
    private boolean runQuery(SentinelConfig config, Map<String, Double> values) {
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
                if(i>1){
                    break;
                }
                logger.info("conditions class: "+conditions.getClass().getName());
                logger.info("conditions element class: "+conditions.get(i).getClass().getName());
                conditionMap=(LinkedHashMap<String, Object>)conditions.get(i);
                condition = new AlarmCondition();
                condition.measurement = (String) conditionMap.get("measurement");
                condition.condition1 = (Integer) conditionMap.get("condition1");
                condition.value1 = (Double) conditionMap.get("value1");
                condition.condition2 = (Integer) conditionMap.get("condition2");
                condition.value2 = (Double) conditionMap.get("value2");
                condition.orOperator = (Boolean) conditionMap.get("orOperator");
                condition.conditionOperator = (Integer) conditionMap.get("conditionOperator");

                boolean ok=false;
                value = values.get(condition.measurement);
                if (value == null) {
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
                    if (condition.conditionOperator == AlarmCondition.CONDITION_OPERATOR_AND) {
                        conditionsMet = conditionsMet && ok;
                    } else if (condition.conditionOperator == AlarmCondition.CONDITION_OPERATOR_OR) {
                        conditionsMet = conditionsMet || ok;
                    }
                }
            }
        }
        return conditionsMet;
    }

}
