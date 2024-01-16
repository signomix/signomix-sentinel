package com.signomix.sentinel.domain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;

import com.signomix.common.Tag;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.db.SentinelDaoIface;
import com.signomix.common.db.SignalDaoIface;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.DeviceGroup;
import com.signomix.common.iot.LastDataPair;
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
        String tag = "";
        String tagValue = "";
        String[] groups = new String[0];
        try {
            Device device = olapDao.getDevice(deviceEui, false);
            List<Tag> tags = olapDao.getDeviceTags(deviceEui);
            logger.info("tags: " + deviceEui + " " + tags.size());
            if (tags.size() > 0) {
                // TODO: handle multiple tags
                logger.info("tag: " + tags.get(0).name + " " + tags.get(0).value);
                tag = tags.get(0).name;
                tagValue = tags.get(0).value;
            }
            groups = device.getGroups().split(",");
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        HashMap<Long, SentinelConfig> configs = new HashMap<>();
        // find all sentinel definitions related to the device
        try {
            List<SentinelConfig> configList = sentinelDao.getConfigsByDevice(deviceEui, 1000, 0);
            for (SentinelConfig config : configList) {
                configs.put(config.id, config);
            }
        } catch (IotDatabaseException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            // TODO: inform user/admin about error
            return;
        }
        if (!tag.isEmpty() && !tagValue.isEmpty()) {
            try {
                List<SentinelConfig> tagConfigs = sentinelDao.getConfigsByTag(tag, tagValue, 1000, 0);
                logger.info("Number of sentinel configs for tag: " + deviceEui + " " + tag + ":" + tagValue + " "
                        + tagConfigs.size());
                for (SentinelConfig config : tagConfigs) {
                    configs.put(config.id, config);
                }
            } catch (IotDatabaseException e) {
                logger.error(e.getMessage());
                e.printStackTrace();
            }
        }
        try {
            String groupName;
            for (int i = 0; i < groups.length; i++) {
                groupName = groups[i].trim();
                if (groupName.isEmpty()) {
                    continue;
                }
                List<SentinelConfig> groupConfigs = sentinelDao.getConfigsByGroup(groups[i].trim(), 1000, 0);
                for (SentinelConfig config : groupConfigs) {
                    configs.put(config.id, config);
                }
                groupConfigs.clear();
            }
        } catch (IotDatabaseException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }

        // check alert conditions for each sentinel definition from configs map
        logger.info("Number of sentinel configs: " + deviceEui + " " + configs.size());
        Iterator it = configs.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            SentinelConfig config = (SentinelConfig) pair.getValue();
            if (!config.active) {
                continue;
            }
            runSentinelCheck(config, deviceEui);
        }
    }

    /**
     * Runs a sentinel check for the given SentinelConfig.
     * 
     * @param config    SentinelConfig to use for the check
     * @param deviceEui the EUI of the device that sent the data which triggered the
     *                  check
     */
    private void runSentinelCheck(SentinelConfig config, String deviceEui) {
        logger.info("Running sentinel check for config: " + config.id);
        // In the map, key==deviceEui, value==(map of {columnName:channel}) where
        // columnName is d1, d2, ..., d24
        Map<String, Map<String, String>> deviceChannelMap = null;
        try {
            deviceChannelMap = sentinelDao.getDeviceChannelsByConfigId(config.id);
        } catch (IotDatabaseException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            return;
        }
        if (deviceChannelMap == null || deviceChannelMap.isEmpty()) {
            logger.info("No devices found for sentinel: " + config.id);
            return;
        }
        checkSentinelRelatedData(config, deviceChannelMap, deviceEui);
    }

    private void checkSentinelRelatedData(SentinelConfig config, Map deviceChannelMap, String eui) {
        List<List<LastDataPair>> values;
        logger.info("Checking sentinel related data for sentinel: " + config.id);
        ConditionViolationResult result = new ConditionViolationResult();
        try {
            values = sentinelDao.getLastValuesOfDevices(deviceChannelMap.keySet(), config.timeShift * 60);
            logger.info(config.id + " number of values: " + values.size());
            boolean configConditionsMet = false; // true if at least one device meets the conditions
            for (List deviceParamsAndValues : values) {
                String deviceEui = ((LastDataPair) deviceParamsAndValues.get(0)).eui;
                result = runConfigQuery(config, deviceEui, deviceChannelMap, values);
                configConditionsMet = configConditionsMet || result.violated;
            }
            int status = sentinelDao.getSentinelStatus(config.id);
            Device device = null;
            DeviceGroup group = null;
            if (!configConditionsMet) {
                logger.info("Conditions not met for sentinel: " + config.id);
                if (status > 0) {
                    // status changed to 0
                    if (device == null) {
                        device = oltpDao.getDevice(eui, false);
                    }
                    saveResetEvent(config, device, result);
                }
                return;
            }
            // conditions met
            logger.info("Conditions met for sentinel: " + config.id);
            if (device == null) {
                device = oltpDao.getDevice(eui, false);
            }
            if (config.everyTime) {
                saveEvent(config, device, result);
            } else {
                // check if the event was already saved
                if (status <= 0) {
                    saveEvent(config, device, result);
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
     * @param config           the SentinelConfig to use for the query
     * @param deviceEui        the EUI of the device that sent the data which
     *                         triggered the check
     * @param deviceChannelMap the map of device EUIs and their channels
     * @param values           the map of measurement values to use for the query
     * @return
     */
    private ConditionViolationResult runConfigQuery(SentinelConfig config, String deviceEui, Map deviceChannelMap,
            List<List<LastDataPair>> values) {
        ConditionViolationResult result = new ConditionViolationResult();
        result.violated = false;
        result.value = null;
        result.measurement = "";
        try {
            // for debugging
            /*
             * ObjectMapper mapper = new ObjectMapper();
             * try {
             * logger.info(mapper.writeValueAsString(config));
             * } catch (JsonProcessingException e) {
             * e.printStackTrace();
             * }
             */
            // end for debugging

            // TODO: take into account previous values (required for hysteresis)

            logger.info("deviceChannelMap size: " + deviceChannelMap.size());
            logger.info("conditions: " + config.conditions.size());
            AlarmCondition condition;
            boolean conditionsMet = false;
            List conditions = config.conditions;
            LinkedHashMap<String, Object> conditionMap = new LinkedHashMap();
            if (conditions != null) {
                boolean actualConditionMet;
                for (int i = 0; i < conditions.size(); i++) {
                    actualConditionMet = false;
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
                    result.measurement = condition.measurement;
                    // as values holds all measurement values for all devices, we need to get only
                    // values for the selected measurement (condition.measurement) from all devices
                    ArrayList<LastDataPair> valuesList = new ArrayList<>();

                    int measurementInex;
                    LastDataPair dataToCheck;
                    for (int j = 0; j < values.size(); j++) {
                        // get column index for measurement
                        logger.info("getting channels map for deviceEui: " + deviceEui);
                        Map<String, String> measurementMap = (Map) deviceChannelMap.get(deviceEui);
                        String columnNumberStr = measurementMap.get(condition.measurement);
                        // logger.info("COLUMN NAME: "+columnNumberStr);
                        measurementInex = Integer.parseInt(columnNumberStr.substring(1));
                        measurementInex--; // column numbers start from 1 (name d1), but list indexes start from 0
                        dataToCheck = (LastDataPair) values.get(j).get(measurementInex);
                        valuesList.add(dataToCheck);
                    }
                    String valuesListStr = condition.measurement
                            + (condition.condition1 == 1 ? " > " : " < " + condition.value1);
                    String valuesListStr2 = "";
                    if (condition.value2 != null) {
                        valuesListStr2 = (condition.orOperator ? " or " : "")
                                + (condition.condition2 == 1 ? " > " : " < " + condition.value2);
                    }
                    if (condition.orOperator) {
                        valuesListStr += valuesListStr2;
                    }
                    logger.info("Condition to check: " + valuesListStr);
                    if (valuesList.size() == 0) {
                        logger.info(i + " values for " + condition.measurement + " not found");
                        continue;
                    }
                    // Double tmpValue
                    Double hysteresis = Math.abs(config.hysteresis);
                    Double valueToCheck;
                    Double diff;
                    if (condition.condition1 == AlarmCondition.CONDITION_GREATER) {
                        for (int j = 0; j < valuesList.size(); j++) {
                            valueToCheck = valuesList.get(j).value;
                            diff = valuesList.get(j).delta;
                            logger.info("VALUE: " + valueToCheck);
                            if (diff >= 0) {
                                actualConditionMet = actualConditionMet
                                        || (valueToCheck.compareTo(condition.value1) > 0);
                            } else {
                                actualConditionMet = actualConditionMet
                                        || (valueToCheck.compareTo(condition.value1 - hysteresis) >= 0);
                            }
                        }
                        // ok = value.compareTo(condition.value1) > 0;
                    } else if (condition.condition1 == AlarmCondition.CONDITION_LESS) {
                        for (int j = 0; j < valuesList.size(); j++) {
                            valueToCheck = valuesList.get(j).value;
                            diff = valuesList.get(j).delta;
                            logger.info("VALUE: " + valueToCheck);
                            if (diff <= 0) {
                                actualConditionMet = actualConditionMet
                                        || (valueToCheck.compareTo(condition.value1) < 0);
                            } else {
                                actualConditionMet = actualConditionMet
                                        || (valueToCheck.compareTo(condition.value1 + hysteresis) <= 0);
                            }
                        }
                        // ok = value.compareTo(condition.value1) < 0;
                    }
                    if (condition.orOperator && condition.value2 != null) {
                        if (condition.condition2 == AlarmCondition.CONDITION_GREATER) {
                            for (int j = 0; j < valuesList.size(); j++) {
                                valueToCheck = valuesList.get(j).value;
                                diff = valuesList.get(j).delta;
                                logger.info("VALUE: " + valueToCheck);
                                if (diff >= 0) {
                                    actualConditionMet = actualConditionMet
                                            || (valueToCheck.compareTo(condition.value2) > 0);
                                } else {
                                    actualConditionMet = actualConditionMet
                                            || (valueToCheck.compareTo(condition.value2 - hysteresis) >= 0);
                                }
                            }
                            // ok = ok || value.compareTo(condition.value2) > 0;
                        } else if (condition.condition2 == AlarmCondition.CONDITION_LESS) {
                            for (int j = 0; j < valuesList.size(); j++) {
                                valueToCheck = valuesList.get(j).value;
                                diff = valuesList.get(j).delta;
                                logger.info("VALUE: " + valueToCheck);
                                if (diff <= 0) {
                                    actualConditionMet = actualConditionMet
                                            || (valueToCheck.compareTo(condition.value1) < 0);
                                } else {
                                    actualConditionMet = actualConditionMet
                                            || (valueToCheck.compareTo(condition.value1 + hysteresis) <= 0);
                                }
                            }
                            // ok = ok || value.compareTo(condition.value2) < 0;
                        }
                    } else if( (!condition.orOperator) && condition.value2 == null) {
                        // TODO
                    }
                    if (i == 0) {
                        conditionsMet = actualConditionMet;
                    } else {
                        if (null != condition.conditionOperator
                                && condition.conditionOperator == AlarmCondition.CONDITION_OPERATOR_AND) {
                            conditionsMet = conditionsMet && actualConditionMet;
                        } else if (null != condition.conditionOperator
                                && condition.conditionOperator == AlarmCondition.CONDITION_OPERATOR_OR) {
                            conditionsMet = conditionsMet || actualConditionMet;
                        }
                    }
                }
            }
            result.violated = conditionsMet;
            return result;
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            result.violated = false;
            return result;
        }
    }

    private void saveResetEvent(SentinelConfig config, Device device, ConditionViolationResult violationResult) {
        DeviceGroup group = null;
        if (config.groupEui != null && !config.groupEui.isEmpty()) {
            try {
                group = olapDao.getGroup(config.groupEui);
            } catch (IotDatabaseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        String message = transformMessage(getMessageBody(config.conditionOkMessage), config, device, group,
                violationResult);
        String alertSubject = transformMessage(getMessageSubject(config.conditionOkMessage), config, device, group,
                violationResult);
        try {
            sentinelDao.addSentinelEvent(config.id, device.getEUI(), (-1 * config.alertLevel), message, message);
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (!config.conditionOk) {
            // logger.info("Condition OK not set for sentinel: " + config.id);
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
                saveSignal(-1 * config.alertLevel, config.id, config.organizationId, teamMembers[i], device.getEUI(),
                        alertSubject, message, createdAt);
                sendAlert(alertType, teamMembers[i], device.getEUI(), alertSubject, message, createdAt);
            }
        }
        if (config.administrators != null && !config.administrators.isEmpty()) {
            String[] admins = config.administrators.split(",");
            for (int i = 0; i < admins.length; i++) {
                if (admins[i].isEmpty()) {
                    continue;
                }
                saveSignal(-1 * config.alertLevel, config.id, config.organizationId, admins[i], device.getEUI(),
                        alertSubject, message, createdAt);
                sendAlert(alertType, admins[i], device.getEUI(), alertSubject, message, createdAt);
            }
        }
    }

    private void saveEvent(SentinelConfig config, Device device, ConditionViolationResult violationResult) {
        logger.info("Saving event for sentinel: " + config.id);
        DeviceGroup group = null;
        if (config.groupEui != null && !config.groupEui.isEmpty()) {
            try {
                group = olapDao.getGroup(config.groupEui);
            } catch (IotDatabaseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        String message = transformMessage(getMessageBody(config.alertMessage), config, device, group, violationResult);
        String alertSubject = transformMessage(getMessageSubject(config.alertMessage), config, device, group,
                violationResult);

        String alertType = getAlertType(config.alertLevel);
        long createdAt = System.currentTimeMillis();
        try {
            sentinelDao.addSentinelEvent(config.id, device.getEUI(), config.alertLevel, message,
                    message);
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
                saveSignal(config.alertLevel, config.id, config.organizationId, teamMembers[i], device.getEUI(),
                        alertSubject, message, createdAt);
                sendAlert(alertType, teamMembers[i], device.getEUI(), alertSubject, message, createdAt);
            }
        }
        if (config.administrators != null && !config.administrators.isEmpty()) {
            String[] admins = config.administrators.split(",");
            for (int i = 0; i < admins.length; i++) {
                if (admins[i].isEmpty()) {
                    continue;
                }
                saveSignal(config.alertLevel, config.id, config.organizationId, admins[i], device.getEUI(),
                        alertSubject, message, createdAt);
                sendAlert(alertType, admins[i], device.getEUI(), alertSubject, message, createdAt);
            }
        }
    }

    private void sendAlert(String alertType, String userId, String deviceEui, String alertSubject, String alertMessage,
            long createdAt) {
        try {
            oltpDao.addAlert(alertType, deviceEui, userId, alertMessage, createdAt);
        } catch (IotDatabaseException e) {
            e.printStackTrace();
        }
        alertEmitter.send(userId + "\t" + deviceEui + "\t" + alertType + "\t" + alertMessage + "\t" + alertSubject);
    }

    private void saveSignal(int alertLevel, long configId, long organizationId, String userId, String deviceEui,
            String alertSubject, String alertMessage, long createdAt) {
        try {
            Signal signal = new Signal();
            signal.deviceEui = deviceEui;
            signal.level = alertLevel;
            signal.subjectPl = alertSubject;
            signal.subjectEn = alertSubject;
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

    private String getMessageSubject(String message) {
        int idx = message.indexOf("{info}");
        if (idx < 0) {
            return "";
        }
        String subject = message.substring(0, idx);
        return subject;
    }

    private String getMessageBody(String message) {
        int idx = message.indexOf("{info}");
        if (idx < 0) {
            return message;
        }
        String body = message.substring(idx + 6);
        return body;
    }

    private String transformMessage(String message, SentinelConfig config,
            Device device, DeviceGroup group, ConditionViolationResult violationResult) {
        String result = message;
        String targetEui = "";
        String targetName = "";
        if (config.deviceEui != null && !config.deviceEui.isEmpty()) {
            targetEui = config.deviceEui;
            targetName = device.getName(); // in this case configured target is the same as the device that triggered
                                           // the alert
        }
        if (config.groupEui != null && !config.groupEui.isEmpty()) {
            targetEui = config.groupEui;
            if (group != null) {
                targetName = group.getName();
            }
        }
        result = result.replaceAll("\\{target.eui\\}", targetEui);
        result = result.replaceAll("\\{target.name\\}", targetName);
        result = result.replaceAll("\\{tag.name\\}", config.tagName);
        result = result.replaceAll("\\{tag.value\\}", config.tagValue);
        result = result.replaceAll("\\{device.eui\\}", device.getEUI());
        result = result.replaceAll("\\{device.name\\}", device.getName());
        result = result.replaceAll("\\{var\\}", violationResult.measurement);
        result = result.replaceAll("\\{value\\}", "");
        return result;
    }

}
