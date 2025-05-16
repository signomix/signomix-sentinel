package com.signomix.sentinel.domain;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.eclipse.microprofile.config.inject.ConfigProperty;
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
import com.signomix.common.iot.sentinel.SentinelConfig;
import com.signomix.common.iot.sentinel.Signal;

import io.agroal.api.AgroalDataSource;
import io.quarkus.agroal.DataSource;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

public abstract class EventLogic {

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

    @ConfigProperty(name = "signomix.signals.used", defaultValue = "false")
    Boolean signalsUsed;

    @Inject
    Vertx vertx;

    public static final int EVENT_TYPE_DATA = 0;
    public static final int EVENT_TYPE_COMMAND = 1;
    public static final int EVENT_TYPE_DEVICE = 2;

    HashMap<String, HashMap<Long, SentinelConfig>> messageConfigs = new HashMap<>();

    void onApplicationStart(@Observes StartupEvent e) {
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
     * @param eui           the EUI of the device that sent the data
     * @param commandString the command string received from the device
     * @param messageId     the message ID of the received data
     */
    public void handleEvent(int type, String eui, String commandString, String messageId) {
        // logger.info("Handling data received event: " + eui);
        // testJsInterpreter(deviceEui);
        // testPythonInterpreter(deviceEui);
        String deviceEui = null;
        String command = commandString;
        String jsonString = null;
        if (command != null && (command.startsWith("&") || command.startsWith("#"))) {
            command = command.substring(1);
        }
        String[] commandParts = {};
        if (command != null) {
            commandParts = command.split(";", -1);
        }
        if (type == EventLogic.EVENT_TYPE_COMMAND && commandParts.length < 2) {
            logger.error("Invalid command: " + command);
            return;
        }

        String tag = "";
        String tagValue = "";
        String[] groups = new String[0];
        if (type == EventLogic.EVENT_TYPE_COMMAND) {
            deviceEui = commandParts[0];
            jsonString = commandParts[1];
        } else {
            deviceEui = eui;
        }
        Device device = null;
        try {
            device = olapDao.getDevice(deviceEui, false);
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
            logger.error(e.getMessage());
            return;
        }

        HashMap<Long, SentinelConfig> configs = new HashMap<>();
        // find all sentinel definitions related to the device
        try {
            List<SentinelConfig> configList = sentinelDao.getConfigsByDevice(deviceEui, 1000, 0, type);
            for (SentinelConfig config : configList) {
                if (config.active) {
                    configs.put(config.id, config);
                }
            }
        } catch (IotDatabaseException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            // TODO: inform user/admin about error
            return;
        }
        if (!tag.isEmpty() && !tagValue.isEmpty()) {
            try {
                List<SentinelConfig> tagConfigs = sentinelDao.getConfigsByTag(tag, tagValue, 1000, 0, type);
                logger.info("Number of sentinel configs for tag: " + deviceEui + " " + tag + ":" + tagValue + " "
                        + tagConfigs.size());
                for (SentinelConfig config : tagConfigs) {
                    if (config.active) {
                        configs.put(config.id, config);
                    }
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
                List<SentinelConfig> groupConfigs = sentinelDao.getConfigsByGroup(groups[i].trim(), 1000, 0, type);
                for (SentinelConfig config : groupConfigs) {
                    if (config.active) {
                        configs.put(config.id, config);
                    }
                }
                groupConfigs.clear();
            }
        } catch (IotDatabaseException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }

        messageConfigs.put(messageId, configs);

        // check alert conditions for each sentinel definition from configs map
        logger.info("Number of sentinel configs: " + deviceEui + " " + configs.size());
        Iterator it = configs.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            SentinelConfig config = (SentinelConfig) pair.getValue();
            if (!config.active) {
                continue;
            }
            runSentinelCheckForConfig(messageId, config, device, jsonString);
        }
    }

    void runSentinelCheckForConfig(String messageId, SentinelConfig config, Device device, String jsonString) {
        logger.info("Running sentinel check for config: " + config.id);

        if (config.eventType == SentinelConfig.EVENT_TYPE_COMMAND) {
            if (!config.useScript) {
                logger.warn("Command event received, but script is not set");
                return;
            }
            if (config.script == null || config.script.isEmpty()) {
                logger.warn("Command event received, but script is empty");
                return;
            }
            runPythonScript(config, device, jsonString);
            return;
        }

        // In the map, key==deviceEui, value==(map of {columnName:channel}) where
        // columnName is d1, d2, ..., d24
        Map<String, Map<String, String>> deviceChannelMap = null;
        try {
            if (config.checkOthers) {
                deviceChannelMap = sentinelDao.getDeviceChannelsByConfigId(config.id);
            } else {
                deviceChannelMap = sentinelDao.getDeviceChannelsByConfigAndEui(config.id, device.getEUI());
            }
        } catch (IotDatabaseException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            return;
        }
        if (deviceChannelMap == null || deviceChannelMap.isEmpty()) {
            logger.info("No devices found for sentinel: " + config.id);
            return;
        }
        checkSentinelRelatedData(messageId, config, deviceChannelMap, device.getEUI());
    }

    void processResult(String messageId, ConditionResult conditionResult) {
        // From all the configs processed for the messageId, find the one
        // that was used to create the conditionResult
        HashMap<Long, SentinelConfig> configs = null;
        configs = messageConfigs.get(messageId);
        SentinelConfig config = null;
        if (configs != null) {
            config = configs.get(conditionResult.configId);
        } else {
            logger.error("No configs found for messageId: " + messageId);
            return;
        }

        logger.info("Processing result: " + conditionResult);

        if (conditionResult.command != null && conditionResult.commandTarget != null) {
            try {
                logger.info("Command: " + conditionResult.command);
                logger.info("Command target: " + conditionResult.commandTarget);
                oltpDao.putDeviceCommand(conditionResult.commandTarget, "ACTUATOR_CMD", conditionResult.command,
                        System.currentTimeMillis());
                // mqtt message about created command will not be send to prevent loops
            } catch (IotDatabaseException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }
        } else {
            if (conditionResult.error) {
                logger.error("Error while processing result: " + conditionResult.errorMessage);
            }
            int status = 0;
            Device device = null;
            try {
                status = sentinelDao.getSentinelStatus(config.id);
                device = oltpDao.getDevice(conditionResult.eui, false);
            } catch (IotDatabaseException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }
            if (device != null) {
                if (conditionResult.violated) {
                    logger.info("Condition violated: " + conditionResult.configId + " " + conditionResult.measurement
                            + " " + conditionResult.value);
                    logger.info("Conditions met for sentinel: " + config.id);
                    if (config.everyTime) {
                        saveEvent(config, device, conditionResult);
                    } else {
                        // check if the event was already saved
                        if (status <= 0) {
                            saveEvent(config, device, conditionResult);
                        }
                    }
                } else {
                    logger.info("Condition not violated: "+ conditionResult.configId + " " + conditionResult.measurement + " " + conditionResult.value);
                    logger.info("Conditions not met for sentinel: " + config.id);
                    if (status > 0) {
                        // status changed to 0
                        saveResetEvent(config, device, conditionResult);
                    }
                }
            } else {
                logger.error("Device not found: " + conditionResult.eui);
            }
        }

        //
        config.processed = true;
        configs.put(conditionResult.configId, config);
        messageConfigs.put(messageId, configs);

        // finally check if all configs related to the messageId are processed
        boolean complete = true;
        for (Map.Entry<Long, SentinelConfig> entry : configs.entrySet()) {
            complete = complete && entry.getValue().processed;
            logger.info("Config processed: " + entry.getKey() + " " + entry.getValue().processed);
        }
        if (complete) {
            logger.info("All configs processed for messageId: " + messageId);
            messageConfigs.remove(messageId);
        }

    }

    void sendAlert(String alertType, String userId, String deviceEui, String alertSubject, String alertMessage,
            long createdAt) {
        if (!signalsUsed) {
            try {
                oltpDao.addAlert(alertType, deviceEui, userId, alertMessage, createdAt);
            } catch (IotDatabaseException e) {
                e.printStackTrace();
            }
        }
        logger.info("Sending alert: " + userId + ";" + deviceEui + ";" + alertType + ";" + alertMessage + ";"
                + alertSubject);
        alertEmitter.send(userId + "\t" + deviceEui + "\t" + alertType + "\t" + alertMessage + "\t" + alertSubject);
    }

    void saveSignal(int alertLevel, long configId, long organizationId, String userId, String deviceEui,
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

    String getAlertType(int alertLevel) {
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

    String getMessageSubject(String message) {
        int idx = message.indexOf("{info}");
        if (idx < 0) {
            return "";
        }
        String subject = message.substring(0, idx);
        return subject;
    }

    String getMessageBody(String message) {
        int idx = message.indexOf("{info}");
        if (idx < 0) {
            return message;
        }
        String body = message.substring(idx + 6);
        return body;
    }

    String transformMessage(String message, SentinelConfig config,
            Device device, DeviceGroup group, ConditionResult violationResult) {
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

        if (violationResult.measurement != null) {
            result = result.replaceAll("\\{measurement\\}", violationResult.measurement);
            result = result.replaceAll("\\{var\\}", violationResult.measurement);
        }
        if (violationResult.value != null) {
            result = result.replaceAll("\\{value\\}", violationResult.value.toString());
        }

        return result;
    }

    /**
     * Transforms the team variables {device.team}, {devic.admins}, {device.owner}
     * to a list of user IDs.
     * 
     * @param team
     * @param device
     * @return
     */
    String transformTeam(String team, Device device) {
        if (team == null || team.isEmpty()) {
            return "";
        }
        String result = team;
        String deviceTeam = device.getTeam();
        String deviceAdmins = device.getAdministrators();
        String deviceOwner = device.getUserID();
        if (deviceTeam != null && !deviceTeam.isEmpty()) {
            result = result.replace("{device.team}", deviceTeam.trim());
        }
        if (deviceAdmins != null && !deviceAdmins.isEmpty()) {
            result = result.replace("{device.admins}", deviceAdmins.trim());
        }
        if (deviceOwner != null && !deviceOwner.isEmpty()) {
            result = result.replace("{device.owner}", deviceOwner.trim());
        }

        return result;
    }

    private void saveResetEvent(SentinelConfig config, Device device, ConditionResult violationResult) {
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
        String team = transformTeam(config.team, device);
        if (!team.isEmpty()) {
            String[] teamMembers = team.split(",");
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

    private void saveEvent(SentinelConfig config, Device device, ConditionResult violationResult) {
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
        String team = transformTeam(config.team, device);
        if (!team.isEmpty()) {
            String[] teamMembers = team.split(",");
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

    abstract ConditionResult runPythonScript(SentinelConfig config, Device device, String jsonString);

    abstract ConditionResult runPythonScript(SentinelConfig config, Device device, Map deviceChannelMap,
            List<List<LastDataPair>> values);

    abstract void checkSentinelRelatedData(String messageId, SentinelConfig config, Map deviceChannelMap, String eui);
}
