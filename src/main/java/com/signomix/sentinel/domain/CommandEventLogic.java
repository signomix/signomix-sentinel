package com.signomix.sentinel.domain;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;
import org.python.core.PyException;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.signomix.common.Tag;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.db.SentinelDaoIface;
import com.signomix.common.db.SignalDaoIface;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.sentinel.SentinelConfig;

import io.agroal.api.AgroalDataSource;
import io.quarkus.agroal.DataSource;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
public class CommandEventLogic {

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

    // @Inject
    // @Channel("command-created")
    // Emitter<String> commandCreatedEmitter;

    @ConfigProperty(name = "signomix.signals.used", defaultValue = "false")
    Boolean signalsUsed;

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
     * @param commandString the EUI of the device that sent the data
     */
    public void handleCommandCreatedEvent(String commandString) {
        // logger.info("Handling data received event: " + deviceEui);
        // testJsInterpreter(deviceEui);
        // testPythonInterpreter(deviceEui);
        String command=commandString;
        if(command.startsWith("&")|| command.startsWith("#")) {
            command=command.substring(1);
        }
        String[] commandParts = command.split(";");
        if (commandParts.length < 2) {
            logger.error("Invalid command: " + command);
            return;
        }
        

        String tag = "";
        String tagValue = "";
        String[] groups = new String[0];
        String deviceEui = commandParts[0];
        String jsonString = commandParts[1];
        Device device=null;

        logger.info("Command received: " + deviceEui + " " + jsonString);
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
            List<SentinelConfig> configList = sentinelDao.getConfigsByDevice(deviceEui, 1000, 0, SentinelConfig.EVENT_TYPE_COMMAND);
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
                List<SentinelConfig> tagConfigs = sentinelDao.getConfigsByTag(tag, tagValue, 1000, 0, SentinelConfig.EVENT_TYPE_COMMAND);
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
                List<SentinelConfig> groupConfigs = sentinelDao.getConfigsByGroup(groups[i].trim(), 1000, 0, SentinelConfig.EVENT_TYPE_COMMAND);
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
            runSentinelCheck(config, device, jsonString);
        }
    }

    /**
     * Runs a sentinel check for the given SentinelConfig.
     * 
     * @param config    SentinelConfig to use for the check
     * @param deviceEui the EUI of the device that sent the data which triggered the
     *                  check
     */
    private void runSentinelCheck(SentinelConfig config, Device device, String jsonString) {
        logger.info("Running sentinel check for config: " + config.id);
        if (config.useScript && config.script != null && !config.script.isEmpty()) {
            logger.info("Running script : " + config.script);
            ConditionResult result = runPythonScript(config, device, jsonString);
            if(result.command!=null && result.commandTarget!=null) {
                try {
                    logger.info("Command: " + result.command);
                    logger.info("Command target: " + result.commandTarget);
                    oltpDao.putDeviceCommand(result.commandTarget, "ACTUATOR_CMD", result.command, System.currentTimeMillis());
                    // mqtt message about created command will not be send to prevent loops 
                } catch (IotDatabaseException e) {
                    e.printStackTrace();
                    logger.error(e.getMessage());
                }
            }
        }

    }

    private ConditionResult runPythonScript(SentinelConfig config, Device device, String jsonString) {
        ConditionResult result = new ConditionResult();
        long startTime = System.currentTimeMillis();
        HashMap<String,Object> commandMap;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            logger.info("Running Python script for sentinel: " + config.id);
            String jString = jsonString.trim();
            if(jString.startsWith("&") || jString.startsWith("#")) {
                jString = jString.substring(1);
            }
            commandMap = objectMapper.readValue(jString, HashMap.class);
            String script = """
                    def process_java_object(config_obj, device_obj, commandMap):
                        global config
                        config = config_obj
                        global device
                        device = device_obj
                        result = ""
                        global command
                        command = commandMap
                        result = checkRule()
                        return result
                    
                    def getDeviceGroupName(groupNumber)
                        if device is None:
                            return None
                        if device.get("groups") is None:
                            return None
                        groups = device.get("groups").split(",")
                        if groupNumber >= len(groups):
                            return None
                        if groups[groupNumber] is None or groups[groupNumber].length() == 0:
                            return None
                        return groups[groupNumber]

                    def getCommandParam(commandParameter):
                        global command
                        if command is None:
                            return None
                        if command.get(commandParameter) is None:
                            return None
                        return command.get(commandParameter)

                    def conditionsNotMet():
                        return ";;"

                    def conditionsMetWithCommand(measurement, value, commandTarget, command):
                        if value is None:
                            return config.deviceEui + ";" + measurement + ";;" + commandTarget + ";" + command
                        return config.deviceEui + ";" + measurement + ";" + str(value) + ";" + commandTarget + ";" + command

                    def newCommand(commandTarget, command):
                        if commandTarget is None or command is None:
                            return conditionsNotMet()
                        return ";;;" + commandTarget + ";" + command

                    #def checkRule():
                    #    v1 = getCommandParam("status")
                    #    if v1 is None:
                    #        return conditionsNotMet()
                    #    cmdString = "{\"command\": \"" + str(v1) + "\", \"value\": \"" + str(getCommandParam('value')) + "\"}"
                    #    return conditionsMetWithCommand("", None, config.deviceEui, cmdString)

                    """;
            script = script + config.script;
            logger.info("\n" + script);
            PythonInterpreter interpreter = null;
            PyObject pResult = null;
            try {
                interpreter = new PythonInterpreter();
                interpreter.set("config_obj", config);
                interpreter.set("device_obj", device);
                interpreter.set("commandMap", commandMap);
                // Execute the Jython script
                interpreter.exec(script);
                // Call the Python function and get the result
                pResult = interpreter.eval("process_java_object(config_obj,device_obj,commandMap)");

                logger.info("pResult: " + pResult.toString());
                logger.info("pResult type: " + pResult.getType());
                // logger.info("pResult asInt: " + pResult.asInt());
                // result.violated = pResult.asInt() > 0;
                String scriptResult = pResult.toString();
                String[] scriptResultArr = scriptResult.split(";");
                result.violated = scriptResultArr[1].length() > 0 && scriptResultArr[2].length() > 0;
                if(scriptResultArr.length < 3) {
                    logger.warn("Invalid script result: " + scriptResult);
                    result.error = true;
                    result.errorMessage = "Invalid script result: " + scriptResult;
                    return result;
                }
                if (result.violated) {
                    logger.info("Script result: " + scriptResult);
                    
                    result.eui = scriptResultArr[0];
                    result.measurement = scriptResultArr[1];
                    try{
                        result.value = Double.parseDouble(scriptResultArr[2]);
                    }catch (NumberFormatException e) {
                        result.value = null;
                    }
                }
                if (scriptResultArr.length == 5) {
                    result.commandTarget = scriptResultArr[3];
                    result.command = scriptResultArr[4];
                }
            } catch (PyException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
                result.error = true;
                result.errorMessage = e.getMessage();
            } finally {
                if (null != interpreter) {
                    interpreter.close();
                }
            }
            long endTime = System.currentTimeMillis();
            logger.info("Python script execution time: " + (endTime - startTime) + " ms");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            result.error = true;
            result.errorMessage = e.getMessage();
        }
        return result;
    }



}
