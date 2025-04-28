package com.signomix.sentinel.domain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;
import org.jboss.logmanager.handlers.SyslogHandler.SyslogType;
import org.python.core.PyException;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

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
            //runPythonScript(config, device, jsonString);
        }

    }

    private ConditionResult runPythonScript(SentinelConfig config, Device device, String jsonString) {
        ConditionResult result = new ConditionResult();
        long startTime = System.currentTimeMillis();
        try {
            logger.info("Running Python script for sentinel: " + config.id);
            String script = """

                    def process_java_object(config_obj, device_obj, commandString):
                        global config
                        config = config_obj
                        global device
                        device = device_obj
                        result = ""
                        result = checkRule()
                        return result
                    
                    def conditionsNotMet():
                        return ";;"

                    def conditionsMetWithCommand(measurement, value, commandTarget, command):
                        return config.deviceEui + ";" + measurement + ";" + str(value) + ";" + commandTarget + ";" + command

                    #def checkRule():
                    #    v1 = getValue("temperature")
                    #    v2 = getValue("humidity")
                    #    if v1 is None or v2 is None:
                    #        return conditionsNotMet()
                    #    if v2 - v1 > 10:
                    #        result = conditionsMet("temperature", v1)
                    #    return conditionsMetWithCommand("", null, "myDeviceEui", "{\"command\": \"myCommand\"}")
                    #    #return conditionsNotMet()

                    """;
            script = script + config.script;
            logger.info("\n" + script);
            PythonInterpreter interpreter = null;
            PyObject pResult = null;
            try {
                interpreter = new PythonInterpreter();
                interpreter.set("config_obj", config);
                interpreter.set("device_obj", device);
                interpreter.set("commandString", jsonString);
                // Execute the Jython script
                interpreter.exec(script);
                // Call the Python function and get the result
                pResult = interpreter.eval("process_java_object(config_obj,device_obj,commandString)");

                logger.info("pResult: " + pResult.toString());
                logger.info("pResult type: " + pResult.getType());
                // logger.info("pResult asInt: " + pResult.asInt());
                // result.violated = pResult.asInt() > 0;
                String scriptResult = pResult.toString();
                result.violated = scriptResult.length() > 0;
                if (result.violated) {
                    logger.info("Script result: " + scriptResult);
                    String[] scriptResultArr = scriptResult.split(";");
                    result.eui = scriptResultArr[0];
                    result.measurement = scriptResultArr[1];
                    result.value = Double.parseDouble(scriptResultArr[2]);
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
