package com.signomix.sentinel.domain;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.python.core.PyException;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.LastDataPair;
import com.signomix.common.iot.sentinel.SentinelConfig;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CommandEventLogic extends EventLogic {

    @Override
    void checkSentinelRelatedData(String messageId, SentinelConfig config, Map deviceChannelMap, String eui, String[] messageArray) {
    }

    @Override
    ConditionResult runPythonScript(SentinelConfig config, Device device, String jsonString) {
        ConditionResult result = new ConditionResult();
        long startTime = System.currentTimeMillis();
        HashMap<String, Object> commandMap;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            logger.info("Running Python script for sentinel: " + config.id);
            String jString = jsonString.trim();
            if (jString.startsWith("&") || jString.startsWith("#")) {
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
                        return ""

                    def conditionsMetWithCommand(measurement, value, commandTarget, command):
                        if value is None:
                            return device.EUI + ";" + measurement + ";;" + commandTarget + ";" + command
                        return device.EUI + ";" + measurement + ";" + str(value) + ";" + commandTarget + ";" + command

                    def newCommand(commandTarget, command):
                        if commandTarget is None or command is None:
                            return conditionsNotMet()
                        return ";;;" + commandTarget + ";" + command

                    # see def checkRule() below for example
                    """;
            /*
            def checkRule():
                v1 = getCommandParam("status")
                if v1 is None:
                    return conditionsNotMet()
                cmdString = "{\"command\": \"" + str(v1) + "\", \"value\": \"" + str(getCommandParam('value')) + "\"}"
                return conditionsMetWithCommand("", None, device.EUI, cmdString)
             */
                    
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
                String[] scriptResultArr = scriptResult.split(";", -1);

                if (scriptResultArr.length < 2) {
                    result.error = false;
                    result.errorMessage = "";
                    return result;
                } else if (scriptResultArr.length < 3) {
                    logger.warn("Invalid script result: " + scriptResult);
                    result.error = true;
                    result.errorMessage = "Invalid script result: " + scriptResult;
                    return result;
                }
                result.violated = scriptResultArr[1].length() > 0 && scriptResultArr[2].length() > 0;
                if (result.violated) {
                    logger.info("Script result: " + scriptResult);

                    result.eui = scriptResultArr[0];
                    result.measurement = scriptResultArr[1];
                    try {
                        result.value = Double.parseDouble(scriptResultArr[2]);
                    } catch (NumberFormatException e) {
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
                if (null != pResult) {
                    pResult = null;
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

/*     @Override
    ConditionResult runPythonScript(SentinelConfig config, Device device, Map deviceChannelMap,
            List<List<LastDataPair>> values) {
        return null;
    } */

    @Override
    ConditionResult runPythonScript(SentinelConfig config, String[] messageArray, int deviceRuleStatus) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'runPythonScript'");
    }

}
