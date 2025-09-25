package com.signomix.sentinel.domain;

import java.util.ArrayList;
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

import org.python.core.PyException;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.LastDataPair;
import com.signomix.common.iot.sentinel.AlarmCondition;
import com.signomix.common.iot.sentinel.SentinelConfig;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class DataEventLogic extends EventLogic {

    @Override
    void checkSentinelRelatedData(String messageId, SentinelConfig config, Map deviceChannelMap, String eui) {
        List<List<LastDataPair>> values;
        logger.info("Checking sentinel related data for sentinel: " + config.id);
        ConditionResult result = new ConditionResult();
        Device device = null;
        try {
            values = sentinelDao.getLastValuesOfDevices(deviceChannelMap.keySet(), config.timeShift * 60);
            logger.info(config.id + " number of values: " + values.size());
            boolean configConditionsMet = false; // true if at least one device meets the conditions
            for (List deviceParamsAndValues : values) {
                String deviceEui = ((LastDataPair) deviceParamsAndValues.get(0)).eui;
                try {
                    device = olapDao.getDevice(deviceEui, false);
                } catch (IotDatabaseException e) {
                    logger.error("Error while getting device: " + deviceEui);
                    continue;
                }
                if (device.getEUI().equalsIgnoreCase(eui)) {
                    result = runConfigQuery(messageId, config, device, deviceChannelMap, values);
                    break;
                }
                /*
                 * if (result.error) {
                 * logger.error("Error while running config query for sentinel: " + config.id);
                 * saveSignal(-1, config.id, config.organizationId, config.userId, eui,
                 * "rule error", result.errorMessage, System.currentTimeMillis());
                 * sendAlert("ALERT", config.userId, eui, "rule error", result.errorMessage,
                 * System.currentTimeMillis());
                 * return;
                 * }
                 * configConditionsMet = configConditionsMet || result.violated;
                 */
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
    private ConditionResult runConfigQuery(String messageId, SentinelConfig config, Device device, Map deviceChannelMap,
            List<List<LastDataPair>> values) {

        ConditionResult result = new ConditionResult();
        result.violated = false;
        result.value = null;
        result.measurement = "";
        result.configId = config.id;

        if (config.useScript) {
            if (config.script != null && !config.script.isEmpty()) {
                logger.info("Running script : " + config.script);
                // return runPythonScript(config, device, deviceChannelMap, values);
                vertx.<ConditionResult>executeBlocking(promise -> {
                    try {
                        // Perform blocking operation (e.g., Jython)
                        ConditionResult result2 = runPythonScript(config, device, deviceChannelMap, values);
                        result2.configId = config.id;
                        result2.eui = device.getEUI();
                        promise.complete(result2);
                    } catch (Exception e) {
                        promise.fail(e);
                    }
                }, res -> {
                    if (res.succeeded()) {
                        processResult(messageId, res.result());
                    } else {
                        logger.error("Error executing Python script (2)", res.cause());
                    }
                });
            } else {
                logger.warn("Script is empty");
            }
        } else {
            logger.info("Checking conditions");
            vertx.<ConditionResult>executeBlocking(promise -> {
                try {
                    // Perform blocking operation (e.g., Jython)
                    ConditionResult result2 = checkConditions(config, device, deviceChannelMap, values);
                    result2.configId = config.id;
                    result2.eui = device.getEUI();
                    promise.complete(result2);
                } catch (Exception e) {
                    promise.fail(e);
                }
            }, res -> {
                if (res.succeeded()) {
                    processResult(messageId, res.result());
                } else {
                    logger.error("Error executing Python script (2)", res.cause());
                }
            });
        }
        return result;
    }

    ConditionResult checkConditions(SentinelConfig config, Device device,
            Map<String, Map<String, String>> deviceChannelMap, List<List<LastDataPair>> values) {
        ConditionResult result = new ConditionResult();
        result.violated = false;
        result.value = null;
        result.measurement = "";
        result.configId = config.id;
        try {
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
                    condition.orOperator = (Boolean) conditionMap.get("orOperator"); // deprecated
                    condition.logic = (Integer) conditionMap.get("logic");
                    condition.conditionOperator = (Integer) conditionMap.get("conditionOperator");
                    logger.info("condition:  " + condition.conditionOperator + " " + condition.measurement + ", "
                            + condition.condition1 + " " + condition.value1 + " " + condition.orOperator + " "
                            + condition.condition2 + " " + condition.value2);
                    result.measurement = condition.measurement;
                    // as values holds all measurement values for all devices, we need to get only
                    // values for the selected measurement (condition.measurement) from all devices
                    ArrayList<LastDataPair> valuesList = new ArrayList<>();

                    int measurementIndex;
                    LastDataPair dataToCheck;
                    for (int j = 0; j < values.size(); j++) {
                        // get column index for measurement
                        Map<String, String> measurementMap = (Map) deviceChannelMap.get(device.getEUI());
                        String columnNumberStr = measurementMap.get(condition.measurement);
                        if (columnNumberStr == null || columnNumberStr.isEmpty()) {
                            logger.info("columnNumberStr is null or empty for measurement: " + condition.measurement);
                            continue;
                        }
                        measurementIndex = Integer.parseInt(columnNumberStr.substring(1));
                        measurementIndex--; // column numbers start from 1 (name d1), but list indexes start from 0
                        dataToCheck = (LastDataPair) values.get(j).get(measurementIndex);
                        valuesList.add(dataToCheck);
                    }
                    if (valuesList.size() == 0) {
                        logger.info(i + " values for " + condition.measurement + " not found");
                        continue;
                    }
                    // Double tmpValue
                    Double hysteresis = Math.abs(config.hysteresis);
                    Double valueToCheck = null;
                    Double diff;
                    if (condition.condition1 == AlarmCondition.CONDITION_GREATER) {
                        for (int j = 0; j < valuesList.size(); j++) {
                            valueToCheck = valuesList.get(j).value;
                            diff = valuesList.get(j).delta;
                            logger.info("VALUE: " + valueToCheck);
                            if (diff > 0) {
                                actualConditionMet = actualConditionMet
                                        || (valueToCheck.compareTo(condition.value1) > 0);
                            } else {
                                actualConditionMet = actualConditionMet
                                        || (valueToCheck.compareTo(condition.value1 - hysteresis) > 0);
                            }
                        }
                        // ok = value.compareTo(condition.value1) > 0;
                    } else if (condition.condition1 == AlarmCondition.CONDITION_LESS) {
                        for (int j = 0; j < valuesList.size(); j++) {
                            valueToCheck = valuesList.get(j).value;
                            diff = valuesList.get(j).delta;
                            logger.info("VALUE: " + valueToCheck);
                            if (diff < 0) {
                                actualConditionMet = actualConditionMet
                                        || (valueToCheck.compareTo(condition.value1) < 0);
                            } else {
                                actualConditionMet = actualConditionMet
                                        || (valueToCheck.compareTo(condition.value1 + hysteresis) < 0);
                            }
                        }
                        // ok = value.compareTo(condition.value1) == 0;
                    } else if (condition.condition1 == AlarmCondition.CONDITION_EQUAL) {
                        logger.info("Checking condition: ==");
                        for (int j = 0; j < valuesList.size(); j++) {
                            valueToCheck = valuesList.get(j).value;
                            diff = valuesList.get(j).delta;
                            logger.info("VALUE: " + valueToCheck + " diff: " + diff + " value1: " + condition.value1);
                            if (diff != 0) {
                                actualConditionMet = actualConditionMet
                                        || (valueToCheck.compareTo(condition.value1) == 0);
                            }
                        }
                    } else if (condition.condition1 == AlarmCondition.CONDITION_NOT_EQUAL) {
                        logger.info("Checking condition: !=");
                        for (int j = 0; j < valuesList.size(); j++) {
                            valueToCheck = valuesList.get(j).value;
                            diff = valuesList.get(j).delta;
                            logger.info("VALUE: " + valueToCheck + " diff: " + diff + " value1: " + condition.value1);
                            if (diff != 0) {
                                actualConditionMet = actualConditionMet
                                        || (valueToCheck.compareTo(condition.value1) != 0);
                            }
                        }
                    }
                    if (condition.logic != null && condition.logic > 0 && condition.value2 != null) {
                        if (condition.condition2 == AlarmCondition.CONDITION_GREATER) {
                            for (int j = 0; j < valuesList.size(); j++) {
                                valueToCheck = valuesList.get(j).value;
                                diff = valuesList.get(j).delta;
                                logger.info("VALUE: " + valueToCheck);
                                if (diff > 0) {
                                    if (condition.logic == 1) {
                                        actualConditionMet = actualConditionMet
                                                || (valueToCheck.compareTo(condition.value2) > 0);
                                    } else {
                                        actualConditionMet = actualConditionMet
                                                && (valueToCheck.compareTo(condition.value2) > 0);
                                    }
                                } else {
                                    if (condition.logic == 1) {
                                        actualConditionMet = actualConditionMet
                                                || (valueToCheck.compareTo(condition.value2 - hysteresis) > 0);
                                    } else {
                                        actualConditionMet = actualConditionMet
                                                && (valueToCheck.compareTo(condition.value2 - hysteresis) > 0);
                                    }
                                }
                            }
                            // ok = ok || value.compareTo(condition.value2) > 0;
                        } else if (condition.condition2 == AlarmCondition.CONDITION_LESS) {
                            for (int j = 0; j < valuesList.size(); j++) {
                                valueToCheck = valuesList.get(j).value;
                                diff = valuesList.get(j).delta;
                                logger.info("VALUE: " + valueToCheck);
                                if (diff < 0) {
                                    if (condition.logic == 1) {
                                        actualConditionMet = actualConditionMet
                                                || (valueToCheck.compareTo(condition.value2) < 0);
                                    } else {
                                        actualConditionMet = actualConditionMet
                                                && (valueToCheck.compareTo(condition.value2) < 0);
                                    }
                                } else {
                                    if (condition.logic == 1) {
                                        actualConditionMet = actualConditionMet
                                                || (valueToCheck.compareTo(condition.value2 + hysteresis) < 0);
                                    } else {
                                        actualConditionMet = actualConditionMet
                                                && (valueToCheck.compareTo(condition.value2 + hysteresis) < 0);
                                    }
                                }
                            }
                            // ok = ok || value.compareTo(condition.value2) < 0;
                        } else if (condition.condition2 == AlarmCondition.CONDITION_EQUAL) {
                            for (int j = 0; j < valuesList.size(); j++) {
                                valueToCheck = valuesList.get(j).value;
                                diff = valuesList.get(j).delta;
                                logger.info("VALUE: " + valueToCheck);
                                if (diff != 0) {
                                    if (condition.logic == 1) {
                                        actualConditionMet = actualConditionMet
                                                || (valueToCheck.compareTo(condition.value2) == 0);
                                    } else {
                                        actualConditionMet = actualConditionMet
                                                && (valueToCheck.compareTo(condition.value2) == 0);
                                    }
                                }
                            }
                        } else if (condition.condition2 == AlarmCondition.CONDITION_NOT_EQUAL) {
                            for (int j = 0; j < valuesList.size(); j++) {
                                valueToCheck = valuesList.get(j).value;
                                diff = valuesList.get(j).delta;
                                logger.info("VALUE: " + valueToCheck);
                                if (diff != 0) {
                                    if (condition.logic == 1) {
                                        actualConditionMet = actualConditionMet
                                                || (valueToCheck.compareTo(condition.value2) != 0);
                                    } else {
                                        actualConditionMet = actualConditionMet
                                                && (valueToCheck.compareTo(condition.value2) != 0);
                                    }
                                }
                            }
                        }
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
                    if (conditionsMet) {
                        result.measurement = condition.measurement;
                        result.value = valueToCheck;
                    }
                }
            }
            result.violated = conditionsMet;
            return result;
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            result.violated = false;
            result.error = true;
            result.errorMessage = e.getMessage();
            return result;
        }
    }

    @Override
    ConditionResult runPythonScript(SentinelConfig config, Device device, String jsonString) {
        return null;
    }

    @Override
    ConditionResult runPythonScript(SentinelConfig config, Device device, Map deviceChannelMap,
            List<List<LastDataPair>> values) {
        ConditionResult result = new ConditionResult();
        long startTime = System.currentTimeMillis();
        try {
            logger.info("Running Python script for sentinel: " + config.id);
            logger.info("deviceChannelMap size: " + deviceChannelMap.size());
            deviceChannelMap.forEach((k, v) -> {
                logger.info("key: " + k + " value: " + v);
            });
            logger.info("values size: " + values.size());
            logger.info("values: " + values.toString());
            String script = """
                    def get_measurementIndex(eui, measurement, deviceChannelMap):
                        measurementIndex = -1
                        index = -1
                        javaLogger.info("get_measurementIndex: " + eui + " " + measurement)
                        deviceChannels = deviceChannelMap.get(eui)
                        if deviceChannels is not None:
                            javaLogger.info("deviceChannels: " + str(deviceChannels))
                            for key, value in deviceChannels.items():
                                index += 1
                                if key == measurement:
                                    measurementIndex = index  #int(value[1:]) -1 # column numbers start from 1 (name d1), but list indexes start from 0
                                    break
                            # print measurement index for debugging
                            print("Measurement index: " + str(measurementIndex))
                        return measurementIndex

                    def get_value(eui, measurement, values, deviceChannelMap):
                        measurementIndex = get_measurementIndex(eui, measurement, deviceChannelMap)
                        if measurementIndex < 0:
                            return None
                        for i in range(len(values)):
                            if values[i][0].eui == eui:
                                return values[i][measurementIndex].value
                        return None

                    def get_delta(eui, measurement, values, deviceChannelMap):
                        measurementIndex = get_measurementIndex(eui, measurement, deviceChannelMap)
                        if measurementIndex < 0:
                            return None
                        for i in range(len(values)):
                            if values[i][0].eui == eui:
                                return values[i][measurementIndex].delta
                        return None

                    def getValue(measurement):
                        measurementIndex = get_measurementIndex(device.EUI, measurement, channelMap)
                        javaLogger.info("Measurement index ("+measurement+"): " + str(measurementIndex))
                        if measurementIndex < 0:
                            return None
                        for i in range(len(valuesArr)):
                            if valuesArr[i][0].eui == device.EUI:
                                return valuesArr[i][measurementIndex].value
                        return None

                    def process_java_object(config_obj, device_obj, values, deviceChannelMap):
                        global config
                        config = config_obj
                        global device
                        device = device_obj
                        global valuesArr
                        valuesArr = values
                        global channelMap
                        channelMap = deviceChannelMap
                        result = ""
                        javaLogger.info("Running Python script for sentinel: " + str(config.id))
                        javaLogger.info("valuesArr size: " + str(len(valuesArr)))
                        result = checkRule()
                        return result

                    def conditionsMetWithCommand(measurement, value, commandTarget, command):
                        return device.EUI + ";" + measurement + ";" + str(value) + ";" + commandTarget + ";" + command

                    def conditionsMet(measurement, value):
                        javaLogger.info("Conditions met for measurement: " + measurement + " value: " + str(value))
                        return device.EUI + ";" + measurement + ";" + str(value)

                    def conditionsNotMet():
                        javaLogger.info("Conditions not met")
                        return ""

                    #def checkRule():
                    #    v1 = getValue("temperature")
                    #    v2 = getValue("humidity")
                    #    if v1 is None or v2 is None:
                    #        return conditionsNotMet()
                    #    if v2 - v1 > 10:
                    #        result = conditionsMet("temperature", v1)
                    #    return conditionsNotMet()

                    """;
            script = script + config.script;
            logger.info("\n" + script);
            PythonInterpreter interpreter = null;
            PyObject pResult = null;
            try {
                interpreter = new PythonInterpreter();
                interpreter.set("config_obj", config);
                interpreter.set("device_obj", device);
                interpreter.set("values", values);
                interpreter.set("deviceChannelMap", deviceChannelMap);
                interpreter.set("javaLogger", logger);
                // Execute the Jython script
                interpreter.exec(script);
                // Call the Python function and get the result
                pResult = interpreter.eval("process_java_object(config_obj,device_obj,values,deviceChannelMap)");

                logger.info("pResult: " + pResult.toString());
                logger.info("pResult type: " + pResult.getType());
                // logger.info("pResult asInt: " + pResult.asInt());
                // result.violated = pResult.asInt() > 0;
                String scriptResult = pResult.toString();
                result.violated = scriptResult.length() > 0;

                logger.info("Script result: " + scriptResult);
                String[] scriptResultArr = scriptResult.split(";", -1);
                if (scriptResultArr.length < 2) {
                    result.error = false;
                    result.errorMessage = "";
                    return result;
                } else if (scriptResultArr.length < 3) {
                    logger.error("Script result is not valid: " + scriptResult);
                    result.error = true;
                    result.errorMessage = "Script result is not valid: " + scriptResult;
                    return result;
                } else {
                    result.eui = scriptResultArr[0].trim();
                    result.violated = result.eui.length() > 0;
                    result.measurement = scriptResultArr[1].trim();
                    try {
                        result.value = Double.parseDouble(scriptResultArr[2]);
                    } catch (Exception e) {
                        logger.debug("Error parsing value: " + scriptResultArr[2]);
                        result.value = null;
                    }
                }
                if (scriptResultArr.length == 5) {
                    result.commandTarget = scriptResultArr[3].trim();
                    result.command = scriptResultArr[4].trim();
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

    private void testPythonInterpreter(String deviceEui) {
        try {
            String script = """
                    def process_java_object(java_obj):
                        # Access the Java object's methods
                        #message = java_obj.getMessage()
                        # Perform some processing (e.g., convert to uppercase)
                        #result = message.upper()
                        result = java_obj.upper()
                        return result
                    """;
            PythonInterpreter interpreter = new PythonInterpreter();
            interpreter.set("java_obj", "Hello from Java!");
            // Execute the Jython script
            interpreter.exec(script);

            // Call the Python function and get the result
            PyObject result = interpreter.eval("process_java_object(java_obj)");
            logger.info("Result: " + result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void testJsInterpreter(String deviceEui) {

        try {
            List<ScriptEngineFactory> engines = new ScriptEngineManager().getEngineFactories();
            logger.info("Available engines: ");
            for (ScriptEngineFactory f : engines) {
                logger.info(f.getLanguageName() + " " + f.getEngineName() + " " + f.getNames());
            }

            String script = "let x=0; let a = 1; let b = 2; let result = a + b + x; result;";
            Integer result = 0;

            ScriptEngine engine = new ScriptEngineManager().getEngineByName("graal.js");
            // ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
            Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
            bindings.put("polyglot.js.allowHostAccess", true);
            bindings.put("polyglot.js.allowHostClassLookup", (Predicate<String>) s -> true);
            engine.put("result", result);
            engine.put("x", 10);

            try {
                logger.info("Script result: " + engine.eval(script));
                logger.info("Result: " + engine.get("result"));
                // logger.info("Result: " + result);
            } catch (ScriptException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
