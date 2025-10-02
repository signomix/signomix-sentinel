package com.signomix.sentinel.domain;

import java.util.ArrayList;
import java.util.HashMap;
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

import com.signomix.common.iot.Device;
import com.signomix.common.iot.LastDataPair;
import com.signomix.common.iot.sentinel.AlarmCondition;
import com.signomix.common.iot.sentinel.SentinelConfig;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class DataEventLogic extends EventLogic {

    @Override
    void checkSentinelRelatedData(String messageId, SentinelConfig config, Map deviceChannelMap, String eui,
            String[] messageArray) {
        runConfigQuery(messageId, config, messageArray);
    }

    /**
     * Runs a query on the given SentinelConfig and values map to check if the
     * conditions are met.
     * @param messageId    the message ID of the data event
     * @param config       the SentinelConfig to use for the query
     * @param messageArray the array of measurement values to use for the query
     * @return
     */
    private ConditionResult runConfigQuery(String messageId, SentinelConfig config, String[] messageArray) {

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
                        ConditionResult result2 = runPythonScript(config, messageArray);
                        result2.configId = config.id;
                        result2.eui = messageArray[0];
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
            vertx.<ConditionResult>executeBlocking(promise -> {
                try {
                    // Perform blocking operation (e.g., Jython)
                    ConditionResult result2 = checkConditions(config, messageArray);
                    result2.configId = config.id;
                    result2.eui = messageArray[0];
                    promise.complete(result2);
                } catch (Exception e) {
                    promise.fail(e);
                }
            }, res -> {
                if (res.succeeded()) {
                    processResult(messageId, res.result());
                } else {
                    logger.error("Error while checking conditions", res.cause());
                }
            });
        }
        return result;
    }

    private LastDataPair buildDataPair(String eui, String measurementStr) {
        LastDataPair pair = new LastDataPair(null, null, null);
        String[] data = measurementStr.split("=");
        if (data.length == 2) {
            pair.eui = eui;
            pair.measurementName = data[0];
            pair.value = Double.valueOf(data[1]);
            pair.delta = null;
        }
        return pair;
    }

    ConditionResult checkConditions(SentinelConfig config, String[] messageArray) {
        ConditionResult result = new ConditionResult();
        result.violated = false;
        result.value = null;
        result.measurement = "";
        result.configId = config.id;
        try {
            // TODO: take into account previous values (required for hysteresis)
            AlarmCondition condition;
            boolean conditionsMet = false;
            List conditions = config.conditions;
            LinkedHashMap<String, Object> conditionMap = new LinkedHashMap();
            logger.info("conditions: " + config.conditions.size());
            if (conditions != null) {
                boolean actualConditionMet;
                Double hysteresis;
                for (int i = 0; i < conditions.size(); i++) {
                    actualConditionMet = false;
                    if (i > 1) {
                        break;
                    }
                    if (messageArray.length < 3) {
                        logger.warn(i + " values for rule " + config.id + " not found");
                        continue;
                    }
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
                    ArrayList<LastDataPair> valuesList = new ArrayList<>();
                    LastDataPair dataToCheck;
                    for (int j = 2; j < messageArray.length; j++) {
                        dataToCheck = buildDataPair(messageArray[0], messageArray[j]);
                        valuesList.add(dataToCheck);
                    }
                    if (valuesList.size() == 0) {
                        logger.info(i + " values for " + condition.measurement + " not found");
                        continue;
                    }
                    Double valueToCheck = getValueToCheck(valuesList, condition.measurement);
                    hysteresis = Math.abs(config.hysteresis);
                    if (condition.condition1 == AlarmCondition.CONDITION_GREATER) {
                        actualConditionMet = actualConditionMet
                                || (valueToCheck.compareTo(condition.value1+hysteresis) > 0);
                    } else if (condition.condition1 == AlarmCondition.CONDITION_LESS) {
                        actualConditionMet = actualConditionMet
                                || (valueToCheck.compareTo(condition.value1-hysteresis) < 0);
                    } else if (condition.condition1 == AlarmCondition.CONDITION_EQUAL) {
                        actualConditionMet = actualConditionMet
                                || (valueToCheck.compareTo(condition.value1) == 0);
                    } else if (condition.condition1 == AlarmCondition.CONDITION_NOT_EQUAL) {
                        actualConditionMet = actualConditionMet
                                || (valueToCheck.compareTo(condition.value1) != 0);
                    }
                    if (condition.logic != null && condition.logic > 0 && condition.value2 != null) {
                        if (condition.condition2 == AlarmCondition.CONDITION_GREATER) {
                            if (condition.logic == 1) {
                                actualConditionMet = actualConditionMet
                                        || (valueToCheck.compareTo(condition.value2+hysteresis) > 0);
                            } else {
                                actualConditionMet = actualConditionMet
                                        && (valueToCheck.compareTo(condition.value2+hysteresis) > 0);
                            }
                        } else if (condition.condition2 == AlarmCondition.CONDITION_LESS) {
                            if (condition.logic == 1) {
                                actualConditionMet = actualConditionMet
                                        || (valueToCheck.compareTo(condition.value2-hysteresis) < 0);
                            } else {
                                actualConditionMet = actualConditionMet
                                        && (valueToCheck.compareTo(condition.value2-hysteresis) < 0);
                            }
                        } else if (condition.condition2 == AlarmCondition.CONDITION_EQUAL) {
                            if (condition.logic == 1) {
                                actualConditionMet = actualConditionMet
                                        || (valueToCheck.compareTo(condition.value2) == 0);
                            } else {
                                actualConditionMet = actualConditionMet
                                        && (valueToCheck.compareTo(condition.value2) == 0);
                            }
                        } else if (condition.condition2 == AlarmCondition.CONDITION_NOT_EQUAL) {
                            if (condition.logic == 1) {
                                actualConditionMet = actualConditionMet
                                        || (valueToCheck.compareTo(condition.value2) != 0);
                            } else {
                                actualConditionMet = actualConditionMet
                                        && (valueToCheck.compareTo(condition.value2) != 0);
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
            logger.error("Error while checking conditions", e);
        }
        return result;
    }

    /**
     * Get value to check from the list of values
     * 
     * @param valuesList
     * @param measurement
     * @return
     */
    Double getValueToCheck(List<LastDataPair> valuesList, String measurement) {
        Predicate<LastDataPair> byMeasurement = p -> p.measurementName.equalsIgnoreCase(measurement);
        LastDataPair pair = valuesList.stream().filter(byMeasurement).findFirst().orElse(null);
        if (pair != null) {
            return pair.value;
        } else {
            return null;
        }
    }

    @Override
    ConditionResult runPythonScript(SentinelConfig config, Device device, String jsonString) {
        return null;
    }

    @Override
    ConditionResult runPythonScript(SentinelConfig config, String[] messageArray) {
        ConditionResult result = new ConditionResult();
        result.eui = messageArray[0];
        long startTime = System.currentTimeMillis();
        try {
            logger.info("Running Python script for sentinel: " + config.id);
            HashMap<String, Double> values = new HashMap<>();
            String[] pair;
            Double value;
            for (int i = 2; i < messageArray.length; i++) {
                pair = messageArray[i].split("=");
                try {
                    value = Double.parseDouble(pair[1]);
                    values.put(pair[0], value);
                } catch (Exception e) {
                    logger.warn("problem parsing value declaration " + messageArray[i]);
                }
            }
            String script = """
                    def getValue(measurement):
                        return valuesMap[measurement]

                    def process_java_object(config_obj, eui, values):
                        global config
                        config = config_obj
                        global valuesMap
                        valuesMap = values
                        global deviceEUI
                        deviceEUI = eui
                        result = ""
                        javaLogger.info("Running Python script for sentinel: " + str(config.id))
                        try:
                            result = checkRule()
                        except Exception as e:
                            result = scriptError("Error in checkRule: "+ str(e))
                        return result

                    def conditionsMetWithCommand(measurement, value, commandTarget, command):
                        return deviceEUI + ";" + measurement + ";" + str(value) + ";" + commandTarget + ";" + command

                    def conditionsMet(measurement, value):
                        javaLogger.info("Conditions met for measurement: " + measurement + " value: " + str(value))
                        return deviceEUI + ";" + measurement + ";" + str(value)

                    def conditionsNotMet():
                        javaLogger.info("Conditions not met")
                        return ""

                    def scriptError(message):
                        javaLogger.warn("Script error: " + message)
                        return message

                    #def checkRule():
                    #    result = conditionsNotMet()
                    #    v1 = getValue("temperature")
                    #    v2 = getValue("humidity")
                    #    if v1 is None or v2 is None:
                    #        return conditionsNotMet()
                    #    if v2 - v1 > 10:
                    #        result = conditionsMet("temperature", v1)
                    #    return result

                    """;
            script = script + config.script;
            logger.info("\n" + script);
            PythonInterpreter interpreter = null;
            PyObject pResult = null;
            try {
                interpreter = new PythonInterpreter();
                interpreter.set("config_obj", config);
                interpreter.set("values", values);
                interpreter.set("eui", messageArray[0]);
                interpreter.set("javaLogger", logger);
                // Execute the Jython script
                interpreter.exec(script);
                // Call the Python function and get the result
                pResult = interpreter.eval("process_java_object(config_obj,eui,values)");

                logger.info("pResult: " + pResult.toString());
                logger.info("pResult type: " + pResult.getType());
                // logger.info("pResult asInt: " + pResult.asInt());
                // result.violated = pResult.asInt() > 0;
                String scriptResult = pResult.toString();
                result.violated = scriptResult.length() > 0;

                logger.info("Script result: " + scriptResult);
                String[] scriptResultArr = scriptResult.split(";", -1);
                if (scriptResultArr.length == 1 && scriptResultArr[0].startsWith("Script error:")) {
                    result.error = true;
                    result.errorMessage = scriptResult;
                    return result;
                }
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
                    result.value = null;
                    try {
                        result.value = Double.parseDouble(scriptResultArr[2]);
                    } catch (Exception e) {
                        logger.warn("Error parsing value: [" + scriptResultArr[2]+"]");
                        result.value = null;
                    }
                }
                if (scriptResultArr.length == 5) {
                    result.commandTarget = scriptResultArr[3].trim();
                    result.command = scriptResultArr[4].trim();
                }

            } catch (PyException e) {
                e.printStackTrace();
                logger.error("E1 " + e.getMessage());
                result.error = true;
                result.errorMessage = e.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("E2" + e.getMessage());
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
