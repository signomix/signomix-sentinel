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
import com.signomix.common.iot.sentinel.SentinelConfig;

import io.agroal.api.AgroalDataSource;
import io.quarkus.agroal.DataSource;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

public class EventLogic {

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
     * @param eui the EUI of the device that sent the data
     * @param commandString the command string received from the device
     * @param messageId the message ID of the received data
     */
    public void handleEvent(int type, String eui, String commandString, String messageId) {
            //logger.info("Handling data received event: " + eui);
        // testJsInterpreter(deviceEui);
        // testPythonInterpreter(deviceEui);
        String deviceEui = null;
        String command = commandString;
        String jsonString = null;
        if (command!=null && (command.startsWith("&") || command.startsWith("#"))) {
            command = command.substring(1);
        }
        String[] commandParts = {};
        if(command!=null){
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
                List<SentinelConfig> tagConfigs = sentinelDao.getConfigsByTag(tag, tagValue, 1000, 0, type);
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
                List<SentinelConfig> groupConfigs = sentinelDao.getConfigsByGroup(groups[i].trim(), 1000, 0, type);
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
            runSentinelCheck(messageId, config, device, jsonString);
        }
    }

    void runSentinelCheck(String messageId, SentinelConfig config, Device device, String commandString) {
    }

}
