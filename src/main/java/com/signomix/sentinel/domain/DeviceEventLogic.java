package com.signomix.sentinel.domain;

import java.util.List;

import org.jboss.logging.Logger;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import com.signomix.common.User;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.db.ReportResult;
import com.signomix.common.db.SentinelDaoIface;
import com.signomix.common.db.UserDaoIface;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.sentinel.SentinelConfig;

import io.agroal.api.AgroalDataSource;
import io.quarkus.agroal.DataSource;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
public class DeviceEventLogic {

    @Inject
    Logger logger;

    @Inject
    @DataSource("oltp")
    AgroalDataSource tsDs;

    @Inject
    @DataSource("olap")
    AgroalDataSource olapDs;

    @Inject
    @DataSource("user")
    AgroalDataSource userDs;

    SentinelDaoIface sentinelDao;
    IotDatabaseIface olapDao;
    UserDaoIface userDao;

    @Inject
    SentinelLogic sentinelLogic;

    void onStart(@Observes StartupEvent ev) {
        sentinelDao = new com.signomix.common.tsdb.SentinelDao();
        sentinelDao.setDatasource(tsDs);
        olapDao = new com.signomix.common.tsdb.IotDatabaseDao();
        olapDao.setDatasource(olapDs);
        userDao = new com.signomix.common.tsdb.UserDao();
        userDao.setDatasource(userDs);
    }

    public void handleDeviceCreatedEvent(String deviceEui) {
        System.out.println("Handling create event: " + deviceEui);
        // TODO
        Device device = null;
        try {
            device = olapDao.getDevice(deviceEui, false);
        } catch (IotDatabaseException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return;
        }
        if (device == null) {
            logger.error("Device not found: " + deviceEui);
            return;
        }
        User user;
        try {
            user = userDao.getUser(device.getUserID());
        } catch (IotDatabaseException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return;
        }
        if (user == null) {
            logger.error("User not found: " + device.getUserID());
            return;
        }
        List<SentinelConfig> configs = sentinelLogic.getSentinelConfigs(user, 100000, 0);
        for (SentinelConfig config : configs) {
            sentinelLogic.updateSentinelConfigDevices(user, config);
        }
        // TODO: update sentinel devices
        // find sentinels related to the device
        // add device to sentinel
    }

    public void handleDeviceRemovedEvent(String deviceEui) {
        System.out.println("Handling remove event: " + deviceEui);
        try {
            sentinelDao.removeDevice(deviceEui);
        } catch (IotDatabaseException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    public void handleDeviceUpdatedEvent(String deviceEui) {
        System.out.println("Handling update event: " + deviceEui);
        handleDeviceCreatedEvent(deviceEui);
    }

    public void handleDeviceControlEvent(String groupEui) {
        System.out.println("Handling control event: " + groupEui);
        try {
            //List<List<List>> result = olapDao.getGroupLastValues(groupEui, 0, groupEui, null, 0);
            ReportResult reportResult;
            testPythonInterpreter(groupEui);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void testPythonInterpreter(String groupEui) {
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
            interpreter.set("java_obj", "Hello "+groupEui);
            // Execute the Jython script
            interpreter.exec(script);

            // Call the Python function and get the result
            PyObject result = interpreter.eval("process_java_object(java_obj)");
            logger.info("Result: " + result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
