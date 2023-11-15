package com.signomix.sentinel.domain;

import org.jboss.logging.Logger;

import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.db.SentinelDaoIface;
import com.signomix.common.iot.Device;

import io.agroal.api.AgroalDataSource;
import io.quarkus.agroal.DataSource;
import jakarta.enterprise.context.ApplicationScoped;
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

    SentinelDaoIface sentinelDao;
    IotDatabaseIface olapDao;

    public void onStartup() {
        sentinelDao = new com.signomix.common.tsdb.SentinelDao();
        sentinelDao.setDatasource(tsDs);
        olapDao = new com.signomix.common.tsdb.IotDatabaseDao();
        olapDao.setDatasource(olapDs);
    }

    public void handleDeviceCreatedEvent(String deviceEui) {
        System.out.println("Handling event: " + deviceEui);
        // TODO
        Device device=null;
        try {
            device = olapDao.getDevice(deviceEui, false);
        } catch (IotDatabaseException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return;
        }
        if(device==null){
            logger.error("Device not found: "+deviceEui);
            return;
        }
        //TODO: update sentinel devices
        // find sentinels related to the device
        // add device to sentinel
    }

    public void handleDeviceRemovedEvent(String deviceEui) {
        System.out.println("Handling event: " + deviceEui);
        // TODO: update sentinel devices
        // find sentinels related to the device
        // remove device from sentinel
    }

    public void handleDeviceUpdatedEvent(String deviceEui) {
        System.out.println("Handling event: " + deviceEui);
        // TODO
        // find sentinels related to the device
        // update device in sentinel
    }

}
