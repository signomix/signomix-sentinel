package com.signomix.sentinel.domain;

import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.SentinelDaoIface;
import com.signomix.common.iot.sentry.SentinelConfig;

import io.agroal.api.AgroalDataSource;
import io.quarkus.agroal.DataSource;
import jakarta.enterprise.context.ApplicationScoped;
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

    public void onStartup() {
        sentinelDao = new com.signomix.common.tsdb.SentinelDao();
        sentinelDao.setDatasource(tsDs);
    }

    public void handleDataReceivedEvent(String deviceEui) {
        System.out.println("Handling data received event: " + deviceEui);
        List<SentinelConfig> configs;
        // find all sentinel definitions related to the device
        try {
            configs = sentinelDao.getConfigsByDevice(deviceEui, 1000, 0);
        } catch (IotDatabaseException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            // TODO: inform user/admin about error
            return;
        }
        // check alert conditions
        for (SentinelConfig config : configs) {
            runSentinelCheck(config, deviceEui);
        }
    }

    private void runSentinelCheck(SentinelConfig config, String deviceEui) {
        // TODO Auto-generated method stub
        System.out.println("Checking sentinel: " + config.name);
        // create and save alert object
        Alert alert = new Alert(null, 0, deviceEui, "");
    }

}
