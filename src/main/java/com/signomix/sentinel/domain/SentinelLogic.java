package com.signomix.sentinel.domain;

import java.util.ArrayList;
import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.User;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.db.SentinelDaoIface;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.sentinel.SentinelConfig;

import io.agroal.api.AgroalDataSource;
import io.quarkus.agroal.DataSource;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
public class SentinelLogic {

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
    IotDatabaseIface oltpDao;

    void onStart(@Observes StartupEvent ev) {
        sentinelDao = new com.signomix.common.tsdb.SentinelDao();
        sentinelDao.setDatasource(tsDs);
        olapDao = new com.signomix.common.tsdb.IotDatabaseDao();
        olapDao.setDatasource(olapDs);
        oltpDao = new com.signomix.common.tsdb.IotDatabaseDao();
        oltpDao.setDatasource(tsDs);
    }

    public SentinelConfig getSentinelConfig(User user, long id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public List<SentinelConfig> getSentinelConfigs(User user, int limit, int offset) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void addSentinelConfig(User user, SentinelConfig config) {
        try {
            sentinelDao.addConfig(config);
            List<Device> devices = getSentinelDevices(config, config.userId.toString(), config.organizationId);
            addSentinelDevices(config, devices);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }

    public void updateSentinelConfig(User user, SentinelConfig config) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void removeSentinelConfig(User user, long id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private List<Device> getSentinelDevices(SentinelConfig config, String userId, long organizationId) {
        ArrayList<Device> devices = new ArrayList<>();
        if (config.deviceEui != null) {
            try {
                Device device = oltpDao.getDevice(config.deviceEui, false);
                if (device != null) {
                    devices.add(device);
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }
        } else if (config.groupEui != null) {
            try {
                List<Device> groupDevices = oltpDao.getGroupDevices(false, userId, organizationId, config.groupEui);
                if (groupDevices != null) {
                    devices.addAll(groupDevices);
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }
        } else if (config.tagName != null && !config.tagName.isEmpty() && config.tagValue != null
                && !config.tagValue.isEmpty()) {
            try {
                List<Device> tagDevices = oltpDao.getDevicesByTag(false, userId, organizationId, config.tagName,
                        config.tagValue);
                if (tagDevices != null) {
                    devices.addAll(tagDevices);
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }
        }
        return devices;
    }

    private void addSentinelDevices(SentinelConfig config, List<Device> devices) {
        String channelMapping;
        for (Device device : devices) {
            try {
                channelMapping = "";
                String[] channels = device.getChannelsAsString().split(",");
                for (int i = 0; i < channels.length; i++) {
                    if (channels[i].isEmpty())
                        continue;
                    channelMapping += channels[i] + ":d" + (i + 1) + ";";
                }
                sentinelDao.addDevice(config.id, device.getEUI(), channelMapping);
            } catch (Exception e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }
        }
    }
}
