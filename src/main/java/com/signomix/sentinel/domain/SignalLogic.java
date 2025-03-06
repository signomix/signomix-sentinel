package com.signomix.sentinel.domain;

import java.util.ArrayList;
import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.User;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.db.SignalDaoIface;
import com.signomix.common.iot.sentinel.Signal;

import io.agroal.api.AgroalDataSource;
import io.quarkus.agroal.DataSource;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
public class SignalLogic {

    @Inject
    Logger logger;

    @Inject
    @DataSource("oltp")
    AgroalDataSource tsDs;

    @Inject
    @DataSource("olap")
    AgroalDataSource olapDs;

    SignalDaoIface signalDao;
    IotDatabaseIface olapDao;
    IotDatabaseIface oltpDao;

    void onStart(@Observes StartupEvent ev) {
        signalDao = new com.signomix.common.tsdb.SignalDao();
        signalDao.setDatasource(tsDs);
        olapDao = new com.signomix.common.tsdb.IotDatabaseDao();
        olapDao.setDatasource(olapDs);
        oltpDao = new com.signomix.common.tsdb.IotDatabaseDao();
        oltpDao.setDatasource(tsDs);
    }

    public List<Signal> getSignals(User user, int limit, int offset){
        List<Signal> signals = new ArrayList<>();
        try {
            signals = signalDao.getUserSignals(user.uid, limit, offset);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return signals;
    }

    public Signal getSignal(User user, long id){
        Signal signal = null;
        try {
            signal = signalDao.getSignalById(id);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return signal;
    }

    public void deleteSignal(User user, long id){
        try {
            Signal signal = signalDao.getSignalById(id);
            if(signal!=null && signal.userId.equals(user.uid)){
                signalDao.deleteSignal(id);
            }else{
                logger.error("Signal not found or not owned by user: "+id+" "+user.uid);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteSignals(User user){
        try {
            signalDao.deleteSignals(user.uid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
