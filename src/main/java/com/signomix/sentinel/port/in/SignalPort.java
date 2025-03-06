package com.signomix.sentinel.port.in;

import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.User;
import com.signomix.common.iot.sentinel.Signal;
import com.signomix.sentinel.domain.SignalLogic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class SignalPort {
    
    @Inject
    Logger logger;

    @Inject
    AuthPort authPort;

    @Inject
    SignalLogic signalLogic;

    public List<Signal> getSignals(User user, int limit, int offset) {
        return signalLogic.getSignals(user, limit, offset);
    }

    public Signal getSignal(User user, long id) {
        return signalLogic.getSignal(user, id);
    }

    public void deleteSignal(User user, long id) {
        signalLogic.deleteSignal(user, id);
    }

    public void deleteSignals(User user) {
        signalLogic.deleteSignals(user);
    }
}