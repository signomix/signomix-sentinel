package com.signomix.sentinel.port.in;

import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.User;
import com.signomix.common.iot.sentinel.Signal;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class SignalPort {
    
    @Inject
    Logger logger;

    @Inject
    AuthPort authPort;

    public List<Signal> getSignals(User user, int limit, int offset) {
        throw new UnsupportedOperationException("Unimplemented method 'getSignals'");
    }

    public Signal getSignal(User user, long id) {
        throw new UnsupportedOperationException("Unimplemented method 'getSignal'");
    }
}