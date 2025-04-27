package com.signomix.sentinel.port.in;

import com.signomix.sentinel.domain.DataEventLogic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class DataEventReceivedPort {

    @Inject
    DataEventLogic dataEventLogic;

    public void receive(byte[] eui) {
        String deviceEui=new String(eui);
        dataEventLogic.handleDataReceivedEvent(deviceEui);
    }
    
}
