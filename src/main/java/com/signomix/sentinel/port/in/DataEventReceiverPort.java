package com.signomix.sentinel.port.in;

import com.signomix.sentinel.domain.DataEventLogic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class DataEventReceiverPort {

    @Inject
    DataEventLogic dataEventLogic;

    public void receive(byte[] eui) {
        String deviceEui=new String(eui);
        System.out.println("Data received: " + deviceEui);
        dataEventLogic.handleDataReceivedEvent(deviceEui);
    }
    
}
