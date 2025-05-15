package com.signomix.sentinel.port.in;

import com.signomix.sentinel.domain.DataEventLogic;
import com.signomix.sentinel.domain.EventLogic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class DataEventReceivedPort {

    @Inject
    DataEventLogic dataEventLogic;

    public void receive(byte[] eui, String messageId) {
        String deviceEui=new String(eui);
        dataEventLogic.handleEvent(EventLogic.EVENT_TYPE_DATA, deviceEui,null, messageId);
    }
    
}
