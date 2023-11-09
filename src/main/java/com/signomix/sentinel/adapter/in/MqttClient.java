package com.signomix.sentinel.adapter.in;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import com.signomix.sentinel.port.in.DataEventReceiverPort;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class MqttClient {

    @Inject
    Logger logger;

    @Inject
    DataEventReceiverPort dataEventReceiverPort;

    @Incoming("data-received")
    public void receive(byte[] eui) {
        logger.info("Data received: " + eui);
        dataEventReceiverPort.receive(eui);
    }

}
