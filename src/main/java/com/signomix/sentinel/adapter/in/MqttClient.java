package com.signomix.sentinel.adapter.in;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import com.signomix.sentinel.port.in.CommandEventReceivedPort;
import com.signomix.sentinel.port.in.DataEventReceivedPort;
import com.signomix.sentinel.port.in.DeviceEventPort;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class MqttClient {

    @Inject
    Logger logger;

    @Inject
    CommandEventReceivedPort commandEventReceivedPort;

    @Inject
    DataEventReceivedPort dataEventReceivedPort;

    @Inject
    DeviceEventPort deviceEventPort;


    @Incoming("command-created")
    public void receiveCommand(byte[] command) {
        commandEventReceivedPort.receive(command);
    }

    @Incoming("data-received")
    public void receive(byte[] eui) {
        logger.info("Data received: " + eui);
        dataEventReceivedPort.receive(eui);
    }

    @Incoming("device-created")
    public void deviceCreated(byte[] eui) {
        logger.info("Device created: " + eui);
        deviceEventPort.deviceCreated(eui);

    }

    @Incoming("device-removed")
    public void deviceRemoved(byte[] eui) {
        logger.info("Device removed: " + eui);
        deviceEventPort.deviceRemoved(eui);
    }

    @Incoming("device-updated")
    public void deviceUpdated(byte[] eui) {
        logger.info("Device updated: " + eui);
        deviceEventPort.deviceUpdated(eui);
    }

    /* @Incoming("device-control")
    public void deviceControl(byte[] groupEui) {
        logger.info("Device control: " + groupEui);
        deviceEventPort.deviceControl(groupEui);
    } */

}
