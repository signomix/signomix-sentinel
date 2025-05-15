package com.signomix.sentinel.adapter.in;

import java.util.concurrent.ThreadLocalRandom;

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
        try {
            String messageId = System.currentTimeMillis() + "-" + ThreadLocalRandom.current().nextInt();
            commandEventReceivedPort.receive(command,messageId);
        } catch (Exception e) {
            logger.error("Error processing command: " + e.getMessage());
        }
    }

    @Incoming("data-received")
    public void receive(byte[] eui) {
        try {
            logger.info("Data received: " + eui);
            String messageId = System.currentTimeMillis() + "-" + ThreadLocalRandom.current().nextInt();
            dataEventReceivedPort.receive(eui,messageId);
        } catch (Exception e) {
            logger.error("Error processing command: " + e.getMessage());
        }
    }

    @Incoming("device-created")
    public void deviceCreated(byte[] eui) {
        try {
            logger.info("Device created: " + eui);
            deviceEventPort.deviceCreated(eui);
        } catch (Exception e) {
            logger.error("Error processing command: " + e.getMessage());
        }

    }

    @Incoming("device-removed")
    public void deviceRemoved(byte[] eui) {
        try {
            logger.info("Device removed: " + eui);
            deviceEventPort.deviceRemoved(eui);
        } catch (Exception e) {
            logger.error("Error processing command: " + e.getMessage());
        }
    }

    @Incoming("device-updated")
    public void deviceUpdated(byte[] eui) {
        try {
            logger.info("Device updated: " + eui);
            deviceEventPort.deviceUpdated(eui);
        } catch (Exception e) {
            logger.error("Error processing command: " + e.getMessage());
        }
    }

    /*
     * @Incoming("device-control")
     * public void deviceControl(byte[] groupEui) {
     * logger.info("Device control: " + groupEui);
     * deviceEventPort.deviceControl(groupEui);
     * }
     */

}
