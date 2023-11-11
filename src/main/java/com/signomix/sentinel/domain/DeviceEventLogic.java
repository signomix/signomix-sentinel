package com.signomix.sentinel.domain;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class DeviceEventLogic {

    public void handleDeviceCreatedEvent(String deviceEui) {
        System.out.println("Handling event: " + deviceEui);
        //
    }

    public void handleDeviceRemovedEvent(String deviceEui) {
        System.out.println("Handling event: " + deviceEui);
        //
    }

    public void handleDeviceUpdatedEvent(String deviceEui) {
        System.out.println("Handling event: " + deviceEui);
        //
    }
    
}
