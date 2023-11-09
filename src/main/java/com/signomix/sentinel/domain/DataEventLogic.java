package com.signomix.sentinel.domain;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class DataEventLogic {

    public void handleDataReceivedEvent(String deviceEui) {
        System.out.println("Handling data received event: " + deviceEui);
    }
    
}
