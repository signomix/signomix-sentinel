package com.signomix.sentinel.domain.detectors;

import java.sql.Timestamp;

public class TemperatureReading {
    //private LocalDateTime timestamp;
    private Timestamp timestamp;
    private double currentTemperature;
    private double requiredTemperature;
    private double requestedTemperature;
    private int roomStatus;

    public TemperatureReading(Timestamp timestamp, double currentTemperature, double requiredTemperature, int roomStatus, double requestedTemperature) {
        this.timestamp = timestamp;
        this.currentTemperature = currentTemperature;
        this.requiredTemperature = requiredTemperature;
        this.roomStatus = roomStatus;
        this.requestedTemperature = requestedTemperature;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }
    public double getCurrentTemperature() {
        return currentTemperature;
    }
    public void setCurrentTemperature(double currentTemperature) {
        this.currentTemperature = currentTemperature;
    }
    public double getRequiredTemperature() {
        return requiredTemperature;
    }
    public void setRequiredTemperature(double desiredTemperature) {
        this.requiredTemperature = desiredTemperature;
    }
    public int getRoomStatus() {
        return roomStatus;
    }
    public void setRoomStatus(int roomStatus) {
        this.roomStatus = roomStatus;
    }
    public double getRequestedTemperature() {
        return requestedTemperature;
    }
    public void setRequestedTemperature(double requestedTemperature) {
        this.requestedTemperature = requestedTemperature;
    }
    
}
