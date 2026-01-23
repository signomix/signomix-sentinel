package com.signomix.sentinel.domain.detectors;

public class StatusTemperature {

    public StatusTemperature(int status, double temperature) {
        this.status = status;
        this.temperature = temperature;
    }

    private int status;
    private double temperature;

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }
}