package com.signomix.sentinel.port.in;

import com.signomix.sentinel.domain.DataEventLogic;
import com.signomix.sentinel.domain.DeviceEventLogic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class DeviceEventPort {

    @Inject
    DeviceEventLogic deviceEventLogic;

    public void deviceCreated(byte[] eui) {
        String deviceEui=new String(eui);
        deviceEventLogic.handleDeviceCreatedEvent(deviceEui);
    }

    public void deviceRemoved(byte[] eui) {
        String deviceEui=new String(eui);
        deviceEventLogic.handleDeviceRemovedEvent(deviceEui);
    }

    public void deviceUpdated(byte[] eui) {
        String deviceEui=new String(eui);
        deviceEventLogic.handleDeviceUpdatedEvent(deviceEui);
    }

    public void deviceControl(byte[] groupEui) {
        String groupEuiStr=new String(groupEui);
        deviceEventLogic.handleDeviceControlEvent(groupEuiStr);
    }
    
}
