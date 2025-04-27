package com.signomix.sentinel.port.in;

import com.signomix.sentinel.domain.CommandEventLogic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class CommandEventReceivedPort {

    @Inject
    CommandEventLogic commandEventLogic;

    public void receive(byte[] command) {
        String commandString=new String(command);
        commandEventLogic.handleCommandCreatedEvent(commandString);
    }
    
}
