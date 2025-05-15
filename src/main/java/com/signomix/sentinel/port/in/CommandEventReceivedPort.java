package com.signomix.sentinel.port.in;

import com.signomix.sentinel.domain.CommandEventLogic;
import com.signomix.sentinel.domain.EventLogic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class CommandEventReceivedPort {

    @Inject
    CommandEventLogic commandEventLogic;

    public void receive(byte[] command, String messageId) {
        String commandString=new String(command);
        commandEventLogic.handleEvent(EventLogic.EVENT_TYPE_COMMAND, null,commandString, messageId);
    }
    
}
