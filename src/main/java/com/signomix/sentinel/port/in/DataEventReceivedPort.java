package com.signomix.sentinel.port.in;

import org.jboss.logging.Logger;

import com.signomix.sentinel.domain.DataEventLogic;
import com.signomix.sentinel.domain.EventLogic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class DataEventReceivedPort {

    @Inject
    DataEventLogic dataEventLogic;
    @Inject
    Logger logger;

    public void receive(byte[] message, String messageId) {
        String decodedMessage=new String(message);
        logger.info("Data received: " + decodedMessage);
        String[] messageArray=decodedMessage.split(",");
        if(messageArray.length<1 || (messageArray.length>1 && messageArray.length<9)){
            //invalid message
            logger.warn("Invalid message received: "+decodedMessage);
        }else if(messageArray.length==1){
            //only EUI
            dataEventLogic.handleEvent(EventLogic.EVENT_TYPE_DATA, messageArray[0],null, messageId);
        }else{
            dataEventLogic.handleEvent(EventLogic.EVENT_TYPE_DATA, messageArray, messageId);
        }
        
    }
    
}
