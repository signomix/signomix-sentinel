package com.signomix.sentinel.port.in;

import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.User;
import com.signomix.common.iot.sentinel.SentinelConfig;
import com.signomix.sentinel.domain.SentinelLogic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class SentinelPort {

    @Inject
    Logger logger;

    @Inject
    SentinelLogic sentinelLogic;

    public SentinelConfig getConfig(User user, long id){
        logger.info("getSentinelConfig: "+id);
        return sentinelLogic.getSentinelConfig(user, id);
    }

    public List<SentinelConfig> getConfigs(User user, int limit, int offset){
        logger.info("getSentinelConfigs: "+limit+" "+offset);
        return sentinelLogic.getSentinelConfigs(user, limit, offset);
    }
    
}
