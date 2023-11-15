package com.signomix.sentinel.port.in;

import org.jboss.logging.Logger;

import com.signomix.common.Token;
import com.signomix.common.User;
import com.signomix.sentinel.domain.AuthLogic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class AuthPort {

    @Inject
    Logger logger;

    @Inject
    AuthLogic authLogic;

    public String getUserId(String token){
        if(token!=null && token.endsWith("/")){
            token=token.substring(0,token.length()-1);
        }
        logger.info("getUserId: "+token);
        Token t=authLogic.getToken(token);
        if(t!=null){
            if(t.getIssuer()!=null && !t.getIssuer().isEmpty()){
                return t.getIssuer();
            }else{
                return t.getUid();
            }
        }else{
            return null;
        }
    }

    public User getUser(String token){
        return authLogic.getUser(getUserId(token));
    }
}
