package com.signomix.sentinel.adapter.in;

import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.User;
import com.signomix.common.iot.sentinel.Signal;
import com.signomix.sentinel.port.in.AuthPort;
import com.signomix.sentinel.port.in.SignalPort;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
@Path("/api/signal")
public class SignalRestApi {

    @Inject
    Logger logger;

    @Inject
    SignalPort signalPort;

    @Inject
    AuthPort authPort;

    @GET
    public Response getSignals(@HeaderParam("Authentication") String token, @QueryParam("limit") int limit, @QueryParam("offset") int offset) {
        logger.info("getSentinelConfigs: "+limit+" "+offset);
        User user = authPort.getUser(token);
        List<Signal> signals = signalPort.getSignals(user, limit, offset);
        return Response.ok().entity(signals).build();
    }

    @GET
    @Path("/{id}")
    public Response getSignal(@HeaderParam("Authentication") String token, @PathParam("id") long id) {
        User user = authPort.getUser(token);
        Signal signal = signalPort.getSignal(user, id);
        return Response.ok().entity(signal).build();
    }
    
}
