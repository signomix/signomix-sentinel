package com.signomix.sentinel.adapter.in;

import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.User;
import com.signomix.common.iot.sentinel.SentinelConfig;
import com.signomix.sentinel.port.in.AuthPort;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
@Path("/api/sentinel")
public class SentinelRestApi {

    @Inject
    Logger logger;

    @Inject
    AuthPort authPort;

    @GET
    public Response getSentinelConfigs(@HeaderParam("Authentication") String token, @QueryParam("limit") int limit, @QueryParam("offset") int offset) {
        logger.info("getSentinelConfigs: "+limit+" "+offset);
        User user = authPort.getUser(token);
        List<SentinelConfig> configs = sentinelDao.getConfigs(userId, limit, offset);
        return Response.ok().entity(configs).build();
    }

    @GET
    @Path("/{id}")
    public Response getSentinelConfig(@HeaderParam("Authentication") String token, @PathParam("id") long id) {
        logger.info("getSentinelConfig: "+id);
        User user = authPort.getUser(token);
        SentinelConfig config = sentinelDao.getConfig(id);
        return Response.ok().entity(config).build();
    }
    
}
