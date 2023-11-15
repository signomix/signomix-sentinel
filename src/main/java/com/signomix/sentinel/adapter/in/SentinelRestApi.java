package com.signomix.sentinel.adapter.in;

import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.iot.sentinel.SentinelConfig;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
@Path("/api/sentinel")
public class SentinelRestApi {

    @Inject
    Logger logger;

    @GET
    public Response getSentinelConfigs(@QueryParam("limit") int limit, @QueryParam("offset") int offset) {
        logger.info("getSentinelConfigs: "+limit+" "+offset);
        List<SentinelConfig> configs = sentinelDao.getConfigs(userId, limit, offset);
        return Response.ok().entity(configs).build();
    }

    @GET
    @Path("/{id}")
    public Response getSentinelConfig(@PathParam("id") long id) {
        logger.info("getSentinelConfig: "+id);
        SentinelConfig config = sentinelDao.getConfig(id);
        return Response.ok().entity(config).build();
    }
    
}
