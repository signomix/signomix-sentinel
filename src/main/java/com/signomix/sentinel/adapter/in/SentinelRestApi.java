package com.signomix.sentinel.adapter.in;

import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.User;
import com.signomix.common.iot.sentinel.SentinelConfig;
import com.signomix.sentinel.port.in.AuthPort;
import com.signomix.sentinel.port.in.SentinelPort;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
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

    @Inject
    SentinelPort sentinelPort;

    @GET
    public Response getSentinelConfigs(@HeaderParam("Authentication") String token, @QueryParam("limit") int limit,
            @QueryParam("offset") int offset) {
        logger.info("getSentinelConfigs: " + limit + " " + offset);
        User user = authPort.getUser(token);
        if (user == null) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        List<SentinelConfig> configs = sentinelPort.getConfigs(user, limit, offset);
        return Response.ok().entity(configs).build();
    }

    @GET
    @Path("/{id}")
    public Response getSentinelConfig(@HeaderParam("Authentication") String token, @PathParam("id") long id) {
        logger.info("getSentinelConfig: " + id);
        User user = authPort.getUser(token);
        if (user == null) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        SentinelConfig config = sentinelPort.getConfig(user, id);
        return Response.ok().entity(config).build();
    }

    @POST
    public Response createSentinelConfig(@HeaderParam("Authentication") String token, SentinelConfig config) {
        try {
            logger.info("createSentinelConfig: " + config);
            User user = authPort.getUser(token);
            if (user == null) {
                return Response.status(Response.Status.UNAUTHORIZED).build();
            }
            sentinelPort.createConfig(user, config);
            return Response.ok().build();
        } catch (Exception e) {
            logger.error("createSentinelConfig: " + e.getMessage());
            e.printStackTrace();
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }

    @PUT
    @Path("/{id}")
    public Response updateSentinelConfig(@HeaderParam("Authentication") String token, @PathParam("id") String id,
            SentinelConfig config) {
        try {
            logger.info("createSentinelConfig: " + config);
            User user = authPort.getUser(token);
            if (user == null) {
                return Response.status(Response.Status.UNAUTHORIZED).build();
            }
            sentinelPort.updateConfig(user, config);
            return Response.ok().build();
        } catch (Exception e) {
            logger.error("createSentinelConfig: " + e.getMessage());
            e.printStackTrace();
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }

    @DELETE
    @Path("/{id}")
    public Response deleteSentinelConfig(@HeaderParam("Authentication") String token, @PathParam("id") long id) {
        logger.info("deleteSentinelConfig: " + id);
        User user = authPort.getUser(token);
        if (user == null) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        sentinelPort.deleteConfig(user, id);
        return Response.ok().build();
    }

}
