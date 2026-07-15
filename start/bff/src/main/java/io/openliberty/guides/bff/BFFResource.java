package io.openliberty.guides.bff;

import io.openliberty.guides.models.SystemLoad;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.sse.OutboundSseEvent;
import jakarta.ws.rs.sse.Sse;
import jakarta.ws.rs.sse.SseBroadcaster;
import jakarta.ws.rs.sse.SseEventSink;
import java.util.logging.Logger;

@ApplicationScoped
@Path("/sse")
public class BFFResource {

    private Logger logger = Logger.getLogger(BFFResource.class.getName());

    private Sse sse;
    private SseBroadcaster broadcaster;

    @GET
    @Path("/")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public void subscribeToSystem(
            @Context SseEventSink sink,
            @Context Sse sse
    ) {

        if (this.sse == null || this.broadcaster == null) {
            this.sse = sse;
            this.broadcaster = sse.newBroadcaster();
        }

        this.broadcaster.register(sink);
        logger.info("New sink registered to broadcaster.");
    }

    private void broadcastData(String name, Object data) {
        if (broadcaster != null) {
            OutboundSseEvent event = sse.newEventBuilder()
                    .name(name)
                    .data(data.getClass(), data)
                    .mediaType(MediaType.APPLICATION_JSON_TYPE)
                    .build();
            broadcaster.broadcast(event);
        } else {
            logger.info("Unable to send SSE. Broadcaster context is not set up.");
        }
    }

    @Incoming("systemLoad")
    public void getSystemLoadMessage(SystemLoad sl)  {
        logger.info("Message received from system.load topic. " + sl.toString());
        broadcastData("systemLoad", sl);
    }
}
