package org.openpredict.exchange.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.atmosphere.config.service.Get;
import org.atmosphere.config.service.ManagedService;
import org.atmosphere.config.service.Message;
import org.atmosphere.cpr.AtmosphereResource;
import org.atmosphere.cpr.AtmosphereResourceEvent;
import org.atmosphere.cpr.AtmosphereResourceEventListenerAdapter;
import org.atmosphere.cpr.Broadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;


@ManagedService(path = "/chat")
public class Chat {

    private final ObjectMapper mapper = new ObjectMapper();

    private final Logger logger = LoggerFactory.getLogger(Chat.class);

    @Get
    public void onOpen(final AtmosphereResource atmoRes) {

        atmoRes.addEventListener(new AtmosphereResourceEventListenerAdapter() {
            @Override
            public void onSuspend(final AtmosphereResourceEvent event) {
                logger.info("User {} connected.", atmoRes.uuid());

                List<Broadcaster> broadcasters = event.getResource().broadcasters();
                logger.info("broadcasters: {}", broadcasters);

            }

            @Override
            public void onDisconnect(final AtmosphereResourceEvent event) {
                if (event.isCancelled()) {
                    logger.info("User {} unexpectedly disconnected", atmoRes.uuid());
                } else if (event.isClosedByClient()) {
                    logger.info("User {} closed the connection", atmoRes.uuid());
                }
            }
        });
    }

    @Message
    public String onMessage(String message) throws IOException {
        ObjectNode jsonNode = mapper.readValue(message, ObjectNode.class);
        switch (jsonNode.get("msgType").toString()) {
            case "chat":
                Data data = mapper.readValue(message, Data.class);
                logger.debug("Data: {}", data);
                return mapper.writeValueAsString(data);
            default:
                return null;
        }
    }

    @ToString
    public final static class Data {

        @Getter
        private final String msgType = "chat";

        private String message;
        private String author;
        private long time;

        public Data() {
            this("", "");
        }

        public Data(String author, String message) {
            this.author = author;
            this.message = message;
            this.time = new Date().getTime();
        }

        public String getMessage() {
            return message;
        }

        public String getAuthor() {
            return author;
        }

        public void setAuthor(String author) {
            this.author = author;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }

    }


}

