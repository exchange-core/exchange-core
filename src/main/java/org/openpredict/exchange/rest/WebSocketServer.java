package org.openpredict.exchange.rest;

import lombok.extern.slf4j.Slf4j;
import org.atmosphere.nettosphere.Config;
import org.atmosphere.nettosphere.Nettosphere;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Service
@Slf4j
public class WebSocketServer {

    private Nettosphere nettosphere;

    @PostConstruct
    public void initWebSocketServer() {

        Config config = (new Config.Builder()).resource(Chat.class)
                // For *-distrubution
                .resource("./webapps")
                // For mvn exec:java
                .resource("./src/main/resources")
                // For running inside an IDE
                .resource("./nettosphere-samples/chat/src/main/resources")
                .port(8081)
                .host("0.0.0.0")
                .build();

        nettosphere = new Nettosphere.Builder().config(config).build();
        nettosphere.start();
    }


    @PreDestroy
    public void stop() {
        nettosphere.stop();
    }
}
