package org.openpredict.exchange.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.atmosphere.cpr.Broadcaster;
import org.atmosphere.cpr.BroadcasterFactory;
import org.atmosphere.nettosphere.Config;
import org.atmosphere.nettosphere.Nettosphere;
import org.openpredict.exchange.rest.events.OrderBookEvent;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Instant;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.LongStream;

@Service
@Slf4j
public class WebSocketServer {

    private Nettosphere nettosphere;

    private final ObjectMapper mapper = new ObjectMapper();

    private Broadcaster webSocketBroadcaster;


    @PostConstruct
    public void initWebSocketServer() {

        Config config = (new Config.Builder())
                .resource(Chat.class)
                .resource("./webapps")
                .resource("./src/main/resources")
                .resource("./nettosphere-samples/chat/src/main/resources")
                .port(8081)
                .host("0.0.0.0")
                .build();

        nettosphere = new Nettosphere.Builder().config(config).build();
        nettosphere.start();


        BroadcasterFactory bf = nettosphere.framework().getBroadcasterFactory();
        Collection<Broadcaster> broadcasters = bf.lookupAll();
        log.info("Broadcasters: {}", broadcasters);

        webSocketBroadcaster = bf.lookup("/chat");
        log.info("webSocketBroadcaster: {}", webSocketBroadcaster);

        if (false) {
            final Random random = new Random();
            Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(() -> {
                //Chat.Data data = new Chat.Data("Server", "msg " + Math.random());

                Function<Integer, long[]> randoms = size -> LongStream.generate(() -> random.nextInt(100)).limit(size).toArray();

                OrderBookEvent data = new OrderBookEvent(
                        "XBTC",
                        Instant.now().toEpochMilli(),
                        new long[]{55, 56, 57, 69},
                        randoms.apply(4),
                        new long[]{52, 51, 49, 45},
                        randoms.apply(4));

                broadcast(data);

            }, 1, 1, TimeUnit.SECONDS);
        }


    }

    public void broadcast(Object data) {
        //log.debug("Broadcasting: {}", data);

        String msg;
        try {
            msg = mapper.writeValueAsString(data);
            log.debug("Broadcasting JSON: {}", msg);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return;
        }

        //log.info("sending {}", msg);
        Future<Object> broadcast = webSocketBroadcaster.broadcast(msg);
//        log.info("broadcast sent");
//        try {
//            log.info("future.result: {}", broadcast.get());
//        } catch (InterruptedException | ExecutionException e) {
//            e.printStackTrace();
//        }
    }


    @PreDestroy
    public void stop() {
        nettosphere.stop();
    }
}
