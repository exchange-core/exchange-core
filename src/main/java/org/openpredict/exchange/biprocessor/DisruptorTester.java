package org.openpredict.exchange.biprocessor;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.openpredict.exchange.beans.CfgWaitStrategyType;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.core.DisruptorExceptionHandler;
import org.openpredict.exchange.util.LatencyTools;

import java.util.Map;
import java.util.concurrent.Executors;

@Slf4j
public class DisruptorTester {


    private Disruptor<OrderCommand> eventsDisruptor;
    private RingBuffer<OrderCommand> cmdRingBuffer;

    private IntLongHashMap latencies = new IntLongHashMap(100000);


    public static void main(String[] args) {
        DisruptorTester dt = new DisruptorTester();
    }


    public DisruptorTester() {

        log.warn("START");

        eventsDisruptor = new Disruptor<>(
                OrderCommand::new,
                65536,
                Executors.defaultThreadFactory(),
                ProducerType.MULTI,
                CfgWaitStrategyType.BUSY_SPIN.create());

        eventsDisruptor.handleEventsWith(this::process);

        eventsDisruptor.setDefaultExceptionHandler(new DisruptorExceptionHandler<>("events"));
        cmdRingBuffer = eventsDisruptor.start();


        int numMessages = 10_000_000;

        for (int j = 0; j < 1000; j++) {

            latencies.clear();

            long t = System.currentTimeMillis();

            for (int i = 0; i < numMessages; i++) {
                int id = i;
                cmdRingBuffer.publishEvent((event, sequence) -> {
                    event.timestamp = System.nanoTime();
                    event.orderId = id;
                });
            }

            Map<String, String> report = LatencyTools.createLatencyReportFast(latencies);
            t = System.currentTimeMillis() - t;
            float perfMt = (float) numMessages / (float) t / 1000.0f;

            log.debug("{}. {} Mmps Latency: {}", j, perfMt, report);

        }


        eventsDisruptor.shutdown();

    }

    private void process(OrderCommand cmd, long seq, boolean eob) {
        int key = (int) ((System.nanoTime() - cmd.timestamp) >> 9);
        latencies.updateValue(key, 0, x -> x + 1);
    }


}
