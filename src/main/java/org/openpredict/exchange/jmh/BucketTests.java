package org.openpredict.exchange.jmh;

import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.core.IOrdersBucket;

@Slf4j
public class BucketTests {

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @BenchmarkMode(Mode.All)
    public void addPerformance(ExecutionPlan ep) {

        IOrdersBucket bucket = ep.bucket;

        //long t = System.currentTimeMillis();
        for (Order order : ep.orders) {
            bucket.add(order);
        }
        //log.debug("{}ms", System.currentTimeMillis() - t);



    }

}
