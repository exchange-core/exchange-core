package org.openpredict.exchange.jmh;

import org.openjdk.jmh.annotations.*;
import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.core.IOrdersBucket;

@State(Scope.Benchmark)
public class ExecutionPlan {

    @Param({"1000", "10000", "100000", "1000000"})
    public int numOrdersToAdd;


    public IOrdersBucket bucket;

    public Order orders[];

    @Setup(Level.Invocation)
    public void setUp() {

        orders = new Order[numOrdersToAdd];
        for (int i = 0; i < numOrdersToAdd; i++) {
            orders[i] = Order.orderBuilder().orderId(i).uid(10).size(i).build();
        }

        bucket = IOrdersBucket.newInstance();
    }
}
