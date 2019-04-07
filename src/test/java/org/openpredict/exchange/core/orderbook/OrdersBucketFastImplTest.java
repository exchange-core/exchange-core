package org.openpredict.exchange.core.orderbook;


import lombok.extern.slf4j.Slf4j;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.core.TradeEventCallback;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.hamcrest.Matchers.is;

// TODO add test ignoring own order

@Slf4j
public class OrdersBucketFastImplTest extends OrdersBucketBaseTest {

    @Override
    protected IOrdersBucket createNewBucket() {
        IOrdersBucket bucket = new OrdersBucketFastImpl();
        bucket.setPrice(PRICE);
        return bucket;
    }

    private static final int UID_2 = 413;
    private static final int UID_9 = 419;

    @Test
    public void compareBucketScenario() {

        Random rnd = new Random(1);

        int numOrdersToAdd = 1000;
        long expectedVolume = 0;

        IOrdersBucket bucketRef = new OrdersBucketNaiveImpl();
        IOrdersBucket bucket = new OrdersBucketFastImpl();

        int orderId = 0;

        for (int j = 0; j < 100; j++) {

            List<Order> orders = new ArrayList<>(numOrdersToAdd);
            for (int i = 0; i < numOrdersToAdd; i++) {

                int size = rnd.nextInt(Integer.MAX_VALUE);
                orderId++;
                Order order1 = Order.orderBuilder().price(1).orderId(orderId).uid(UID_2).size(size).build();
                Order order2 = Order.orderBuilder().price(1).orderId(orderId).uid(UID_2).size(size).build();
                orders.add(order1);

                bucket.add(order1);
                bucketRef.add(order2);

                expectedVolume += size;

                //log.debug("{}-{}: orderId:{}", j, i, orderId);

                MatcherAssert.assertThat(bucket, is(bucketRef));
            }

            Collections.shuffle(orders, rnd);

            List<Order> ordersToRemove = orders.subList(0, 900);
            for (Order order : ordersToRemove) {
                bucket.remove(order.orderId, UID_2);
                bucketRef.remove(order.orderId, UID_2);
                expectedVolume -= order.size;
                MatcherAssert.assertThat(bucket, is(bucketRef));
            }


            TradeEventCallback.TradeEventCollector events = new TradeEventCallback.TradeEventCollector();
            TradeEventCallback.TradeEventCollector eventsRef = new TradeEventCallback.TradeEventCollector();
            long toMatch = expectedVolume / 2;
            long totalVolume = bucket.match(toMatch, UID_9, events::collect);
            bucketRef.match(toMatch, UID_9, eventsRef::collect);
            expectedVolume -= totalVolume;
            MatcherAssert.assertThat(bucket, is(bucketRef));
            MatcherAssert.assertThat(events, is(eventsRef));
        }

        TradeEventCallback.TradeEventCollector events = new TradeEventCallback.TradeEventCollector();
        TradeEventCallback.TradeEventCollector eventsRef = new TradeEventCallback.TradeEventCollector();
        bucket.match(expectedVolume, UID_9, events::collect);
        bucketRef.match(expectedVolume, UID_9, eventsRef::collect);
        MatcherAssert.assertThat(bucket, is(bucketRef));
        MatcherAssert.assertThat(events, is(eventsRef));
    }


}
