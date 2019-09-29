/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.core.orderbook;


import lombok.extern.slf4j.Slf4j;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import exchange.core2.core.common.Order;
import exchange.core2.core.common.cmd.OrderCommand;

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
                Order order1 = Order.builder().price(1).orderId(orderId).uid(UID_2).size(size).build();
                Order order2 = Order.builder().price(1).orderId(orderId).uid(UID_2).size(size).build();
                orders.add(order1);

                bucket.put(order1);
                bucketRef.put(order2);

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

//            TradeEventCallback.TradeEventCollector events = new TradeEventCallback.TradeEventCollector();
//            TradeEventCallback.TradeEventCollector eventsRef = new TradeEventCallback.TradeEventCollector();
            long toMatch = expectedVolume / 2;

            OrderCommand trig = OrderCommand.update(1238729387, UID_9, 1000);
            OrderCommand trigRef = OrderCommand.update(1238729387, UID_9, 1000);

            long totalVolume = bucket.match(toMatch, trig, trig,IGNORE_CMD_CONSUMER);
            bucketRef.match(toMatch, trigRef, trigRef, IGNORE_CMD_CONSUMER);
            expectedVolume -= totalVolume;
            MatcherAssert.assertThat(bucket, is(bucketRef));
//            MatcherAssert.assertThat(events, is(eventsRef));
        }

//        TradeEventCallback.TradeEventCollector events = new TradeEventCallback.TradeEventCollector();
//        TradeEventCallback.TradeEventCollector eventsRef = new TradeEventCallback.TradeEventCollector();
        OrderCommand trig = OrderCommand.update(1238729387, UID_9, 1000);
        OrderCommand trigRef = OrderCommand.update(1238729387, UID_9, 1000);
        bucket.match(expectedVolume, trig, trig, IGNORE_CMD_CONSUMER);
        bucketRef.match(expectedVolume, trigRef, trigRef, IGNORE_CMD_CONSUMER);
        MatcherAssert.assertThat(bucket, is(bucketRef));
//        MatcherAssert.assertThat(events, is(eventsRef));
    }


}
