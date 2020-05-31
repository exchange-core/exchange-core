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


import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.Order;
import exchange.core2.core.common.cmd.OrderCommand;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@Slf4j
public final class OrdersBucketNaiveTest {

    private static final int PRICE = 1000;
    private static final int UID_1 = 412;
    private static final int UID_2 = 413;
    private static final int UID_9 = 419;

    private final OrderBookEventsHelper eventsHelper = new OrderBookEventsHelper(MatcherTradeEvent::new);

    private OrdersBucketNaive bucket;

    @Before
    public void beforeGlobal() {

        bucket = new OrdersBucketNaive(PRICE);

        bucket.put(Order.builder().orderId(1).uid(UID_1).size(100).build());
        assertThat(bucket.getNumOrders(), is(1));
        assertThat(bucket.getTotalVolume(), is(100L));

        bucket.validate();

        bucket.put(Order.builder().orderId(2).uid(UID_2).size(40).build());
        assertThat(bucket.getNumOrders(), is(2));
        assertThat(bucket.getTotalVolume(), is(140L));

        bucket.validate();

        bucket.put(Order.builder().orderId(3).uid(UID_1).size(1).build());
        assertThat(bucket.getNumOrders(), is(3));
        assertThat(bucket.getTotalVolume(), is(141L));

        bucket.validate();

        bucket.remove(2, UID_2);
        assertThat(bucket.getNumOrders(), is(2));
        assertThat(bucket.getTotalVolume(), is(101L));

        bucket.validate();

        bucket.put(Order.builder().orderId(4).uid(UID_1).size(200).build());
        assertThat(bucket.getNumOrders(), is(3));
        assertThat(bucket.getTotalVolume(), is(301L));
    }

    @Test
    public void shouldAddOrder() {
        bucket.put(Order.builder().orderId(5).uid(UID_2).size(240).build());

        assertThat(bucket.getNumOrders(), is(4));
        assertThat(bucket.getTotalVolume(), is(541L));
    }


    @Test
    public void shouldRemoveOrders() {

        Order removed = bucket.remove(1, UID_1);
        assertNotNull(removed);
        assertThat(bucket.getNumOrders(), is(2));
        assertThat(bucket.getTotalVolume(), is(201L));

        removed = bucket.remove(4, UID_1);
        assertNotNull(removed);
        assertThat(bucket.getNumOrders(), is(1));
        assertThat(bucket.getTotalVolume(), is(1L));

        // can not remove existing order
        removed = bucket.remove(4, UID_1);
        assertNull(removed);
        assertThat(bucket.getNumOrders(), is(1));
        assertThat(bucket.getTotalVolume(), is(1L));

        removed = bucket.remove(3, UID_1);
        assertNotNull(removed);
        assertThat(bucket.getNumOrders(), is(0));
        assertThat(bucket.getTotalVolume(), is(0L));
    }


    @Test
    public void shouldAddManyOrders() {
        int numOrdersToAdd = 100_000;
        long expectedVolume = bucket.getTotalVolume();
        int expectedNumOrders = bucket.getNumOrders() + numOrdersToAdd;
        for (int i = 0; i < numOrdersToAdd; i++) {
            bucket.put(Order.builder().orderId(i + 5).uid(UID_2).size(i).build());
            expectedVolume += i;
        }

        assertThat(bucket.getNumOrders(), is(expectedNumOrders));
        assertThat(bucket.getTotalVolume(), is(expectedVolume));
    }

    @Test
    public void shouldAddAndRemoveManyOrders() {
        int numOrdersToAdd = 100;
        long expectedVolume = bucket.getTotalVolume();
        int expectedNumOrders = bucket.getNumOrders() + numOrdersToAdd;

        List<Order> orders = new ArrayList<>(numOrdersToAdd);
        for (int i = 0; i < numOrdersToAdd; i++) {
            Order order = Order.builder().orderId(i + 5).uid(UID_2).size(i).build();
            orders.add(order);
            bucket.put(order);
            expectedVolume += i;
        }

        assertThat(bucket.getNumOrders(), is(expectedNumOrders));
        assertThat(bucket.getTotalVolume(), is(expectedVolume));

        Collections.shuffle(orders, new Random(1));

        for (Order order : orders) {
            bucket.remove(order.orderId, UID_2);
            expectedNumOrders--;
            expectedVolume -= order.size;
            assertThat(bucket.getNumOrders(), is(expectedNumOrders));
            assertThat(bucket.getTotalVolume(), is(expectedVolume));
        }

    }


    @Test
    public void shouldMatchAllOrders() {
        int numOrdersToAdd = 100;
        long expectedVolume = bucket.getTotalVolume();
        int expectedNumOrders = bucket.getNumOrders() + numOrdersToAdd;

        int orderId = 5;

        List<Order> orders = new ArrayList<>(numOrdersToAdd);
        for (int i = 0; i < numOrdersToAdd; i++) {
            Order order = Order.builder().orderId(orderId++).uid(UID_2).size(i).build();
            orders.add(order);
            bucket.put(order);
            expectedVolume += i;
        }

        assertThat(bucket.getNumOrders(), is(expectedNumOrders));
        assertThat(bucket.getTotalVolume(), is(expectedVolume));

        Collections.shuffle(orders, new Random(1));

        List<Order> orders1 = orders.subList(0, 80);

        for (Order order : orders1) {
            bucket.remove(order.orderId, UID_2);
            expectedNumOrders--;
            expectedVolume -= order.size;
            assertThat(bucket.getNumOrders(), is(expectedNumOrders));
            assertThat(bucket.getTotalVolume(), is(expectedVolume));
        }

        OrderCommand triggerOrd = OrderCommand.update(8182, UID_9, 1000);
        OrdersBucketNaive.MatcherResult matcherResult = bucket.match(expectedVolume, triggerOrd, eventsHelper);

        assertThat(MatcherTradeEvent.asList(matcherResult.eventsChainHead).size(), is(expectedNumOrders));

        assertThat(bucket.getNumOrders(), is(0));
        assertThat(bucket.getTotalVolume(), is(0L));

        bucket.getNumOrders();

    }

    @Test
    public void shouldMatchAllOrders2() {
        int numOrdersToAdd = 1000;
        long expectedVolume = bucket.getTotalVolume();
        int expectedNumOrders = bucket.getNumOrders();

        bucket.validate();
        int orderId = 5;

        for (int j = 0; j < 100; j++) {
            List<Order> orders = new ArrayList<>(numOrdersToAdd);
            for (int i = 0; i < numOrdersToAdd; i++) {
                Order order = Order.builder().orderId(orderId++).uid(UID_2).size(i).build();
                orders.add(order);

                bucket.put(order);
                expectedNumOrders++;
                expectedVolume += i;

                //log.debug("{}-{}: orderId:{}", j, i, orderId);

                bucket.validate();
            }

            assertThat(bucket.getNumOrders(), is(expectedNumOrders));
            assertThat(bucket.getTotalVolume(), is(expectedVolume));

            Collections.shuffle(orders, new Random(1));

            List<Order> orders1 = orders.subList(0, 900);

            for (Order order : orders1) {
                bucket.remove(order.orderId, UID_2);
                expectedNumOrders--;
                expectedVolume -= order.size;
                assertThat(bucket.getNumOrders(), is(expectedNumOrders));
                assertThat(bucket.getTotalVolume(), is(expectedVolume));

                bucket.validate();
            }

            long toMatch = expectedVolume / 2;

            OrderCommand triggerOrd = OrderCommand.update(119283900, UID_9, 1000);

            OrdersBucketNaive.MatcherResult matcherResult = bucket.match(toMatch, triggerOrd, eventsHelper);
            long totalVolume = matcherResult.volume;
            assertThat(totalVolume, is(toMatch));
            expectedVolume -= totalVolume;
            assertThat(bucket.getTotalVolume(), is(expectedVolume));
            expectedNumOrders = bucket.getNumOrders();

            bucket.validate();
        }

        OrderCommand triggerOrd = OrderCommand.update(1238729387, UID_9, 1000);

        OrdersBucketNaive.MatcherResult matcherResult = bucket.match(expectedVolume, triggerOrd, eventsHelper);

        assertThat(MatcherTradeEvent.asList(matcherResult.eventsChainHead).size(), is(expectedNumOrders));

        assertThat(bucket.getNumOrders(), is(0));
        assertThat(bucket.getTotalVolume(), is(0L));

        bucket.getNumOrders();

    }


}
