package org.openpredict.exchange.tests.performance;


import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.core.IOrdersBucket;
import org.openpredict.exchange.core.TradeEventCallback;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class ITOrderBucket {


    private static final int PRICE = 1000;
    private static final int UID_1 = 412;
    private static final int UID_2 = 413;
    private static final int UID_9 = 419;

    private IOrdersBucket bucket;

    @Before
    public void before() {
        bucket = IOrdersBucket.newInstance();
        bucket.setPrice(PRICE);

        bucket.add(Order.orderBuilder().orderId(1).uid(UID_1).size(100).build());
        assertThat(bucket.getNumOrders(), is(1));
        assertThat(bucket.getTotalVolume(), is(100L));

        bucket.validate();

        bucket.add(Order.orderBuilder().orderId(2).uid(UID_2).size(40).build());
        assertThat(bucket.getNumOrders(), is(2));
        assertThat(bucket.getTotalVolume(), is(140L));

        bucket.validate();

        bucket.add(Order.orderBuilder().orderId(3).uid(UID_1).size(1).build());
        assertThat(bucket.getNumOrders(), is(3));
        assertThat(bucket.getTotalVolume(), is(141L));

        bucket.validate();

        bucket.remove(2, UID_2);
        assertThat(bucket.getNumOrders(), is(2));
        assertThat(bucket.getTotalVolume(), is(101L));

        bucket.validate();

        bucket.add(Order.orderBuilder().orderId(4).uid(UID_1).size(200).build());
        assertThat(bucket.getNumOrders(), is(3));
        assertThat(bucket.getTotalVolume(), is(301L));
    }


    @Test
    public void shouldMatchAllOrdersAndValidateEachStep() {
        int numOrdersToAdd = 1000;
        long expectedVolume = bucket.getTotalVolume();
        int expectedNumOrders = bucket.getNumOrders();

        bucket.validate();
        int orderId = 5;

        for (int j = 0; j < 300; j++) {
            List<Order> orders = new ArrayList<>(numOrdersToAdd);
            for (int i = 0; i < numOrdersToAdd; i++) {
                Order order = Order.orderBuilder().orderId(orderId++).uid(UID_2).size(i).build();
                orders.add(order);

                bucket.add(order);
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
            long totalVolume = bucket.match(toMatch, UID_9, TradeEventCallback::empty);
            assertThat(totalVolume, is(toMatch));
            expectedVolume -= totalVolume;
            assertThat(bucket.getTotalVolume(), is(expectedVolume));
            expectedNumOrders = bucket.getNumOrders();

            bucket.validate();
        }

        TradeEventCallback callbackMock = Mockito.mock(TradeEventCallback.class);
        bucket.match(expectedVolume, UID_9, callbackMock);

        verify(callbackMock, times(expectedNumOrders)).submit(any(), anyLong(), anyBoolean(), anyBoolean());

        //assertThat(matching.records.size(), is(expectedNumOrders));

        assertThat(bucket.getNumOrders(), is(0));
        assertThat(bucket.getTotalVolume(), is(0L));

        bucket.getNumOrders();

    }


    // ---------------------- PERFORMANCE ----------------

    @Test
    public void perfAddManyOrders() {
        int numOrdersToAdd = 20_000_000;
        long expectedVolume = bucket.getTotalVolume();
        int expectedNumOrders = bucket.getNumOrders() + numOrdersToAdd;
        Order orders[] = new Order[numOrdersToAdd];
        for (int i = 0; i < numOrdersToAdd; i++) {
            orders[i] = Order.orderBuilder().orderId(i + 5).uid(UID_2).size(i).build();
            expectedVolume += i;
        }

        long t = System.currentTimeMillis();
        for (Order order : orders) {
            bucket.add(order);
        }
        log.debug("{}ms", System.currentTimeMillis() - t);

        assertThat(bucket.getNumOrders(), is(expectedNumOrders));
        assertThat(bucket.getTotalVolume(), is(expectedVolume));
    }


    @Test
    public void perfFullCycle() {
        int numOrdersToAdd = 1000;
        long expectedVolume = bucket.getTotalVolume();
        int expectedNumOrders = bucket.getNumOrders();

        long timeAccum = 0;

        bucket.validate();
        int orderId = 5;

        for (int j = 0; j < 50_000; j++) {
            List<Order> orders = new ArrayList<>(numOrdersToAdd);

            long s = System.nanoTime();
            for (int i = 0; i < numOrdersToAdd; i++) {
                Order order = Order.orderBuilder().orderId(orderId++).uid(UID_2).size(i).build();
                orders.add(order);

                bucket.add(order);
                expectedNumOrders++;
                expectedVolume += i;

                //log.debug("{}-{}: orderId:{}", j, i, orderId);
            }
            timeAccum += System.nanoTime() - s;

            assertThat(bucket.getNumOrders(), is(expectedNumOrders));
            assertThat(bucket.getTotalVolume(), is(expectedVolume));

            Collections.shuffle(orders, new Random(1));

            List<Order> orders1 = orders.subList(0, 900);

            s = System.nanoTime();
            for (Order order : orders1) {
                bucket.remove(order.orderId, UID_2);
                expectedNumOrders--;
                expectedVolume -= order.size;
                assertThat(bucket.getNumOrders(), is(expectedNumOrders));
                assertThat(bucket.getTotalVolume(), is(expectedVolume));
            }
            timeAccum += System.nanoTime() - s;

            long toMatch = expectedVolume / 2;
            s = System.nanoTime();
            long matchingTotalVol = bucket.match(toMatch, UID_9, TradeEventCallback::empty);
            timeAccum += System.nanoTime() - s;
            assertThat(matchingTotalVol, is(toMatch));
            expectedVolume -= matchingTotalVol;
            assertThat(bucket.getTotalVolume(), is(expectedVolume));
            expectedNumOrders = bucket.getNumOrders();

        }

        log.debug("Time: {}ms", timeAccum / 1000000);

    }

}
