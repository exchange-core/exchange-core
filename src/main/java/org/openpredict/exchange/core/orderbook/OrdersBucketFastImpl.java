package org.openpredict.exchange.core.orderbook;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.eclipse.collections.api.map.primitive.MutableLongIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongIntHashMap;
import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.beans.cmd.OrderCommand;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Fast version of Order Bucket.<br/>
 * Implementation is optimized for fastest place/remove operations O(1).<br/>
 * Matching operation can be slower though.<br/>
 * <p>
 * Orders are stored in resizable queue array.<br/>
 * Queue is indexed by hashmap for fast cancel/update operations.<br/>
 */
@NoArgsConstructor
@Slf4j
@ToString
public final class OrdersBucketFastImpl implements IOrdersBucket {

    @Getter
    @Setter
    private long price;

    private MutableLongIntMap positions = new LongIntHashMap();

    private Order[] queue = new Order[4];

    public int tail = 0;
    public int head = 0;
    public int queueSize = 0;
    public int realSize = 0;

    @Getter
    private long totalVolume = 0;

    @Override
    public void add(Order order) {

        //validate();

        totalVolume += (order.size - order.filled);

        queue[tail] = order;
        queueSize++;
        realSize++;
        tail++;
        positions.put(order.orderId, tail); // always put actual value+1 (null=0 specifics)
        if (tail == queue.length) {
            tail = 0;
        }

        // check if no space left
        if (queueSize == queue.length) {
            upsizeBuffer();
        }

        //validate();

    }

    private void upsizeBuffer() {
        int oldLength = queue.length;
        Order[] array2 = new Order[oldLength * 2];
        int tail2 = 0;
        int left = realSize;

        while (left > 0) {
            while (queue[head] == null) {
                head++;
                queueSize--;
                if (head == queue.length) {
                    head = 0;
                }
            }
            Order order = queue[head++];
            if (head == queue.length) {
                head = 0;
            }
            array2[tail2++] = order;
            positions.put(order.orderId, tail2);
            left--;
        }

        queue = array2;
        head = 0;
        tail = tail2;

    }


    /**
     * Remove order
     *
     * @param orderId
     * @return
     */
    @Override
    public Order remove(long orderId, long uid) {

        //validate();

        int pos = positions.get(orderId);
        if (pos == 0) {
            // TODO why this msg never logged ?
            //log.debug("order not found {}", orderId);
            return null;
        }

        Order order = queue[pos - 1];
        if (order.uid != uid) {
            // can not remove other user's order
            return null;
        }

        positions.remove(orderId);

        queue[pos - 1] = null;
        totalVolume -= (order.size - order.filled);
        assert totalVolume >= 0;
        realSize--;

        //validate();
        return order;
    }

    /**
     * Collect a list of matching orders starting from eldest records
     * Completely matching orders will be removed, partially matched order kept in the bucked.
     */
    @Override

    public long match(long volumeToCollect, OrderCommand activeOrder, OrderCommand triggerCmd, Consumer<Order> removeOrderCallback) {

        //validate();

        long totalMatchingVolume = 0;

        int ptr = head;
        int numOrdersToScan = realSize;
        int ownOrderBarrier = -1;
        final long ignoreUid = activeOrder.uid;

        while (numOrdersToScan > 0 && volumeToCollect > 0) {
            // fast-forward head pointer until non-empty element found
            // (assume if realSize>0 then at least one order will be found)
            while (queue[ptr] == null) {
                ptr++;
                ptr = (ptr == queue.length) ? 0 : ptr;
            }

            Order order = queue[ptr];
            long orderId = order.orderId;

            // ignoring own orders
            if (order.uid == ignoreUid) {
                numOrdersToScan--;

                // set own order barrier - after matching procedure set head there
                // so next matching will start with this order
                if (ownOrderBarrier == -1) {
                    ownOrderBarrier = ptr;
                }

                ptr++;
                ptr = (ptr == queue.length) ? 0 : ptr;

                continue;
            }

            // calculate exact volume can fill for this order
//            log.debug("volumeToCollect={} order: s{} f{}", volumeToCollect, order.size, order.filled);
            long v = Math.min(volumeToCollect, order.size - order.filled);
            totalMatchingVolume += v;
//            log.debug("totalMatchingVolume={} v={}", totalMatchingVolume, v);

            order.filled += v;
            volumeToCollect -= v;
            totalVolume -= v;
            if (totalVolume < 0) {
                log.warn("{}", totalVolume);
            }

            // remove from order book filled orders
            boolean fullMatch = order.size == order.filled;

            OrderBookEventsHelper.sendTradeEvent(triggerCmd, activeOrder, order, fullMatch, volumeToCollect == 0, price, v);

            if (fullMatch) {

                removeOrderCallback.accept(order);

                // remove head
                queue[ptr] = null;
                ptr++;
                ptr = (ptr == queue.length) ? 0 : ptr;
                realSize--;
                numOrdersToScan--;
                positions.remove(orderId);
            }
        }

        head = (ownOrderBarrier == -1) ? ptr : ownOrderBarrier;
        queueSize = tail - head;
        if (queueSize < 0) {
            queueSize += queue.length;
        }

        //validate();

        return totalMatchingVolume;
    }

    /**
     * Reduce order volume if possible
     * <p>
     * orderId
     * newSize
     *
     * @return
     */
    @Override
    public boolean tryReduceSize(OrderCommand cmd) {

        //validate();

        int pos = positions.get(cmd.orderId);
        if (pos == 0) {
            return false;
        }

        Order order = queue[pos - 1];

        if (order.uid != cmd.uid) {
            return false;
        }

        long reduceBy = order.size - order.filled - cmd.size;
        if (reduceBy > 0) {
            order.size -= reduceBy;
            totalVolume -= reduceBy;
            OrderBookEventsHelper.sendReduceEvent(cmd, order, reduceBy);
        }

        //validate();

        return true;
    }

    @Override
    public void validate() {
        //log.debug("validate: {}", this.price);

        int c = 0;
        int positionInQueue = 0;
        for (Order order : queue) {
            positionInQueue++;
            if (order != null) {
                c++;
                int positionFromHashtable = positions.get(order.orderId);
                if (positionFromHashtable != positionInQueue) {
                    throw new IllegalStateException("positionFromHashtable=" + positionFromHashtable
                            + " positionInQueue=" + positionInQueue + " order:" + order);
                }
            }
        }

        if (positions.size() != c) {
            String msg = String.format("%d: Found %d orders in queue, but there are %d in hash table", price, c, positions.size());
            throw new IllegalStateException(msg);
        }

        if (positions.size() != realSize) {
            throw new IllegalStateException();
        }

        int expectedSize = tail - head;
        if (expectedSize >= 0) {
            if (expectedSize != queueSize) {
                String msg = String.format("tail=%d head=%d queueSize=%d expectedSize=%d", tail, head, queueSize, expectedSize);
                throw new IllegalStateException(msg);
            }
        } else {
            if (expectedSize + queue.length != queueSize) {
                throw new IllegalStateException();
            }
        }

    }

    @Override
    public Order findOrder(long orderId) {
        int pos = positions.get(orderId);
        if (pos == 0) {
            return null;
        }
        return queue[pos - 1];
    }

    @Override
    public List<Order> getAllOrders() {
        return Arrays.asList(asOrdersArray());
    }

    private void printSchema() {
        StringBuilder s = new StringBuilder();
        for (Order order : queue) {
            s.append(order == null ? '.' : 'O');
        }

        log.debug("       {}", StringUtils.repeat(' ', tail) + "T");
        log.debug("queue:[{}]", s.toString());
        log.debug("       {}", StringUtils.repeat(' ', head) + "H");
    }

    @Override
    public int getNumOrders() {
        return realSize;
    }

    @Override
    public int hashCode() {
        return IOrdersBucket.hash(price, asOrdersArray());
    }

    private Order[] asOrdersArray() {
        int ptr = head;
        Order[] result = new Order[realSize];
        for (int i = 0; i < realSize; i++) {
            // fast-forward head pointer until non-empty element found
            // (assume if realSize>0 then at least one order can be found)
            while (queue[ptr] == null) {
                ptr = inc(ptr);
            }
            result[i] = queue[ptr];
            ptr = inc(ptr);
        }
        return result;
    }

    // TODO try in main algos
    private int inc(int p) {
        p++;
        return (p == queue.length) ? 0 : p;
    }


    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof IOrdersBucket)) return false;
        IOrdersBucket other = (IOrdersBucket) o;
        return new EqualsBuilder()
                .append(price, other.getPrice())
                .append(getAllOrders(), other.getAllOrders())
                .isEquals();
    }

}
