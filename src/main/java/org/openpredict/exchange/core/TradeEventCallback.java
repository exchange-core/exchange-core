package org.openpredict.exchange.core;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.openpredict.exchange.beans.Order;

import java.util.ArrayList;
import java.util.List;

@FunctionalInterface
public interface TradeEventCallback {

    void submit(Order order, long volume, boolean fullMatch, boolean fullMatchForActiveOrder);

    static void empty(Order order, long volume, boolean fullMatch, boolean fullMatchForActiveOrder) {
    }


    @AllArgsConstructor
    @EqualsAndHashCode
    class TradeEventContainer {
        public Order order;
        public long volume;
        public boolean fullMatch;
        public boolean fullMatchForActiveOrder;
    }

    @EqualsAndHashCode
    class TradeEventCollector {
        private List<TradeEventContainer> list = new ArrayList<>();

        void collect(Order order, long volume, boolean fullMatch, boolean fullMatchForActiveOrder) {
            list.add(new TradeEventContainer(order, volume, fullMatch, fullMatchForActiveOrder));
        }

    }

}
