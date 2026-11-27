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
package exchange.core2.core.common.cmd;

import exchange.core2.core.common.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

public final class OrderCommand implements IOrder {

    public static final int LEGACY_WITHDRAWAL_REPLAY = 1 << 1;

    public OrderCommandType command;

    // order UUID: the most and the least significant bits;
    // other commands keep their long argument in orderIdLo (transaction id, dump id, binary frame word 0, mode)
    public long orderIdHi;
    public long orderIdLo;

    public int symbol;

    public long price;

    public long size;

    public long orderTakerFee = -1;

    public long orderMakerFee = -1;

    // new orders INPUT - reserved price for fast moves of GTC bid orders in exchange mode
    public long reserveBidPrice;

    // required for PLACE_ORDER only;
    // for CANCEL/MOVE contains original order action (filled by orderbook)
    public OrderAction action;

    public OrderType orderType;

    // user UUID: the most and the least significant bits
    public long uidHi;
    public long uidLo;

    public long timestamp;

    public int userCookie;

    // filled by grouping processor:

    public long eventsGroup;
    public int serviceFlags;

    // result code of command execution - can also be used for saving intermediate state
    public CommandResultCode resultCode;

    // trade events chain
    public MatcherTradeEvent matcherEvent;

    // optional market data
    public L2MarketData marketData;

    // sequence of last available for this command
    //public long matcherEventSequence;
    // ---- potential false sharing section ------

    public static OrderCommand newOrder(OrderType orderType, UUID orderId, UUID uid, long price, long reserveBidPrice, long size, OrderAction action) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = OrderCommandType.PLACE_ORDER;
        cmd.setOrderId(orderId);
        cmd.setUid(uid);
        cmd.price = price;
        cmd.reserveBidPrice = reserveBidPrice;
        cmd.size = size;
        cmd.action = action;
        cmd.orderType = orderType;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    public static OrderCommand cancel(UUID orderId, UUID uid) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = OrderCommandType.CANCEL_ORDER;
        cmd.setOrderId(orderId);
        cmd.setUid(uid);
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    public static OrderCommand reduce(UUID orderId, UUID uid, long reduceSize) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = OrderCommandType.REDUCE_ORDER;
        cmd.setOrderId(orderId);
        cmd.setUid(uid);
        cmd.size = reduceSize;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    public static OrderCommand update(UUID orderId, UUID uid, long price) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = OrderCommandType.MOVE_ORDER;
        cmd.setOrderId(orderId);
        cmd.setUid(uid);
        cmd.price = price;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    /**
     * Handles full MatcherTradeEvent chain, without removing/revoking them
     *
     * @param handler - MatcherTradeEvent handler
     */
    public void processMatcherEvents(Consumer<MatcherTradeEvent> handler) {
        MatcherTradeEvent mte = this.matcherEvent;
        while (mte != null) {
            handler.accept(mte);
            mte = mte.nextEvent;
        }
    }

    /**
     * Produces garbage
     * For testing only !!!
     *
     * @return list of events
     */
    public List<MatcherTradeEvent> extractEvents() {
        List<MatcherTradeEvent> list = new ArrayList<>();
        processMatcherEvents(list::add);
        return list;
    }

    // Traverse and remove:
//    private void cleanMatcherEvents() {
//        MatcherTradeEvent ev = this.matcherEvent;
//        this.matcherEvent = null;
//        while (ev != null) {
//            MatcherTradeEvent tmp = ev;
//            ev = ev.nextEvent;
//            tmp.nextEvent = null;
//        }
//    }
//


    /**
     * Write only command data, not status or events
     *
     * @param cmd2 command to overwrite to
     */
    public void writeTo(OrderCommand cmd2) {
        cmd2.command = this.command;
        cmd2.orderIdHi = this.orderIdHi;
        cmd2.orderIdLo = this.orderIdLo;
        cmd2.symbol = this.symbol;
        cmd2.uidHi = this.uidHi;
        cmd2.uidLo = this.uidLo;
        cmd2.timestamp = this.timestamp;

        cmd2.reserveBidPrice = this.reserveBidPrice;
        cmd2.price = this.price;
        cmd2.size = this.size;
        cmd2.action = this.action;
        cmd2.orderType = this.orderType;
        cmd2.orderTakerFee = this.orderTakerFee;
        cmd2.orderMakerFee = this.orderMakerFee;
    }

    // A move, cancellation or reduction must settle against the original order.
    public void copySettlementFields(IOrder restingOrder) {
        action = restingOrder.getAction();
        reserveBidPrice = restingOrder.getReserveBidPrice();
        orderTakerFee = restingOrder.getOrderTakerFee();
        orderMakerFee = restingOrder.getOrderMakerFee();
    }

    /**
     * Copies the command and its response data, preserving matcher event order.
     *
     * @return an independent copy of this command
     */
    public OrderCommand copy() {
        final OrderCommand copy = new OrderCommand();
        writeTo(copy);
        copy.userCookie = userCookie;
        copy.eventsGroup = eventsGroup;
        copy.serviceFlags = serviceFlags;
        copy.resultCode = resultCode;

        MatcherTradeEvent tail = null;
        for (MatcherTradeEvent event = matcherEvent; event != null; event = event.nextEvent) {
            final MatcherTradeEvent eventCopy = event.copy();
            if (tail == null) {
                copy.matcherEvent = eventCopy;
            } else {
                tail.nextEvent = eventCopy;
            }
            tail = eventCopy;
        }

        if (marketData != null) {
            copy.marketData = marketData.copy();
        }

        return copy;
    }

    @Override
    public long getFilled() {
        return 0;
    }

    @Override
    public int stateHash() {
        throw new UnsupportedOperationException("Command does not represents state");
    }

    public OrderCommand() {
    }

    public OrderCommand(
            OrderCommandType command,
            long orderIdHi,
            long orderIdLo,
            int symbol,
            long price,
            long size,
            long orderTakerFee,
            long orderMakerFee,
            long reserveBidPrice,
            OrderAction action,
            OrderType orderType,
            long uidHi,
            long uidLo,
            long timestamp,
            int userCookie,
            long eventsGroup,
            int serviceFlags,
            CommandResultCode resultCode,
            MatcherTradeEvent matcherEvent,
            L2MarketData marketData) {
        this.command = command;
        this.orderIdHi = orderIdHi;
        this.orderIdLo = orderIdLo;
        this.symbol = symbol;
        this.price = price;
        this.size = size;
        this.orderTakerFee = orderTakerFee;
        this.orderMakerFee = orderMakerFee;
        this.reserveBidPrice = reserveBidPrice;
        this.action = action;
        this.orderType = orderType;
        this.uidHi = uidHi;
        this.uidLo = uidLo;
        this.timestamp = timestamp;
        this.userCookie = userCookie;
        this.eventsGroup = eventsGroup;
        this.serviceFlags = serviceFlags;
        this.resultCode = resultCode;
        this.matcherEvent = matcherEvent;
        this.marketData = marketData;
    }

    public static OrderCommandBuilder builder() {
        return new OrderCommandBuilder();
    }

    @Override
    public String toString() {
        return "OrderCommand(command=" + this.command
                + ", orderId=" + this.getOrderId()
                + ", symbol=" + this.symbol
                + ", price=" + this.getPrice()
                + ", size=" + this.getSize()
                + ", orderTakerFee=" + this.getOrderTakerFee()
                + ", orderMakerFee=" + this.getOrderMakerFee()
                + ", reserveBidPrice=" + this.getReserveBidPrice()
                + ", action=" + this.getAction()
                + ", orderType=" + this.orderType
                + ", uid=" + this.getUid()
                + ", timestamp=" + this.getTimestamp()
                + ", userCookie=" + this.userCookie
                + ", eventsGroup=" + this.eventsGroup
                + ", serviceFlags=" + this.serviceFlags
                + ", resultCode=" + this.resultCode
                + ", matcherEvent=" + this.matcherEvent
                + ", marketData=" + this.marketData + ")";
    }

    @Override
    public long getOrderIdHi() {
        return this.orderIdHi;
    }

    @Override
    public long getOrderIdLo() {
        return this.orderIdLo;
    }

    public void setOrderId(UUID orderId) {
        this.orderIdHi = orderId.getMostSignificantBits();
        this.orderIdLo = orderId.getLeastSignificantBits();
    }

    public long getPrice() {
        return this.price;
    }

    public long getSize() {
        return this.size;
    }

    @Override
    public long getOrderTakerFee() {
        return orderTakerFee;
    }

    @Override
    public long getOrderMakerFee() {
        return orderMakerFee;
    }

    public long getReserveBidPrice() {
        return this.reserveBidPrice;
    }

    public OrderAction getAction() {
        return this.action;
    }

    @Override
    public long getUidHi() {
        return this.uidHi;
    }

    @Override
    public long getUidLo() {
        return this.uidLo;
    }

    public void setUid(UUID uid) {
        this.uidHi = uid.getMostSignificantBits();
        this.uidLo = uid.getLeastSignificantBits();
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public static class OrderCommandBuilder {
        private OrderCommandType command;
        private long orderIdHi;
        private long orderIdLo;
        private int symbol;
        private long price;
        private long size;
        private long orderTakerFee = -1;
        private long orderMakerFee = -1;
        private long reserveBidPrice;
        private OrderAction action;
        private OrderType orderType;
        private long uidHi;
        private long uidLo;
        private long timestamp;
        private int userCookie;
        private long eventsGroup;
        private int serviceFlags;
        private CommandResultCode resultCode;
        private MatcherTradeEvent matcherEvent;
        private L2MarketData marketData;

        OrderCommandBuilder() {
        }

        public OrderCommandBuilder command(OrderCommandType command) {
            this.command = command;
            return this;
        }

        public OrderCommandBuilder orderId(UUID orderId) {
            this.orderIdHi = orderId.getMostSignificantBits();
            this.orderIdLo = orderId.getLeastSignificantBits();
            return this;
        }

        public OrderCommandBuilder symbol(int symbol) {
            this.symbol = symbol;
            return this;
        }

        public OrderCommandBuilder price(long price) {
            this.price = price;
            return this;
        }

        public OrderCommandBuilder size(long size) {
            this.size = size;
            return this;
        }

        public OrderCommandBuilder orderTakerFee(long orderTakerFee) {
            this.orderTakerFee = orderTakerFee;
            return this;
        }

        public OrderCommandBuilder orderMakerFee(long orderMakerFee) {
            this.orderMakerFee = orderMakerFee;
            return this;
        }

        public OrderCommandBuilder reserveBidPrice(long reserveBidPrice) {
            this.reserveBidPrice = reserveBidPrice;
            return this;
        }

        public OrderCommandBuilder action(OrderAction action) {
            this.action = action;
            return this;
        }

        public OrderCommandBuilder orderType(OrderType orderType) {
            this.orderType = orderType;
            return this;
        }

        public OrderCommandBuilder uid(UUID uid) {
            this.uidHi = uid.getMostSignificantBits();
            this.uidLo = uid.getLeastSignificantBits();
            return this;
        }

        public OrderCommandBuilder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public OrderCommandBuilder userCookie(int userCookie) {
            this.userCookie = userCookie;
            return this;
        }

        public OrderCommandBuilder eventsGroup(long eventsGroup) {
            this.eventsGroup = eventsGroup;
            return this;
        }

        public OrderCommandBuilder serviceFlags(int serviceFlags) {
            this.serviceFlags = serviceFlags;
            return this;
        }

        public OrderCommandBuilder resultCode(CommandResultCode resultCode) {
            this.resultCode = resultCode;
            return this;
        }

        public OrderCommandBuilder matcherEvent(MatcherTradeEvent matcherEvent) {
            this.matcherEvent = matcherEvent;
            return this;
        }

        public OrderCommandBuilder marketData(L2MarketData marketData) {
            this.marketData = marketData;
            return this;
        }

        public OrderCommand build() {
            return new OrderCommand(
                    this.command,
                    this.orderIdHi,
                    this.orderIdLo,
                    this.symbol,
                    this.price,
                    this.size,
                    this.orderTakerFee,
                    this.orderMakerFee,
                    this.reserveBidPrice,
                    this.action,
                    this.orderType,
                    this.uidHi,
                    this.uidLo,
                    this.timestamp,
                    this.userCookie,
                    this.eventsGroup,
                    this.serviceFlags,
                    this.resultCode,
                    this.matcherEvent,
                    this.marketData);
        }

        @Override
        public String toString() {
            return "OrderCommand.OrderCommandBuilder(command=" + this.command
                    + ", orderId=" + new UUID(this.orderIdHi, this.orderIdLo)
                    + ", symbol=" + this.symbol
                    + ", price=" + this.price
                    + ", size=" + this.size
                    + ", orderTakerFee=" + this.orderTakerFee
                    + ", orderMakerFee=" + this.orderMakerFee
                    + ", reserveBidPrice=" + this.reserveBidPrice
                    + ", action=" + this.action
                    + ", orderType=" + this.orderType
                    + ", uid=" + new UUID(this.uidHi, this.uidLo)
                    + ", timestamp=" + this.timestamp
                    + ", userCookie=" + this.userCookie
                    + ", eventsGroup=" + this.eventsGroup
                    + ", serviceFlags=" + this.serviceFlags
                    + ", resultCode=" + this.resultCode
                    + ", matcherEvent=" + this.matcherEvent
                    + ", marketData=" + this.marketData + ")";
        }
    }
}
