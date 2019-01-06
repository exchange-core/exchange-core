package org.openpredict.exchange.beans.cmd;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.openpredict.exchange.beans.L2MarketData;
import org.openpredict.exchange.beans.MatcherTradeEvent;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.OrderType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.openpredict.exchange.beans.cmd.OrderCommandType.*;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class OrderCommand {

    public OrderCommandType command;

    public long orderId;
    public int symbol;
    public long price;  // optional for move
    public long size;

    // required for PLACE_ORDER only;
    public OrderAction action;
    public OrderType orderType;

    public long uid;

    public long timestamp;

    // ---- false sharing section ------

    public long eventsGroup;

    // result code of command execution - can also be used for saving intermediate state
    public CommandResultCode resultCode;

    // trade events chain
    public MatcherTradeEvent matcherEvent;

    // optional market data
    public L2MarketData marketData;

    // sequence of last available for this command
    //public long matcherEventSequence;
    // ---- potential false sharing section ------

    public static OrderCommand limitOrder(long orderId, int uid, long price, long size, OrderAction action) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = PLACE_ORDER;
        cmd.orderId = orderId;
        cmd.uid = uid;
        cmd.price = price;
        cmd.size = size;
        cmd.action = action;
        cmd.orderType = OrderType.LIMIT;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    public static OrderCommand marketOrder(long orderId, int uid, long size, OrderAction action) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = PLACE_ORDER;
        cmd.orderId = orderId;
        cmd.uid = uid;
        cmd.price = 0;
        cmd.size = size;
        cmd.action = action;
        cmd.orderType = OrderType.MARKET;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }


    public static OrderCommand cancel(long orderId, int uid) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = CANCEL_ORDER;
        cmd.orderId = orderId;
        cmd.uid = uid;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    public static OrderCommand update(long orderId, int uid, long price, long size) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = MOVE_ORDER;
        cmd.orderId = orderId;
        cmd.uid = uid;
        cmd.price = price;
        cmd.size = size;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    /**
     * Handles full MatcherTradeEvent chain, without removing/revoking them
     *
     * @param handler - MatcherTradeEvent handler
     */
    public void processMatherEvents(Consumer<MatcherTradeEvent> handler) {
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
     * @return
     */
    public List<MatcherTradeEvent> extractEvents() {
        List<MatcherTradeEvent> list = new ArrayList<>();
        processMatherEvents(list::add);
        return Lists.reverse(list);
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
     * @param cmd2
     */
    public void writeTo(OrderCommand cmd2) {
        cmd2.command = this.command;
        cmd2.orderId = this.orderId;
        cmd2.symbol = this.symbol;
        cmd2.uid = this.uid;
        cmd2.timestamp = this.timestamp;

        cmd2.price = this.price;
        cmd2.size = this.size;
        cmd2.action = this.action;
        cmd2.orderType = this.orderType;
    }

    public OrderCommand copy() {

        OrderCommand newCmd = new OrderCommand();
        writeTo(newCmd);
        newCmd.resultCode = this.resultCode;

        List<MatcherTradeEvent> events = extractEvents();

//        System.out.println(">>> events: " + events);
        for (MatcherTradeEvent event : events) {
            MatcherTradeEvent copy = event.copy();
            copy.nextEvent = newCmd.matcherEvent;
            newCmd.matcherEvent = copy;
//            System.out.println(">>> newCmd.matcherEvent: " + newCmd.matcherEvent);
        }

//        System.out.println(">>> newCmd: " + newCmd);
        return newCmd;
    }

}
