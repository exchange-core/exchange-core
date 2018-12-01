package org.openpredict.exchange.beans.cmd;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.MatcherTradeEvent;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.OrderType;

import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.openpredict.exchange.beans.cmd.OrderCommandType.*;
import static org.openpredict.exchange.rdma.RdmaApiConstants.*;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Slf4j
public class OrderCommand {

    public OrderCommandType command;
    public byte subCommandCode;

    public long orderId;
    public int symbol;
    public long price;  // optional for move
    public long size;

    // required for PLACE_ORDER only;
    public OrderAction action;
    public OrderType orderType;

    public long uid;

    public long timestamp;

    public int userCookie;

    // ---- false sharing section ------

    public long eventsGroup;

    // result code of command execution - can also be used for saving intermediate state
    public CommandResultCode resultCode;

    public MatcherTradeEvent matcherEvent;

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

        for (MatcherTradeEvent event : events) {
            MatcherTradeEvent copy = event.copy();
            copy.nextEvent = newCmd.matcherEvent;
            newCmd.matcherEvent = copy;
        }

        return newCmd;
    }

    /**
     * @return binary representation (64 bytes array) of the command
     */
    public long[] toBinary() {

        long[] buffer = new long[8];
        buffer[CMD_HEADER] = command.getCode() + (subCommandCode << 8) + ((long) symbol << 32);
        buffer[CMD_TIMESTAMP] = timestamp;
        buffer[CMD_UID] = uid;
        buffer[CMD_ORDER_ID] = orderId;
        buffer[CMD_PRICE] = price;
        buffer[CMD_SIZE] = size;
        if (command == PLACE_ORDER) {
            buffer[CMD_PLACEORDER_FLAGS] = action.getCode() + (orderType.getCode() << 8) + ((long) userCookie << 32);
        }

        return buffer;
    }

    public void readFromLongBuffer(LongBuffer longRcvBuffer) {

        log.debug(">>> {}", longRcvBuffer);

        final long headerWord = longRcvBuffer.get(CMD_HEADER);
        byte cmdCode = (byte) (headerWord & 0x7f);
        log.debug("cmdCode={}", cmdCode);
        final OrderCommandType commandType = OrderCommandType.valueOf(cmdCode);
        log.debug("commandType={}", commandType);

        this.command = commandType;
        this.symbol = (int) ((headerWord >> 32) & 0x7fff);
        this.resultCode = CommandResultCode.NEW;
        this.timestamp = longRcvBuffer.get(CMD_TIMESTAMP);
        this.uid = longRcvBuffer.get(CMD_UID);

        log.debug("symbol={} timestamp={} uid={}", symbol, timestamp, uid);


        if (commandType == OrderCommandType.PLACE_ORDER) {
            this.orderId = longRcvBuffer.get(CMD_ORDER_ID);
            this.price = longRcvBuffer.get(CMD_PRICE);
            this.size = longRcvBuffer.get(CMD_SIZE);
            long placeOrderFlags = longRcvBuffer.get(CMD_PLACEORDER_FLAGS);
            this.action = OrderAction.valueOf((byte) ((placeOrderFlags >> 8) & 0x7f));
            this.orderType = OrderType.valueOf((byte) ((placeOrderFlags) & 0x7f));
            this.userCookie = (int) ((placeOrderFlags >> 32));

        } else if (commandType == OrderCommandType.MOVE_ORDER) {
            this.orderId = longRcvBuffer.get(CMD_ORDER_ID);
            this.price = longRcvBuffer.get(CMD_PRICE);
            this.size = longRcvBuffer.get(CMD_SIZE);

        } else if (commandType == OrderCommandType.CANCEL_ORDER) {
            this.orderId = longRcvBuffer.get(CMD_ORDER_ID);

        } else if (commandType == OrderCommandType.ADD_USER) {
            //

        } else if (commandType == OrderCommandType.BALANCE_ADJUSTMENT) {
            this.price = longRcvBuffer.get(CMD_PRICE);

        } else if (commandType == OrderCommandType.SYMBOL_COMMANDS) {

            byte subCommandCode = (byte) ((headerWord >> 8) & 0x7f);
            this.subCommandCode = subCommandCode;

            log.debug("subCommandCode={}", subCommandCode);

            SymbolCommandSubType subCommand = SymbolCommandSubType.valueOf(subCommandCode);
            if (subCommand == SymbolCommandSubType.ADD_SYMBOL) {
                this.price = longRcvBuffer.get(CMD_PRICE);
            } else {
                // TODO Implement
                throw new UnsupportedOperationException("Not supported sub-command: " + subCommand);
            }

        } else {
            throw new UnsupportedOperationException("Not supported command: " + commandType);
        }

        log.debug("this.command={}", this.command);

    }


}
