package org.openpredict.exchange.rest;

import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.OrderType;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.core.ExchangeCore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AsyncTradingInterface {

    @Autowired
    private ExchangeCore exchangeCore;


    public void placeNewOrder(
            int ticket,
            long price,
            long size,
            OrderAction action,
            OrderType orderType,
            int symbol,
            long uid) {

        exchangeCore.getRingBuffer().publishEvent((cmd, seq) -> {
            cmd.command = OrderCommandType.PLACE_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.price = price;
            cmd.size = size;
            cmd.orderId = seq;
            cmd.timestamp = System.currentTimeMillis();
            cmd.action = action;
            cmd.orderType = orderType;
            cmd.symbol = symbol;
            cmd.uid = uid;
            cmd.userCookie = ticket;
        });
    }

    public void moveOrder(
            int ticket,
            long price,
            long size,
            long orderId,
            int symbol,
            long uid) {

        exchangeCore.getRingBuffer().publishEvent((cmd, seq) -> {
            cmd.command = OrderCommandType.MOVE_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.price = price;
            cmd.size = size;
            cmd.orderId = orderId;
            cmd.timestamp = System.currentTimeMillis();
            cmd.symbol = symbol;
            cmd.uid = uid;
            cmd.userCookie = ticket;
        });
    }

    public void cancelOrder(
            int ticket,
            long orderId,
            int symbol,
            long uid) {

        exchangeCore.getRingBuffer().publishEvent((cmd, seq) -> {
            cmd.command = OrderCommandType.CANCEL_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.orderId = orderId;
            cmd.timestamp = System.currentTimeMillis();
            cmd.symbol = symbol;
            cmd.uid = uid;
            cmd.userCookie = ticket;
        });

    }
}
