package org.openpredict.exchange.rest;

import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.core.ExchangeCore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AsyncAdminInterface {

    @Autowired
    private ExchangeCore exchangeCore;


    public void createUser(int ticket, long userId) {

        exchangeCore.getRingBuffer().publishEvent(((cmd, seq) -> {
            cmd.command = OrderCommandType.ADD_USER;
            cmd.orderId = -1;
            cmd.symbol = -1;
            cmd.uid = userId;
            cmd.timestamp = System.currentTimeMillis();
            cmd.resultCode = CommandResultCode.NEW;
            cmd.userCookie = ticket;
        }));

    }

    public void balanceAdjustment(int ticket, long userId, long transactionId, long longAmount) {

        exchangeCore.getRingBuffer().publishEvent(((cmd, seq) -> {
            cmd.command = OrderCommandType.BALANCE_ADJUSTMENT;
            cmd.orderId = transactionId;
            cmd.symbol = -1;
            cmd.uid = userId;
            cmd.price = longAmount;
            cmd.size = 0;
            cmd.timestamp = System.currentTimeMillis();
            cmd.resultCode = CommandResultCode.NEW;
            cmd.userCookie = ticket;
        }));

    }

    public void orderBookRequest(int ticket, int symbolId) {

        exchangeCore.getRingBuffer().publishEvent(((cmd, seq) -> {
            cmd.command = OrderCommandType.ORDER_BOOK_REQUEST;
            cmd.orderId = -1;
            cmd.symbol = symbolId;
            cmd.uid = -1;
            cmd.timestamp = System.currentTimeMillis();
            cmd.resultCode = CommandResultCode.NEW;
            cmd.userCookie = ticket;
        }));

    }


    public void addSymbol(int ticket, int symbolId, long depositBuy, long depositSell, long priceLowLimit, long priceHighLimit) {
        exchangeCore.getRingBuffer().publishEvent(((cmd, seq) -> {
            cmd.command = OrderCommandType.ADD_SYMBOL;
            cmd.symbol = symbolId;

            cmd.price = depositBuy;
            cmd.uid = depositSell;
            cmd.orderId = priceLowLimit;
            cmd.size = priceHighLimit;

            cmd.timestamp = System.currentTimeMillis();
            cmd.resultCode = CommandResultCode.NEW;
            cmd.userCookie = ticket;
        }));
    }
}
