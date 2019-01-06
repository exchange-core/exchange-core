package org.openpredict.exchange.rest;

import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.GatewaySymbolSpecification;
import org.openpredict.exchange.beans.L2MarketData;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.rest.events.OrderBookEvent;
import org.openpredict.exchange.rest.events.admin.SymbolUpdateAdminEvent;
import org.openpredict.exchange.rest.events.admin.UserBalanceAdjustmentAdminEvent;
import org.openpredict.exchange.rest.events.admin.UserCreatedAdminEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;


@Service
@Slf4j
public class CommandEventsRouter implements Consumer<OrderCommand> {

    @Autowired
    private GatewayState gatewayState;

    @Autowired
    private WebSocketServer webSocketServer;

    /**
     * TODO put non-latency-critical commands into a queue
     *
     * @param cmd command placeholder
     */
    @Override
    public void accept(OrderCommand cmd) {
        log.debug("EVENT CMD: " + cmd);

        if (cmd.command == OrderCommandType.ADD_USER && cmd.resultCode == CommandResultCode.SUCCESS) {
            UserCreatedAdminEvent apiEvent = UserCreatedAdminEvent.builder().uid(cmd.uid).build();
            webSocketServer.broadcast(apiEvent);
        }

        if (cmd.command == OrderCommandType.BALANCE_ADJUSTMENT && cmd.resultCode == CommandResultCode.SUCCESS) {
            UserBalanceAdjustmentAdminEvent apiEvent = UserBalanceAdjustmentAdminEvent.builder()
                    .uid(cmd.uid)
                    .transactionId(cmd.orderId)
                    .amount(cmd.price)
                    .balance(cmd.size)
                    .build();
            webSocketServer.broadcast(apiEvent);
        }

        if (cmd.command == OrderCommandType.ADD_SYMBOL && cmd.resultCode == CommandResultCode.SUCCESS) {
            GatewaySymbolSpecification spec = gatewayState.activateSymbol(cmd.symbol);
            // TODO send ADD_SYMBOL_SUCCESS and SYMBOL_UPDATE events

            SymbolUpdateAdminEvent apiEvent = SymbolUpdateAdminEvent.builder()
                    .symbolId(cmd.symbol)
                    .symbolName(spec.symbolName)
                    .priceStep(spec.priceStep)
                    .priceScale(spec.priceScale)
                    .lotSize(spec.lotSize)
                    .depositBuy(cmd.price)
                    .depositSell(cmd.uid)
                    .priceLowLimit(cmd.orderId)
                    .priceHighLimit(cmd.size)
                    .build();

            webSocketServer.broadcast(apiEvent);
        }


        if (cmd.marketData != null) {
            log.debug("MARKET DATA: " + cmd.marketData.dumpOrderBook());

            L2MarketData marketData = cmd.marketData;
            OrderBookEvent orderBook = new OrderBookEvent(
                    "UNKNOWN",
                    marketData.timestamp,
                    marketData.getAskPricesCopy(),
                    marketData.getAskVolumesCopy(),
                    marketData.getBidPricesCopy(),
                    marketData.getBidVolumesCopy()
            );

            webSocketServer.broadcast(orderBook);
        }
        cmd.processMatherEvents(evt -> {
            log.debug("INTERNAL EVENT: " + evt);
        });

    }
}
