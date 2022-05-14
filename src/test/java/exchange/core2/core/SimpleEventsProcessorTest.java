package exchange.core2.core;

import exchange.core2.core.common.MatcherEventType;
import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.api.ApiCancelOrder;
import exchange.core2.core.common.api.ApiPlaceOrder;
import exchange.core2.core.common.api.ApiReduceOrder;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.core.Is;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Slf4j
@ExtendWith(MockitoExtension.class)
public final class SimpleEventsProcessorTest {

    private SimpleEventsProcessor processor;

    @Mock
    private IEventsHandler handler;

    @Captor
    private ArgumentCaptor<IEventsHandler.ApiCommandResult> commandResultCaptor;

    @Captor
    private ArgumentCaptor<IEventsHandler.ReduceEvent> reduceEventCaptor;

    @Captor
    private ArgumentCaptor<IEventsHandler.TradeEvent> tradeEventCaptor;

    @Captor
    private ArgumentCaptor<IEventsHandler.RejectEvent> rejectEventCaptor;

    @BeforeEach
    public void before() {
        processor = new SimpleEventsProcessor(handler);
    }

    @Test
    public void shouldHandleSimpleCommand() {

        OrderCommand cmd = sampleCancelCommand();

        processor.accept(cmd, 192837L);

        verify(handler, times(1)).commandResult(commandResultCaptor.capture());
        verify(handler, never()).tradeEvent(any());
        verify(handler, never()).rejectEvent(any());
        verify(handler, never()).reduceEvent(any());

        assertThat(commandResultCaptor.getValue().getCommand(),
                Is.is(ApiCancelOrder.builder().orderId(123L).symbol(3).uid(29851L).build()));
    }

    @Test
    public void shouldHandleWithReduceCommand() {

        OrderCommand cmd = sampleReduceCommand();

        cmd.matcherEvent = MatcherTradeEvent.builder()
                .eventType(MatcherEventType.REDUCE)
                .activeOrderCompleted(true)
                .price(20100L)
                .size(8272L)
                .nextEvent(null)
                .build();

        processor.accept(cmd, 192837L);

        verify(handler, times(1)).commandResult(commandResultCaptor.capture());
        verify(handler, never()).tradeEvent(any());
        verify(handler, never()).rejectEvent(any());
        verify(handler, times(1)).reduceEvent(reduceEventCaptor.capture());

        assertThat(commandResultCaptor.getValue().getCommand(),
                Is.is(ApiReduceOrder.builder().orderId(123L).reduceSize(3200L).symbol(3).uid(29851L).build()));

        assertThat(reduceEventCaptor.getValue().getOrderId(), Is.is(123L));
        assertThat(reduceEventCaptor.getValue().getPrice(), Is.is(20100L));
        assertThat(reduceEventCaptor.getValue().getReducedVolume(), Is.is(8272L));
        assertTrue(reduceEventCaptor.getValue().isOrderCompleted());
    }

    @Test
    public void shouldHandleWithSingleTrade() {

        OrderCommand cmd = samplePlaceOrderCommand();

        cmd.matcherEvent = MatcherTradeEvent.builder()
                .eventType(MatcherEventType.TRADE)
                .activeOrderCompleted(false)
                .matchedOrderId(276810L)
                .matchedOrderUid(10332L)
                .matchedOrderCompleted(true)
                .price(20100L)
                .size(8272L)
                .nextEvent(null)
                .build();


        processor.accept(cmd, 192837L);

        verify(handler, times(1)).commandResult(commandResultCaptor.capture());
        verify(handler, never()).rejectEvent(any());
        verify(handler, never()).reduceEvent(any());
        verify(handler, times(1)).tradeEvent(tradeEventCaptor.capture());

        assertThat(commandResultCaptor.getValue().getCommand(),
                Is.is(ApiPlaceOrder.builder()
                        .orderId(123L)
                        .symbol(3)
                        .price(52200L)
                        .size(3200L)
                        .reservePrice(12800L)
                        .action(OrderAction.BID)
                        .orderType(OrderType.IOC)
                        .uid(29851)
                        .userCookie(44188)
                        .build()));

        IEventsHandler.TradeEvent tradeEvent = tradeEventCaptor.getValue();
        assertThat(tradeEvent.getSymbol(), Is.is(3));
        assertThat(tradeEvent.getTotalVolume(), Is.is(8272L));
        assertThat(tradeEvent.getTakerOrderId(), Is.is(123L));
        assertThat(tradeEvent.getTakerUid(), Is.is(29851L));
        assertThat(tradeEvent.getTakerAction(), Is.is(OrderAction.BID));
        assertFalse(tradeEvent.isTakeOrderCompleted());

        List<IEventsHandler.Trade> trades = tradeEvent.getTrades();
        assertThat(trades.size(), Is.is(1));
        IEventsHandler.Trade trade = trades.get(0);

        assertThat(trade.getMakerOrderId(), Is.is(276810L));
        assertThat(trade.getMakerUid(), Is.is(10332L));
        assertTrue(trade.isMakerOrderCompleted());
        assertThat(trade.getPrice(), Is.is(20100L));
        assertThat(trade.getVolume(), Is.is(8272L));
    }


    @Test
    public void shouldHandleWithTwoTrades() {

        OrderCommand cmd = samplePlaceOrderCommand();

        MatcherTradeEvent firstTrade = MatcherTradeEvent.builder()
                .eventType(MatcherEventType.TRADE)
                .activeOrderCompleted(false)
                .matchedOrderId(276810L)
                .matchedOrderUid(10332L)
                .matchedOrderCompleted(true)
                .price(20100L)
                .size(8272L)
                .nextEvent(null)
                .build();

        MatcherTradeEvent secondTrade = MatcherTradeEvent.builder()
                .eventType(MatcherEventType.TRADE)
                .activeOrderCompleted(true)
                .matchedOrderId(100293L)
                .matchedOrderUid(1982L)
                .matchedOrderCompleted(false)
                .price(20110L)
                .size(3121L)
                .nextEvent(null)
                .build();

        cmd.matcherEvent = firstTrade;
        firstTrade.nextEvent = secondTrade;

        processor.accept(cmd, 12981721239L);

        verify(handler, times(1)).commandResult(commandResultCaptor.capture());
        verify(handler, never()).rejectEvent(any());
        verify(handler, never()).reduceEvent(any());
        verify(handler, times(1)).tradeEvent(tradeEventCaptor.capture());

        assertThat(commandResultCaptor.getValue().getCommand(),
                Is.is(ApiPlaceOrder.builder()
                        .orderId(123L)
                        .symbol(3)
                        .price(52200L)
                        .size(3200L)
                        .reservePrice(12800L)
                        .action(OrderAction.BID)
                        .orderType(OrderType.IOC)
                        .uid(29851)
                        .userCookie(44188)
                        .build()));

        // validating first event
        IEventsHandler.TradeEvent tradeEvent = tradeEventCaptor.getAllValues().get(0);
        assertThat(tradeEvent.getSymbol(), Is.is(3));
        assertThat(tradeEvent.getTotalVolume(), Is.is(11393L));
        assertThat(tradeEvent.getTakerOrderId(), Is.is(123L));
        assertThat(tradeEvent.getTakerUid(), Is.is(29851L));
        assertThat(tradeEvent.getTakerAction(), Is.is(OrderAction.BID));
        assertTrue(tradeEvent.isTakeOrderCompleted());

        List<IEventsHandler.Trade> trades = tradeEvent.getTrades();
        assertThat(trades.size(), Is.is(2));

        IEventsHandler.Trade trade = trades.get(0);
        assertThat(trade.getMakerOrderId(), Is.is(276810L));
        assertThat(trade.getMakerUid(), Is.is(10332L));
        assertTrue(trade.isMakerOrderCompleted());
        assertThat(trade.getPrice(), Is.is(20100L));
        assertThat(trade.getVolume(), Is.is(8272L));

        trade = trades.get(1);
        assertThat(trade.getMakerOrderId(), Is.is(100293L));
        assertThat(trade.getMakerUid(), Is.is(1982L));
        assertFalse(trade.isMakerOrderCompleted());
        assertThat(trade.getPrice(), Is.is(20110L));
        assertThat(trade.getVolume(), Is.is(3121L));
    }

    @Test
    public void shouldHandleWithTwoTradesAndReject() {

        OrderCommand cmd = samplePlaceOrderCommand();

        MatcherTradeEvent firstTrade = MatcherTradeEvent.builder()
                .eventType(MatcherEventType.TRADE)
                .activeOrderCompleted(false)
                .matchedOrderId(276810L)
                .matchedOrderUid(10332L)
                .matchedOrderCompleted(true)
                .price(20100L)
                .size(8272L)
                .nextEvent(null)
                .build();

        MatcherTradeEvent secondTrade = MatcherTradeEvent.builder()
                .eventType(MatcherEventType.TRADE)
                .activeOrderCompleted(true)
                .matchedOrderId(100293L)
                .matchedOrderUid(1982L)
                .matchedOrderCompleted(false)
                .price(20110L)
                .size(3121L)
                .nextEvent(null)
                .build();

        MatcherTradeEvent reject = MatcherTradeEvent.builder()
                .eventType(MatcherEventType.REJECT)
                .activeOrderCompleted(true)
                .size(8272L)
                .nextEvent(null)
                .build();

        cmd.matcherEvent = firstTrade;
        firstTrade.nextEvent = secondTrade;
        secondTrade.nextEvent = reject;

        processor.accept(cmd, 12981721239L);

        verify(handler, times(1)).commandResult(commandResultCaptor.capture());
        verify(handler, times(1)).rejectEvent(rejectEventCaptor.capture());
        verify(handler, never()).reduceEvent(any());
        verify(handler, times(1)).tradeEvent(tradeEventCaptor.capture());

        assertThat(commandResultCaptor.getValue().getCommand(),
                Is.is(ApiPlaceOrder.builder()
                        .orderId(123L)
                        .symbol(3)
                        .price(52200L)
                        .size(3200L)
                        .reservePrice(12800L)
                        .action(OrderAction.BID)
                        .orderType(OrderType.IOC)
                        .uid(29851)
                        .userCookie(44188)
                        .build()));

        // validating first event
        IEventsHandler.TradeEvent tradeEvent = tradeEventCaptor.getAllValues().get(0);
        assertThat(tradeEvent.getSymbol(), Is.is(3));
        assertThat(tradeEvent.getTotalVolume(), Is.is(11393L));
        assertThat(tradeEvent.getTakerOrderId(), Is.is(123L));
        assertThat(tradeEvent.getTakerUid(), Is.is(29851L));
        assertThat(tradeEvent.getTakerAction(), Is.is(OrderAction.BID));
        assertTrue(tradeEvent.isTakeOrderCompleted());

        List<IEventsHandler.Trade> trades = tradeEvent.getTrades();
        assertThat(trades.size(), Is.is(2));

        IEventsHandler.Trade trade = trades.get(0);
        assertThat(trade.getMakerOrderId(), Is.is(276810L));
        assertThat(trade.getMakerUid(), Is.is(10332L));
        assertTrue(trade.isMakerOrderCompleted());
        assertThat(trade.getPrice(), Is.is(20100L));
        assertThat(trade.getVolume(), Is.is(8272L));

        trade = trades.get(1);
        assertThat(trade.getMakerOrderId(), Is.is(100293L));
        assertThat(trade.getMakerUid(), Is.is(1982L));
        assertFalse(trade.isMakerOrderCompleted());
        assertThat(trade.getPrice(), Is.is(20110L));
        assertThat(trade.getVolume(), Is.is(3121L));
    }


    @Test
    public void shouldHandlerWithSingleReject() {

        OrderCommand cmd = samplePlaceOrderCommand();

        cmd.matcherEvent = MatcherTradeEvent.builder()
                .eventType(MatcherEventType.REJECT)
                .activeOrderCompleted(true)
                .size(8272L)
                .price(52201L)
                .nextEvent(null)
                .build();

        processor.accept(cmd, 192837L);

        verify(handler, times(1)).commandResult(commandResultCaptor.capture());
        verify(handler, never()).tradeEvent(any());
        verify(handler, never()).reduceEvent(any());
        verify(handler, times(1)).rejectEvent(rejectEventCaptor.capture());

        assertThat(commandResultCaptor.getValue().getCommand(),
                Is.is(ApiPlaceOrder.builder()
                        .orderId(123L)
                        .symbol(3)
                        .price(52200L)
                        .size(3200L)
                        .reservePrice(12800L)
                        .action(OrderAction.BID)
                        .orderType(OrderType.IOC)
                        .uid(29851L)
                        .userCookie(44188)
                        .build()));

        IEventsHandler.RejectEvent rejectEvent = rejectEventCaptor.getValue();
        assertThat(rejectEvent.getSymbol(), Is.is(3));
        assertThat(rejectEvent.getOrderId(), Is.is(123L));
        assertThat(rejectEvent.getRejectedVolume(), Is.is(8272L));
        assertThat(rejectEvent.getPrice(), Is.is(52201L));
        assertThat(rejectEvent.getUid(), Is.is(29851L));
    }


    private OrderCommand sampleCancelCommand() {

        return OrderCommand.builder()
                .command(OrderCommandType.CANCEL_ORDER)
                .orderId(123L)
                .symbol(3)
                .price(12800L)
                .size(3L)
                .reserveBidPrice(12800L)
                .action(OrderAction.BID)
                .orderType(OrderType.GTC)
                .uid(29851L)
                .timestamp(1578930983745201L)
                .userCookie(44188)
                .resultCode(CommandResultCode.MATCHING_INVALID_ORDER_BOOK_ID)
                .matcherEvent(null)
                .marketData(null)
                .build();
    }


    private OrderCommand sampleReduceCommand() {

        return OrderCommand.builder()
                .command(OrderCommandType.REDUCE_ORDER)
                .orderId(123L)
                .symbol(3)
                .price(52200L)
                .size(3200L)
                .reserveBidPrice(12800L)
                .action(OrderAction.BID)
                .orderType(OrderType.GTC)
                .uid(29851L)
                .timestamp(1578930983745201L)
                .userCookie(44188)
                .resultCode(CommandResultCode.SUCCESS)
                .matcherEvent(null)
                .marketData(null)
                .build();
    }

    private OrderCommand samplePlaceOrderCommand() {

        return OrderCommand.builder()
                .command(OrderCommandType.PLACE_ORDER)
                .orderId(123L)
                .symbol(3)
                .price(52200L)
                .size(3200L)
                .reserveBidPrice(12800L)
                .action(OrderAction.BID)
                .orderType(OrderType.IOC)
                .uid(29851L)
                .timestamp(1578930983745201L)
                .userCookie(44188)
                .resultCode(CommandResultCode.SUCCESS)
                .matcherEvent(null)
                .marketData(null)
                .build();
    }

    private void verifyOriginalFields(OrderCommand source, OrderCommand result) {

        assertThat(source.command, Is.is(result.command));
        assertThat(source.orderId, Is.is(result.orderId));
        assertThat(source.symbol, Is.is(result.symbol));
        assertThat(source.price, Is.is(result.price));
        assertThat(source.size, Is.is(result.size));
        assertThat(source.reserveBidPrice, Is.is(result.reserveBidPrice));
        assertThat(source.action, Is.is(result.action));
        assertThat(source.orderType, Is.is(result.orderType));
        assertThat(source.uid, Is.is(result.uid));
        assertThat(source.timestamp, Is.is(result.timestamp));
        assertThat(source.userCookie, Is.is(result.userCookie));
        assertThat(source.resultCode, Is.is(result.resultCode));
    }

}