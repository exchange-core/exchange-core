package org.openpredict.exchange.core;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.wire.Wire;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.openpredict.exchange.beans.api.*;
import org.openpredict.exchange.beans.api.reports.ReportQuery;
import org.openpredict.exchange.beans.api.reports.ReportResult;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.core.orderbook.OrderBookEventsHelper;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Stream;

@Slf4j
public final class ExchangeApi {

    private final RingBuffer<OrderCommand> ringBuffer;

    // promises cache (TODO can be changed to queue)
    private final LongObjectHashMap<Consumer<OrderCommand>> promises = new LongObjectHashMap<>();

    public ExchangeApi(RingBuffer<OrderCommand> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void processResult(final long seq, final OrderCommand cmd) {
        final Consumer<OrderCommand> consumer = promises.remove(seq);
        if (consumer != null) {
            consumer.accept(cmd);
        }
    }

    public void submitCommand(ApiCommand cmd) {
        //log.debug("{}", cmd);

        // TODO benchmark instanceof performance

        if (cmd instanceof ApiMoveOrder) {
            ringBuffer.publishEvent(MOVE_ORDER_TRANSLATOR, (ApiMoveOrder) cmd);
        } else if (cmd instanceof ApiPlaceOrder) {
            ringBuffer.publishEvent(NEW_ORDER_TRANSLATOR, (ApiPlaceOrder) cmd);
        } else if (cmd instanceof ApiCancelOrder) {
            ringBuffer.publishEvent(CANCEL_ORDER_TRANSLATOR, (ApiCancelOrder) cmd);
        } else if (cmd instanceof ApiOrderBookRequest) {
            ringBuffer.publishEvent(ORDER_BOOK_REQUEST_TRANSLATOR, (ApiOrderBookRequest) cmd);
        } else if (cmd instanceof ApiAddUser) {
            ringBuffer.publishEvent(ADD_USER_TRANSLATOR, (ApiAddUser) cmd);
        } else if (cmd instanceof ApiAdjustUserBalance) {
            ringBuffer.publishEvent(ADJUST_USER_BALANCE_TRANSLATOR, (ApiAdjustUserBalance) cmd);
        } else if (cmd instanceof ApiBinaryDataCommand) {
            publishBinaryData((ApiBinaryDataCommand) cmd, seq -> {
            });
        } else if (cmd instanceof ApiPersistState) {
            publishPersistCmd((ApiPersistState) cmd);
        } else if (cmd instanceof ApiReset) {
            ringBuffer.publishEvent(RESET_TRANSLATOR, (ApiReset) cmd);
        } else if (cmd instanceof ApiNoOp) {
            ringBuffer.publishEvent(NOOP_TRANSLATOR, (ApiNoOp) cmd);
        } else {
            throw new IllegalArgumentException("Unsupported command type: " + cmd.getClass().getSimpleName());
        }
    }

    public <R> Future<R> submitBinaryCommandAsync(
            final WriteBytesMarshallable data,
            final int transferId,
            final Function<OrderCommand, R> translator) {

        final CompletableFuture<R> future = new CompletableFuture<>();

        publishBinaryData(
                ApiBinaryDataCommand.builder().data(data).transferId(transferId).build(),
                seq -> promises.put(seq, orderCommand -> future.complete(translator.apply(orderCommand))));

        return future;
    }

    public <Q extends ReportQuery<R>, R extends ReportResult> Future<R> processReport(final Q query, final int transferId) {
        return submitBinaryCommandAsync(
                query,
                transferId,
                cmd -> {
                    final Stream<BytesIn> sections = OrderBookEventsHelper.deserializeEvents(cmd.matcherEvent).values().stream().map(Wire::bytes);
                    return query.getResultBuilder().apply(sections);
                });
    }

    private void publishBinaryData(final ApiBinaryDataCommand apiCmd, final LongConsumer endSeqConsumer) {

        final int longsPerMessage = 5;
        long[] longArray = Utils.bytesToLongArray(BinaryCommandsProcessor.serializeObject(apiCmd.data), longsPerMessage);

        int i = 0;
        int n = longArray.length / longsPerMessage;
        long highSeq = ringBuffer.next(n);
        long lowSeq = highSeq - n + 1;

//        log.debug("longArray[{}] n={} seq={}..{}", longArray.length, n, lowSeq, highSeq);

        try {
            for (long seq = lowSeq; seq <= highSeq; seq++) {

                OrderCommand cmd = ringBuffer.get(seq);
                cmd.command = OrderCommandType.BINARY_DATA;
                cmd.userCookie = apiCmd.transferId;
                cmd.symbol = seq == highSeq ? -1 : 0;

                cmd.orderId = longArray[i];
                cmd.price = longArray[i + 1];
                cmd.reserveBidPrice = longArray[i + 2];
                cmd.size = longArray[i + 3];
                cmd.uid = longArray[i + 4];

                cmd.timestamp = apiCmd.timestamp;
                cmd.resultCode = CommandResultCode.NEW;

//                log.debug("seq={} cmd.size={} data={}", seq, cmd.size, cmd.price);

                i += longsPerMessage;
            }
        } catch (final Exception ex) {
            log.error("Binary commands processing exception: ", ex);

        } finally {
            endSeqConsumer.accept(highSeq);
            ringBuffer.publish(lowSeq, highSeq);
        }
    }

    private void publishPersistCmd(final ApiPersistState api) {

        long secondSeq = ringBuffer.next(2);
        long firstSeq = secondSeq - 1;

        try {
            // will be ignored by risk handlers, but processed by matching engine
            final OrderCommand cmdMatching = ringBuffer.get(firstSeq);
            cmdMatching.command = OrderCommandType.PERSIST_STATE_MATCHING;
            cmdMatching.orderId = api.dumpId;
            cmdMatching.symbol = -1;
            cmdMatching.uid = 0;
            cmdMatching.price = 0;
            cmdMatching.timestamp = api.timestamp;
            cmdMatching.resultCode = CommandResultCode.NEW;

            //log.debug("seq={} cmd.command={} data={}", firstSeq, cmdMatching.command, cmdMatching.price);

            // sequential command will make risk handler to create snapshot
            final OrderCommand cmdRisk = ringBuffer.get(secondSeq);
            cmdRisk.command = OrderCommandType.PERSIST_STATE_RISK;
            cmdRisk.orderId = api.dumpId;
            cmdRisk.symbol = -1;
            cmdRisk.uid = 0;
            cmdRisk.price = 0;
            cmdRisk.timestamp = api.timestamp;
            cmdRisk.resultCode = CommandResultCode.NEW;

            //log.debug("seq={} cmd.command={} data={}", firstSeq, cmdMatching.command, cmdMatching.price);

            // short delay to reduce probability of batching both commands together in R1
        } finally {
            ringBuffer.publish(firstSeq, secondSeq);
        }
    }


    private static final EventTranslatorOneArg<OrderCommand, ApiPlaceOrder> NEW_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.PLACE_ORDER;
        cmd.price = api.price;
        cmd.reserveBidPrice = api.reservePrice;
        cmd.size = api.size;
        cmd.orderId = api.id;
        cmd.timestamp = api.timestamp;
        cmd.action = api.action;
        cmd.orderType = api.orderType;
        cmd.symbol = api.symbol;
        cmd.uid = api.uid;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiMoveOrder> MOVE_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.MOVE_ORDER;
        cmd.price = api.newPrice;
        //cmd.price2
        cmd.orderId = api.id;
        cmd.symbol = api.symbol;
        cmd.uid = api.uid;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiCancelOrder> CANCEL_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.CANCEL_ORDER;
        cmd.orderId = api.id;
        cmd.price = -1;
        cmd.size = -1;
        cmd.symbol = api.symbol;
        cmd.uid = api.uid;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiOrderBookRequest> ORDER_BOOK_REQUEST_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.ORDER_BOOK_REQUEST;
        cmd.orderId = -1;
        cmd.symbol = api.symbol;
        cmd.price = -1;
        cmd.size = api.size;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiAddUser> ADD_USER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.ADD_USER;
        cmd.orderId = -1;
        cmd.symbol = -1;
        cmd.uid = api.uid;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiAdjustUserBalance> ADJUST_USER_BALANCE_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.BALANCE_ADJUSTMENT;
        cmd.orderId = api.transactionId;
        cmd.symbol = api.currency;
        cmd.uid = api.uid;
        cmd.price = api.amount;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiReset> RESET_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.RESET;
        cmd.orderId = -1;
        cmd.symbol = -1;
        cmd.uid = -1;
        cmd.price = -1;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiNoOp> NOOP_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.NOP;
        cmd.orderId = -1;
        cmd.symbol = -1;
        cmd.uid = -1;
        cmd.price = -1;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };
}
