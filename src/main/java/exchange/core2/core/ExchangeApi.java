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
package exchange.core2.core;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import exchange.core2.core.common.BalanceAdjustmentType;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.api.*;
import exchange.core2.core.common.api.reports.ReportQuery;
import exchange.core2.core.common.api.reports.ReportResult;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.orderbook.OrderBookEventsHelper;
import exchange.core2.core.processors.BinaryCommandsProcessor;
import exchange.core2.core.utils.SerializationUtils;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.wire.Wire;
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;

import java.util.Map;
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
    private final Map<Long, Consumer<OrderCommand>> promises = new ConcurrentHashMap<>();

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
        } else if (cmd instanceof ApiResumeUser) {
            ringBuffer.publishEvent(RESUME_USER_TRANSLATOR, (ApiResumeUser) cmd);
        } else if (cmd instanceof ApiSuspendUser) {
            ringBuffer.publishEvent(SUSPEND_USER_TRANSLATOR, (ApiSuspendUser) cmd);
        } else if (cmd instanceof ApiBinaryDataCommand) {
            publishBinaryData((ApiBinaryDataCommand) cmd, seq -> {
            });
        } else if (cmd instanceof ApiPersistState) {
            publishPersistCmd((ApiPersistState) cmd);
        } else if (cmd instanceof ApiReset) {
            ringBuffer.publishEvent(RESET_TRANSLATOR, (ApiReset) cmd);
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

    public void submitBinaryCommandAsync(
            final WriteBytesMarshallable data,
            final int transferId,
            final Consumer<OrderCommand> consumer,
            final boolean isQuery) {

        publishBinaryData(
                ApiBinaryDataCommand.builder().data(data).transferId(transferId).isQuery(isQuery).build(),
                seq -> promises.put(seq, consumer));
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

    public void publishBinaryData(final ApiBinaryDataCommand apiCmd, final LongConsumer endSeqConsumer) {

        final OrderCommandType cmdType = apiCmd.isQuery ? OrderCommandType.BINARY_DATA_QUERY : OrderCommandType.BINARY_DATA_COMMAND;

        final int longsPerMessage = 5;
        long[] longArray = SerializationUtils.bytesToLongArray(BinaryCommandsProcessor.serializeObject(apiCmd.data), longsPerMessage);

        int i = 0;
        int n = longArray.length / longsPerMessage;
        long highSeq = ringBuffer.next(n);
        long lowSeq = highSeq - n + 1;

//        log.debug("longArray[{}] n={} seq={}..{}", longArray.length, n, lowSeq, highSeq);

        try {
            for (long seq = lowSeq; seq <= highSeq; seq++) {

                OrderCommand cmd = ringBuffer.get(seq);
                cmd.command = cmdType;
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
            // report last sequence before actually publishing data
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
        cmd.orderId = api.id;
        cmd.symbol = api.symbol;
        cmd.uid = api.uid;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiCancelOrder> CANCEL_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.CANCEL_ORDER;
        cmd.orderId = api.id;
        cmd.symbol = api.symbol;
        cmd.uid = api.uid;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiOrderBookRequest> ORDER_BOOK_REQUEST_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.ORDER_BOOK_REQUEST;
        cmd.symbol = api.symbol;
        cmd.size = api.size;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiAddUser> ADD_USER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.ADD_USER;
        cmd.uid = api.uid;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiSuspendUser> SUSPEND_USER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.SUSPEND_USER;
        cmd.uid = api.uid;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiResumeUser> RESUME_USER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.RESUME_USER;
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
        cmd.orderType = OrderType.of(api.adjustmentType.getCode());
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiReset> RESET_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.RESET;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    public void createUser(long userId, Consumer<OrderCommand> callback) {
        ringBuffer.publishEvent(((cmd, seq) -> {
            cmd.command = OrderCommandType.ADD_USER;
            cmd.orderId = -1;
            cmd.symbol = -1;
            cmd.uid = userId;
            cmd.timestamp = System.currentTimeMillis();
            cmd.resultCode = CommandResultCode.NEW;

            promises.put(seq, callback);
        }));
    }

    public void suspendUser(long userId, Consumer<OrderCommand> callback) {
        ringBuffer.publishEvent(((cmd, seq) -> {
            cmd.command = OrderCommandType.SUSPEND_USER;
            cmd.orderId = -1;
            cmd.symbol = -1;
            cmd.uid = userId;
            cmd.timestamp = System.currentTimeMillis();
            cmd.resultCode = CommandResultCode.NEW;

            promises.put(seq, callback);
        }));
    }

    public void resumeUser(long userId, Consumer<OrderCommand> callback) {
        ringBuffer.publishEvent(((cmd, seq) -> {
            cmd.command = OrderCommandType.RESUME_USER;
            cmd.orderId = -1;
            cmd.symbol = -1;
            cmd.uid = userId;
            cmd.timestamp = System.currentTimeMillis();
            cmd.resultCode = CommandResultCode.NEW;

            promises.put(seq, callback);
        }));
    }

    public void balanceAdjustment(long userId,
                                  long transactionId,
                                  int currency,
                                  long longAmount,
                                  BalanceAdjustmentType adjustmentType,
                                  Consumer<OrderCommand> callback) {

        ringBuffer.publishEvent(((cmd, seq) -> {
            cmd.command = OrderCommandType.BALANCE_ADJUSTMENT;
            cmd.orderId = transactionId;
            cmd.symbol = currency;
            cmd.uid = userId;
            cmd.price = longAmount;
            cmd.orderType = OrderType.of(adjustmentType.getCode());
            cmd.size = 0;
            cmd.timestamp = System.currentTimeMillis();
            cmd.resultCode = CommandResultCode.NEW;

            promises.put(seq, callback);
        }));

    }

    public void orderBookRequest(int symbolId, int depth, Consumer<OrderCommand> callback) {

        ringBuffer.publishEvent(((cmd, seq) -> {
            cmd.command = OrderCommandType.ORDER_BOOK_REQUEST;
            cmd.orderId = -1;
            cmd.symbol = symbolId;
            cmd.uid = -1;
            cmd.size = depth;
            cmd.timestamp = System.currentTimeMillis();
            cmd.resultCode = CommandResultCode.NEW;

            promises.put(seq, callback);
        }));

    }

    public long placeNewOrder(
            int userCookie,
            long price,
            long reservedBidPrice,
            long size,
            OrderAction action,
            OrderType orderType,
            int symbol,
            long uid,
            Consumer<OrderCommand> callback) {

        final long seq = ringBuffer.next();
        try {
            OrderCommand cmd = ringBuffer.get(seq);
            cmd.command = OrderCommandType.PLACE_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.price = price;
            cmd.reserveBidPrice = reservedBidPrice;
            cmd.size = size;
            cmd.orderId = seq;
            cmd.timestamp = System.currentTimeMillis();
            cmd.action = action;
            cmd.orderType = orderType;
            cmd.symbol = symbol;
            cmd.uid = uid;
            cmd.userCookie = userCookie;
            promises.put(seq, callback);

        } finally {
            ringBuffer.publish(seq);
        }
        return seq;
    }

    public void moveOrder(
            long price,
            long orderId,
            int symbol,
            long uid,
            Consumer<OrderCommand> callback) {

        ringBuffer.publishEvent((cmd, seq) -> {
            cmd.command = OrderCommandType.MOVE_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.price = price;
            cmd.orderId = orderId;
            cmd.timestamp = System.currentTimeMillis();
            cmd.symbol = symbol;
            cmd.uid = uid;

            promises.put(seq, callback);
        });
    }

    public void cancelOrder(
            long orderId,
            int symbol,
            long uid,
            Consumer<OrderCommand> callback) {

        ringBuffer.publishEvent((cmd, seq) -> {
            cmd.command = OrderCommandType.CANCEL_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.orderId = orderId;
            cmd.timestamp = System.currentTimeMillis();
            cmd.symbol = symbol;
            cmd.uid = uid;

            promises.put(seq, callback);
        });

    }

}
