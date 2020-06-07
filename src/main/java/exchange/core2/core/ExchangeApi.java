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
import exchange.core2.core.common.L2MarketData;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.api.*;
import exchange.core2.core.common.api.binary.BinaryDataCommand;
import exchange.core2.core.common.api.reports.ApiReportQuery;
import exchange.core2.core.common.api.reports.ReportQuery;
import exchange.core2.core.common.api.reports.ReportResult;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.orderbook.OrderBookEventsHelper;
import exchange.core2.core.processors.BinaryCommandsProcessor;
import exchange.core2.core.utils.SerializationUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.jpountz.lz4.LZ4Compressor;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.wire.Wire;
import org.agrona.collections.LongLongConsumer;
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public final class ExchangeApi {

    private final RingBuffer<OrderCommand> ringBuffer;
    private final LZ4Compressor lz4Compressor;

    // promises cache (TODO can be changed to queue)
    private final Map<Long, Consumer<OrderCommand>> promises = new ConcurrentHashMap<>();

    public static final int LONGS_PER_MESSAGE = 5;


    public void processResult(final long seq, final OrderCommand cmd) {

//        if (cmd.command == OrderCommandType.BINARY_DATA_COMMAND
//                || cmd.command == OrderCommandType.BINARY_DATA_QUERY) {

        final Consumer<OrderCommand> consumer = promises.remove(seq);
        if (consumer != null) {
            consumer.accept(cmd);
        }
    }

    public void submitCommand(ApiCommand cmd) {
        //log.debug("{}", cmd);

        if (cmd instanceof ApiMoveOrder) {
            ringBuffer.publishEvent(MOVE_ORDER_TRANSLATOR, (ApiMoveOrder) cmd);
        } else if (cmd instanceof ApiPlaceOrder) {
            ringBuffer.publishEvent(NEW_ORDER_TRANSLATOR, (ApiPlaceOrder) cmd);
        } else if (cmd instanceof ApiCancelOrder) {
            ringBuffer.publishEvent(CANCEL_ORDER_TRANSLATOR, (ApiCancelOrder) cmd);
        } else if (cmd instanceof ApiReduceOrder) {
            ringBuffer.publishEvent(REDUCE_ORDER_TRANSLATOR, (ApiReduceOrder) cmd);
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
            publishPersistCmd((ApiPersistState) cmd, (seq1, seq2) -> {
            });
        } else if (cmd instanceof ApiReset) {
            ringBuffer.publishEvent(RESET_TRANSLATOR, (ApiReset) cmd);
        } else if (cmd instanceof ApiNop) {
            ringBuffer.publishEvent(NOP_TRANSLATOR, (ApiNop) cmd);
        } else {
            throw new IllegalArgumentException("Unsupported command type: " + cmd.getClass().getSimpleName());
        }
    }

    public CompletableFuture<CommandResultCode> submitCommandAsync(ApiCommand cmd) {
        //log.debug("{}", cmd);

        if (cmd instanceof ApiMoveOrder) {
            return submitCommandAsync(MOVE_ORDER_TRANSLATOR, (ApiMoveOrder) cmd);
        } else if (cmd instanceof ApiPlaceOrder) {
            return submitCommandAsync(NEW_ORDER_TRANSLATOR, (ApiPlaceOrder) cmd);
        } else if (cmd instanceof ApiCancelOrder) {
            return submitCommandAsync(CANCEL_ORDER_TRANSLATOR, (ApiCancelOrder) cmd);
        } else if (cmd instanceof ApiReduceOrder) {
            return submitCommandAsync(REDUCE_ORDER_TRANSLATOR, (ApiReduceOrder) cmd);
        } else if (cmd instanceof ApiOrderBookRequest) {
            return submitCommandAsync(ORDER_BOOK_REQUEST_TRANSLATOR, (ApiOrderBookRequest) cmd);
        } else if (cmd instanceof ApiAddUser) {
            return submitCommandAsync(ADD_USER_TRANSLATOR, (ApiAddUser) cmd);
        } else if (cmd instanceof ApiAdjustUserBalance) {
            return submitCommandAsync(ADJUST_USER_BALANCE_TRANSLATOR, (ApiAdjustUserBalance) cmd);
        } else if (cmd instanceof ApiResumeUser) {
            return submitCommandAsync(RESUME_USER_TRANSLATOR, (ApiResumeUser) cmd);
        } else if (cmd instanceof ApiSuspendUser) {
            return submitCommandAsync(SUSPEND_USER_TRANSLATOR, (ApiSuspendUser) cmd);
        } else if (cmd instanceof ApiBinaryDataCommand) {
            return submitBinaryDataAsync(((ApiBinaryDataCommand) cmd).data);
        } else if (cmd instanceof ApiPersistState) {
            return submitPersistCommandAsync((ApiPersistState) cmd);
        } else if (cmd instanceof ApiReset) {
            return submitCommandAsync(RESET_TRANSLATOR, (ApiReset) cmd);
        } else if (cmd instanceof ApiNop) {
            return submitCommandAsync(NOP_TRANSLATOR, (ApiNop) cmd);
        } else {
            throw new IllegalArgumentException("Unsupported command type: " + cmd.getClass().getSimpleName());
        }
    }

    public CompletableFuture<OrderCommand> submitCommandAsyncFullResponse(ApiCommand cmd) {

        if (cmd instanceof ApiMoveOrder) {
            return submitCommandAsyncFullResponse(MOVE_ORDER_TRANSLATOR, (ApiMoveOrder) cmd);
        } else if (cmd instanceof ApiPlaceOrder) {
            return submitCommandAsyncFullResponse(NEW_ORDER_TRANSLATOR, (ApiPlaceOrder) cmd);
        } else if (cmd instanceof ApiCancelOrder) {
            return submitCommandAsyncFullResponse(CANCEL_ORDER_TRANSLATOR, (ApiCancelOrder) cmd);
        } else if (cmd instanceof ApiReduceOrder) {
            return submitCommandAsyncFullResponse(REDUCE_ORDER_TRANSLATOR, (ApiReduceOrder) cmd);
        } else if (cmd instanceof ApiOrderBookRequest) {
            return submitCommandAsyncFullResponse(ORDER_BOOK_REQUEST_TRANSLATOR, (ApiOrderBookRequest) cmd);
        } else if (cmd instanceof ApiAddUser) {
            return submitCommandAsyncFullResponse(ADD_USER_TRANSLATOR, (ApiAddUser) cmd);
        } else if (cmd instanceof ApiAdjustUserBalance) {
            return submitCommandAsyncFullResponse(ADJUST_USER_BALANCE_TRANSLATOR, (ApiAdjustUserBalance) cmd);
        } else if (cmd instanceof ApiResumeUser) {
            return submitCommandAsyncFullResponse(RESUME_USER_TRANSLATOR, (ApiResumeUser) cmd);
        } else if (cmd instanceof ApiSuspendUser) {
            return submitCommandAsyncFullResponse(SUSPEND_USER_TRANSLATOR, (ApiSuspendUser) cmd);
        } else if (cmd instanceof ApiReset) {
            return submitCommandAsyncFullResponse(RESET_TRANSLATOR, (ApiReset) cmd);
        } else if (cmd instanceof ApiNop) {
            return submitCommandAsyncFullResponse(NOP_TRANSLATOR, (ApiNop) cmd);
        } else {
            throw new IllegalArgumentException("Unsupported command type: " + cmd.getClass().getSimpleName());
        }
    }


    public void submitCommandsSync(List<? extends ApiCommand> cmd) {
        if (cmd.isEmpty()) {
            return;
        }

        cmd.subList(0, cmd.size() - 1).forEach(this::submitCommand);
        submitCommandAsync(cmd.get(cmd.size() - 1)).join();
    }

    public void submitCommandsSync(Stream<? extends ApiCommand> stream) {

        stream.forEach(this::submitCommand);
        submitCommandAsync(ApiNop.builder().build()).join();
    }

    private <T extends ApiCommand> CompletableFuture<CommandResultCode> submitCommandAsync(EventTranslatorOneArg<OrderCommand, T> translator, final T apiCommand) {
        return submitCommandAsync(translator, apiCommand, c -> c.resultCode);
    }

    private <T extends ApiCommand> CompletableFuture<OrderCommand> submitCommandAsyncFullResponse(EventTranslatorOneArg<OrderCommand, T> translator, final T apiCommand) {
        return submitCommandAsync(translator, apiCommand, Function.identity());
    }

    private <T extends ApiCommand, R> CompletableFuture<R> submitCommandAsync(final EventTranslatorOneArg<OrderCommand, T> translator,
                                                                              final T apiCommand,
                                                                              final Function<OrderCommand, R> responseTranslator) {
        final CompletableFuture<R> future = new CompletableFuture<>();

        ringBuffer.publishEvent(
                (cmd, seq, apiCmd) -> {
                    translator.translateTo(cmd, seq, apiCmd);
                    promises.put(seq, orderCommand -> future.complete(responseTranslator.apply(orderCommand)));
                },
                apiCommand);

        return future;
    }

    private CompletableFuture<CommandResultCode> submitPersistCommandAsync(final ApiPersistState apiCommand) {

        final CompletableFuture<CommandResultCode> future1 = new CompletableFuture<>();
        final CompletableFuture<CommandResultCode> future2 = new CompletableFuture<>();

        publishPersistCmd(apiCommand, (seq1, seq2) -> {
            promises.put(seq1, cmd -> future1.complete(cmd.resultCode));
            promises.put(seq2, cmd -> future2.complete(cmd.resultCode));
        });

        return future1.thenCombineAsync(future2, CommandResultCode::mergeToFirstFailed);
    }

    public CompletableFuture<CommandResultCode> submitBinaryDataAsync(final BinaryDataCommand data) {

        final CompletableFuture<CommandResultCode> future = new CompletableFuture<>();

        publishBinaryData(
                OrderCommandType.BINARY_DATA_COMMAND,
                data,
                data.getBinaryCommandTypeCode(),
                (int) System.nanoTime(), // can be any value because sequence is used for result identification, not transferId
                0L,
                seq -> promises.put(seq, orderCommand -> future.complete(orderCommand.resultCode)));

        return future;
    }

    public <R> CompletableFuture<R> submitBinaryCommandAsync(
            final BinaryDataCommand data,
            final int transferId,
            final Function<OrderCommand, R> translator) {

        final CompletableFuture<R> future = new CompletableFuture<>();

        publishBinaryData(
                ApiBinaryDataCommand.builder().data(data).transferId(transferId).build(),
                seq -> promises.put(seq, orderCommand -> future.complete(translator.apply(orderCommand))));

        return future;
    }

    public <R> CompletableFuture<R> submitQueryAsync(
            final ReportQuery<?> data,
            final int transferId,
            final Function<OrderCommand, R> translator) {

        final CompletableFuture<R> future = new CompletableFuture<>();

        publishQuery(
                ApiReportQuery.builder().query(data).transferId(transferId).build(),
                seq -> promises.put(seq, orderCommand -> future.complete(translator.apply(orderCommand))));

        return future;
    }

    public <Q extends ReportQuery<R>, R extends ReportResult> CompletableFuture<R> processReport(final Q query, final int transferId) {
        return submitQueryAsync(
                query,
                transferId,
                cmd -> query.createResult(
                        OrderBookEventsHelper.deserializeEvents(cmd).values().parallelStream().map(Wire::bytes)));
    }

    public void publishBinaryData(final ApiBinaryDataCommand apiCmd, final LongConsumer endSeqConsumer) {

        publishBinaryData(
                OrderCommandType.BINARY_DATA_COMMAND,
                apiCmd.data,
                apiCmd.data.getBinaryCommandTypeCode(),
                apiCmd.transferId,
                apiCmd.timestamp,
                endSeqConsumer);
    }

    public void publishQuery(final ApiReportQuery apiCmd, final LongConsumer endSeqConsumer) {
        publishBinaryData(
                OrderCommandType.BINARY_DATA_QUERY,
                apiCmd.query,
                apiCmd.query.getReportTypeCode(),
                apiCmd.transferId,
                apiCmd.timestamp,
                endSeqConsumer);
    }

    private void publishBinaryData(final OrderCommandType cmdType,
                                   final WriteBytesMarshallable data,
                                   final int dataTypeCode,
                                   final int transferId,
                                   final long timestamp,
                                   final LongConsumer endSeqConsumer) {

        final long[] longsArrayData = SerializationUtils.bytesToLongArrayLz4(
                lz4Compressor,
                BinaryCommandsProcessor.serializeObject(data, dataTypeCode),
                LONGS_PER_MESSAGE);

        final int totalNumMessagesToClaim = longsArrayData.length / LONGS_PER_MESSAGE;

//        log.debug("longsArrayData[{}] n={}", longsArrayData.length, totalNumMessagesToClaim);

        // max fragment size is quarter of ring buffer
        final int batchSize = ringBuffer.getBufferSize() / 4;

        int offset = 0;
        boolean isLastFragment = false;
        int fragmentSize = batchSize;

        do {

            if (offset + batchSize >= totalNumMessagesToClaim) {
                fragmentSize = totalNumMessagesToClaim - offset;
                isLastFragment = true;
            }

            publishBinaryMessageFragment(cmdType, transferId, timestamp, endSeqConsumer, longsArrayData, fragmentSize, offset, isLastFragment);

            offset += batchSize;

        } while (!isLastFragment);

    }

    private void publishBinaryMessageFragment(OrderCommandType cmdType,
                                              int transferId,
                                              long timestamp,
                                              LongConsumer endSeqConsumer,
                                              long[] longsArrayData,
                                              int fragmentSize,
                                              int offset,
                                              boolean isLastFragment) {

        final long highSeq = ringBuffer.next(fragmentSize);
        final long lowSeq = highSeq - fragmentSize + 1;

//        log.debug("  offset*longsPerMessage={} longsArrayData[{}] n={} seq={}..{} lastFragment={} fragmentSize={}",
//                offset * LONGS_PER_MESSAGE, longsArrayData.length, fragmentSize, lowSeq, highSeq, isLastFragment, fragmentSize);

        try {
            int ptr = offset * LONGS_PER_MESSAGE;
            for (long seq = lowSeq; seq <= highSeq; seq++) {

                OrderCommand cmd = ringBuffer.get(seq);
                cmd.command = cmdType;
                cmd.userCookie = transferId;
                cmd.symbol = (isLastFragment && seq == highSeq) ? -1 : 0;

                cmd.orderId = longsArrayData[ptr];
                cmd.price = longsArrayData[ptr + 1];
                cmd.reserveBidPrice = longsArrayData[ptr + 2];
                cmd.size = longsArrayData[ptr + 3];
                cmd.uid = longsArrayData[ptr + 4];

                cmd.timestamp = timestamp;
                cmd.resultCode = CommandResultCode.NEW;

//                log.debug("ORIG {}", String.format("f=%d word0=%X word1=%X word2=%X word3=%X word4=%X",
//                cmd.symbol, longArray[i], longArray[i + 1], longArray[i + 2], longArray[i + 3], longArray[i + 4]));

//                log.debug("seq={} cmd.size={} data={}", seq, cmd.size, cmd.price);

                ptr += LONGS_PER_MESSAGE;
            }
        } catch (final Exception ex) {
            log.error("Binary commands processing exception: ", ex);

        } finally {
            if (isLastFragment) {
                // report last sequence before actually publishing data
                endSeqConsumer.accept(highSeq);
            }
            ringBuffer.publish(lowSeq, highSeq);
        }
    }

    private void publishPersistCmd(final ApiPersistState api,
                                   final LongLongConsumer seqConsumer) {

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
            seqConsumer.accept(firstSeq, secondSeq);
            ringBuffer.publish(firstSeq, secondSeq);
        }
    }


    private static final EventTranslatorOneArg<OrderCommand, ApiPlaceOrder> NEW_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.PLACE_ORDER;
        cmd.price = api.price;
        cmd.reserveBidPrice = api.reservePrice;
        cmd.size = api.size;
        cmd.orderId = api.orderId;
        cmd.timestamp = api.timestamp;
        cmd.action = api.action;
        cmd.orderType = api.orderType;
        cmd.symbol = api.symbol;
        cmd.uid = api.uid;
        cmd.userCookie = api.userCookie;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiMoveOrder> MOVE_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.MOVE_ORDER;
        cmd.price = api.newPrice;
        cmd.orderId = api.orderId;
        cmd.symbol = api.symbol;
        cmd.uid = api.uid;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiCancelOrder> CANCEL_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.CANCEL_ORDER;
        cmd.orderId = api.orderId;
        cmd.symbol = api.symbol;
        cmd.uid = api.uid;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiReduceOrder> REDUCE_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.REDUCE_ORDER;
        cmd.orderId = api.orderId;
        cmd.symbol = api.symbol;
        cmd.uid = api.uid;
        cmd.size = api.reduceSize;
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

    private static final EventTranslatorOneArg<OrderCommand, ApiNop> NOP_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.NOP;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    public void binaryData(int serviceFlags, long eventsGroup, long timestampNs, byte lastFlag, long word0, long word1, long word2, long word3, long word4) {
        ringBuffer.publishEvent(((cmd, seq) -> {

            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.BINARY_DATA_COMMAND;
            cmd.symbol = lastFlag;
            cmd.orderId = word0;
            cmd.price = word1;
            cmd.reserveBidPrice = word2;
            cmd.size = word3;
            cmd.uid = word4;
            cmd.timestamp = timestampNs;
            cmd.resultCode = CommandResultCode.NEW;
//            log.debug("REPLAY {}", String.format("f=%d word0=%X word1=%X word2=%X word3=%X word4=%X", lastFlag, word0, word1, word2, word3, word4));
//            log.debug("REPLAY seq={} cmd={}", seq, cmd);
        }));
    }

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

    public void createUser(int serviceFlags, long eventsGroup, long timestampNs, long userId) {
        ringBuffer.publishEvent(((cmd, seq) -> {

            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.ADD_USER;
            cmd.orderId = -1;
            cmd.symbol = -1;
            cmd.uid = userId;
            cmd.timestamp = timestampNs;
            cmd.resultCode = CommandResultCode.NEW;

        }));
    }

    public void suspendUser(int serviceFlags, long eventsGroup, long timestampNs, long userId) {
        ringBuffer.publishEvent(((cmd, seq) -> {

            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.SUSPEND_USER;
            cmd.orderId = -1;
            cmd.symbol = -1;
            cmd.uid = userId;
            cmd.timestamp = timestampNs;
            cmd.resultCode = CommandResultCode.NEW;

        }));
    }

    public void resumeUser(int serviceFlags, long eventsGroup, long timestampNs, long userId) {
        ringBuffer.publishEvent(((cmd, seq) -> {

            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.RESUME_USER;
            cmd.orderId = -1;
            cmd.symbol = -1;
            cmd.uid = userId;
            cmd.timestamp = timestampNs;
            cmd.resultCode = CommandResultCode.NEW;

        }));
    }

    public void balanceAdjustment(long uid,
                                  long transactionId,
                                  int currency,
                                  long longAmount,
                                  BalanceAdjustmentType adjustmentType,
                                  Consumer<OrderCommand> callback) {

        ringBuffer.publishEvent(((cmd, seq) -> {
            cmd.command = OrderCommandType.BALANCE_ADJUSTMENT;
            cmd.orderId = transactionId;
            cmd.symbol = currency;
            cmd.uid = uid;
            cmd.price = longAmount;
            cmd.orderType = OrderType.of(adjustmentType.getCode());
            cmd.size = 0;
            cmd.timestamp = System.currentTimeMillis();
            cmd.resultCode = CommandResultCode.NEW;

            promises.put(seq, callback);
        }));

    }

    public void balanceAdjustment(int serviceFlags,
                                  long eventsGroup,
                                  long timestampNs,
                                  long uid,
                                  long transactionId,
                                  int currency,
                                  long longAmount,
                                  BalanceAdjustmentType adjustmentType) {

        ringBuffer.publishEvent(((cmd, seq) -> {
            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;
            cmd.command = OrderCommandType.BALANCE_ADJUSTMENT;
            cmd.orderId = transactionId;
            cmd.symbol = currency;
            cmd.uid = uid;
            cmd.price = longAmount;
            cmd.orderType = OrderType.of(adjustmentType.getCode());
            cmd.size = 0;
            cmd.timestamp = timestampNs;
            cmd.resultCode = CommandResultCode.NEW;
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

    public CompletableFuture<L2MarketData> requestOrderBookAsync(int symbolId, int depth) {

        final CompletableFuture<L2MarketData> future = new CompletableFuture<>();

        ringBuffer.publishEvent(((cmd, seq) -> {
            cmd.command = OrderCommandType.ORDER_BOOK_REQUEST;
            cmd.orderId = -1;
            cmd.symbol = symbolId;
            cmd.uid = -1;
            cmd.size = depth;
            cmd.timestamp = System.currentTimeMillis();
            cmd.resultCode = CommandResultCode.NEW;

            promises.put(seq, cmd1 -> future.complete(cmd1.marketData));
        }));

        return future;
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


    public void placeNewOrder(int serviceFlags,
                              long eventsGroup,
                              long timestampNs,
                              long orderId,
                              int userCookie,
                              long price,
                              long reservedBidPrice,
                              long size,
                              OrderAction action,
                              OrderType orderType,
                              int symbol,
                              long uid) {

        ringBuffer.publishEvent((cmd, seq) -> {
            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.PLACE_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.price = price;
            cmd.reserveBidPrice = reservedBidPrice;
            cmd.size = size;
            cmd.orderId = orderId;
            cmd.timestamp = timestampNs;
            cmd.action = action;
            cmd.orderType = orderType;
            cmd.symbol = symbol;
            cmd.uid = uid;
            cmd.userCookie = userCookie;
        });
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

    public void moveOrder(int serviceFlags,
                          long eventsGroup,
                          long timestampNs,
                          long price,
                          long orderId,
                          int symbol,
                          long uid) {

        ringBuffer.publishEvent((cmd, seq) -> {

            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.MOVE_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.price = price;
            cmd.orderId = orderId;
            cmd.timestamp = timestampNs;
            cmd.symbol = symbol;
            cmd.uid = uid;
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

    public void cancelOrder(int serviceFlags,
                            long eventsGroup,
                            long timestampNs,
                            long orderId,
                            int symbol,
                            long uid) {

        ringBuffer.publishEvent((cmd, seq) -> {

            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.CANCEL_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.orderId = orderId;
            cmd.timestamp = timestampNs;
            cmd.symbol = symbol;
            cmd.uid = uid;
        });
    }

    public void reduceOrder(
            long reduceSize,
            long orderId,
            int symbol,
            long uid,
            Consumer<OrderCommand> callback) {

        ringBuffer.publishEvent((cmd, seq) -> {
            cmd.command = OrderCommandType.REDUCE_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.size = reduceSize;
            cmd.orderId = orderId;
            cmd.timestamp = System.currentTimeMillis();
            cmd.symbol = symbol;
            cmd.uid = uid;

            promises.put(seq, callback);
        });
    }

    public void reduceOrder(int serviceFlags,
                            long eventsGroup,
                            long timestampNs,
                            long reduceSize,
                            long orderId,
                            int symbol,
                            long uid) {

        ringBuffer.publishEvent((cmd, seq) -> {

            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.REDUCE_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.size = reduceSize;
            cmd.orderId = orderId;
            cmd.timestamp = timestampNs;
            cmd.symbol = symbol;
            cmd.uid = uid;
        });
    }

    public void groupingControl(long timestampNs, long mode) {

        ringBuffer.publishEvent((cmd, seq) -> {
            cmd.command = OrderCommandType.GROUPING_CONTROL;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.orderId = mode;
            cmd.timestamp = timestampNs;
        });

    }

    public void reset(long timestampNs) {

        ringBuffer.publishEvent((cmd, seq) -> {
            cmd.command = OrderCommandType.RESET;
            cmd.resultCode = CommandResultCode.NEW;
            cmd.timestamp = timestampNs;
        });

    }
}
