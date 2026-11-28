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
import net.jpountz.lz4.LZ4Compressor;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.wire.Wire;
import org.agrona.collections.LongLongConsumer;
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Stream;

public final class ExchangeApi {

    private static final Logger log = LoggerFactory.getLogger(ExchangeApi.class);

    private final RingBuffer<OrderCommand> ringBuffer;
    private final LZ4Compressor lz4Compressor;

    // promises cache (TODO can be changed to queue)
    private final Map<Long, Consumer<OrderCommand>> promises = new ConcurrentHashMap<>();

    // a binary frame carries six longs in orderIdLo, price, reserveBidPrice, size, uidHi, uidLo
    public static final int LONGS_PER_MESSAGE = 6;


    public void processResult(final long seq, final OrderCommand cmd) {

//        if (cmd.command == OrderCommandType.BINARY_DATA_COMMAND
//                || cmd.command == OrderCommandType.BINARY_DATA_QUERY) {

        final Consumer<OrderCommand> consumer = promises.remove(seq);
        if (consumer != null) {
            consumer.accept(cmd);
        }
    }

    public void submitCommand(ApiCommand cmd) {
        switch (cmd) {
            case ApiMoveOrder order -> ringBuffer.publishEvent(MOVE_ORDER_TRANSLATOR, order);
            case ApiPlaceOrder order -> ringBuffer.publishEvent(NEW_ORDER_TRANSLATOR, order);
            case ApiCancelOrder order -> ringBuffer.publishEvent(CANCEL_ORDER_TRANSLATOR, order);
            case ApiReduceOrder order -> ringBuffer.publishEvent(REDUCE_ORDER_TRANSLATOR, order);
            case ApiOrderBookRequest request -> ringBuffer.publishEvent(ORDER_BOOK_REQUEST_TRANSLATOR, request);
            case ApiAddUser user -> ringBuffer.publishEvent(ADD_USER_TRANSLATOR, user);
            case ApiAdjustUserBalance adjustment -> ringBuffer.publishEvent(ADJUST_USER_BALANCE_TRANSLATOR, adjustment);
            case ApiResumeUser user -> ringBuffer.publishEvent(RESUME_USER_TRANSLATOR, user);
            case ApiRemoveUser user -> ringBuffer.publishEvent(REMOVE_USER_TRANSLATOR, user);
            case ApiSuspendUser user -> ringBuffer.publishEvent(SUSPEND_USER_TRANSLATOR, user);
            case ApiBinaryDataCommand binary -> publishBinaryData(binary, _ -> {});
            case ApiPersistState persist -> publishPersistCmd(persist, (_, _) -> {});
            case ApiReset reset -> ringBuffer.publishEvent(RESET_TRANSLATOR, reset);
            case ApiNop nop -> ringBuffer.publishEvent(NOP_TRANSLATOR, nop);
            default -> throw new IllegalArgumentException("Unsupported command type: " + cmd.getClass().getSimpleName());
        }
    }

    public CompletableFuture<CommandResultCode> submitCommandAsync(ApiCommand cmd) {
        return switch (cmd) {
            case ApiMoveOrder order -> submitCommandAsync(MOVE_ORDER_TRANSLATOR, order);
            case ApiPlaceOrder order -> submitCommandAsync(NEW_ORDER_TRANSLATOR, order);
            case ApiCancelOrder order -> submitCommandAsync(CANCEL_ORDER_TRANSLATOR, order);
            case ApiReduceOrder order -> submitCommandAsync(REDUCE_ORDER_TRANSLATOR, order);
            case ApiOrderBookRequest request -> submitCommandAsync(ORDER_BOOK_REQUEST_TRANSLATOR, request);
            case ApiAddUser user -> submitCommandAsync(ADD_USER_TRANSLATOR, user);
            case ApiAdjustUserBalance adjustment -> submitCommandAsync(ADJUST_USER_BALANCE_TRANSLATOR, adjustment);
            case ApiResumeUser user -> submitCommandAsync(RESUME_USER_TRANSLATOR, user);
            case ApiRemoveUser user -> submitCommandAsync(REMOVE_USER_TRANSLATOR, user);
            case ApiSuspendUser user -> submitCommandAsync(SUSPEND_USER_TRANSLATOR, user);
            case ApiBinaryDataCommand binary -> submitBinaryDataAsync(binary.data);
            case ApiPersistState persist -> submitPersistCommandAsync(persist);
            case ApiReset reset -> submitCommandAsync(RESET_TRANSLATOR, reset);
            case ApiNop nop -> submitCommandAsync(NOP_TRANSLATOR, nop);
            default -> throw new IllegalArgumentException("Unsupported command type: " + cmd.getClass().getSimpleName());
        };
    }

    /**
     * Submits a command and returns an independent snapshot of its response.
     * The caller can retain the response, including matcher events and market data.
     *
     * @param cmd the command to submit
     * @return a future for the full response
     */
    public CompletableFuture<OrderCommand> submitCommandAsyncFullResponse(ApiCommand cmd) {
        return switch (cmd) {
            case ApiMoveOrder order -> submitCommandAsyncFullResponse(MOVE_ORDER_TRANSLATOR, order);
            case ApiPlaceOrder order -> submitCommandAsyncFullResponse(NEW_ORDER_TRANSLATOR, order);
            case ApiCancelOrder order -> submitCommandAsyncFullResponse(CANCEL_ORDER_TRANSLATOR, order);
            case ApiReduceOrder order -> submitCommandAsyncFullResponse(REDUCE_ORDER_TRANSLATOR, order);
            case ApiOrderBookRequest request -> submitCommandAsyncFullResponse(ORDER_BOOK_REQUEST_TRANSLATOR, request);
            case ApiAddUser user -> submitCommandAsyncFullResponse(ADD_USER_TRANSLATOR, user);
            case ApiAdjustUserBalance adjustment -> submitCommandAsyncFullResponse(ADJUST_USER_BALANCE_TRANSLATOR, adjustment);
            case ApiResumeUser user -> submitCommandAsyncFullResponse(RESUME_USER_TRANSLATOR, user);
            case ApiRemoveUser user -> submitCommandAsyncFullResponse(REMOVE_USER_TRANSLATOR, user);
            case ApiSuspendUser user -> submitCommandAsyncFullResponse(SUSPEND_USER_TRANSLATOR, user);
            case ApiReset reset -> submitCommandAsyncFullResponse(RESET_TRANSLATOR, reset);
            case ApiNop nop -> submitCommandAsyncFullResponse(NOP_TRANSLATOR, nop);
            default -> throw new IllegalArgumentException("Unsupported command type: " + cmd.getClass().getSimpleName());
        };
    }


    public void submitCommandsSync(List<? extends ApiCommand> cmd) {
        if (cmd.isEmpty()) {
            return;
        }

        cmd.subList(0, cmd.size() - 1).forEach(this::submitCommand);
        submitCommandAsync(cmd.getLast()).join();
    }

    public void submitCommandsSync(Stream<? extends ApiCommand> stream) {

        stream.forEach(this::submitCommand);
        submitCommandAsync(ApiNop.builder().build()).join();
    }

    private <T extends ApiCommand> CompletableFuture<CommandResultCode> submitCommandAsync(EventTranslatorOneArg<OrderCommand, T> translator, final T apiCommand) {
        return submitCommandAsync(translator, apiCommand, c -> c.resultCode);
    }

    private <T extends ApiCommand> CompletableFuture<OrderCommand> submitCommandAsyncFullResponse(EventTranslatorOneArg<OrderCommand, T> translator, final T apiCommand) {
        return submitCommandAsync(translator, apiCommand, OrderCommand::copy);
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
                // The Chronicle buffers are owned by the results thread.
                cmd -> query.createResult(
                        OrderBookEventsHelper.deserializeEvents(cmd).values().stream().map(Wire::bytes)));
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

        final net.openhft.chronicle.bytes.NativeBytes<Void> serializedBytes =
                BinaryCommandsProcessor.serializeObject(data, dataTypeCode);

        final long[] longsArrayData;
        try {
            longsArrayData = SerializationUtils.bytesToLongArrayLz4(lz4Compressor, serializedBytes, LONGS_PER_MESSAGE);
        } finally {
            serializedBytes.releaseLast();
        }

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

                cmd.orderIdLo = longsArrayData[ptr];
                cmd.price = longsArrayData[ptr + 1];
                cmd.reserveBidPrice = longsArrayData[ptr + 2];
                cmd.size = longsArrayData[ptr + 3];
                cmd.uidHi = longsArrayData[ptr + 4];
                cmd.uidLo = longsArrayData[ptr + 5];

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
            cmdMatching.orderIdLo = api.dumpId;
            cmdMatching.symbol = -1;
            cmdMatching.uidHi = 0;
            cmdMatching.uidLo = 0;
            cmdMatching.price = 0;
            cmdMatching.timestamp = api.timestamp;
            cmdMatching.resultCode = CommandResultCode.NEW;

            //log.debug("seq={} cmd.command={} data={}", firstSeq, cmdMatching.command, cmdMatching.price);

            // sequential command will make risk handler to create snapshot
            final OrderCommand cmdRisk = ringBuffer.get(secondSeq);
            cmdRisk.command = OrderCommandType.PERSIST_STATE_RISK;
            cmdRisk.orderIdLo = api.dumpId;
            cmdRisk.symbol = -1;
            cmdRisk.uidHi = 0;
            cmdRisk.uidLo = 0;
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
        cmd.setOrderId(api.orderId);
        cmd.timestamp = api.timestamp;
        cmd.action = api.action;
        cmd.orderType = api.orderType;
        cmd.symbol = api.symbol;
        cmd.setUid(api.uid);
        cmd.userCookie = api.userCookie;
        cmd.orderTakerFee = api.orderTakerFee;
        cmd.orderMakerFee = api.orderMakerFee;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiMoveOrder> MOVE_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.MOVE_ORDER;
        cmd.price = api.newPrice;
        cmd.setOrderId(api.orderId);
        cmd.symbol = api.symbol;
        cmd.setUid(api.uid);
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiCancelOrder> CANCEL_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.CANCEL_ORDER;
        cmd.setOrderId(api.orderId);
        cmd.symbol = api.symbol;
        cmd.setUid(api.uid);
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiReduceOrder> REDUCE_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.REDUCE_ORDER;
        cmd.setOrderId(api.orderId);
        cmd.symbol = api.symbol;
        cmd.setUid(api.uid);
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
        cmd.setUid(api.uid);
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiSuspendUser> SUSPEND_USER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.SUSPEND_USER;
        cmd.setUid(api.uid);
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiRemoveUser> REMOVE_USER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.REMOVE_USER;
        cmd.setUid(api.uid);
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiResumeUser> RESUME_USER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.RESUME_USER;
        cmd.setUid(api.uid);
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiAdjustUserBalance> ADJUST_USER_BALANCE_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.BALANCE_ADJUSTMENT;
        cmd.orderIdLo = api.transactionId;
        cmd.symbol = api.currency;
        cmd.setUid(api.uid);
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

    public void binaryData(int serviceFlags, long eventsGroup, long timestampNs, byte lastFlag, long word0, long word1, long word2, long word3, long word4, long word5) {
        ringBuffer.publishEvent(((cmd, seq) -> {

            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.BINARY_DATA_COMMAND;
            cmd.symbol = lastFlag;
            cmd.orderIdLo = word0;
            cmd.price = word1;
            cmd.reserveBidPrice = word2;
            cmd.size = word3;
            cmd.uidHi = word4;
            cmd.uidLo = word5;
            cmd.timestamp = timestampNs;
            cmd.resultCode = CommandResultCode.NEW;
//            log.debug("REPLAY {}", String.format("f=%d word0=%X word1=%X word2=%X word3=%X word4=%X", lastFlag, word0, word1, word2, word3, word4));
//            log.debug("REPLAY seq={} cmd={}", seq, cmd);
        }));
    }

    public void createUser(UUID userId, Consumer<OrderCommand> callback) {
        ringBuffer.publishEvent(((cmd, seq) -> {
            cmd.command = OrderCommandType.ADD_USER;
            cmd.orderIdHi = -1;
            cmd.orderIdLo = -1;
            cmd.symbol = -1;
            cmd.setUid(userId);
            cmd.timestamp = System.currentTimeMillis();
            cmd.resultCode = CommandResultCode.NEW;

            promises.put(seq, callback);
        }));
    }

    public void suspendUser(UUID userId, Consumer<OrderCommand> callback) {
        ringBuffer.publishEvent(((cmd, seq) -> {
            cmd.command = OrderCommandType.SUSPEND_USER;
            cmd.orderIdHi = -1;
            cmd.orderIdLo = -1;
            cmd.symbol = -1;
            cmd.setUid(userId);
            cmd.timestamp = System.currentTimeMillis();
            cmd.resultCode = CommandResultCode.NEW;

            promises.put(seq, callback);
        }));
    }

    public void removeUser(UUID userId, Consumer<OrderCommand> callback) {
        ringBuffer.publishEvent(((cmd, seq) -> {
            cmd.command = OrderCommandType.REMOVE_USER;
            cmd.orderIdHi = -1;
            cmd.orderIdLo = -1;
            cmd.symbol = -1;
            cmd.setUid(userId);
            cmd.timestamp = System.currentTimeMillis();
            cmd.resultCode = CommandResultCode.NEW;

            promises.put(seq, callback);
        }));
    }

    public void resumeUser(UUID userId, Consumer<OrderCommand> callback) {
        ringBuffer.publishEvent(((cmd, seq) -> {
            cmd.command = OrderCommandType.RESUME_USER;
            cmd.orderIdHi = -1;
            cmd.orderIdLo = -1;
            cmd.symbol = -1;
            cmd.setUid(userId);
            cmd.timestamp = System.currentTimeMillis();
            cmd.resultCode = CommandResultCode.NEW;

            promises.put(seq, callback);
        }));
    }

    public void createUser(int serviceFlags, long eventsGroup, long timestampNs, long uidHi, long uidLo) {
        ringBuffer.publishEvent(((cmd, seq) -> {

            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.ADD_USER;
            cmd.orderIdHi = -1;
            cmd.orderIdLo = -1;
            cmd.symbol = -1;
            cmd.uidHi = uidHi;
            cmd.uidLo = uidLo;
            cmd.timestamp = timestampNs;
            cmd.resultCode = CommandResultCode.NEW;

        }));
    }

    public void suspendUser(int serviceFlags, long eventsGroup, long timestampNs, long uidHi, long uidLo) {
        ringBuffer.publishEvent(((cmd, seq) -> {

            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.SUSPEND_USER;
            cmd.orderIdHi = -1;
            cmd.orderIdLo = -1;
            cmd.symbol = -1;
            cmd.uidHi = uidHi;
            cmd.uidLo = uidLo;
            cmd.timestamp = timestampNs;
            cmd.resultCode = CommandResultCode.NEW;

        }));
    }

    public void removeUser(int serviceFlags, long eventsGroup, long timestampNs, long uidHi, long uidLo) {
        ringBuffer.publishEvent(((cmd, seq) -> {

            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.REMOVE_USER;
            cmd.orderIdHi = -1;
            cmd.orderIdLo = -1;
            cmd.symbol = -1;
            cmd.uidHi = uidHi;
            cmd.uidLo = uidLo;
            cmd.timestamp = timestampNs;
            cmd.resultCode = CommandResultCode.NEW;

        }));
    }

    public void resumeUser(int serviceFlags, long eventsGroup, long timestampNs, long uidHi, long uidLo) {
        ringBuffer.publishEvent(((cmd, seq) -> {

            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.RESUME_USER;
            cmd.orderIdHi = -1;
            cmd.orderIdLo = -1;
            cmd.symbol = -1;
            cmd.uidHi = uidHi;
            cmd.uidLo = uidLo;
            cmd.timestamp = timestampNs;
            cmd.resultCode = CommandResultCode.NEW;

        }));
    }

    public void balanceAdjustment(UUID uid,
                                  long transactionId,
                                  int currency,
                                  long longAmount,
                                  BalanceAdjustmentType adjustmentType,
                                  Consumer<OrderCommand> callback) {

        ringBuffer.publishEvent(((cmd, seq) -> {
            cmd.command = OrderCommandType.BALANCE_ADJUSTMENT;
            cmd.orderIdLo = transactionId;
            cmd.symbol = currency;
            cmd.setUid(uid);
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
                                  long uidHi, long uidLo,
                                  long transactionId,
                                  int currency,
                                  long longAmount,
                                  BalanceAdjustmentType adjustmentType) {

        ringBuffer.publishEvent(((cmd, seq) -> {
            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;
            cmd.command = OrderCommandType.BALANCE_ADJUSTMENT;
            cmd.orderIdLo = transactionId;
            cmd.symbol = currency;
            cmd.uidHi = uidHi;
            cmd.uidLo = uidLo;
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
            cmd.orderIdHi = -1;
            cmd.orderIdLo = -1;
            cmd.symbol = symbolId;
            cmd.uidHi = -1;
            cmd.uidLo = -1;
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
            cmd.orderIdHi = -1;
            cmd.orderIdLo = -1;
            cmd.symbol = symbolId;
            cmd.uidHi = -1;
            cmd.uidLo = -1;
            cmd.size = depth;
            cmd.timestamp = System.currentTimeMillis();
            cmd.resultCode = CommandResultCode.NEW;

            promises.put(seq, cmd1 -> future.complete(cmd1.marketData));
        }));

        return future;
    }

    public long placeNewOrder(UUID orderId, int userCookie, long price, long reservedBidPrice, long size,
                              OrderAction action, OrderType orderType, int symbol, UUID uid,
                              Consumer<OrderCommand> callback) {
        return placeNewOrder(orderId, userCookie, price, reservedBidPrice, size, action, orderType, symbol, uid, -1, -1, callback);
    }

    public long placeNewOrder(
            UUID orderId,
            int userCookie,
            long price,
            long reservedBidPrice,
            long size,
            OrderAction action,
            OrderType orderType,
            int symbol,
            UUID uid,
            long orderTakerFee,
            long orderMakerFee,
            Consumer<OrderCommand> callback) {

        final long seq = ringBuffer.next();
        try {
            OrderCommand cmd = ringBuffer.get(seq);
            cmd.command = OrderCommandType.PLACE_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.price = price;
            cmd.reserveBidPrice = reservedBidPrice;
            cmd.size = size;
            cmd.setOrderId(orderId);
            cmd.timestamp = System.currentTimeMillis();
            cmd.action = action;
            cmd.orderType = orderType;
            cmd.symbol = symbol;
            cmd.setUid(uid);
            cmd.orderTakerFee = orderTakerFee;
            cmd.orderMakerFee = orderMakerFee;
            cmd.userCookie = userCookie;
            promises.put(seq, callback);

        } finally {
            ringBuffer.publish(seq);
        }
        return seq;
    }


    public void placeNewOrder(int serviceFlags, long eventsGroup, long timestampNs, long orderIdHi, long orderIdLo,
                              int userCookie, long price, long reservedBidPrice, long size,
                              OrderAction action, OrderType orderType, int symbol, long uidHi, long uidLo) {
        placeNewOrder(serviceFlags, eventsGroup, timestampNs, orderIdHi, orderIdLo, userCookie, price, reservedBidPrice,
                size, action, orderType, symbol, uidHi, uidLo, -1, -1);
    }

    public void placeNewOrder(int serviceFlags,
                              long eventsGroup,
                              long timestampNs,
                              long orderIdHi, long orderIdLo,
                              int userCookie,
                              long price,
                              long reservedBidPrice,
                              long size,
                              OrderAction action,
                              OrderType orderType,
                              int symbol,
                              long uidHi, long uidLo,
                              long orderTakerFee,
                              long orderMakerFee) {

        ringBuffer.publishEvent((cmd, seq) -> {
            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.PLACE_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.price = price;
            cmd.reserveBidPrice = reservedBidPrice;
            cmd.size = size;
            cmd.orderIdHi = orderIdHi;
            cmd.orderIdLo = orderIdLo;
            cmd.timestamp = timestampNs;
            cmd.action = action;
            cmd.orderType = orderType;
            cmd.symbol = symbol;
            cmd.uidHi = uidHi;
            cmd.uidLo = uidLo;
            cmd.orderTakerFee = orderTakerFee;
            cmd.orderMakerFee = orderMakerFee;
            cmd.userCookie = userCookie;
        });
    }

    public void moveOrder(
            long price,
            UUID orderId,
            int symbol,
            UUID uid,
            Consumer<OrderCommand> callback) {

        ringBuffer.publishEvent((cmd, seq) -> {
            cmd.command = OrderCommandType.MOVE_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.price = price;
            cmd.setOrderId(orderId);
            cmd.timestamp = System.currentTimeMillis();
            cmd.symbol = symbol;
            cmd.setUid(uid);

            promises.put(seq, callback);
        });
    }

    public void moveOrder(int serviceFlags,
                          long eventsGroup,
                          long timestampNs,
                          long price,
                          long orderIdHi, long orderIdLo,
                          int symbol,
                          long uidHi, long uidLo) {

        ringBuffer.publishEvent((cmd, seq) -> {

            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.MOVE_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.price = price;
            cmd.orderIdHi = orderIdHi;
            cmd.orderIdLo = orderIdLo;
            cmd.timestamp = timestampNs;
            cmd.symbol = symbol;
            cmd.uidHi = uidHi;
            cmd.uidLo = uidLo;
        });
    }

    public void cancelOrder(
            UUID orderId,
            int symbol,
            UUID uid,
            Consumer<OrderCommand> callback) {

        ringBuffer.publishEvent((cmd, seq) -> {
            cmd.command = OrderCommandType.CANCEL_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.setOrderId(orderId);
            cmd.timestamp = System.currentTimeMillis();
            cmd.symbol = symbol;
            cmd.setUid(uid);

            promises.put(seq, callback);
        });

    }

    public void cancelOrder(int serviceFlags,
                            long eventsGroup,
                            long timestampNs,
                            long orderIdHi, long orderIdLo,
                            int symbol,
                            long uidHi, long uidLo) {

        ringBuffer.publishEvent((cmd, seq) -> {

            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.CANCEL_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.orderIdHi = orderIdHi;
            cmd.orderIdLo = orderIdLo;
            cmd.timestamp = timestampNs;
            cmd.symbol = symbol;
            cmd.uidHi = uidHi;
            cmd.uidLo = uidLo;
        });
    }

    public void reduceOrder(
            long reduceSize,
            UUID orderId,
            int symbol,
            UUID uid,
            Consumer<OrderCommand> callback) {

        ringBuffer.publishEvent((cmd, seq) -> {
            cmd.command = OrderCommandType.REDUCE_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.size = reduceSize;
            cmd.setOrderId(orderId);
            cmd.timestamp = System.currentTimeMillis();
            cmd.symbol = symbol;
            cmd.setUid(uid);

            promises.put(seq, callback);
        });
    }

    public void reduceOrder(int serviceFlags,
                            long eventsGroup,
                            long timestampNs,
                            long reduceSize,
                            long orderIdHi, long orderIdLo,
                            int symbol,
                            long uidHi, long uidLo) {

        ringBuffer.publishEvent((cmd, seq) -> {

            cmd.serviceFlags = serviceFlags;
            cmd.eventsGroup = eventsGroup;

            cmd.command = OrderCommandType.REDUCE_ORDER;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.size = reduceSize;
            cmd.orderIdHi = orderIdHi;
            cmd.orderIdLo = orderIdLo;
            cmd.timestamp = timestampNs;
            cmd.symbol = symbol;
            cmd.uidHi = uidHi;
            cmd.uidLo = uidLo;
        });
    }

    public void groupingControl(long timestampNs, long mode) {
        groupingControl(timestampNs, mode, ignored -> {});
    }

    public void groupingControl(long timestampNs, long mode, LongConsumer beforePublish) {
        ringBuffer.publishEvent((cmd, seq) -> {
            cmd.command = OrderCommandType.GROUPING_CONTROL;
            cmd.resultCode = CommandResultCode.NEW;

            cmd.orderIdLo = mode;
            cmd.timestamp = timestampNs;
            beforePublish.accept(seq);
        });
    }

    public void reset(long timestampNs) {

        ringBuffer.publishEvent((cmd, seq) -> {
            cmd.command = OrderCommandType.RESET;
            cmd.resultCode = CommandResultCode.NEW;
            cmd.timestamp = timestampNs;
        });

    }

    public ExchangeApi(RingBuffer<OrderCommand> ringBuffer, LZ4Compressor lz4Compressor) {
        this.ringBuffer = ringBuffer;
        this.lz4Compressor = lz4Compressor;
    }
}
