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
package exchange.core2.core.processors;

import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.StateHash;
import exchange.core2.core.common.api.binary.BatchAddAccountsCommand;
import exchange.core2.core.common.api.binary.BatchAddSymbolsCommand;
import exchange.core2.core.common.api.reports.ReportQueriesHandler;
import exchange.core2.core.common.api.reports.ReportQuery;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.ReportsQueriesConfiguration;
import exchange.core2.core.orderbook.OrderBookEventsHelper;
import exchange.core2.core.utils.HashingUtils;
import exchange.core2.core.utils.SerializationUtils;
import exchange.core2.core.utils.UnsafeUtils;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.*;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Stateful Binary Commands Processor
 * <p>
 * Has incoming data buffer
 * Can receive events in arbitrary order and duplicates - at-least-once-delivery compatible.
 */
@Slf4j
public final class BinaryCommandsProcessor implements WriteBytesMarshallable, StateHash {

    // transactionId -> TransferRecord (long array + bitset)
    private final LongObjectHashMap<TransferRecord> incomingData;

    // TODO improve type (Object is not ok)
    private final Function<Object, Optional<? extends WriteBytesMarshallable>> completeMessagesHandler;

    private final ReportQueriesHandler reportQueriesHandler;

    private final OrderBookEventsHelper eventsHelper;

    private final ReportsQueriesConfiguration queriesConfiguration;

    private final int section;

    public BinaryCommandsProcessor(final Function<Object, Optional<? extends WriteBytesMarshallable>> completeMessagesHandler,
                                   final ReportQueriesHandler reportQueriesHandler,
                                   final SharedPool sharedPool,
                                   final ReportsQueriesConfiguration queriesConfiguration,
                                   final int section) {
        this.completeMessagesHandler = completeMessagesHandler;
        this.reportQueriesHandler = reportQueriesHandler;
        this.incomingData = new LongObjectHashMap<>();
        this.eventsHelper = new OrderBookEventsHelper(sharedPool::getChain);
        this.queriesConfiguration = queriesConfiguration;
        this.section = section;
    }

    public BinaryCommandsProcessor(final Function<Object, Optional<? extends WriteBytesMarshallable>> completeMessagesHandler,
                                   final ReportQueriesHandler reportQueriesHandler,
                                   final SharedPool sharedPool,
                                   final ReportsQueriesConfiguration queriesConfiguration,
                                   final BytesIn bytesIn,
                                   int section) {
        this.completeMessagesHandler = completeMessagesHandler;
        this.reportQueriesHandler = reportQueriesHandler;
        this.incomingData = SerializationUtils.readLongHashMap(bytesIn, b -> new TransferRecord(bytesIn));
        this.eventsHelper = new OrderBookEventsHelper(sharedPool::getChain);
        this.section = section;
        this.queriesConfiguration = queriesConfiguration;
    }

    public CommandResultCode acceptBinaryFrame(OrderCommand cmd) {

        final int transferId = cmd.userCookie;

        final TransferRecord record = incomingData.getIfAbsentPut(transferId, TransferRecord::new);

        record.addWord(cmd.orderId);
        record.addWord(cmd.price);
        record.addWord(cmd.reserveBidPrice);
        record.addWord(cmd.size);
        record.addWord(cmd.uid);

        if (cmd.symbol == -1) {
            // all frames received

            incomingData.removeKey(transferId);

            final BytesIn bytesIn = SerializationUtils.longsToWire(record.dataArray).bytes();

            final Optional<? extends WriteBytesMarshallable> resultOpt = (cmd.command == OrderCommandType.BINARY_DATA_QUERY)
                    ? deserializeQuery(bytesIn).flatMap(reportQueriesHandler::handleReport)
                    : completeMessagesHandler.apply(deserializeObject(bytesIn));

            resultOpt.ifPresent(res -> {
                final NativeBytes<Void> bytes = Bytes.allocateElasticDirect(128);
                res.writeMarshallable(bytes);
                final MatcherTradeEvent binaryEventsChain = eventsHelper.createBinaryEventsChain(cmd.timestamp, section, bytes);
                UnsafeUtils.appendEventsVolatile(cmd, binaryEventsChain);
            });

            return CommandResultCode.SUCCESS;
        } else {
            return CommandResultCode.ACCEPTED;
        }
    }

    @NotNull
    public static Object deserializeObject(BytesIn bytesIn) {

        final int classCode = bytesIn.readInt();

        switch (classCode) {
            case 1002:
                return new BatchAddSymbolsCommand(bytesIn);
            case 1003:
                return new BatchAddAccountsCommand(bytesIn);
            default:
                throw new IllegalStateException("Unsupported classCode: " + classCode);
        }
    }

    @NotNull
    private Optional<ReportQuery<?>> deserializeQuery(BytesIn bytesIn) {

        final int classCode = bytesIn.readInt();

        final Constructor<? extends ReportQuery<?>> constructor = queriesConfiguration.getReportClasses().get(classCode);
        if (constructor == null) {
            log.error("Unknown Report Query class code: {}", classCode);
            return Optional.empty();
        }

        try {
            return Optional.of(constructor.newInstance(bytesIn));

        } catch (final IllegalAccessException | InstantiationException | InvocationTargetException ex) {
            log.error("Failed to deserialize report instance of class {} error: {}", constructor.getDeclaringClass().getSimpleName(), ex.getMessage());
            return Optional.empty();
        }
    }

    @NotNull
    public static NativeBytes<Void> serializeObject(WriteBytesMarshallable data, int objectType) {
        final NativeBytes<Void> bytes = Bytes.allocateElasticDirect(128);
        bytes.writeInt(objectType);
        data.writeMarshallable(bytes);
        return bytes;
    }

    public void reset() {
        incomingData.clear();
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {

        // write symbolSpecs
        SerializationUtils.marshallLongHashMap(incomingData, bytes);
    }

    @Override
    public int stateHash() {
        return HashingUtils.stateHash(incomingData);
    }


    private static class TransferRecord implements WriteBytesMarshallable, StateHash {

        private long[] dataArray;
        private int wordsTransfered;

        public TransferRecord() {
            this.wordsTransfered = 0;
            this.dataArray = new long[256];
        }

        public TransferRecord(BytesIn bytes) {
            wordsTransfered = bytes.readInt();
            this.dataArray = SerializationUtils.readLongArray(bytes);
        }

        public void addWord(long word) {

            if (wordsTransfered == dataArray.length) {
                long[] newArray = new long[dataArray.length * 2];
                System.arraycopy(dataArray, 0, newArray, 0, dataArray.length);
                dataArray = newArray;
            }

            dataArray[wordsTransfered++] = word;

        }

        @Override
        public void writeMarshallable(BytesOut bytes) {
            bytes.writeInt(wordsTransfered);
            SerializationUtils.marshallLongArray(dataArray, bytes);
        }

        @Override
        public int stateHash() {
            return Objects.hash(Arrays.hashCode(dataArray), wordsTransfered);
        }
    }

}
