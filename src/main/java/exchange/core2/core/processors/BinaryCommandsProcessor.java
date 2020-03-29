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

import exchange.core2.core.ExchangeApi;
import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.StateHash;
import exchange.core2.core.common.api.binary.BinaryDataCommand;
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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Stateful Binary Commands Processor
 * <p>
 * Has incoming data buffer
 * Can receive events in arbitrary order and duplicates - at-least-once-delivery compatible.
 */
@Slf4j
public final class BinaryCommandsProcessor implements WriteBytesMarshallable, StateHash {

    // TODO connect object pool

    // transactionId -> TransferRecord (long array + bitset)
    private final LongObjectHashMap<TransferRecord> incomingData;

    // TODO improve type (Object is not ok)
    private final Consumer<BinaryDataCommand> completeMessagesHandler;

    private final ReportQueriesHandler reportQueriesHandler;

    private final OrderBookEventsHelper eventsHelper;

    private final ReportsQueriesConfiguration queriesConfiguration;

    private final int section;

    public BinaryCommandsProcessor(final Consumer<BinaryDataCommand> completeMessagesHandler,
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

    public BinaryCommandsProcessor(final Consumer<BinaryDataCommand> completeMessagesHandler,
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

        final TransferRecord record = incomingData.getIfAbsentPut(
                transferId,
                () -> {
                    final int bytesLength = (int) (cmd.orderId >> 32) & 0x7FFF_FFFF;
                    final int longArraySize = SerializationUtils.requiredLongArraySize(bytesLength, ExchangeApi.LONGS_PER_MESSAGE);
//            log.debug("EXPECTED: bytesLength={} longArraySize={}", bytesLength, longArraySize);
                    return new TransferRecord(longArraySize);
                });

        record.addWord(cmd.orderId);
        record.addWord(cmd.price);
        record.addWord(cmd.reserveBidPrice);
        record.addWord(cmd.size);
        record.addWord(cmd.uid);

        if (cmd.symbol == -1) {
            // all frames received

            incomingData.removeKey(transferId);

            final BytesIn bytesIn = SerializationUtils.longsLz4ToWire(record.dataArray, record.wordsTransfered).bytes();

            if (cmd.command == OrderCommandType.BINARY_DATA_QUERY) {

                deserializeQuery(bytesIn)
                        .flatMap(reportQueriesHandler::handleReport)
                        .ifPresent(res -> {
                            final NativeBytes<Void> bytes = Bytes.allocateElasticDirect(128);
                            res.writeMarshallable(bytes);
                            final MatcherTradeEvent binaryEventsChain = eventsHelper.createBinaryEventsChain(cmd.timestamp, section, bytes);
                            UnsafeUtils.appendEventsVolatile(cmd, binaryEventsChain);
                        });

            } else if (cmd.command == OrderCommandType.BINARY_DATA_COMMAND) {

//                log.debug("Unpack {} words", record.wordsTransfered);
                final BinaryDataCommand binaryDataCommand = deserializeBinaryCommand(bytesIn);
//                log.debug("Succeed");
                completeMessagesHandler.accept(binaryDataCommand);

            } else {
                throw new IllegalStateException();
            }


            return CommandResultCode.SUCCESS;
        } else {
            return CommandResultCode.ACCEPTED;
        }
    }

    private BinaryDataCommand deserializeBinaryCommand(BytesIn bytesIn) {

        final int classCode = bytesIn.readInt();

        final Constructor<? extends BinaryDataCommand> constructor = queriesConfiguration.getBinaryCommandConstructors().get(classCode);
        if (constructor == null) {
            throw new IllegalStateException("Unknown Binary Data Command class code: " + classCode);
        }

        try {
            return constructor.newInstance(bytesIn);

        } catch (final IllegalAccessException | InstantiationException | InvocationTargetException ex) {
            throw new IllegalStateException("Failed to deserialize Binary Data Command instance of class " + constructor.getDeclaringClass().getSimpleName(), ex);
        }
    }

    private Optional<ReportQuery<?>> deserializeQuery(BytesIn bytesIn) {

        final int classCode = bytesIn.readInt();

        final Constructor<? extends ReportQuery<?>> constructor = queriesConfiguration.getReportConstructors().get(classCode);
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

        public TransferRecord(int expectedLength) {
            this.wordsTransfered = 0;
            this.dataArray = new long[expectedLength];
        }

        public TransferRecord(BytesIn bytes) {
            wordsTransfered = bytes.readInt();
            this.dataArray = SerializationUtils.readLongArray(bytes);
        }

        public void addWord(long word) {

            if (wordsTransfered == dataArray.length) {
                // should never happen
                log.warn("Resizing incoming transfer buffer to {} longs", dataArray.length * 2);
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
