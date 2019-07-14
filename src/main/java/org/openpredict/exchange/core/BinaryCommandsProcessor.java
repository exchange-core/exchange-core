package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.*;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.jetbrains.annotations.NotNull;
import org.openpredict.exchange.beans.MatcherTradeEvent;
import org.openpredict.exchange.beans.StateHash;
import org.openpredict.exchange.beans.api.binary.BatchAddAccountsCommand;
import org.openpredict.exchange.beans.api.binary.BatchAddSymbolsCommand;
import org.openpredict.exchange.beans.api.reports.SingleUserReportQuery;
import org.openpredict.exchange.beans.api.reports.StateHashReportQuery;
import org.openpredict.exchange.beans.api.reports.TotalCurrencyBalanceReportQuery;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.core.orderbook.OrderBookEventsHelper;

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

    private final Function<Object, Optional<? extends WriteBytesMarshallable>> completeMessagesHandler;

    private final int section;

    public BinaryCommandsProcessor(Function<Object, Optional<? extends WriteBytesMarshallable>> completeMessagesHandler, int section) {
        this.completeMessagesHandler = completeMessagesHandler;
        this.incomingData = new LongObjectHashMap<>();
        this.section = section;
    }

    public BinaryCommandsProcessor(Function<Object, Optional<? extends WriteBytesMarshallable>> completeMessagesHandler, BytesIn bytesIn, int section) {
        this.completeMessagesHandler = completeMessagesHandler;
        this.incomingData = Utils.readLongHashMap(bytesIn, b -> new TransferRecord(bytesIn));
        this.section = section;
    }

    public boolean acceptBinaryFrame(OrderCommand cmd) {

        final int transferId = cmd.userCookie;

        final TransferRecord record = incomingData.getIfAbsentPut(transferId, TransferRecord::new);

        record.addWord(cmd.orderId);
        record.addWord(cmd.price);
        record.addWord(cmd.reserveBidPrice);
        record.addWord(cmd.size);
        record.addWord(cmd.uid);


        if (cmd.symbol == -1) {
            // all frames received
            //log.debug("OBJ={}", object);
            incomingData.removeKey(transferId);

            final BytesIn bytesIn = Utils.longsToWire(record.dataArray).bytes();


            completeMessagesHandler.apply(deserializeObject(bytesIn)).ifPresent(res -> {
                final NativeBytes<Void> bytes = Bytes.allocateElasticDirect(128);
                res.writeMarshallable(bytes);
                final MatcherTradeEvent binaryEventsChain = OrderBookEventsHelper.createBinaryEventsChain(cmd.timestamp, section, bytes);
                Utils.appendEventsVolatile(cmd, binaryEventsChain);
            });

            return true;
        } else {
            return false;
        }

    }

    @NotNull
    public static Object deserializeObject(BytesIn bytesIn) {

        int classCode = bytesIn.readInt();

        switch (classCode) {
            case 1002:
                return new BatchAddSymbolsCommand(bytesIn);
            case 1003:
                return new BatchAddAccountsCommand(bytesIn);
            case 2001:
                return new StateHashReportQuery(bytesIn);
            case 2002:
                return new SingleUserReportQuery(bytesIn);
            case 2003:
                return new TotalCurrencyBalanceReportQuery(bytesIn);
            default:
                throw new IllegalStateException("Unsupported classCode: " + classCode);
        }
    }

    @NotNull
    public static NativeBytes<Void> serializeObject(WriteBytesMarshallable data) {
        final NativeBytes<Void> bytes = Bytes.allocateElasticDirect(128);
        if (data instanceof BatchAddSymbolsCommand) {
            bytes.writeInt(1002);
        } else if (data instanceof BatchAddAccountsCommand) {
            bytes.writeInt(1003);
        } else if (data instanceof StateHashReportQuery) {
            bytes.writeInt(2001);
        } else if (data instanceof SingleUserReportQuery) {
            bytes.writeInt(2002);
        } else if (data instanceof TotalCurrencyBalanceReportQuery) {
            bytes.writeInt(2003);
        } else {
            throw new IllegalStateException("Unsupported class: " + data.getClass());
        }
        data.writeMarshallable(bytes);
        return bytes;
    }

    public void reset() {
        incomingData.clear();
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {

        // write symbolSpecs
        Utils.marshallLongHashMap(incomingData, bytes);
    }

    @Override
    public int stateHash() {
        return Utils.stateHash(incomingData);
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
            this.dataArray = Utils.readLongArray(bytes);
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
            Utils.marshallLongArray(dataArray, bytes);
        }

        @Override
        public int stateHash() {
            return Objects.hash(Arrays.hashCode(dataArray), wordsTransfered);
        }
    }

}
