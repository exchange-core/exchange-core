package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.*;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.MatcherTradeEvent;
import org.openpredict.exchange.beans.StateHash;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.reports.SingleUserReportQuery;
import org.openpredict.exchange.beans.reports.StateHashReportQuery;
import org.openpredict.exchange.beans.reports.TotalCurrencyBalanceReportQuery;
import org.openpredict.exchange.core.orderbook.OrderBookEventsHelper;

import java.util.Arrays;
import java.util.BitSet;
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

        final int dataSizeBytes = (int) (cmd.size >> 32);
        final int position = (int) (cmd.size);
        final long transactionId = cmd.orderId;
        final long dataFrame = cmd.price;

        //log.debug("transactionId={}, position={}, dataFrame={}, sizeInBytes={}", transactionId, position, dataFrame, sizeInBytes);

        final TransferRecord record = incomingData.getIfAbsentPut(transactionId, () -> new TransferRecord(dataSizeBytes));

        final long[] dataArray = record.dataArray;
        dataArray[position] = dataFrame;

        final BitSet framesReceived = record.framesReceived;
        framesReceived.set(position);

        //int x = dataArray.length ;
        //log.debug("previousClearBit({}) = {}, {}", x, framesReceived.previousClearBit(x), framesReceived);

        if (framesReceived.previousClearBit(dataArray.length - 1) == -1) {
            // all frames received
            //log.debug("OBJ={}", object);
            incomingData.removeKey(transactionId);

            final BytesIn bytesIn = Utils.longsToWire(dataArray).bytes();
            final Object obj;
            int classCode = bytesIn.readInt();
            switch (classCode) {
                case 1002:
                    obj = new CoreSymbolSpecification(bytesIn);
                    break;
                case 2001:
                    obj = new StateHashReportQuery(bytesIn);
                    break;
                case 2002:
                    obj = new SingleUserReportQuery(bytesIn);
                    break;
                case 2003:
                    obj = new TotalCurrencyBalanceReportQuery(bytesIn);
                    break;
                default:
                    throw new IllegalStateException("Unsupported classCode: " + classCode);
            }

            completeMessagesHandler.apply(obj).ifPresent(res -> {
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

        private final long[] dataArray;
        private final BitSet framesReceived;

        public TransferRecord(int dataSizeBytes) {

            int longArraySize = Utils.requiredLongArraySize(dataSizeBytes);

            this.dataArray = new long[longArraySize];
            this.framesReceived = new BitSet(longArraySize);
        }

        public TransferRecord(BytesIn bytes) {
            this.dataArray = Utils.readLongArray(bytes);
            this.framesReceived = Utils.readBitSet(bytes);
        }

        @Override
        public void writeMarshallable(BytesOut bytes) {
            Utils.marshallLongArray(dataArray, bytes);
            Utils.marshallBitSet(framesReceived, bytes);
        }

        @Override
        public int stateHash() {
            return Objects.hash(Arrays.hashCode(dataArray), Utils.stateHash(framesReceived));
        }
    }

}
