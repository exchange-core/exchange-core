package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.nustaq.serialization.FSTConfiguration;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.StateHash;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;
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

    private final Function<CoreSymbolSpecification, CommandResultCode> symbolsConsumer;

    private final CommandResultCode acceptedResultCode;

    private final static FSTConfiguration minBin = FSTConfiguration.createMinBinConfiguration();

    static {
        minBin.registerCrossPlatformClassMappingUseSimpleName(CoreSymbolSpecification.class);
    }

    public BinaryCommandsProcessor(Function<CoreSymbolSpecification, CommandResultCode> symbolsConsumer, CommandResultCode acceptedResultCode) {
        this.symbolsConsumer = symbolsConsumer;
        this.acceptedResultCode = acceptedResultCode;
        this.incomingData = new LongObjectHashMap<>();
    }

    public BinaryCommandsProcessor(Function<CoreSymbolSpecification, CommandResultCode> symbolsConsumer,
                                   CommandResultCode acceptedResultCode,
                                   BytesIn bytesIn) {
        this.symbolsConsumer = symbolsConsumer;
        this.acceptedResultCode = acceptedResultCode;
        this.incomingData = Utils.readLongHashMap(bytesIn, b -> new TransferRecord(bytesIn));
    }

    public CommandResultCode binaryData(OrderCommand cmd) {

        final int dataSizeBytes = (int) (cmd.size >> 32);
        final int frameIndex = (int) (cmd.size);

        Object obj = registerData(cmd.orderId, frameIndex, cmd.price, dataSizeBytes);
        if (obj == null) {
            return acceptedResultCode;

        } else if (obj instanceof CoreSymbolSpecification) {

            final CoreSymbolSpecification symbolSpecification = (CoreSymbolSpecification) obj;

            return symbolsConsumer.apply(symbolSpecification);

        } else {
            return CommandResultCode.BINARY_COMMAND_FAILED;
        }
    }

    private Object registerData(long transactionId, int position, long dataFrame, int sizeInBytes) {

        //log.debug("transactionId={}, position={}, dataFrame={}, sizeInBytes={}", transactionId, position, dataFrame, sizeInBytes);

        final TransferRecord record = incomingData.getIfAbsentPut(transactionId, () -> new TransferRecord(sizeInBytes));

        final long[] dataArray = record.dataArray;
        dataArray[position] = dataFrame;

        final BitSet framesReceived = record.framesReceived;
        framesReceived.set(position);

        //int x = dataArray.length ;
        //log.debug("previousClearBit({}) = {}, {}", x, framesReceived.previousClearBit(x), framesReceived);
        if (framesReceived.previousClearBit(dataArray.length - 1) == -1) {
            // all frames received
            final ByteBuffer byteBuffer = ByteBuffer.allocate(dataArray.length * 8);
            byteBuffer.asLongBuffer().put(dataArray);

            final byte[] bytes = new byte[sizeInBytes];
            byteBuffer.get(bytes);

            //log.debug("byte[{}]={}", bytes.length, bytes);

            Object object = minBin.asObject(bytes);
            //byteBuffer.get()

            //log.debug("OBJ={}", object);

            incomingData.removeKey(transactionId);

            return object;
        }

        return null;
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
