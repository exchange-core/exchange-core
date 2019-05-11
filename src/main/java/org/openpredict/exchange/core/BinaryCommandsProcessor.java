package org.openpredict.exchange.core;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.nustaq.serialization.FSTConfiguration;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.function.Function;

/**
 * Stateful Binary Commands Processor
 * <p>
 * Has incoming data buffer
 * Can receive events in arbitrary order and duplicates - at-least-once-delivery compatible.
 */
@Slf4j
@RequiredArgsConstructor
public final class BinaryCommandsProcessor {

    // transactionId -> TransferRecord (long array + bitset)
    private final LongObjectHashMap<TransferRecord> incomingData = new LongObjectHashMap<>();

    private final Function<CoreSymbolSpecification, CommandResultCode> symbolsConsumer;

    private final CommandResultCode acceptedResultCode;

    private final static FSTConfiguration minBin = FSTConfiguration.createMinBinConfiguration();

    static {
        minBin.registerCrossPlatformClassMappingUseSimpleName(CoreSymbolSpecification.class);
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

    private static class TransferRecord {
        public TransferRecord(int dataSizeBytes) {

            int longArraySize = Utils.requiredLongArraySize(dataSizeBytes);

            this.dataArray = new long[longArraySize];
            this.framesReceived = new BitSet(longArraySize);
        }

        private final long[] dataArray;
        private final BitSet framesReceived;
    }

}
