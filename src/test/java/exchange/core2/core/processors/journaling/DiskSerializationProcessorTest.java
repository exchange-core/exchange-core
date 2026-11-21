package exchange.core2.core.processors.journaling;

import com.lmax.disruptor.RingBuffer;
import exchange.core2.core.ExchangeApi;
import exchange.core2.core.common.Order;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.ExchangeConfiguration;
import exchange.core2.core.common.config.InitialStateConfiguration;
import exchange.core2.core.orderbook.OrderBookDirectImpl.DirectOrder;
import net.jpountz.lz4.LZ4Compressor;
import net.openhft.chronicle.bytes.Bytes;
import net.jpountz.lz4.LZ4Factory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class DiskSerializationProcessorTest {

    @Test
    void shutdownBeforeFirstJournaledCommand(@TempDir Path journalDirectory) throws IOException {
        final LZ4Compressor compressor = LZ4Factory.fastestJavaInstance().fastCompressor();
        final InitialStateConfiguration initial = InitialStateConfiguration.cleanStartJournaling("test");
        final DiskSerializationProcessor processor = createProcessor(journalDirectory, initial, compressor);
        final ExchangeApi api = new ExchangeApi(RingBuffer.createSingleProducer(OrderCommand::new, 8), compressor);
        processor.enableJournaling(0, api);

        final OrderCommand shutdown = new OrderCommand();
        shutdown.command = OrderCommandType.SHUTDOWN_SIGNAL;
        processor.writeToJournal(shutdown, 1, true);

        try (Stream<Path> files = Files.list(journalDirectory)) {
            assertFalse(files.findAny().isPresent());
        }
    }

    private static DiskSerializationProcessor createProcessor(Path journalDirectory, InitialStateConfiguration initial,
                                                              LZ4Compressor compressor) {
        return new DiskSerializationProcessor(
                ExchangeConfiguration.defaultBuilder()
                        .initStateCfg(initial)
                        .build(),
                DiskSerializationProcessorConfiguration.builder()
                        .storageFolder(journalDirectory.toString())
                        .journalFileMaxSize(1024 * 1024)
                        .journalBufferSize(4096)
                        .journalBatchCompressThreshold(2048)
                        .snapshotLz4CompressorFactory(() -> compressor)
                        .journalLz4CompressorFactory(() -> compressor)
                        .build());
    }

    @Test
    void shouldReplayLegacyPlaceAndSuspendBetweenTaggedFeeOrders(@TempDir Path journalDirectory) throws IOException {
        final LZ4Compressor compressor = LZ4Factory.fastestJavaInstance().fastCompressor();
        final var initial = InitialStateConfiguration.cleanStartJournaling("test");
        final var writer = createProcessor(journalDirectory, initial, compressor);
        writer.enableJournaling(0, new ExchangeApi(RingBuffer.createSingleProducer(OrderCommand::new, 8), compressor));

        final OrderCommand first = OrderCommand.newOrder(OrderType.GTC, 100, 7, 10, 12, 3, OrderAction.BID);
        writer.writeToJournal(first, 1, true);
        final OrderCommand suspend = new OrderCommand();
        suspend.command = OrderCommandType.SUSPEND_USER;
        suspend.uid = 42;
        writer.writeToJournal(suspend, 2, true);
        final OrderCommand withFees = OrderCommand.newOrder(OrderType.GTC, 101, 8, 11, 11, 4, OrderAction.ASK);
        withFees.orderTakerFee = 4;
        withFees.orderMakerFee = 9;
        writer.writeToJournal(withFees, 3, true);
        final OrderCommand withdrawal = new OrderCommand();
        withdrawal.command = OrderCommandType.BALANCE_ADJUSTMENT;
        withdrawal.uid = 7;
        withdrawal.symbol = 20;
        withdrawal.orderId = 2;
        withdrawal.price = -3;
        withdrawal.orderType = OrderType.GTC;
        withdrawal.serviceFlags = OrderCommand.LEGACY_WITHDRAWAL_REPLAY;
        writer.writeToJournal(withdrawal, 4, true);
        withdrawal.serviceFlags = 0;
        withdrawal.orderId = 3;
        writer.writeToJournal(withdrawal, 5, true);
        final OrderCommand shutdown = new OrderCommand();
        shutdown.command = OrderCommandType.SHUTDOWN_SIGNAL;
        writer.writeToJournal(shutdown, 6, true);

        // Validate the boundaries of the two legacy records independently of the reader.
        final byte[] data = Files.readAllBytes(journalDirectory.resolve("test_journal_0_0001.ecj"));
        assertEquals(OrderCommandType.PLACE_ORDER.getCode(), data[0]);
        final int legacyPlaceOrderLength = 78;
        final int legacySuspendLength = 37;
        assertEquals(OrderCommandType.SUSPEND_USER.getCode(), data[legacyPlaceOrderLength]);
        assertEquals(5, data[legacyPlaceOrderLength + legacySuspendLength]);

        final var replay = InitialStateConfiguration.lastKnownStateFromJournal("test", 0, 0);
        final var reader = createProcessor(journalDirectory, replay, compressor);
        final var ring = RingBuffer.createSingleProducer(OrderCommand::new, 8);
        assertEquals(5L, reader.replayJournalFull(replay, new ExchangeApi(ring, compressor)));
        assertEquals(100L, ring.get(1).orderId);
        assertEquals(-1L, ring.get(1).orderTakerFee);
        assertEquals(-1L, ring.get(1).orderMakerFee);
        assertEquals(OrderCommandType.SUSPEND_USER, ring.get(2).command);
        assertEquals(42L, ring.get(2).uid);
        assertEquals(101L, ring.get(3).orderId);
        assertEquals(4L, ring.get(3).orderTakerFee);
        assertEquals(9L, ring.get(3).orderMakerFee);
        assertEquals(OrderCommandType.BALANCE_ADJUSTMENT, ring.get(4).command);
        assertEquals(OrderCommand.LEGACY_WITHDRAWAL_REPLAY, ring.get(4).serviceFlags);
        assertEquals(OrderCommandType.BALANCE_ADJUSTMENT, ring.get(5).command);
        assertEquals(0, ring.get(5).serviceFlags);
    }

    @Test
    void shouldReadLegacyOrdersAndKeepFeeOverridesInSnapshots() {
        final var bytes = Bytes.allocateElasticOnHeap();
        try {
            bytes.writeLong(77).writeLong(100).writeLong(5).writeLong(2).writeLong(110);
            bytes.writeByte(OrderAction.BID.getCode()).writeLong(8).writeLong(1234);
            final var legacy = new Order(bytes);
            assertEquals(1234L, legacy.timestamp);
            assertEquals(-1L, legacy.orderTakerFee);
            assertEquals(-1L, legacy.orderMakerFee);

            bytes.clear();
            legacy.writeMarshallable(bytes);
            assertEquals(57L, bytes.writePosition());
            final var direct = new DirectOrder(bytes);
            assertEquals(77L, direct.orderId);
            assertEquals(legacy.stateHash(), direct.stateHash());

            bytes.clear();
            direct.orderTakerFee = 4;
            direct.orderMakerFee = 8;
            direct.writeMarshallable(bytes);
            final var withFees = new Order(bytes);
            assertEquals(1234L, withFees.timestamp);
            assertEquals(4L, withFees.orderTakerFee);
            assertEquals(8L, withFees.orderMakerFee);
            assertEquals(direct.stateHash(), withFees.stateHash());
        } finally {
            bytes.releaseLast();
        }
    }
}
