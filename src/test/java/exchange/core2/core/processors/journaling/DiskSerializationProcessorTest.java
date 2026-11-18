package exchange.core2.core.processors.journaling;

import com.lmax.disruptor.RingBuffer;
import exchange.core2.core.ExchangeApi;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.ExchangeConfiguration;
import exchange.core2.core.common.config.InitialStateConfiguration;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;

class DiskSerializationProcessorTest {

    @Test
    void shutdownBeforeFirstJournaledCommand(@TempDir Path journalDirectory) throws IOException {
        final LZ4Compressor compressor = LZ4Factory.fastestJavaInstance().fastCompressor();
        final DiskSerializationProcessor processor = new DiskSerializationProcessor(
                ExchangeConfiguration.defaultBuilder()
                        .initStateCfg(InitialStateConfiguration.cleanStartJournaling("test"))
                        .build(),
                DiskSerializationProcessorConfiguration.builder()
                        .storageFolder(journalDirectory.toString())
                        .journalFileMaxSize(1024 * 1024)
                        .journalBufferSize(4096)
                        .journalBatchCompressThreshold(2048)
                        .snapshotLz4CompressorFactory(() -> compressor)
                        .journalLz4CompressorFactory(() -> compressor)
                        .build());
        final ExchangeApi api = new ExchangeApi(RingBuffer.createSingleProducer(OrderCommand::new, 8), compressor);
        processor.enableJournaling(0, api);

        final OrderCommand shutdown = new OrderCommand();
        shutdown.command = OrderCommandType.SHUTDOWN_SIGNAL;
        processor.writeToJournal(shutdown, 1, true);

        try (Stream<Path> files = Files.list(journalDirectory)) {
            assertFalse(files.findAny().isPresent());
        }
    }
}
