package exchange.core2.core.processors.journaling;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.util.function.Supplier;

@AllArgsConstructor
@Getter
@Builder
public class DiskSerializationProcessorConfiguration {

    public static final String DEFAULT_FOLDER = "./dumps";

    private static final long ONE_MEGABYTE = 1024 * 1024;

    public static final Supplier<LZ4Compressor> LZ4_FAST = () -> LZ4Factory.fastestInstance().fastCompressor();
    public static final Supplier<LZ4Compressor> LZ4_HIGH = () -> LZ4Factory.fastestInstance().highCompressor();

    private final String storageFolder;

    // -------- snapshot settings ---------------

    // Snapshots LZ4 compressor
    // note: using LZ4 HIGH will require about twice more time
    private final Supplier<LZ4Compressor> snapshotLz4CompressorFactory;

    // -------- journal settings ---------------

    private final long journalFileMaxSize;
    private final int journalBufferSize;

    // use LZ4 compression if batch size (in bytes) exceeds this value for batches threshold
    // average batch size depends on traffic and disk write delay and can reach up to 20-100 kilobytes (3M TPS and 0.15ms disk write delay)
    // under moderate load for single messages compression is never used
    private final int journalBatchCompressThreshold;

    // Journals LZ4 compressor
    // note: using LZ4 HIGH is not recommended because of very high impact on throughput
    private final Supplier<LZ4Compressor> journalLz4CompressorFactory;

    public static DiskSerializationProcessorConfiguration createDefaultConfig() {

        return DiskSerializationProcessorConfiguration.builder()
                .storageFolder(DEFAULT_FOLDER)
                .snapshotLz4CompressorFactory(LZ4_FAST)
                .journalFileMaxSize(4000 * ONE_MEGABYTE)
                .journalBufferSize(256 * 1024) // 256 KB - TODO calculate based on ringBufferSize
                .journalBatchCompressThreshold(2048)
                .journalLz4CompressorFactory(LZ4_FAST)
                .build();
    }
}
