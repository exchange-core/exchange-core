package exchange.core2.core.processors.journaling;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@AllArgsConstructor
@Getter
@Builder
public class DiskSerializationProcessorConfiguration {

    public static final String DEFAULT_FOLDER = "./dumps";

    private static final long ONE_MEGABYTE = 1024 * 1024;

    private final String storageFolder;
    private final long journalFileMaxSize;
    private final int journalBufferSize;

    // use LZ4 compression for batches threshold if batch size exceeds this value
    private final int journalBatchCompressThreshold;

    public static DiskSerializationProcessorConfiguration createDefaultConfig() {

        return DiskSerializationProcessorConfiguration.builder()
                .journalFileMaxSize(4 * 1024 * ONE_MEGABYTE)
                .journalBufferSize(256 * 1024) // 256 KB - TODO calculate based on ringBufferSize
                .storageFolder(DEFAULT_FOLDER)
                .journalBatchCompressThreshold(1024)
                .build();
    }
}
