/*
 * Copyright 2019-2026 Maksim Zheravin
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
package exchange.core2.core.processors.journaling;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.util.function.Supplier;

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

    public DiskSerializationProcessorConfiguration(
            String storageFolder,
            Supplier<LZ4Compressor> snapshotLz4CompressorFactory,
            long journalFileMaxSize,
            int journalBufferSize,
            int journalBatchCompressThreshold,
            Supplier<LZ4Compressor> journalLz4CompressorFactory) {
        this.storageFolder = storageFolder;
        this.snapshotLz4CompressorFactory = snapshotLz4CompressorFactory;
        this.journalFileMaxSize = journalFileMaxSize;
        this.journalBufferSize = journalBufferSize;
        this.journalBatchCompressThreshold = journalBatchCompressThreshold;
        this.journalLz4CompressorFactory = journalLz4CompressorFactory;
    }

    public static DiskSerializationProcessorConfigurationBuilder builder() {
        return new DiskSerializationProcessorConfigurationBuilder();
    }

    public String getStorageFolder() {
        return this.storageFolder;
    }

    public Supplier<LZ4Compressor> getSnapshotLz4CompressorFactory() {
        return this.snapshotLz4CompressorFactory;
    }

    public long getJournalFileMaxSize() {
        return this.journalFileMaxSize;
    }

    public int getJournalBufferSize() {
        return this.journalBufferSize;
    }

    public int getJournalBatchCompressThreshold() {
        return this.journalBatchCompressThreshold;
    }

    public Supplier<LZ4Compressor> getJournalLz4CompressorFactory() {
        return this.journalLz4CompressorFactory;
    }

    public static class DiskSerializationProcessorConfigurationBuilder {
        private String storageFolder;
        private Supplier<LZ4Compressor> snapshotLz4CompressorFactory;
        private long journalFileMaxSize;
        private int journalBufferSize;
        private int journalBatchCompressThreshold;
        private Supplier<LZ4Compressor> journalLz4CompressorFactory;

        DiskSerializationProcessorConfigurationBuilder() {
        }

        public DiskSerializationProcessorConfigurationBuilder storageFolder(String storageFolder) {
            this.storageFolder = storageFolder;
            return this;
        }

        public DiskSerializationProcessorConfigurationBuilder snapshotLz4CompressorFactory(Supplier<LZ4Compressor> snapshotLz4CompressorFactory) {
            this.snapshotLz4CompressorFactory = snapshotLz4CompressorFactory;
            return this;
        }

        public DiskSerializationProcessorConfigurationBuilder journalFileMaxSize(long journalFileMaxSize) {
            this.journalFileMaxSize = journalFileMaxSize;
            return this;
        }

        public DiskSerializationProcessorConfigurationBuilder journalBufferSize(int journalBufferSize) {
            this.journalBufferSize = journalBufferSize;
            return this;
        }

        public DiskSerializationProcessorConfigurationBuilder journalBatchCompressThreshold(int journalBatchCompressThreshold) {
            this.journalBatchCompressThreshold = journalBatchCompressThreshold;
            return this;
        }

        public DiskSerializationProcessorConfigurationBuilder journalLz4CompressorFactory(Supplier<LZ4Compressor> journalLz4CompressorFactory) {
            this.journalLz4CompressorFactory = journalLz4CompressorFactory;
            return this;
        }

        public DiskSerializationProcessorConfiguration build() {
            return new DiskSerializationProcessorConfiguration(
                    this.storageFolder,
                    this.snapshotLz4CompressorFactory,
                    this.journalFileMaxSize,
                    this.journalBufferSize,
                    this.journalBatchCompressThreshold,
                    this.journalLz4CompressorFactory);
        }

        @Override
        public String toString() {
            return "DiskSerializationProcessorConfiguration.DiskSerializationProcessorConfigurationBuilder(storageFolder=" + this.storageFolder
                    + ", snapshotLz4CompressorFactory=" + this.snapshotLz4CompressorFactory
                    + ", journalFileMaxSize=" + this.journalFileMaxSize
                    + ", journalBufferSize=" + this.journalBufferSize
                    + ", journalBatchCompressThreshold=" + this.journalBatchCompressThreshold
                    + ", journalLz4CompressorFactory=" + this.journalLz4CompressorFactory + ")";
        }
    }
}
