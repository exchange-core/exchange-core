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
package exchange.core2.core.common.config;

import exchange.core2.core.common.CoreWaitStrategy;
import exchange.core2.core.orderbook.IOrderBook;
import exchange.core2.core.orderbook.OrderBookDirectImpl;
import exchange.core2.core.orderbook.OrderBookNaiveImpl;
import exchange.core2.core.utils.AffinityThreadFactory;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

/**
 * Exchange performance configuration
 */
public final class PerformanceConfiguration {

    public static final PerformanceConfiguration DEFAULT = PerformanceConfiguration.baseBuilder().build();

    /*
     * Disruptor ring buffer size (number of commands). Must be power of 2.
     */
    private final int ringBufferSize;

    /*
     * Number of matching engines. Each instance requires extra CPU core.
     */
    private final int matchingEnginesNum;

    /*
     * Number of risk engines. Each instance requires extra CPU core.
     */
    private final int riskEnginesNum;

    /*
     * max number of messages not processed by R2 stage. Must be less than quarter of ringBufferSize.
     * Lower values, like 100, provide better mean latency.
     * Higher values, like 2000 provide better throughput and tail latency.
     */
    private final int msgsInGroupLimit;


    /*
     * max interval when messages not processed by R2 stage.
     * Interfere with msgsInGroupLimit parameter.
     * Lower values, like 1000 (1us), provide better mean latency.
     * Higher values, like 2000 provide better throughput and tail latency.
     */
    private final int maxGroupDurationNs;

    /*
     * send L2 for every successfully executed command
     *
     * Regular L2 updates is important for Risk Processor, to evaluate PnL for margin trading.
     * By default (false), Matching Engine sends L2 only when requested by Grouping Processor (every 10ms).
     * When true - L2 data will be sent for every successfully executed command.
     * Enabling this will impact the performance.
     *
     */
    private final boolean sendL2ForEveryCmd;

    /*
     * Depth of Regular L2 updates.
     * Default is 8 (sufficient for Risk Processor because it does not check order book depth)
     * If set Integer.MAX_VALUE - full order book will be sent.
     */
    private final int l2RefreshDepth;

    /*
     * Disruptor threads factory
     */
    private final ThreadFactory threadFactory;

    /*
     * Disruptor wait strategy
     */
    private final CoreWaitStrategy waitStrategy;

    /*
     * Order books factory
     */
    private final IOrderBook.OrderBookFactory orderBookFactory;

    /*
     * LZ4 compressor factory for binary commands and reports
     */
    private final Supplier<LZ4Compressor> binaryCommandsLz4CompressorFactory;

    @Override
    public String toString() {
        return "PerformanceConfiguration{" +
                "ringBufferSize=" + ringBufferSize +
                ", matchingEnginesNum=" + matchingEnginesNum +
                ", riskEnginesNum=" + riskEnginesNum +
                ", msgsInGroupLimit=" + msgsInGroupLimit +
                ", maxGroupDurationNs=" + maxGroupDurationNs +
                ", sendL2ForEveryCmd=" + sendL2ForEveryCmd +
                ", l2RefreshDepth=" + l2RefreshDepth +
                ", threadFactory=" + (threadFactory == null ? null : threadFactory.getClass().getSimpleName()) +
                ", waitStrategy=" + waitStrategy +
                ", orderBookFactory=" + (orderBookFactory == null ? null : orderBookFactory.getClass().getSimpleName()) +
                ", binaryCommandsLz4CompressorFactory=" + (binaryCommandsLz4CompressorFactory == null ? null : binaryCommandsLz4CompressorFactory.getClass().getSimpleName()) +
                '}';
    }

    // TODO add expected number of users and symbols

    public static PerformanceConfigurationBuilder baseBuilder() {

        return builder()
                .ringBufferSize(16 * 1024)
                .matchingEnginesNum(1)
                .riskEnginesNum(1)
                .msgsInGroupLimit(256)
                .maxGroupDurationNs(10_000)
                .sendL2ForEveryCmd(false)
                .l2RefreshDepth(8)
                .threadFactory(Thread::new)
                .waitStrategy(CoreWaitStrategy.BLOCKING)
                .binaryCommandsLz4CompressorFactory(() -> LZ4Factory.fastestInstance().highCompressor())
                .orderBookFactory((IOrderBook.ConfiguredOrderBookFactory) OrderBookNaiveImpl::new);
    }

    public static PerformanceConfigurationBuilder latencyPerformanceBuilder() {

        return builder()
                .ringBufferSize(2 * 1024)
                .matchingEnginesNum(1)
                .riskEnginesNum(1)
                .msgsInGroupLimit(256)
                .maxGroupDurationNs(10_000)
                .sendL2ForEveryCmd(false)
                .l2RefreshDepth(8)
                .threadFactory(new AffinityThreadFactory(AffinityThreadFactory.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE))
                .waitStrategy(CoreWaitStrategy.BUSY_SPIN)
                .binaryCommandsLz4CompressorFactory(() -> LZ4Factory.fastestInstance().highCompressor())
                .orderBookFactory((IOrderBook.ConfiguredOrderBookFactory) OrderBookDirectImpl::new);
    }

    public static PerformanceConfigurationBuilder throughputPerformanceBuilder() {

        return builder()
                .ringBufferSize(64 * 1024)
                .matchingEnginesNum(4)
                .riskEnginesNum(2)
                .msgsInGroupLimit(4_096)
                .maxGroupDurationNs(4_000_000)
                .sendL2ForEveryCmd(false)
                .l2RefreshDepth(8)
                .threadFactory(new AffinityThreadFactory(AffinityThreadFactory.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE))
                .waitStrategy(CoreWaitStrategy.BUSY_SPIN)
                .binaryCommandsLz4CompressorFactory(() -> LZ4Factory.fastestInstance().highCompressor())
                .orderBookFactory((IOrderBook.ConfiguredOrderBookFactory) OrderBookDirectImpl::new);
    }

    public PerformanceConfiguration(
            int ringBufferSize,
            int matchingEnginesNum,
            int riskEnginesNum,
            int msgsInGroupLimit,
            int maxGroupDurationNs,
            boolean sendL2ForEveryCmd,
            int l2RefreshDepth,
            ThreadFactory threadFactory,
            CoreWaitStrategy waitStrategy,
            IOrderBook.OrderBookFactory orderBookFactory,
            Supplier<LZ4Compressor> binaryCommandsLz4CompressorFactory) {
        this.ringBufferSize = ringBufferSize;
        this.matchingEnginesNum = matchingEnginesNum;
        this.riskEnginesNum = riskEnginesNum;
        this.msgsInGroupLimit = msgsInGroupLimit;
        this.maxGroupDurationNs = maxGroupDurationNs;
        this.sendL2ForEveryCmd = sendL2ForEveryCmd;
        this.l2RefreshDepth = l2RefreshDepth;
        this.threadFactory = threadFactory;
        this.waitStrategy = waitStrategy;
        this.orderBookFactory = orderBookFactory;
        this.binaryCommandsLz4CompressorFactory = binaryCommandsLz4CompressorFactory;
    }

    public static PerformanceConfigurationBuilder builder() {
        return new PerformanceConfigurationBuilder();
    }

    public int getRingBufferSize() {
        return this.ringBufferSize;
    }

    public int getMatchingEnginesNum() {
        return this.matchingEnginesNum;
    }

    public int getRiskEnginesNum() {
        return this.riskEnginesNum;
    }

    public int getMsgsInGroupLimit() {
        return this.msgsInGroupLimit;
    }

    public int getMaxGroupDurationNs() {
        return this.maxGroupDurationNs;
    }

    public boolean isSendL2ForEveryCmd() {
        return this.sendL2ForEveryCmd;
    }

    public int getL2RefreshDepth() {
        return this.l2RefreshDepth;
    }

    public ThreadFactory getThreadFactory() {
        return this.threadFactory;
    }

    public CoreWaitStrategy getWaitStrategy() {
        return this.waitStrategy;
    }

    public IOrderBook.OrderBookFactory getOrderBookFactory() {
        return this.orderBookFactory;
    }

    public Supplier<LZ4Compressor> getBinaryCommandsLz4CompressorFactory() {
        return this.binaryCommandsLz4CompressorFactory;
    }

    public static class PerformanceConfigurationBuilder {
        private int ringBufferSize;
        private int matchingEnginesNum;
        private int riskEnginesNum;
        private int msgsInGroupLimit;
        private int maxGroupDurationNs;
        private boolean sendL2ForEveryCmd;
        private int l2RefreshDepth;
        private ThreadFactory threadFactory;
        private CoreWaitStrategy waitStrategy;
        private IOrderBook.OrderBookFactory orderBookFactory;
        private Supplier<LZ4Compressor> binaryCommandsLz4CompressorFactory;

        PerformanceConfigurationBuilder() {
        }

        public PerformanceConfigurationBuilder ringBufferSize(int ringBufferSize) {
            this.ringBufferSize = ringBufferSize;
            return this;
        }

        public PerformanceConfigurationBuilder matchingEnginesNum(int matchingEnginesNum) {
            this.matchingEnginesNum = matchingEnginesNum;
            return this;
        }

        public PerformanceConfigurationBuilder riskEnginesNum(int riskEnginesNum) {
            this.riskEnginesNum = riskEnginesNum;
            return this;
        }

        public PerformanceConfigurationBuilder msgsInGroupLimit(int msgsInGroupLimit) {
            this.msgsInGroupLimit = msgsInGroupLimit;
            return this;
        }

        public PerformanceConfigurationBuilder maxGroupDurationNs(int maxGroupDurationNs) {
            this.maxGroupDurationNs = maxGroupDurationNs;
            return this;
        }

        public PerformanceConfigurationBuilder sendL2ForEveryCmd(boolean sendL2ForEveryCmd) {
            this.sendL2ForEveryCmd = sendL2ForEveryCmd;
            return this;
        }

        public PerformanceConfigurationBuilder l2RefreshDepth(int l2RefreshDepth) {
            this.l2RefreshDepth = l2RefreshDepth;
            return this;
        }

        public PerformanceConfigurationBuilder threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public PerformanceConfigurationBuilder waitStrategy(CoreWaitStrategy waitStrategy) {
            this.waitStrategy = waitStrategy;
            return this;
        }

        public PerformanceConfigurationBuilder orderBookFactory(IOrderBook.OrderBookFactory orderBookFactory) {
            this.orderBookFactory = orderBookFactory;
            return this;
        }

        public PerformanceConfigurationBuilder binaryCommandsLz4CompressorFactory(Supplier<LZ4Compressor> binaryCommandsLz4CompressorFactory) {
            this.binaryCommandsLz4CompressorFactory = binaryCommandsLz4CompressorFactory;
            return this;
        }

        public PerformanceConfiguration build() {
            return new PerformanceConfiguration(
                    this.ringBufferSize,
                    this.matchingEnginesNum,
                    this.riskEnginesNum,
                    this.msgsInGroupLimit,
                    this.maxGroupDurationNs,
                    this.sendL2ForEveryCmd,
                    this.l2RefreshDepth,
                    this.threadFactory,
                    this.waitStrategy,
                    this.orderBookFactory,
                    this.binaryCommandsLz4CompressorFactory);
        }

        @Override
        public String toString() {
            return "PerformanceConfiguration.PerformanceConfigurationBuilder(ringBufferSize=" + this.ringBufferSize
                    + ", matchingEnginesNum=" + this.matchingEnginesNum
                    + ", riskEnginesNum=" + this.riskEnginesNum
                    + ", msgsInGroupLimit=" + this.msgsInGroupLimit
                    + ", maxGroupDurationNs=" + this.maxGroupDurationNs
                    + ", sendL2ForEveryCmd=" + this.sendL2ForEveryCmd
                    + ", l2RefreshDepth=" + this.l2RefreshDepth
                    + ", threadFactory=" + this.threadFactory
                    + ", waitStrategy=" + this.waitStrategy
                    + ", orderBookFactory=" + this.orderBookFactory
                    + ", binaryCommandsLz4CompressorFactory=" + this.binaryCommandsLz4CompressorFactory + ")";
        }
    }
}
