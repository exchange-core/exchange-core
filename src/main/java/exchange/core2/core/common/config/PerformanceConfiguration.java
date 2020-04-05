package exchange.core2.core.common.config;


import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.CoreWaitStrategy;
import exchange.core2.core.orderbook.IOrderBook;
import exchange.core2.core.orderbook.OrderBookDirectImpl;
import exchange.core2.core.orderbook.OrderBookNaiveImpl;
import exchange.core2.core.processors.ObjectsPool;
import exchange.core2.core.utils.AffinityThreadFactory;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.util.concurrent.ThreadFactory;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Exchange performance configuration
 */
@AllArgsConstructor
@Getter
@Builder
public final class PerformanceConfiguration {

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
     */
    private final int msgsInGroupLimit;

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
    private final BiFunction<CoreSymbolSpecification, ObjectsPool, IOrderBook> orderBookFactory;

    /*
     * LZ4 compressor factory for binary commands and reports
     */
    private final Supplier<LZ4Compressor> binaryCommandsLz4CompressorFactory;

    // TODO add expected number of users and symbols

    public static PerformanceConfiguration.PerformanceConfigurationBuilder baseBuilder() {

        return builder()
                .ringBufferSize(2 * 1024)
                .matchingEnginesNum(1)
                .riskEnginesNum(1)
                .msgsInGroupLimit(256)
                .threadFactory(Thread::new)
                .waitStrategy(CoreWaitStrategy.SLEEPING)
                .binaryCommandsLz4CompressorFactory(() -> LZ4Factory.fastestInstance().highCompressor())
                .orderBookFactory((spec, pool) -> new OrderBookNaiveImpl(spec));
    }

    public static PerformanceConfiguration.PerformanceConfigurationBuilder latencyPerformanceBuilder() {

        return builder()
                .ringBufferSize(2 * 1024)
                .matchingEnginesNum(1)
                .riskEnginesNum(1)
                .msgsInGroupLimit(256)
                .threadFactory(new AffinityThreadFactory(AffinityThreadFactory.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE))
                .waitStrategy(CoreWaitStrategy.BUSY_SPIN)
                .binaryCommandsLz4CompressorFactory(() -> LZ4Factory.fastestInstance().highCompressor())
                .orderBookFactory(OrderBookDirectImpl::new);
    }

    public static PerformanceConfiguration.PerformanceConfigurationBuilder throughputPerformanceBuilder() {

        return builder()
                .ringBufferSize(64 * 1024)
                .matchingEnginesNum(2)
                .riskEnginesNum(4)
                .msgsInGroupLimit(2048)
                .threadFactory(new AffinityThreadFactory(AffinityThreadFactory.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE))
                .waitStrategy(CoreWaitStrategy.BUSY_SPIN)
                .binaryCommandsLz4CompressorFactory(() -> LZ4Factory.fastestInstance().highCompressor())
                .orderBookFactory(OrderBookDirectImpl::new);
    }
}
