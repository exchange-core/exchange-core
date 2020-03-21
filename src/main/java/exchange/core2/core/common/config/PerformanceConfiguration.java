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

import java.util.concurrent.ThreadFactory;
import java.util.function.BiFunction;

@AllArgsConstructor
@Getter
@Builder
public final class PerformanceConfiguration {

    private final int ringBufferSize;
    private final int matchingEnginesNum;
    private final int riskEnginesNum;
    private final int msgsInGroupLimit;

    private final ThreadFactory threadFactory;
    private final CoreWaitStrategy waitStrategy;
    private final BiFunction<CoreSymbolSpecification, ObjectsPool, IOrderBook> orderBookFactory;

    // TODO add expected number of users and symbols

    public static PerformanceConfiguration.PerformanceConfigurationBuilder baseBuilder() {

        return builder()
                .ringBufferSize(32768)
                .matchingEnginesNum(1)
                .riskEnginesNum(1)
                .msgsInGroupLimit(512)
                .threadFactory(Thread::new)
                .waitStrategy(CoreWaitStrategy.SLEEPING)
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
                .orderBookFactory(OrderBookDirectImpl::new);
    }
}
