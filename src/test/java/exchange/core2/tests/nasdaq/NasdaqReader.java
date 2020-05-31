package exchange.core2.tests.nasdaq;

import com.paritytrading.juncture.nasdaq.itch50.ITCH50Parser;
import com.paritytrading.nassau.util.BinaryFILE;
import exchange.core2.core.ExchangeApi;
import exchange.core2.core.common.config.InitialStateConfiguration;
import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.core.common.config.SerializationConfiguration;
import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.ExecutionTime;
import exchange.core2.tests.util.TestConstants;
import lombok.extern.slf4j.Slf4j;
import org.agrona.BitUtil;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

@Slf4j
public class NasdaqReader {


    @Test
    public void test() throws Exception {

        final int numUsersSuggested = 1_000_000;

        final int numUsers = BitUtil.findNextPositivePowerOfTwo(numUsersSuggested);
        final int numUsersMask = numUsers - 1;

        final PerformanceConfiguration perfCfg = PerformanceConfiguration.throughputPerformanceBuilder().build();
        final InitialStateConfiguration initStateCfg = InitialStateConfiguration.cleanStart("NASDAQ_TEST");

        try (final ExchangeTestContainer container = ExchangeTestContainer.create(perfCfg, initStateCfg, SerializationConfiguration.DEFAULT)) {


            final ExchangeApi api = container.getApi();


            // common accounts configuration for all users
            BitSet currencies = new BitSet();
            currencies.set(TestConstants.CURRENECY_USD);

            List<BitSet> userCurrencies = new ArrayList<>(numUsers + 1);
            IntStream.rangeClosed(1, numUsers + 1).forEach(uid -> {
                userCurrencies.add(currencies);
            });
            container.userAccountsInit(userCurrencies);


            ITCH50StatListener statListener = new ITCH50StatListener();
            ITCH50Parser listener = new ITCH50Parser(statListener);


            final String pathname = "../../nasdaq/01302020.NASDAQ_ITCH50";
//          final String pathname = "../../nasdaq/20190730.PSX_ITCH_50";
//          final String pathname = "../../nasdaq/20190730.BX_ITCH_50";

            final ExecutionTime executionTime = new ExecutionTime(d -> log.debug("Time: {}", d));
            BinaryFILE.read(new File(pathname), listener);
            executionTime.close();

            statListener.printStat();

            long totalSymbolMessages = statListener.getSymbolStat().values().stream().mapToLong(c -> c.counter).sum();
//            counters.forEach((k, v) -> log.debug("{} = {}", k, v));
            log.info("TOTAL: {} troughput = {} TPS", totalSymbolMessages, totalSymbolMessages * 1_000_000_000L / executionTime.getResultNs().join());

//            symbolPrecisions.forEachKeyValue((symbolId, multiplier) -> {
//                int min = symbolRangeMin.get(symbolId);
//                int max = symbolRangeMax.get(symbolId);
//                log.debug("{} (executionTime{}) {}..{}", symbolNames.get(symbolId).trim(), multiplier, min, max);
//            });


//            perSymbolCounter.forEachKeyValue((symbolId, messagesStat) -> {
//                log.debug("{}:", symbolNames.get(symbolId).trim());
//                messagesStat.forEach((k, v) -> log.debug("      {} = {}", k, v));
//            });


//            log.debug();


        }

    }

    public static int hashToUid(long orderId, int numUsersMask) {
        long x = ((orderId * 0xcc9e2d51) << 15) * 0x1b873593;
        return 1 + ((int) (x >> 32 ^ x) & numUsersMask);
    }

    public static long convertTime(int high, long low) {
        return low + (((long) high) << 32);
    }


//    public void validatePrice(long price, int stockLocate) {
//        if (price % 100 != 0) { // expect price in cents (original NASDAQ precision is 4 dp)
//            throw new IllegalStateException("Unexpected price: " + price + " for " + symbolNames.get(stockLocate));
//        }
//    }
}
