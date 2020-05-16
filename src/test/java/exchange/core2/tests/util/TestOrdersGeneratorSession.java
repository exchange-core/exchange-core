/*
 * Copyright 2019 Maksim Zheravin
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
package exchange.core2.tests.util;

import exchange.core2.core.orderbook.IOrderBook;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;

import java.util.*;
import java.util.function.UnaryOperator;

@Slf4j
public final class TestOrdersGeneratorSession {

    public final IOrderBook orderBook;

    public final int transactionsNumber;
    public final int targetOrderBookOrdersHalf;

    public final long priceDeviation;

    public final boolean avalancheIOC;

    public final int numUsers;
    public final UnaryOperator<Integer> uidMapper;

    public final int symbol;

    public final Random rand;

    public final IntIntHashMap orderPrices = new IntIntHashMap();
    public final IntIntHashMap orderSizes = new IntIntHashMap();
    public final Map<Integer, Integer> orderUids = new LinkedHashMap<>();

    public final List<Integer> orderBookSizeAskStat = new ArrayList<>();
    public final List<Integer> orderBookSizeBidStat = new ArrayList<>();
    public final List<Integer> orderBookNumOrdersAskStat = new ArrayList<>();
    public final List<Integer> orderBookNumOrdersBidStat = new ArrayList<>();

    public final long minPrice;
    public final long maxPrice;

    public long lastTradePrice;

    // set to 1 to make price move up and down
    public int priceDirection;

    public boolean initialOrdersPlaced = false;

    public long numCompleted = 0;
    public long numRejected = 0;
    public long numReduced = 0;

    public long counterPlaceMarket = 0;
    public long counterPlaceLimit = 0;
    public long counterCancel = 0;
    public long counterMove = 0;
    public long counterReduce = 0;

    public int seq = 1;

    public Integer filledAtSeq = null;

    // statistics (updated every 256 orders)
    public int lastOrderBookOrdersSizeAsk = 0;
    public int lastOrderBookOrdersSizeBid = 0;
    public long lastTotalVolumeAsk = 0;
    public long lastTotalVolumeBid = 0;

//    public SingleWriterRecorder hdrRecorder = new SingleWriterRecorder(Integer.MAX_VALUE, 2);

    public TestOrdersGeneratorSession(IOrderBook orderBook,
                                      int transactionsNumber,
                                      int targetOrderBookOrdersHalf,
                                      boolean avalancheIOC,
                                      int numUsers,
                                      UnaryOperator<Integer> uidMapper,
                                      int symbol,
                                      boolean enableSlidingPrice,
                                      int seed) {
        this.orderBook = orderBook;
        this.transactionsNumber = transactionsNumber;
        this.targetOrderBookOrdersHalf = targetOrderBookOrdersHalf;
        this.avalancheIOC = avalancheIOC;
        this.numUsers = numUsers;
        this.uidMapper = uidMapper;
        this.symbol = symbol;
        this.rand = new Random(Objects.hash(symbol * -177277, seed * 10037 + 198267));

        int price = (int) Math.pow(10, 3.3 + rand.nextDouble() * 1.5 + rand.nextDouble() * 1.5);

        this.lastTradePrice = price;
        this.priceDeviation = Math.min((int) (price * 0.05), 10000);
        this.minPrice = price - priceDeviation * 5;
        this.maxPrice = price + priceDeviation * 5;

        // log.debug("Symbol:{} price={} dev={} range({},{})", symbol, price, priceDeviation, minPrice, maxPrice);

        this.priceDirection = enableSlidingPrice ? 1 : 0;
    }
}
