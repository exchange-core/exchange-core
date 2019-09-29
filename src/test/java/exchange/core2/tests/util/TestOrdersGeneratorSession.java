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

import lombok.NonNull;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import exchange.core2.core.orderbook.IOrderBook;

import java.util.*;
import java.util.function.UnaryOperator;

public final class TestOrdersGeneratorSession {

    public final IOrderBook orderBook;

    public final int targetOrderBookOrders;

    public final long priceDeviation;

    public final int numUsers;
    public final UnaryOperator<Integer> uidMapper;

    public final int symbol;

    public final Random rand;

    public final IntIntHashMap orderPrices = new IntIntHashMap();
    public final Map<Integer, Integer> orderUids = new LinkedHashMap<>();

    public final List<Integer> orderBookSizeAskStat = new ArrayList<>();
    public final List<Integer> orderBookSizeBidStat = new ArrayList<>();
    public final List<Integer> orderBookNumOrdersStat = new ArrayList<>();

    @NonNull
    public long lastTradePrice;

    @NonNull
    // set to 1 to make price move up and down
    public int priceDirection;

    public long orderbooksFilledAtSequence = 0;

    public long numCompleted = 0;
    public long numRejected = 0;
    public long numCancelled = 0;

    public long counterPlaceMarket = 0;
    public long counterPlaceLimit = 0;
    public long counterCancel = 0;
    public long counterMove = 0;

    public int seq = 1;

    public int lastOrderBookOrdersSize = 0;

//    public SingleWriterRecorder hdrRecorder = new SingleWriterRecorder(Integer.MAX_VALUE, 2);

    public TestOrdersGeneratorSession(IOrderBook orderBook, int targetOrderBookOrders, long priceDeviation, int numUsers, UnaryOperator<Integer> uidMapper, int symbol, long centralPrice, boolean enableSlidingPrice) {
        this.orderBook = orderBook;
        this.targetOrderBookOrders = targetOrderBookOrders;
        this.priceDeviation = priceDeviation;
        this.numUsers = numUsers;
        this.uidMapper = uidMapper;
        this.symbol = symbol;
        this.rand = new Random(symbol);

        this.lastTradePrice = centralPrice;
        this.priceDirection = enableSlidingPrice ? 1 : 0;
    }
}
