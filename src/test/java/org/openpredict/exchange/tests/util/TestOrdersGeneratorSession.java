package org.openpredict.exchange.tests.util;

import lombok.NonNull;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.openpredict.exchange.core.orderbook.IOrderBook;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

public class TestOrdersGeneratorSession {

    public final IOrderBook orderBook;

    public final int targetOrderBookOrders;

    public final long priceDeviation;

    public final int numUsers;

    public final int symbol;

    public final Random rand;

    //    public ConcurrentBitSet actualOrders;
    public final BitSet actualOrders = new BitSet();

    //    public Map<Integer, Integer> orderPrices = new ConcurrentHashMap<>();
    public final IntIntHashMap orderPrices = new IntIntHashMap();
    public final IntLongHashMap orderUids = new IntLongHashMap();

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
    public long numReduced = 0;

    public long counterPlaceMarket = 0;
    public long counterPlaceLimit = 0;
    public long counterCancel = 0;
    public long counterMove = 0;

    public int seq = 1;

    public int lastOrderBookOrdersSize = 0;

    public TestOrdersGeneratorSession(IOrderBook orderBook, int targetOrderBookOrders, long priceDeviation, int numUsers, int symbol, long centralPrice, boolean enableSlidingPrice) {
        this.orderBook = orderBook;
        this.targetOrderBookOrders = targetOrderBookOrders;
        this.priceDeviation = priceDeviation;
        this.numUsers = numUsers;
        this.symbol = symbol;
        this.rand = new Random(symbol);

        this.lastTradePrice = centralPrice;
        this.priceDirection = enableSlidingPrice ? 1 : 0;
    }
}
