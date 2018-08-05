package org.openpredict.exchange.tests.util;

import lombok.RequiredArgsConstructor;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.openpredict.exchange.core.IOrderBook;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

@RequiredArgsConstructor
public class TestOrdersGeneratorSession {

    public Random rand = new Random(1L);

    public final IOrderBook orderBook;

    //    public ConcurrentBitSet actualOrders;
    public BitSet actualOrders = new BitSet();

    //    public Map<Integer, Integer> orderPrices = new ConcurrentHashMap<>();
    public IntIntHashMap orderPrices = new IntIntHashMap();
    public IntLongHashMap orderUids = new IntLongHashMap();

    public long numCompleted = 0;
    public long numRejected = 0;
    public long numReduced = 0;

    public long counterPlaceMarket = 0;
    public long counterPlaceLimit = 0;
    public long counterCancel = 0;
    public long counterMove = 0;

    public List<Long> uid;


    public int seq = 1;

    public final int targetOrderBookOrders;

    public int lastOrderBookOrdersSize = 0;

    public List<Integer> orderBookSizeAskStat = new ArrayList<>();
    public List<Integer> orderBookSizeBidStat = new ArrayList<>();
    public List<Integer> orderBookNumOrdersStat = new ArrayList<>();

}
