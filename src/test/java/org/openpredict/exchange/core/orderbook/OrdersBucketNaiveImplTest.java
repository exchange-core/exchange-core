package org.openpredict.exchange.core.orderbook;


import lombok.extern.slf4j.Slf4j;

// TODO add test ignoring own order

@Slf4j
public class OrdersBucketNaiveImplTest extends OrdersBucketBaseTest {

    @Override
    protected IOrdersBucket createNewBucket() {
        IOrdersBucket bucket = new OrdersBucketNaiveImpl();
        bucket.setPrice(PRICE);
        return bucket;
    }
}
