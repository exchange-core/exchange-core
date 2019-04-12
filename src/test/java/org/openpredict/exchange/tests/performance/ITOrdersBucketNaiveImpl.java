package org.openpredict.exchange.tests.performance;

import org.openpredict.exchange.core.orderbook.IOrdersBucket;
import org.openpredict.exchange.core.orderbook.OrdersBucketNaiveImpl;

public class ITOrdersBucketNaiveImpl extends ITOrdersBucketBase {


    @Override
    protected IOrdersBucket createNewOrdersBucket() {
        return new OrdersBucketNaiveImpl();
    }
}
