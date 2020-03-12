package exchange.core2.tests.steps;

import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.L2MarketDataHelper;
import io.cucumber.java.en.Then;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class OrderBookStepdefs {

    private final ExchangeTestContainer container;

    public OrderBookStepdefs(ExchangeTestContainer container) {
        this.container = container;
    }

    @Then("An {symbol} order book is:")
    public void an_order_book_is(CoreSymbolSpecification symbol, L2MarketDataHelper orderBook) {
        assertEquals(orderBook.build(), container.requestCurrentOrderBook(symbol.symbolId));
    }
}
