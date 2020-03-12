package exchange.core2.tests.steps;

import exchange.core2.core.common.*;
import exchange.core2.core.common.api.ApiMoveOrder;
import exchange.core2.core.common.api.ApiPlaceOrder;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.tests.util.ExchangeTestContainer;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

@Slf4j
public class OrderStepdefs {

    private final ExchangeTestContainer container;


    private List<MatcherTradeEvent> matcherEvents;
    private Map<Long, ApiPlaceOrder> orders = new HashMap<>();

    public OrderStepdefs(ExchangeTestContainer container) {
        this.container = container;
    }

    @Before
    public void before() throws Exception{
        log.info("before");
        container.initBasicSymbols();
        container.initBasicUsers();
    }

    @After
    public void after(){
        if(container != null){
            container.close();
        }
    }

    @When(value = "A client {long} places an {word} order {long} at {long}@{long} \\(type: {word}, symbol: {symbol})")
    public void aClientPlacesAnOrderAtTypeGTCSymbolEUR_USD(long clientId, String side, long orderId, long price, long size,
                                                           String orderType, CoreSymbolSpecification symbol) throws InterruptedException {
        aClientPassAnOrder(clientId, side, orderId, price, size, orderType, symbol, 0);
    }

    @When(value = "A client {long} places an {word} order {long} at {long}@{long} \\(type: {word}, symbol: {symbol}, reservePrice: {long})")
    public void aClientPlacesAnOrderAtTypeGTCSymbolEUR_USD(long clientId, String side, long orderId, long price, long size,
                                                           String orderType, CoreSymbolSpecification symbol, long reservePrice) throws InterruptedException {
        aClientPassAnOrder(clientId, side, orderId, price, size, orderType, symbol, reservePrice);
    }

    private void aClientPassAnOrder(long clientId, String side, long orderId, long price, long size, String orderType, CoreSymbolSpecification symbol, long reservePrice) throws InterruptedException {

        ApiPlaceOrder.ApiPlaceOrderBuilder builder = ApiPlaceOrder.builder().uid(clientId).id(orderId).price(price).size(size)
                .action(OrderAction.valueOf(side)).orderType(OrderType.valueOf(orderType))
                .symbol(symbol.symbolId);

        if(reservePrice > 0){
            builder.reservePrice(reservePrice);
        }

        final ApiPlaceOrder order = builder.build();

        orders.put(orderId, order);

        log.debug("PLACE : {}", order);
        container.submitCommandSync(order, cmd -> {
            assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
            assertThat(cmd.orderId, is(orderId));
            assertThat(cmd.uid, is(clientId));
            assertThat(cmd.price, is(price));
            assertThat(cmd.size, is(size));
            assertThat(cmd.action, is(OrderAction.valueOf(side)));
            assertThat(cmd.orderType, is(OrderType.valueOf(orderType)));
            assertThat(cmd.symbol, is(symbol.symbolId));

            OrderStepdefs.this.matcherEvents = cmd.extractEvents();
        });
    }


    @Then("The order {long} is partially matched. LastPx: {long}, LastQty: {long}")
    public void theOrderIsPartiallyMatchedLastPxLastQty(long orderId, long lastPx, long lastQty) {
        theOrderIsMatched(orderId, lastPx, lastQty, false);
    }

    private void theOrderIsMatched(long orderId, long lastPx, long lastQty, boolean completed) {
        assertThat(matcherEvents.size(), is(1));

        MatcherTradeEvent evt = matcherEvents.get(0);
        assertThat(evt.matchedOrderId, is(orderId));
        assertThat(evt.matchedOrderUid, is(orders.get(orderId).uid));
        assertThat(evt.matchedOrderCompleted, is(completed));
        assertThat(evt.eventType, is(MatcherEventType.TRADE));
        assertThat(evt.size, is(lastQty));
        assertThat(evt.price, is(lastPx));
    }

    @And("No trade events")
    public void noTradeEvents() {
        assertEquals(0, matcherEvents.size());
    }

    @When("A client {long} moves a price to {long} of the order {long}")
    public void aClientMovesAPriceToOfTheOrder(long clientId, long newPrice, long orderId) throws Exception {

        ApiPlaceOrder initialOrder = orders.get(orderId);

        final ApiMoveOrder moveOrder = ApiMoveOrder.builder().symbol(initialOrder.symbol).uid(clientId).id(orderId)
                .newPrice(newPrice).build();
        log.debug("MOVE : {}", moveOrder);
        container.submitCommandSync(moveOrder, cmd -> {
            assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));

            assertThat(cmd.orderId, is(orderId));
            assertThat(cmd.uid, is(clientId));

            matcherEvents = cmd.extractEvents();
        });
    }

    @Then("The order {long} is fully matched. LastPx: {long}, LastQty: {long}")
    public void theOrderIsFullyMatchedLastPxLastQty(long orderId, long lastPx, long lastQty) {
        theOrderIsMatched(orderId, lastPx, lastQty, true);
    }
}
