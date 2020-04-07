package exchange.core2.tests.steps;

import exchange.core2.core.common.*;
import exchange.core2.core.common.api.*;
import exchange.core2.core.common.api.reports.SingleUserReportResult;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.L2MarketDataHelper;
import exchange.core2.tests.util.TestConstants;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static exchange.core2.tests.util.ExchangeTestContainer.CHECK_SUCCESS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Slf4j
public class OrderStepdefs {

    private ExchangeTestContainer container;

    private List<MatcherTradeEvent> matcherEvents;
    private Map<Long, ApiPlaceOrder> orders = new HashMap<>();

    public OrderStepdefs() {
    }

    @Before
    public void before() throws Exception {
        log.info("before");
        container = new ExchangeTestContainer();
        container.initBasicSymbols();
    }

    @After
    public void after() {
        if (container != null) {
            container.close();
        }
    }

    @When(value = "A client {user} places an {word} order {long} at {long}@{long} \\(type: {word}, symbol: {symbol})")
    public void aClientPlacesAnOrderAtTypeGTCSymbolEUR_USD(long clientId, String side, long orderId, long price, long size,
                                                           String orderType, CoreSymbolSpecification symbol) throws InterruptedException {
        aClientPassAnOrder(clientId, side, orderId, price, size, orderType, symbol, 0, CommandResultCode.SUCCESS);
    }

    @When(value = "A client {user} places an {word} order {long} at {long}@{long} \\(type: {word}, symbol: {symbol}, reservePrice: {long})")
    public void aClientPlacesAnOrderAtTypeGTCSymbolEUR_USD(long clientId, String side, long orderId, long price, long size,
                                                           String orderType, CoreSymbolSpecification symbol, long reservePrice) throws InterruptedException {
        aClientPassAnOrder(clientId, side, orderId, price, size, orderType, symbol, reservePrice, CommandResultCode.SUCCESS);
    }

    private void aClientPassAnOrder(long clientId, String side, long orderId, long price, long size, String orderType,
                                    CoreSymbolSpecification symbol, long reservePrice, CommandResultCode resultCode) throws InterruptedException {

        ApiPlaceOrder.ApiPlaceOrderBuilder builder = ApiPlaceOrder.builder().uid(clientId).orderId(orderId).price(price).size(size)
                .action(OrderAction.valueOf(side)).orderType(OrderType.valueOf(orderType))
                .symbol(symbol.symbolId);

        if (reservePrice > 0) {
            builder.reservePrice(reservePrice);
        }

        final ApiPlaceOrder order = builder.build();

        orders.put(orderId, order);

        log.debug("PLACE : {}", order);
        container.getApi().submitCommandAsyncFullResponse(order).thenAccept(cmd -> {
            assertThat(cmd.orderId, is(orderId));
            assertThat(cmd.resultCode, is(resultCode));
            assertThat(cmd.uid, is(clientId));
            assertThat(cmd.price, is(price));
            assertThat(cmd.size, is(size));
            assertThat(cmd.action, is(OrderAction.valueOf(side)));
            assertThat(cmd.orderType, is(OrderType.valueOf(orderType)));
            assertThat(cmd.symbol, is(symbol.symbolId));

            OrderStepdefs.this.matcherEvents = cmd.extractEvents();
        }).join();
    }


    @Then("The order {long} is partially matched. LastPx: {long}, LastQty: {long}")
    public void theOrderIsPartiallyMatchedLastPxLastQty(long orderId, long lastPx, long lastQty) {
        theOrderIsMatched(orderId, lastPx, lastQty, false, null);
    }

    @Then("The order {long} is fully matched. LastPx: {long}, LastQty: {long}, bidderHoldPrice: {long}")
    public void theOrderIsFullyMatchedLastPxLastQtyBidderHoldPrice(long orderId, long lastPx, long lastQty, long bidderHoldPrice) {
        theOrderIsMatched(orderId, lastPx, lastQty, true, bidderHoldPrice);
    }

    private void theOrderIsMatched(long orderId, long lastPx, long lastQty, boolean completed, Long bidderHoldPrice) {
        assertThat(matcherEvents.size(), is(1));

        MatcherTradeEvent evt = matcherEvents.get(0);
        assertThat(evt.matchedOrderId, is(orderId));
        assertThat(evt.matchedOrderUid, is(orders.get(orderId).uid));
        assertThat(evt.matchedOrderCompleted, is(completed));
        assertThat(evt.eventType, is(MatcherEventType.TRADE));
        assertThat(evt.size, is(lastQty));
        assertThat(evt.price, is(lastPx));
        if (bidderHoldPrice != null) {
            assertThat(evt.bidderHoldPrice, is(bidderHoldPrice));
        }
    }

    @And("No trade events")
    public void noTradeEvents() {
        assertEquals(0, matcherEvents.size());
    }

    @When("A client {user} moves a price to {long} of the order {long}")
    public void aClientMovesAPriceToOfTheOrder(long clientId, long newPrice, long orderId) throws Exception {
        moveOrder(clientId, newPrice, orderId, CommandResultCode.SUCCESS);
    }

    @When("A client {user} could not move a price to {long} of the order {long} due to {word}")
    public void aClientCouldNotMoveOrder(long clientId, long newPrice, long orderId, String resultCode) throws InterruptedException {
        moveOrder(clientId, newPrice, orderId, CommandResultCode.valueOf(resultCode));
    }

    private void moveOrder(long clientId, long newPrice, long orderId, CommandResultCode resultCode2) throws InterruptedException {
        ApiPlaceOrder initialOrder = orders.get(orderId);

        final ApiMoveOrder moveOrder = ApiMoveOrder.builder().symbol(initialOrder.symbol).uid(clientId).orderId(orderId)
                .newPrice(newPrice).build();
        log.debug("MOVE : {}", moveOrder);
        container.submitCommandSync(moveOrder, cmd -> {
            assertThat(cmd.resultCode, is(resultCode2));
            assertThat(cmd.orderId, is(orderId));
            assertThat(cmd.uid, is(clientId));

            matcherEvents = cmd.extractEvents();
        });
    }

    @Then("The order {long} is fully matched. LastPx: {long}, LastQty: {long}")
    public void theOrderIsFullyMatchedLastPxLastQty(long orderId, long lastPx, long lastQty) {
        theOrderIsMatched(orderId, lastPx, lastQty, true, null);
    }

    @Then("An {symbol} order book is:")
    public void an_order_book_is(CoreSymbolSpecification symbol, L2MarketDataHelper orderBook) {
        assertEquals(orderBook.build(), container.requestCurrentOrderBook(symbol.symbolId));
    }

    @Given("New client {user} has a balance:")
    public void newClientAHasABalance(long clientId, List<List<String>> balance) throws InterruptedException {

        final List<ApiCommand> cmds = new ArrayList<>();

        cmds.add(ApiAddUser.builder().uid(clientId).build());

        int transactionId = 0;

        for (List<String> entry : balance) {
            transactionId++;
            cmds.add(ApiAdjustUserBalance.builder().uid(clientId).transactionId(transactionId)
                    .amount(Long.parseLong(entry.get(1)))
                    .currency(TestConstants.getCurrency(entry.get(0)))
                    .build());
        }

        container.getApi().submitCommandsSync(cmds);

    }

    @When("A client {user} could not place an {word} order {long} at {long}@{long} \\(type: {word}, symbol: {symbol}, reservePrice: {long}) due to {word}")
    public void aClientCouldNotPlaceOrder(long clientId, String side, long orderId, long price, long size,
                                          String orderType, CoreSymbolSpecification symbol, long reservePrice, String resultCode) throws InterruptedException {
        aClientPassAnOrder(clientId, side, orderId, price, size, orderType, symbol, reservePrice, CommandResultCode.valueOf(resultCode));
    }

    @And("A balance of a client {user}:")
    public void aCurrentBalanceOfAClientA(long clientId, List<List<String>> balance) throws ExecutionException, InterruptedException {
        SingleUserReportResult profile = container.getUserProfile(clientId);
        for (List<String> record : balance) {
            assertThat("Unexpected balance of: " + record.get(0), profile.getAccounts().get(TestConstants.getCurrency(record.get(0))), is(Long.parseLong(record.get(1))));
        }
    }

    @And("A client {user} orders:")
    public void aCurrentOrdersOfAClientA(long clientId, List<List<String>> table) throws ExecutionException, InterruptedException {

        //| id | price | size | filled | reservePrice | side |

        SingleUserReportResult profile = container.getUserProfile(clientId);

        //skip a header if it presents
        Map<String, Integer> fieldNameByIndex = new HashMap<>();

        //read a header
        int i = 0;
        for (String field : table.get(0)) {
            fieldNameByIndex.put(field, i++);
        }

        //remove header
        table = table.subList(1, table.size());

        Map<Long, Order> orders = profile.fetchIndexedOrders();

        for (List<String> record : table) {
            long orderId = Long.parseLong(record.get(fieldNameByIndex.get("id")));
            Order order = orders.get(orderId);
            assertNotNull(order);

            checkField(fieldNameByIndex, record, "price", order.getPrice());
            checkField(fieldNameByIndex, record, "size", order.getSize());
            checkField(fieldNameByIndex, record, "filled", order.getFilled());
            checkField(fieldNameByIndex, record, "reservePrice", order.getReserveBidPrice());

            if (fieldNameByIndex.containsKey("side")) {
                OrderAction action = OrderAction.valueOf(record.get(fieldNameByIndex.get("side")));
                assertEquals("Unexpected action", action, order.getAction());
            }

        }
    }

    private void checkField(Map<String, Integer> fieldNameByIndex, List<String> record, String field, long expected) {
        if (fieldNameByIndex.containsKey(field)) {
            long actual = Long.parseLong(record.get(fieldNameByIndex.get(field)));
            assertEquals("Unexpected value for " + field, actual, expected);
        }
    }

    @And("A client {user} does not have active orders")
    public void aClientBDoesNotHaveActiveOrders(long clientId) throws ExecutionException, InterruptedException {
        SingleUserReportResult profile = container.getUserProfile(clientId);
        assertEquals(0, profile.fetchIndexedOrders().size());
    }

    @Given("{long} {word} is added to the balance of a client {user}")
    public void xbtIsAddedToTheBalanceOfAClientA(long ammount, String currency, long clientId) throws InterruptedException {

        // add 1 szabo more
        container.submitCommandSync(ApiAdjustUserBalance.builder()
                .uid(clientId)
                .currency(TestConstants.getCurrency(currency))
                .amount(ammount).transactionId(2193842938742L).build(), CHECK_SUCCESS);
    }


    @When("A client {user} cancels the remaining size {long} of the order {long}")
    public void aClientACancelsTheOrder(long clientId, long size, long orderId) throws InterruptedException {

        ApiPlaceOrder initialOrder = orders.get(orderId);

        ApiCancelOrder order = ApiCancelOrder.builder().orderId(orderId).uid(clientId).symbol(initialOrder.symbol).build();

        container.getApi().submitCommandAsyncFullResponse(order).thenAccept(
                cmd -> {
                    assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                    assertThat(cmd.command, is(OrderCommandType.CANCEL_ORDER));
                    assertThat(cmd.orderId, is(orderId));
                    assertThat(cmd.uid, is(clientId));
                    assertThat(cmd.symbol, is(initialOrder.symbol));
                    assertThat(cmd.action, is(initialOrder.action));

                    final MatcherTradeEvent evt = cmd.matcherEvent;
                    assertNotNull(evt);
                    assertThat(evt.eventType, is(MatcherEventType.CANCEL));
                    assertThat(evt.size, is(size));
                }).join();
    }
}
