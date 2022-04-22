package exchange.core2.tests.steps;

import static exchange.core2.tests.util.ExchangeTestContainer.CHECK_SUCCESS;
import static exchange.core2.tests.util.TestConstants.SYMBOLSPEC_ETH_XBT;
import static exchange.core2.tests.util.TestConstants.SYMBOLSPEC_EUR_USD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.MatcherEventType;
import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.Order;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.api.ApiAddUser;
import exchange.core2.core.common.api.ApiAdjustUserBalance;
import exchange.core2.core.common.api.ApiCancelOrder;
import exchange.core2.core.common.api.ApiCommand;
import exchange.core2.core.common.api.ApiMoveOrder;
import exchange.core2.core.common.api.ApiPlaceOrder;
import exchange.core2.core.common.api.reports.SingleUserReportResult;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.L2MarketDataHelper;
import exchange.core2.tests.util.TestConstants;
import io.cucumber.datatable.DataTable;
import io.cucumber.java8.En;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderStepdefs implements En {

    public static PerformanceConfiguration testPerformanceConfiguration = null;

    private ExchangeTestContainer container = null;

    private List<MatcherTradeEvent> matcherEvents;
    private Map<Long, ApiPlaceOrder> orders = new HashMap<>();

    final Map<String, CoreSymbolSpecification> symbolSpecificationMap = new HashMap<>();
    final Map<String, Long> users = new HashMap<>();

    public OrderStepdefs() {
        symbolSpecificationMap.put("EUR_USD", SYMBOLSPEC_EUR_USD);
        symbolSpecificationMap.put("ETH_XBT", SYMBOLSPEC_ETH_XBT);
        users.put("Alice", 1440001L);
        users.put("Bob", 1440002L);
        users.put("Charlie", 1440003L);

        ParameterType(
            "symbol",
            "(EUR_USD)?(ETH_XBT)?",
            symbolSpecificationMap::get
        );
        ParameterType("user",
            "(Alice)?(Bob)?(Charlie)?",
            users::get);

        DataTableType((DataTable entry) -> {
            List<List<String>> list = entry.asLists();

            //skip a header if it presents
            if (list.get(0).get(0) != null && list.get(0).get(0).trim().equals("bid")) {
                list = list.subList(1, list.size());
            }

            //format | bid | price | ask |
            final L2MarketDataHelper l2helper = new L2MarketDataHelper();
            for (List<String> row : list) {
                int price = Integer.parseInt(row.get(1));

                String bid = row.get(0);
                if (bid != null && bid.length() > 0) {
                    l2helper.addBid(price, Integer.parseInt(bid));
                } else {
                    l2helper.addAsk(price, Integer.parseInt(row.get(2)));
                }
            }
            return l2helper;
        });

        Before((HookNoArgsBody) -> {
            container = ExchangeTestContainer.create(testPerformanceConfiguration);
            container.initBasicSymbols();
        });
        After((HookNoArgsBody) -> {
            if (container != null) {
                container.close();
            }
        });

        Given("New client {user} has a balance:",
            (Long clientId, List<List<String>> balance) -> {

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

            });

        When("A client {user} places an {word} order {long} at {long}@{long} \\(type: {word}, symbol: {symbol})",
            (Long clientId, String side, Long orderId, Long price, Long size, String orderType, CoreSymbolSpecification symbol) -> {
                aClientPassAnOrder(clientId, side, orderId, price, size, orderType, symbol, 0,
                    CommandResultCode.SUCCESS);
            });

        When(
            "A client {user} places an {word} order {long} at {long}@{long} \\(type: {word}, symbol: {symbol}, reservePrice: {long})",
            (Long clientId, String side, Long orderId, Long price, Long size, String orderType, CoreSymbolSpecification symbol, Long reservePrice) -> {
                aClientPassAnOrder(clientId, side, orderId, price, size, orderType, symbol, reservePrice,
                    CommandResultCode.SUCCESS);
            });
        Then("The order {long} is partially matched. LastPx: {long}, LastQty: {long}",
            (Long orderId, Long lastPx, Long lastQty) -> {
                theOrderIsMatched(orderId, lastPx, lastQty, false, null);
            });
        Then("The order {long} is fully matched. LastPx: {long}, LastQty: {long}, bidderHoldPrice: {long}",
            (Long orderId, Long lastPx, Long lastQty, Long bidderHoldPrice) -> {
                theOrderIsMatched(orderId, lastPx, lastQty, true, bidderHoldPrice);
            });
        And("No trade events", () -> {
            assertEquals(0, matcherEvents.size());
        });

        When("A client {user} moves a price to {long} of the order {long}",
            (Long clientId, Long newPrice, Long orderId) -> {
                moveOrder(clientId, newPrice, orderId, CommandResultCode.SUCCESS);
            });

        When("A client {user} could not move a price to {long} of the order {long} due to {word}",
            (Long clientId, Long newPrice, Long orderId, String resultCode) -> {
                moveOrder(clientId, newPrice, orderId, CommandResultCode.valueOf(resultCode));
            });

        Then("The order {long} is fully matched. LastPx: {long}, LastQty: {long}",
            (Long orderId, Long lastPx, Long lastQty) -> {
                theOrderIsMatched(orderId, lastPx, lastQty, true, null);
            });

        Then("An {symbol} order book is:",
            (CoreSymbolSpecification symbol, L2MarketDataHelper orderBook) -> {
                assertEquals(orderBook.build(), container.requestCurrentOrderBook(symbol.symbolId));
            });



        When(
            "A client {user} could not place an {word} order {long} at {long}@{long} \\(type: {word}, symbol: {symbol}, reservePrice: {long}) due to {word}",
            (Long clientId, String side, Long orderId, Long price, Long size,
                String orderType, CoreSymbolSpecification symbol, Long reservePrice, String resultCode) -> {
                aClientPassAnOrder(clientId, side, orderId, price, size, orderType, symbol, reservePrice,
                    CommandResultCode.valueOf(resultCode));
            });

        And("A balance of a client {user}:",
            (Long clientId, List<List<String>> balance) -> {
                SingleUserReportResult profile = container.getUserProfile(clientId);
                for (List<String> record : balance) {
                    assertThat("Unexpected balance of: " + record.get(0),
                        profile.getAccounts().get(TestConstants.getCurrency(record.get(0))),
                        is(Long.parseLong(record.get(1))));
                }
            });

        And("A client {user} orders:", (Long clientId, List<List<String>> table) -> {
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
        });

        And("A client {user} does not have active orders", (Long clientId) -> {
            SingleUserReportResult profile = container.getUserProfile(clientId);
            assertEquals(0, profile.fetchIndexedOrders().size());
        });

        Given("{long} {word} is added to the balance of a client {user}",
            (Long ammount, String currency, Long clientId) -> {

                // add 1 szabo more
                container.submitCommandSync(ApiAdjustUserBalance.builder()
                    .uid(clientId)
                    .currency(TestConstants.getCurrency(currency))
                    .amount(ammount).transactionId(2193842938742L).build(), CHECK_SUCCESS);
            });

        When("A client {user} cancels the remaining size {long} of the order {long}",
            (Long clientId, Long size, Long orderId) -> {
                ApiPlaceOrder initialOrder = orders.get(orderId);

                ApiCancelOrder order = ApiCancelOrder.builder().orderId(orderId).uid(clientId)
                    .symbol(initialOrder.symbol)
                    .build();

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
                        assertThat(evt.eventType, is(MatcherEventType.REDUCE));
                        assertThat(evt.size, is(size));
                    }).join();
            });
    }

    private void aClientPassAnOrder(long clientId, String side, long orderId, long price, long size, String orderType,
        CoreSymbolSpecification symbol, long reservePrice, CommandResultCode resultCode) {

        ApiPlaceOrder.ApiPlaceOrderBuilder builder = ApiPlaceOrder.builder().uid(clientId).orderId(orderId).price(price)
            .size(size)
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

    private void moveOrder(long clientId, long newPrice, long orderId, CommandResultCode resultCode2) {
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

    private void checkField(Map<String, Integer> fieldNameByIndex, List<String> record, String field, long expected) {
        if (fieldNameByIndex.containsKey(field)) {
            long actual = Long.parseLong(record.get(fieldNameByIndex.get(field)));
            assertEquals("Unexpected value for " + field, actual, expected);
        }
    }

}
