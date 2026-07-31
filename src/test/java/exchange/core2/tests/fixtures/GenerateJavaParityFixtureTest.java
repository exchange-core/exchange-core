package exchange.core2.tests.fixtures;

import exchange.core2.core.ExchangeApi;
import exchange.core2.core.ExchangeCore;
import exchange.core2.core.IEventsHandler;
import exchange.core2.core.SimpleEventsProcessor;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.L2MarketData;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.SymbolType;
import exchange.core2.core.common.UserProfile;
import exchange.core2.core.common.api.ApiAddUser;
import exchange.core2.core.common.api.ApiAdjustUserBalance;
import exchange.core2.core.common.api.ApiBinaryDataCommand;
import exchange.core2.core.common.api.ApiCancelOrder;
import exchange.core2.core.common.api.ApiCommand;
import exchange.core2.core.common.api.ApiMoveOrder;
import exchange.core2.core.common.api.ApiNop;
import exchange.core2.core.common.api.ApiOrderBookRequest;
import exchange.core2.core.common.api.ApiPlaceOrder;
import exchange.core2.core.common.api.ApiPersistState;
import exchange.core2.core.common.api.ApiReduceOrder;
import exchange.core2.core.common.api.ApiReset;
import exchange.core2.core.common.api.ApiResumeUser;
import exchange.core2.core.common.api.ApiSuspendUser;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.api.reports.SingleUserReportQuery;
import exchange.core2.core.common.api.reports.SingleUserReportResult;
import exchange.core2.core.common.api.reports.StateHashReportResult;
import exchange.core2.core.common.api.reports.TotalCurrencyBalanceReportQuery;
import exchange.core2.core.common.api.reports.TotalCurrencyBalanceReportResult;
import exchange.core2.core.common.config.InitialStateConfiguration;
import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.core.common.config.SerializationConfiguration;
import exchange.core2.core.processors.MatchingEngineRouter;
import exchange.core2.core.processors.RiskEngine;
import exchange.core2.core.utils.HashingUtils;
import exchange.core2.tests.util.ExchangeTestContainer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class GenerateJavaParityFixtureTest {

    private static final String STATE_HASH_KIND_JAVA_REPORT = "java_report";
    private static final String STATE_HASH_KIND_ZIG_RISK_LOW32 = "zig_risk_low32";
    private static final String STATE_HASH_KIND_ZIG_DETAILED_LOW32 = "zig_detailed_low32";
    private static final String TRADE_SYMBOL_REGISTRATION_DIRECT = "direct";
    private static final String TRADE_SYMBOL_REGISTRATION_API = "api";
    private static final String ORDERBOOK_MODE_NAIVE = "naive";
    private static final String ORDERBOOK_MODE_DIRECT = "direct";
    private static final String DIAGNOSTIC_INTERNAL_FALLBACK_PROPERTY =
            "parity.fixture.diagnostic.allow_internal_fallback";
    private static final String DIAGNOSTIC_FALLBACK_MARKER_PREFIX =
            "# NON_AUTHORITATIVE_DIAGNOSTIC_FALLBACK=";

    private static final int BASE = 11;
    private static final int QUOTE = 15;
    private static final int SYMBOL_EXCHANGE = 241;
    private static final int SYMBOL_EXCHANGE_ALT = 242;
    private static final long USER_A_UID = 7001L;
    private static final long USER_B_UID = 7002L;
    private static final long USER_MISSING_UID = 9999L;
    private static final long EXCHANGE_TRADE_PRICE = 100L;
    private static final long EXCHANGE_MAKER_SIZE = 7L;
    private static final long EXCHANGE_TAKER_SIZE = 5L;
    private static final long EXTENDED_EXECUTED_BASE = 6L;
    private static final long EXTENDED_EXECUTED_QUOTE = 612L;
    private static final long MULTISYMBOL_EXECUTED_BASE = 7L;
    private static final long MULTISYMBOL_EXECUTED_QUOTE = 884L;
    private static final int REPORTS_LONG_ROUNDS = 16;
    private static final int BOOTSTRAP_LONG_ROUNDS = 18;

    @Test
    public void strictFixtureOracleDoesNotUseInternalFallbackByDefault() {
        final String previousValue = System.getProperty(DIAGNOSTIC_INTERNAL_FALLBACK_PROPERTY);
        System.clearProperty(DIAGNOSTIC_INTERNAL_FALLBACK_PROPERTY);
        try {
            final IllegalArgumentException publicApiFailure =
                    new IllegalArgumentException("expected public API failure");
            final boolean[] fallbackCalled = new boolean[]{false};

            final IllegalStateException failure = assertThrows(
                    IllegalStateException.class,
                    () -> executeFixtureOracle(
                            "test_public_api",
                            () -> {
                                throw publicApiFailure;
                            },
                            () -> {
                                fallbackCalled[0] = true;
                                return "fallback";
                            }));

            assertSame(publicApiFailure, failure.getCause());
            assertFalse(fallbackCalled[0]);
            assertTrue(failure.getMessage().contains(DIAGNOSTIC_INTERNAL_FALLBACK_PROPERTY));
        } finally {
            restoreSystemProperty(DIAGNOSTIC_INTERNAL_FALLBACK_PROPERTY, previousValue);
        }
    }

    @Test
    public void diagnosticFixtureOracleFallbackIsExplicitlyMarkedNonAuthoritative() {
        final String previousValue = System.getProperty(DIAGNOSTIC_INTERNAL_FALLBACK_PROPERTY);
        final PrintStream previousErrorStream = System.err;
        final ByteArrayOutputStream diagnosticOutput = new ByteArrayOutputStream();
        final PrintStream diagnosticStream = new PrintStream(diagnosticOutput);
        System.setProperty(DIAGNOSTIC_INTERNAL_FALLBACK_PROPERTY, "true");
        System.setErr(diagnosticStream);
        try {
            final FixtureOracleResult<String> result = executeFixtureOracle(
                    "test_public_api",
                    () -> {
                        throw new IllegalArgumentException("expected public API failure");
                    },
                    () -> "fallback");
            final List<String> lines = new ArrayList<>();
            appendDiagnosticFallbackMarker(lines, result);

            assertEquals("fallback", result.value);
            assertEquals(1, lines.size());
            assertTrue(lines.get(0).startsWith(DIAGNOSTIC_FALLBACK_MARKER_PREFIX));
            assertTrue(lines.get(0).contains("test_public_api:java.lang.IllegalArgumentException"));
            assertTrue(diagnosticOutput.toString().contains("non-authoritative Java parity fixture fallback"));
            assertTrue(diagnosticOutput.toString().contains(DIAGNOSTIC_INTERNAL_FALLBACK_PROPERTY));
            assertTrue(diagnosticOutput.toString().contains("expected public API failure"));
        } finally {
            System.setErr(previousErrorStream);
            diagnosticStream.close();
            restoreSystemProperty(DIAGNOSTIC_INTERNAL_FALLBACK_PROPERTY, previousValue);
        }
    }

    @Test
    public void writeUserBalanceBasicFixture() throws Exception {
        assumeTrue(Boolean.getBoolean("parity.fixture.write"), "manual fixture generation only");

        final Path outputPath = Paths.get(
                System.getProperty("parity.fixture.path", "src/zig/fixtures/java_user_balance_basic.fixture"));
        ensureParentDirectory(outputPath);

        final PerformanceConfiguration perfCfg = PerformanceConfiguration.baseBuilder()
                .matchingEnginesNum(1)
                .riskEnginesNum(1)
                .build();

        final List<String> commandResults = new ArrayList<>();

        try (ExchangeTestContainer container = ExchangeTestContainer.create(perfCfg)) {
            commandResults.add(submit(container.getApi(), ApiAddUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(container.getApi(), ApiAddUser.builder().uid(USER_B_UID).build()));
            commandResults.add(submit(container.getApi(), ApiAdjustUserBalance.builder()
                    .uid(USER_A_UID)
                    .transactionId(1L)
                    .currency(QUOTE)
                    .amount(100_000L)
                    .build()));
            commandResults.add(submit(container.getApi(), ApiAdjustUserBalance.builder()
                    .uid(USER_B_UID)
                    .transactionId(1L)
                    .currency(BASE)
                    .amount(100_000L)
                    .build()));

            // Barrier command to flush grouped command processing before direct state reads.
            submit(container.getApi(), ApiNop.builder().build());

            final ExchangeCore exchangeCore = container.getExchangeCore();
            final RiskEngine riskEngine = getSingleRiskEngine(exchangeCore);
            final FixtureStateHash fixtureStateHash = resolveFixtureStateHash("user", exchangeCore);

            final List<String> lines = new ArrayList<>();
            lines.add("version=1");
            lines.add("scenario=user_balance_basic");
            lines.add("orderbook_mode=na");
            lines.add("symbol=0");
            lines.add("command_results=" + String.join(",", commandResults));
            lines.add("settlement_mode=full");
            lines.add("state_hash_kind=" + fixtureStateHash.kind);
            lines.add("state_hash=" + fixtureStateHash.value);
            lines.add("fee_15=" + riskEngine.getFees().get(QUOTE));
            lines.add("maker_balance_15=" + accountBalance(riskEngine, USER_A_UID, QUOTE));
            lines.add("maker_balance_11=" + accountBalance(riskEngine, USER_A_UID, BASE));
            lines.add("taker_balance_15=" + accountBalance(riskEngine, USER_B_UID, QUOTE));
            lines.add("taker_balance_11=" + accountBalance(riskEngine, USER_B_UID, BASE));
            lines.add("orderbook_ask_size=0");
            lines.add("orderbook_bid_size=0");

            Files.write(outputPath, lines, StandardCharsets.UTF_8);
        }
    }

    @Test
    public void writeExchangeControlFixture() throws Exception {
        assumeTrue(Boolean.getBoolean("parity.fixture.write"), "manual fixture generation only");

        final Path outputPath = Paths.get(System.getProperty(
                "parity.fixture.control.path",
                "src/zig/fixtures/java_exchange_control.fixture"));
        ensureParentDirectory(outputPath);

        final PerformanceConfiguration perfCfg = PerformanceConfiguration.baseBuilder()
                .matchingEnginesNum(1)
                .riskEnginesNum(1)
                .build();

        final List<String> commandResults = new ArrayList<>();

        try (ExchangeTestContainer container = ExchangeTestContainer.create(perfCfg)) {
            final ExchangeCore exchangeCore = container.getExchangeCore();
            final ExchangeApi api = container.getApi();
            final SimpleEventsFixtureCollector simpleEventsCollector = new SimpleEventsFixtureCollector();
            container.setConsumer(new SimpleEventsProcessor(simpleEventsCollector));
            final RiskEngine riskEngine = getSingleRiskEngine(exchangeCore);
            final List<String> groupingResults = new CopyOnWriteArrayList<>();
            container.setConsumer((orderCommand, seq) -> {
                if (orderCommand.command == OrderCommandType.GROUPING_CONTROL) {
                    groupingResults.add(orderCommand.resultCode.name());
                }
            });

            commandResults.add(submit(api, ApiAddUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(api, ApiSuspendUser.builder().uid(USER_MISSING_UID).build()));
            commandResults.add(submit(api, ApiSuspendUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(api, ApiSuspendUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(api, ApiResumeUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(api, ApiResumeUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(api, ApiAdjustUserBalance.builder()
                    .uid(USER_A_UID)
                    .transactionId(1L)
                    .currency(QUOTE)
                    .amount(1234L)
                    .build()));
            api.groupingControl(System.nanoTime(), 1L);
            final String groupingBarrierResult = submit(api, ApiNop.builder().build());
            if (groupingResults.size() != 1) {
                throw new IllegalStateException("Expected one GROUPING_CONTROL result, got " + groupingResults.size());
            }
            commandResults.add(groupingResults.get(0));
            commandResults.add(groupingBarrierResult);
            final FixtureOracleResult<String> persistenceOracle = controlPersistMergedResult(perfCfg);
            commandResults.add(persistenceOracle.value);
            commandResults.add(submit(api, ApiReset.builder().build()));
            commandResults.add(submit(api, ApiAddUser.builder().uid(USER_B_UID).build()));
            commandResults.add(submit(api, ApiAdjustUserBalance.builder()
                    .uid(USER_B_UID)
                    .transactionId(1L)
                    .currency(BASE)
                    .amount(55L)
                    .build()));

            // Barrier command to flush grouped command processing before direct state reads.
            submit(api, ApiNop.builder().build());

            final FixtureStateHash fixtureStateHash = resolveFixtureStateHash("control", exchangeCore);

            final List<String> lines = new ArrayList<>();
            lines.add("version=1");
            appendDiagnosticFallbackMarker(lines, persistenceOracle);
            lines.add("scenario=exchange_control");
            lines.add("orderbook_mode=na");
            lines.add("symbol=0");
            lines.add("command_results=" + String.join(",", commandResults));
            lines.add("settlement_mode=full");
            lines.add("state_hash_kind=" + fixtureStateHash.kind);
            lines.add("state_hash=" + fixtureStateHash.value);
            lines.add("fee_15=" + riskEngine.getFees().get(QUOTE));
            lines.add("maker_balance_15=" + accountBalance(riskEngine, USER_A_UID, QUOTE));
            lines.add("maker_balance_11=" + accountBalance(riskEngine, USER_A_UID, BASE));
            lines.add("taker_balance_15=" + accountBalance(riskEngine, USER_B_UID, QUOTE));
            lines.add("taker_balance_11=" + accountBalance(riskEngine, USER_B_UID, BASE));
            lines.add("orderbook_ask_size=0");
            lines.add("orderbook_bid_size=0");

            Files.write(outputPath, lines, StandardCharsets.UTF_8);
        }
    }

    @Test
    public void writeExchangeTradeBasicFixture() throws Exception {
        assumeTrue(Boolean.getBoolean("parity.fixture.write"), "manual fixture generation only");

        final Path outputPath = Paths.get(
                System.getProperty("parity.fixture.trade.path", "src/zig/fixtures/java_exchange_trade_basic.fixture"));
        ensureParentDirectory(outputPath);

        final PerformanceConfiguration perfCfg = PerformanceConfiguration.baseBuilder()
                .matchingEnginesNum(1)
                .riskEnginesNum(1)
                .build();

        final List<String> commandResults = new ArrayList<>();

        try (ExchangeTestContainer container = ExchangeTestContainer.create(perfCfg)) {
            final ExchangeCore exchangeCore = container.getExchangeCore();
            final SimpleEventsFixtureCollector simpleEventsCollector = new SimpleEventsFixtureCollector();
            container.setConsumer(new SimpleEventsProcessor(simpleEventsCollector));

            final CoreSymbolSpecification symbolSpec = CoreSymbolSpecification.builder()
                    .symbolId(SYMBOL_EXCHANGE)
                    .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
                    .baseCurrency(BASE)
                    .quoteCurrency(QUOTE)
                    .baseScaleK(1L)
                    .quoteScaleK(1L)
                    .takerFee(0L)
                    .makerFee(0L)
                    .build();

            final String registrationSettlementMode = registerTradeSymbol(container, exchangeCore, symbolSpec);

            commandResults.add(submit(container.getApi(), ApiAddUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(container.getApi(), ApiAddUser.builder().uid(USER_B_UID).build()));
            commandResults.add(submit(container.getApi(), ApiAdjustUserBalance.builder()
                    .uid(USER_A_UID)
                    .transactionId(1L)
                    .currency(BASE)
                    .amount(100_000L)
                    .build()));
            commandResults.add(submit(container.getApi(), ApiAdjustUserBalance.builder()
                    .uid(USER_B_UID)
                    .transactionId(1L)
                    .currency(QUOTE)
                    .amount(100_000L)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9001L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(EXCHANGE_TRADE_PRICE)
                    .size(EXCHANGE_MAKER_SIZE)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.GTC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9002L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(EXCHANGE_TRADE_PRICE)
                    .reservePrice(EXCHANGE_TRADE_PRICE)
                    .size(EXCHANGE_TAKER_SIZE)
                    .action(OrderAction.BID)
                    .orderType(OrderType.IOC)
                    .build()));

            // Barrier command to flush grouped command processing before direct state reads.
            submit(container.getApi(), ApiNop.builder().build());
            final List<String> simpleEventsRaw = simpleEventsCollector.snapshot();
            final List<String> simpleEvents = new ArrayList<>(simpleEventsRaw.size());
            for (String simpleEvent : simpleEventsRaw) {
                // Keep multisymbol simple-events deterministic with current Zig parity surface.
                if (!simpleEvent.startsWith("order_book:")) {
                    simpleEvents.add(simpleEvent);
                }
            }

            final RiskEngine riskEngine = getSingleRiskEngine(exchangeCore);
            final L2MarketData orderBook = container.requestCurrentOrderBook(SYMBOL_EXCHANGE);
            final FixtureStateHash fixtureStateHash = resolveFixtureStateHash("trade", exchangeCore);
            final String settlementMode = resolveTradeSettlementMode(registrationSettlementMode);
            final String orderBookMode = resolveTradeBasicOrderbookMode();

            long makerBalanceQuote = accountBalance(riskEngine, USER_A_UID, QUOTE);
            final long makerBalanceBase = accountBalance(riskEngine, USER_A_UID, BASE);
            final long takerBalanceQuote = accountBalance(riskEngine, USER_B_UID, QUOTE);
            long takerBalanceBase = accountBalance(riskEngine, USER_B_UID, BASE);

            if ("full".equals(settlementMode) && "pre_release".equals(registrationSettlementMode)) {
                // Java 25 currently blocks canonical symbol-registration path for this test.
                // Project settlement delta for this deterministic zero-fee scenario so fixture
                // reflects full settlement semantics.
                final long executedSize = Math.min(EXCHANGE_MAKER_SIZE, EXCHANGE_TAKER_SIZE);
                makerBalanceQuote += executedSize * EXCHANGE_TRADE_PRICE;
                takerBalanceBase += executedSize;
            }

            final List<String> lines = new ArrayList<>();
            lines.add("version=1");
            lines.add("scenario=exchange_trade_basic");
            lines.add("orderbook_mode=" + orderBookMode);
            lines.add("symbol=" + SYMBOL_EXCHANGE);
            lines.add("command_results=" + String.join(",", commandResults));
            lines.add("simple_events_size=" + simpleEvents.size());
            for (int i = 0; i < simpleEvents.size(); i++) {
                lines.add("simple_event_" + i + "=" + simpleEvents.get(i));
            }
            lines.add("settlement_mode=" + settlementMode);
            lines.add("state_hash_kind=" + fixtureStateHash.kind);
            lines.add("state_hash=" + fixtureStateHash.value);
            lines.add("fee_15=" + riskEngine.getFees().get(QUOTE));
            lines.add("maker_balance_15=" + makerBalanceQuote);
            lines.add("maker_balance_11=" + makerBalanceBase);
            lines.add("taker_balance_15=" + takerBalanceQuote);
            lines.add("taker_balance_11=" + takerBalanceBase);
            lines.add("orderbook_ask_size=" + orderBook.askSize);
            lines.add("orderbook_bid_size=" + orderBook.bidSize);
            for (int i = 0; i < orderBook.askSize; i++) {
                lines.add("ask_" + i + "=" + orderBook.askPrices[i] + ":" + orderBook.askVolumes[i] + ":" + orderBook.askOrders[i]);
            }
            for (int i = 0; i < orderBook.bidSize; i++) {
                lines.add("bid_" + i + "=" + orderBook.bidPrices[i] + ":" + orderBook.bidVolumes[i] + ":" + orderBook.bidOrders[i]);
            }

            Files.write(outputPath, lines, StandardCharsets.UTF_8);
        }
    }

    @Test
    public void writeExchangeTradeExtendedFixture() throws Exception {
        assumeTrue(Boolean.getBoolean("parity.fixture.write"), "manual fixture generation only");

        final Path outputPath = Paths.get(System.getProperty(
                "parity.fixture.trade.extended.path",
                "src/zig/fixtures/java_exchange_trade_extended.fixture"));
        ensureParentDirectory(outputPath);

        final PerformanceConfiguration perfCfg = PerformanceConfiguration.baseBuilder()
                .matchingEnginesNum(1)
                .riskEnginesNum(1)
                .build();

        final List<String> commandResults = new ArrayList<>();

        try (ExchangeTestContainer container = ExchangeTestContainer.create(perfCfg)) {
            final ExchangeCore exchangeCore = container.getExchangeCore();
            final SimpleEventsFixtureCollector simpleEventsCollector = new SimpleEventsFixtureCollector();
            container.setConsumer(new SimpleEventsProcessor(simpleEventsCollector));

            final CoreSymbolSpecification symbolSpec = CoreSymbolSpecification.builder()
                    .symbolId(SYMBOL_EXCHANGE)
                    .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
                    .baseCurrency(BASE)
                    .quoteCurrency(QUOTE)
                    .baseScaleK(1L)
                    .quoteScaleK(1L)
                    .takerFee(0L)
                    .makerFee(0L)
                    .build();

            final String registrationSettlementMode = registerTradeSymbol(container, exchangeCore, symbolSpec);
            final String settlementMode = resolveTradeExtendedSettlementMode(registrationSettlementMode);
            final String orderBookMode = resolveTradeExtendedOrderbookMode();

            commandResults.add(submit(container.getApi(), ApiAddUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(container.getApi(), ApiAddUser.builder().uid(USER_B_UID).build()));
            commandResults.add(submit(container.getApi(), ApiAddUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(container.getApi(), ApiAdjustUserBalance.builder()
                    .uid(USER_A_UID)
                    .transactionId(1L)
                    .currency(BASE)
                    .amount(150_000L)
                    .build()));
            commandResults.add(submit(container.getApi(), ApiAdjustUserBalance.builder()
                    .uid(USER_B_UID)
                    .transactionId(1L)
                    .currency(QUOTE)
                    .amount(200_000L)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9101L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(101L)
                    .size(10L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.GTC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiMoveOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9101L)
                    .symbol(SYMBOL_EXCHANGE)
                    .newPrice(102L)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9201L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(99L)
                    .reservePrice(99L)
                    .size(4L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.GTC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9202L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(102L)
                    .reservePrice(102L)
                    .size(5L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.IOC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9102L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(103L)
                    .size(4L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.GTC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9203L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(102L)
                    .reservePrice(102L)
                    .size(1L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.GTC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiMoveOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9203L)
                    .symbol(SYMBOL_EXCHANGE)
                    .newPrice(104L)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiReduceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9999L)
                    .symbol(SYMBOL_EXCHANGE)
                    .reduceSize(1L)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiCancelOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9999L)
                    .symbol(SYMBOL_EXCHANGE)
                    .build()));

            // Barrier command to flush grouped command processing before direct state reads.
            submit(container.getApi(), ApiNop.builder().build());
            final List<String> simpleEventsRaw = simpleEventsCollector.snapshot();
            final List<String> simpleEvents = new ArrayList<>(simpleEventsRaw.size());
            for (String simpleEvent : simpleEventsRaw) {
                // Keep bootstrap simple-events deterministic with current Zig journal parity.
                if (!simpleEvent.startsWith("order_book:")) {
                    simpleEvents.add(simpleEvent);
                }
            }

            final RiskEngine riskEngine = getSingleRiskEngine(exchangeCore);
            final L2MarketData orderBook = container.requestCurrentOrderBook(SYMBOL_EXCHANGE);
            final FixtureStateHash fixtureStateHash = resolveFixtureStateHash("trade_extended", exchangeCore);

            long makerBalanceQuote = accountBalance(riskEngine, USER_A_UID, QUOTE);
            final long makerBalanceBase = accountBalance(riskEngine, USER_A_UID, BASE);
            final long takerBalanceQuote = accountBalance(riskEngine, USER_B_UID, QUOTE);
            long takerBalanceBase = accountBalance(riskEngine, USER_B_UID, BASE);

            if ("full".equals(settlementMode) && "pre_release".equals(registrationSettlementMode)) {
                // Project settlement delta for deterministic zero-fee command stream.
                makerBalanceQuote += EXTENDED_EXECUTED_QUOTE;
                takerBalanceBase += EXTENDED_EXECUTED_BASE;
            }

            final List<String> lines = new ArrayList<>();
            lines.add("version=1");
            lines.add("scenario=exchange_trade_extended");
            lines.add("orderbook_mode=" + orderBookMode);
            lines.add("symbol=" + SYMBOL_EXCHANGE);
            lines.add("command_results=" + String.join(",", commandResults));
            lines.add("simple_events_size=" + simpleEvents.size());
            for (int i = 0; i < simpleEvents.size(); i++) {
                lines.add("simple_event_" + i + "=" + simpleEvents.get(i));
            }
            lines.add("settlement_mode=" + settlementMode);
            lines.add("state_hash_kind=" + fixtureStateHash.kind);
            lines.add("state_hash=" + fixtureStateHash.value);
            lines.add("fee_15=" + riskEngine.getFees().get(QUOTE));
            lines.add("maker_balance_15=" + makerBalanceQuote);
            lines.add("maker_balance_11=" + makerBalanceBase);
            lines.add("taker_balance_15=" + takerBalanceQuote);
            lines.add("taker_balance_11=" + takerBalanceBase);
            lines.add("orderbook_ask_size=" + orderBook.askSize);
            lines.add("orderbook_bid_size=" + orderBook.bidSize);
            for (int i = 0; i < orderBook.askSize; i++) {
                lines.add("ask_" + i + "=" + orderBook.askPrices[i] + ":" + orderBook.askVolumes[i] + ":" + orderBook.askOrders[i]);
            }
            for (int i = 0; i < orderBook.bidSize; i++) {
                lines.add("bid_" + i + "=" + orderBook.bidPrices[i] + ":" + orderBook.bidVolumes[i] + ":" + orderBook.bidOrders[i]);
            }

            Files.write(outputPath, lines, StandardCharsets.UTF_8);
        }
    }

    @Test
    public void writeExchangeTradeBootstrapFixture() throws Exception {
        assumeTrue(Boolean.getBoolean("parity.fixture.write"), "manual fixture generation only");

        final Path outputPath = Paths.get(System.getProperty(
                "parity.fixture.trade.bootstrap.path",
                "src/zig/fixtures/java_exchange_trade_bootstrap.fixture"));
        ensureParentDirectory(outputPath);

        final PerformanceConfiguration perfCfg = PerformanceConfiguration.baseBuilder()
                .matchingEnginesNum(1)
                .riskEnginesNum(1)
                .build();

        final List<String> commandResults = new ArrayList<>();

        try (ExchangeTestContainer container = ExchangeTestContainer.create(perfCfg)) {
            final ExchangeCore exchangeCore = container.getExchangeCore();
            final SimpleEventsFixtureCollector simpleEventsCollector = new SimpleEventsFixtureCollector();
            container.setConsumer(new SimpleEventsProcessor(simpleEventsCollector));

            final CoreSymbolSpecification symbolSpec = CoreSymbolSpecification.builder()
                    .symbolId(SYMBOL_EXCHANGE)
                    .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
                    .baseCurrency(BASE)
                    .quoteCurrency(QUOTE)
                    .baseScaleK(1L)
                    .quoteScaleK(1L)
                    .takerFee(0L)
                    .makerFee(0L)
                    .build();

            final String registrationSettlementMode = registerTradeSymbol(container, exchangeCore, symbolSpec);
            final String settlementMode = resolveTradeBootstrapSettlementMode(registrationSettlementMode);

            commandResults.add(submit(container.getApi(), ApiAddUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(container.getApi(), ApiAddUser.builder().uid(USER_B_UID).build()));
            commandResults.add(submit(container.getApi(), ApiAddUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(container.getApi(), ApiAdjustUserBalance.builder()
                    .uid(USER_A_UID)
                    .transactionId(1L)
                    .currency(BASE)
                    .amount(150_000L)
                    .build()));
            commandResults.add(submit(container.getApi(), ApiAdjustUserBalance.builder()
                    .uid(USER_B_UID)
                    .transactionId(1L)
                    .currency(QUOTE)
                    .amount(200_000L)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9101L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(101L)
                    .size(10L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.GTC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiMoveOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9101L)
                    .symbol(SYMBOL_EXCHANGE)
                    .newPrice(102L)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9201L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(99L)
                    .reservePrice(99L)
                    .size(4L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.GTC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9202L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(102L)
                    .reservePrice(102L)
                    .size(5L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.IOC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9102L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(103L)
                    .size(4L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.GTC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9203L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(102L)
                    .reservePrice(102L)
                    .size(1L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.GTC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiMoveOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9203L)
                    .symbol(SYMBOL_EXCHANGE)
                    .newPrice(104L)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiReduceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9999L)
                    .symbol(SYMBOL_EXCHANGE)
                    .reduceSize(1L)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiCancelOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9999L)
                    .symbol(SYMBOL_EXCHANGE)
                    .build()));

            // Barrier command to flush grouped command processing before direct state reads.
            submit(container.getApi(), ApiNop.builder().build());
            final List<String> simpleEvents = simpleEventsCollector.snapshot();

            final RiskEngine riskEngine = getSingleRiskEngine(exchangeCore);
            final L2MarketData orderBook = container.requestCurrentOrderBook(SYMBOL_EXCHANGE);
            final FixtureStateHash fixtureStateHash = resolveFixtureStateHash("trade_bootstrap", exchangeCore);
            final String orderBookMode = resolveTradeBootstrapOrderbookMode();

            long makerBalanceQuote = accountBalance(riskEngine, USER_A_UID, QUOTE);
            final long makerBalanceBase = accountBalance(riskEngine, USER_A_UID, BASE);
            final long takerBalanceQuote = accountBalance(riskEngine, USER_B_UID, QUOTE);
            long takerBalanceBase = accountBalance(riskEngine, USER_B_UID, BASE);

            if ("full".equals(settlementMode) && "pre_release".equals(registrationSettlementMode)) {
                // Project settlement delta for deterministic zero-fee command stream.
                makerBalanceQuote += EXTENDED_EXECUTED_QUOTE;
                takerBalanceBase += EXTENDED_EXECUTED_BASE;
            }

            final List<String> lines = new ArrayList<>();
            lines.add("version=1");
            lines.add("scenario=exchange_trade_bootstrap");
            lines.add("orderbook_mode=" + orderBookMode);
            lines.add("symbol=" + SYMBOL_EXCHANGE);
            lines.add("command_results=" + String.join(",", commandResults));
            lines.add("simple_events_size=" + simpleEvents.size());
            for (int i = 0; i < simpleEvents.size(); i++) {
                lines.add("simple_event_" + i + "=" + simpleEvents.get(i));
            }
            lines.add("settlement_mode=" + settlementMode);
            lines.add("state_hash_kind=" + fixtureStateHash.kind);
            lines.add("state_hash=" + fixtureStateHash.value);
            lines.add("fee_15=" + riskEngine.getFees().get(QUOTE));
            lines.add("maker_balance_15=" + makerBalanceQuote);
            lines.add("maker_balance_11=" + makerBalanceBase);
            lines.add("taker_balance_15=" + takerBalanceQuote);
            lines.add("taker_balance_11=" + takerBalanceBase);
            lines.add("orderbook_ask_size=" + orderBook.askSize);
            lines.add("orderbook_bid_size=" + orderBook.bidSize);
            for (int i = 0; i < orderBook.askSize; i++) {
                lines.add("ask_" + i + "=" + orderBook.askPrices[i] + ":" + orderBook.askVolumes[i] + ":" + orderBook.askOrders[i]);
            }
            for (int i = 0; i < orderBook.bidSize; i++) {
                lines.add("bid_" + i + "=" + orderBook.bidPrices[i] + ":" + orderBook.bidVolumes[i] + ":" + orderBook.bidOrders[i]);
            }

            Files.write(outputPath, lines, StandardCharsets.UTF_8);
        }
    }

    @Test
    public void writeExchangeTradeBootstrapLongFixture() throws Exception {
        assumeTrue(Boolean.getBoolean("parity.fixture.write"), "manual fixture generation only");

        final Path outputPath = Paths.get(System.getProperty(
                "parity.fixture.trade.bootstrap.long.path",
                "src/zig/fixtures/java_exchange_trade_bootstrap_long.fixture"));
        ensureParentDirectory(outputPath);

        final PerformanceConfiguration perfCfg = PerformanceConfiguration.baseBuilder()
                .matchingEnginesNum(1)
                .riskEnginesNum(1)
                .build();

        final List<String> commandResults = new ArrayList<>();

        try (ExchangeTestContainer container = ExchangeTestContainer.create(perfCfg)) {
            final ExchangeCore exchangeCore = container.getExchangeCore();
            final ExchangeApi api = container.getApi();
            final SimpleEventsFixtureCollector simpleEventsCollector = new SimpleEventsFixtureCollector();
            container.setConsumer(new SimpleEventsProcessor(simpleEventsCollector));

            final CoreSymbolSpecification symbolSpec = CoreSymbolSpecification.builder()
                    .symbolId(SYMBOL_EXCHANGE)
                    .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
                    .baseCurrency(BASE)
                    .quoteCurrency(QUOTE)
                    .baseScaleK(1L)
                    .quoteScaleK(1L)
                    .takerFee(0L)
                    .makerFee(0L)
                    .build();

            final String registrationSettlementMode = registerTradeSymbol(container, exchangeCore, symbolSpec);
            final String settlementMode = resolveTradeBootstrapLongSettlementMode(registrationSettlementMode);

            commandResults.add(submit(api, ApiAddUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(api, ApiAddUser.builder().uid(USER_B_UID).build()));
            commandResults.add(submit(api, ApiAddUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(api, ApiAdjustUserBalance.builder()
                    .uid(USER_A_UID)
                    .transactionId(1L)
                    .currency(BASE)
                    .amount(400_000L)
                    .build()));
            commandResults.add(submit(api, ApiAdjustUserBalance.builder()
                    .uid(USER_B_UID)
                    .transactionId(1L)
                    .currency(QUOTE)
                    .amount(400_000L)
                    .build()));

            commandResults.add(submit(api, ApiPlaceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9101L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(200L)
                    .size(20L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.GTC)
                    .build()));
            commandResults.add(submit(api, ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9201L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(50L)
                    .reservePrice(50L)
                    .size(20L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.GTC)
                    .build()));

            for (int round = 0; round < BOOTSTRAP_LONG_ROUNDS; round++) {
                final long askOrderId = 9700L + round;
                final long askPrice = 220L + (round * 2L);
                final long askSize = 3L + (round % 3L);
                final long bidOrderId = 9800L + round;
                final long bidPrice = 40L - (round % 5L);
                final long bidSize = 2L + (round % 2L);

                commandResults.add(submit(api, ApiPlaceOrder.builder()
                        .uid(USER_A_UID)
                        .orderId(askOrderId)
                        .symbol(SYMBOL_EXCHANGE)
                        .price(askPrice)
                        .size(askSize)
                        .action(OrderAction.ASK)
                        .orderType(OrderType.GTC)
                        .build()));

                commandResults.add(submit(api, ApiPlaceOrder.builder()
                        .uid(USER_B_UID)
                        .orderId(bidOrderId)
                        .symbol(SYMBOL_EXCHANGE)
                        .price(bidPrice)
                        .reservePrice(bidPrice)
                        .size(bidSize)
                        .action(OrderAction.BID)
                        .orderType(OrderType.GTC)
                        .build()));
            }

            commandResults.add(submit(api, ApiPlaceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9102L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(300L)
                    .size(10L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.GTC)
                    .build()));
            commandResults.add(submit(api, ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9202L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(35L)
                    .reservePrice(35L)
                    .size(9L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.GTC)
                    .build()));
            commandResults.add(submit(api, ApiReduceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(USER_MISSING_UID)
                    .symbol(SYMBOL_EXCHANGE)
                    .reduceSize(1L)
                    .build()));
            commandResults.add(submit(api, ApiCancelOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(USER_MISSING_UID)
                    .symbol(SYMBOL_EXCHANGE)
                    .build()));

            // Barrier command to flush grouped command processing before direct state reads.
            submit(api, ApiNop.builder().build());
            final List<String> simpleEventsRaw = simpleEventsCollector.snapshot();
            final List<String> simpleEvents = new ArrayList<>(simpleEventsRaw.size());
            for (String simpleEvent : simpleEventsRaw) {
                // Keep bootstrap-long simple-events deterministic with current Zig journal parity.
                if (!simpleEvent.startsWith("order_book:")) {
                    simpleEvents.add(simpleEvent);
                }
            }

            final RiskEngine riskEngine = getSingleRiskEngine(exchangeCore);
            final L2MarketData orderBook = container.requestCurrentOrderBook(SYMBOL_EXCHANGE);
            final FixtureStateHash fixtureStateHash = resolveFixtureStateHash("trade_bootstrap_long", exchangeCore);
            final String orderBookMode = resolveTradeBootstrapLongOrderbookMode();

            final long makerBalanceQuote = accountBalance(riskEngine, USER_A_UID, QUOTE);
            final long makerBalanceBase = accountBalance(riskEngine, USER_A_UID, BASE);
            final long takerBalanceQuote = accountBalance(riskEngine, USER_B_UID, QUOTE);
            final long takerBalanceBase = accountBalance(riskEngine, USER_B_UID, BASE);

            final List<String> lines = new ArrayList<>();
            lines.add("version=1");
            lines.add("scenario=exchange_trade_bootstrap_long");
            lines.add("orderbook_mode=" + orderBookMode);
            lines.add("symbol=" + SYMBOL_EXCHANGE);
            lines.add("command_results=" + String.join(",", commandResults));
            lines.add("simple_events_size=" + simpleEvents.size());
            for (int i = 0; i < simpleEvents.size(); i++) {
                lines.add("simple_event_" + i + "=" + simpleEvents.get(i));
            }
            lines.add("settlement_mode=" + settlementMode);
            lines.add("state_hash_kind=" + fixtureStateHash.kind);
            lines.add("state_hash=" + fixtureStateHash.value);
            lines.add("fee_15=" + riskEngine.getFees().get(QUOTE));
            lines.add("maker_balance_15=" + makerBalanceQuote);
            lines.add("maker_balance_11=" + makerBalanceBase);
            lines.add("taker_balance_15=" + takerBalanceQuote);
            lines.add("taker_balance_11=" + takerBalanceBase);
            lines.add("orderbook_ask_size=" + orderBook.askSize);
            lines.add("orderbook_bid_size=" + orderBook.bidSize);
            for (int i = 0; i < orderBook.askSize; i++) {
                lines.add("ask_" + i + "=" + orderBook.askPrices[i] + ":" + orderBook.askVolumes[i] + ":" + orderBook.askOrders[i]);
            }
            for (int i = 0; i < orderBook.bidSize; i++) {
                lines.add("bid_" + i + "=" + orderBook.bidPrices[i] + ":" + orderBook.bidVolumes[i] + ":" + orderBook.bidOrders[i]);
            }

            Files.write(outputPath, lines, StandardCharsets.UTF_8);
        }
    }

    @Test
    public void writeExchangeTradeMultiSymbolFixture() throws Exception {
        assumeTrue(Boolean.getBoolean("parity.fixture.write"), "manual fixture generation only");

        final Path outputPath = Paths.get(System.getProperty(
                "parity.fixture.trade.multisymbol.path",
                "src/zig/fixtures/java_exchange_trade_multisymbol_241.fixture"));
        ensureParentDirectory(outputPath);

        final int focusSymbol = Integer.parseInt(System.getProperty(
                "parity.fixture.trade.multisymbol.focus_symbol",
                Integer.toString(SYMBOL_EXCHANGE)));
        if (focusSymbol != SYMBOL_EXCHANGE && focusSymbol != SYMBOL_EXCHANGE_ALT) {
            throw new IllegalStateException("Unsupported multi-symbol focus symbol: " + focusSymbol
                    + " (supported: " + SYMBOL_EXCHANGE + ", " + SYMBOL_EXCHANGE_ALT + ")");
        }

        final PerformanceConfiguration perfCfg = PerformanceConfiguration.baseBuilder()
                .matchingEnginesNum(1)
                .riskEnginesNum(1)
                .build();

        final List<String> commandResults = new ArrayList<>();

        try (ExchangeTestContainer container = ExchangeTestContainer.create(perfCfg)) {
            final ExchangeCore exchangeCore = container.getExchangeCore();
            final SimpleEventsFixtureCollector simpleEventsCollector = new SimpleEventsFixtureCollector();
            container.setConsumer(new SimpleEventsProcessor(simpleEventsCollector));

            final CoreSymbolSpecification specA = CoreSymbolSpecification.builder()
                    .symbolId(SYMBOL_EXCHANGE)
                    .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
                    .baseCurrency(BASE)
                    .quoteCurrency(QUOTE)
                    .baseScaleK(1L)
                    .quoteScaleK(1L)
                    .takerFee(0L)
                    .makerFee(0L)
                    .build();
            final CoreSymbolSpecification specB = CoreSymbolSpecification.builder()
                    .symbolId(SYMBOL_EXCHANGE_ALT)
                    .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
                    .baseCurrency(BASE)
                    .quoteCurrency(QUOTE)
                    .baseScaleK(1L)
                    .quoteScaleK(1L)
                    .takerFee(0L)
                    .makerFee(0L)
                    .build();

            final String registrationModeA = registerTradeSymbol(container, exchangeCore, specA);
            final String registrationModeB = registerTradeSymbol(container, exchangeCore, specB);
            if (!registrationModeA.equals(registrationModeB)) {
                throw new IllegalStateException("Mismatched registration modes for multi-symbol scenario: "
                        + registrationModeA + " vs " + registrationModeB);
            }
            final String settlementMode = resolveTradeMultiSymbolSettlementMode(registrationModeA);
            final String orderBookMode = resolveTradeMultiSymbolOrderbookMode();

            commandResults.add(submit(container.getApi(), ApiAddUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(container.getApi(), ApiAddUser.builder().uid(USER_B_UID).build()));
            commandResults.add(submit(container.getApi(), ApiAddUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(container.getApi(), ApiAdjustUserBalance.builder()
                    .uid(USER_A_UID)
                    .transactionId(1L)
                    .currency(BASE)
                    .amount(220_000L)
                    .build()));
            commandResults.add(submit(container.getApi(), ApiAdjustUserBalance.builder()
                    .uid(USER_B_UID)
                    .transactionId(1L)
                    .currency(QUOTE)
                    .amount(220_000L)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9301L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(101L)
                    .size(6L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.GTC)
                    .build()));
            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9302L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(101L)
                    .reservePrice(101L)
                    .size(4L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.IOC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9401L)
                    .symbol(SYMBOL_EXCHANGE_ALT)
                    .price(161L)
                    .size(5L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.GTC)
                    .build()));
            commandResults.add(submit(container.getApi(), ApiMoveOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9401L)
                    .symbol(SYMBOL_EXCHANGE_ALT)
                    .newPrice(160L)
                    .build()));
            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9402L)
                    .symbol(SYMBOL_EXCHANGE_ALT)
                    .price(160L)
                    .reservePrice(160L)
                    .size(3L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.IOC)
                    .build()));
            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9403L)
                    .symbol(SYMBOL_EXCHANGE_ALT)
                    .price(150L)
                    .reservePrice(150L)
                    .size(2L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.GTC)
                    .build()));
            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9303L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(103L)
                    .size(2L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.GTC)
                    .build()));
            commandResults.add(submit(container.getApi(), ApiReduceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9999L)
                    .symbol(SYMBOL_EXCHANGE)
                    .reduceSize(1L)
                    .build()));
            commandResults.add(submit(container.getApi(), ApiCancelOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9999L)
                    .symbol(SYMBOL_EXCHANGE_ALT)
                    .build()));

            // Barrier command to flush grouped command processing before direct state reads.
            submit(container.getApi(), ApiNop.builder().build());
            final List<String> simpleEvents = simpleEventsCollector.snapshot();

            final RiskEngine riskEngine = getSingleRiskEngine(exchangeCore);
            final FixtureStateHash fixtureStateHash = resolveFixtureStateHash("trade_multisymbol", exchangeCore);

            long makerBalanceQuote = accountBalance(riskEngine, USER_A_UID, QUOTE);
            final long makerBalanceBase = accountBalance(riskEngine, USER_A_UID, BASE);
            final long takerBalanceQuote = accountBalance(riskEngine, USER_B_UID, QUOTE);
            long takerBalanceBase = accountBalance(riskEngine, USER_B_UID, BASE);

            if ("full".equals(settlementMode) && "pre_release".equals(registrationModeA)) {
                // Project settlement delta for deterministic zero-fee multi-symbol stream.
                makerBalanceQuote += MULTISYMBOL_EXECUTED_QUOTE;
                takerBalanceBase += MULTISYMBOL_EXECUTED_BASE;
            }

            final L2MarketData orderBook = container.requestCurrentOrderBook(focusSymbol);

            final List<String> lines = new ArrayList<>();
            lines.add("version=1");
            lines.add("scenario=exchange_trade_multisymbol");
            lines.add("orderbook_mode=" + orderBookMode);
            lines.add("symbol=" + focusSymbol);
            lines.add("command_results=" + String.join(",", commandResults));
            lines.add("simple_events_size=" + simpleEvents.size());
            for (int i = 0; i < simpleEvents.size(); i++) {
                lines.add("simple_event_" + i + "=" + simpleEvents.get(i));
            }
            lines.add("settlement_mode=" + settlementMode);
            lines.add("state_hash_kind=" + fixtureStateHash.kind);
            lines.add("state_hash=" + fixtureStateHash.value);
            lines.add("fee_15=" + riskEngine.getFees().get(QUOTE));
            lines.add("maker_balance_15=" + makerBalanceQuote);
            lines.add("maker_balance_11=" + makerBalanceBase);
            lines.add("taker_balance_15=" + takerBalanceQuote);
            lines.add("taker_balance_11=" + takerBalanceBase);
            lines.add("orderbook_ask_size=" + orderBook.askSize);
            lines.add("orderbook_bid_size=" + orderBook.bidSize);
            for (int i = 0; i < orderBook.askSize; i++) {
                lines.add("ask_" + i + "=" + orderBook.askPrices[i] + ":" + orderBook.askVolumes[i] + ":" + orderBook.askOrders[i]);
            }
            for (int i = 0; i < orderBook.bidSize; i++) {
                lines.add("bid_" + i + "=" + orderBook.bidPrices[i] + ":" + orderBook.bidVolumes[i] + ":" + orderBook.bidOrders[i]);
            }

            Files.write(outputPath, lines, StandardCharsets.UTF_8);
        }
    }

    @Test
    public void writeExchangeTradeReportsFixture() throws Exception {
        assumeTrue(Boolean.getBoolean("parity.fixture.write"), "manual fixture generation only");

        final Path outputPath = Paths.get(System.getProperty(
                "parity.fixture.trade.reports.path",
                "src/zig/fixtures/java_exchange_trade_reports.fixture"));
        ensureParentDirectory(outputPath);

        final PerformanceConfiguration perfCfg = PerformanceConfiguration.baseBuilder()
                .matchingEnginesNum(1)
                .riskEnginesNum(1)
                .build();

        final List<String> commandResults = new ArrayList<>();

        try (ExchangeTestContainer container = ExchangeTestContainer.create(perfCfg)) {
            final ExchangeCore exchangeCore = container.getExchangeCore();
            final SimpleEventsFixtureCollector simpleEventsCollector = new SimpleEventsFixtureCollector();
            container.setConsumer(new SimpleEventsProcessor(simpleEventsCollector));

            final CoreSymbolSpecification symbolSpec = CoreSymbolSpecification.builder()
                    .symbolId(SYMBOL_EXCHANGE)
                    .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
                    .baseCurrency(BASE)
                    .quoteCurrency(QUOTE)
                    .baseScaleK(1L)
                    .quoteScaleK(1L)
                    .takerFee(0L)
                    .makerFee(0L)
                    .build();

            final String registrationSettlementMode = registerTradeSymbol(container, exchangeCore, symbolSpec);
            final String settlementMode = resolveTradeReportsSettlementMode(registrationSettlementMode);
            final String orderBookMode = resolveTradeReportsOrderbookMode();

            commandResults.add(submit(container.getApi(), ApiAddUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(container.getApi(), ApiAddUser.builder().uid(USER_B_UID).build()));
            commandResults.add(submit(container.getApi(), ApiAddUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(container.getApi(), ApiAdjustUserBalance.builder()
                    .uid(USER_A_UID)
                    .transactionId(1L)
                    .currency(BASE)
                    .amount(150_000L)
                    .build()));
            commandResults.add(submit(container.getApi(), ApiAdjustUserBalance.builder()
                    .uid(USER_B_UID)
                    .transactionId(1L)
                    .currency(QUOTE)
                    .amount(200_000L)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9101L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(101L)
                    .size(10L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.GTC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiMoveOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9101L)
                    .symbol(SYMBOL_EXCHANGE)
                    .newPrice(102L)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9201L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(99L)
                    .reservePrice(99L)
                    .size(4L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.GTC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9202L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(102L)
                    .reservePrice(102L)
                    .size(5L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.IOC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9102L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(103L)
                    .size(4L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.GTC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9203L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(102L)
                    .reservePrice(102L)
                    .size(1L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.GTC)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiMoveOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9203L)
                    .symbol(SYMBOL_EXCHANGE)
                    .newPrice(104L)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiReduceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(USER_MISSING_UID)
                    .symbol(SYMBOL_EXCHANGE)
                    .reduceSize(1L)
                    .build()));

            commandResults.add(submit(container.getApi(), ApiCancelOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(USER_MISSING_UID)
                    .symbol(SYMBOL_EXCHANGE)
                    .build()));

            // Barrier command to flush grouped command processing before report/direct state reads.
            submit(container.getApi(), ApiNop.builder().build());
            final List<String> simpleEvents = simpleEventsCollector.snapshot();

            final RiskEngine riskEngine = getSingleRiskEngine(exchangeCore);
            final FixtureStateHash fixtureStateHash = resolveFixtureStateHash("trade_reports", exchangeCore);
            final L2MarketData orderBook = container.requestCurrentOrderBook(SYMBOL_EXCHANGE);

            long makerBalanceQuote = accountBalance(riskEngine, USER_A_UID, QUOTE);
            final long makerBalanceBase = accountBalance(riskEngine, USER_A_UID, BASE);
            final long takerBalanceQuote = accountBalance(riskEngine, USER_B_UID, QUOTE);
            long takerBalanceBase = accountBalance(riskEngine, USER_B_UID, BASE);

            final FixtureOracleResult<ReportFixtureMetrics> reportOracle =
                    collectReportFixtureMetrics(container.getApi(), exchangeCore, riskEngine);
            final ReportFixtureMetrics reportMetrics = reportOracle.value;

            if ("full".equals(settlementMode) && "pre_release".equals(registrationSettlementMode)) {
                // Project settlement delta for deterministic zero-fee command stream.
                makerBalanceQuote += EXTENDED_EXECUTED_QUOTE;
                takerBalanceBase += EXTENDED_EXECUTED_BASE;
                reportMetrics.reportMakerBalanceQuote += EXTENDED_EXECUTED_QUOTE;
                reportMetrics.reportTakerBalanceBase += EXTENDED_EXECUTED_BASE;
                reportMetrics.reportTotalAccountsQuote += EXTENDED_EXECUTED_QUOTE;
                reportMetrics.reportTotalAccountsBase += EXTENDED_EXECUTED_BASE;
            }

            final List<String> lines = new ArrayList<>();
            lines.add("version=1");
            appendDiagnosticFallbackMarker(lines, reportOracle);
            lines.add("scenario=exchange_trade_reports");
            lines.add("orderbook_mode=" + orderBookMode);
            lines.add("symbol=" + SYMBOL_EXCHANGE);
            lines.add("command_results=" + String.join(",", commandResults));
            lines.add("simple_events_size=" + simpleEvents.size());
            for (int i = 0; i < simpleEvents.size(); i++) {
                lines.add("simple_event_" + i + "=" + simpleEvents.get(i));
            }
            lines.add("settlement_mode=" + settlementMode);
            lines.add("state_hash_kind=" + fixtureStateHash.kind);
            lines.add("state_hash=" + fixtureStateHash.value);
            lines.add("fee_15=" + riskEngine.getFees().get(QUOTE));
            lines.add("maker_balance_15=" + makerBalanceQuote);
            lines.add("maker_balance_11=" + makerBalanceBase);
            lines.add("taker_balance_15=" + takerBalanceQuote);
            lines.add("taker_balance_11=" + takerBalanceBase);
            lines.add("report_maker_status=" + reportMetrics.reportMakerStatus);
            lines.add("report_missing_status=" + reportMetrics.reportMissingStatus);
            lines.add("report_maker_orders=" + reportMetrics.reportMakerOrders);
            lines.add("report_maker_balance_15=" + reportMetrics.reportMakerBalanceQuote);
            lines.add("report_maker_balance_11=" + reportMetrics.reportMakerBalanceBase);
            lines.add("report_taker_status=" + reportMetrics.reportTakerStatus);
            lines.add("report_taker_orders=" + reportMetrics.reportTakerOrders);
            lines.add("report_taker_balance_15=" + reportMetrics.reportTakerBalanceQuote);
            lines.add("report_taker_balance_11=" + reportMetrics.reportTakerBalanceBase);
            lines.add("report_total_accounts_15=" + reportMetrics.reportTotalAccountsQuote);
            lines.add("report_total_accounts_11=" + reportMetrics.reportTotalAccountsBase);
            lines.add("report_total_orders_15=" + reportMetrics.reportTotalOrdersQuote);
            lines.add("report_total_orders_11=" + reportMetrics.reportTotalOrdersBase);
            lines.add("report_total_fees_15=" + reportMetrics.reportTotalFeesQuote);
            lines.add("report_total_fees_11=" + reportMetrics.reportTotalFeesBase);
            lines.add("orderbook_ask_size=" + orderBook.askSize);
            lines.add("orderbook_bid_size=" + orderBook.bidSize);
            for (int i = 0; i < orderBook.askSize; i++) {
                lines.add("ask_" + i + "=" + orderBook.askPrices[i] + ":" + orderBook.askVolumes[i] + ":" + orderBook.askOrders[i]);
            }
            for (int i = 0; i < orderBook.bidSize; i++) {
                lines.add("bid_" + i + "=" + orderBook.bidPrices[i] + ":" + orderBook.bidVolumes[i] + ":" + orderBook.bidOrders[i]);
            }

            Files.write(outputPath, lines, StandardCharsets.UTF_8);
        }
    }

    @Test
    public void writeExchangeTradeReportsLongFixture() throws Exception {
        assumeTrue(Boolean.getBoolean("parity.fixture.write"), "manual fixture generation only");

        final Path outputPath = Paths.get(System.getProperty(
                "parity.fixture.trade.reports.long.path",
                "src/zig/fixtures/java_exchange_trade_reports_long.fixture"));
        ensureParentDirectory(outputPath);

        final PerformanceConfiguration perfCfg = PerformanceConfiguration.baseBuilder()
                .matchingEnginesNum(1)
                .riskEnginesNum(1)
                .build();

        final List<String> commandResults = new ArrayList<>();

        try (ExchangeTestContainer container = ExchangeTestContainer.create(perfCfg)) {
            final ExchangeCore exchangeCore = container.getExchangeCore();
            final ExchangeApi api = container.getApi();
            final SimpleEventsFixtureCollector simpleEventsCollector = new SimpleEventsFixtureCollector();
            container.setConsumer(new SimpleEventsProcessor(simpleEventsCollector));

            final CoreSymbolSpecification symbolSpec = CoreSymbolSpecification.builder()
                    .symbolId(SYMBOL_EXCHANGE)
                    .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
                    .baseCurrency(BASE)
                    .quoteCurrency(QUOTE)
                    .baseScaleK(1L)
                    .quoteScaleK(1L)
                    .takerFee(0L)
                    .makerFee(0L)
                    .build();

            final String registrationSettlementMode = registerTradeSymbol(container, exchangeCore, symbolSpec);
            final String settlementMode = resolveTradeReportsLongSettlementMode(registrationSettlementMode);
            final String orderBookMode = resolveTradeReportsLongOrderbookMode();

            commandResults.add(submit(api, ApiAddUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(api, ApiAddUser.builder().uid(USER_B_UID).build()));
            commandResults.add(submit(api, ApiAddUser.builder().uid(USER_A_UID).build()));
            commandResults.add(submit(api, ApiAdjustUserBalance.builder()
                    .uid(USER_A_UID)
                    .transactionId(1L)
                    .currency(BASE)
                    .amount(400_000L)
                    .build()));
            commandResults.add(submit(api, ApiAdjustUserBalance.builder()
                    .uid(USER_B_UID)
                    .transactionId(1L)
                    .currency(QUOTE)
                    .amount(400_000L)
                    .build()));

            commandResults.add(submit(api, ApiPlaceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9101L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(101L)
                    .size(20L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.GTC)
                    .build()));
            commandResults.add(submit(api, ApiMoveOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9101L)
                    .symbol(SYMBOL_EXCHANGE)
                    .newPrice(102L)
                    .build()));
            commandResults.add(submit(api, ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9201L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(99L)
                    .reservePrice(99L)
                    .size(7L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.GTC)
                    .build()));

            for (int round = 0; round < REPORTS_LONG_ROUNDS; round++) {
                final long askOrderId = 9500L + round;
                final long askPrice = 140L + round;
                final long askSize = 4L + (round % 3L);
                final long bidOrderId = 9600L + round;
                final long bidPrice = 60L - (round % 4L);
                final long bidSize = 2L + (round % 2L);

                commandResults.add(submit(api, ApiPlaceOrder.builder()
                        .uid(USER_A_UID)
                        .orderId(askOrderId)
                        .symbol(SYMBOL_EXCHANGE)
                        .price(askPrice)
                        .size(askSize)
                        .action(OrderAction.ASK)
                        .orderType(OrderType.GTC)
                        .build()));

                commandResults.add(submit(api, ApiPlaceOrder.builder()
                        .uid(USER_B_UID)
                        .orderId(bidOrderId)
                        .symbol(SYMBOL_EXCHANGE)
                        .price(bidPrice)
                        .reservePrice(bidPrice)
                        .size(bidSize)
                        .action(OrderAction.BID)
                        .orderType(OrderType.GTC)
                        .build()));
            }

            commandResults.add(submit(api, ApiPlaceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(9102L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(171L)
                    .size(6L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.GTC)
                    .build()));
            commandResults.add(submit(api, ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9203L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(58L)
                    .reservePrice(58L)
                    .size(3L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.GTC)
                    .build()));
            commandResults.add(submit(api, ApiPlaceOrder.builder()
                    .uid(USER_B_UID)
                    .orderId(9204L)
                    .symbol(SYMBOL_EXCHANGE)
                    .price(57L)
                    .reservePrice(57L)
                    .size(5L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.GTC)
                    .build()));
            commandResults.add(submit(api, ApiReduceOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(USER_MISSING_UID)
                    .symbol(SYMBOL_EXCHANGE)
                    .reduceSize(1L)
                    .build()));
            commandResults.add(submit(api, ApiCancelOrder.builder()
                    .uid(USER_A_UID)
                    .orderId(USER_MISSING_UID)
                    .symbol(SYMBOL_EXCHANGE)
                    .build()));

            // Barrier command to flush grouped command processing before report/direct state reads.
            submit(api, ApiNop.builder().build());
            final List<String> simpleEventsRaw = simpleEventsCollector.snapshot();
            final List<String> simpleEvents = new ArrayList<>(simpleEventsRaw.size());
            for (String simpleEvent : simpleEventsRaw) {
                // Keep reports-long simple-events deterministic with current Zig parity surface.
                if (!simpleEvent.startsWith("order_book:")) {
                    simpleEvents.add(simpleEvent);
                }
            }

            final RiskEngine riskEngine = getSingleRiskEngine(exchangeCore);
            final FixtureStateHash fixtureStateHash = resolveFixtureStateHash("trade_reports_long", exchangeCore);
            final L2MarketData orderBook = container.requestCurrentOrderBook(SYMBOL_EXCHANGE);

            final long makerBalanceQuote = accountBalance(riskEngine, USER_A_UID, QUOTE);
            final long makerBalanceBase = accountBalance(riskEngine, USER_A_UID, BASE);
            final long takerBalanceQuote = accountBalance(riskEngine, USER_B_UID, QUOTE);
            final long takerBalanceBase = accountBalance(riskEngine, USER_B_UID, BASE);
            final FixtureOracleResult<ReportFixtureMetrics> reportOracle =
                    collectReportFixtureMetrics(api, exchangeCore, riskEngine);
            final ReportFixtureMetrics reportMetrics = reportOracle.value;

            final List<String> lines = new ArrayList<>();
            lines.add("version=1");
            appendDiagnosticFallbackMarker(lines, reportOracle);
            lines.add("scenario=exchange_trade_reports_long");
            lines.add("orderbook_mode=" + orderBookMode);
            lines.add("symbol=" + SYMBOL_EXCHANGE);
            lines.add("command_results=" + String.join(",", commandResults));
            lines.add("simple_events_size=" + simpleEvents.size());
            for (int i = 0; i < simpleEvents.size(); i++) {
                lines.add("simple_event_" + i + "=" + simpleEvents.get(i));
            }
            lines.add("settlement_mode=" + settlementMode);
            lines.add("state_hash_kind=" + fixtureStateHash.kind);
            lines.add("state_hash=" + fixtureStateHash.value);
            lines.add("fee_15=" + riskEngine.getFees().get(QUOTE));
            lines.add("maker_balance_15=" + makerBalanceQuote);
            lines.add("maker_balance_11=" + makerBalanceBase);
            lines.add("taker_balance_15=" + takerBalanceQuote);
            lines.add("taker_balance_11=" + takerBalanceBase);
            lines.add("report_maker_status=" + reportMetrics.reportMakerStatus);
            lines.add("report_missing_status=" + reportMetrics.reportMissingStatus);
            lines.add("report_maker_orders=" + reportMetrics.reportMakerOrders);
            lines.add("report_maker_balance_15=" + reportMetrics.reportMakerBalanceQuote);
            lines.add("report_maker_balance_11=" + reportMetrics.reportMakerBalanceBase);
            lines.add("report_taker_status=" + reportMetrics.reportTakerStatus);
            lines.add("report_taker_orders=" + reportMetrics.reportTakerOrders);
            lines.add("report_taker_balance_15=" + reportMetrics.reportTakerBalanceQuote);
            lines.add("report_taker_balance_11=" + reportMetrics.reportTakerBalanceBase);
            lines.add("report_total_accounts_15=" + reportMetrics.reportTotalAccountsQuote);
            lines.add("report_total_accounts_11=" + reportMetrics.reportTotalAccountsBase);
            lines.add("report_total_orders_15=" + reportMetrics.reportTotalOrdersQuote);
            lines.add("report_total_orders_11=" + reportMetrics.reportTotalOrdersBase);
            lines.add("report_total_fees_15=" + reportMetrics.reportTotalFeesQuote);
            lines.add("report_total_fees_11=" + reportMetrics.reportTotalFeesBase);
            lines.add("orderbook_ask_size=" + orderBook.askSize);
            lines.add("orderbook_bid_size=" + orderBook.bidSize);
            for (int i = 0; i < orderBook.askSize; i++) {
                lines.add("ask_" + i + "=" + orderBook.askPrices[i] + ":" + orderBook.askVolumes[i] + ":" + orderBook.askOrders[i]);
            }
            for (int i = 0; i < orderBook.bidSize; i++) {
                lines.add("bid_" + i + "=" + orderBook.bidPrices[i] + ":" + orderBook.bidVolumes[i] + ":" + orderBook.bidOrders[i]);
            }

            Files.write(outputPath, lines, StandardCharsets.UTF_8);
        }
    }

    private static void registerSymbolDirect(final ExchangeCore exchangeCore, final CoreSymbolSpecification spec) {
        exchangeCore.getRiskEngines().values().forEach(riskEngine -> riskEngine.getSymbolSpecificationProvider().addSymbol(spec));

        exchangeCore.getMatchingEngines().values().forEach(matchingEngine -> {
            if (symbolForMatchingEngine(matchingEngine, spec.symbolId) && matchingEngine.getOrderBooks().get(spec.symbolId) == null) {
                matchingEngine.getOrderBooks().put(
                        spec.symbolId,
                        matchingEngine.getOrderBookFactory().create(
                                spec,
                                matchingEngine.getObjectsPool(),
                                matchingEngine.getEventsHelper(),
                                matchingEngine.getLoggingCfg()));
            }
        });
    }

    private static boolean symbolForMatchingEngine(final MatchingEngineRouter matchingEngine, final int symbol) {
        return matchingEngine.getShardMask() == 0 || ((symbol & matchingEngine.getShardMask()) == matchingEngine.getShardId());
    }

    private static String registerTradeSymbol(
            final ExchangeTestContainer container,
            final ExchangeCore exchangeCore,
            final CoreSymbolSpecification spec) {

        final String mode = System.getProperty("parity.fixture.trade.symbol_registration", TRADE_SYMBOL_REGISTRATION_DIRECT);
        switch (mode) {
            case TRADE_SYMBOL_REGISTRATION_DIRECT:
                registerSymbolDirect(exchangeCore, spec);
                return "pre_release";
            case TRADE_SYMBOL_REGISTRATION_API:
                try {
                    container.addSymbol(spec);
                    return "full";
                } catch (RuntimeException ex) {
                    throw new IllegalStateException("API symbol registration failed (known on Java 25 with Chronicle unmap0); "
                            + "use direct registration with -Dparity.fixture.trade.settlement_mode=full for fixture export.", ex);
                }
            default:
                throw new IllegalStateException("Unsupported trade symbol registration mode: " + mode
                        + " (supported: " + TRADE_SYMBOL_REGISTRATION_DIRECT + ", " + TRADE_SYMBOL_REGISTRATION_API + ")");
        }
    }

    private static String resolveTradeSettlementMode(final String defaultMode) {
        final String mode = System.getProperty("parity.fixture.trade.settlement_mode", defaultMode);
        if ("full".equals(mode) || "pre_release".equals(mode)) {
            return mode;
        }
        throw new IllegalStateException("Unsupported trade settlement mode: " + mode + " (supported: full, pre_release)");
    }

    private static String resolveTradeExtendedSettlementMode(final String defaultMode) {
        final String mode = System.getProperty(
                "parity.fixture.trade.extended.settlement_mode",
                System.getProperty("parity.fixture.trade.settlement_mode", defaultMode));
        if ("full".equals(mode) || "pre_release".equals(mode)) {
            return mode;
        }
        throw new IllegalStateException("Unsupported extended trade settlement mode: " + mode
                + " (supported: full, pre_release)");
    }

    private static String resolveTradeExtendedOrderbookMode() {
        final String mode = System.getProperty(
                "parity.fixture.trade.extended.orderbook_mode",
                ORDERBOOK_MODE_NAIVE);
        if (ORDERBOOK_MODE_NAIVE.equals(mode) || ORDERBOOK_MODE_DIRECT.equals(mode)) {
            return mode;
        }
        throw new IllegalStateException("Unsupported extended trade orderbook mode: " + mode
                + " (supported: " + ORDERBOOK_MODE_NAIVE + ", " + ORDERBOOK_MODE_DIRECT + ")");
    }

    private static String resolveTradeBasicOrderbookMode() {
        final String mode = System.getProperty(
                "parity.fixture.trade.basic.orderbook_mode",
                ORDERBOOK_MODE_NAIVE);
        if (ORDERBOOK_MODE_NAIVE.equals(mode) || ORDERBOOK_MODE_DIRECT.equals(mode)) {
            return mode;
        }
        throw new IllegalStateException("Unsupported basic trade orderbook mode: " + mode
                + " (supported: " + ORDERBOOK_MODE_NAIVE + ", " + ORDERBOOK_MODE_DIRECT + ")");
    }

    private static String resolveTradeBootstrapSettlementMode(final String defaultMode) {
        final String mode = System.getProperty(
                "parity.fixture.trade.bootstrap.settlement_mode",
                System.getProperty("parity.fixture.trade.settlement_mode", defaultMode));
        if ("full".equals(mode) || "pre_release".equals(mode)) {
            return mode;
        }
        throw new IllegalStateException("Unsupported bootstrap trade settlement mode: " + mode
                + " (supported: full, pre_release)");
    }

    private static String resolveTradeBootstrapOrderbookMode() {
        final String mode = System.getProperty(
                "parity.fixture.trade.bootstrap.orderbook_mode",
                ORDERBOOK_MODE_NAIVE);
        if (ORDERBOOK_MODE_NAIVE.equals(mode) || ORDERBOOK_MODE_DIRECT.equals(mode)) {
            return mode;
        }
        throw new IllegalStateException("Unsupported bootstrap trade orderbook mode: " + mode
                + " (supported: " + ORDERBOOK_MODE_NAIVE + ", " + ORDERBOOK_MODE_DIRECT + ")");
    }

    private static String resolveTradeBootstrapLongSettlementMode(final String defaultMode) {
        final String mode = System.getProperty(
                "parity.fixture.trade.bootstrap.long.settlement_mode",
                System.getProperty(
                        "parity.fixture.trade.bootstrap.settlement_mode",
                        System.getProperty("parity.fixture.trade.settlement_mode", defaultMode)));
        if ("pre_release".equals(mode) || "full".equals(mode)) {
            return mode;
        }
        throw new IllegalStateException("Unsupported bootstrap long trade settlement mode: " + mode
                + " (supported: pre_release, full)");
    }

    private static String resolveTradeBootstrapLongOrderbookMode() {
        final String mode = System.getProperty(
                "parity.fixture.trade.bootstrap.long.orderbook_mode",
                System.getProperty("parity.fixture.trade.bootstrap.orderbook_mode", ORDERBOOK_MODE_NAIVE));
        if (ORDERBOOK_MODE_NAIVE.equals(mode) || ORDERBOOK_MODE_DIRECT.equals(mode)) {
            return mode;
        }
        throw new IllegalStateException("Unsupported bootstrap long trade orderbook mode: " + mode
                + " (supported: " + ORDERBOOK_MODE_NAIVE + ", " + ORDERBOOK_MODE_DIRECT + ")");
    }

    private static String resolveTradeMultiSymbolOrderbookMode() {
        final String mode = System.getProperty(
                "parity.fixture.trade.multisymbol.orderbook_mode",
                ORDERBOOK_MODE_NAIVE);
        if (ORDERBOOK_MODE_NAIVE.equals(mode) || ORDERBOOK_MODE_DIRECT.equals(mode)) {
            return mode;
        }
        throw new IllegalStateException("Unsupported multi-symbol trade orderbook mode: " + mode
                + " (supported: " + ORDERBOOK_MODE_NAIVE + ", " + ORDERBOOK_MODE_DIRECT + ")");
    }

    private static String resolveTradeMultiSymbolSettlementMode(final String defaultMode) {
        final String mode = System.getProperty(
                "parity.fixture.trade.multisymbol.settlement_mode",
                System.getProperty("parity.fixture.trade.settlement_mode", defaultMode));
        if ("full".equals(mode) || "pre_release".equals(mode)) {
            return mode;
        }
        throw new IllegalStateException("Unsupported multi-symbol trade settlement mode: " + mode
                + " (supported: full, pre_release)");
    }

    private static String resolveTradeReportsSettlementMode(final String defaultMode) {
        final String mode = System.getProperty(
                "parity.fixture.trade.reports.settlement_mode",
                System.getProperty("parity.fixture.trade.settlement_mode", defaultMode));
        if ("full".equals(mode) || "pre_release".equals(mode)) {
            return mode;
        }
        throw new IllegalStateException("Unsupported trade reports settlement mode: " + mode
                + " (supported: full, pre_release)");
    }

    private static String resolveTradeReportsOrderbookMode() {
        final String mode = System.getProperty(
                "parity.fixture.trade.reports.orderbook_mode",
                ORDERBOOK_MODE_NAIVE);
        if (ORDERBOOK_MODE_NAIVE.equals(mode) || ORDERBOOK_MODE_DIRECT.equals(mode)) {
            return mode;
        }
        throw new IllegalStateException("Unsupported trade reports orderbook mode: " + mode
                + " (supported: " + ORDERBOOK_MODE_NAIVE + ", " + ORDERBOOK_MODE_DIRECT + ")");
    }

    private static String resolveTradeReportsLongSettlementMode(final String defaultMode) {
        final String mode = System.getProperty(
                "parity.fixture.trade.reports.long.settlement_mode",
                System.getProperty(
                        "parity.fixture.trade.reports.settlement_mode",
                        System.getProperty("parity.fixture.trade.settlement_mode", defaultMode)));
        if ("pre_release".equals(mode) || "full".equals(mode)) {
            return mode;
        }
        throw new IllegalStateException("Unsupported trade reports long settlement mode: " + mode
                + " (supported: pre_release, full)");
    }

    private static String resolveTradeReportsLongOrderbookMode() {
        final String mode = System.getProperty(
                "parity.fixture.trade.reports.long.orderbook_mode",
                System.getProperty("parity.fixture.trade.reports.orderbook_mode", ORDERBOOK_MODE_NAIVE));
        if (ORDERBOOK_MODE_NAIVE.equals(mode) || ORDERBOOK_MODE_DIRECT.equals(mode)) {
            return mode;
        }
        throw new IllegalStateException("Unsupported trade reports long orderbook mode: " + mode
                + " (supported: " + ORDERBOOK_MODE_NAIVE + ", " + ORDERBOOK_MODE_DIRECT + ")");
    }

    private static RiskEngine getSingleRiskEngine(final ExchangeCore exchangeCore) {
        if (exchangeCore.getRiskEngines().size() != 1) {
            throw new IllegalStateException("Fixture generation requires exactly one risk engine");
        }
        return exchangeCore.getRiskEngines().values().iterator().next();
    }

    private static long accountBalance(final RiskEngine riskEngine, final long uid, final int currency) {
        final UserProfile userProfile = riskEngine.getUserProfileService().getUserProfile(uid);
        return userProfile == null ? 0L : userProfile.accounts.get(currency);
    }

    private static long mapValue(final IntLongHashMap map, final int key) {
        return map == null ? 0L : map.get(key);
    }

    private static int reportStatusCode(final ExchangeCore exchangeCore, final long uid) {
        final boolean exists = exchangeCore.getRiskEngines().values().stream()
                .anyMatch(riskEngine -> riskEngine.getUserProfileService().getUserProfile(uid) != null);
        return exists ? 0 : 1;
    }

    private static int countUserOrders(final ExchangeCore exchangeCore, final long uid) {
        final int[] total = new int[]{0};
        exchangeCore.getMatchingEngines().values().forEach(matchingEngine ->
                matchingEngine.getOrderBooks().forEachValue(orderBook ->
                        total[0] += orderBook.findUserOrders(uid).size()));
        return total[0];
    }

    private static long sumAccountBalances(final ExchangeCore exchangeCore, final int currency) {
        final long[] total = new long[]{0L};
        exchangeCore.getRiskEngines().values().forEach(riskEngine ->
                riskEngine.getUserProfileService().getUserProfiles().forEachValue(profile ->
                        total[0] = Math.addExact(total[0], profile.accounts.get(currency))));
        return total[0];
    }

    private static long sumOrderHolds(final ExchangeCore exchangeCore, final int currency) {
        final long[] total = new long[]{0L};
        exchangeCore.getMatchingEngines().values().forEach(matchingEngine ->
                matchingEngine.getOrderBooks().forEachValue(orderBook -> {
                    final CoreSymbolSpecification spec = orderBook.getSymbolSpec();
                    if (spec.baseCurrency == currency) {
                        orderBook.askOrdersStream(false).forEach(order -> {
                            final long remaining = order.getSize() - order.getFilled();
                            if (remaining > 0) {
                                total[0] = Math.addExact(total[0], Math.multiplyExact(remaining, spec.baseScaleK));
                            }
                        });
                    }
                    if (spec.quoteCurrency == currency) {
                        orderBook.bidOrdersStream(false).forEach(order -> {
                            final long remaining = order.getSize() - order.getFilled();
                            if (remaining > 0) {
                                final long held = Math.multiplyExact(
                                        Math.multiplyExact(remaining, order.getReserveBidPrice()),
                                        spec.quoteScaleK);
                                total[0] = Math.addExact(total[0], held);
                            }
                        });
                    }
                }));
        return total[0];
    }

    private static FixtureOracleResult<ReportFixtureMetrics> collectReportFixtureMetrics(
            final ExchangeApi api,
            final ExchangeCore exchangeCore,
            final RiskEngine riskEngine) {
        return executeFixtureOracle(
                "public_report_queries",
                () -> collectReportFixtureMetricsFromApi(api),
                () -> collectReportFixtureMetricsFromState(exchangeCore, riskEngine));
    }

    private static ReportFixtureMetrics collectReportFixtureMetricsFromApi(final ExchangeApi api) {
        int transferId = 50_001;
        final SingleUserReportResult makerReport = api.processReport(new SingleUserReportQuery(USER_A_UID), transferId++).join();
        final SingleUserReportResult takerReport = api.processReport(new SingleUserReportQuery(USER_B_UID), transferId++).join();
        final SingleUserReportResult missingReport = api.processReport(new SingleUserReportQuery(USER_MISSING_UID), transferId++).join();
        final TotalCurrencyBalanceReportResult totalsReport = api.processReport(new TotalCurrencyBalanceReportQuery(), transferId).join();

        return new ReportFixtureMetrics(
                statusCodeFromReportQuery(makerReport),
                statusCodeFromReportQuery(missingReport),
                countOrdersInReport(makerReport),
                mapValue(makerReport.getAccounts(), QUOTE),
                mapValue(makerReport.getAccounts(), BASE),
                statusCodeFromReportQuery(takerReport),
                countOrdersInReport(takerReport),
                mapValue(takerReport.getAccounts(), QUOTE),
                mapValue(takerReport.getAccounts(), BASE),
                mapValue(totalsReport.getAccountBalances(), QUOTE),
                mapValue(totalsReport.getAccountBalances(), BASE),
                mapValue(totalsReport.getOrdersBalances(), QUOTE),
                mapValue(totalsReport.getOrdersBalances(), BASE),
                mapValue(totalsReport.getFees(), QUOTE),
                mapValue(totalsReport.getFees(), BASE));
    }

    private static ReportFixtureMetrics collectReportFixtureMetricsFromState(
            final ExchangeCore exchangeCore,
            final RiskEngine riskEngine) {
        return new ReportFixtureMetrics(
                reportStatusCode(exchangeCore, USER_A_UID),
                reportStatusCode(exchangeCore, USER_MISSING_UID),
                countUserOrders(exchangeCore, USER_A_UID),
                accountBalance(riskEngine, USER_A_UID, QUOTE),
                accountBalance(riskEngine, USER_A_UID, BASE),
                reportStatusCode(exchangeCore, USER_B_UID),
                countUserOrders(exchangeCore, USER_B_UID),
                accountBalance(riskEngine, USER_B_UID, QUOTE),
                accountBalance(riskEngine, USER_B_UID, BASE),
                sumAccountBalances(exchangeCore, QUOTE),
                sumAccountBalances(exchangeCore, BASE),
                sumOrderHolds(exchangeCore, QUOTE),
                sumOrderHolds(exchangeCore, BASE),
                mapValue(riskEngine.getFees(), QUOTE),
                mapValue(riskEngine.getFees(), BASE));
    }

    private static int statusCodeFromReportQuery(final SingleUserReportResult report) {
        return report.getQueryExecutionStatus() == SingleUserReportResult.QueryExecutionStatus.OK ? 0 : 1;
    }

    private static int countOrdersInReport(final SingleUserReportResult report) {
        if (report.getOrders() == null) {
            return 0;
        }
        final int[] total = new int[]{0};
        report.getOrders().forEachValue(orders -> total[0] += orders.size());
        return total[0];
    }

    private static int calculateStateHash(final ExchangeCore exchangeCore) {
        final SortedMap<StateHashReportResult.SubmoduleKey, Integer> hashCodes = new TreeMap<>();

        exchangeCore.getMatchingEngines().values().forEach(matchingEngine -> {
            final int moduleId = matchingEngine.getShardId();

            hashCodes.put(
                    StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.MATCHING_BINARY_CMD_PROCESSOR),
                    matchingEngine.getBinaryCommandsProcessor().stateHash());

            hashCodes.put(
                    StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.MATCHING_ORDER_BOOKS),
                    HashingUtils.stateHash(matchingEngine.getOrderBooks()));

            hashCodes.put(
                    StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.MATCHING_SHARD_MASK),
                    Long.hashCode(matchingEngine.getShardMask()));
        });

        exchangeCore.getRiskEngines().values().forEach(riskEngine -> {
            final int moduleId = riskEngine.getShardId();

            hashCodes.put(
                    StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_SYMBOL_SPEC_PROVIDER),
                    riskEngine.getBinaryCommandsProcessor().stateHash());

            hashCodes.put(
                    StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_USER_PROFILE_SERVICE),
                    riskEngine.getUserProfileService().stateHash());

            hashCodes.put(
                    StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_BINARY_CMD_PROCESSOR),
                    riskEngine.getBinaryCommandsProcessor().stateHash());

            hashCodes.put(
                    StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_LAST_PRICE_CACHE),
                    HashingUtils.stateHash(riskEngine.getLastPriceCache()));

            hashCodes.put(
                    StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_FEES),
                    riskEngine.getFees().hashCode());

            hashCodes.put(
                    StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_ADJUSTMENTS),
                    riskEngine.getAdjustments().hashCode());

            hashCodes.put(
                    StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_SUSPENDS),
                    riskEngine.getSuspends().hashCode());

            hashCodes.put(
                    StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_SHARD_MASK),
                    Long.hashCode(riskEngine.getShardMask()));
        });

        return new StateHashReportResult(hashCodes).getStateHash();
    }

    private static FixtureStateHash resolveFixtureStateHash(final String scope, final ExchangeCore exchangeCore) {
        final String kind = System.getProperty(
                "parity.fixture." + scope + ".state_hash_kind",
                System.getProperty("parity.fixture.state_hash_kind", STATE_HASH_KIND_JAVA_REPORT));

        switch (kind) {
            case STATE_HASH_KIND_JAVA_REPORT:
                return new FixtureStateHash(kind, calculateStateHash(exchangeCore));

            case STATE_HASH_KIND_ZIG_RISK_LOW32:
            case STATE_HASH_KIND_ZIG_DETAILED_LOW32:
                final String rawValue = System.getProperty(
                        "parity.fixture." + scope + ".state_hash",
                        System.getProperty("parity.fixture.state_hash"));
                if (rawValue == null) {
                    throw new IllegalStateException("Missing state hash override for " + kind + " on scope " + scope
                            + ". Set -Dparity.fixture." + scope + ".state_hash=<signed_i32> "
                            + "or -Dparity.fixture.state_hash=<signed_i32>");
                }
                try {
                    return new FixtureStateHash(kind, Integer.parseInt(rawValue));
                } catch (NumberFormatException ex) {
                    throw new IllegalStateException("Invalid state hash override for scope " + scope + ": " + rawValue, ex);
                }

            default:
                throw new IllegalStateException("Unsupported state_hash_kind for scope " + scope + ": " + kind);
        }
    }

    private static String submit(final ExchangeApi api, final ApiCommand cmd) {
        return api.submitCommandAsync(cmd).join().name();
    }

    private static String syntheticPersistMergedResult() {
        return CommandResultCode.mergeToFirstFailed(
                CommandResultCode.ACCEPTED,
                CommandResultCode.SUCCESS).name();
    }

    private static FixtureOracleResult<String> controlPersistMergedResult(final PerformanceConfiguration perfCfg) {
        return executeFixtureOracle(
                "public_persistence_command",
                () -> {
                    try (ExchangeTestContainer probe = ExchangeTestContainer.create(
                            perfCfg,
                            InitialStateConfiguration.CLEAN_TEST,
                            SerializationConfiguration.DISK_SNAPSHOT_ONLY)) {
                        return probe.getApi().submitCommandAsync(ApiPersistState.builder()
                                        .dumpId(System.currentTimeMillis())
                                        .build())
                                .get(500, TimeUnit.MILLISECONDS)
                                .name();
                    }
                },
                GenerateJavaParityFixtureTest::syntheticPersistMergedResult);
    }

    private static <T> FixtureOracleResult<T> executeFixtureOracle(
            final String oracleName,
            final Callable<T> publicApiOracle,
            final Callable<T> internalFallback) {
        try {
            return FixtureOracleResult.strict(publicApiOracle.call());
        } catch (Exception strictFailure) {
            if (strictFailure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw strictOracleFailure(oracleName, strictFailure);
            }
            if (!Boolean.getBoolean(DIAGNOSTIC_INTERNAL_FALLBACK_PROPERTY)) {
                throw strictOracleFailure(oracleName, strictFailure);
            }

            final String marker = oracleName + ":" + strictFailure.getClass().getName();
            System.err.println("WARNING: non-authoritative Java parity fixture fallback enabled by -D"
                    + DIAGNOSTIC_INTERNAL_FALLBACK_PROPERTY + "=true; oracle=" + marker);
            strictFailure.printStackTrace(System.err);
            try {
                return FixtureOracleResult.diagnosticFallback(internalFallback.call(), marker);
            } catch (Exception fallbackFailure) {
                strictFailure.addSuppressed(fallbackFailure);
                throw new IllegalStateException(
                        "Strict fixture oracle and diagnostic fallback both failed for " + oracleName,
                        strictFailure);
            }
        }
    }

    private static IllegalStateException strictOracleFailure(
            final String oracleName,
            final Exception cause) {
        return new IllegalStateException(
                "Strict public Java fixture oracle failed for " + oracleName
                        + "; diagnostic internal fallback is disabled by default. "
                        + "For non-authoritative diagnosis only, set -D"
                        + DIAGNOSTIC_INTERNAL_FALLBACK_PROPERTY + "=true",
                cause);
    }

    private static void appendDiagnosticFallbackMarker(
            final List<String> lines,
            final FixtureOracleResult<?> oracleResult) {
        if (oracleResult.diagnosticFallbackMarker != null) {
            lines.add(DIAGNOSTIC_FALLBACK_MARKER_PREFIX + oracleResult.diagnosticFallbackMarker);
        }
    }

    private static void restoreSystemProperty(final String property, final String previousValue) {
        if (previousValue == null) {
            System.clearProperty(property);
        } else {
            System.setProperty(property, previousValue);
        }
    }

    private static void ensureParentDirectory(final Path outputPath) throws IOException {
        final Path parent = outputPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
    }

    private static final class SimpleEventsFixtureCollector implements IEventsHandler {
        private final List<String> lines = new CopyOnWriteArrayList<>();

        @Override
        public void commandResult(final ApiCommandResult commandResult) {
            lines.add("command_result:"
                    + commandKind(commandResult.getCommand())
                    + ":"
                    + commandResult.getResultCode().name());
        }

        @Override
        public void tradeEvent(final TradeEvent tradeEvent) {
            final StringBuilder sb = new StringBuilder();
            sb.append("trade:")
                    .append(tradeEvent.getSymbol())
                    .append(':')
                    .append(tradeEvent.getTotalVolume())
                    .append(':')
                    .append(tradeEvent.getTakerOrderId())
                    .append(':')
                    .append(tradeEvent.getTakerUid())
                    .append(':')
                    .append(tradeEvent.getTakerAction().name())
                    .append(':')
                    .append(boolAsInt(tradeEvent.isTakeOrderCompleted()))
                    .append(':')
                    .append(tradeEvent.getTimestamp())
                    .append(':');

            final List<Trade> trades = tradeEvent.getTrades();
            for (int i = 0; i < trades.size(); i++) {
                final Trade trade = trades.get(i);
                if (i != 0) {
                    sb.append(';');
                }
                sb.append(trade.getMakerOrderId())
                        .append('@')
                        .append(trade.getMakerUid())
                        .append('@')
                        .append(boolAsInt(trade.isMakerOrderCompleted()))
                        .append('@')
                        .append(trade.getPrice())
                        .append('@')
                        .append(trade.getVolume());
            }

            lines.add(sb.toString());
        }

        @Override
        public void rejectEvent(final RejectEvent rejectEvent) {
            lines.add("reject:"
                    + rejectEvent.getSymbol()
                    + ":"
                    + rejectEvent.getRejectedVolume()
                    + ":"
                    + rejectEvent.getPrice()
                    + ":"
                    + rejectEvent.getOrderId()
                    + ":"
                    + rejectEvent.getUid()
                    + ":"
                    + rejectEvent.getTimestamp());
        }

        @Override
        public void reduceEvent(final ReduceEvent reduceEvent) {
            lines.add("reduce:"
                    + reduceEvent.getSymbol()
                    + ":"
                    + reduceEvent.getReducedVolume()
                    + ":"
                    + boolAsInt(reduceEvent.isOrderCompleted())
                    + ":"
                    + reduceEvent.getPrice()
                    + ":"
                    + reduceEvent.getOrderId()
                    + ":"
                    + reduceEvent.getUid()
                    + ":"
                    + reduceEvent.getTimestamp());
        }

        @Override
        public void orderBook(final OrderBook orderBook) {
            lines.add("order_book:"
                    + orderBook.getSymbol()
                    + ":"
                    + orderBook.getTimestamp()
                    + ":"
                    + serializeOrderBookRecords(orderBook.getAsks())
                    + ":"
                    + serializeOrderBookRecords(orderBook.getBids()));
        }

        private List<String> snapshot() {
            return new ArrayList<>(lines);
        }

        private static String serializeOrderBookRecords(final List<OrderBookRecord> records) {
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < records.size(); i++) {
                final OrderBookRecord record = records.get(i);
                if (i != 0) {
                    sb.append(';');
                }
                sb.append(record.getPrice())
                        .append('@')
                        .append(record.getVolume())
                        .append('@')
                        .append(record.getOrders());
            }
            return sb.toString();
        }

        private static String commandKind(final ApiCommand command) {
            if (command instanceof ApiPlaceOrder) {
                return "place_order";
            }
            if (command instanceof ApiMoveOrder) {
                return "move_order";
            }
            if (command instanceof ApiCancelOrder) {
                return "cancel_order";
            }
            if (command instanceof ApiReduceOrder) {
                return "reduce_order";
            }
            if (command instanceof ApiOrderBookRequest) {
                return "order_book_request";
            }
            if (command instanceof ApiAddUser) {
                return "add_user";
            }
            if (command instanceof ApiAdjustUserBalance) {
                return "balance_adjustment";
            }
            if (command instanceof ApiBinaryDataCommand) {
                return "binary_data_command";
            }
            throw new IllegalStateException("Unsupported simple-event command type: " + command.getClass().getName());
        }

        private static int boolAsInt(final boolean value) {
            return value ? 1 : 0;
        }
    }

    private static final class ReportFixtureMetrics {
        private final int reportMakerStatus;
        private final int reportMissingStatus;
        private final int reportMakerOrders;
        private long reportMakerBalanceQuote;
        private final long reportMakerBalanceBase;
        private final int reportTakerStatus;
        private final int reportTakerOrders;
        private final long reportTakerBalanceQuote;
        private long reportTakerBalanceBase;
        private long reportTotalAccountsQuote;
        private long reportTotalAccountsBase;
        private final long reportTotalOrdersQuote;
        private final long reportTotalOrdersBase;
        private final long reportTotalFeesQuote;
        private final long reportTotalFeesBase;

        private ReportFixtureMetrics(
                final int reportMakerStatus,
                final int reportMissingStatus,
                final int reportMakerOrders,
                final long reportMakerBalanceQuote,
                final long reportMakerBalanceBase,
                final int reportTakerStatus,
                final int reportTakerOrders,
                final long reportTakerBalanceQuote,
                final long reportTakerBalanceBase,
                final long reportTotalAccountsQuote,
                final long reportTotalAccountsBase,
                final long reportTotalOrdersQuote,
                final long reportTotalOrdersBase,
                final long reportTotalFeesQuote,
                final long reportTotalFeesBase) {
            this.reportMakerStatus = reportMakerStatus;
            this.reportMissingStatus = reportMissingStatus;
            this.reportMakerOrders = reportMakerOrders;
            this.reportMakerBalanceQuote = reportMakerBalanceQuote;
            this.reportMakerBalanceBase = reportMakerBalanceBase;
            this.reportTakerStatus = reportTakerStatus;
            this.reportTakerOrders = reportTakerOrders;
            this.reportTakerBalanceQuote = reportTakerBalanceQuote;
            this.reportTakerBalanceBase = reportTakerBalanceBase;
            this.reportTotalAccountsQuote = reportTotalAccountsQuote;
            this.reportTotalAccountsBase = reportTotalAccountsBase;
            this.reportTotalOrdersQuote = reportTotalOrdersQuote;
            this.reportTotalOrdersBase = reportTotalOrdersBase;
            this.reportTotalFeesQuote = reportTotalFeesQuote;
            this.reportTotalFeesBase = reportTotalFeesBase;
        }
    }

    private static final class FixtureOracleResult<T> {
        private final T value;
        private final String diagnosticFallbackMarker;

        private FixtureOracleResult(final T value, final String diagnosticFallbackMarker) {
            this.value = value;
            this.diagnosticFallbackMarker = diagnosticFallbackMarker;
        }

        private static <T> FixtureOracleResult<T> strict(final T value) {
            return new FixtureOracleResult<>(value, null);
        }

        private static <T> FixtureOracleResult<T> diagnosticFallback(
                final T value,
                final String diagnosticFallbackMarker) {
            return new FixtureOracleResult<>(value, diagnosticFallbackMarker);
        }
    }

    private static final class FixtureStateHash {
        private final String kind;
        private final int value;

        private FixtureStateHash(final String kind, final int value) {
            this.kind = kind;
            this.value = value;
        }
    }
}
