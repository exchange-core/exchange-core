# Java Parity Fixtures

This folder stores machine-readable fixture files exported from the Java engine
for Zig parity comparison.

## Persistence fixture (`java-persistence/v1`)

`java-persistence/v1` contains a real 1ME/1RE
`DiskSerializationProcessor` snapshot and raw journal:

- `JAVAPERSISTV1_snapshot_4242424242_ME0.ecs`
- `JAVAPERSISTV1_snapshot_4242424242_RE0.ecs`
- `JAVAPERSISTV1_journal_4242424242_0001.ecj`
- `manifest.json`, including byte lengths, SHA-256 checksums, topology,
  sequences, and expected snapshot/post-journal business state

The generator runs the same scenario twice, requires byte-identical source
artifacts, verifies snapshot-only and snapshot-plus-journal recovery in Java,
then publishes or compares the committed fixture. The committed manifest omits
JVM enum/object identity hashes because they are process-local; each generator
run still checks those hashes across its own snapshot and journal recovery:

```sh
mvn \
  -Dtest=exchange.core2.tests.fixtures.GenerateJavaPersistenceFixtureTest#writeDeterministicDiskPersistenceFixture \
  -Djava.persistence.fixture.write=true \
  '-Dtest.jvm.addExports=--add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-exports java.base/jdk.internal.ref=ALL-UNNAMED' \
  test
```

Chronicle 2.19.1 requires a compatible JDK; the committed v1 family was
generated on Temurin 11.0.32+9. The `.eca` source log is deliberately excluded
because its metadata contains wall-clock values; `manifest.json` records that
policy. The writer was run successfully in two separate JVM processes to prove
portable-manifest determinism. Zig converter tests embed the three source
artifacts, convert them to XCM2/XCJ3, and assert native readback plus balances,
fees, adjustments, positions, orders, L2 state, actual journal results, and
sequence frontiers.

## Fixture format (`.fixture`)

Current fixture key-value lines:

- `version`
- `scenario`
- `orderbook_mode`
- `symbol`
- `command_results` (comma-separated `CommandResultCode` names)
- optional simple-events parity stream fields:
  - `simple_events_size`
  - `simple_event_<n>` ordered from `0..simple_events_size-1`
  - canonical payload format:
    - `command_result:<command_kind>:<result_code>`
    - `trade:<symbol>:<total_volume>:<taker_order_id>:<taker_uid>:<taker_action>:<taker_order_completed_0_or_1>:<timestamp>:<maker_order_id>@<maker_uid>@<maker_order_completed_0_or_1>@<price>@<volume>[;...]`
    - `reject:<symbol>:<rejected_volume>:<price>:<order_id>:<uid>:<timestamp>`
    - `reduce:<symbol>:<reduced_volume>:<order_completed_0_or_1>:<price>:<order_id>:<uid>:<timestamp>`
    - `order_book:<symbol>:<timestamp>:<ask_price>@<ask_volume>@<ask_orders>[;...]:<bid_price>@<bid_volume>@<bid_orders>[;...]`
- `settlement_mode` (`full` or `pre_release`)
- `state_hash_kind` (`zig_risk_low32` or `zig_detailed_low32`; `java_report` is
  accepted as a legacy alias)
- `state_hash`
- `fee_15`
- `maker_balance_15`
- `maker_balance_11`
- `taker_balance_15`
- `taker_balance_11`
- report fields for `scenario=exchange_trade_reports`:
  - `report_maker_status`
  - `report_missing_status`
  - `report_maker_orders`
  - `report_maker_balance_15`
  - `report_maker_balance_11`
  - `report_taker_status`
  - `report_taker_orders`
  - `report_taker_balance_15`
  - `report_taker_balance_11`
  - `report_total_accounts_15`
  - `report_total_accounts_11`
  - `report_total_orders_15`
  - `report_total_orders_11`
  - `report_total_fees_15`
  - `report_total_fees_11`
- `orderbook_ask_size`
- `orderbook_bid_size`
- `ask_<n>` and `bid_<n>` lines in `price:volume:orders` format

Current parity-diff normalization rules:

- `state_hash` is compared for both deterministic kinds.
- `state_hash_kind=java_report` is treated as a legacy alias of
  `zig_detailed_low32` in parity-diff.
- `zig_risk_low32` and `zig_detailed_low32` fixture values are Zig low-32-bit
  projections supplied to Java generation. They are not independent Java
  identity hashes; exact StateHash requires `JavaIdentityHashContext`.
- For `settlement_mode=pre_release`, parity-diff applies deterministic
  Java-aligned settlement normalization:
  - Per-user settlement-sensitive balances are compared against canonical
    full-settlement targets:
    - `exchange_trade_basic`: `maker_balance_15=500`, `taker_balance_11=5`
    - `exchange_trade_extended`, `exchange_trade_bootstrap`,
      `exchange_trade_reports`: `maker_balance_15=612`, `taker_balance_11=6`
    - `exchange_trade_multisymbol`: `maker_balance_15=884`, `taker_balance_11=7`
  - Report aggregate account totals (for scenarios with report fields) are
    normalized additively with the same quote/base deltas.

## Generate fixture from Java

The generator lives in:

- `src/test/java/exchange/core2/tests/fixtures/GenerateJavaParityFixtureTest.java`

Run (when Maven is available):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest \
    -Dparity.fixture.write=true \
    -Dparity.fixture.path=src/zig/fixtures/java_user_balance_basic.fixture \
    -Dparity.fixture.control.path=src/zig/fixtures/java_exchange_control.fixture \
    -Dparity.fixture.trade.path=src/zig/fixtures/java_exchange_trade_basic.fixture \
    test
```

The test is opt-in and only writes files when `-Dparity.fixture.write=true` is set.

Generate strict low32 hash fixture (for Zig risk hash parity checks):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest \
    -Dparity.fixture.write=true \
    -Dparity.fixture.path=src/zig/fixtures/java_user_balance_basic_low32.fixture \
    -Dparity.fixture.user.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.user.state_hash=<signed_i32_low32_hash> \
    test
```

Generate strict low32 detailed-hash fixture (for Zig detailed state-hash parity checks):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest \
    -Dparity.fixture.write=true \
    -Dparity.fixture.path=src/zig/fixtures/java_user_balance_basic_detailed_low32.fixture \
    -Dparity.fixture.user.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.user.state_hash=<signed_i32_low32_hash> \
    test
```

State hash override properties:

- Global kind/value: `parity.fixture.state_hash_kind`, `parity.fixture.state_hash`
- Per-scope kind/value: `parity.fixture.user.state_hash_kind`, `parity.fixture.user.state_hash`,
  `parity.fixture.control.state_hash_kind`, `parity.fixture.control.state_hash`,
  `parity.fixture.trade.state_hash_kind`, `parity.fixture.trade.state_hash`,
  `parity.fixture.trade_extended.state_hash_kind`, `parity.fixture.trade_extended.state_hash`,
  `parity.fixture.trade_bootstrap.state_hash_kind`, `parity.fixture.trade_bootstrap.state_hash`,
  `parity.fixture.trade_bootstrap_long.state_hash_kind`, `parity.fixture.trade_bootstrap_long.state_hash`,
  `parity.fixture.trade_multisymbol.state_hash_kind`, `parity.fixture.trade_multisymbol.state_hash`,
  `parity.fixture.trade_reports.state_hash_kind`, `parity.fixture.trade_reports.state_hash`,
  `parity.fixture.trade_reports_long.state_hash_kind`, `parity.fixture.trade_reports_long.state_hash`

Generate exchange control fixture (suspend/resume + grouping-control + nop + persist + reset parity stream):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeControlFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.control.path=src/zig/fixtures/java_exchange_control.fixture \
    test
```

The control fixture now attempts `ApiPersistState` first (via an isolated
snapshot-enabled probe container) and records the merged persist command result
returned by Java. On Java 25 runtimes where persistence initialization is still
blocked (for example: Chronicle initialization failures such as
`NoSuchFieldException: address`) or the persist command does not complete
within the bounded timeout, the public writer fails closed. An internal
fallback is available only for explicitly non-authoritative diagnostics when
`-Dparity.fixture.diagnostic.allow_internal_fallback=true`; fallback output is
marked and is not accepted as release evidence.

Generate exchange control strict low32 fixtures (risk + detailed):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeControlFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.control.path=src/zig/fixtures/java_exchange_control_low32.fixture \
    -Dparity.fixture.control.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.control.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeControlFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.control.path=src/zig/fixtures/java_exchange_control_detailed_low32.fixture \
    -Dparity.fixture.control.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.control.state_hash=<signed_i32_low32_hash> \
    test
```

Generate full-settlement exchange trade fixture (strictly comparable balances):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.path=src/zig/fixtures/java_exchange_trade_basic_full.fixture \
    -Dparity.fixture.trade.settlement_mode=full \
    test
```

Generate full-settlement exchange trade fixture with strict low32 hash check:

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.path=src/zig/fixtures/java_exchange_trade_basic_full_low32.fixture \
    -Dparity.fixture.trade.settlement_mode=full \
    -Dparity.fixture.trade.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade.state_hash=<signed_i32_low32_hash> \
    test
```

Generate full-settlement exchange trade fixture with strict detailed low32 hash check:

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.path=src/zig/fixtures/java_exchange_trade_basic_full_detailed_low32.fixture \
    -Dparity.fixture.trade.settlement_mode=full \
    -Dparity.fixture.trade.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade.state_hash=<signed_i32_low32_hash> \
    test
```

Generate pre-release exchange trade fixtures with strict low32 checks (naive + direct):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBasicFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.path=src/zig/fixtures/java_exchange_trade_basic_low32.fixture \
    -Dparity.fixture.trade.settlement_mode=pre_release \
    -Dparity.fixture.trade.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBasicFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.path=src/zig/fixtures/java_exchange_trade_basic_detailed_low32.fixture \
    -Dparity.fixture.trade.settlement_mode=pre_release \
    -Dparity.fixture.trade.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBasicFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.path=src/zig/fixtures/java_exchange_trade_basic_direct_low32.fixture \
    -Dparity.fixture.trade.settlement_mode=pre_release \
    -Dparity.fixture.trade.basic.orderbook_mode=direct \
    -Dparity.fixture.trade.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBasicFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.path=src/zig/fixtures/java_exchange_trade_basic_direct_detailed_low32.fixture \
    -Dparity.fixture.trade.settlement_mode=pre_release \
    -Dparity.fixture.trade.basic.orderbook_mode=direct \
    -Dparity.fixture.trade.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade.state_hash=<signed_i32_low32_hash> \
    test
```

Generate direct-orderbook exchange trade fixtures (pre-release and full):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBasicFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.path=src/zig/fixtures/java_exchange_trade_basic_direct.fixture \
    -Dparity.fixture.trade.settlement_mode=pre_release \
    -Dparity.fixture.trade.basic.orderbook_mode=direct \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBasicFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.path=src/zig/fixtures/java_exchange_trade_basic_direct_full.fixture \
    -Dparity.fixture.trade.settlement_mode=full \
    -Dparity.fixture.trade.basic.orderbook_mode=direct \
    test
```

Generate direct-orderbook full-settlement exchange trade fixtures with strict low32 checks:

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBasicFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.path=src/zig/fixtures/java_exchange_trade_basic_direct_full_low32.fixture \
    -Dparity.fixture.trade.settlement_mode=full \
    -Dparity.fixture.trade.basic.orderbook_mode=direct \
    -Dparity.fixture.trade.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBasicFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.path=src/zig/fixtures/java_exchange_trade_basic_direct_full_detailed_low32.fixture \
    -Dparity.fixture.trade.settlement_mode=full \
    -Dparity.fixture.trade.basic.orderbook_mode=direct \
    -Dparity.fixture.trade.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade.state_hash=<signed_i32_low32_hash> \
    test
```

Trade export properties:

- `parity.fixture.trade.symbol_registration`: `direct` (default) or `api`
- `parity.fixture.trade.settlement_mode`: `pre_release` (default for `direct`) or `full`
- `parity.fixture.trade.basic.orderbook_mode`: `naive` (default) or `direct`

Generate extended mixed-flow exchange trade fixture:

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeExtendedFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.extended.path=src/zig/fixtures/java_exchange_trade_extended.fixture \
    test
```

Generate bootstrap checkpoint exchange trade fixture:

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.path=src/zig/fixtures/java_exchange_trade_bootstrap.fixture \
    test
```

Generate bootstrap checkpoint exchange trade fixture with full settlement:

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.path=src/zig/fixtures/java_exchange_trade_bootstrap_full.fixture \
    -Dparity.fixture.trade.bootstrap.settlement_mode=full \
    test
```

Bootstrap trade export properties:

- `parity.fixture.trade.bootstrap.path`
- `parity.fixture.trade.bootstrap.settlement_mode`
- `parity.fixture.trade.bootstrap.orderbook_mode` (`naive` default, or `direct`)

Generate long bootstrap checkpoint exchange trade fixtures (pre-release deterministic large command stream):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.long.path=src/zig/fixtures/java_exchange_trade_bootstrap_long.fixture \
    -Dparity.fixture.trade.bootstrap.long.settlement_mode=pre_release \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.long.path=src/zig/fixtures/java_exchange_trade_bootstrap_long_low32.fixture \
    -Dparity.fixture.trade.bootstrap.long.settlement_mode=pre_release \
    -Dparity.fixture.trade_bootstrap_long.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_bootstrap_long.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.long.path=src/zig/fixtures/java_exchange_trade_bootstrap_long_detailed_low32.fixture \
    -Dparity.fixture.trade.bootstrap.long.settlement_mode=pre_release \
    -Dparity.fixture.trade_bootstrap_long.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_bootstrap_long.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.long.path=src/zig/fixtures/java_exchange_trade_bootstrap_long_direct.fixture \
    -Dparity.fixture.trade.bootstrap.long.settlement_mode=pre_release \
    -Dparity.fixture.trade.bootstrap.long.orderbook_mode=direct \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.long.path=src/zig/fixtures/java_exchange_trade_bootstrap_long_direct_low32.fixture \
    -Dparity.fixture.trade.bootstrap.long.settlement_mode=pre_release \
    -Dparity.fixture.trade.bootstrap.long.orderbook_mode=direct \
    -Dparity.fixture.trade_bootstrap_long.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_bootstrap_long.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.long.path=src/zig/fixtures/java_exchange_trade_bootstrap_long_direct_detailed_low32.fixture \
    -Dparity.fixture.trade.bootstrap.long.settlement_mode=pre_release \
    -Dparity.fixture.trade.bootstrap.long.orderbook_mode=direct \
    -Dparity.fixture.trade_bootstrap_long.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_bootstrap_long.state_hash=<signed_i32_low32_hash> \
    test
```

Generate long bootstrap checkpoint exchange trade fixtures (full settlement, naive + direct):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.long.path=src/zig/fixtures/java_exchange_trade_bootstrap_long_full.fixture \
    -Dparity.fixture.trade.bootstrap.long.settlement_mode=full \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.long.path=src/zig/fixtures/java_exchange_trade_bootstrap_long_full_low32.fixture \
    -Dparity.fixture.trade.bootstrap.long.settlement_mode=full \
    -Dparity.fixture.trade_bootstrap_long.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_bootstrap_long.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.long.path=src/zig/fixtures/java_exchange_trade_bootstrap_long_full_detailed_low32.fixture \
    -Dparity.fixture.trade.bootstrap.long.settlement_mode=full \
    -Dparity.fixture.trade_bootstrap_long.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_bootstrap_long.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.long.path=src/zig/fixtures/java_exchange_trade_bootstrap_long_direct_full.fixture \
    -Dparity.fixture.trade.bootstrap.long.settlement_mode=full \
    -Dparity.fixture.trade.bootstrap.long.orderbook_mode=direct \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.long.path=src/zig/fixtures/java_exchange_trade_bootstrap_long_direct_full_low32.fixture \
    -Dparity.fixture.trade.bootstrap.long.settlement_mode=full \
    -Dparity.fixture.trade.bootstrap.long.orderbook_mode=direct \
    -Dparity.fixture.trade_bootstrap_long.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_bootstrap_long.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.long.path=src/zig/fixtures/java_exchange_trade_bootstrap_long_direct_full_detailed_low32.fixture \
    -Dparity.fixture.trade.bootstrap.long.settlement_mode=full \
    -Dparity.fixture.trade.bootstrap.long.orderbook_mode=direct \
    -Dparity.fixture.trade_bootstrap_long.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_bootstrap_long.state_hash=<signed_i32_low32_hash> \
    test
```

Long bootstrap export properties:

- `parity.fixture.trade.bootstrap.long.path`
- `parity.fixture.trade.bootstrap.long.settlement_mode`: `pre_release` or `full`
- `parity.fixture.trade.bootstrap.long.orderbook_mode`: `naive` (default) or `direct`

Generate bootstrap low32 fixtures (risk + detailed):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.path=src/zig/fixtures/java_exchange_trade_bootstrap_low32.fixture \
    -Dparity.fixture.trade.bootstrap.settlement_mode=pre_release \
    -Dparity.fixture.trade_bootstrap.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_bootstrap.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.path=src/zig/fixtures/java_exchange_trade_bootstrap_detailed_low32.fixture \
    -Dparity.fixture.trade.bootstrap.settlement_mode=pre_release \
    -Dparity.fixture.trade_bootstrap.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_bootstrap.state_hash=<signed_i32_low32_hash> \
    test
```

Generate direct-orderbook bootstrap fixtures (pre-release and full settlement):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.path=src/zig/fixtures/java_exchange_trade_bootstrap_direct.fixture \
    -Dparity.fixture.trade.bootstrap.orderbook_mode=direct \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.path=src/zig/fixtures/java_exchange_trade_bootstrap_direct_full.fixture \
    -Dparity.fixture.trade.bootstrap.orderbook_mode=direct \
    -Dparity.fixture.trade.bootstrap.settlement_mode=full \
    test
```

Generate direct-orderbook bootstrap low32 fixtures (risk + detailed, pre-release and full):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.path=src/zig/fixtures/java_exchange_trade_bootstrap_direct_low32.fixture \
    -Dparity.fixture.trade.bootstrap.orderbook_mode=direct \
    -Dparity.fixture.trade_bootstrap.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_bootstrap.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.path=src/zig/fixtures/java_exchange_trade_bootstrap_direct_detailed_low32.fixture \
    -Dparity.fixture.trade.bootstrap.orderbook_mode=direct \
    -Dparity.fixture.trade_bootstrap.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_bootstrap.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.path=src/zig/fixtures/java_exchange_trade_bootstrap_direct_full_low32.fixture \
    -Dparity.fixture.trade.bootstrap.orderbook_mode=direct \
    -Dparity.fixture.trade.bootstrap.settlement_mode=full \
    -Dparity.fixture.trade_bootstrap.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_bootstrap.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.path=src/zig/fixtures/java_exchange_trade_bootstrap_direct_full_detailed_low32.fixture \
    -Dparity.fixture.trade.bootstrap.orderbook_mode=direct \
    -Dparity.fixture.trade.bootstrap.settlement_mode=full \
    -Dparity.fixture.trade_bootstrap.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_bootstrap.state_hash=<signed_i32_low32_hash> \
    test
```

Generate bootstrap full-settlement low32 fixtures (risk + detailed):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.path=src/zig/fixtures/java_exchange_trade_bootstrap_full_low32.fixture \
    -Dparity.fixture.trade.bootstrap.settlement_mode=full \
    -Dparity.fixture.trade_bootstrap.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_bootstrap.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeBootstrapFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.bootstrap.path=src/zig/fixtures/java_exchange_trade_bootstrap_full_detailed_low32.fixture \
    -Dparity.fixture.trade.bootstrap.settlement_mode=full \
    -Dparity.fixture.trade_bootstrap.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_bootstrap.state_hash=<signed_i32_low32_hash> \
    test
```

Extended trade export properties:

- `parity.fixture.trade.extended.path`
- `parity.fixture.trade.extended.settlement_mode`
- `parity.fixture.trade.extended.orderbook_mode`: `naive` (default) or `direct`

Generate extended mixed-flow exchange trade fixture with full settlement:

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeExtendedFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.extended.path=src/zig/fixtures/java_exchange_trade_extended_full.fixture \
    -Dparity.fixture.trade.extended.settlement_mode=full \
    test
```

Generate direct-orderbook extended mixed-flow exchange trade fixtures (pre-release and full):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeExtendedFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.extended.path=src/zig/fixtures/java_exchange_trade_extended_direct.fixture \
    -Dparity.fixture.trade.extended.settlement_mode=pre_release \
    -Dparity.fixture.trade.extended.orderbook_mode=direct \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeExtendedFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.extended.path=src/zig/fixtures/java_exchange_trade_extended_direct_full.fixture \
    -Dparity.fixture.trade.extended.settlement_mode=full \
    -Dparity.fixture.trade.extended.orderbook_mode=direct \
    test
```

Generate extended mixed-flow pre-release low32 fixtures (risk + detailed):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeExtendedFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.extended.path=src/zig/fixtures/java_exchange_trade_extended_low32.fixture \
    -Dparity.fixture.trade.extended.settlement_mode=pre_release \
    -Dparity.fixture.trade_extended.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_extended.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeExtendedFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.extended.path=src/zig/fixtures/java_exchange_trade_extended_detailed_low32.fixture \
    -Dparity.fixture.trade.extended.settlement_mode=pre_release \
    -Dparity.fixture.trade_extended.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_extended.state_hash=<signed_i32_low32_hash> \
    test
```

Generate extended mixed-flow full-settlement low32 fixtures (risk + detailed):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeExtendedFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.extended.path=src/zig/fixtures/java_exchange_trade_extended_full_low32.fixture \
    -Dparity.fixture.trade.extended.settlement_mode=full \
    -Dparity.fixture.trade_extended.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_extended.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeExtendedFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.extended.path=src/zig/fixtures/java_exchange_trade_extended_full_detailed_low32.fixture \
    -Dparity.fixture.trade.extended.settlement_mode=full \
    -Dparity.fixture.trade_extended.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_extended.state_hash=<signed_i32_low32_hash> \
    test
```

Generate direct-orderbook extended mixed-flow low32 fixtures (risk + detailed, pre-release and full):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeExtendedFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.extended.path=src/zig/fixtures/java_exchange_trade_extended_direct_low32.fixture \
    -Dparity.fixture.trade.extended.settlement_mode=pre_release \
    -Dparity.fixture.trade.extended.orderbook_mode=direct \
    -Dparity.fixture.trade_extended.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_extended.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeExtendedFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.extended.path=src/zig/fixtures/java_exchange_trade_extended_direct_detailed_low32.fixture \
    -Dparity.fixture.trade.extended.settlement_mode=pre_release \
    -Dparity.fixture.trade.extended.orderbook_mode=direct \
    -Dparity.fixture.trade_extended.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_extended.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeExtendedFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.extended.path=src/zig/fixtures/java_exchange_trade_extended_direct_full_low32.fixture \
    -Dparity.fixture.trade.extended.settlement_mode=full \
    -Dparity.fixture.trade.extended.orderbook_mode=direct \
    -Dparity.fixture.trade_extended.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_extended.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeExtendedFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.extended.path=src/zig/fixtures/java_exchange_trade_extended_direct_full_detailed_low32.fixture \
    -Dparity.fixture.trade.extended.settlement_mode=full \
    -Dparity.fixture.trade.extended.orderbook_mode=direct \
    -Dparity.fixture.trade_extended.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_extended.state_hash=<signed_i32_low32_hash> \
    test
```

Generate multi-symbol exchange trade fixtures (focus symbols `241` and `242`):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_241.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=241 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=pre_release \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_242.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=242 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=pre_release \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_full_241.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=241 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=full \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_full_242.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=242 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=full \
    test
```

Generate direct-orderbook multi-symbol fixtures (focus symbols `241` and `242`):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_direct_241.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=241 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=pre_release \
    -Dparity.fixture.trade.multisymbol.orderbook_mode=direct \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_direct_242.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=242 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=pre_release \
    -Dparity.fixture.trade.multisymbol.orderbook_mode=direct \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_direct_full_241.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=241 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=full \
    -Dparity.fixture.trade.multisymbol.orderbook_mode=direct \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_direct_full_242.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=242 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=full \
    -Dparity.fixture.trade.multisymbol.orderbook_mode=direct \
    test
```

Generate multi-symbol low32 fixtures (risk + detailed, focus symbols `241` and `242`):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_241_low32.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=241 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=pre_release \
    -Dparity.fixture.trade_multisymbol.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_multisymbol.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_241_detailed_low32.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=241 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=pre_release \
    -Dparity.fixture.trade_multisymbol.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_multisymbol.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_full_241_low32.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=241 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=full \
    -Dparity.fixture.trade_multisymbol.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_multisymbol.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_full_241_detailed_low32.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=241 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=full \
    -Dparity.fixture.trade_multisymbol.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_multisymbol.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_242_low32.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=242 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=pre_release \
    -Dparity.fixture.trade_multisymbol.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_multisymbol.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_242_detailed_low32.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=242 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=pre_release \
    -Dparity.fixture.trade_multisymbol.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_multisymbol.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_full_242_low32.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=242 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=full \
    -Dparity.fixture.trade_multisymbol.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_multisymbol.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_full_242_detailed_low32.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=242 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=full \
    -Dparity.fixture.trade_multisymbol.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_multisymbol.state_hash=<signed_i32_low32_hash> \
    test
```

Generate direct-orderbook multi-symbol low32 fixtures (risk + detailed, focus symbols `241` and `242`):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_direct_241_low32.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=241 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=pre_release \
    -Dparity.fixture.trade.multisymbol.orderbook_mode=direct \
    -Dparity.fixture.trade_multisymbol.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_multisymbol.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_direct_241_detailed_low32.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=241 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=pre_release \
    -Dparity.fixture.trade.multisymbol.orderbook_mode=direct \
    -Dparity.fixture.trade_multisymbol.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_multisymbol.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_direct_242_low32.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=242 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=pre_release \
    -Dparity.fixture.trade.multisymbol.orderbook_mode=direct \
    -Dparity.fixture.trade_multisymbol.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_multisymbol.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_direct_242_detailed_low32.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=242 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=pre_release \
    -Dparity.fixture.trade.multisymbol.orderbook_mode=direct \
    -Dparity.fixture.trade_multisymbol.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_multisymbol.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_direct_full_241_low32.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=241 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=full \
    -Dparity.fixture.trade.multisymbol.orderbook_mode=direct \
    -Dparity.fixture.trade_multisymbol.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_multisymbol.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_direct_full_241_detailed_low32.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=241 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=full \
    -Dparity.fixture.trade.multisymbol.orderbook_mode=direct \
    -Dparity.fixture.trade_multisymbol.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_multisymbol.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_direct_full_242_low32.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=242 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=full \
    -Dparity.fixture.trade.multisymbol.orderbook_mode=direct \
    -Dparity.fixture.trade_multisymbol.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_multisymbol.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeMultiSymbolFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.multisymbol.path=src/zig/fixtures/java_exchange_trade_multisymbol_direct_full_242_detailed_low32.fixture \
    -Dparity.fixture.trade.multisymbol.focus_symbol=242 \
    -Dparity.fixture.trade.multisymbol.settlement_mode=full \
    -Dparity.fixture.trade.multisymbol.orderbook_mode=direct \
    -Dparity.fixture.trade_multisymbol.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_multisymbol.state_hash=<signed_i32_low32_hash> \
    test
```

Generate exchange trade reports fixture (single-user + total-currency-balance parity fields):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.path=src/zig/fixtures/java_exchange_trade_reports.fixture \
    -Dparity.fixture.trade.reports.settlement_mode=full \
    test
```

Reports export properties:

- `parity.fixture.trade.reports.path`
- `parity.fixture.trade.reports.settlement_mode`
- `parity.fixture.trade.reports.orderbook_mode`: `naive` (default) or `direct`

Generate direct-orderbook exchange trade reports fixture:

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.path=src/zig/fixtures/java_exchange_trade_reports_direct.fixture \
    -Dparity.fixture.trade.reports.settlement_mode=full \
    -Dparity.fixture.trade.reports.orderbook_mode=direct \
    test
```

Generate exchange trade reports low32 fixtures (risk + detailed, naive + direct):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.path=src/zig/fixtures/java_exchange_trade_reports_low32.fixture \
    -Dparity.fixture.trade.reports.settlement_mode=full \
    -Dparity.fixture.trade_reports.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_reports.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.path=src/zig/fixtures/java_exchange_trade_reports_detailed_low32.fixture \
    -Dparity.fixture.trade.reports.settlement_mode=full \
    -Dparity.fixture.trade_reports.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_reports.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.path=src/zig/fixtures/java_exchange_trade_reports_direct_low32.fixture \
    -Dparity.fixture.trade.reports.settlement_mode=full \
    -Dparity.fixture.trade.reports.orderbook_mode=direct \
    -Dparity.fixture.trade_reports.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_reports.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.path=src/zig/fixtures/java_exchange_trade_reports_direct_detailed_low32.fixture \
    -Dparity.fixture.trade.reports.settlement_mode=full \
    -Dparity.fixture.trade.reports.orderbook_mode=direct \
    -Dparity.fixture.trade_reports.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_reports.state_hash=<signed_i32_low32_hash> \
    test
```

Generate long-sequence exchange trade reports fixtures (pre-release deterministic large command stream):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.long.path=src/zig/fixtures/java_exchange_trade_reports_long.fixture \
    -Dparity.fixture.trade.reports.long.settlement_mode=pre_release \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.long.path=src/zig/fixtures/java_exchange_trade_reports_long_low32.fixture \
    -Dparity.fixture.trade.reports.long.settlement_mode=pre_release \
    -Dparity.fixture.trade_reports_long.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_reports_long.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.long.path=src/zig/fixtures/java_exchange_trade_reports_long_detailed_low32.fixture \
    -Dparity.fixture.trade.reports.long.settlement_mode=pre_release \
    -Dparity.fixture.trade_reports_long.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_reports_long.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.long.path=src/zig/fixtures/java_exchange_trade_reports_long_direct.fixture \
    -Dparity.fixture.trade.reports.long.settlement_mode=pre_release \
    -Dparity.fixture.trade.reports.long.orderbook_mode=direct \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.long.path=src/zig/fixtures/java_exchange_trade_reports_long_direct_low32.fixture \
    -Dparity.fixture.trade.reports.long.settlement_mode=pre_release \
    -Dparity.fixture.trade.reports.long.orderbook_mode=direct \
    -Dparity.fixture.trade_reports_long.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_reports_long.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.long.path=src/zig/fixtures/java_exchange_trade_reports_long_direct_detailed_low32.fixture \
    -Dparity.fixture.trade.reports.long.settlement_mode=pre_release \
    -Dparity.fixture.trade.reports.long.orderbook_mode=direct \
    -Dparity.fixture.trade_reports_long.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_reports_long.state_hash=<signed_i32_low32_hash> \
    test
```

Generate long-sequence exchange trade reports fixtures (full settlement, naive + direct):

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.long.path=src/zig/fixtures/java_exchange_trade_reports_long_full.fixture \
    -Dparity.fixture.trade.reports.long.settlement_mode=full \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.long.path=src/zig/fixtures/java_exchange_trade_reports_long_full_low32.fixture \
    -Dparity.fixture.trade.reports.long.settlement_mode=full \
    -Dparity.fixture.trade_reports_long.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_reports_long.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.long.path=src/zig/fixtures/java_exchange_trade_reports_long_full_detailed_low32.fixture \
    -Dparity.fixture.trade.reports.long.settlement_mode=full \
    -Dparity.fixture.trade_reports_long.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_reports_long.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.long.path=src/zig/fixtures/java_exchange_trade_reports_long_direct_full.fixture \
    -Dparity.fixture.trade.reports.long.settlement_mode=full \
    -Dparity.fixture.trade.reports.long.orderbook_mode=direct \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.long.path=src/zig/fixtures/java_exchange_trade_reports_long_direct_full_low32.fixture \
    -Dparity.fixture.trade.reports.long.settlement_mode=full \
    -Dparity.fixture.trade.reports.long.orderbook_mode=direct \
    -Dparity.fixture.trade_reports_long.state_hash_kind=zig_risk_low32 \
    -Dparity.fixture.trade_reports_long.state_hash=<signed_i32_low32_hash> \
    test

mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest#writeExchangeTradeReportsLongFixture \
    -Dparity.fixture.write=true \
    -Dparity.fixture.trade.reports.long.path=src/zig/fixtures/java_exchange_trade_reports_long_direct_full_detailed_low32.fixture \
    -Dparity.fixture.trade.reports.long.settlement_mode=full \
    -Dparity.fixture.trade.reports.long.orderbook_mode=direct \
    -Dparity.fixture.trade_reports_long.state_hash_kind=zig_detailed_low32 \
    -Dparity.fixture.trade_reports_long.state_hash=<signed_i32_low32_hash> \
    test
```

Long reports export properties:

- `parity.fixture.trade.reports.long.path`
- `parity.fixture.trade.reports.long.settlement_mode`: `pre_release` or `full`
- `parity.fixture.trade.reports.long.orderbook_mode`: `naive` (default) or `direct`

Reports fixtures query Java reports via
`SingleUserReportQuery`/`TotalCurrencyBalanceReportQuery` to export canonical
report fields. Public report writers fail closed if that Java path is
unavailable. The internal deterministic fallback is diagnostic-only, requires
`-Dparity.fixture.diagnostic.allow_internal_fallback=true`, and marks its output
as non-authoritative.

## Run Zig diff

```sh
zig build parity-diff
```

With no explicit paths, `parity-diff` enforces the exact committed 96-file
manifest: missing and unexpected `.fixture` files both fail. The aggregate
`zig build gate` always runs that default manifest and cannot be narrowed by
arguments supplied to another build step. Explicit paths remain available only
for focused standalone `parity-diff` diagnostics.
