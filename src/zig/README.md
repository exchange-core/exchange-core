# exchange-core Zig port

This directory contains the incremental Java -> Zig migration for `exchange-core`.

## Usage guide

- See `src/zig/USAGE_EXAMPLES.md` for copy-paste integration, persistence, orchestrator, and migration workflow examples.

## Current scope

- Build/test scaffold via `build.zig`.
- Ported baseline `core/common` and `core/cmd` models:
  - symbol/order enums
  - `CoreSymbolSpecification`
  - `OrderCommandType` and `CommandResultCode`
  - `OrderCommand` constructors and copy behavior
  - `core/config.zig` typed configuration model for orders-processing, matching, runtime, initial-state, reports/logging, serialization, profile conversion, and initial-state/serialization cross-validation
  - Java-shaped performance/config adapters in `core/config.zig`:
    - `CoreWaitStrategy` enum parity surface
    - `PerformanceConfig` presets (`base`, `latency`, `throughput`) with explicit validation and single-kernel adapter fallback for multi-engine Java presets
    - `JavaExchangeConfig` composition adapter (`orders_processing_cfg`, `performance_cfg`, `init_state_cfg`, `reports_queries_cfg`, `logging_cfg`, `serialization_cfg`) into existing `ExchangeCoreConfig`
- Ported first matching engines:
  - `core/orderbook_naive.zig` with command flow + L2 snapshots + invariants
  - `core/orderbook_direct.zig` with linked-chain buckets + order index + parity tests against naive
  - kernel-level parity gate validates naive/direct equivalence over a shared command stream
  - multi-shard UID-routed parity harness validates deterministic cross-shard user-state invariants against single-shard reference
  - split matching/risk shard simulation harness validates cross-shard trade parity (symbol-routed matching + uid-routed risk accounting)
  - production sharded core baseline (`core/exchange_core_sharded.zig`) for routed/split execution across matching shards
  - seeded fault-injection deterministic simulation harness validates split matching/risk parity under drop/duplicate/reorder delivery perturbations
  - multi-symbol seeded fault-injection split simulation harness validates routed symbol parity across distinct matching shards
- Ported user/risk foundation:
  - `core/user_profile.zig` for user accounts, positions, and suspend/resume logic
  - `core/events.zig` for matcher event chain and market-data update contracts
  - `core/risk.zig` for:
    - RiskEngine R1 preprocessor subset (user mgmt, balance adjust, exchange order hold checks)
    - Risk release accounting (`handlerRiskRelease`) for exchange and margin event flows
- Added kernel/API pipeline:
  - `core/exchange_kernel.zig` for risk -> matching -> risk-release execution flow
  - `core/exchange_runtime.zig` for deterministic grouped command draining and result-sink publication
  - runtime-group metadata stamping for drained commands (`events_group` boundary progression + deterministic service-flags cadence: Java-style time pulse via `runtime.l2_publish_interval_ns` (default `10_000_000ns`, strict `>` threshold), optional periodic bit-0 via `runtime.l2_flags_every_n_commands`, plus optional timestamp-gap boundary via `runtime.max_group_duration_ns`)
  - Java-parity result-sink gating: `GROUPING_CONTROL` toggles callback emission (`mode=0` suppresses, `mode=1` re-enables and emits the control command itself)
  - `core/simple_events_processor.zig` adapter for Java-style event callbacks (`command_result`, `trade`, `reject`, `reduce`, `order_book`) from command responses and matcher event chains, including Java-parity command-result allow-list behavior (control commands and `binary_data_command` with `accepted` are intentionally suppressed), now wireable in live flows via `setSimpleEventsHandler`/`clearSimpleEventsHandler` on `exchange_core` and `exchange_api` (sync, async, and runtime-drain paths publish matcher-event-backed trade/reject/reduce callbacks when available)
  - `core/exchange_core.zig` orchestration wrapper that owns kernel/runtime/api wiring (including `initFromJavaConfig` adapter for `config.JavaExchangeConfig`)
  - `core/exchange_api_sharded.zig` sharded API facade with persistence/startup compatibility wrappers and routed/split command execution modes
  - `core/exchange_orchestrator.zig` production orchestrator facade that selects single vs sharded backend from Java config
  - seeded startup/bootstrap/replay perturbation simulations for sharded persistence recovery (baseline replay, missing-partition, corrupted-sequence, strict malformed-main-log)
  - `core/exchange_api.zig` facade exposing Java-style API entrypoints over `exchange_core` (`submitCommand`, async handles, list sync, and nop-barrier stream sync helpers), callback-style convenience wrappers that register non-blocking callbacks (`createUserWithCallback`, `suspendUserWithCallback`, `resumeUserWithCallback`, `balanceAdjustmentWithCallback`, `orderBookRequest`, `placeNewOrderWithCallback`, `moveOrderWithCallback`, `cancelOrderWithCallback`, `reduceOrderWithCallback`) and explicit callback-processing helpers (`registerResultCallback`, `processResult`, `processRegisteredCallback`, `processPendingCallbacks`) with explicit exclusion of `.persist_state` handles from callback registration, raw binary async wrappers (`submitBinaryDataPayloadAsync`, `submitBinaryQueryPayloadAsync`, Java-style aliases `submitBinaryDataAsync`, `submitBinaryCommandAsync`, `submitQueryAsync`, `publishBinaryData`, `publishQuery`), binary ingestion wrappers (`submitAddSymbolsBinary`, `submitAddSymbolsBinaryAsync`, `submitAddAccountsBinary`, `submitAddAccountsBinaryAsync`), explicit async ergonomics (`submitCommandAsyncResultCode`, `submitCommandAsyncFullResponse`, `processCommandResultCode`, `processCommandFullResponse`, `takeResultCode`, `takeFullResponse`, core-compat aliases `submitApiCommand`/`submitApiCommandAsync`/`submitApiCommandsSync`/`submitApiCommandsSyncWithNop`/`isApiCommandAsyncResultReady`/`takeApiCommandAsyncResult`, request-id async aliases `isAsyncResultReady`/`takeAsyncResult`/`drainAsyncPending`, direct persist-state pair wrappers `persistState`/`persistStateAsync`/`isPersistStateResultReady`/`takePersistStateResult`, low-level command wrappers `addUser`/`addUserAsync`/`adjustUserBalance`/`adjustUserBalanceAsync`/`placeOrder`/`placeOrderAsync`/`suspendUserAsync`/`resumeUserAsync`/`moveOrderAsync`/`cancelOrderAsync`/`reduceOrderAsync`/`resetAsync`/`groupingControlAsync`/`nopAsync`, low-level binary wrappers `submitBinaryPayload`/`submitBinaryPayloadAsync`/`submitBinaryReportQuery`, batch wrapper `submitCommandBatch`, runtime sink wrapper `setResultSink`, typed persist-state helpers `submitPersistStateAsync`/`processPersistStateResultCode`, `submitBinaryReportQueryAsync`, `takeReportResponse`, typed report helpers `requestStateHashAsync`/`takeStateHashDetailed`, `requestSingleUserReportAsync`/`takeSingleUserReport`, `requestTotalCurrencyBalanceAsync`/`takeTotalCurrencyBalanceReport`, one-shot report helpers `processReport`, `processStateHashReport`, `processSingleUserReport`, `processTotalCurrencyBalanceReport`, direct kernel report helpers `queryReport`/`queryRiskStateHash`/`queryFeeCollected`/`queryUserBalance`/`queryOrderBook`, status/queue observability wrappers `isStarted`/`isStopped`/`journalingEnabled`/`journalLen`/`asyncPendingCount`/`asyncReadyCount`/`runtimePendingCount`, runtime queue wrappers `enqueueRuntime`/`drainRuntime`, persistence descriptor lookup helpers `persistenceFindSnapshotById`/`persistenceSnapshotById`/`persistenceCurrentSnapshotRef`, persistence observability helpers `persistenceSnapshotCount`/`persistenceJournalCount`/`persistenceCurrentSnapshot`/`persistenceJournalByRef`/`persistenceFindJournalForSequence`, default persistence builder wrappers `defaultPersistenceSnapshotFileName`/`defaultPersistenceJournalFileName`/`defaultPersistenceMainLogFileName`/`defaultPersistenceSnapshotPath`/`defaultPersistenceJournalPath`/`defaultPersistenceMainLogPath`, snapshot probe helpers `snapshotExistsDefaultPath`/`snapshotExistsFile`, default-path persistence I/O wrappers `writeJournalToDefaultPath`/`loadJournalFromDefaultPath`/`replayJournalFromDefaultPath`/`replayJournalFromDefaultPathRange`/`captureSnapshotToDefaultPath`/`validateSnapshotDefaultPath`, explicit file-path persistence wrappers `writeJournalToFile`/`loadJournalFromFile`/`replayJournalFromFile`/`captureSnapshotToFile`/`validateSnapshotFile`, lifecycle/bootstrap wrappers `deinit`/`initFromJavaConfig`/`startup`/`shutdown`/`bootstrapFromPersistence`/`startupFromPersistence`/`startupFromConfiguredPersistence`, discovered persistence startup helpers `startupFromDiscoveredConfiguredPersistence`/`startupFromDiscoveredConfiguredPersistenceStrictMainLog`, and typed order-book helpers `requestOrderBook`/`requestOrderBookAsync`/`takeOrderBookSnapshot`/`processOrderBookRequest`), plus metadata-aware replay-style helpers (`createUser`, `suspendUser`, `resumeUser`, `balanceAdjustment`, `placeNewOrder`, `moveOrder`, `cancelOrder`, `reduceOrder`, `binaryData`, `groupingControl`, `reset`, `nop`) and explicit persist-state `.eca` wrapper (`persistStateWithDefaultMainLog`)
  - Java-vs-Zig `ExchangeApi` method-surface inventory artifacts generated at:
    - `src/zig/EXCHANGE_API_SURFACE_DIFF.json` (machine-readable)
    - `src/zig/EXCHANGE_API_SURFACE_DIFF.md` (human-readable summary)
  - Java-vs-Zig `ExchangeApi` class-by-class parity audit artifacts generated at:
    - `src/zig/EXCHANGE_API_PARITY_AUDIT.json` (machine-readable family/signature coverage)
    - `src/zig/EXCHANGE_API_PARITY_AUDIT.md` (human-readable overload-family audit)
  - Java processor-ecosystem parity decomposition artifact:
    - `src/zig/PROCESSOR_PARITY_DECOMPOSITION.md` (Java processor class ownership mapping to Zig modules + explicit consolidation rationale)
  - persistence interoperability decision artifact:
    - `src/zig/PERSISTENCE_INTEROP_DECISION.md` (native Zig persistence policy; explicit unsupported legacy LZ4-format detection in loaders)
  - config-driven journaling in `exchange_core` for sync, async-drain, and runtime-drain command paths (recording, journal length, file write/load APIs)
  - initial-state bootstrap helper can replay journal files up to `initial_state.journal_timestamp_ns`, and when snapshot bootstrap is enabled it replays only entries with `sequence > initial_state.snapshot_base_seq`
  - snapshot bootstrap policy restores full kernel state from snapshot files (symbols/books/users/risk maps), honors `throw_if_snapshot_not_found`, and initializes sequence from `initial_state.snapshot_base_seq`
  - snapshot file APIs on `exchange_core`: capture current kernel snapshot to disk and validate a snapshot file against live kernel state
  - lifecycle helpers on `exchange_core`: `startup`, `startupFromPersistence`, `shutdown`, and started/stopped state flags
  - deterministic sequence IDs on command responses
  - typed ExchangeApi sync/async helpers for add-user, balance adjust, place/move/cancel/reduce, order-book request, reset, and nop flows
  - typed `ApiCommand` dispatch layer on `exchange_core` with generic sync/async submission (`submitApiCommand`, `submitApiCommandAsync`) across command, order-book, persist-state, and typed binary paths (with typed result/handle variants)
  - `ApiCommand.raw_command` low-level pass-through for Java-parity dispatch of fully-populated `OrderCommand` payloads (preserving `timestamp`, `events_group`, `service_flags`, and other metadata fields)
  - batch ApiCommand helper `submitApiCommandsSync` that enqueues async handles, drains once, deinitializes intermediate response payloads, and returns the final typed result
  - batch command submission + merged result semantics
  - async API queue (`submitCommandAsync`, `drainPending`, `takeResult`) with FIFO execution and request/result routing
  - typed report queries (`risk_state_hash`, `state_hash_detailed`, `fee_collected`, `single_user`, `total_currency_balance`, `user_balance`, `order_book`)
  - matching-side L2 signal parity for margin valuation: risk last-price cache can refresh from successful matching commands via `service_flags` bit-0 or `matching.send_l2_for_every_cmd` (with `matching.l2_refresh_depth`)
  - report-query capability gating in kernel config for `state_hash`, `single_user`, and `total_currency_balance` paths (applies to direct and binary-query flows)
  - binary-command capability gating in kernel config for `ADD_SYMBOLS` and `ADD_ACCOUNTS` payload execution paths
  - `total_currency_balance` aggregates risk-side balances (accounts, fees, adjustments, suspends, open interest) plus matching-engine resting order holds
  - binary frame assembler (`core/binary_frames.zig`) for transfer-id keyed `BINARY_DATA_*` fragment accumulation
  - kernel-level `BINARY_DATA_*` fragment result semantics (`accepted` until final fragment)
  - kernel-level `BINARY_DATA_QUERY` payload execution for supported report query codes (`STATE_HASH`, `SINGLE_USER_REPORT`, `TOTAL_CURRENCY_BALANCE`) with typed report response attachment (`STATE_HASH` -> `state_hash_detailed`)
  - binary command ingestion helpers (`core/binary_commands.zig`) for batch symbol/account application
  - binary payload codec helpers (`core/binary_payload.zig`) for Java-format `ADD_SYMBOLS`/`ADD_ACCOUNTS` and report query type-code byte/word streams (encode + decode)
  - typed exchange-core binary wrappers for sync/async framed submission:
    - `submitAddSymbolsBinary` / `submitAddSymbolsBinaryAsync`
    - `submitAddAccountsBinary` / `submitAddAccountsBinaryAsync`
    - `submitBinaryReportQuery` / `submitBinaryReportQueryAsync`
  - kernel-level binary payload execution for `ADD_SYMBOLS`/`ADD_ACCOUNTS` on final `BINARY_DATA_COMMAND` frame (`success` on apply, `binary_command_failed` on decode/apply error)
  - matcher event wiring from both orderbook implementations into risk-release
- Added deterministic state hashing:
  - `UserProfileService.stateHash` and `RiskEngine.stateHash`
  - deterministic orderbook state hashing in both `.naive` and `.direct` implementations
  - detailed kernel state-hash report with per-submodule entries plus aggregate hash
  - sorted-key hashing for map-backed state to avoid insertion-order drift
- Added journaling foundation:
  - `core/journal.zig` in-memory command journal
  - `core/persistence_paths.zig` Java-style persistence filename/path helpers (`.ecs`/`.ecj`/`.eca`) with module-code parity (`RE`/`ME`) and zero-padded uppercase journal partitions
  - `exchange_core` default-path persistence wrappers (`write/load/replay journal`, `capture/validate snapshot`, `startupFromConfiguredPersistence`, `startupFromDiscoveredConfiguredPersistence`, strict diagnostic `startupFromDiscoveredConfiguredPersistenceStrictMainLog`) built on configured exchange-id + storage-folder layout, with automatic storage-folder creation for default-path journal/snapshot writes, sequential journal-partition replay during configured startup, Java-style tolerance for a missing first configured journal partition, auto-discovery of latest snapshot-id for module+instance startup flows, best-effort base-sequence derivation from `.eca` metadata (fallback to configured `snapshot_base_seq`), strict malformed-entry detection mode for `.eca` diagnostics, and `.eca` main-log append entries on default snapshot capture
  - explicit persist-state `.eca` helper (`persistStateWithDefaultMainLog`) that appends a main-log line for successful persist transitions using a caller-provided directory
  - lightweight snapshot existence probes on `exchange_core` (`snapshotExistsFile`, `snapshotExistsDefaultPath`) for bootstrap diagnostics without loading snapshot payloads
  - `core/persistence_descriptors.zig` snapshot/journal descriptor graph (Java `SnapshotDescriptor`/`JournalDescriptor` parity model with deterministic journal ordering)
  - `exchange_core` descriptor integration for sync `persistState`, bootstrap snapshot replay, and journal file write/load/replay flows (`persistenceSnapshotCount`, `persistenceJournalCount`, `persistenceCurrentSnapshot`, `persistenceFindSnapshotById`, `persistenceSnapshotById`, `persistenceFindJournalForSequence`)
  - deterministic replay verifier (sequence + result parity) against fresh kernel state
  - large seeded journal+snapshot+report-query simulation gate for deterministic replay/state convergence with periodic snapshot-restore checkpoints
  - multi-symbol seeded journal+snapshot+report-query simulation gate for cross-symbol replay/state convergence with periodic snapshot-restore checkpoints
  - exchange-core seeded persistence checkpoint gate validating `startupFromPersistence` snapshot+journal suffix replay convergence against live state
  - exchange-core seeded sync-vs-async typed binary ingestion simulation gate validating state convergence (`risk_state_hash`, `state_hash_detailed`, and sampled user balances)
  - `core/snapshot.zig` snapshot capture/save/load/restore for full kernel state (symbols/resting orders/users/risk maps) plus per-symbol L2 validation views
- Added Tiger Style enforcement checklist:
  - `TIGER_STYLE_CHECKLIST.md`
- Added Java parity-fixture scaffolding:
  - fixture format docs at `src/zig/fixtures/README.md`
  - Java exporter test scaffold at `src/test/java/exchange/core2/tests/fixtures/GenerateJavaParityFixtureTest.java`

## Migration rules

- Keep behavioral parity with Java first, then optimize.
- Preserve code mappings (enum/integer values) for wire compatibility.
- Add parity tests for every ported module before integrating it.
- Use explicit Zig error sets for decode/validation failures.
- Use exhaustive `switch` statements for command/event handling.
- Apply Tiger Style discipline for:
  - aggressive invariant checking (`std.debug.assert`) at module boundaries,
  - explicit control flow and no hidden side effects,
  - predictable memory ownership (`init`/`deinit`, no global allocators),
  - small, testable functions with bounded loops,
  - deterministic seeded perturbation simulations for async callback/poll parity and exactly-once delivery behavior,
  - deterministic one-shot helper simulations that cross-check `processCommandResultCode`/`processCommandFullResponse` against explicit async submit+drain+take flows with per-step detailed state-hash convergence.

## Build and test

```sh
ZIG_GLOBAL_CACHE_DIR=.zig-cache-global \
ZIG_LOCAL_CACHE_DIR=.zig-cache-local \
zig build test
```

ReleaseSafe hardening gate:

```sh
ZIG_GLOBAL_CACHE_DIR=.zig-cache-global \
ZIG_LOCAL_CACHE_DIR=.zig-cache-local \
zig build test-release-safe
```

API surface parity check (ExchangeCore -> ExchangeApi public methods):

```sh
ZIG_GLOBAL_CACHE_DIR=.zig-cache-global \
ZIG_LOCAL_CACHE_DIR=.zig-cache-local \
zig build api-surface-check
```

Java-vs-Zig ExchangeApi surface inventory artifact generation:

```sh
ZIG_GLOBAL_CACHE_DIR=.zig-cache-global \
ZIG_LOCAL_CACHE_DIR=.zig-cache-local \
zig build java-api-surface-diff
```

Java-vs-Zig ExchangeApi class-by-class parity audit artifact generation:

```sh
ZIG_GLOBAL_CACHE_DIR=.zig-cache-global \
ZIG_LOCAL_CACHE_DIR=.zig-cache-local \
zig build java-api-parity-audit
```

Full migration gate (tests + ReleaseSafe + parity diff + API surface parity):

```sh
ZIG_GLOBAL_CACHE_DIR=.zig-cache-global \
ZIG_LOCAL_CACHE_DIR=.zig-cache-local \
zig build gate
```

## Benchmark

```sh
ZIG_GLOBAL_CACHE_DIR=.zig-cache-global \
ZIG_LOCAL_CACHE_DIR=.zig-cache-local \
zig build bench -- 50000
```

The optional numeric arg is the benchmark round count (defaults to `50000`).

## Java Fixture Diff

```sh
ZIG_GLOBAL_CACHE_DIR=.zig-cache-global \
ZIG_LOCAL_CACHE_DIR=.zig-cache-local \
zig build parity-diff
```

You can also pass explicit fixture file paths:

```sh
zig build parity-diff -- src/zig/fixtures/java_exchange_trade_basic.fixture
```

Default parity-diff coverage includes:

- user-balance baseline + strict low32 risk/detailed fixtures.
- exchange-control stream:
  - suspend/resume transitions, duplicate-edge outcomes, grouping-control success path, and reset-state transition,
  - nop/control-flow and persist-merged-result parity with deterministic balance snapshots,
  - strict low32 risk/detailed state-hash fixtures.
- exchange-trade basic stream:
  - pre-release fixture (`java_exchange_trade_basic.fixture`) with settlement-sensitive normalization + strict low32 risk/detailed fixtures,
  - full-settlement fixture + strict low32 risk/detailed fixtures,
  - mirrored direct-orderbook fixtures for pre-release/full with strict low32 risk/detailed checks in both settlement modes.
- exchange-trade extended stream:
  - pre-release fixture + strict low32 risk/detailed fixtures,
  - full-settlement fixture + strict low32 risk/detailed fixtures,
  - mirrored direct-orderbook fixtures for pre-release/full and strict low32 risk/detailed checks in both settlement modes.
- exchange-trade bootstrap stream:
  - pre-release fixture (`java_exchange_trade_bootstrap.fixture`) executed through Zig snapshot+journal checkpoint restore path,
  - full-settlement fixture (`java_exchange_trade_bootstrap_full.fixture`) for strict settlement-sensitive balances,
  - strict low32 risk/detailed fixtures in both pre-release and full-settlement modes,
  - long pre-release fixture (`java_exchange_trade_bootstrap_long.fixture`) for larger deterministic checkpoint-replay coverage with strict low32 risk/detailed companions,
  - long full-settlement fixture set (`java_exchange_trade_bootstrap_long_full*`) for strict settlement-mode coverage on the checkpoint-replay path,
  - mirrored direct long bootstrap fixtures (`java_exchange_trade_bootstrap_long_direct*`) for pre-release with strict low32 risk/detailed checks,
  - mirrored direct long bootstrap full-settlement fixtures (`java_exchange_trade_bootstrap_long_direct_full*`) with strict low32 risk/detailed checks,
  - mirrored direct-orderbook fixture set (`*_bootstrap_direct*`) for naive/direct persistence-path parity coverage.
- exchange-trade multi-symbol stream (symbols `241` and `242` for orderbook focus):
  - pre-release fixtures for both focus symbols,
  - full-settlement fixtures for both focus symbols,
  - mirrored direct-orderbook fixtures for both focus symbols in pre-release and full-settlement modes,
  - strict low32 risk/detailed fixtures for both focus symbols in both pre-release and full-settlement modes, including mirrored direct-orderbook low32 variants.
- exchange-trade reports stream:
  - full-settlement fixture (`java_exchange_trade_reports.fixture`) with strict comparison of
    single-user and total-currency-balance report fields,
  - mirrored direct-orderbook reports fixture (`java_exchange_trade_reports_direct.fixture`),
  - strict low32 risk/detailed fixtures for naive and direct report streams,
  - long-sequence pre-release reports fixture (`java_exchange_trade_reports_long.fixture`) for larger deterministic orderbook/report query coverage with strict low32 risk/detailed companions,
  - mirrored direct-orderbook long-sequence pre-release reports fixtures (`java_exchange_trade_reports_long_direct*`) with strict low32 risk/detailed companions,
  - long-sequence full-settlement reports fixtures (`java_exchange_trade_reports_long_full*`) for strict long-stream report parity coverage,
  - mirrored direct-orderbook long-sequence full-settlement reports fixtures (`java_exchange_trade_reports_long_direct_full*`) with strict low32 risk/detailed companions.

Current migration-gate status is tracked in `src/zig/PARITY_BENCHMARK_REPORT.md`.

For this repository and toolchain, Zig `0.15.2` is the baseline.
