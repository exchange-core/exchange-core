# Java production class → Zig responsibility map

Updated: 2026-07-31

Scope: every one of the 91 production `.java` files under
`src/main/java/exchange/core2/core`.

This is the production-source companion to `MIGRATION_COVERAGE.md`. Each row
uses the exact Java path relative to `src/main/java/exchange/core2/core` and
repository-relative Zig owner paths that the checker verifies exist. `Closed`
means the Java source's externally observable responsibility is owned by those
modules and participates in the verification gates. `Intentional native
boundary` means a JVM representation or mechanism is replaced at the stated
Zig boundary. `Intentional capability boundary` means interoperability is
deliberately provided through the evidence named in the row. Nested Java DTOs
remain part of their enclosing source's responsibility; rows do not assert a
class-for-class Zig structure.

Current status totals: 82 `Closed`, 8 `Intentional native boundary`, and 1
`Intentional capability boundary` across 91 exact Java paths.

## Core facades and events

| Java production source | Zig owners | Status | Responsibility evidence |
| --- | --- | --- | --- |
| `ExchangeApi.java` | `src/zig/core/exchange_api.zig`, `src/zig/core/exchange_api_sharded.zig`, `src/zig/core/exchange_orchestrator.zig`, `src/zig/core/api_command.zig` | Closed | synchronous, asynchronous, batched, callback, binary, report, persistence, timestamp, and result-ownership entry points. |
| `ExchangeCore.java` | `src/zig/core/exchange_core.zig`, `src/zig/core/exchange_api_sharded.zig`, `src/zig/core/exchange_runtime.zig`, `src/zig/core/sharded_pipeline.zig` | Closed | lifecycle, admission, grouping, journaling, processor orchestration, shutdown, and fatal settlement. |
| `IEventsHandler.java` | `src/zig/core/simple_events_processor.zig`, `src/zig/core/events.zig` | Closed | command, trade, reject, reduce, and order-book callback contracts with borrowed callback lifetime. |
| `SimpleEventsProcessor.java` | `src/zig/core/simple_events_processor.zig` | Closed | matcher-event translation, callback ordering, ownership, error propagation, and Java-compatible ignoring of non-simple matcher-event kinds. |

## Common domain model

| Java production source | Zig owners | Status | Responsibility evidence |
| --- | --- | --- | --- |
| `common/BalanceAdjustmentType.java` | `src/zig/core/common.zig` | Closed | adjustment and suspend adjustment discriminants. |
| `common/CoreSymbolSpecification.java` | `src/zig/core/common.zig`, `src/zig/interop/java_persistence/snapshot_body.zig` | Closed | symbol economics, validation, hashing, and Java persistence decoding. |
| `common/CoreWaitStrategy.java` | `src/zig/core/config.zig`, `src/zig/core/runtime_queue.zig`, `src/zig/core/sharded_pipeline.zig` | Closed | public strategy selection and equivalent blocking/yield/spin runtime behavior. |
| `common/IOrder.java` | `src/zig/core/common.zig`, `src/zig/core/orderbook_naive.zig`, `src/zig/core/orderbook_direct.zig` | Closed | immutable order view contract used by reports, hashing, and snapshots. |
| `common/L2MarketData.java` | `src/zig/core/events.zig`, `src/zig/core/orderbook_naive.zig`, `src/zig/core/orderbook_direct.zig` | Closed | bounded/full asks and bids, totals, timestamps, and ownership. |
| `common/MatcherEventType.java` | `src/zig/core/events.zig` | Closed | trade, reject, and reduce event kinds. |
| `common/MatcherTradeEvent.java` | `src/zig/core/events.zig`, `src/zig/core/simple_events_processor.zig` | Closed | event chain fields, allocation, cloning, traversal, and destruction. |
| `common/Order.java` | `src/zig/core/common.zig`, `src/zig/core/compat/java/report_wire.zig` | Closed | report-facing order representation and Java wire order. |
| `common/OrderAction.java` | `src/zig/core/common.zig`, `src/zig/core/compat/java/report_producer.zig` | Closed | ask/bid semantics and exact-Java identity-assisted hashing. |
| `common/OrderType.java` | `src/zig/core/common.zig`, `src/zig/core/orderbook_naive.zig`, `src/zig/core/orderbook_direct.zig` | Closed | GTC, IOC, IOC-budget, FOK, and FOK-budget execution semantics. |
| `common/PositionDirection.java` | `src/zig/core/common.zig`, `src/zig/core/user_profile.zig`, `src/zig/core/risk.zig` | Closed | empty/long/short position direction and margin accounting. |
| `common/StateHash.java` | `src/zig/core/compat/java/hash32.zig`, `src/zig/core/compat/java/report_producer.zig`, `src/zig/core/common.zig`, `src/zig/core/user_profile.zig`, `src/zig/core/risk.zig`, `src/zig/core/orderbook_naive.zig`, `src/zig/core/orderbook_direct.zig` | Closed | exact Java 32-bit composition with an explicit process identity context and independent native fingerprints. |
| `common/SymbolPositionRecord.java` | `src/zig/core/user_profile.zig`, `src/zig/core/risk.zig`, `src/zig/core/compat/java/report_wire.zig` | Closed | pending/open volume, profit, margin, settlement, reports, and persistence. |
| `common/SymbolType.java` | `src/zig/core/common.zig`, `src/zig/core/risk.zig` | Closed | exchange-pair and futures-contract risk paths. |
| `common/UserProfile.java` | `src/zig/core/user_profile.zig`, `src/zig/core/risk.zig` | Closed | accounts, positions, transaction tracking, status, hashing, and persisted encounter history. |
| `common/UserStatus.java` | `src/zig/core/common.zig`, `src/zig/core/user_profile.zig`, `src/zig/core/compat/java/report_producer.zig` | Closed | active/suspended transitions and exact-Java identity-assisted hashing. |

## Typed API commands

| Java production source | Zig owners | Status | Responsibility evidence |
| --- | --- | --- | --- |
| `common/api/ApiAddUser.java` | `src/zig/core/api_command.zig`, `src/zig/core/exchange_api.zig`, `src/zig/core/exchange_api_sharded.zig` | Closed | add-user fields, timestamps, sync/async/callback publication, and result mapping. |
| `common/api/ApiAdjustUserBalance.java` | `src/zig/core/api_command.zig`, `src/zig/core/exchange_api.zig`, `src/zig/core/exchange_api_sharded.zig` | Closed | currency, amount, transaction ID, adjustment type, timestamp, and result mapping. |
| `common/api/ApiBinaryDataCommand.java` | `src/zig/core/api_command.zig`, `src/zig/core/binary_payload.zig`, `src/zig/core/exchange_api.zig`, `src/zig/core/exchange_api_sharded.zig` | Closed | owned binary command publication, framing, and terminal completion. |
| `common/api/ApiCancelOrder.java` | `src/zig/core/api_command.zig`, `src/zig/core/exchange_api.zig`, `src/zig/core/exchange_api_sharded.zig` | Closed | order/user/symbol identity, timestamps, matcher events, and result mapping. |
| `common/api/ApiCommand.java` | `src/zig/core/api_command.zig` | Closed | exhaustive tagged command union and Java-field translation. |
| `common/api/ApiMoveOrder.java` | `src/zig/core/api_command.zig`, `src/zig/core/exchange_api.zig`, `src/zig/core/exchange_api_sharded.zig` | Closed | price mutation, timestamps, matcher events, and result mapping. |
| `common/api/ApiNop.java` | `src/zig/core/api_command.zig`, `src/zig/core/exchange_api.zig`, `src/zig/core/exchange_api_sharded.zig` | Closed | barrier semantics, timestamps, and Java callback allowlist behavior. |
| `common/api/ApiOrderBookRequest.java` | `src/zig/core/api_command.zig`, `src/zig/core/exchange_api.zig`, `src/zig/core/exchange_api_sharded.zig` | Closed | symbol/depth request, timestamps, and L2 ownership. |
| `common/api/ApiPersistState.java` | `src/zig/core/api_command.zig`, `src/zig/core/exchange_api.zig`, `src/zig/core/exchange_api_sharded.zig` | Closed | adjacent matching/risk persistence pair and merged result. |
| `common/api/ApiPlaceOrder.java` | `src/zig/core/api_command.zig`, `src/zig/core/exchange_api.zig`, `src/zig/core/exchange_api_sharded.zig` | Closed | all order fields, timestamps, holds, matching, and events. |
| `common/api/ApiReduceOrder.java` | `src/zig/core/api_command.zig`, `src/zig/core/exchange_api.zig`, `src/zig/core/exchange_api_sharded.zig` | Closed | size reduction, timestamps, release accounting, and events. |
| `common/api/ApiReset.java` | `src/zig/core/api_command.zig`, `src/zig/core/exchange_api.zig`, `src/zig/core/exchange_api_sharded.zig`, `src/zig/core/exchange_kernel.zig`, `src/zig/core/exchange_runtime.zig` | Closed | global book, risk, user, binary, price, fee, grouping, and pending-state reset. |
| `common/api/ApiResumeUser.java` | `src/zig/core/api_command.zig`, `src/zig/core/exchange_api.zig`, `src/zig/core/exchange_api_sharded.zig` | Closed | status transition, timestamps, and duplicate-edge result. |
| `common/api/ApiSuspendUser.java` | `src/zig/core/api_command.zig`, `src/zig/core/exchange_api.zig`, `src/zig/core/exchange_api_sharded.zig` | Closed | status transition, open-position guard, timestamps, and duplicate-edge result. |

## Binary commands and reports

| Java production source | Zig owners | Status | Responsibility evidence |
| --- | --- | --- | --- |
| `common/api/binary/BatchAddAccountsCommand.java` | `src/zig/core/binary_commands.zig`, `src/zig/core/compat/java/binary_wire.zig`, `src/zig/core/binary_frames.zig` | Closed | exact Java payload, bounded framing, sharded broadcast, and account application. |
| `common/api/binary/BatchAddSymbolsCommand.java` | `src/zig/core/binary_commands.zig`, `src/zig/core/compat/java/binary_wire.zig`, `src/zig/core/binary_frames.zig` | Closed | exact Java payload, bounded framing, sharded broadcast, and symbol application. |
| `common/api/binary/BinaryCommandType.java` | `src/zig/core/binary_payload.zig`, `src/zig/core/binary_commands.zig` | Closed | built-in command codes and unknown-code failure. |
| `common/api/binary/BinaryDataCommand.java` | `src/zig/core/binary_payload.zig`, `src/zig/core/binary_frames.zig`, `src/zig/core/binary_commands.zig` | Closed | binary command envelope, LZ4/raw decoding, transfer assembly, and execution. |
| `common/api/reports/ApiReportQuery.java` | `src/zig/core/api_command.zig`, `src/zig/core/report_query_envelope.zig`, `src/zig/core/exchange_api.zig`, `src/zig/core/exchange_api_sharded.zig` | Closed | typed and owned arbitrary report-query publication. |
| `common/api/reports/ReportQueriesHandler.java` | `src/zig/core/exchange_kernel.zig`, `src/zig/core/custom_report_pipeline.zig`, `src/zig/core/report_registry.zig` | Closed | per-shard query dispatch, section transport, reducer order, and cleanup. |
| `common/api/reports/ReportQuery.java` | `src/zig/core/report_query_envelope.zig`, `src/zig/core/report_registry.zig` | Closed | built-in and registered custom query contracts with allocator-bound ownership. |
| `common/api/reports/ReportResult.java` | `src/zig/core/compat/java/report_wire.zig`, `src/zig/core/custom_report_pipeline.zig` | Closed | exact built-in reduction and typed custom reduction. |
| `common/api/reports/ReportType.java` | `src/zig/core/compat/java/report_wire.zig`, `src/zig/core/report_registry.zig` | Closed | reserved codes, duplicate rejection, and unknown-code handling. |
| `common/api/reports/SingleUserReportQuery.java` | `src/zig/core/compat/java/report_producer.zig`, `src/zig/core/compat/java/report_wire.zig` | Closed | user lookup and per-matching/per-risk Java sections. |
| `common/api/reports/SingleUserReportResult.java` | `src/zig/core/compat/java/report_wire.zig`, `src/zig/core/exchange_api.zig` | Closed | status, accounts, positions, orders, query status, and deterministic reduction. |
| `common/api/reports/StateHashReportQuery.java` | `src/zig/core/compat/java/report_producer.zig`, `src/zig/core/compat/java/report_wire.zig` | Closed | requested submodules and exact section production. |
| `common/api/reports/StateHashReportResult.java` | `src/zig/core/compat/java/report_producer.zig`, `src/zig/core/compat/java/report_wire.zig`, `src/zig/core/compat/java/hash32.zig` | Closed | Java wrapping reduction and explicit JVM identity capability. |
| `common/api/reports/TotalCurrencyBalanceReportQuery.java` | `src/zig/core/compat/java/report_producer.zig`, `src/zig/core/compat/java/report_wire.zig` | Closed | risk and matching balance sections. |
| `common/api/reports/TotalCurrencyBalanceReportResult.java` | `src/zig/core/compat/java/report_wire.zig`, `src/zig/core/risk.zig` | Closed | account, fee, adjustment, suspend, order, and open-interest reduction. |

## Command protocol

| Java production source | Zig owners | Status | Responsibility evidence |
| --- | --- | --- | --- |
| `common/cmd/CommandResultCode.java` | `src/zig/core/cmd.zig` | Closed | complete result-code values, severity ordering, and merged persistence results. |
| `common/cmd/OrderCommand.java` | `src/zig/core/cmd.zig`, `src/zig/core/events.zig` | Closed | command fields, copy/reset rules, matcher chain/L2 ownership, and result completion. |
| `common/cmd/OrderCommandType.java` | `src/zig/core/cmd.zig` | Closed | command codes, mutability/journaling predicate, shutdown/reset/global classification. |

## Configuration

| Java production source | Zig owners | Status | Responsibility evidence |
| --- | --- | --- | --- |
| `common/config/ExchangeConfiguration.java` | `src/zig/core/config.zig` | Closed | composed Java-shaped configuration adapter and validation. |
| `common/config/InitialStateConfiguration.java` | `src/zig/core/config.zig`, `src/zig/core/exchange_core.zig`, `src/zig/core/exchange_core_sharded.zig` | Closed | clean start, snapshot, journal, sequence, exchange ID, and strict-missing policy. |
| `common/config/LoggingConfiguration.java` | `src/zig/core/config.zig` | Closed | logging flags are preserved as typed configuration metadata; JVM logger plumbing is a native divergence. |
| `common/config/OrdersProcessingConfiguration.java` | `src/zig/core/config.zig`, `src/zig/core/risk.zig` | Closed | risk-processing and margin-trading modes. |
| `common/config/PerformanceConfiguration.java` | `src/zig/core/config.zig`, `src/zig/core/runtime_queue.zig`, `src/zig/core/sharded_pipeline.zig` | Closed | queue size, topology, group sizing/time, L2 policy/depth, wait strategy, and naive/direct selection. Java object factories for threads, compressors, and books are represented by Zig-owned lifecycle, the exact wire codec, and the closed built-in book enum; injecting JVM objects is an intentional native divergence. |
| `common/config/ReportsQueriesConfiguration.java` | `src/zig/core/config.zig`, `src/zig/core/report_registry.zig`, `src/zig/core/custom_report_pipeline.zig` | Closed | standard reports, custom registration, reserved codes, constructors, ownership, and digest binding. |
| `common/config/SerializationConfiguration.java` | `src/zig/core/config.zig`, `src/zig/core/serialization_processor.zig` | Closed | dummy/disk presets, journaling switch, and owned custom processor factory. |

## Order books

| Java production source | Zig owners | Status | Responsibility evidence |
| --- | --- | --- | --- |
| `orderbook/IOrderBook.java` | `src/zig/core/orderbook_naive.zig`, `src/zig/core/orderbook_direct.zig`, `src/zig/core/exchange_kernel.zig` | Closed | place/move/cancel/reduce, L2, user orders, streams/views, validation, hashing, and snapshot contract. |
| `orderbook/OrderBookDirectImpl.java` | `src/zig/core/orderbook_direct.zig` | Closed | indexed linked price levels, object pools, matching, mutation, OOM atomicity, validation, and snapshots. Zig uses native arrays/hash indexes rather than Java ART nodes. |
| `orderbook/OrderBookEventsHelper.java` | `src/zig/core/events.zig`, `src/zig/core/orderbook_naive.zig`, `src/zig/core/orderbook_direct.zig` | Closed | trade/reject/reduce allocation and chain ordering. |
| `orderbook/OrderBookNaiveImpl.java` | `src/zig/core/orderbook_naive.zig` | Closed | reference price-level/order lists and full command semantics. |
| `orderbook/OrdersBucketNaive.java` | `src/zig/core/orderbook_naive.zig` | Closed | FIFO price-bucket matching, volume accounting, removal, and validation. |
| `orderbook/OrdersSpliterator.java` | `src/zig/core/orderbook_naive.zig`, `src/zig/core/orderbook_direct.zig`, `src/zig/core/compat/java/report_producer.zig` | Intentional native boundary | Java streams are replaced by deterministic ask-then-bid and FIFO slice traversal used by reports and hashes. |

## Processor ecosystem

| Java production source | Zig owners | Status | Responsibility evidence |
| --- | --- | --- | --- |
| `processors/BinaryCommandsProcessor.java` | `src/zig/core/binary_frames.zig`, `src/zig/core/binary_payload.zig`, `src/zig/core/exchange_kernel.zig` | Closed | bounded assembly, broadcast, exact report/command dispatch, persistence, and reset; Java's command type table is likewise not arbitrarily extendable. |
| `processors/DisruptorExceptionHandler.java` | `src/zig/core/exchange_runtime.zig`, `src/zig/core/sharded_pipeline.zig` | Intentional native boundary | one immutable fatal frontier, wake-all, suffix suppression, and deterministic join replace Disruptor callbacks. |
| `processors/GroupingProcessor.java` | `src/zig/core/grouping_processor.zig`, `src/zig/core/sharded_pipeline.zig` | Closed | group IDs, time/size boundaries, L2 service flags, control gating, and journal/result frontiers. |
| `processors/MatchingEngineRouter.java` | `src/zig/core/exchange_kernel.zig`, `src/zig/core/exchange_core_sharded.zig`, `src/zig/core/sharded_pipeline.zig` | Closed | symbol ownership, binary broadcast, persistence, reports, reset, L2, and matching stage. |
| `processors/ResultsHandler.java` | `src/zig/core/exchange_api.zig`, `src/zig/core/exchange_runtime.zig`, `src/zig/core/sharded_pipeline.zig`, `src/zig/core/simple_events_processor.zig` | Closed | grouping-control suppression and raw → reduced → simple-event publication ordering after journal acknowledgement. |
| `processors/RiskEngine.java` | `src/zig/core/risk.zig`, `src/zig/core/exchange_kernel.zig`, `src/zig/core/sharded_pipeline.zig` | Closed | R1 admission, margin/fees, cached prices, binary commands/reports, persistence, and staged atomic R2 release. |
| `processors/SharedPool.java` | `src/zig/core/events.zig`, `src/zig/core/locked_allocator.zig` | Intentional native boundary | explicit allocator ownership and non-aliasing replace the JVM object pool while preserving bounded lifetime. |
| `processors/SimpleEventHandler.java` | `src/zig/core/simple_events_processor.zig` | Closed | callback dispatch and ownership contract. |
| `processors/SymbolSpecificationProvider.java` | `src/zig/core/risk.zig`, `src/zig/core/common.zig` | Closed | symbol storage, lookup, reset, hashing, binary load, and persistence. |
| `processors/TwoStepMasterProcessor.java` | `src/zig/core/sharded_pipeline.zig` | Closed | stable R1 frontier, prior-group dependency, stage coordination, and exception propagation. |
| `processors/TwoStepSlaveProcessor.java` | `src/zig/core/sharded_pipeline.zig` | Closed | delayed R2 prepare/commit, all-shard atomicity, group completion, and terminal quiescence. |
| `processors/UserProfileService.java` | `src/zig/core/user_profile.zig`, `src/zig/core/risk.zig` | Closed | add/suspend/resume/reset, lookup, hashing, report views, and persistence. |
| `processors/WaitSpinningHelper.java` | `src/zig/core/runtime_queue.zig`, `src/zig/core/sharded_pipeline.zig` | Intentional native boundary | Zig mutex/condition and configured outer wait strategies implement bounded progress without Disruptor sequences. |

## Persistence processors

| Java production source | Zig owners | Status | Responsibility evidence |
| --- | --- | --- | --- |
| `processors/journaling/DiskSerializationProcessor.java` | `src/zig/core/snapshot.zig`, `src/zig/core/journal.zig`, `src/zig/interop/java_persistence/converter.zig`, `src/zig/interop/java_persistence/journal_converter.zig` | Intentional capability boundary | XCM2/XCJ3 are the native durability formats; Chronicle `.ecs`/`.ecj` interoperability is an offline conversion boundary with native restore/replay verification. |
| `processors/journaling/DiskSerializationProcessorConfiguration.java` | `src/zig/core/config.zig`, `src/zig/core/persistence_paths.zig` | Closed | storage root, naming, topology, journal/snapshot policy, and limits. |
| `processors/journaling/DummySerializationProcessor.java` | `src/zig/core/serialization_processor.zig`, `src/zig/core/exchange_core.zig` | Closed | explicit no-op backend and fail-closed persistence-required startup. |
| `processors/journaling/ISerializationProcessor.java` | `src/zig/core/serialization_processor.zig` | Closed | owned type-erased factory, startup restore/replay state, per-module store, journal, shutdown, destruction, and typed failures; custom callbacks remain serial, while snapshot attempts visit every configured ME/RE instance after an earlier store failure and preserve independent matching/risk result codes. |
| `processors/journaling/JournalDescriptor.java` | `src/zig/core/persistence_descriptors.zig`, `src/zig/core/journal.zig` | Closed | partition ranges, sequence/timestamp frontiers, links, and durable registration. |
| `processors/journaling/SnapshotDescriptor.java` | `src/zig/core/persistence_descriptors.zig`, `src/zig/core/sharded_snapshot_store.zig` | Closed | topology-aware snapshots, module instances, parent/journal links, and a commit boundary that publishes custom descriptors only after every matching and risk store succeeds. |

## JVM utilities

| Java production source | Zig owners | Status | Responsibility evidence |
| --- | --- | --- | --- |
| `utils/AffinityThreadFactory.java` | `src/zig/core/exchange_runtime.zig`, `src/zig/core/sharded_pipeline.zig` | Intentional native boundary | Zig owns worker creation/join; configured topology and wait strategy remain observable, JVM thread naming/affinity objects do not cross the native boundary. |
| `utils/CoreArithmeticUtils.java` | `src/zig/core/risk.zig`, `src/zig/core/orderbook_naive.zig`, `src/zig/core/orderbook_direct.zig`, `src/zig/core/compat/java/hash32.zig` | Closed | fees, holds, margin, P&L, wrapping Java report math, and checked native arithmetic. |
| `utils/HashingUtils.java` | `src/zig/core/compat/java/hash32.zig`, `src/zig/core/compat/java/report_producer.zig`, `src/zig/core/common.zig`, `src/zig/core/user_profile.zig`, `src/zig/core/risk.zig` | Closed | Java list/stream/map/hash composition and native fingerprints remain isolated. |
| `utils/ReflectionUtils.java` | `src/zig/core/common.zig`, `src/zig/core/report_registry.zig` | Intentional native boundary | compile-time/type-erased constructors replace Java reflection with typed duplicate and constructor errors. |
| `utils/SerializationUtils.java` | `src/zig/core/compat/java/binary_wire.zig`, `src/zig/core/compat/java/report_wire.zig`, `src/zig/interop/java_persistence/snapshot_wire.zig`, `src/zig/interop/java_persistence/journal_wire.zig`, `src/zig/core/compat/java/eclipse_collections/history_codec.zig` | Closed | primitive collections, nullable values, LZ4 framing, Chronicle bodies, and native persistence codecs. |
| `utils/UnsafeUtils.java` | `src/zig/core/cmd.zig` | Intentional native boundary | typed mutation replaces JVM `Unsafe` field compare-and-set while preserving result-code merge behavior. |
| `utils/Utils.java` | `src/zig/core/common.zig` | Intentional native boundary | power-of-two, sorting, optional, and collection helpers are expressed through native checked APIs. |

## Verification linkage

- `evidence/EXCHANGE_API_PARITY_AUDIT.md` maps every public Java `ExchangeApi`
  declaration to a Zig entry point, observable contract, and executable test.
- `PROCESSOR_PARITY_DECOMPOSITION.md` records processor timing, ownership,
  failure, and persistence boundaries.
- `MIGRATION_COVERAGE.md` records the current Debug, ReleaseSafe, Java fixture,
  audit, format, capacity, and repository-wide gates.
- The class-map coverage check compares this file with all 91 exact production
  Java source paths, verifies every Zig owner path, and rejects missing,
  duplicate, invalid-status, empty-evidence, and placeholder rows.
