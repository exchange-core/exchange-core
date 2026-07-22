# Java -> Zig Migration Coverage (Core)

Date: 2026-02-11
Scope: `src/main/java/exchange/core2/core` -> `src/zig/core`

## Baseline

Current migration gate is green on Zig 0.16.0:

- `zig build test`: PASS
- `zig build gate`: PASS
- targeted bootstrap matrix parity (`zig build parity-diff -- src/zig/fixtures/java_exchange_trade_bootstrap*.fixture`): PASS

This document tracks remaining migration scope beyond the currently passing fixture/test gate.

## Coverage Matrix

| Java area | Zig mapping | Status | Evidence / Notes |
| --- | --- | --- | --- |
| `common/*` domain types (orders, symbols, positions, user status, L2, matcher events) | `core/common.zig`, `core/events.zig`, `core/user_profile.zig` | Ported | Present with parity tests and consumed by kernel/api paths. |
| `common/cmd/*` command/result protocol | `core/cmd.zig` | Ported | Used across kernel/runtime/api and fixture parity flow. |
| `common/config/*` Java config families | `core/config.zig` | Ported (adapter baseline) | Java-family adapters and throughput preset mapping are implemented with explicit single-kernel fallback semantics where required. |
| `common/api/*` typed API commands | `core/api_command.zig`, `core/exchange_api.zig` | Ported | Typed submit/take helpers and binary/report wrappers are implemented. |
| `common/api/binary/*` | `core/binary_payload.zig`, `core/binary_commands.zig`, `core/binary_frames.zig` | Ported | Framed binary ingestion/query paths and decoding are covered in tests. |
| `common/api/reports/*` | `core/exchange_kernel.zig`, `core/exchange_api.zig` report query paths | Ported | `state_hash`, `single_user`, `total_currency_balance`, `user_balance`, `order_book` are wired. |
| `orderbook/*` (`IOrderBook`, naive/direct impls, event helper) | `core/orderbook_naive.zig`, `core/orderbook_direct.zig` | Ported | Naive/direct parity tests and kernel parity gate are present. |
| `processors/RiskEngine.java` + `UserProfileService.java` | `core/risk.zig`, `core/user_profile.zig` | Partial (intentional topology) | Core exchange-mode flow is ported; seeded lifecycle deterministic campaigns now harden add/balance/suspend/resume/place-order paths while broader Java processor class topology remains intentionally consolidated. |
| `processors/GroupingProcessor.java` + result publication pipeline | `core/exchange_runtime.zig` | Ported | Grouped drain, boundary stamping, and sink/callback routing are implemented. |
| `processors/MatchingEngineRouter.java` / multi-engine routing | `core/exchange_core_sharded.zig`, `core/exchange_api_sharded.zig`, `core/exchange_orchestrator.zig` | Ported (baseline) | Production sharded routing baseline is wired with single-vs-sharded backend selection through orchestrator facade. |
| `processors/BinaryCommandsProcessor.java` | `core/binary_commands.zig` + kernel binary dispatch | Ported | Add-symbol/add-account ingestion path implemented. |
| `processors/journaling/*` (`ISerializationProcessor`, descriptors, snapshot/journal flow) | `core/journal.zig`, `core/snapshot.zig`, `core/persistence_descriptors.zig`, `core/persistence_paths.zig`, `core/exchange_core.zig` | Partial (intentional topology) | Snapshot/journal/default-path discovery/parity helpers are ported; seeded partition-corruption startup tests now harden sequence/result mismatch boundaries while Java processor topology remains intentionally consolidated. |
| `ExchangeCore.java` orchestration | `core/exchange_core.zig`, `core/exchange_orchestrator.zig` | Ported (baseline) | Lifecycle/bootstrap/persistence/api wiring is implemented and orchestrator now selects single or sharded backend from Java config. |
| `ExchangeApi.java` facade | `core/exchange_api.zig` | Ported | Same-name class-family parity is now audit-gated (`java-api-parity-audit` is part of `zig build gate`) and behavior-depth wrappers/callback/report/binary/persistence coverage is in-place. |
| `SimpleEventsProcessor.java` + `IEventsHandler.java` | `core/simple_events_processor.zig` and core/api registration helpers | Ported | Java-style callback stream is implemented and fixture-backed. |
| `utils/*` (`AffinityThreadFactory`, `ReflectionUtils`, `UnsafeUtils`, etc.) | mostly absorbed into Zig stdlib + local helpers | Partial | No 1:1 module mapping by design; parity-critical behavior is embedded where required. |

## Current Gap Signals

- High-value production sharded orchestration baseline is now implemented (`exchange_core_sharded`, `exchange_api_sharded`, `exchange_orchestrator`).
- Tiger-style deterministic seeded fault/recovery coverage now includes startup/bootstrap/replay perturbations with reproducible seed + replay hints, including orchestrator startup-mode matrix coverage (configured/discovered/discovered-strict) under malformed-main-log perturbations, journaling replay corruption simulations across single+sharded wrappers, long-horizon multi-cycle restart/persist strict-probe simulations, per-cycle configured+discovered startup probes under the same long-horizon perturbation stream, mixed sync/async submission deterministic campaigns, and callback-processing campaigns, each expanded with multi-seed matrix coverage and same-backend seed-sensitivity checks.
- Risk/user partial parity hardening now includes seeded deterministic lifecycle campaigns in `core/risk.zig` that repeatedly exercise add/balance/suspend/resume/place-order flows with per-round state-hash mixing and invariant guards.
- Journaling consolidation hardening now includes seeded configured-startup partition corruption campaigns in `core/exchange_core.zig` that deterministically validate `SequenceMismatch` and `ResultMismatch` failure boundaries.
- ExchangeApi class-by-class parity artifact is now generated (`EXCHANGE_API_PARITY_AUDIT.md` / `.json`): 24 Java method families, 33 Java declarations, and 24/24 same-name family coverage in Zig.
- ExchangeApi class-by-class parity is now enforced in the migration gate: `zig build gate` runs `java-api-parity-audit` and fails if any Java method family lacks same-name Zig coverage.
- Processor-ecosystem decomposition artifact is now generated (`PROCESSOR_PARITY_DECOMPOSITION.md`) with Java processor-class ownership mapping and explicit consolidation rationale.
- Consolidated behavior-depth hardening now includes deterministic runtime edge-case coverage and orchestrator persistence-wrapper delegation checks.
- Persistence interoperability policy is now explicit (`PERSISTENCE_INTEROP_DECISION.md`) and enforced in loaders via dedicated unsupported-legacy-format errors.
- Optional legacy adapter phase-0/1 scaffold is now in place (`core/legacy_persistence_adapter.zig`, `LEGACY_PERSISTENCE_ADAPTER_SPIKE.md`) with typed preflight classification, explicit legacy header parser skeleton, and `NotImplemented` conversion boundary.
- Remaining migration work is now optional adapter phase-2+ implementation only if an external requirement appears.

## Recently Completed Slices

1. ExchangeApi parity audit (class-by-class) [Completed]
- Goal: explicitly verify Java `ExchangeApi` public surface against Zig `exchange_api`.
- Done criteria:
  - machine-readable method inventory diff (or strict checklist) committed,
  - each missing method categorized (ported, intentionally dropped, deferred),
  - follow-up tasks created for deferred parity gaps.

2. Consolidated-responsibility behavior hardening [Completed]
- Goal: add targeted behavioral regression guards where Java processor responsibilities were intentionally consolidated in Zig.
- Done criteria:
  - targeted deterministic tests for consolidated areas (runtime gating, wait/pipeline semantics, persistence policy paths),
  - explicit rationale links to decomposition artifacts for each hardened area,
  - no behavior regression in `zig build gate`.

3. Coverage/status artifact refresh automation [Completed]
- Goal: keep migration status documents and parity evidence synchronized with code changes.
- Done criteria:
  - migration coverage tables reference current implementation files and status,
  - verification command set in docs matches current gate/test commands,
  - remaining gaps are prioritized and bounded.

4. Persistence format interoperability decision [Completed]
- Goal: explicitly decide and document whether Chronicle/LZ4 byte-level compatibility is required, and if required, define adapter scope.
- Done criteria:
  - decision recorded with rationale (native Zig format-only vs compatibility adapter),
  - if adapter required, bounded implementation ticket(s) created,
  - if adapter not required, migration docs explicitly capture the intentional divergence.

5. Long-horizon deterministic simulation expansion for consolidated paths [Completed]
- Goal: expand seeded deterministic simulations around persistence/discovery edge cases and orchestrator backend selection perturbations.
- Done criteria:
  - seeded scenarios include replay instructions for failures,
  - simulations exercise both single and sharded orchestrator paths,
  - long-horizon restart campaigns now include per-cycle configured+discovered startup probes alongside strict-mode checks,
  - `zig build gate` remains green.

6. Risk/UserProfile partial parity hardening [Completed]
- Goal: harden correctness-sensitive risk/user lifecycle behavior under seeded deterministic campaigns while keeping intentional non-1:1 topology explicit.
- Done criteria:
  - seeded campaign covers add/balance/suspend/resume/place-order paths with replay-oriented failure context,
  - per-round state-hash mixing + account-balance invariants guard deterministic behavior depth,
  - migration coverage notes now mark risk/user as intentional topology partial with explicit hardening evidence.

7. Journaling topology consolidation hardening [Completed]
- Goal: tighten journaling persistence boundaries under consolidated Zig ownership with deterministic corruption coverage.
- Done criteria:
  - seeded configured-startup campaigns now exercise baseline, partition sequence mismatch, and partition result mismatch scenarios deterministically,
  - failure paths carry replay-oriented seed/context diagnostics,
  - migration coverage notes now mark journaling as intentional topology partial with explicit boundary hardening evidence.

8. Legacy-format adapter spike (phase 0 scaffold) [Completed]
- Goal: establish a bounded offline adapter boundary without changing runtime persistence readers.
- Done criteria:
  - standalone phase-0 spike design note added (`LEGACY_PERSISTENCE_ADAPTER_SPIKE.md`),
  - typed probe/preflight module added (`core/legacy_persistence_adapter.zig`) with explicit failure taxonomy,
  - conversion path remains explicit `NotImplemented`, preserving native runtime format ownership.

9. Legacy-format adapter phase 1 parser skeleton [Completed]
- Goal: parse bounded Chronicle/LZ4 frame header metadata in an offline adapter path without changing runtime readers.
- Done criteria:
  - typed parser metadata model added (standard/skippable header shapes),
  - parser returns explicit malformed/truncated/unsupported descriptor errors,
  - preflight now captures parsed header metadata while preserving explicit non-legacy rejection behavior.

## Prioritized Next Execution Slices

1. Optional legacy-format adapter phase 2 semantic conversion skeleton (only if external requirement appears)
- Goal: map parsed legacy archive payloads into native Zig snapshot/journal writer paths in an offline adapter flow, still without runtime reader changes.
- Done criteria:
  - explicit producer/consumer trigger and bounded archive set are documented for the conversion scope,
  - conversion path emits native Zig artifacts via existing persistence writers with typed failures,
  - no regression to native Zig persistence invariants.

## Immediate Next Task Recommendation

No mandatory migration slice remains in the current scope. Only start Slice 1 if a concrete external requirement appears for Chronicle/LZ4 compatibility import; otherwise keep native Zig persistence as authoritative.
