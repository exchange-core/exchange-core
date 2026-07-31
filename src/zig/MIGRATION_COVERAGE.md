# Java → Zig Migration Coverage

Date: 2026-07-31
Scope: `src/main/java/exchange/core2/core` → `src/zig/core`

This is the completed-port evidence ledger for the scoped Java package. A row
is only “verified current” when its cited checks were run on the current
worktree. `JAVA_PRODUCTION_CLASS_MAP.md` supplies the exhaustive 91-file map;
this document summarizes the observable subsystem gates.

## Status legend

- **Verified current** — focused Debug and ReleaseSafe checks passed after the
  latest relevant change.
- **Intentional native divergence** — Zig owns a native runtime representation;
  Java interoperability is provided by an offline boundary instead.

## Verification snapshot

Current toolchain:

- Zig 0.16.0
- system JDK 25.0.2
- workspace-local Temurin JDK 11.0.32+9 for the Chronicle 2.19.1 persistence
  oracle

Focused and broad checks on the final grouped-default and carrier boundary:

- `persistent grouped` through the current test root: 45/45 Debug and 45/45
  ReleaseSafe, covering worker reuse, batch admission, backpressure, R1
  lookahead, R2 prepare-all/commit-none, matching/R1 failure ordering, poison,
  asymmetric state, allocator ownership, and terminal settlement
- R2 boundary-cache focus: 4/4 Debug and 4/4 ReleaseSafe; exact adjacency
  probes are 1, 16, 256, and 4096 for closed groups of those sizes instead of
  the previous 1, 136, 32896, and 8390656 repeated probes
- `java_binary_wire.zig`: 350/350 Debug and 350/350 ReleaseSafe, including
  byte-exact one-allocation request carriers, SIMD/scalar equality, every tail,
  deliberately misaligned byte and word slices, padding, and allocation faults
- `persistent grouped` through `exchange_api_sharded.zig`: 43/43 ReleaseSafe;
  broader `grouped` coverage through `exchange_orchestrator.zig`: 67/67
  ReleaseSafe
- custom serialization/main-log boundary: 2/2 single-core and 3/3 sharded
  Debug and ReleaseSafe; custom backends fail before publication with
  `ConfiguredMainLogRequiresDiskBackend`, while dummy and stopped error
  precedence remains compatible
- committed Java `DiskSerializationProcessor` fixture path: 4/4 Debug and 4/4
  ReleaseSafe after the deterministic-manifest correction
- `zig build -j1 gate --summary all`: 43/43 steps and 2305/2305 tests,
  comprising 1124 core tests, 13 standalone app tests, and 5 SIMD benchmark
  tests in each of Debug and ReleaseSafe, plus 21 evidence-tool tests; the gate
  also compiles the fixed-ReleaseFast benchmark and runs the exact 96/96
  default fixture manifest, API surface, Java API parity audit, and executable
  91-class production-map check;
  persistence-disabled and injected-fault warnings are expected test evidence
- pinned Zig 0.16.0 cross-target ReleaseSafe compilation: x86_64 Linux
  baseline, x86_64 Linux AVX2, RISC-V Linux scalar fallback, and s390x Linux
  big-endian all passed; AVX2 codegen contains scoped `vpshufb` carrier loops
- ReleaseFast `bench-orchestrator-ab` with four counterbalanced trials, 20,000
  settled commands, 20,000 detached commands, 500 latency samples, and 512
  memory-probe commands: all 16 rows produced canonical fingerprint
  `115306a07a302c98`; all eight persistent capacity rows passed their executable
  1x1/4x2 throughput and latency floors; exact raw rows are retained in
  `ORCHESTRATOR_AB_RAW_2026-07-31.txt`
- fixed-ReleaseFast 16 MiB carrier benchmark: three independent processes,
  24 counterbalanced trials each; median process-level production/direct
  speedups were 1.1366x pack and 1.1126x unpack, with every timing and paired
  row retained in `SIMD_BENCHMARK_RAW_2026-07-31.txt`
- compatible Temurin 11.0.32+9 `mvn test`: 174 tests, 0 failures, 0 errors, 10
  intentionally skipped opt-in fixture writers
- the persistence writer passed in two separate JVM processes after removing
  process-local component hashes from the portable manifest; both runs also
  generated two byte-identical families internally and recovered snapshot and
  journal state in Java
- strict public report and persistence writers: 2/2 with
  `parity.fixture.diagnostic.allow_internal_fallback` unset; fallback remains
  unavailable unless that diagnostic-only property is explicitly `true`
- `zig fmt --check` and `git diff --check`: required on the final packaged
  worktree

Current Java archive conversion checks:

- snapshot-family converter: 9/9 Debug and 9/9 ReleaseSafe
- journal-family converter: 15/15 Debug and 15/15 ReleaseSafe
- committed Java persistence fixture cross-language path: 4/4 Debug and 4/4
  ReleaseSafe

The aggregate gate is the release evidence for this worktree. It must be rerun
after any later source or fixture change.

Fresh Java persistence-fixture generation is verified with workspace-local
Temurin 11.0.32+9. Independent `DiskSerializationProcessor` runs across
separate JVM processes produced byte-identical `.ecs`/`.ecj` artifacts,
recovered the expected snapshot and journal states in Java, and published the
checksummed
`src/zig/fixtures/java-persistence/v1` family. Zig converts those exact bytes
to XCM2/XCJ3 and checks business reports and native readback in both
optimization modes. The source `.eca` is excluded by an explicit metadata
policy because it contains wall-clock log data. Java StateHash and component
hashes remain checked for same-process round-trip equivalence but are excluded
from the portable manifest because enum/object identity is process-local.

## Observable subsystem matrix

| Java area | Zig mapping | Current status | Evidence and exact boundary |
| --- | --- | --- | --- |
| Domain and command protocol | `common.zig`, `cmd.zig`, `events.zig`, `user_profile.zig` | Verified current | Types, result-code merging, event ownership, and state hashing are consumed by kernel/runtime tests. |
| Configuration families | `config.zig`, `serialization_processor.zig`, `exchange_core_sharded.zig` | Verified current | Independent matching/risk counts, semantic settings, wait strategy, grouping, L2 policy, native book selection, custom-report registries, and an owned custom serialization-processor factory propagate through construction. Custom processors receive startup restore/replay, synchronous per-module snapshot stores, journal writes, and exactly-once shutdown/destruction. Configured main-log helpers reject custom backends before publication with `ConfiguredMainLogRequiresDiskBackend`. Java factories that inject JVM thread, compressor, or order-book objects map to Zig-owned lifecycle, the exact wire codec, and the closed native book enum; they are intentional native representations, not missing observable behavior. |
| Exchange API and lifecycle | `api_command.zig`, `exchange_api.zig`, `exchange_core.zig`, `exchange_api_sharded.zig` | Verified current | Lifecycle-owned workers, FIFO async progress, callbacks, bounded results, and fatal-tail settlement exist. `drainPending`/`drainAsyncPending` capture a contiguous accepted prefix, wait for terminal callback/poll/detached ownership even after poison, include terminal grouped R2, and single-claim overlapping async-drain counts. Core `drainRuntime` separately claims its runtime frontier on success while waiting for the captured global accepted prefix before reporting poison; cold owners retain their exact grouped outcome before releasing ownership, and overlapping callers cannot double-report. `shutdownTimeout`, `shutdownState`, and typed `ShutdownOutcome` fence admission immediately, bound accepted work/callback/worker quiescence, allow timeout or callback/worker deferral to be retried, and preserve exact poisoned-tail settlement across single, sharded, and orchestrated backends. Every Java ordinary typed command family can preserve caller timestamps through single/sharded sync, detached, async, batch, and command-supported callback paths, plus orchestrated sync, detached, async, batch, full-response, and command-supported callback paths, raw observation, grouping, and journaling. Struct-backed forms retain default-zero fields and existing report behavior; `*_with_timestamp` companions preserve legacy scalar/unit literal shapes, and reset/nop remain outside the direct Consumer-callback allowlist in both forms. Barrier-owning callbacks share a nesting-safe execution context: checked foreign reconfiguration returns `CallbackReconfigurationWouldBlock`, all synchronous/lifecycle/query/wait/composite roots return `CoreReentrantCall` before publication, and async/callback/detached publication remains legal only when every target lock and capacity requirement is immediately available. The generated surface inventory is 24 Java / 207 Zig / 24 shared / 0 Java-only / 183 Zig-only. The semantic audit covers 24/24 scoped Java families by locating each exact cited test body and validating its direct-call token. |
| Binary transport and state | `binary_payload.zig`, `java_binary_wire.zig`, `binary_frames.zig`, `binary_commands.zig` | Verified current | Java LZ4 envelopes, bounded multi-frame publication, reset, sharded broadcast, terminal-only completion, poison handling, and pending-transfer persistence are covered. Built-in command types are add-symbols and add-accounts. Java also hard-codes its binary command type table, so arbitrary binary commands are not an extension surface on either side. |
| Built-in exact Java reports | `java_report_producer.zig`, `java_report_wire.zig`, kernel/API report lanes | Verified current | StateHash, SingleUser, and TotalCurrencyBalance sections, reducers, wrapping arithmetic, section labels, and event carriers exist. Exact and Zig-native projections are isolated. Live int/long Eclipse layout histories, exact query-local copies/reducers, and XMB4/XRB4 persistence/import preserve encounter history. Exact StateHash identity uses the explicit capability boundary below. |
| StateHash identity | `java_report_producer.zig`, `exchange_kernel.zig`, `exchange_api.zig` | Verified current — explicit capability boundary | `JavaIdentityHashContext` supplies the process-specific Java enum/object identity values required for exact StateHash projection. Exact requests fail closed with `MissingJavaIdentityHashContext` when it is absent; native requests remain available. Portable persistence artifacts intentionally omit process-local identity hashes while Java regeneration verifies them within each JVM process. |
| Custom reports | `report_registry.zig`, `report_query_envelope.zig`, `custom_report_pipeline.zig`, sharded config/API lanes | Verified current | Typed registry ownership, duplicate/unknown-code handling, owned arbitrary query envelopes, per-engine dispatch, Java BINARY_EVENT section/reducer transport, API-owned request tokens and lifecycle settlement, and XCM2 configuration-digest binding are wired. Legacy XCS/XME/XRE/XCM snapshot paths fail closed for a non-empty registry instead of silently restoring incompatible state. |
| Risk and user profiles | `risk.zig`, `user_profile.zig` | Verified current | Spot and margin admission, staged release, fees, P&L, suspend/resume, reports, arithmetic failure atomicity, multi-risk prepare-all/commit-all coverage, and persisted Eclipse layout history exist. Absent-currency spot NSF preserves Java's active zero-valued key, and rejected temporary margin positions preserve the zero quote-account key. Compatible-JDK Java recovery and the real cross-language persistence fixture are current. |
| Naive/direct order books | `orderbook_naive.zig`, `orderbook_direct.zig` | Verified current | GTC/IOC/FOK-budget behavior, duplicate-GTC reject-before-trade settlement, reduce flags, OOM atomicity, holds, and naive/direct parity are covered. Zig direct uses native arrays/hash indexes/pools rather than Java’s ART; the representation is intentionally native and this ledger makes no cross-host Java performance claim. |
| Sharded state ownership | `exchange_kernel.zig`, `exchange_core_sharded.zig` | Verified current | Matching is symbol-owned, risk is UID-owned, independent ME/RE topologies work, and routed/split share one publication frontier. |
| Sharded execution topology | `exchange_kernel.zig`, `sharded_pipeline.zig`, `exchange_api_sharded.zig` | Verified current | Default `persistent_grouped` mode owns stable slots, one persistent paired R1/R2 worker per RE, one worker per ME, journal/result/R2 frontiers, Java-compatible same-group R1 lookahead, cross-group matching/R2 barriers, global-command terminal barriers, query/snapshot/shutdown quiescence, deterministic lifecycle/fatal teardown, and asymmetric topology coverage. A matching fatal can leave later same-group R1 mutations, matching Java processor timing; poison is terminal and suppresses the fatal sequence plus suffix results. Explicit `immediate` remains the compatibility pin. The ReleaseFast capacity gate produced fingerprint `115306a07a302c98` in all 16 rows and passed every 1x1/4x2 throughput and latency floor. |
| Grouping and event publication | `grouping_processor.zig`, `sharded_pipeline.zig`, `exchange_runtime.zig`, `simple_events_processor.zig`, core/API/orchestrator facades | Verified current | Live persistent mode carries group stamps, deadlines, L2 flags, journal-gated pre-R2 results, global barriers, callbacks outside execution locks, suppression rules, and delayed R2. `SimpleEventsProcessor` ignores matcher events whose kinds are outside Java's simple trade/reject/reduce mapping. A shared borrowed full-response observer mirrors Java `ResultsHandler` visibility on single and sharded backends before request-specific completion. |
| Native snapshot/journal | `snapshot.zig`, `sharded_snapshot*.zig`, `journal.zig`, core facades | Verified current | XCM2/XCJ3 every-shard restore, topology/config/frontier binding, checksums, atomic manifests, durable directory publication, crash-tail repair, retry-safe cold replay, sparse continuation, and Java’s non-mutating journal predicate are covered. A true `JournalBatchPosition` captures only the queue prefix present at the persistence boundary; concurrent suffix work is excluded, including the `{2,2}` suffix and persistent-grouped regression cases. Grouped startup inventories valid XCM1/XCM2 manifests and returns `LegacyXcm1RequiresImmediateOrMigration` when XCM1 is newest; explicit immediate mode can restore it for migration. Compatible-JDK fixture generation, Java recovery, offline conversion, and Zig native readback are current. |
| Java snapshot import | `java_snapshot_wire.zig`, `java_snapshot_body.zig`, `java_persistence_converter.zig` | Verified current | Semantic `.ecs` family decoding, atomic XCM2 conversion and native restore are covered with a real checksummed Chronicle fixture plus synthetic asymmetric, corrupt, and OOM families in Debug and ReleaseSafe. |
| Java journal import | `java_journal_wire.zig`, `java_journal_replay.zig`, `java_journal_converter.zig` | Verified current | Raw/compressed family conversion, routed/split replay, persistence-barrier validation, sparse appendable XCJ3 continuation, family/frontier binding, atomic output publication, and native readback are covered. A real checksummed raw Chronicle journal is replayed after its converted Java snapshot in Debug and ReleaseSafe. |
| Persistence representation | Native XCM2/XCJ3 plus offline Java converters | Intentional native divergence | Runtime readers remain Zig-native. Offline `.ecs`/`.ecj` conversion is mandatory migration scope; Java byte formats are not loaded directly by live engines. |
| JVM/unsafe/thread utilities | Zig stdlib and local runtime helpers | Intentional native divergence | No 1:1 class mapping is intended. Observable wait, lifecycle, ownership, failure, and serialization behavior must still be proved at subsystem boundaries. |

## Remaining parity blockers

None in the scoped Java production-class map.

JVM identity remains an explicit portability boundary, not a blocker:
`JavaIdentityHashContext` accepts the identity values from the owning Java
process, and exact StateHash requests fail closed when those values are not
available. Native hashing and all portable business-state comparisons remain
available without that capability.

## Closed grouped-default promotion slice

`ExchangeApiSharded`, `ExchangeCoreConfig`, and `JavaExchangeConfig` now default
to `persistent_grouped`; explicit `immediate` remains available. Live ordinary
commands, journal-gated result transfer, delayed R2, global commands, queries,
snapshots, persistence, reset, fatal teardown, and shutdown are integrated.
Java-default 1x1 orchestration now selects sharded/XCM2. If the newest committed
snapshot is legacy XCM1, grouped startup returns
`LegacyXcm1RequiresImmediateOrMigration`; pinning immediate restores XCM1 so it
can be migrated instead of silently losing state.

The implementation sequence is complete: the serial timing oracle, persistent
per-RE and per-ME workers, live API integration, terminal global barriers,
query/snapshot/reset/shutdown quiescence, fail-closed XCM1 inventory, A/B
benchmark, default switch, and 2305-test repository gate are all current. The
dated benchmark report records covered-workload final-state equivalence and the
executable capacity result.

## Closed performance and capacity gate

The ReleaseFast four-trial gate passed 3/3 build steps and all eight persistent
1x1/4x2 capacity rows. The 1x1 grouped medians were 636575.92 settled
commands/s, 1005495.13 detached commands/s, 32792 ns terminal-drain p50, and
11229.5 ns report p50. The 4x2 medians were 189886.68 settled commands/s,
281443.53 detached commands/s, 54979 ns terminal-drain p50, and 11500 ns report
p50. Every row
returned to zero live requested bytes after deinit. The report is a same-host
Zig capacity gate, not a cross-host Java benchmark claim. See
[`ORCHESTRATOR_AB_BENCHMARK_REPORT_2026-07-31.md`](ORCHESTRATOR_AB_BENCHMARK_REPORT_2026-07-31.md).

## Closed focused slices

- Zig 0.16.0 migration and current build API adaptation
- lifecycle-owned single and sharded workers with bounded publication
- deterministic single and sharded drain settlement over a captured contiguous
  accepted prefix, including poisoned callback/poll/detached owners, grouped
  terminal R2/reclaim, deferred unpublished binary suffixes, cold ownership
  handoff, exact cold runtime grouping outcomes, and monotonic single-claim
  accounting for overlapping async and runtime drains
- nesting-safe global callback preflight across core/core, sharded/sharded, and
  both mixed core/sharded directions: synchronous submission, lifecycle,
  query, wait, drain, callback-dispatch, and composite roots fail with
  `CoreReentrantCall` before publication, while async/callback/detached work is
  admitted only through no-wait driver, execution, capacity, journal, and
  binary-stream gates; bounded A-to-B-to-A cycles, contended/saturated
  no-mutation cases, fitting and would-wait binary paths, and fatal callback
  settlement are covered
- typed, retryable bounded shutdown across single, sharded, and orchestrated
  backends, including immediate admission fencing, cold heap-stable drainers,
  callback/worker deferral, timeout-to-retry/deinit convergence, exact poison
  settlement, and documented post-quiescence untimed OS-thread join tails
- standalone fixed-slot serial and persistent per-shard grouped runtimes,
  including journal-before-result, delayed R2, deterministic lifecycle races,
  and thread-safe allocator enforcement
- default live persistent grouped execution in `ExchangeApiSharded`, with an
  explicit immediate compatibility pin, including
  global barriers, callbacks outside execution locks, query/snapshot/shutdown
  quiescence, startup rollback, and poison-safe worker/runtime teardown
- grouped-default XCM1 inventory and typed pin-or-migrate failure,
  covered-workload A/B final-state fingerprint parity, and the 2305-test
  repository gate
- Java-equivalent borrowed raw full-response observation across single,
  sharded, and orchestrated execution, with one grouping gate, journal-first
  publication, raw/runtime/SimpleEvents ordering, pre-R2 sharded delivery, and
  replay/fatal/unpublished-suffix silence
- nesting-safe shared callback execution scope for single and sharded backend
  barriers, preserving same-instance reconfiguration while making checked
  cross-backend setter reconfiguration return
  `CallbackReconfigurationWouldBlock` instead of forming an ABBA wait
- caller-supplied timestamps on every Java ordinary typed command across
  single/sharded sync, detached, async, batch, and command-supported callback
  paths, plus orchestrated sync, detached, async, batch, full-response, and
  command-supported callback paths, raw observer order, grouping, journaling,
  zero-default compatibility, and existing GTC report behavior;
  explicit `*_with_timestamp` variants add timestamps to add-user, suspend,
  resume, reset, and nop without changing their legacy scalar/unit literal
  shapes or callback allowlists
- terminal-only oversized binary streams and fatal suffix settlement
- independent ME/RE state ownership and routed/split publication identity
- global reset and pending binary state restore
- exact/native report-lane isolation, including Java wrapping versus native
  checked overflow
- duplicate-GTC Java event ordering and collateral/fee conservation
- naive/direct order-book semantic parity on covered command families,
  including terminal taker completion across multiple makers
- XCM2 semantic every-shard snapshot/restore and offline `.ecs` conversion
- exact Eclipse int/long live histories, query-local reducer order, and XMB4/XRB4
  persistence/import
- typed custom-report registry/configuration, per-engine dispatch, request-token
  lifecycle, and XCM2 configuration-digest binding
- XCJ3 checksummed records, durable restart/frontier handling, partial-tail
  repair, retry-safe replay, sparse continuation, and Java mutability predicate

## Final release gate — passed

- focused Debug and ReleaseSafe topology, persistence, lifecycle, and custom
  extension checks passed;
- the serialized aggregate gate passed 43/43 build steps and 2305/2305 tests:
  1124 core, 13 standalone app, and 5 SIMD benchmark tests in each of Debug and
  ReleaseSafe, plus 21 evidence-tool tests, including the exact 96/96 default
  fixture manifest and both API audits;
- the executable 91-class Java production map passed;
- Temurin 11.0.32+9 ran the complete Java suite: 174 tests, 0 failures, 0
  errors, and 10 intentionally skipped opt-in writers;
- the real Chronicle persistence writer passed in two separate JVM processes,
  and Zig converted and recovered the resulting fixture in both modes;
- the ReleaseFast semantic/capacity gate passed all thresholds and returned all
  tracked allocations at deinit;
- formatting, patch hygiene, the final aggregate rerun, and the source archive
  checksum are recorded with the packaged worktree.

There are no tracked P0/P1 parity gaps in the scoped 91-class production map.
