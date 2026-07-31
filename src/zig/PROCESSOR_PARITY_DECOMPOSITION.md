# Processor Ecosystem Parity Decomposition (Java -> Zig)

Updated: 2026-07-31

Scope: `src/main/java/exchange/core2/core/processors/**` -> `src/zig/core/**`

This maps observable Java processor responsibilities to current Zig ownership.
It is not a class-for-class requirement, and “implemented” does not by itself
mean execution-timing or persistence parity is closed.

## Responsibility Matrix

| Java processor | Zig ownership | Status | Remaining parity boundary |
| --- | --- | --- | --- |
| `BinaryCommandsProcessor` | `binary_frames.zig`, `binary_payload.zig`, `java_binary_wire.zig`, `exchange_kernel.zig` | Implemented, hardened | Bounded transfer assembly, native/exact-Java report projection, and custom report registry/configuration/dispatch exist. Java also hard-codes its binary command type table, so arbitrary binary commands are not an extension surface on either side. |
| `RiskEngine` | `risk.zig`, `user_profile.zig`, `exchange_kernel.zig`, `sharded_pipeline.zig` | Verified current | Default persistent grouped mode runs one paired R1/R2 worker per risk shard with Java-compatible same-group R1 lookahead, group-delayed R2, prepare-all/commit-none, deterministic stage retirement, global barriers, and terminal quiescence. Cross-group R1 waits for prior matching and R2. A matching fatal can leave a same-group R1-mutated suffix, but poison is terminal and prevents reuse or suffix result publication. Explicit immediate mode remains the compatibility pin; the executable capacity gate is current. |
| `UserProfileService` | `user_profile.zig`, `risk.zig` | Verified current | Exact Eclipse int/long live layout histories and XMB4/XRB4 persistence/import preserve report encounter order. Compatible-JDK Java recovery and the real cross-language persistence fixture are current. |
| `MatchingEngineRouter` | `exchange_kernel.zig`, `exchange_core_sharded.zig`, `sharded_pipeline.zig`, `exchange_api_sharded.zig` | State sharding and live persistent execution implemented | Default persistent grouped mode runs one worker per ME with asymmetric routing and coherent global barriers. Explicit immediate mode remains available for compatibility. |
| `GroupingProcessor` | `grouping_processor.zig`, `sharded_pipeline.zig`, runtime/API orchestration | Verified current | Live group boundaries, journal/result frontiers, global barriers, and callback reentry/shutdown schedules are verified in the grouped default, repository gate, and ReleaseFast capacity gate. |
| `TwoStepMasterProcessor` / `TwoStepSlaveProcessor` | `exchange_kernel.zig`, `sharded_pipeline.zig`, sharded runtime/API orchestration | Live ordinary-command responsibilities implemented | Stable frontiers, previous-group R2, prepare/commit atomicity, lifecycle joins, global quiescence, and deterministic failures are verified in default persistent mode. Explicit immediate mode remains available for compatibility. |
| `ResultsHandler` | `exchange_runtime.zig`, `exchange_core.zig`, `exchange_api*.zig`, `exchange_orchestrator.zig`, `sharded_pipeline.zig`, `simple_events_processor.zig` | Implemented and verified on single and sharded backends | One shared borrowed full-response observer applies Java's command-driven grouping gate, publishes after journal acknowledgement and before request-specific completion, and precedes reduced runtime and SimpleEvents callbacks. Persistent grouped mode preserves Java's result/R2 sibling timing by publishing without waiting for R2. Replay, fatal, and unpublished suffix work remain silent. |
| `SimpleEventHandler` | `simple_events_processor.zig` | Implemented | Callback ownership and event-chain lifetime are covered under the now-default concurrent topology. Like Java, the adapter ignores matcher-event kinds outside the simple trade/reject/reduce mapping. |
| `SymbolSpecificationProvider` | `risk.zig`, `common.zig` | Implemented | No known standalone blocker; it participates in snapshot and exact-hash validation. |
| `ISerializationProcessor` | `serialization_processor.zig` plus snapshot/journal/converter/path/descriptor modules | Native presets and custom factory implemented | Dummy/disk remain presets. A factory-created owned custom processor receives startup restore/replay state, every ME/RE store request (including asymmetric sharded instances), journal writes, and exactly-once shutdown/destruction with bounded errors. Native crash-safe snapshot/journal adoption is closed, and a real checksummed `DiskSerializationProcessor` snapshot/journal family is converted and business-state checked in Debug and ReleaseSafe. |
| `DiskSerializationProcessor` | `snapshot.zig`, `sharded_snapshot*.zig`, `journal.zig`, `exchange_core.zig`, `exchange_api_sharded.zig` | Native XCM2/XCJ3 paths implemented and hardened | XCJ3 durable publication/frontier, crash-tail repair, retry-safe replay, sparse continuation, and directory-sync boundaries are covered. XCM2 binds the custom-registry configuration digest; legacy XCS/XME/XRE/XCM restores fail closed for a non-empty registry. Grouped discovery also rejects newest-committed XCM1 with `LegacyXcm1RequiresImmediateOrMigration`; explicit immediate mode remains the restore/migration pin. |
| `DummySerializationProcessor` | typed configuration and disabled persistence paths | Consolidated | Disabled persistence must continue to reject persistence-required startup/config combinations explicitly. |
| `DiskSerializationProcessorConfiguration` | `config.zig`, `persistence_paths.zig` | Implemented | Keep topology, storage-root ownership, and initial-state validation bound to the actual native formats. |
| `JournalDescriptor` / `SnapshotDescriptor` | `persistence_descriptors.zig` | Implemented, hardened | Descriptor registration follows the corresponding durable commit boundary. |
| `DisruptorExceptionHandler` | Zig error unions and fail-closed/poisoned state | Architecture-specific, live verified | Persistent stages preserve one immutable fatal record, deterministic sequence retirement, wake-all poison, fatal-tail settlement, and poison-safe outer/inner worker teardown. |
| `WaitSpinningHelper` | runtime queue/wait configuration, `PersistentGroupedRuntime` conditions | Verified native implementation | Persistent workers use Zig mutex/condition scheduling rather than Disruptor wait strategies; default persistent-mode integration is verified across configured outer wait strategies, and the persistent ReleaseFast capacity floors pass. |
| `SharedPool` | allocator-owned commands, responses, and event chains; `locked_allocator.zig` | Architecture-specific, focused verified | Response and R2 chains are non-aliasing, runtime state is heap-stable, and concurrent payload allocation requires one stable locked facade or an intrinsically thread-safe allocator. |

## Execution Topology Status and Capacity Gate

Java’s important behavior is the dependency graph, not the Disruptor class
names:

```text
grouping/journaling
        |
        v
R1 on every risk engine -> matching on every matching engine -> results
        ^                                               |
        |                                               v
        +------------- previous-group R2 ---------------+
```

The Zig grouped runtime implements stable slots, explicit stage frontiers,
persistent per-shard workers, previous-group R2, journal-gated pre-R2 result
transfer, two-phase R2 prepare/commit, allocator synchronization, and
deterministic lifecycle/failure barriers. `ExchangeApiSharded` now uses that
runtime by default and bridges global commands, queries, snapshots, reset,
persistence, callbacks, and shutdown through terminal barriers. Explicit
`immediate` mode remains the compatibility pin. Java-default 1x1 orchestration
therefore selects sharded/XCM2; grouped startup inventories XCM1/XCM2 and
returns `LegacyXcm1RequiresImmediateOrMigration` when legacy XCM1 is newest,
while explicit immediate mode can restore it for migration.

Focused topology tests and the 2255-test repository gate verify the default
cutover. The four-trial orchestrator A/B produced canonical fingerprint
`115306a07a302c98` in all 16 rows and passed all eight persistent 1x1/4x2
throughput and latency floors. The 1x1 grouped medians were 523096.04 settled
commands/s, 785795.70 detached commands/s, 32896 ns terminal-drain p50, and
11229 ns report p50; 4x2 medians were 171107.15, 232996.80, 52521 ns, and
11458.5 ns respectively. This is a reproducible same-host Zig capacity gate, not a
cross-host Java performance claim. See
[`ORCHESTRATOR_AB_BENCHMARK_REPORT_2026-07-31.md`](ORCHESTRATOR_AB_BENCHMARK_REPORT_2026-07-31.md).

## Persistence Boundary

Native XCM2/XCJ3 is the live Zig boundary. Direct Chronicle/LZ4 startup is
intentionally absent, while offline `.ecs`/`.ecj` conversion is mandatory
migration scope. See `PERSISTENCE_INTEROP_DECISION.md` and
`LEGACY_PERSISTENCE_ADAPTER_SPIKE.md` for the current implementation and
evidence limits. XCJ3 durability/crash recovery, exact Eclipse layout-history
persistence through XMB4/XRB4, and XCM2 custom-report configuration-digest
binding are implemented. Legacy XCS/XME/XRE/XCM paths reject a non-empty custom
registry rather than silently loading incompatible state. Legacy XCM1 is also
recognized during grouped discovery: if it is the newest committed candidate,
startup fails with the typed pin-or-migrate error rather than silently starting
empty.

Authoritative Chronicle-generated persistence evidence is current through
workspace-local Temurin 11.0.32+9. Exact JVM identity remains an explicit
capability boundary because Java identity hashes vary by process; callers
supply `JavaIdentityHashContext`, and exact requests fail closed without it.
True journal persistence positions capture only the queue prefix present at
the boundary; concurrent suffix work, including `{2,2}`, is excluded and
covered in persistent-grouped tests. Custom serializer callbacks preserve
end-of-batch markers but execute serially rather than with Java module-thread
parallelism.

## Completion Evidence

Processor parity is closed with observable behavior verified across:

- Java and Zig command results, event order, balances, positions, order books,
  reports, grouping behavior, and persistence recovery;
- 1x1 and asymmetric matching/risk shard topologies;
- Debug and ReleaseSafe focused tests plus repository-wide build/gate targets;
- authoritative Java-generated fixtures using a compatible JDK;
- deterministic concurrency, allocation-failure, I/O-failure, and crash-window
  schedules;
- all 91 Java production files in the executable class map.

Those gates are current. JVM object factories, JVM identity values, and direct
Chronicle loading remain explicit native/capability boundaries rather than
unimplemented processor behavior.
