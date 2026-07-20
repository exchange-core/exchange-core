# Processor Ecosystem Parity Decomposition (Java -> Zig)

Date: 2026-02-11
Scope: `src/main/java/exchange/core2/core/processors/**` -> `src/zig/core/**`

This artifact maps Java processor responsibilities to current Zig ownership.
It is a responsibility decomposition, not a strict class-for-class 1:1 requirement.

## Responsibility Matrix

| Java processor class | Zig ownership | Status | Notes |
| --- | --- | --- | --- |
| `BinaryCommandsProcessor` | `src/zig/core/binary_frames.zig`, `src/zig/core/binary_payload.zig`, `src/zig/core/binary_commands.zig`, `src/zig/core/exchange_kernel.zig` | Ported (decomposed) | Java stateful transfer assembly + decode/report routing is split into explicit frame assembler, payload codec, and kernel dispatch. |
| `RiskEngine` | `src/zig/core/risk.zig`, `src/zig/core/exchange_kernel.zig` | Ported (core flow) | Pre-process, hold/release, fees/adjustments/suspends, and risk hash paths are implemented in Zig risk + kernel orchestration. |
| `UserProfileService` | `src/zig/core/user_profile.zig`, `src/zig/core/risk.zig` | Ported | User lifecycle, balances, and suspension semantics are owned by user-profile + risk flows. |
| `MatchingEngineRouter` | `src/zig/core/exchange_kernel.zig`, `src/zig/core/exchange_core_sharded.zig`, `src/zig/core/exchange_api_sharded.zig`, `src/zig/core/exchange_orchestrator.zig` | Ported (baseline) | Symbol/uid routing and sharded execution are implemented across kernel-set + sharded wrappers + orchestrator backend selection. |
| `GroupingProcessor` | `src/zig/core/exchange_runtime.zig` | Ported | Runtime batching, grouping-control behavior, and deterministic drain sequencing are implemented in runtime queue processing. |
| `ResultsHandler` | `src/zig/core/exchange_runtime.zig`, `src/zig/core/exchange_api.zig`, `src/zig/core/simple_events_processor.zig` | Ported (decomposed) | Java results-consumer gating is represented by runtime sink wiring and command/event publication helpers. |
| `SimpleEventHandler` | `src/zig/core/simple_events_processor.zig` | Ported | Java callback/event projection semantics are implemented and wired through API/core registration points. |
| `SymbolSpecificationProvider` | `src/zig/core/risk.zig`, `src/zig/core/common.zig` | Ported | Symbol spec registration/lookup and symbol-state hashing are modeled in risk/provider abstractions. |
| `ISerializationProcessor` | `src/zig/core/journal.zig`, `src/zig/core/snapshot.zig`, `src/zig/core/persistence_descriptors.zig`, `src/zig/core/persistence_paths.zig`, `src/zig/core/exchange_core.zig`, `src/zig/core/exchange_api_sharded.zig` | Ported (decomposed) | Java interface behavior is represented as explicit snapshot/journal/path/descriptor primitives and startup/replay flows. |
| `DiskSerializationProcessor` | `src/zig/core/journal.zig`, `src/zig/core/snapshot.zig`, `src/zig/core/exchange_core.zig`, `src/zig/core/exchange_api_sharded.zig` | Ported (baseline) | Disk-backed snapshot/journal + replay/bootstrap behaviors are covered in Zig persistence paths. Compression format is not a direct Chronicle/LZ4 1:1 port. |
| `DummySerializationProcessor` | No direct class; represented by config-driven no-op/disabled paths in `src/zig/core/config.zig`, `src/zig/core/exchange_core.zig`, `src/zig/core/exchange_api_sharded.zig` | Consolidated | Zig uses capability flags and error paths instead of a separate dummy implementation class. |
| `DiskSerializationProcessorConfiguration` | `src/zig/core/config.zig` + `src/zig/core/persistence_paths.zig` | Ported (policy shape) | Storage folder and persistence-policy controls are represented in typed Zig config. |
| `JournalDescriptor` | `src/zig/core/persistence_descriptors.zig` | Ported | Descriptor graph modeled explicitly in Zig with snapshot linkage and sequence bounds. |
| `SnapshotDescriptor` | `src/zig/core/persistence_descriptors.zig` | Ported | Snapshot lineage + journal adjacency are represented in descriptor store. |
| `DisruptorExceptionHandler` | No direct class; surfaced through Zig error unions and call-site handling | Consolidated | Zig execution model avoids Disruptor exception callback infrastructure. |
| `WaitSpinningHelper` | `src/zig/core/exchange_runtime.zig` (deterministic queue drain policy), `src/zig/core/config.zig` (`CoreWaitStrategy`) | Consolidated | Wait strategy exists as config surface; low-level Disruptor spinning/blocking mechanics are intentionally not ported 1:1. |
| `TwoStepMasterProcessor` | No direct class; responsibilities collapsed into runtime + API drain sequencing (`src/zig/core/exchange_runtime.zig`, `src/zig/core/exchange_api.zig`) | Consolidated | Two-step pipeline is replaced by explicit queue/drain and deterministic per-command execution flow. |
| `TwoStepSlaveProcessor` | No direct class; responsibilities collapsed into runtime + API drain sequencing (`src/zig/core/exchange_runtime.zig`, `src/zig/core/exchange_api.zig`) | Consolidated | Same as master/slave split: no direct Disruptor worker pair in Zig architecture. |
| `SharedPool` | No direct class; allocation lifecycle is explicit via allocator ownership and command response/event-chain handling in kernel/runtime | Consolidated | Zig model uses allocator-managed ownership patterns rather than pooled Java object chains. |

## Remaining Explicit Deltas

1. Chronicle/LZ4 persistence format parity is not implemented as a byte-identical port.
- Policy is now explicit in `src/zig/PERSISTENCE_INTEROP_DECISION.md`: native Zig persistence format is authoritative, and loaders return explicit `UnsupportedLegacyLz4Format` for legacy LZ4-framed inputs.

2. Disruptor master/slave and wait-spinning internals are not ported as infrastructure classes.
- Current Zig runtime preserves behavioral goals through deterministic queue/drain orchestration and config controls.

3. Processor-level decomposition is complete at ownership level, but some responsibilities are intentionally consolidated.
- Behavior-level regression hardening is now in place for consolidated runtime and orchestrator persistence-wrapper paths; additional expansion should remain behavior-driven rather than class cloning.

## Verification Commands

```sh
zig build test
zig build gate
```
