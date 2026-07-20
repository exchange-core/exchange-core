# Persistence Interoperability Decision (Java Chronicle/LZ4 -> Zig)

Date: 2026-02-11  
Scope: `src/main/java/exchange/core2/core/processors/journaling/*` persistence files vs `src/zig/core/{snapshot,journal}.zig`

## Decision

Zig persistence remains a native format (`XCS1`/`XCS2` snapshots, `XCJ1` journals) and is the authoritative runtime format for the Zig port.

Byte-identical Chronicle/LZ4 persistence compatibility is **not** implemented in core runtime paths.

## Rationale

1. Safety and determinism
- Native Zig formats are typed, deterministic, and validated by `zig build gate` fixture/test workflows.
- Direct Chronicle/LZ4 byte compatibility would add parser/compression complexity and expand failure surface in critical startup/restore paths.

2. Migration economics
- Current migration goals are behavior parity and operational determinism, not legacy byte-stream reuse.
- Existing parity fixtures and startup/replay tests already validate business semantics without requiring byte-identical file interchange.

3. Developer experience
- Explicit format ownership keeps persistence debugging local to Zig modules.
- Avoids hidden dependency on JVM-era serialization/compression internals.

## Runtime Enforcement

To make this decision explicit in code:

- `src/zig/core/snapshot.zig` detects LZ4 frame headers and returns `error.UnsupportedLegacyLz4Format`.
- `src/zig/core/journal.zig` detects LZ4 frame headers and returns `error.UnsupportedLegacyLz4Format`.
- Non-Zig, non-LZ4 unknown inputs continue to return `error.InvalidMagic`.

This distinguishes:
- unsupported legacy format, vs
- corrupted/unknown input.

## Revisit Conditions

Revisit this decision only if one of these becomes a hard requirement:

1. Direct bootstrap from existing Chronicle/LZ4 archives without conversion.
2. Regulatory/audit requirement for byte-level historical interchange with Java artifacts.
3. Cross-runtime hot failover requiring mixed Java/Zig persistence readers.

If revisited, implement as an adapter layer (separate module/build step), not by complicating core runtime readers.

## Deferred Adapter Spike Boundary (If Triggered)

This section defines the bounded spike scope up front so work can start quickly if a trigger appears.

### Phase-0/1 Status (implemented)

The following phase-0/1 scaffolding is now present while keeping runtime readers unchanged:

1. Standalone spike note:
   - `src/zig/LEGACY_PERSISTENCE_ADAPTER_SPIKE.md`
2. Typed offline probe/preflight boundary:
   - `src/zig/core/legacy_persistence_adapter.zig`
   - detects Zig/legacy/unknown/truncated magic,
   - validates legacy import request shape,
   - parses legacy LZ4 standard/skippable header metadata with explicit malformed/truncated/unsupported errors,
   - exposes explicit `error.NotImplemented` conversion placeholder.

### Trigger Gate (all required)

1. A concrete producer/consumer pair is identified (`who` writes Chronicle/LZ4, `who` must read it in Zig).
2. Required artifact set is bounded (snapshot only, journal only, or both; date/partition range).
3. Operational objective is explicit (one-time migration import vs repeated ingestion vs mixed-runtime restore drills).
4. Acceptance owner signs off on semantic-equivalence criteria (not byte-identical rewrite in core runtime).

### In-Scope Spike Outputs

1. A standalone adapter design note (input assumptions, conversion pipeline, failure taxonomy).
2. A prototype converter boundary outside runtime readers:
   - input: Chronicle/LZ4 archive(s),
   - output: native Zig persistence files (`XCS1`/`XCS2`, `XCJ1`) ready for existing startup paths.
3. Deterministic fixture-backed equivalence check on representative archives (same resulting user/report/state-hash behavior after import).

### Out-of-Scope (hard boundaries)

1. No changes to `src/zig/core/snapshot.zig` or `src/zig/core/journal.zig` runtime format readers beyond existing explicit legacy-format rejection.
2. No dual-format branch logic in `ExchangeCore` startup paths.
3. No requirement to preserve Chronicle/LZ4 bytes in rewritten outputs.

### Safety and Verification Guardrails

1. Adapter failures must be explicit and typed (bad magic, truncated archive, unsupported variant, sequence mismatch).
2. Converted outputs must pass existing startup/replay tests without special runtime flags.
3. `zig build test` and `zig build gate` remain green with adapter code disabled by default.

## Verification

```sh
zig build test
zig build gate
```
