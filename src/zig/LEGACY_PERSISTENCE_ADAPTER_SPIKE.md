# Legacy Persistence Adapter Spike (Phase 0-1)

Date: 2026-02-13  
Scope: optional Chronicle/LZ4 -> Zig persistence import boundary

## Purpose

Prepare a safe, bounded adapter path for legacy persistence import without changing Zig runtime readers.

This spike does not implement conversion. It currently implements:
- boundary types,
- input preflight classification,
- legacy LZ4 frame-header parsing skeleton with typed metadata/errors,
- explicit `NotImplemented` conversion behavior.

## Runtime boundary

Runtime readers remain authoritative and unchanged:
- `core/snapshot.zig` reads native `XCS1`/`XCS2`.
- `core/journal.zig` reads native `XCJ1`.
- legacy LZ4 inputs still fail fast with `error.UnsupportedLegacyLz4Format`.

No dual-format startup behavior is introduced.

## Phase 0 Deliverables

Implemented module:
- `src/zig/core/legacy_persistence_adapter.zig`

Key API:
- `detectMagic(magic_prefix)` -> classifies `XCS1`/`XCS2`/`XCJ1`/legacy-LZ4/unknown/truncated
- `probeFileMagic(dir, path)` -> typed file-level magic probe
- `preflightLegacyImport(dir, request)` -> validates import request and enforces legacy-LZ4 inputs
- `convertLegacyPersistenceArchives(...)` -> explicit `error.NotImplemented` after successful preflight

Typed failure taxonomy:
- request validation failures (`EmptyOutputFolder`, `MissingInputArchive`)
- probe I/O failures (`OpenError`, `ReadError`)
- preflight format mismatches (`SnapshotArchiveNotLegacyLz4`, `JournalArchiveNotLegacyLz4`)
- conversion placeholder (`NotImplemented`)

## Phase 1 Deliverables

Implemented parser skeleton in the same module:
- `parseLegacyLz4FrameHeader(header_prefix)`
- `parseLegacyLz4FileHeader(dir, path)`

Header metadata model:
- standard frame descriptor metadata (`version`, checksums/presence flags, block max-size code, header lengths)
- skippable frame metadata (`user_magic_tag`, payload length)

Typed parser errors:
- malformed magic (`InvalidLegacyMagic`)
- truncation (`TruncatedFrameMagic`, `TruncatedStandardHeaderDescriptor`, `TruncatedSkippableHeader`)
- unsupported/invalid descriptor bits (`UnsupportedFrameVersion`, `ReservedFlagBitSet`, `ReservedBlockDescriptorBitsSet`, `UnsupportedBlockMaxSizeCode`)

## Why this shape

1. Safety
- conversion logic is isolated from runtime bootstrap paths.
- preflight rejects wrong format early and explicitly.

2. Determinism
- existing startup/replay behavior remains unchanged.
- import concerns are moved to an offline boundary.

3. Developer experience
- future adapter work has a concrete entrypoint and typed errors.
- trigger-gated work can start without reopening persistence design decisions.

## Next phases (only if externally required)

1. Phase 2: semantic conversion
- map parsed archive contents into native Zig snapshot/journal writers.
- enforce sequence/result invariants during rewrite.

2. Phase 3: fixture-backed equivalence
- run converted outputs through existing startup/replay and report checks.
- compare state hash and business-level report outcomes.

3. Phase 4: operational wrapping
- package as an explicit offline command/tool.
- keep runtime core readers unchanged.

## Trigger gate reminder

Advance past phase 1 only when all are true:
1. concrete producer/consumer pair exists,
2. artifact set is bounded,
3. migration objective is explicit (one-time import vs recurring ingestion),
4. acceptance owner agrees on semantic equivalence criteria.
