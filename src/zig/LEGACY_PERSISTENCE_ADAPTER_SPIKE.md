# Java Persistence Conversion Boundary

Updated: 2026-07-31

This file began as a phase-0/1 adapter spike. The implementation has since
advanced into dedicated snapshot and journal converters, so this document now
records the completed conversion boundary and its release evidence.

## Purpose

Convert supported Java Chronicle/LZ4 persistence families into native Zig
artifacts without adding a legacy branch to runtime startup. Conversion targets
semantic equivalence: restored users, balances, positions, symbol state, order
books, command results, and publication frontiers must match. It does not target
byte-identical Java output.

## Architecture

### Shared legacy preflight

`core/legacy_persistence_adapter.zig` owns legacy magic classification, request
preflight, LZ4 frame metadata validation, bounded snapshot decompression, and a
typed failure taxonomy.

The obsolete `convertLegacyPersistenceArchives` umbrella placeholder was
removed. Its request shape could not carry the topology, snapshot frontier,
sequence policy, or destination metadata needed for a safe conversion. Callers
must use the dedicated snapshot-family and journal-family converters below,
whose typed requests make those authorities explicit.

### Snapshot-family conversion

- `core/java_snapshot_wire.zig` reads Java primitive/object wire values.
- `core/java_snapshot_body.zig` decodes matching and risk module bodies.
- `core/java_snapshot_import.zig` validates and stages decoded modules.
- `core/java_persistence_converter.zig` requires the complete expected module
  family and atomically advertises an `XCM2` bundle after staged import and
  verifies the result through native XCM2 read-back.

The runtime then restores that bundle through the normal XCM2 path.

### Journal-family conversion

- `core/java_journal_wire.zig` decodes partition contents with explicit limits.
- `core/java_journal_replay.zig` replays records against staged kernels and
  computes command results while enforcing the selected sequence policy. Java
  journal records do not contain result codes to compare.
- `core/java_journal_converter.zig` validates the ordered family and publishes
  appendable `XCJ3` after staged byte-identity verification. The explicit
  sparse compatibility policy preserves each retained Java sequence, allowing
  the live appender to continue at the final frontier plus one without
  fabricating omitted commands.

Java journals omit non-mutating publications, so sparse records do not prove a
terminal high-water mark. Conversion must receive or derive that authority from
a validated persistence boundary; it must never guess it from the final record.

## Runtime Boundary

Normal startup remains single-format:

- current sharded committed snapshots use `XCM2`;
- current live durable journals use `XCJ3`;
- compatibility XCS/XCJ readers reject legacy LZ4 input with
  `error.UnsupportedLegacyLz4Format`, while XCM2 treats wrong manifest magic as
  invalid native input;
- conversion is performed offline and produces data accepted by the ordinary
  native restore/replay implementation.

## Failure Model

The boundary must fail without publishing partial output for:

- unknown, truncated, unsupported, or checksum-invalid LZ4 frames;
- decompression or collection sizes above configured limits;
- missing, duplicate, swapped, or topologically inconsistent modules;
- missing or invalid matching/risk persistence barriers;
- journal gaps without an explicit preservation policy and trusted frontier;
- records inconsistent with the caller-supplied execution mode, gap policy, or
  post-snapshot sequence high-water;
- target collisions, symbolic-link targets, ambiguous writes, failed syncs, or
  failed native read-back.

Conversion errors after staged replay must not leak a partially mutated live
kernel. Failures before publication must leave the destination absent or leave
a prior committed artifact unchanged. A failure after the atomic link but
before parent-directory sync is ambiguous: the new path can already be visible,
so the converter must report the ambiguity and callers must not retry blindly.

## Verification Status

The modules have focused Debug and ReleaseSafe coverage for supported layouts,
corruption, limits, allocation failures, replay policy failures, atomic
publication, native read-back, and Java persistence-barrier validation. A
checksummed Chronicle-generated `.ecs`/`.ecj` family is committed under
`fixtures/java-persistence/v1` and is regenerated and recovered by the
compatible-JDK Java fixture gate.

The authoritative Java fixture is generated with workspace-local Temurin
11.0.32+9 because the pinned Chronicle dependency is incompatible with the
system JDK 25. Two separate JVM invocations produced the committed
byte-identical `.ecs`/`.ecj` family, recovered snapshot-only and
snapshot-plus-journal business state in Java, and matched the portable
manifest. Zig converted and recovered the same bytes in Debug and ReleaseSafe.

## Completion Evidence

1. A real 1ME/1RE Chronicle snapshot and journal family is generated and
   recovered by Java; synthetic fixtures cover asymmetric topology, multiple
   partitions, sparse sequences, corruption, and allocation failure.
2. Dedicated converters have no test-only source-format exceptions.
3. Converted XCM2/XCJ3 state is restored through ordinary native readers and
   checked against Java business state and command results.
4. Focused tests exercise torn tails, advertisement/adoption ambiguity,
   collision policy, retry safety, and directory-sync boundaries.
5. Converter tests pass 9/9 snapshot and 15/15 journal cases in both Debug and
   ReleaseSafe; the aggregate gate passes 26/26 steps and 2255/2255 tests.
