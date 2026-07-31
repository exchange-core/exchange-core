# Persistence Interoperability Decision (Java Chronicle/LZ4 -> Zig)

Updated: 2026-07-31

Scope: Java `DiskSerializationProcessor` `.ecs`/`.ecj` artifacts and the Zig
snapshot, journal, restore, and conversion paths.

## Decision

Zig startup and live append paths use native, explicitly versioned formats.
The current sharded snapshot bundle is `XCM2`, and the current durable journal
append format is `XCJ3`. Older native readers remain only where the code and
tests intentionally preserve backward compatibility.

The live runtime does not parse Chronicle/LZ4 files directly. Existing Java
artifacts cross an offline conversion boundary before Zig startup. That
conversion is a required migration capability for whole-port parity, not an
optional future spike and not byte-for-byte format compatibility.

## Boundary

```text
Java .ecs/.ecj files
  -> bounded LZ4 and wire decoding
  -> Java semantic validation/import or replay
  -> staged native state
  -> native encoding and publication checks
  -> atomic native publication
  -> ordinary Zig startup/restore
```

Keeping conversion outside startup has two useful properties:

- malformed Java input cannot select a second live restore implementation;
- converted output must satisfy the same native readers and invariants as data
  produced by Zig itself.

The compatibility single-kernel XCS and XCJ readers distinguish an unsupported
legacy LZ4 frame from unknown/corrupt native input. The XCM2 manifest decoder is
not a legacy-format probe and reports a wrong manifest magic generically.

## Implemented Conversion Surface

The conversion surface is decomposed rather than hidden behind one generic
adapter function:

- `core/legacy_persistence_adapter.zig` detects and bounds legacy LZ4 input and
  implements the supported snapshot-frame decompression profile;
- `core/java_snapshot_wire.zig`, `core/java_snapshot_body.zig`, and
  `core/java_snapshot_import.zig` decode and import Java snapshot modules;
- `core/java_persistence_converter.zig` converts a complete Java snapshot
  family into an atomically advertised `XCM2` bundle;
- `core/java_journal_wire.zig` and `core/java_journal_replay.zig` decode and
  semantically validate Java journal records;
- `core/java_journal_converter.zig` converts a validated Java journal family
  into appendable `XCJ3`. Under the explicit sparse-sequence policy it retains
  each Java sequence on its next replayable record, so gaps remain visible in
  publication identity while the live appender can continue at the final
  frontier plus one.

The obsolete umbrella `convertLegacyPersistenceArchives` placeholder was
removed because its request did not carry enough authority for safe conversion.
The dedicated snapshot-family and journal-family converters are the only
supported entry points.

## Required Safety Properties

Conversion and native persistence are not considered release-ready unless all
of these hold:

1. Every input length, count, topology, sequence, and decompression expansion
   is bounded before allocation or mutation.
2. A converter validates the complete source family before mutating staged
   state or publishing an output path.
3. Java persistence barriers and snapshot frontiers are validated as a family;
   a torn, reversed, duplicated, or mid-family boundary fails closed.
4. A skipped Java command cannot cause Zig to reuse a published sequence after
   restart. The native journal must durably preserve the logical frontier.
5. Snapshot advertisement and journal adoption form a recoverable boundary. A
   crash must leave either the prior family or the new committed family usable.
6. Files and newly linked parent directories are synced before success is
   exposed.
7. Before the converter is release-ready, converted outputs must be read back
   through native decoders before success is exposed. The destination must
   never be followed or silently replaced, and an ambiguous post-link sync
   failure must be surfaced rather than retried blindly.

## Current Verification Boundary

Focused Zig tests cover the native formats, bounded decoders, semantic import,
complete-family conversion, atomic publication, native readback, restart-safe
continuation, corruption, and allocation-failure cleanup. Snapshot conversion
commits through the XCM2 store and restores the committed bundle through its
ordinary reader. Journal conversion decodes the completed XCJ3 file before
publication and the tests reopen and replay the published file.

The committed `src/zig/fixtures/java-persistence/v1` family was produced twice
through Java `DiskSerializationProcessor` on Temurin 11.0.32+9 and was
byte-identical across the two runs. Its manifest records:

- `JAVAPERSISTV1_snapshot_4242424242_ME0.ecs`:
  `ae8b93d2ffe6515a3eebbcf08d62168f2ccc45ffeabd1b118dc216093db43562`
- `JAVAPERSISTV1_snapshot_4242424242_RE0.ecs`:
  `659d8e8216695524122e3691aebafdb0e772d10865be4687c3876b0070091f7d`
- `JAVAPERSISTV1_journal_4242424242_0001.ecj`:
  `2666e245529c3f382531fa7d2918b9c59ea9842a961cc1443b72d1d90b11033f`

The Java generator proves snapshot-only and snapshot-plus-journal recovery
inside the JVM. Zig tests then convert those exact embedded bytes to XCM2/XCJ3
and assert balances, fees, adjustments, positions, orders, L2 state, sparse
frontiers, actual command results, and native journal readback in Debug and
ReleaseSafe. The Java `.eca` file is intentionally excluded because it contains
wall-clock log metadata; the fixture manifest records that policy explicitly.

## Non-Goals

- No Chronicle/LZ4 branch in normal Zig startup.
- No byte-identical rewrite of Java archives.
- No inference of publication history or a terminal sequence that the Java
  files do not contain; missing authority must be supplied explicitly or fail.
- No claim that old native formats are the current write target merely because
  compatibility readers remain.

## Release Gate — passed

The focused snapshot-family converter passed 9/9 cases and the journal-family
converter passed 15/15 cases in both Debug and ReleaseSafe. The aggregate gate
passed 26/26 steps and 2255/2255 tests (1117 Debug, 1117 ReleaseSafe, and 21
evidence-tool tests), including the exact 96/96 default fixture manifest and
both API audits:

```sh
zig build test
zig build test-release-safe
zig build gate
```

The authoritative fixture was regenerated in two separate Temurin
11.0.32+9 JVM processes, recovered in Java, and converted/read back by Zig in
both optimization modes. Crash-boundary tests are green.

`JournalBatchPosition` is a captured queue-prefix authority, not a later view
of the queue. Concurrent suffix work is excluded; `{2,2}` suffix and
persistent-grouped cases prove that boundary. Custom serializer callbacks keep
end-of-batch markers but remain serialized compared with Java's module-thread
parallelism.
