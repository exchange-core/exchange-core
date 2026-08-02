# Java-to-Zig port source snapshot — 2026-07-24

This manifest identifies the exact source snapshot used for the final
Java-to-Zig parity and capacity evidence.

## Source identity

- Repository: `exchange-core`
- Branch: `feat/zig-port`
- Base Git HEAD: `67725b35689ba3edd54e68c762cdd963c33b8a16`
- Snapshot type: working-tree source archive, including tracked files and
  untracked port source/artifacts
- Archive: `JAVA_ZIG_PORT_SOURCE_2026-07-24.tar.gz`
- Checksum: external
  `JAVA_ZIG_PORT_SOURCE_2026-07-24.tar.gz.sha256` sidecar

The port was completed as an existing dirty working-tree workstream. The
archive is the reconstructable source identity for the evidence below; the
base commit alone is not.

## Archive scope

The archive contains the 362 tracked and untracked source/artifact paths
reported by:

```sh
git ls-files -co --exclude-standard
```

It excludes only:

- `.dex/` task metadata;
- `.zig-*` local/global compiler caches;
- `target/`, `zig-out/`, `dumps/`, and `api-path-builders/` generated outputs;
- the archive and checksum sidecar themselves.

The source manifest is included in the archive. The checksum sidecar is
external to avoid a self-referential digest.

## Final verification

- `zig fmt --check src/zig build.zig`: passed
- `git diff --check`: passed
- compatible Temurin 11.0.31+11 `mvn test`: 172 tests, 0 failures, 0 errors,
  10 intentionally skipped opt-in fixture writers
- Java persistence writer: passed in two separate JVM processes; each run
  generated two byte-identical artifact families and recovered snapshot-only
  and snapshot-plus-journal state in Java
- committed Java persistence fixture through Zig conversion/readback: 4/4
  Debug and 4/4 ReleaseSafe
- serialized `zig build -j1 gate --summary all`: 13/13 steps and 2216/2216
  tests, comprising 1108/1108 Debug and 1108/1108 ReleaseSafe plus fixture
  diff, API surface check, Java API parity audit, and the executable 91-class
  production-map check
- ReleaseFast capacity gate: all 16 timing rows produced canonical fingerprint
  `115306a07a302c98`; all eight persistent 1x1/4x2 capacity rows passed
- final ReleaseFast smoke: semantic fingerprint `7c806cf6036ecfad` matched
  across immediate/grouped and 1x1/4x2

The aggregate-gate persistence-disabled and injected-fault warnings are
expected test evidence, not failures.

## Capacity command

```sh
zig build -Doptimize=ReleaseFast bench-orchestrator-ab \
  --cache-dir .zig-cache-capacity-gate \
  --global-cache-dir .zig-global-cache-capacity-gate -- \
  --trials 4 \
  --commands 20000 \
  --detached-commands 20000 \
  --latency-samples 500 \
  --warmup 256 \
  --window 256 \
  --memory-probe-commands 512
```

See `ORCHESTRATOR_AB_BENCHMARK_REPORT_2026-07-24.md` for the checked floors,
raw rows, medians, and memory accounting.

## Java persistence artifact digests

- matching snapshot:
  `ae8b93d2ffe6515a3eebbcf08d62168f2ccc45ffeabd1b118dc216093db43562`
- risk snapshot:
  `659d8e8216695524122e3691aebafdb0e772d10865be4687c3876b0070091f7d`
- raw journal:
  `2666e245529c3f382531fa7d2918b9c59ea9842a961cc1443b72d1d90b11033f`

The portable fixture manifest excludes `.eca` wall-clock metadata and
process-local JVM enum/object identity hashes. Each Java process still checks
its own identity hashes across persistence recovery.
