# Java-to-Zig port source snapshot — 2026-07-31

This manifest identifies the dirty working-tree snapshot used for the current
Java-to-Zig parity, persistence, and capacity evidence.

## Source identity

- Repository: `exchange-core`
- Branch: `feat/zig-port`
- Base Git HEAD: `67725b35689ba3edd54e68c762cdd963c33b8a16`
- Snapshot type: dirty working tree, including tracked and untracked port
  source and evidence artifacts
- Archive: `JAVA_ZIG_PORT_SOURCE_2026-07-31.tar.gz`
- Archive path count: `364`
- Checksum: external `JAVA_ZIG_PORT_SOURCE_2026-07-31.tar.gz.sha256` sidecar

The worktree contains the port beyond the base commit. The archive, dated
Markdown manifest, and external checksum sidecar together identify the tested
source; the base commit alone does not.

## Archive scope

The pre-filter candidate path set is reported by:

```sh
git ls-files -co --exclude-standard
```

The archive applies the exclusions below to that candidate set and includes
the resulting tracked and untracked source, fixtures, and dated Markdown
manifests, including this file. Packaging excludes:

- `.dex/` task metadata;
- `.zig-*` local/global compiler caches;
- `target/`, `zig-out/`, `dumps/`, and `api-path-builders/` generated outputs;
- every `JAVA_ZIG_PORT_SOURCE_*.tar.gz` archive and
  `JAVA_ZIG_PORT_SOURCE_*.tar.gz.sha256` sidecar.

The checksum remains external so the archive never contains its own digest.
Earlier dated Markdown manifests remain included because they document their
own archived snapshots.

## Verification represented by this snapshot

- Zig 0.16.0 serial gate: 26/26 build steps and 2255/2255 tests, comprising
  1117 Debug, 1117 ReleaseSafe, and 21 evidence-tool tests.
- Default parity fixture manifest: exactly 96/96 committed fixtures; the gate
  always runs the default manifest and cannot be narrowed by build arguments.
- ExchangeApi surface inventory: 24 Java methods, 207 Zig methods, 24 shared,
  0 Java-only, and 183 Zig-only.
- ExchangeApi semantic audit: 24/24 scoped rows, each checked by locating the
  exact cited test body and validating its direct-call token.
- Production map: 91 exact Java paths with verified owners, statuses, and
  responsibility evidence (82 closed, 8 intentional native boundaries, and 1
  intentional capability boundary).
- Temurin 11.0.32+9 `mvn test`: 174 tests, 0 failures, 0 errors, and 10 skipped
  opt-in fixture writers.
- Strict public report and persistence writers: 2/2 with
  `parity.fixture.diagnostic.allow_internal_fallback` unset. The diagnostic
  fallback is fail-closed unless that property is explicitly `true`.
- Two separate JVM persistence writer/recovery runs passed and matched the
  committed artifacts.
- Zig snapshot and journal converters passed 9/9 and 15/15 focused cases in
  both Debug and ReleaseSafe and recovered the committed Java artifacts through
  ordinary XCM2/XCJ3 readers.
- ReleaseFast capacity gate: 3/3 build steps, fingerprint
  `115306a07a302c98` in all 16 rows, 8/8 persistent capacity rows, and zero
  tracked requested bytes after every deinit.
- macOS 26.6 (build 25G72), Darwin 25.6.0 arm64.

The aggregate gate's persistence-disabled and injected-fault warnings are
expected negative-path test evidence, not failures.

## Explicit evidence boundaries

- Exact StateHash requires a caller-supplied `JavaIdentityHashContext` and
  fails closed without it.
- Chronicle/LZ4 interoperability is an offline conversion into XCM2/XCJ3, not
  a second live startup format.
- Fixture `zig_risk_low32` and `zig_detailed_low32` values are Zig low-32-bit
  projections supplied to Java fixture generation. They are not independent
  Java identity hashes.
- Custom serializer callbacks preserve end-of-batch boundaries but are
  serialized rather than parallelized across Java module threads.
- JVM object factories, thread scheduling, allocator behavior, and runtime
  representations are native boundaries.
- The capacity report is same-host Zig evidence, not cross-host Java
  performance evidence.

See `ORCHESTRATOR_AB_BENCHMARK_REPORT_2026-07-31.md` for the measured capacity
medians and threshold interpretation.
