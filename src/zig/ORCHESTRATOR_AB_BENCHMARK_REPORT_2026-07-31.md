# Orchestrator A/B capacity gate — 2026-07-31

This is current same-host Zig semantic and capacity evidence for the
persistent-grouped runtime. It is not a cross-host Java performance comparison.

## Source and environment

- Zig: `0.16.0`
- Build mode: `ReleaseFast`
- Host architecture: arm64
- OS: macOS 26.6 (build 25G72), Darwin 25.6.0
- Source commit: `46933667d0c6123b6475f6e5e4ec0d945c115249`
- Branch: `feat/zig-port`
- Raw capture: `ORCHESTRATOR_AB_RAW_2026-07-31.txt`
  (`sha256:5bcc032ef532506a1288e2cca5638c9ce3a6350046d8ab9b70c7822705235035`)

Command:

```sh
zig build -j1 -Doptimize=ReleaseFast bench-orchestrator-ab \
  --cache-dir /tmp/exchange-core-orchestrator-46933667-cache \
  --global-cache-dir /tmp/exchange-core-orchestrator-46933667-global -- \
  --trials 4 \
  --commands 20000 \
  --detached-commands 20000 \
  --latency-samples 500 \
  --warmup 256 \
  --window 256 \
  --memory-probe-commands 512
```

The ReleaseFast capacity invocation passed 3/3 build steps. All 16
counterbalanced timing rows produced canonical fingerprint
`115306a07a302c98`, all 8 persistent capacity rows passed their executable
1x1/4x2 throughput and latency floors, and tracked requested bytes returned to
zero after deinit in every scenario.

## Persistent-grouped four-trial medians

Medians are the mean of the middle two per-trial values.

| Topology | Settled commands/s | Detached commands/s | Terminal drain p50 | Report p50 |
| --- | ---: | ---: | ---: | ---: |
| 1x1 | 636575.92 | 1005495.13 | 32792 ns | 11229.5 ns |
| 4x2 | 189886.68 | 281443.53 | 54979 ns | 11500 ns |

The checked floors remain deliberately below the observed medians to leave
room for local scheduling variance:

| Topology | Settled commands/s | Detached commands/s | Terminal p50 | Report p50 |
| --- | ---: | ---: | ---: | ---: |
| 1x1 | >= 300000 | >= 450000 | <= 100000 ns | <= 25000 ns |
| 4x2 | >= 150000 | >= 180000 | <= 150000 ns | <= 25000 ns |

## R2 group-boundary discovery

The same source commit replaces repeated forward discovery of a closed
`events_group` boundary with one allocation-free monotonic cache. For a group
closed by the following unequal command, the old implementation inspected
`G(G+1)/2` adjacencies across R2 sequences; the cache inspects each adjacency
once and reuses the closed boundary:

| Group size | Previous probes | Cached probes | Probe reduction |
| ---: | ---: | ---: | ---: |
| 1 | 1 | 1 | 1.0x |
| 16 | 136 | 16 | 8.5x |
| 256 | 32896 | 256 | 128.5x |
| 4096 | 8390656 | 4096 | 2048.5x |

The counts are exact deterministic test assertions, not timing estimates. A
live `maxInt(i64), maxInt(i64), minInt(i64)` runtime flow also records exactly
two adjacency probes despite worker wakeups and terminal shutdown. Existing
persistent-grouped lifecycle tests cover frontier adoption, quiescence,
poison, partial-start rollback, and slot reuse; the full serial gate passed
43/43 steps and 2305/2305 tests.

## Interpretation and boundary

The fingerprint proves covered-workload equivalence among the benchmark's
immediate and persistent-grouped 1x1/4x2 scenarios. The capacity rows measure
the current Zig implementations on this host; they do not establish Java
throughput parity on another runtime or machine. Native worker factories,
thread scheduling, allocator behavior, and runtime representations remain
Zig-owned.

The refreshed medians are higher than the earlier same-host snapshot, but the
runs were not interleaved against the old implementation. The deterministic
probe counts establish the algorithmic reduction; the timing rows establish
that the current implementation remains above its executable capacity floors,
not a causal percentage improvement attributable solely to the cache.

Persistent grouped execution preserves Java-compatible same-group R1
lookahead, cross-group matching/R2 barriers, journal-before-result ordering,
delayed R2, fatal suffix suppression, and terminal quiescence. Custom
serializer callbacks preserve end-of-batch boundaries, but callback execution
is serialized rather than using Java's per-module thread parallelism.

The 2026-07-23 exploratory report is superseded by this capacity gate. The
2026-07-24 report remains historical evidence for its archived dirty-worktree
snapshot.
