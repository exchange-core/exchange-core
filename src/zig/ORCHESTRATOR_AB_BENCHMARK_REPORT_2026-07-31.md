# Orchestrator A/B capacity gate — 2026-07-31

This is current same-host Zig semantic and capacity evidence for the
persistent-grouped runtime. It is not a cross-host Java performance comparison.

## Source and environment

- Zig: `0.16.0`
- Build mode: `ReleaseFast`
- Host architecture: arm64
- OS: macOS 26.6 (build 25G72), Darwin 25.6.0
- Base Git HEAD: `67725b35689ba3edd54e68c762cdd963c33b8a16`
- Branch: `feat/zig-port`
- Snapshot: dirty working tree identified by
  `JAVA_ZIG_PORT_SOURCE_2026-07-31.md`; the base commit alone is not the tested
  source identity

Command:

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

The ReleaseFast capacity invocation passed 3/3 build steps. All 16
counterbalanced timing rows produced canonical fingerprint
`115306a07a302c98`, all 8 persistent capacity rows passed their executable
1x1/4x2 throughput and latency floors, and tracked requested bytes returned to
zero after deinit in every scenario.

## Persistent-grouped four-trial medians

Medians are the mean of the middle two per-trial values.

| Topology | Settled commands/s | Detached commands/s | Terminal drain p50 | Report p50 |
| --- | ---: | ---: | ---: | ---: |
| 1x1 | 523096.04 | 785795.70 | 32896 ns | 11229 ns |
| 4x2 | 171107.15 | 232996.80 | 52521 ns | 11458.5 ns |

The checked floors remain deliberately below the observed medians to leave
room for local scheduling variance:

| Topology | Settled commands/s | Detached commands/s | Terminal p50 | Report p50 |
| --- | ---: | ---: | ---: | ---: |
| 1x1 | >= 300000 | >= 450000 | <= 100000 ns | <= 25000 ns |
| 4x2 | >= 150000 | >= 180000 | <= 150000 ns | <= 25000 ns |

## Interpretation and boundary

The fingerprint proves covered-workload equivalence among the benchmark's
immediate and persistent-grouped 1x1/4x2 scenarios. The capacity rows measure
the current Zig implementations on this host; they do not establish Java
throughput parity on another runtime or machine. Native worker factories,
thread scheduling, allocator behavior, and runtime representations remain
Zig-owned.

Persistent grouped execution preserves Java-compatible same-group R1
lookahead, cross-group matching/R2 barriers, journal-before-result ordering,
delayed R2, fatal suffix suppression, and terminal quiescence. Custom
serializer callbacks preserve end-of-batch boundaries, but callback execution
is serialized rather than using Java's per-module thread parallelism.

The 2026-07-23 exploratory report is superseded by this capacity gate. The
2026-07-24 report remains historical evidence for its archived dirty-worktree
snapshot.
