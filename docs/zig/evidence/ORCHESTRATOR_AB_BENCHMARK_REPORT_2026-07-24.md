# Orchestrator A/B capacity gate — 2026-07-24

This is local performance and semantic evidence for the persistent-grouped
runtime after replacing per-command all-shard R1/matching/R2 condition
rendezvous with stable stage coordinators. It is a reproducible local capacity
gate, not a claim of Java benchmark parity on different hardware.

## Source and environment

- Zig: `0.16.0` (Homebrew `0.16.0_1`)
- Build mode: `ReleaseFast`
- Host: Apple M4 Mac mini, 10 cores, 16 GB RAM, arm64
- OS: macOS 26.5.2
- Git HEAD: `67725b35689ba3edd54e68c762cdd963c33b8a16`
- Exact worktree source archive:
  `docs/zig/evidence/JAVA_ZIG_PORT_SOURCE_2026-07-24.tar.gz` with its SHA-256 sidecar

Command:

```sh
zig build -Doptimize=ReleaseFast bench-orchestrator-ab -- \
  --trials 4 \
  --commands 20000 \
  --detached-commands 20000 \
  --latency-samples 500 \
  --warmup 256 \
  --window 256 \
  --memory-probe-commands 512
```

The harness starts from `JavaExchangeConfig.javaDefault()`, alternates
immediate-first and grouped-first trial order, and changes only topology plus
the explicit pipeline mode. Every timing row produced canonical fingerprint
`115306a07a302c98`; mode equivalence, topology equivalence, memory cleanup, and
all eight persistent capacity rows passed.

The checked ReleaseFast floors are intentionally below the four-trial medians
to leave local scheduling headroom:

| Topology | Settled commands/s | Detached commands/s | Terminal p50 | Report p50 |
| --- | ---: | ---: | ---: | ---: |
| 1x1 | >= 300,000 | >= 450,000 | <= 100,000 ns | <= 25,000 ns |
| 4x2 | >= 150,000 | >= 180,000 | <= 150,000 ns | <= 25,000 ns |

These correspond to fixed immediate-reference targets of at least 30%/20% for
1x1 settled/detached and 15%/10% for 4x2 settled/detached. The benchmark fails
immediately when any non-smoke persistent row misses a floor. `--smoke`
continues to enforce semantic fingerprints but marks capacity not applicable.

## Four-trial medians

Medians are the mean of the middle two per-trial values.

| Topology | Mode / backend | Workers | Startup ns | Terminal drain p50 ns | Report p50 ns | Settled commands/s | Detached commands/s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1x1 | immediate / single | 1 | 11,667 | 3,312 | 167 | 1,332,283 | 2,410,885 |
| 1x1 | persistent_grouped / sharded | 3 | 687,063 | 33,376 | 9,792 | 454,308 | 627,739 |
| 4x2 | immediate / sharded | 1 | 12,104 | 3,667 | 250 | 1,129,266 | 1,900,816 |
| 4x2 | persistent_grouped / sharded | 7 | 1,000,667 | 72,938 | 10,458 | 179,641 | 279,430 |

Observed grouped/immediate deltas:

- 1x1: grouped settled throughput is 34.1% of immediate and detached
  throughput is 26.0%.
- 4x2, with the sharded backend held constant: grouped settled throughput is
  15.9% of immediate and detached throughput is 14.7%.

Relative to the earlier same-day R1-lookahead checkpoint, grouped settled
throughput improved from 325,169 to 454,308 commands/s for 1x1 (1.40x) and from
15,574 to 179,641 for 4x2 (11.53x). The 4x2 terminal p50 fell from 120,604 ns
to 72,938 ns.

## Memory evidence

| Topology | Mode | Workers | After init | After startup | After setup | After settlement | After shutdown | After deinit |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1x1 | immediate | 1 | 17,468,568 | 17,468,568 | 17,489,560 | 17,493,464 | 17,493,464 | 0 |
| 1x1 | persistent_grouped | 3 | 31,397,400 | 40,967,380 | 40,988,756 | 40,992,660 | 31,422,680 | 0 |
| 4x2 | immediate | 1 | 31,399,440 | 31,399,440 | 31,444,944 | 31,449,040 | 31,449,040 | 0 |
| 4x2 | persistent_grouped | 7 | 31,399,440 | 40,969,936 | 41,015,440 | 41,019,536 | 31,449,040 | 0 |

All scenarios returned to zero live requested bytes after deinit. The requested
heap metric excludes RSS and OS thread stacks.

## Interpretation

Same-group R1 lookahead is now behaviorally aligned with Java:

- all R1 shards still retire each sequence as a prefix;
- same-group R1 may advance while matching is blocked;
- a group transition waits for prior matching and all-risk R2 commit;
- a matching fatal can leave an R1-mutated same-group suffix;
- the poisoned runtime cannot restart or publish the fatal sequence/suffix.

Ordinary R1 and matching work is single-owner by UID/symbol. Stable
coordinators execute that owner and atomically advance the no-op shard
frontiers, eliminating an all-worker condition convoy. R2 still prepares every
risk shard before any commit; its coordinator keeps prepare-all/commit-none
failure atomicity. All configured worker threads remain lifecycle-owned and
stable, while idle non-coordinators block.

Focused Debug and ReleaseSafe tests cover strict R1 failure ordering,
same-group overlap, cross-group blocking, all-shard R2 failure atomicity,
matching-fatal suffix state, terminal non-reuse, worker lifecycle, and
asymmetric state equivalence. The remaining difference from immediate is the
cost of persistent worker ownership and terminal grouped barriers; it is now
bounded by an executable local capacity gate.
