# Orchestrator immediate vs persistent-grouped A/B — 2026-07-23

> Superseded for current capacity decisions by
> `ORCHESTRATOR_AB_BENCHMARK_REPORT_2026-07-31.md`. This file is retained as
> historical exploratory evidence only.

This is current local evidence for the grouped-default cutover. It is not a
performance threshold gate and it does not establish production capacity.

## Environment

- Zig: `0.16.0` (Homebrew `0.16.0_1`)
- Build mode: `ReleaseFast`
- Host: Apple M4 Mac mini, 10 cores, 16 GB RAM, arm64
- OS: macOS 26.5.2 (25F84)
- Git HEAD: `67725b35689ba3edd54e68c762cdd963c33b8a16`
- Worktree: dirty, with 114 tracked/untracked status entries from the ongoing
  port. The two hashes below identify the harness and build entrypoint, but no
  complete patch/archive of all imported Zig sources was captured. This run is
  therefore local evidence, not an independently reconstructable source
  snapshot; release evidence must rerun from a clean commit or archived tree.
- `src/zig/bench/orchestrator_ab.zig` SHA-256:
  `9218f30252bc33edec10d70540d26085c08797cc7d07f056e8ff6eb233e6236b`
- `build.zig` SHA-256:
  `2c2a952e23e54b11588102e94234992e3c1b567bc16994192b8c5c51d2b10db1`

Equivalent command with cache locations normalized:

```sh
ZIG_GLOBAL_CACHE_DIR=.zig-cache-global \
ZIG_LOCAL_CACHE_DIR=.zig-cache-orchestrator-ab \
zig build -Doptimize=ReleaseFast bench-orchestrator-ab -- \
  --trials 4 \
  --commands 20000 \
  --latency-samples 500 \
  --warmup 256 \
  --window 256 \
  --memory-probe-commands 512
```

## Method

- Every scenario starts from `JavaExchangeConfig.javaDefault()` and changes
  only matching/risk topology and explicit `sharded_pipeline_mode`.
- Trials alternate immediate-first and grouped-first order.
- The 1x1 comparison includes the intended default backend change: immediate
  uses the single backend, while grouped uses the sharded backend. The 4x2
  comparison isolates pipeline mode within the sharded backend.
- Pre-R2 completion latency measures one NOP through async publication,
  `drainPending`, and result consumption. Fully settled latency additionally
  includes a terminal TotalCurrencyBalance report barrier from the same start.
- Throughput runs paired GTC trades in bounded windows, consumes every result,
  and includes the terminal settlement barrier before stopping the timer.
- The semantic fingerprint canonicalizes order books, users, orders, balances,
  fees, adjustments, suspends, and open interest.
- Memory is live requested heap from a separate `DebugAllocator`, sampled only
  after a terminal barrier. It excludes RSS and OS thread stacks. Worker counts
  are configured expectations because no public aggregate runtime counter
  exists.
- Each trial computes nearest-rank latency percentiles from that trial's own
  samples. The table then takes the median across four per-trial values, using
  the mean of the middle two; samples are not pooled across trials.
- Memory rows come from one separate terminally settled probe per scenario;
  they are not four-trial medians.

## Median results

Medians are the mean of the middle two per-trial values from four
counterbalanced trials.

| Topology | Mode / backend | Expected workers | Startup ns | Pre-R2 p50 ns | Pre-R2 p99 ns | Settled p50 ns | Settled p99 ns | Settled commands/s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1x1 | immediate / single | 1 | 13,417 | 3,729 | 7,375 | 3,896 | 7,875 | 1,763,083 |
| 1x1 | persistent_grouped / sharded | 3 | 820,105 | 13,083 | 26,688 | 30,709 | 46,021 | 98,069 |
| 4x2 | immediate / sharded | 1 | 11,542 | 3,729 | 7,292 | 4,001 | 7,667 | 1,340,523 |
| 4x2 | persistent_grouped / sharded | 7 | 1,074,084 | 55,438 | 91,292 | 104,104 | 153,125 | 13,547 |

Observed grouped/immediate deltas:

- 1x1: 61.1x startup time, 7.9x settled p50 latency, 94.4% lower
  settled throughput, and 27,623,184 additional live requested bytes after
  startup. This includes the single-to-sharded backend change.
- 4x2: 93.1x startup time, 26.0x settled p50 latency, 99.0% lower
  settled throughput, and 17,958,584 additional live requested bytes after
  startup. This comparison keeps the sharded backend constant.

These figures make grouped execution's current performance cost explicit. The
A/B establishes final-state fingerprint equality only for this deterministic
workload; it does not compare Java execution, per-command results/events,
callback order, journal timing, or failure schedules. Focused tests and the full
gate provide the broader default-promotion evidence. None of this makes the
port performance-parity or production-capacity ready. Optimizing persistent
stage scheduling and synchronization remains required before production
migration.

## Memory evidence

| Topology | Mode | Expected workers | After init | After startup | After setup | After settled workload | After shutdown | After deinit |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1x1 | immediate | 1 | 17,468,528 | 17,468,528 | 17,489,520 | 17,493,424 | 17,493,424 | 0 |
| 1x1 | persistent_grouped | 3 | 31,262,160 | 45,091,712 | 45,113,088 | 45,116,992 | 31,287,440 | 0 |
| 4x2 | immediate | 1 | 31,264,200 | 31,264,200 | 31,309,704 | 31,313,800 | 31,313,800 | 0 |
| 4x2 | persistent_grouped | 7 | 31,264,200 | 49,222,784 | 49,268,288 | 49,272,384 | 31,313,800 | 0 |

All scenarios returned to zero live requested bytes after deinit.

## Covered-workload semantic result

All 16 timing rows produced fingerprint `8471257b2d6e7eac`:

```text
semantic_gate=PASS canonical_fingerprint=8471257b2d6e7eac fingerprint_comparisons=15 performance_thresholds=none
```

Here `semantic_gate=PASS` is the harness label for equality of the canonical
final-state fingerprint across the selected immediate/grouped scenarios. It is
not a Java parity verdict.

## Raw timing rows

```csv
trial,run_order,topology,config_mode,backend,matching_shards,risk_shards,expected_driver_workers,expected_matching_workers,expected_risk_workers,expected_owned_workers,init_ns,startup_ns,pre_r2_completion_p50_ns,pre_r2_completion_p95_ns,pre_r2_completion_p99_ns,pre_r2_completion_max_ns,settled_barrier_p50_ns,settled_barrier_p95_ns,settled_barrier_p99_ns,settled_barrier_max_ns,throughput_commands,throughput_settled_elapsed_ns,throughput_settled_commands_per_second,shutdown_ns,fingerprint
0,0,1x1,immediate,single,1,1,1,0,0,1,25292,13000,3542,5917,7084,11209,3750,6167,7666,12292,20000,11351208,1761927.01,16458,8471257b2d6e7eac
0,1,1x1,persistent_grouped,sharded,1,1,1,1,1,3,11917,779125,12791,17750,19667,26166,30834,36833,39417,44042,20000,236340000,84623.85,535250,8471257b2d6e7eac
1,0,1x1,persistent_grouped,sharded,1,1,1,1,1,3,18208,881042,14458,24708,36334,43292,30792,41834,53708,61209,20000,204433625,97831.26,482333,8471257b2d6e7eac
1,1,1x1,immediate,single,1,1,1,0,0,1,10625,21667,3791,5875,7250,12458,3958,6250,7792,12750,20000,11336333,1764238.93,13250,8471257b2d6e7eac
2,0,1x1,immediate,single,1,1,1,0,0,1,6833,9917,3750,6708,8375,48542,3917,6958,8542,48833,20000,12792625,1563400.79,12667,8471257b2d6e7eac
2,1,1x1,persistent_grouped,sharded,1,1,1,1,1,3,10791,801834,13375,19875,33709,47667,30625,39083,51917,75750,20000,203443459,98307.41,474208,8471257b2d6e7eac
3,0,1x1,persistent_grouped,sharded,1,1,1,1,1,3,14167,838375,12750,17833,19583,24833,30625,37000,40125,43083,20000,202722916,98656.83,466791,8471257b2d6e7eac
3,1,1x1,immediate,single,1,1,1,0,0,1,10250,13834,3708,6042,7500,10500,3875,6375,7958,10750,20000,11035709,1812298.60,7708,8471257b2d6e7eac
0,0,4x2,immediate,sharded,4,2,1,0,0,1,13584,11084,3792,5917,7542,12500,4084,6417,7834,12750,20000,14461292,1383002.29,11958,8471257b2d6e7eac
0,1,4x2,persistent_grouped,sharded,4,2,1,4,2,7,12292,957166,55250,82333,92917,122667,104375,137167,153125,186584,20000,1444526916,13845.36,1167000,8471257b2d6e7eac
1,0,4x2,persistent_grouped,sharded,4,2,1,4,2,7,14042,1087500,54750,80792,89667,97458,103083,139709,153125,167708,20000,1472952292,13578.17,1032791,8471257b2d6e7eac
1,1,4x2,immediate,sharded,4,2,1,0,0,1,14500,12000,3666,6000,7417,12334,3917,6458,7959,12709,20000,14859333,1345955.43,7625,8471257b2d6e7eac
2,0,4x2,immediate,sharded,4,2,1,0,0,1,12458,7791,3459,5792,6417,12250,3750,6208,7000,12667,20000,16154833,1238019.61,12291,8471257b2d6e7eac
2,1,4x2,persistent_grouped,sharded,4,2,1,4,2,7,10958,1060667,55625,77542,88417,99041,103833,133875,145834,160292,20000,1479767917,13515.63,1429375,8471257b2d6e7eac
3,0,4x2,persistent_grouped,sharded,4,2,1,4,2,7,17208,1158083,61250,85750,98958,109250,110583,146000,157541,171084,20000,1493740958,13389.20,1138292,8471257b2d6e7eac
3,1,4x2,immediate,sharded,4,2,1,0,0,1,16708,12667,3792,6000,7167,10375,4084,6458,7500,10708,20000,14980250,1335091.20,8041,8471257b2d6e7eac
```

## Verification around the harness

- Combined and topology-filtered ReleaseFast smoke runs passed.
- A single scenario reports `semantic_gate=NOT_APPLICABLE`, not a false A/B
  pass.
- A four-trial probe confirmed exact alternating run order.
- `zig fmt --check build.zig src/zig/bench/orchestrator_ab.zig` passed.
- `git diff --check -- build.zig src/zig/bench/orchestrator_ab.zig` passed.
- The historical `zig build -Doptimize=ReleaseFast bench -- 10` still passed;
  naive and direct both produced risk hash `9ac4f8133de45c30`.
