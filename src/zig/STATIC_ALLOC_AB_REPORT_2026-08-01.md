# Static allocation vs heap — orchestrator A/B (2026-08-01)

Same-host ReleaseFast comparison using `bench-orchestrator-ab` with
`--static-alloc`. Workload: 1×1 persistent_grouped, 4 trials, 20k settled +
20k detached commands, 500 latency samples, warmup 256, window 256.

## Environment

- Build: `ReleaseFast`, Zig 0.16.0
- Topology: 1 ME / 1 RE, `persistent_grouped`
- Publication: routed
- Semantic fingerprint: identical across all four backends
  (`115306a07a302c98`)
- Capacity gate floors: PASS for every row

## Modes

| Mode | Matcher events | Order/bucket pools | Order book |
| --- | --- | --- | --- |
| `heap` | unbounded `create`/`destroy` | unbounded freelist+grow | naive (java default) |
| `derive` | pool `ring×64×2` (~2.1M nodes) | unbounded | direct |
| `latency` | pool `ring×32×2` (~1.0M nodes) | unbounded | direct |
| `fixed` | pool fixed ~2.1M nodes | fixed 256k orders / 32k buckets | direct |

## Median results (middle of 4 trials)

| Mode | Settled cmd/s | Detached cmd/s | Terminal p50 (ns) | Report p50 (ns) | Live bytes after init | Live bytes after setup |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `heap` | 641332.7 | 980914.0 | 33312 | 11333 | 33,495,024 | 44,135,356 |
| `derive` | 618145.3 | 988078.9 | 32521 | 11208 | 167,712,864 | 178,353,196 |
| `latency` | 659242.8 | 1012316.4 | 33062 | 11354 | 100,604,000 | 111,244,332 |
| `fixed` | 643427.4 | 984044.9 | 31666 | 11333 | 167,712,864 | 284,259,372 |

## Relative to heap

| Mode | Settled TPS | Detached TPS | Terminal p50 | Init RSS proxy | Setup RSS proxy |
| --- | ---: | ---: | ---: | ---: | ---: |
| `heap` | 1.000× | 1.000× | 1.000× | 1.00× | 1.00× |
| `derive` | 0.964× | 1.007× | 0.976× | 5.01× | 4.04× |
| `latency` | 1.028× | 1.032× | 0.992× | 3.00× | 2.52× |
| `fixed` | 1.003× | 1.003× | 0.951× | 5.01× | 6.44× |

## Interpretation

1. **Semantic equivalence:** same final-state fingerprint in all 16 rows
   (4 trials × 4 backends). Static alloc did not change matching outcomes.
2. **Throughput:** within noise for this host/workload. Median settled TPS
   ranged from **0.96×** (derive) to **1.03×** (latency) vs heap. No large
   win on settled commands/s for this durable 1×1 grouped path.
3. **Latency:** terminal-drain p50 was similar or slightly better for static
   modes (about **0.95–0.99×** heap). Not a dramatic p50 shift; journal/group
   barriers still dominate.
4. **Memory:** the clear difference. Event pools reserve ~1–5× more live
   requested bytes at init (`latency` ~3.0×, `derive`/`fixed` ~5.0× heap).
   `fixed` also grows setup RSS for order/bucket slabs (~6.4× heap setup).
5. **Init time:** static modes pay more init (pool slab fill); heap init is
   ~16979 ns median vs derive ~1413146 ns.
6. **Takeaway:** for this engine, static allocation is primarily a
   **predictability / tail / capacity-bound** tool, not a free TPS multiplier
   on the durable grouped path. Prefer `latency` (half the event RAM of
   full derive) if enabling pools without hard book caps.

## Reproduce

```sh
COMMON='--topology 1x1 --mode persistent_grouped --trials 4 \
  --commands 20000 --detached-commands 20000 --latency-samples 500 \
  --warmup 256 --window 256 --memory-probe-commands 512'
for m in heap derive latency fixed; do
  zig build -j1 -Doptimize=ReleaseFast bench-orchestrator-ab -- \
    $COMMON --static-alloc $m | tee ab-$m.log
done
```

Raw logs: `ab-heap.log`, `ab-derive.log`, `ab-latency.log`, `ab-fixed.log`
(captured under the measurement scratch dir for this run).

