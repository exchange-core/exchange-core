# Static allocation ops notes

See also `STATIC_ALLOCATION_DESIGN.md` for the full design contract.

## Defaults

| Surface | Matcher events | Order/bucket pools |
| --- | --- | --- |
| `JavaExchangeConfig.javaDefault()` / `basePreset` | `unbounded_heap` | `unbounded_heap` |
| `JavaExchangeConfig.latencyProduction()` | `.derive` (sizing 32) | `unbounded_heap` until hard-cap enabled |
| WASM / browser | Prefer heap or `StaticCapacityConfig.browserPreset()` (small fixed) | Small fixed if used |
| Unit tests | heap or `testSmall()` | tiny fixed for capacity tests |

**Never** use throughput full-derive slab sizes as a browser/WASM default.

## Result code

- Pure resting book capacity: `CommandResultCode.matching_engine_capacity` = **-3060**
- Accompanied by a **full-size reject** matcher event so R2 restores R1 holds
- Post-match / mid-move pool exhaust: pipeline **poison** (`error.OutOfMemory`), never -3060

## Ops override

```zig
var cfg = core.config.JavaExchangeConfig.latencyProduction();
// or explicit:
cfg.static_capacity_cfg = .{
    .matcher_events = .{ .fixed = 65_536 },
    .order_pools = .{ .fixed = .{ .max_orders = 64_000, .max_price_buckets = 8_000 } },
};
```

Resize requires process restart (no live grow of static slabs).

## Verification

```sh
zig build test
zig build -j1 gate --summary all
```

## Performance A/B

`bench-orchestrator-ab` accepts `--static-alloc heap|derive|latency|fixed`.

```sh
zig build -j1 -Doptimize=ReleaseFast bench-orchestrator-ab -- \
  --topology 1x1 --mode persistent_grouped --static-alloc latency \
  --trials 4 --commands 20000 --detached-commands 20000
```

Measured same-host comparison (2026-08-01): see
`STATIC_ALLOC_AB_REPORT_2026-08-01.md`. Summary: fingerprint-identical;
settled TPS within ~±4% of heap; memory 3–6× higher with pools; no large
durable-path TPS win.
