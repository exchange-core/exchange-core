# SIMD binary-carrier benchmark — 2026-07-31

## Verdict

The Java-compatible binary-carrier pack/unpack path now uses explicit Zig SIMD
with a target-selected vector width, a scalar fallback, and scalar tail
handling. On the measured Apple M4 host, ReleaseFast code generation uses two
`u64` lanes (16 bytes) per iteration and emits ARM NEON `rev64.16b`.

Across three independent 24-trial processes for a 16 MiB payload, the median of
the per-process paired medians was:

- pack: **3.8393x** the exact original scalar source and **1.1366x** a matched
  scalar-only control;
- unpack: **1.1212x** the original scalar source and **1.1126x** the matched
  scalar-only control.

Every process-level median favored production. Individual timed rows were
noisy and sometimes crossed 1.0, so the raw capture is committed and the
process-level result—not four-decimal certainty for one row—is the evidence
boundary.

The pack result against the original source combines removal of the old
per-word temporary-buffer copy, clearing only frame padding instead of the
entire output, and explicit SIMD. The matched `scalar_direct` comparison keeps
the same allocation, live-word writes, padding-only zero-fill, framing, and
scalar tail, isolating the vector endian transform.

This is a localized steady-state bulk carrier result. It is not ordinary
order-matching TPS and does not establish whole-exchange throughput improvement.

## Production changes

The measured functions are:

- `java_binary_wire.packSignedWordsBigEndian`
- `java_binary_wire.unpackSignedWordsBigEndian`

Each vector iteration loads complete `u64` words, applies
`std.mem.nativeToBig` lane-wise, and stores the transformed vector. Remaining
complete words and the final 0-7 bytes use the scalar path. Five-word frame
padding and signed Java `long` bit representation remain unchanged.

Vector selection is centralized in `carrierWordVectorLength`. Generic x86-64
without SSSE3 stays scalar because the SSE2 emulation requires substantially
more shuffle/unpack instructions than scalar `bswap`. Targets without a Zig
vector recommendation also stay scalar; big-endian targets retain identity
conversion.

The same source commit also reduces `encodeRequestWords` from three
allocations to one. It writes the Java size prefix and literal-only LZ4 payload
directly into final carrier storage, zeros only tail/frame padding, and
normalizes words in place. Exact composed-path equality, decode roundtrip,
boundary sizes, padding, failure cleanup, and a one-allocation assertion cover
that change. It is not included in the pack/unpack headline benchmark.

## Method

`zig build bench-simd` fixes both the harness and imported production module
to ReleaseFast regardless of the caller's global optimize option. The
executable also rejects a non-ReleaseFast build.

The harness compares three opaque/noinline timed call paths:

- `scalar_source`: exact pre-SIMD pack/unpack source;
- `scalar_direct`: matched scalar endian loops without an explicit vector;
- `production`: actual public pack/unpack functions.

For each operation it:

- validates complete outputs before timing;
- uses one calibrated iteration count for all three implementations;
- cycles through all six implementation-order permutations;
- runs 24 trials, four complete counterbalance cycles;
- computes a full-output checksum outside each timed row;
- prints every timing row and paired ratio as CSV;
- reports min, nearest-rank p05, median, nearest-rank p95, and max;
- includes allocation, transform, and free in the timed scope.

This is a steady-state repeated-buffer workload using
`std.heap.smp_allocator`. One deterministic input is reused, and an untimed
full-output transform/checksum immediately precedes each timed row. It describes
warmed repeated use, not cold-cache first use.

Pack throughput counts logical payload bytes. Unpack throughput counts the
physical padded carrier bytes actually transformed. Paired speedups always
compare identical work.

## Source and environment

- Source commit: `46933667d0c6123b6475f6e5e4ec0d945c115249`
- Pre-SIMD production commit: `c3a2b4917fc403a3f664c932056b4a0bf67cd9db`
- Zig: `0.16.0`
- Build mode: fixed `ReleaseFast`
- Target/CPU: native Apple arm64
- Host: Apple M4 Mac mini, 10 cores, 16 GB RAM
- OS: macOS 26.6 (build 25G72)
- Selected width: 2 x `u64` = 16 bytes
- Raw capture: `SIMD_BENCHMARK_RAW_2026-07-31.txt`
- Raw capture SHA-256:
  `ec0b30ff03da5a0d0339d2ddf31c8b140f59b77712b1a2b8f691b5fc1f5fa51f`

Each of the three processes used:

```sh
zig build -j1 -Dtarget=native -Dcpu=native \
  --cache-dir /tmp/exchange-core-simd-46933667-cache \
  --global-cache-dir /tmp/exchange-core-simd-46933667-global \
  bench-simd -- \
  --bytes 16777216 --warmup 6 --trials 24 --min-trial-ms 100
```

The tracked raw capture contains 432 timing rows, 144 paired-ratio rows,
per-process summaries, per-process SHA-256 hashes, and identical
per-operation checksums across implementations.

## Results

Paired speedup medians from each independent process:

| Process | Pack/original | Pack/direct | Unpack/original | Unpack/direct |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 3.7724x | 1.1366x | 1.1607x | 1.1233x |
| 2 | 4.0590x | 1.1382x | 1.1212x | 1.1067x |
| 3 | 3.8393x | 1.1256x | 1.1158x | 1.1126x |
| Median of process medians | **3.8393x** | **1.1366x** | **1.1212x** | **1.1126x** |

Median of the three per-process throughput medians:

| Operation | Original scalar | Direct scalar | Production SIMD |
| --- | ---: | ---: | ---: |
| pack | 3.345 GB/s | 11.563 GB/s | 12.752 GB/s |
| unpack | 11.740 GB/s | 11.702 GB/s | 12.555 GB/s |

The process medians are much more stable than individual rows. For example,
pack/direct process medians span 1.1256x-1.1382x even though scheduler outliers
inside individual processes span below and well above 1.0. The report therefore
does not use a single-run p05 or minimum as a claim that every invocation is
faster.

## Generated instruction and portability proof

The exact source commit was compiled as native ReleaseFast assembly.
`packSignedWordsBigEndian` contains:

```text
ldr      q0, [x11], #16
rev64.16b v0, v0
str      q0, [x12], #16
```

Additional compile/codegen checks established:

- x86_64 Linux baseline: scalar `bswapq`, without SSE2 shuffle emulation;
- x86_64 Linux with AVX2: four-lane `vpshufb`;
- RISC-V Linux baseline: null-vector scalar fallback compiles;
- s390x Linux baseline: big-endian identity path compiles.

The pinned Zig CI workflow repeats the native serial gate and all four
cross-target compile checks. Its AVX2 job narrowly asserts `vpshufb` inside
both carrier function bodies.

## Correctness and repository gates

- focused `java_binary_wire.zig`, Debug: 350/350 passed;
- focused `java_binary_wire.zig`, ReleaseSafe: 350/350 passed;
- vector-misaligned input and word-slice cases passed;
- all four ReleaseSafe cross-target test-root compilations passed;
- native ReleaseFast codegen inspection passed;
- three primary benchmark processes completed with matching checksums;
- full serial `zig build -j1 gate --summary all`: 43/43 steps and
  2305/2305 tests passed.

The full gate includes the benchmark's five invariant/equivalence tests in
Debug and ReleaseSafe, compiles the fixed-ReleaseFast executable, and retains
the Java fixture, API-surface, class-map, persistence, application, and core
verification stages.

## Limits

- Same-host medians are not cross-machine or Java-vs-Zig throughput claims.
- Public pack/unpack APIs allocate their outputs; allocator behavior remains in
  the measurement.
- Foreign targets were compile/codegen checked, not executed under QEMU or
  native hardware.
- The workload reuses one warmed buffer rather than rotating cold inputs.
- Ordinary matching, risk, persistence, and orchestrator performance are
  covered separately by `ORCHESTRATOR_AB_BENCHMARK_REPORT_2026-07-31.md`.
