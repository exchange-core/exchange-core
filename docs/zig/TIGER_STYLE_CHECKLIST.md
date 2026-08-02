# Tiger Style Checklist for Zig Port

Use this checklist when implementing each migration slice.

## Safety

- Assert critical invariants at module boundaries and after state mutation.
- Keep invalid states unrepresentable where practical (typed enums, explicit error sets).
- Avoid implicit fallthrough and hidden behavior; use exhaustive `switch`.
- Fail fast on impossible states (`unreachable` only for true invariants).
- Use seeded deterministic simulations for state-machine flows (same seed => same behavior/result hashes).
- Prefer injectable `MonotonicClock` / `WallClock` (`RuntimeConfig`) over raw OS clocks for grouping and convenience timestamps that enter journaled or hashed state.

## Performance

- Keep hot paths allocation-aware; thread allocators explicitly.
- Use predictable, bounded loops over explicit collections.
- Prefer simple data layouts over opaque abstractions on hot paths.
- Keep per-command work deterministic and side-effect scoped.

## Developer Experience

- Keep functions small and purpose-specific.
- Name operations by intent (`locateOrder`, `removeOrderAt`, `tryMatchInstantly`).
- Co-locate tests with module logic.
- Document parity deviations immediately when introduced.

## Required Verification per task

- `zig build test` passes for touched modules.
- New logic includes edge-case tests for rejection/error paths.
- `validateInternalState`-style invariants are covered in tests.
- For async/batched flows, include at least one deterministic sync-vs-async convergence check.
- For seeded perturbation tests, print `seed=...` and a `zig build test --test-filter ...` replay hint on divergence.
- Kernel seeded fault-injection sims check single-vs-split invariants after every delivered command and print seed/step on failure.
- Multi-seed Java oracle campaign dual-runs committed `java_*.fixture` oracles under seed-shuffled order and seed-driven naive/direct random tapes; include seed=42 and print seed + test-filter on diverge.
- Prefer `SerialGroupedPipeline` (cooperative, no threads) for seeded schedule campaigns that interleave submit/journal-ack/take-result and compare fingerprints to immediate MultiShardKernelSet.
- Seeded journal I/O campaigns schedule pre-write/sync inject seams and torn tails under PRNG, reopen for recovery, and dual-run with seed + test-filter on diverge.
