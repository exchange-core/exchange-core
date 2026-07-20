# Zig Parity + Benchmark Report

Date: 2026-02-13

## Scope

This report captures migration-gate status for the current Zig core:

- internal parity between `.naive` and `.direct` orderbook modes,
- deterministic benchmark harness output from `zig build bench`,
- Java-vs-Zig fixture comparison status and known normalization rules.

## Pass/Fail Checklist

- [x] Zig kernel parity gate exists and is executed in unit tests.
- [x] Zig benchmark harness exists and runs via `zig build bench -- <rounds>`.
- [x] Benchmark output includes command count, elapsed time, throughput, and risk hash.
- [x] Java reference fixtures are generated for deterministic scenarios.
- [x] Automated Zig-vs-Java diff run is produced for result codes, balances, fees, and orderbook views.
- [x] All current Zig-vs-Java fixture checks pass with zero mismatches.

## Verification Evidence

Executed on 2026-02-13:

```sh
zig build bench
```

Observed output:

```text
exchange-core zig bench (rounds=50000)
kind     commands    elapsed_ms    throughput_cmd_per_s    risk_hash
naive     100048      1231.318              81252.74    b83366340d090bd3
direct    100048       705.478             141815.92    b83366340d090bd3
```

Executed on 2026-02-13:

```sh
zig build test
```

Executed on 2026-02-13:

```sh
zig build parity-diff
```

Executed on 2026-02-09 (full parity fixture suite):

```sh
zig build parity-diff
```

Observed fixture diff output:

- `java_user_balance_basic.fixture`: PASS (`state_hash` compared via legacy alias `state_hash_kind=java_report` -> `zig_detailed_low32`).
- `java_user_balance_basic_low32.fixture`: PASS (`state_hash` compared strictly as `zig_risk_low32`).
- `java_user_balance_basic_detailed_low32.fixture`: PASS (`state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_control.fixture`: PASS (`state_hash` compared via legacy alias `state_hash_kind=java_report` -> `zig_detailed_low32`; suspend/resume, grouping-control, persist-result (API-first with synthetic fallback), reset, and nop control results compared strictly).
- `java_exchange_control_low32.fixture`: PASS (`state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_control_detailed_low32.fixture`: PASS (`state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_basic.fixture`: PASS (`state_hash` compared via legacy alias `state_hash_kind=java_report` -> `zig_detailed_low32`; settlement-sensitive balances normalized deterministically for `settlement_mode=pre_release`).
- `java_exchange_trade_basic_full.fixture`: PASS (`settlement_mode=full`; `maker_balance_15` and `taker_balance_11` compared strictly).
- `java_exchange_trade_basic_full_low32.fixture`: PASS (`settlement_mode=full`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_basic_full_detailed_low32.fixture`: PASS (`settlement_mode=full`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_basic_direct.fixture`: PASS (`orderbook_mode=direct`; `state_hash` compared via legacy alias `state_hash_kind=java_report` -> `zig_detailed_low32`; settlement-sensitive balances normalized deterministically in `pre_release`).
- `java_exchange_trade_basic_direct_full.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; basic-stream balances compared strictly).
- `java_exchange_trade_basic_low32.fixture`: PASS (`settlement_mode=pre_release`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_basic_detailed_low32.fixture`: PASS (`settlement_mode=pre_release`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_basic_direct_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_basic_direct_detailed_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_basic_direct_full_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_basic_direct_full_detailed_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_extended.fixture`: PASS (`state_hash` compared via legacy alias `state_hash_kind=java_report` -> `zig_detailed_low32`; settlement-sensitive balances normalized deterministically for `settlement_mode=pre_release`).
- `java_exchange_trade_extended_direct.fixture`: PASS (`orderbook_mode=direct`; `state_hash` compared via legacy alias `state_hash_kind=java_report` -> `zig_detailed_low32`; settlement-sensitive balances normalized deterministically in `pre_release`).
- `java_exchange_trade_bootstrap.fixture`: PASS (`state_hash` compared via legacy alias `state_hash_kind=java_report` -> `zig_detailed_low32`; settlement-sensitive balances normalized deterministically for `settlement_mode=pre_release`; Zig scenario executes snapshot+journal checkpoint restore path).
- `java_exchange_trade_bootstrap_full.fixture`: PASS (`settlement_mode=full`; bootstrap stream balances compared strictly).
- `java_exchange_trade_bootstrap_low32.fixture`: PASS (`settlement_mode=pre_release`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_bootstrap_detailed_low32.fixture`: PASS (`settlement_mode=pre_release`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_bootstrap_full_low32.fixture`: PASS (`settlement_mode=full`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_bootstrap_full_detailed_low32.fixture`: PASS (`settlement_mode=full`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_bootstrap_direct.fixture`: PASS (`orderbook_mode=direct`; state_hash compared via legacy alias `state_hash_kind=java_report` -> `zig_detailed_low32`; settlement-sensitive balances normalized deterministically in `pre_release`).
- `java_exchange_trade_bootstrap_direct_full.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; bootstrap stream balances compared strictly).
- `java_exchange_trade_bootstrap_direct_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_bootstrap_direct_detailed_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_bootstrap_direct_full_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_bootstrap_direct_full_detailed_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_bootstrap_long.fixture`: PASS (`settlement_mode=pre_release`; larger deterministic checkpoint-replay command stream).
- `java_exchange_trade_bootstrap_long_low32.fixture`: PASS (`settlement_mode=pre_release`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_bootstrap_long_detailed_low32.fixture`: PASS (`settlement_mode=pre_release`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_bootstrap_long_direct.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; larger deterministic checkpoint-replay command stream).
- `java_exchange_trade_bootstrap_long_direct_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_bootstrap_long_direct_detailed_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_bootstrap_long_full.fixture`: PASS (`settlement_mode=full`; larger deterministic checkpoint-replay command stream).
- `java_exchange_trade_bootstrap_long_full_low32.fixture`: PASS (`settlement_mode=full`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_bootstrap_long_full_detailed_low32.fixture`: PASS (`settlement_mode=full`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_bootstrap_long_direct_full.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; larger deterministic checkpoint-replay command stream).
- `java_exchange_trade_bootstrap_long_direct_full_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_bootstrap_long_direct_full_detailed_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_extended_low32.fixture`: PASS (`settlement_mode=pre_release`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_extended_detailed_low32.fixture`: PASS (`settlement_mode=pre_release`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_extended_full.fixture`: PASS (`settlement_mode=full`; extended-stream balances compared strictly).
- `java_exchange_trade_extended_direct_full.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; extended-stream balances compared strictly).
- `java_exchange_trade_extended_full_low32.fixture`: PASS (`settlement_mode=full`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_extended_full_detailed_low32.fixture`: PASS (`settlement_mode=full`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_extended_direct_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_extended_direct_detailed_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_extended_direct_full_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_extended_direct_full_detailed_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_multisymbol_241.fixture`: PASS (`state_hash` compared via legacy alias `state_hash_kind=java_report` -> `zig_detailed_low32`; settlement-sensitive balances normalized deterministically for `settlement_mode=pre_release`).
- `java_exchange_trade_multisymbol_242.fixture`: PASS (`state_hash` compared via legacy alias `state_hash_kind=java_report` -> `zig_detailed_low32`; settlement-sensitive balances normalized deterministically for `settlement_mode=pre_release`).
- `java_exchange_trade_multisymbol_full_241.fixture`: PASS (`settlement_mode=full`; multi-symbol balances compared strictly).
- `java_exchange_trade_multisymbol_full_242.fixture`: PASS (`settlement_mode=full`; multi-symbol balances compared strictly with alternate focus-symbol orderbook view).
- `java_exchange_trade_multisymbol_direct_241.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; state_hash compared via legacy alias `state_hash_kind=java_report` -> `zig_detailed_low32`; settlement-sensitive balances normalized deterministically).
- `java_exchange_trade_multisymbol_direct_242.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; state_hash compared via legacy alias `state_hash_kind=java_report` -> `zig_detailed_low32`; alternate focus-symbol orderbook view).
- `java_exchange_trade_multisymbol_direct_full_241.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; multi-symbol balances compared strictly).
- `java_exchange_trade_multisymbol_direct_full_242.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; multi-symbol balances compared strictly with alternate focus-symbol orderbook view).
- `java_exchange_trade_multisymbol_241_low32.fixture`: PASS (`settlement_mode=pre_release`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_multisymbol_241_detailed_low32.fixture`: PASS (`settlement_mode=pre_release`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_multisymbol_242_low32.fixture`: PASS (`settlement_mode=pre_release`; `state_hash` compared strictly as `zig_risk_low32`; alternate focus-symbol orderbook view).
- `java_exchange_trade_multisymbol_242_detailed_low32.fixture`: PASS (`settlement_mode=pre_release`; `state_hash` compared strictly as `zig_detailed_low32`; alternate focus-symbol orderbook view).
- `java_exchange_trade_multisymbol_direct_241_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_multisymbol_direct_241_detailed_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_multisymbol_direct_242_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; `state_hash` compared strictly as `zig_risk_low32`; alternate focus-symbol orderbook view).
- `java_exchange_trade_multisymbol_direct_242_detailed_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; `state_hash` compared strictly as `zig_detailed_low32`; alternate focus-symbol orderbook view).
- `java_exchange_trade_multisymbol_full_241_low32.fixture`: PASS (`settlement_mode=full`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_multisymbol_full_241_detailed_low32.fixture`: PASS (`settlement_mode=full`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_multisymbol_full_242_low32.fixture`: PASS (`settlement_mode=full`; `state_hash` compared strictly as `zig_risk_low32`; alternate focus-symbol orderbook view).
- `java_exchange_trade_multisymbol_full_242_detailed_low32.fixture`: PASS (`settlement_mode=full`; `state_hash` compared strictly as `zig_detailed_low32`; alternate focus-symbol orderbook view).
- `java_exchange_trade_multisymbol_direct_full_241_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_multisymbol_direct_full_241_detailed_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_multisymbol_direct_full_242_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; `state_hash` compared strictly as `zig_risk_low32`; alternate focus-symbol orderbook view).
- `java_exchange_trade_multisymbol_direct_full_242_detailed_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; `state_hash` compared strictly as `zig_detailed_low32`; alternate focus-symbol orderbook view).
- `java_exchange_trade_reports.fixture`: PASS (`settlement_mode=full`; single-user and total-currency-balance report fields compared strictly).
- `java_exchange_trade_reports_direct.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; report fields compared strictly).
- `java_exchange_trade_reports_low32.fixture`: PASS (`settlement_mode=full`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_reports_detailed_low32.fixture`: PASS (`settlement_mode=full`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_reports_direct_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_reports_direct_detailed_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_reports_long.fixture`: PASS (`settlement_mode=pre_release`; larger deterministic command stream with report/orderbook parity checks).
- `java_exchange_trade_reports_long_low32.fixture`: PASS (`settlement_mode=pre_release`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_reports_long_detailed_low32.fixture`: PASS (`settlement_mode=pre_release`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_reports_long_direct.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; larger deterministic command stream with report/orderbook parity checks).
- `java_exchange_trade_reports_long_direct_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_reports_long_direct_detailed_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=pre_release`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_reports_long_full.fixture`: PASS (`settlement_mode=full`; larger deterministic command stream with report/orderbook parity checks).
- `java_exchange_trade_reports_long_full_low32.fixture`: PASS (`settlement_mode=full`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_reports_long_full_detailed_low32.fixture`: PASS (`settlement_mode=full`; `state_hash` compared strictly as `zig_detailed_low32`).
- `java_exchange_trade_reports_long_direct_full.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; larger deterministic command stream with report/orderbook parity checks).
- `java_exchange_trade_reports_long_direct_full_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; `state_hash` compared strictly as `zig_risk_low32`).
- `java_exchange_trade_reports_long_direct_full_detailed_low32.fixture`: PASS (`orderbook_mode=direct`; `settlement_mode=full`; `state_hash` compared strictly as `zig_detailed_low32`).

Strict low32 hash check was validated in two steps:

- Intentional mismatch (`state_hash=0`) fails with `actual(i32)=-321725011`.
- Valid fixture regenerated with `state_hash=-321725011` passes.
- Exchange trade pre-release low32 mismatch (`state_hash=0`) fails with `actual(i32)=-1442405089`; regenerated naive + direct fixtures with `state_hash=-1442405089` pass.
- User-balance detailed low32 mismatch (`state_hash=0`) fails with `actual(i32)=1221872883`; regenerated fixture with `state_hash=1221872883` passes.
- Exchange control low32 risk mismatch (`state_hash=0`) fails with `actual(i32)=1822592909`; regenerated fixture with `state_hash=1822592909` passes.
- Exchange control low32 detailed mismatch (`state_hash=0`) fails with `actual(i32)=2065005962`; regenerated fixture with `state_hash=2065005962` passes.
- Exchange trade pre-release detailed low32 mismatch (`state_hash=0`) fails with `actual(i32)=-95572745`; regenerated naive + direct fixtures with `state_hash=-95572745` pass.
- Exchange trade full-settlement low32 mismatch (`state_hash=0`) fails with `actual(i32)=-1442405089`; regenerated naive + direct fixtures with `state_hash=-1442405089` pass.
- Exchange trade full-settlement detailed low32 mismatch (`state_hash=0`) fails with `actual(i32)=-95572745`; regenerated naive + direct fixtures with `state_hash=-95572745` pass.
- Extended trade pre-release low32 mismatch (`state_hash=0`) fails with `actual(i32)=2096567512`; regenerated naive + direct fixtures with `state_hash=2096567512` pass.
- Extended trade pre-release detailed low32 mismatch (`state_hash=0`) fails with `actual(i32)=2051924884`; regenerated naive + direct fixtures with `state_hash=2051924884` pass.
- Extended trade full-settlement low32 mismatch (`state_hash=0`) fails with `actual(i32)=2096567512`; regenerated naive + direct fixtures with `state_hash=2096567512` pass.
- Extended trade full-settlement detailed low32 mismatch (`state_hash=0`) fails with `actual(i32)=2051924884`; regenerated naive + direct fixtures with `state_hash=2051924884` pass.
- Bootstrap trade pre-release low32 mismatch (`state_hash=0`) fails with `actual(i32)=2096567512`; regenerated fixture with `state_hash=2096567512` passes.
- Bootstrap trade pre-release detailed low32 mismatch (`state_hash=0`) fails with `actual(i32)=2051924884`; regenerated fixture with `state_hash=2051924884` passes.
- Bootstrap trade full-settlement low32 mismatch (`state_hash=0`) fails with `actual(i32)=2096567512`; regenerated fixture with `state_hash=2096567512` passes.
- Bootstrap trade full-settlement detailed low32 mismatch (`state_hash=0`) fails with `actual(i32)=2051924884`; regenerated fixture with `state_hash=2051924884` passes.
- Bootstrap trade direct pre-release low32 mismatch (`state_hash=0`) fails with `actual(i32)=2096567512`; regenerated fixture with `state_hash=2096567512` passes.
- Bootstrap trade direct pre-release detailed low32 mismatch (`state_hash=0`) fails with `actual(i32)=2051924884`; regenerated fixture with `state_hash=2051924884` passes.
- Bootstrap trade direct full-settlement low32 mismatch (`state_hash=0`) fails with `actual(i32)=2096567512`; regenerated fixture with `state_hash=2096567512` passes.
- Bootstrap trade direct full-settlement detailed low32 mismatch (`state_hash=0`) fails with `actual(i32)=2051924884`; regenerated fixture with `state_hash=2051924884` passes.
- Bootstrap trade long pre-release low32 mismatch (`state_hash=0`) fails with `actual(i32)=-733670551`; regenerated fixture with `state_hash=-733670551` passes.
- Bootstrap trade long pre-release detailed low32 mismatch (`state_hash=0`) fails with `actual(i32)=559081494`; regenerated fixture with `state_hash=559081494` passes.
- Bootstrap trade long direct pre-release low32 mismatch (`state_hash=0`) fails with `actual(i32)=-733670551`; regenerated fixture with `state_hash=-733670551` passes.
- Bootstrap trade long direct pre-release detailed low32 mismatch (`state_hash=0`) fails with `actual(i32)=559081494`; regenerated fixture with `state_hash=559081494` passes.
- Bootstrap trade long full-settlement low32 mismatch (`state_hash=0`) fails with `actual(i32)=-733670551`; regenerated naive + direct fixtures with `state_hash=-733670551` pass.
- Bootstrap trade long full-settlement detailed low32 mismatch (`state_hash=0`) fails with `actual(i32)=559081494`; regenerated naive + direct fixtures with `state_hash=559081494` pass.
- Multi-symbol pre-release/full low32 risk mismatch (`state_hash=0`) fails with `actual(i32)=814202586`; regenerated `241` and `242` focus-symbol fixtures (naive + direct) with `state_hash=814202586` pass.
- Multi-symbol pre-release/full low32 detailed mismatch (`state_hash=0`) fails with `actual(i32)=1042804799`; regenerated `241` and `242` focus-symbol fixtures (naive + direct) with `state_hash=1042804799` pass.
- Reports full-settlement low32 risk mismatch (`state_hash=0`) fails with `actual(i32)=2096567512`; regenerated naive + direct reports fixtures with `state_hash=2096567512` pass.
- Reports full-settlement low32 detailed mismatch (`state_hash=0`) fails with `actual(i32)=2051924884`; regenerated naive + direct reports fixtures with `state_hash=2051924884` pass.
- Reports long pre-release low32 risk mismatch (`state_hash=0`) fails with `actual(i32)=-741618970`; regenerated naive + direct fixtures with `state_hash=-741618970` pass.
- Reports long pre-release low32 detailed mismatch (`state_hash=0`) fails with `actual(i32)=1800747790`; regenerated naive + direct fixtures with `state_hash=1800747790` pass.
- Reports long full-settlement low32 risk mismatch (`state_hash=0`) fails with `actual(i32)=-741618970`; regenerated naive + direct fixtures with `state_hash=-741618970` pass.
- Reports long full-settlement low32 detailed mismatch (`state_hash=0`) fails with `actual(i32)=1800747790`; regenerated naive + direct fixtures with `state_hash=1800747790` pass.

Parity-relevant tests currently include:

- `kernel parity gate keeps naive and direct behavior aligned`
- `journal replay reproduces sequence, results, risk hash, and L2 view`
- `journal, snapshot, and report queries stay deterministic under large seeded stream`
- `journal, snapshot, and report queries stay deterministic under multi-symbol seeded stream`
- `exchange core seeded persistence checkpoints keep snapshot-journal bootstrap deterministic`
- `snapshot roundtrip and kernel validation`
- `exchange api report queries expose risk hash, fees, balances, and orderbook`
- `kernel detailed state hash report is deterministic and mutation sensitive`
- `risk engine state hash is order-independent and mutation-sensitive`
- `kernel deterministic simulation parity keeps naive and direct aligned`
- `multi-shard uid-routed parity matches single-shard user state invariants`
- `split matching/risk shard simulation keeps cross-shard trade parity`
- `seeded fault-injection split simulation is deterministic and seed-sensitive`
- `seeded fault-injection multi-symbol split simulation is deterministic and seed-sensitive`
- `batch add symbols adds only new symbols`
- `batch add accounts creates users and applies balances`
- `exchange runtime drains in fixed-size groups with deterministic sequences`
- `exchange runtime grouping control toggles batching deterministically`
- `kernel control commands keep java-compatible result semantics`
- `exchange api suspend and resume helpers mirror user-state transitions`
- `exchange api persistState sync helper merges matching and risk results`
- `binary frame assembler accepts fragments and completes command transfer`
- `kernel binary frame handling keeps query accepted-until-last semantics`
- `kernel binary query unknown report type returns report_query_unknown_type`
- `kernel binary query total currency balance payload returns typed report`
- `kernel binary query single user payload returns typed report`
- `kernel binary data command applies add-symbols and add-accounts payloads`
- `kernel binary data command malformed payload returns binary command failed`
- `kernel total currency balance report aggregates risk balances and open interest`
- `accumulateOrderHolds reports resting ask/bid currency holds`
- `direct book accumulateOrderHolds matches naive`
- `naive book state hash is deterministic and mutation sensitive`
- `direct book state hash matches naive and is mutation sensitive`
- `total currency balance report includes matching-order holds with naive/direct parity`
- `single user report found and not-found semantics`
- `single user report keeps naive/direct parity on open orders and balances`

## Current Gaps

- Exchange trade fixtures include `pre_release` variants where settlement-sensitive balances are now compared via deterministic Java-aligned normalization (no skip-based comparison path).
- Report fixtures now export canonical Java report-query fields first (`SingleUserReportQuery` and `TotalCurrencyBalanceReportQuery`). On Java 25 runtimes where Chronicle `unmap0` incompatibility breaks marshalling, fixture generation takes a bounded deterministic fallback to in-memory report-equivalent values.
- Control fixtures now attempt `ApiPersistState` first via an isolated snapshot-enabled probe container. On Java 25 runtimes where persistence initialization is blocked (for example Chronicle initialization failures such as `NoSuchFieldException: address`) or persist completion times out, fixture generation falls back to a deterministic synthetic merged persist result.

## Follow-up Work

- None at current fixture scope.
