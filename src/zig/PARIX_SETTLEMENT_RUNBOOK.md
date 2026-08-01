# Parix settlement runbook

This runbook covers the optional **native** Parix/TigerBeetle settlement plugin. The exchange core remains usable without it. The browser/WASM build never receives Parix credentials and does not contain the native HTTP transport; browser clients send exchange commands to the native server.

The supplied database is a shared Parix development-tier database. It has no production SLA and must not be used for production funds.

## Fixed demo profile

The current integration deliberately uses a fixed roster. The canonical definitions live in `apps/parix_settlement_profile.zig`.

| Purpose | ID | Ledger | Code | Flags | Initial funding |
| --- | ---: | ---: | ---: | ---: | ---: |
| Base treasury | 420011 | 421 | 100 | 8 | source |
| Quote treasury | 420015 | 425 | 100 | 8 | source |
| Base fee | 42011 | 421 | 210 | 8 | none |
| Quote fee | 42015 | 425 | 210 | 8 | none |
| Seller 1001, base 11 | 421011 | 421 | 110 | 10 | 1000 |
| Seller 1001, quote 15 | 421015 | 425 | 110 | 10 | none |
| Buyer 1002, base 11 | 422011 | 421 | 110 | 10 | none |
| Buyer 1002, quote 15 | 422015 | 425 | 110 | 10 | 2000 |

- Database: `exchange-core` (`0e46f90e-c38b-487a-b9f9-630f7839cc3f`)
- Symbol: `241`
- Base currency/ledger: `11` / `421`
- Quote currency/ledger: `15` / `425`
- Seller/buyer UIDs: `1001` / `1002`
- Funding transfer `8421011`: base treasury `420011` to seller base `421011`, amount `1000`
- Funding transfer `8422015`: quote treasury `420015` to buyer quote `422015`, amount `2000`

TigerBeetle account and transfer records are immutable. Records created in the live database cannot be edited or deleted; correcting a balance requires a new compensating transfer. Treat all live smoke commands as durable external mutations.

## Configuration

Copy `.env.example` to `.env` and set exactly one credential:

```sh
PARIX_BASE_URL=https://dev.parix.io
PARIX_DATABASE_ID=0e46f90e-c38b-487a-b9f9-630f7839cc3f
PARIX_API_KEY=replace-with-api-key
# PARIX_BEARER_TOKEN=replace-with-bearer-token
```

Credentials are environment-only: never put them in command-line flags, logs, source files, screenshots, or commits. `PARIX_API_KEY` and `PARIX_BEARER_TOKEN` are mutually exclusive. Load a local file for the current shell before starting the server:

```sh
set -a
source .env
set +a
```

The demo TCP protocol has no client authentication or transport encryption. Keep the default `127.0.0.1` binding, or place it behind a trusted authenticated and encrypted gateway before accepting remote clients. The repository-relative persistence-root check does not confine operator-created symlinks; create that directory in a private, trusted workspace and do not point it at shared or untrusted storage.

## Start the native server

Run formatting and focused tests first:

```sh
zig fmt --check build.zig src/zig/apps
zig build test
zig build wasm-settlement-check
```

Start settlement with a stable, repository-relative persistence root:

```sh
zig build run-server -- \
  --parix-settlement \
  --persistence-root zig-out/runtime/parix
```

Without `--parix-settlement`, the native server follows its normal in-memory path and does not contact Parix. Keep the persistence root stable across restarts; changing or deleting it breaks the local journal/outbox recovery chain.

Startup is intentionally fail-closed:

1. The server opens and validates the local exchange journal and durable settlement outbox before any remote write is possible.
2. It then looks up all eight accounts and the two funding transfers.
3. Missing records are created once.
4. Existing records must exactly match the immutable profile; a mismatch prevents readiness.
5. A timeout or connection loss after submission is treated as ambiguous. The bootstrap looks up the same deterministic ID and reconciles its exact fields before deciding whether another submission is safe. It never blindly retries an indeterminate create.
6. The settlement worker starts only after local recovery and remote provisioning both succeed.

Do not route mutations to the process until `zig build run-client -- ping` returns `OK ready=true settlement=ready`. The one-time startup line reports only the settlement state and is not an admission probe. A `settling` state means durable work still exists and new mutations are rejected, while ping and read requests remain available. On shutdown, the listener stops accepting work, the exchange core shuts down, and the settlement worker is joined before its transport is released. Restart with the same persistence root; committed outbox entries are recovered idempotently by deterministic transfer ID.

## Local protocol smoke

With the server running in another terminal:

```sh
zig build run-client -- ping
zig build run-client -- balance 1001 11
zig build run-client -- balance 1002 15
zig build run-client -- book 241 10
```

Submit a crossing demo trade. Use fresh order IDs for each retained persistence root:

```sh
SELL_ORDER_ID=$(date +%s)
BUY_ORDER_ID=$((SELL_ORDER_ID + 1))
zig build run-client -- place "$SELL_ORDER_ID" 1001 241 5 10 ask gtc
zig build run-client -- place "$BUY_ORDER_ID" 1002 241 5 10 bid ioc
zig build run-client -- balance 1001 11
zig build run-client -- balance 1001 15
zig build run-client -- balance 1002 11
zig build run-client -- balance 1002 15
```

With the fixed symbol scales, size `10` moves `1000` base units and `500` quote units. The expected post-fill local balances are seller base/quote `0`/`490` and buyer base/quote `1000`/`1480`; the quote fee account receives `30`.

Wait for the server readiness/settlement status to show that the durable outbox has drained before checking Parix. Request shutdown through the protocol:

```sh
zig build run-client -- shutdown
```

`OK shutdown` acknowledges that the listener accepted the request; it does not certify that settlement draining has finished. The process then gives the worker its bounded drain window and joins it. A drain timeout, failed dispatch, or quarantine produces a nonzero server exit after cleanup, while leaving the durable outbox intact for recovery from the same persistence root.

Restart the server with the same persistence root and confirm it becomes ready without creating duplicate TigerBeetle transfers.

## Live Parix verification

The `parix` CLI uses the authenticated local session. These lookup commands are read-only:

```sh
parix tb lookup-accounts \
  0e46f90e-c38b-487a-b9f9-630f7839cc3f \
  --base-url https://dev.parix.io \
  --ids 420011,420015,42011,42015,421011,421015,422011,422015 \
  --json

parix tb lookup-transfers \
  0e46f90e-c38b-487a-b9f9-630f7839cc3f \
  --base-url https://dev.parix.io \
  --ids 8421011,8422015 \
  --json
```

After a fill, query the relevant accounts and transfer history to confirm the external balances and fee movements, then restart with the same persistence root and confirm no extra transfer appears. Parix may encode TigerBeetle `u128` amount fields as hexadecimal strings in JSON; use the integration decoder or compare the decoded numeric value instead of assuming the text is decimal.

## Unsupported boundaries

- Deposits and withdrawals are not mirrored to TigerBeetle. The server must reject deposit-style mutations while settlement is enabled rather than updating only one system.
- Dynamic user provisioning is not implemented. Only UIDs `1001` and `1002`, symbol `241`, and currencies `11`/`15` are routable in this demo profile.
- Changing the roster, ledgers, account IDs, or database requires a reviewed profile migration and a new deterministic provisioning plan.
- This integration does not turn the shared development database into a production-grade custody or accounting service.
- Parix credentials belong only in the native server process. Never expose them to the WASM bundle, browser storage, or client-side network requests.

For a production rollout, use a dedicated database with an appropriate SLA, define deposit/withdrawal accounting, introduce controlled user provisioning, rehearse journal/outbox disaster recovery, and monitor both exchange readiness and settlement lag.
