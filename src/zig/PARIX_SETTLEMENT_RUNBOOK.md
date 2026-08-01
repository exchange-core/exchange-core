# Parix settlement runbook

This runbook covers the optional **native** Parix/TigerBeetle settlement plugin. The exchange core remains usable without it. The browser/WASM build never receives Parix credentials and does not contain the native HTTP transport; browser clients send exchange commands to the native server.

The supplied database is a shared Parix development-tier database. It has no production SLA, may already contain prior smoke activity, and must not be used for production funds. The checked-in profile is a schema and reconciliation fixture, not proof that this shared ledger is safe to attach to a new empty local journal.

## Operator-owned profile

Settlement requires an immutable JSON profile supplied with `--parix-profile`. The loader accepts one currency-exchange market, exactly two currency routes, up to 16 pre-provisioned users, and up to four optional initial-funding records with at most one record per user. The one-per-user rule matches the core's monotonic balance-adjustment counter and makes initial funding restart-idempotent. TigerBeetle account/transfer IDs and amounts are decimal JSON strings so the file does not lose `u128` precision. The file is bounded to 64 KiB, rejects unknown or duplicate fields, and is copied into an owned, validated profile before startup mutates local or remote state.

[`fixtures/parix-settlement-profile.example.json`](fixtures/parix-settlement-profile.example.json) preserves the development profile used by the smoke commands below:

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

Account codes, transfer codes, and account flags remain fixed by this first productization slice. Profile-controlled fields include the database UUID, publication domain, complete symbol economics, currency ledgers and treasury/fee accounts, user account routes, and initial funding. Arrays are sorted before hashing, so harmless ordering changes retain the same identity while every economic or routing change produces a different digest.

On a fresh persistence root, startup atomically writes and fsyncs `settlement-profile.spf1` before opening the outbox or core persistence. The manifest binds the database UUID, publication namespace/epoch, canonical destination-registry digest, and complete profile digest. Every restart requires an exact match.

A non-empty legacy root without this manifest is rejected. It is not auto-adopted, and copying a manifest into it is not a migration. Likewise, do not point a new empty local root at a database that already has exchange activity: the local journal and external ledger would no longer describe the same history. Profile or legacy-root migration requires an offline, reviewed reconciliation procedure; that procedure is intentionally outside this slice.

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

Keep the profile file outside the persistence root. The root must be empty when its first manifest is created.

## Start the native server

Run formatting and focused tests first:

```sh
zig fmt --check build.zig src/zig/apps
zig build test
zig build wasm-settlement-check
```

Start settlement with a stable, repository-relative persistence root and an operator-owned profile. Use a dedicated database that is known to be consistent with this local root:

```sh
zig build run-server -- \
  --parix-settlement \
  --parix-profile path/to/operator-profile.json \
  --persistence-root zig-out/runtime/parix
```

Without `--parix-settlement`, the native server follows its normal in-memory path and does not contact Parix. Keep the persistence root stable across restarts; changing or deleting it breaks the local journal/outbox recovery chain.

The production-safe default is validate-only: every configured account and initial-funding transfer must already exist and match exactly. Missing records stop startup without sending a mutation. Two independent, explicit development controls are available:

```sh
# Permit creation of missing configured accounts only.
zig build run-server -- \
  --parix-settlement \
  --parix-profile path/to/operator-profile.json \
  --parix-create-accounts \
  --persistence-root zig-out/runtime/parix

# Permit creation of configured initial-funding transfers. Accounts must
# already exist, or combine this with --parix-create-accounts.
zig build run-server -- \
  --parix-settlement \
  --parix-profile path/to/operator-profile.json \
  --parix-development-seed \
  --persistence-root zig-out/runtime/parix
```

Do not enable development seeding for production treasury.

Startup is intentionally fail-closed:

1. The server loads and validates the bounded profile and checks that `PARIX_DATABASE_ID` names the same database.
2. It creates or validates the durable profile manifest before opening the settlement outbox or local core persistence.
3. It opens and validates the local exchange journal and durable settlement outbox before any remote write is possible.
4. It looks up every configured account and initial-funding transfer. Existing records must exactly match their immutable fields.
5. Missing records stop validate-only startup. Records are created only when the matching explicit provisioning flag is present.
6. A timeout or connection loss after an allowed submission is treated as ambiguous. The bootstrap looks up the same deterministic ID and reconciles its exact fields before deciding whether another submission is safe. It never blindly retries an indeterminate create.
7. The local symbol, users, and configured initial balances are bootstrapped idempotently, then the settlement worker starts.

Do not route mutations to the process until `zig build run-client -- ping` returns `OK ready=true settlement=ready`. The one-time startup line reports only the settlement state and is not an admission probe. A `settling` state means durable work still exists and new mutations are rejected, while ping and read requests remain available. On shutdown, the listener stops accepting work, the exchange core shuts down, and the settlement worker is joined before its transport is released. Restart with the same persistence root; committed outbox entries are recovered idempotently by deterministic transfer ID.

## Local protocol smoke

These expected values assume the checked-in development profile was provisioned into a clean dedicated database and paired with its original local root. They are not a safe bootstrap recipe for the shared sample database after prior trading activity.

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
- The roster is operator-configurable but immutable for one persistence root. Online user onboarding and profile migration are not implemented.
- Changing symbol economics, users, ledgers, account IDs, funding, database, or publication domain requires a reviewed offline migration. Exact-digest restart rejection is intentional.
- Legacy roots created before `settlement-profile.spf1` are not automatically adopted.
- Client authentication and UID/role authorization are not implemented in the TCP demo. TLS/mTLS, timeouts, connection caps, and rate limiting belong at a loopback L4 proxy; application identity and authorization still need to be enforced by the Zig server.
- This integration does not turn the shared development database into a production-grade custody or accounting service.
- Parix credentials belong only in the native server process. Never expose them to the WASM bundle, browser storage, or client-side network requests.

The next productization slices are authenticated/authorized ingress behind a TLS proxy, followed by durable deposit and withdrawal sagas with explicit accepted, pending, completed, and quarantined states. Before a production rollout, use a dedicated database with an appropriate SLA, rehearse journal/outbox disaster recovery, and monitor both exchange readiness and settlement lag.
