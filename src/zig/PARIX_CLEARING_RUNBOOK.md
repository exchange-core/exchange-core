# Parix payment-clearing runbook

This runbook covers the optional native payment-clearing profile and runtime.
Clearing is an application plugin beside exchange-core: it does not modify
matching, risk, trade settlement, FSG1 encoding, or browser/WASM.

Rollout slices 1 through 6 are implemented: the pure saga and `CSG1` store,
strict clearing profile, classified TigerBeetle runtime, typed FSG1 adapter,
versioned provider HMAC adapter, and bounded loopback HTTP ingress. Slice 4's
native signed-callback client, operator-only status observation, exact restart
procedure, and approved live authorize/post/idempotency rehearsal are complete.
The rehearsal exposed and closed a missing-empty-outbox recovery-anchor gap.
Slice 5 adds durable authorization deadlines, deterministic automatic voids,
and restart-safe FSG1/TigerBeetle reconciliation before compensation.
Slice 6 adds an authenticated read-only operations snapshot and the
reconciliation, coordinated backup/restore, and incident procedures below.
The native server composes clearing only when every option is supplied. The
public HTTPS or mTLS endpoint remains an operator-owned L7 proxy; the Zig
process never binds provider ingress beyond loopback.

## Clearing profile

Use the checked-in development shape at
`fixtures/parix-clearing-profile.example.json` as a schema example. The bounded
version-1 document has exactly six top-level fields:

```json
{
  "version": 1,
  "database_uuid": "0e46f90e-c38b-487a-b9f9-630f7839cc3f",
  "provider_id": "7001",
  "transfer_code": 20,
  "timeout_bounds": {
    "minimum_seconds": 30,
    "maximum_seconds": 3600
  },
  "routes": [
    {
      "currency": 11,
      "ledger": 421,
      "treasury_account_id": "420011",
      "provider_control_account_id": "430011"
    },
    {
      "currency": 15,
      "ledger": 425,
      "treasury_account_id": "420015",
      "provider_control_account_id": "430015"
    }
  ]
}
```

Provider and account IDs are decimal strings so JSON number precision cannot
change them. The profile contains no endpoint, credential, token, signature
key, account number, or customer PII.

Profile loading pairs the document with one already-validated settlement
profile and rejects:

- a different database UUID;
- a zero provider or transfer code;
- invalid or inverted timeout bounds;
- missing, duplicate, or unknown currency routes;
- ledger or treasury drift from the settlement profile;
- duplicate provider-control accounts or a collision with any settlement
  account.

The canonical clearing digest hashes the normalized economic fields rather
than JSON bytes. Route ordering therefore does not change identity. `CSG1`
stores the destination, settlement-profile digest, clearing-profile digest,
and provider ID independently and rejects any restart drift.

## Provider authentication profile

Use `fixtures/parix-clearing-provider.example.json` as the exact non-secret
version-1 schema:

```json
{
  "version": 1,
  "provider_id": "7001",
  "path": "/v1/clearing/events",
  "replay_window_seconds": 300,
  "secret_file_env": "PARIX_CLEARING_PROVIDER_SECRET_FILE"
}
```

The provider ID must equal the clearing profile. `secret_file_env` names an
environment variable whose value is the signing-key file path. The key file
must be a regular non-symlink file with mode exactly `0600` and contain exactly
64 lowercase hexadecimal bytes with no trailing newline. The loader checks the
file again after reading, keeps the decoded 32-byte key only in heap-owned
process memory, zeroes temporary and owned key buffers, and never logs it.
Version 1 configures one provider identity and one endpoint per exchange
instance; deploy separate instances for independent provider trust domains.

Version 1 accepts exactly one `POST`, one `Content-Length`, one
`Content-Type: application/json`, one `X-Exchange-Provider-Timestamp`, and one
`X-Exchange-Provider-Signature`. Chunked transfer encoding, content encoding,
`Expect`, empty bodies, bodies over 16 KiB, duplicate required headers, unknown
JSON fields, duplicate JSON fields, and numeric coercions fail closed. Event
and payment IDs are decimal strings so a JSON implementation cannot truncate
`u128` values.

The signature is 64 lowercase hexadecimal characters containing
HMAC-SHA256. Compute it over the following fields in order, prefixing every
field with its unsigned four-byte big-endian byte length:

1. ASCII domain `exchange-core/parix-clearing-provider/hmac-sha256/v1`;
2. provider ID encoded as eight-byte unsigned big-endian;
3. exact bytes `POST`;
4. exact configured request-target bytes;
5. exact canonical positive-decimal timestamp-header bytes;
6. exact raw request-body bytes.

The body is authenticated before JSON parsing. Signature comparison is
constant-time, then the Unix timestamp must fall within the configured past or
future replay window, inclusive. The provider may safely retry a lost response
with the same canonical event: CSG1 returns an exact duplicate only when every
immutable field still matches.

## Provider rehearsal client

`exchange-core-provider-client` is a native, loopback-only operator tool for
signing one fixture and sending one callback to the configured provider ingress.
It loads the same provider profile and owner-only signing key as the server,
uses the current Unix time unless `--timestamp` is explicitly supplied, and
authenticates the exact body-file bytes. Reserve `--timestamp` for deterministic
tests or a clock-controlled rehearsal; a stale fixed timestamp is rejected by
the replay window.

Build the installed client or run it through the build graph:

```sh
zig build -j1 provider-client

zig build -j1 run-provider-client -- \
  --profile path/to/operator-provider-profile.json \
  --body-file path/to/archived-authorization.json \
  --port 7380
```

The equivalent installed-binary invocation is:

```sh
zig-out/bin/exchange-core-provider-client \
  --profile path/to/operator-provider-profile.json \
  --body-file path/to/archived-authorization.json \
  --port 7380
```

The only accepted host is literal `127.0.0.1`. Success prints exactly one JSON
line: `{"status":202}` for a newly durable event or `{"status":200}` for an
exact durable duplicate. Any other HTTP outcome, malformed response, timeout,
or transport failure exits nonzero. The client is not a public webhook sender,
TLS client, named-provider adapter, or permission to bypass the operator-owned
L7 boundary.

Use `fixtures/parix-clearing-authorization.example.json` and
`fixtures/parix-clearing-post.example.json` as the version-1 rehearsal bodies.
For every dedicated rehearsal, allocate a fresh stable payment ID and two fresh
stable event IDs, make reviewed copies of both fixtures with those IDs, and
archive the final exact bytes before the first request. Keep the authorization
and post copies unchanged through replay: even a trailing-newline change alters
the signature and violates exact-duplicate evidence.

Terminate public TLS or mTLS at an operator-owned L7 proxy and forward to the
configured loopback provider port. Do not rewrite the target, timestamp,
signature, content type, or body. Enforce public connection/rate limits at the
proxy as well as the host's 32-session, 8-KiB-head, 16-KiB-body, and five-second
absolute pre-admission limits. Never expose the backend port on a public or
private-LAN interface.

## Account requirements

Each route requires a provider-control TigerBeetle account on the same ledger
as its treasury account. The IDs must be provisioned and verified before the
runtime starts. This slice does not automatically create accounts. The server
requires `--parix-clearing-accounts-verified` as an explicit operator
attestation, but that flag does not perform or replace the check. Use a reviewed
Parix/TigerBeetle provisioning ceremony and compare every governed immutable
field after lookup before supplying it. The current clearing profile does not
carry enough account metadata to pretend that an ID-and-ledger lookup is an
exact automated verification.

Do not reuse a treasury, fee, or customer account as provider control. The
account represents value at the external rail and must be reconciled with
provider reports and bank statements.

## Native enablement

Clearing is disabled unless it is paired with settlement, authenticated raw
TCP ingress, both clearing profiles, and the account attestation. A normal
restart uses an existing FSG1 and CSG1:

```sh
zig-out/bin/exchange-core-server \
  --parix-settlement \
  --parix-profile path/to/operator-settlement-profile.json \
  --ingress-auth-profile path/to/operator-ingress-profile.json \
  --parix-clearing-profile path/to/operator-clearing-profile.json \
  --parix-provider-profile path/to/operator-provider-profile.json \
  --parix-clearing-accounts-verified \
  --provider-port 7380 \
  --persistence-root path/to/durable-root
```

Supply the three existing `PARIX_BASE_URL`, `PARIX_DATABASE_ID`, and either
`PARIX_API_KEY` or `PARIX_BEARER_TOKEN` variables described in
`PARIX_SETTLEMENT_RUNBOOK.md`, plus the secret-file environment variable named
by the provider profile. Use `--parix-clearing-initialize` exactly once only
after proving that this profile-bound CSG1 has never existed; omit it on every
restart. The raw authenticated exchange listener and provider HTTP listener
must use distinct loopback ports.

## Durable startup

The two store policies are deliberately separate:

- `initialize` requires `clearing-sagas.csg1` to be absent and publishes an
  empty, profile-bound snapshot;
- `open_existing` requires it to exist and validates and recovers it before
  admission.

Never infer initialization from a missing snapshot during normal startup.
`parix_clearing_runtime.Runtime.init` owns and opens the CSG1 store according
to `Config.open_policy`; it borrows the clearing profile, TigerBeetle
`Transport`, and `FundingPort` for its entire lifetime. Use
`FundingRuntimeAdapter.asPort` to bind the existing funding runtime. Initialization
rejects a port whose destination or settlement-profile digest differs from the
clearing profile. `start` requires that exact port to report FSG1 admission open
and creates the clearing worker. Admission is available only while the worker
accepts work.

Start dependencies in this order:

1. Load and validate the settlement, clearing, and provider profiles and key.
2. Initialize the outbound Parix transport.
3. Start exchange-core and recover its native persistence.
4. Open and start FSG1, including live core-counter validation.
5. Complete the reviewed provider-control account verification ceremony.
6. Open or explicitly initialize CSG1.
7. Start settlement, funding, and then clearing workers.
8. Bind the second loopback listener and start provider admission only after
   all enabled components report admissible health.

Shutdown reverses the dependency: atomically fence provider admission, drain
every HTTP session and any uncancelable CSG1 admission, call
`Runtime.stopAndJoin` while FSG1 still reports running, then stop funding while
exchange-core is live, and finally stop the core and trade-settlement
transport. A provider-listener failure marks aggregate readiness false and
wakes the raw server loop. Call `Runtime.deinit` only after the worker has
joined.

## Side-effect certainty

Every CSG1 claim is durable before the runtime releases its mutex and calls an
external dependency. Completion is persisted after the call. The runtime maps
dependency evidence conservatively:

| Evidence | Action |
| --- | --- |
| Exact persisted TigerBeetle create | Commit the stage |
| Valid conflict response | Lookup the same deterministic transfer ID |
| Definitely not sent | Retry the byte-equivalent action |
| Ambiguous write | Lookup before any retry |
| Generic permanent transport response | Quarantine for operator action |
| Exact lookup | Commit the stage |
| Absent lookup after an ambiguous write | Retry the exact action |
| Immutable mismatch or unexpected partial result | Quarantine |

Child FSG1 status reconciliation carries the complete canonical request. An
existing operation is accepted only when operation ID, kind, UID, currency,
and amount all match. A same-ID/different-payload record is an idempotency
conflict, never success.

If publishing a claim or completion fails even before a storage write, the
runtime terminal-fails and fences admission. Restart then converts the durable
in-flight action to lookup or status reconciliation before any retry.

## Automatic authorization expiry

For each newly admitted authorization, CSG1 version 2 stores the absolute
deadline computed from the runtime's positive Unix-second clock plus the
profile-validated timeout. The deadline is part of the original durable
admission and an exact callback duplicate never extends it. CSG1 version-1
snapshots remain readable; their existing entries decode with deadline `0` and
are intentionally manual-only rather than being assigned a guessed deadline.
The next successful mutation writes the snapshot as version 2.

At or after the deadline, the worker durably records one deterministic full
void at authorization revision plus one. No external call occurs before that
commit. A ready action that is proven not sent is stopped. An exposed
TigerBeetle authorization is looked up before deciding whether to void it, and
an exposed FSG1 withdrawal debit is status-checked before deciding whether a
refund is required. If a pending transfer exists, the normal exact void path is
used. If no pending transfer exists, a deposit completes without a remote
effect; a withdrawal completes only after any committed local debit is refunded
exactly once. The worker never changes an in-flight claim's generation: the
claim first completes, or restart recovery converts it to stable lookup/status
work, before expiry is persisted.

Automatic expiry does not cancel the external provider operation. Its durable
resolution reserves the terminal revision, so a later provider post or void is
rejected as a conflict and requires the reviewed late-presentment policy. Do
not replay that late event with a new ID to bypass the conflict.

## Provider acknowledgement boundary

An adapter may acknowledge a provider event only after
`Runtime.acceptAuthorization` or `Runtime.acceptResolution` returns from the
durable CSG1 commit. `StorageAmbiguous` or a poisoned store must produce no
success acknowledgement and must fence new admission. Once admission begins,
a client disconnect, write deadline, or server shutdown cannot cancel it. A
lost acknowledgement can safely replay the same canonical event ID; divergent
reuse is rejected. The HTTP mapping is closed:

| Status | Meaning |
| --- | --- |
| `202 Accepted` | New canonical event was durably inserted in CSG1 |
| `200 OK` | Exact canonical event already exists durably |
| `400 Bad Request` | Invalid HTTP shape or authenticated event schema |
| `401 Unauthorized` | Invalid signature or timestamp outside the replay window |
| `409 Conflict` | Event identity, revision, state, or immutable-field conflict |
| `503 Service Unavailable` | Admission fenced, dependency unhealthy, or durability not proven |

Only the first two outcomes are success. Static error responses disclose no
runtime or storage detail. Use
`Runtime.status`, `Runtime.summary`, and `Runtime.operationsReport` for
read-only durable observations;
`Runtime.health` reports `cold`, `ready`, `pending`, `failed`, `quarantined`,
or `stopped`.

The authenticated exchange TCP listener exposes only the operator-only,
read-only `clearing-status PAYMENT_ID` and `clearing-operations` observations;
it accepts no clearing mutation or provider payload. Its `ping` readiness includes the optional
clearing runtime and provider host, while provider payloads use only the
separate loopback HTTP listener. The status line includes `expires_at` and
`resolution_source` (`none`, `provider`, or `expiration`) so an operator can
distinguish a provider terminal event from the automatic void. A named-vendor
wire adapter and live provider-rail calls remain separate rollout work.
Browser/WASM never receives Parix or provider credentials.

## Isolated lifecycle and restart rehearsal

Slice 4 is complete. Run any future rehearsal only against a dedicated,
isolated, non-production Parix database. A shared or billable database requires
separate explicit mutation approval; prior permission to inspect it is not
approval to create accounts, transfers, or clearing events.

Before first boot, satisfy and record every safety gate:

1. The database UUID is dedicated to this rehearsal, the settlement, clearing,
   ingress, and provider profiles all bind that destination, and no unrelated
   writer is active.
2. Every settlement account, every settlement-profile initial-funding transfer,
   and both provider-control accounts are provisioned. Exact account and
   transfer lookups confirm all governed immutable fields and route ledgers
   before `--parix-clearing-accounts-verified` is supplied. The flag is only an
   attestation; it performs no verification. Complete any approved account or
   initial-funding provisioning as a separate reviewed ceremony and archive its
   post-write lookups; do not silently add `--parix-create-accounts` or
   `--parix-development-seed` to the lifecycle command below.
3. The Parix credential, ingress credentials, and provider HMAC key are valid,
   scoped, owner-readable only, and absent from shell history and evidence
   files. The provider key environment variable named by the profile is set.
4. One new persistence root is allocated and included in the rehearsal backup.
   Prove that neither FSG1 nor CSG1 has existed there before using either
   one-shot initialization flag.
5. Start from the checked-in authorization and post fixture shapes, allocate a
   fresh payment ID and fresh event IDs, and archive the reviewed exact body
   bytes. Revision 1 is the authorization; revision 2 is its post. Do not reuse
   those IDs for another rehearsal.
6. Capture the pre-run exact TigerBeetle transfer query for the fresh payment
   ID and require zero rows. Successful Parix reads are metered, so include
   them in the approved scope.

On the proven first boot, supply both one-shot initialization flags exactly
once:

```sh
zig-out/bin/exchange-core-server \
  --parix-settlement \
  --parix-profile path/to/operator-settlement-profile.json \
  --ingress-auth-profile path/to/operator-ingress-profile.json \
  --parix-funding-initialize \
  --parix-clearing-profile path/to/operator-clearing-profile.json \
  --parix-provider-profile path/to/operator-provider-profile.json \
  --parix-clearing-accounts-verified \
  --parix-clearing-initialize \
  --provider-port 7380 \
  --persistence-root path/to/dedicated-durable-root
```

Require an authenticated `ping` to report aggregate readiness. Then capture
the baseline balance for UID `1001`, currency `11`, with the profile-bound
trader credential; call that local balance `B`:

```sh
EXCHANGE_INGRESS_KEY_FILE=path/to/operator-key \
zig-out/bin/exchange-core-client \
  --auth-instance ingress-instance-uuid \
  --auth-credential operator-credential-id \
  ping

EXCHANGE_INGRESS_KEY_FILE=path/to/trader-key \
zig-out/bin/exchange-core-client \
  --auth-instance ingress-instance-uuid \
  --auth-credential trader-credential-id \
  balance 1001 11
```

Send the archived authorization body and require `{"status":202}`. Because
that response proves the CSG1 insert rather than completion of downstream work,
poll the dedicated database until one exact pending transfer exists before
sending the post:

```sh
zig-out/bin/exchange-core-provider-client \
  --profile path/to/operator-provider-profile.json \
  --body-file path/to/archived-authorization.json \
  --port 7380

parix tb query-transfers dedicated-database-uuid \
  --base-url https://dev.parix.io \
  --ledger 421 \
  --code 20 \
  --user-data-128 fresh-payment-id \
  --limit 10 \
  --json
```

The pending row must have one nonzero ID; provider-control debit account;
treasury credit account; amount `25`; `pending_id=0`; the fresh payment ID in
`user_data_128`; provider ID in `user_data_64`; `user_data_32=1`; timeout
`60`; ledger `421`; code `20`; and only the TigerBeetle `pending` flag (`2`).
Archive that ID and require it to remain unchanged. A visible pending row proves
only that TigerBeetle applied the first remote stage; it can race CSG1's local
completion commit. Poll the operator-only, read-only status command for the
exact payment ID:

```sh
EXCHANGE_INGRESS_KEY_FILE=path/to/operator-key \
zig-out/bin/exchange-core-client \
  --auth-instance ingress-instance-uuid \
  --auth-credential operator-credential-id \
  clearing-status fresh-payment-id
```

Require the same payment and authorization event IDs, authorization revision
`1`, a positive future `expires_at`, `state=authorized`, `resolution=none`,
`resolution_source=none`, and `reason=none`. `accepted` or
`pending` is transient and may be polled again within the rehearsal's bounded
deadline; `failed`, `quarantined`, not-found, unavailable, or any field drift
is an abort. Only the exact `authorized` status proves that CSG1 may accept the
resolution. Then send the archived post body once and require `{"status":202}`:

```sh
zig-out/bin/exchange-core-provider-client \
  --profile path/to/operator-provider-profile.json \
  --body-file path/to/archived-post.json \
  --port 7380
```

Poll the same `clearing-status` command until it reports the same payment and
authorization fields, `state=completed`, `resolution=post`, the archived post
event ID, resolution revision `2`, `resolution_source=provider`, and
`reason=none`. Then poll the authenticated
`balance 1001 11` command until it is exactly `B + 25`. Repeat the filtered
Parix query and archive exactly two rows: the unchanged pending row and one post
row. The post must have its own distinct nonzero ID, the pending transfer ID in
`pending_id`, the same accounts, amount, payment and provider metadata, timeout
zero, ledger and code, and only `post_pending_transfer` (`4`). Archive both IDs;
no third row is permitted.

Request graceful shutdown with the operator credential and wait for the server
process to exit successfully before touching the persistence root:

```sh
EXCHANGE_INGRESS_KEY_FILE=path/to/operator-key \
zig-out/bin/exchange-core-client \
  --auth-instance ingress-instance-uuid \
  --auth-credential operator-credential-id \
  shutdown
```

Restart the same binary against the same unchanged database, profiles, and
persistence root, using the first-boot command above with both
`--parix-funding-initialize` and `--parix-clearing-initialize` removed. Do not
replace a missing store or change a digest to make startup pass. Require a
signed `ping` to report aggregate readiness, then resend the two archived body
files in authorization-then-post order. Each invocation signs the unchanged
body with a fresh current timestamp and must return `{"status":200}`.

After duplicate replay, require `clearing-status` to return the identical
completed/post evidence, and prove that the authenticated local balance is
still exactly `B + 25`. Run and archive the same filtered Parix query: it must
still contain exactly the same two immutable clearing-transfer rows and no
extra pending, post, or void row. Compare the pre-shutdown and post-restart
outputs field by field, not merely their counts or amounts. The balance check is
the separate proof that duplicate callbacks did not repeat the child FSG1
credit; the filtered code-20 query alone cannot prove funding-transfer count.

Abort immediately on any profile/database/digest drift, missing or unexpected
account, shared-state ambiguity, unexpected pre-existing transfer, non-ready
health, missing snapshot, startup request for an initialization flag, client
status other than the expected `202` or `200`, HTTP `401`, `409`, or `503`,
quarantine or failed health, balance other than the expected value, or an extra
or mismatched Parix row. Fence admission, preserve the root and exact evidence,
and reconcile; do not retry with new IDs, reinitialize, create a compensating
transfer, or edit a snapshot to force progress.

## Completed shared-development rehearsal

The approved rehearsal completed on 2026-08-02 against the shared development
database `exchange-core`, ID `0e46f90e-c38b-487a-b9f9-630f7839cc3f`, in
organization `acme2` at `https://dev.parix.io`. The active Developer database
reported TigerBeetle `0.17.6`. Exact preflight reads confirmed all eight
settlement accounts and six existing transfers. One explicitly approved
stable-ID batch provisioned only provider-control account `430011` on ledger
`421` and account `430015` on ledger `425`, each with code `220`, `history`
flags (`8`), zero user-data fields, and zero balances. A database-scoped API key
was created for the native runtime; its one-time secret is not evidence and is
not stored in this repository.

Fresh payment `920260802000000000000` used authorization event
`920260802000000000001` and post event `920260802000000000002`. Exact preflight
querying returned no transfer. First boot accepted both callbacks with `202`,
advanced `clearing-status` from authorized to completed/post, and advanced UID
`1001` currency `11` from `1000` to `1025`. Parix retained exactly pending
transfer `284337591712546989007383477003008732256` and post transfer
`177340896706004521714231805841213082740`; both have amount `25`, ledger `421`,
code `20`, provider `7001`, and the expected account and pending-link fields.

The first untouched-root restart failed closed with `CorePoisoned`: XCJ3 had a
non-trade clearing-credit prefix, but the lazily empty trade-settlement outbox
had never written a durable image. `DurableFileOutbox.ensureDurableRecoveryAnchor`
now atomically seeds a checksummed empty snapshot and immediately fences it for
common-frontier recovery before hooks are installed. The regression deletes
that snapshot to reproduce the legacy layout, writes a non-trade XCJ3 command,
and proves one-start recovery with zero transport submissions.

The rebuilt server then recovered the preserved root without either
initialization flag. Signed readiness reported settlement, funding, clearing,
and provider ingress ready; the completed/post status and balance `1025` were
unchanged. Replaying the exact authorization and post bodies returned `200`
for both. The final exact Parix query still contained only the same two transfer
IDs with identical fields, and graceful shutdown exited successfully. No third
transfer, repeated local credit, compensating transfer, reinitialization, or
snapshot edit occurred. Successful Parix reads remain metered.

## Recovery and operations

On restart, an exposed TigerBeetle submission becomes exact-ID lookup work. An
exposed FSG1 submission becomes status reconciliation with its original
certainty cause. Neither path blindly resubmits.

### Operations snapshot

Use the operator credential to capture one atomic read of durable CSG1 state:

```sh
EXCHANGE_INGRESS_KEY_FILE=path/to/operator-key \
zig-out/bin/exchange-core-client \
  --auth-instance ingress-instance-uuid \
  --auth-credential operator-credential-id \
  clearing-operations
```

The command takes the runtime clock and CSG1 mutex, then releases both without
changing the store or contacting TigerBeetle, FSG1, or the provider. Record the
complete line with the incident timeline. Its fields mean:

- `capacity`, `total`, and the six public-state counts describe retained CSG1
  occupancy; terminal entries are not pruned;
- `ready` and `in_flight` count executable durable actions, while
  `remote_reconcile` and `funding_reconcile` isolate exact TigerBeetle lookup
  and FSG1 status work;
- `provider_resolutions`, `expiration_resolutions`, `remote_attempts`, and
  `funding_attempts` are cumulative over retained entries;
- `expired_unresolved`, `max_deadline_lag`, and `oldest_open_age` use the
  reported `observed_at` clock; expiry resolutions already in progress move
  out of `expired_unresolved` and remain visible as ready or in-flight work;
- `legacy_manual_expiry` counts nonterminal CSG1 v1 entries with no invented
  deadline;
- the four payment fields identify deterministic incident starting points.
  Value `0` means no such entry; they are neither cursors nor acknowledgements.

Alert on nonzero failed/quarantined counts, sustained reconciliation or
deadline backlog, attempt growth without state progress, runtime health other
than `ready`/`pending`, and operator-defined capacity headroom. Also monitor
provider duplicate/conflict and late-terminal rates at ingress, because
rejected events are intentionally not written into CSG1 counters.

### Reconciliation triage

1. Capture two `clearing-operations` lines far enough apart to distinguish
   progress from a stable backlog. Do not retry a callback under a new event
   ID.
2. Query every nonzero representative with `clearing-status PAYMENT_ID` and
   archive its authorization event, revision, deadline, resolution source,
   state, and reason.
3. For `remote_reconcile`, verify the deterministic pending/post/void rows in
   the bound Parix database. For `funding_reconcile`, compare the deterministic
   child operation with FSG1 and the corresponding core balance history. Treat
   mismatched immutable fields as quarantine evidence, never as permission to
   overwrite either side.
4. Compare provider reports and bank statements to the same payment ID and
   amount. A provider callback is not authoritative over contradictory durable
   TigerBeetle or FSG1 evidence.
5. Restore dependency health and let the existing exact lookup/status action
   converge. Never manually create the expected transfer, edit CSG1/FSG1, or
   blind-resubmit an exposed operation.
6. If the representative or counters do not progress, fence provider
   admission, drain the HTTP host, preserve the full persistence root, and
   escalate with all four evidence sets before deciding any compensating
   business action.

### Coordinated backup and restore

An online copy is not a coordinated backup. Quiesce in dependency order:

1. Fence and drain provider admission so no callback can enter CSG1.
2. Capture and record the final operations line after the drain and immediately
   before worker shutdown, together with the exact profile/manifest digests.
3. Stop and join clearing while FSG1 is still running, then stop funding while
   exchange-core is live; stop core and trade settlement last.
4. Copy the persistence root as one unit: XCJ3, the settlement outbox, FSG1,
   CSG1, their lock files, profiles, and manifests. Produce checksums and retain
   the database UUID, destination, and backup generation in external metadata.
   Keep secret keys in the secret manager, not in the persistence archive.

Restore the whole generation into an empty, owner-only root on the intended
destination. Verify checksums, file permissions, database UUID, profile
digests, and destination binding before startup. Use only `open_existing`;
never pass either initialization flag to repair a restore. Start the core,
settlement, FSG1, and clearing in their normal order, verify the restored total
and immutable event identities against the recorded final line, and account
for any expected lookup/status recovery progress before enabling provider
admission last. A partial or mixed-generation restore is an incident and must
remain fenced.

Run the restore drill against an isolated destination with recorded or fake
transports. The drill passes only when profile-bound stores open without
initialization, total/terminal counts and immutable identities agree, any
ready/in-flight change is explained by restart conversion to lookup/status
work, and no blind external submit occurs. Never point a drill copy at the
production TigerBeetle database.

### Incident actions

| Evidence | Immediate action | Recovery condition |
| --- | --- | --- |
| `health=failed` or CSG1 ambiguity | Fence provider admission; preserve files and logs; do not delete or initialize CSG1 | Exact existing generation opens and dependency evidence is reconciled |
| `health=quarantined` or nonzero `quarantined` | Inspect `first_quarantined_payment`; compare immutable CSG1, FSG1, TigerBeetle, and provider evidence | Reviewed evidence explains the mismatch and an approved recovery path exists |
| Growing `remote_reconcile` or `funding_reconcile` | Restore the affected dependency; allow only exact lookup/status convergence | Counts and attempts stabilize, then fall without conflicting rows |
| Nonzero `expired_unresolved` or growing deadline lag | Check worker, clock, FSG1, and TigerBeetle health; do not inject a manual void | Automatic expiry is durably visible and the backlog drains |
| Nonzero `legacy_manual_expiry` | Apply the reviewed legacy/provider-resolution policy; never synthesize a deadline | Every legacy item has provider evidence or an approved manual disposition |
| Capacity nearing `total=capacity` | Fence new callbacks and plan an audited retention migration | Sufficient headroom exists before admission resumes |

Verification gates:

```sh
zig build -j1 test-parix-clearing --summary all
zig build -j1 test-parix-clearing-release-safe --summary all
zig build -j1 test-parix-clearing-provider-adapter --summary all
zig build -j1 test-parix-clearing-provider-adapter-release-safe --summary all
zig build -j1 test-parix-clearing-provider-client --summary all
zig build -j1 test-parix-clearing-provider-client-release-safe --summary all
zig build -j1 test-parix-clearing-http-ingress --summary all
zig build -j1 test-parix-clearing-http-ingress-release-safe --summary all
zig build -j1 gate --summary all
zig build -j1 wasm-smoke --summary all
```
