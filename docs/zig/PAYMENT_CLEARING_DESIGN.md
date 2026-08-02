# Payment-provider clearing design

Status: rollout slices 1 through 6 are implemented. The pure clearing saga,
lifecycle mapper, deterministic recovery campaign, durable `CSG1` store,
strict clearing profile, app-owned native runtime, transport classifier, and
typed FSG1 port are joined by a versioned provider HMAC adapter and bounded
loopback HTTP host in the Debug and ReleaseSafe build graph. Slice 4's native
signed-callback client, operator-only status observation, and exact isolated
first-boot/restart procedure are implemented and verified by an approved live
authorize/post/restart/duplicate rehearsal. That rehearsal also closed the
legacy empty-outbox recovery-anchor gap before rollout completion. Slice 5 adds
durable admission-anchored expiry, deterministic automatic voids, and exact
FSG1/TigerBeetle reconciliation before compensation. Public TLS/mTLS, a
named-vendor adapter, and live provider-rail calls remain external or follow-up
work. Slice 6 adds a clock-anchored, read-only operations snapshot and the
coordinated reconciliation, backup/restore, and incident procedures that use
it. See
[`PARIX_CLEARING_RUNBOOK.md`](PARIX_CLEARING_RUNBOOK.md) for the exact
authentication, composition, and recovery contract.

## Decision

Payment-provider clearing is an optional native application plugin beside the
exchange engine. It does not change matching, risk, the final-fill settlement
outbox, or browser/WASM behavior.

The committed funding store remains `FSG1` version 1 and remains the only owner
of native-core balance-adjustment transaction IDs. Clearing uses a separate
`CSG1` version-2 store and never calls the core balance adapter directly.
Version 2 reads version-1 snapshots; legacy entries have no inferred deadline
and remain manual-only until a provider resolution arrives. The next durable
mutation rewrites the whole snapshot as version 2. Clearing requests
user-facing balance changes through deterministic child operations on the
existing funding runtime.

This prevents two durable stores from allocating the same per-user monotonic
core counter and keeps existing persistence roots backward-compatible when
clearing is disabled.

## Economic model

Each currency route binds one Parix destination and ledger to distinct treasury
and provider-control accounts. The provider-control account represents value at
the external payment rail; it is not a customer balance and must be reconciled
against provider or bank statements.

### Deposit

1. Persist a verified provider authorization in `CSG1`.
2. Create a pending transfer from provider control to treasury.
3. A verified provider success posts that pending transfer.
4. Only after exact post proof, submit a deterministic child FSG1 deposit from
   treasury to the user and credit the native core.
5. A verified provider cancellation voids the pending transfer and creates no
   user funding operation.

### Withdrawal

1. Persist a verified provider authorization in `CSG1`.
2. Complete a deterministic child FSG1 withdrawal, debiting the native core and
   moving the user's TigerBeetle balance to treasury.
3. Create a pending transfer from treasury to provider control.
4. A verified provider success posts that pending transfer.
5. A verified void refunds the full amount through a deterministic child FSG1
   deposit. A partial post refunds only the released remainder.

`CSG1` reaches a successful terminal state only after both its required
TigerBeetle lifecycle record and every required child FSG1 operation are proven
complete.

## TigerBeetle lifecycle contract

The raw flags are:

| Record | Flag | `pending_id` | `timeout` | Amount |
| --- | ---: | ---: | ---: | --- |
| Pending authorization | `pending` (`2`) | `0` | positive seconds | authorized amount |
| Full or partial post | `post_pending_transfer` (`4`) | pending transfer ID | `0` | positive and no greater than authorized |
| Void | `void_pending_transfer` (`8`) | pending transfer ID | `0` | original authorized amount |

Post and void use new stable transfer IDs and copy the original external debit
account, credit account, ledger, and code. A partial post is terminal: it posts
the supplied amount and releases the remainder. The application rejects a
zero-amount post even if a TigerBeetle version would accept it.

The trade-settlement outbox remains intentionally single phase. Its validation
must continue rejecting nonzero `pending_id`, nonzero `timeout`, and clearing
lifecycle flags.

## Provider event contract

A provider adapter verifies the provider-specific signature and normalizes a
message before calling the clearing layer. Network ownership, raw payloads,
secrets, PII, account numbers, and replay-window policy remain outside the pure
saga and outside TigerBeetle.

The canonical event contains:

- nonzero provider, event, and payment identities;
- a positive monotonic provider revision;
- direction, user ID, currency, and integer minor-unit amount;
- `authorize`, `post`, or `void` kind;
- a positive timeout in seconds for authorization only.

The same event ID with the exact same canonical payload is idempotent. Reusing
an event ID with different fields is an idempotency conflict. A terminal event
must preserve the authorization's provider, payment, direction, user, and
currency, and must use the next revision. Stale, skipped, mismatched, and
competing terminal events fail before mutation.

Provider acknowledgement is allowed only after event acceptance is durable.
The native adapter verifies an exchange-owned HMAC-SHA256 v1 transcript over
the provider ID, method, target, canonical timestamp, and exact raw body before
bounded JSON normalization. Its separate HTTP listener binds only to loopback;
an operator-owned L7 proxy terminates public TLS or mTLS without weakening the
end-to-end provider MAC. The raw TCP exchange command listener is still not a
webhook server and exposes no clearing mutation command.

## Stable identity

There is no request-level TigerBeetle idempotency key. Persist complete immutable
payloads before the first write and derive domain-separated high-bit `u128`
identities for:

- pending transfer;
- post or void transfer;
- initial withdrawal child funding;
- deposit credit child funding;
- withdrawal refund child funding.

Retries reuse the same ID and byte-equivalent economic fields. Existing IDs are
successful only after exact lookup comparison; an immutable mismatch is a
quarantine condition.

## Durable state and recovery

`CSG1` is a strict, checksummed, whole-snapshot format with an exclusive lock,
owner-only permissions, atomic replacement, parent-directory synchronization,
and poison-on-ambiguous-storage semantics. It binds:

- Parix destination;
- settlement-profile digest;
- clearing-profile digest;
- provider identity or registry digest;
- compile-time capacity and format version.

Every external action has a durable ready and in-flight phase. Reopening an
in-flight TigerBeetle submission creates exact-ID lookup work before any retry.
Child FSG1 submit certainty is also durable: known-not-sent work retries the
same deterministic operation ID, ambiguous submission polls status before any
retry, and disappearance of a previously accepted operation quarantines the
entry. Reopening preserves that distinction. A snapshot with corruption,
trailing bytes, identity drift, invalid phases, or contradictory terminal
evidence fails closed.

## Outcome policy

- Known-not-sent: retry the identical immutable action with bounded backoff.
- Ambiguous write: lookup first; accept an exact match and retry only proven
  absence.
- Generic permanent transport or policy failure: quarantine because it does not
  prove whether value moved.
- Destination or immutable-field mismatch: quarantine.
- Provider-authoritative decline or cancellation: use the explicit void path;
  never reinterpret a generic HTTP failure as a provider decision.
- Post/void race, already-resolved pending transfer, or expiry conflict: stop
  automatic retries and reconcile provider evidence against ledger history.

Expiration releases the TigerBeetle reservation but does not cancel an external
provider operation. Late presentment requires an explicit reviewed policy. A
return after a successful post is a new compensating workflow and transfer code,
not a void of the completed authorization.

For new entries, CSG1 persists `accepted Unix seconds + timeout seconds` in the
same commit that acknowledges the authorization. Exact duplicate delivery does
not move that deadline. At or after it, the worker persists one domain-separated
deterministic full void at the authorization's next revision before issuing any
new side effect. Work proven not sent is stopped. Exposed TigerBeetle writes are
looked up and exposed FSG1 withdrawals are status-checked first; an exact pending
transfer is voided, while a committed local withdrawal debit is refunded exactly
once. An in-flight claim is never invalidated by the clock: completion or restart
recovery first produces stable reconciliation work, then expiry is recorded.
The automatic resolution makes any later provider post or void a terminal
conflict for operator review.

## Profile and account requirements

The clearing profile is separate from the existing settlement profile and must
agree with its database, treasury account, currency, and ledger routing. It adds
the provider-control account, transfer code, provider identity, and timeout
bounds. Enabling clearing must be explicit; a missing `CSG1` requires a separate
one-shot initialization ceremony and is never inferred as first startup.

The native application layer now implements this as a bounded, strict JSON
loader paired with one validated settlement profile. Its app-owned runtime
borrows the profile, TigerBeetle transport, and typed FSG1 port; it owns the
`CSG1` store and worker. The runtime persists and releases a claim before every
external call, classifies submit and lookup evidence conservatively, and
requires the FSG1 destination and settlement-profile digest to match exactly.
FSG1 remains running through clearing shutdown. The optional native server
composition loads both clearing profiles, starts the runtime after funding,
then starts the distinct provider listener. Shutdown fences and drains the
provider listener before clearing and funding stop. Disabled configurations
retain the existing native path.

Account flags and codes are governed configuration. Customer or funded asset
accounts normally use `history | debits_must_not_exceed_credits`; control-account
constraints must follow the reviewed accounting model. IDs and user-data fields
contain only opaque non-PII correlations.

## Operational invariants

- At most one pending reservation exists per payment.
- Exactly one post or void may resolve it.
- No customer credit occurs before exact deposit post proof.
- No outbound reservation begins before the child FSG1 withdrawal completes.
- A partial outbound post refunds exactly `authorized - posted`.
- CSG1 never allocates, observes, or applies native-core transaction IDs.
- Ambiguous writes never trigger a blind resubmit.
- Matching, risk, fill publication, and WASM contain no provider credentials or
  clearing transport.

Monitor accepted and pending counts, oldest pending age, submit and lookup
attempts, provider revision conflicts, exact duplicates, post/void conflicts,
expired authorizations, child-funding lag, quarantine reason, storage health,
and retained capacity. Reconciliation compares CSG1, FSG1, TigerBeetle account
history, provider reports, and bank statements.

The native `clearing-operations` observation is computed under the CSG1 mutex
at the runtime clock without mutating the store or contacting a dependency. It
reports capacity and public-state counts; ready and in-flight actions;
TigerBeetle lookup and FSG1 status-reconciliation backlogs; provider and
automatic-expiry resolution counts; cumulative attempt counts; unresolved
deadline lag; legacy manual-expiry entries; and deterministic representative
payment IDs for expired, reconciliation, failed, and quarantined work. These
IDs are incident starting points, not cursors or acknowledgements.

Coordinated backup and recovery includes XCJ3, the trade-settlement outbox,
FSG1, CSG1, manifests, both profiles, and lock metadata. Never restore one store
into an unrelated persistence root.

## Rollout slices

1. **Complete:** pure clearing saga, transfer mapper, deterministic recovery
   simulation, and durable CSG1 store.
2. **Complete:** strict clearing profile, transport classifier, app-owned
   native runtime, and typed FSG1 port/adapter.
3. **Complete:** versioned provider HMAC adapter, loopback HTTP ingress,
   durable acknowledgement mapping, and native lifecycle composition.
4. **Complete:** native loopback signed-callback client,
   operator-only read-only clearing status, and exact isolated
   first-initialization, lifecycle, graceful-shutdown, same-root restart,
   duplicate, and exactly-once evidence procedure, verified by the approved
   development authorize/post/restart/duplicate rehearsal.
5. **Complete:** durable authorization deadlines, deterministic automatic
   voids, withdrawal compensation, restart-safe ambiguous-effect
   reconciliation, CSG1 v1 reads, and operator-visible deadline/source fields.
6. **Complete:** deterministic CSG1 operations snapshot, authenticated native
   operator observation, reconciliation triage, coordinated backup/restore,
   incident response, and recovery-drill procedures.
