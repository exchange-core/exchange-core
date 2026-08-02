# Payment-provider clearing design

Status: rollout slice 1 is implemented: the pure clearing saga, lifecycle
mapper, deterministic recovery campaign, and durable `CSG1` store are included
in the Debug and ReleaseSafe build graph. Native runtime,
provider-authenticated ingress, and live rail calls are follow-up slices.

## Decision

Payment-provider clearing is an optional native application plugin beside the
exchange engine. It does not change matching, risk, the final-fill settlement
outbox, or browser/WASM behavior.

The committed funding store remains `FSG1` version 1 and remains the only owner
of native-core balance-adjustment transaction IDs. Clearing uses a separate
`CSG1` version-1 store and never calls the core balance adapter directly. It
requests user-facing balance changes through deterministic child operations on
the existing funding runtime.

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
The raw TCP exchange command listener is not a webhook server; a later HTTPS
host or gateway owns provider authentication and passes only verified events to
the typed plugin boundary.

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

## Profile and account requirements

The clearing profile is separate from the existing settlement profile and must
agree with its database, treasury account, currency, and ledger routing. It adds
the provider-control account, transfer code, provider identity, and timeout
bounds. Enabling clearing must be explicit; a missing `CSG1` requires a separate
one-shot initialization ceremony and is never inferred as first startup.

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

Coordinated backup and recovery includes XCJ3, the trade-settlement outbox,
FSG1, CSG1, manifests, both profiles, and lock metadata. Never restore one store
into an unrelated persistence root.

## Rollout slices

1. **Complete:** pure clearing saga, transfer mapper, deterministic recovery
   simulation, and durable CSG1 store.
2. Clearing profile, transport classifier, runtime, and typed FSG1 port.
3. Provider adapter boundary and provider-authenticated HTTPS ingress.
4. Isolated non-production Parix lifecycle and restart rehearsal.
5. Production observability, reconciliation, backup, and incident runbooks.
