# Authenticated ingress runbook

This runbook covers the authenticated native TCP ingress used with the
Parix/TigerBeetle settlement server. Authentication and settlement are paired:
`--parix-settlement`, `--parix-profile`, and `--ingress-auth-profile` must be
supplied together. Omitting all three keeps the loopback-only, unauthenticated
development server.

The matching core is still an in-process engine. Authentication, request
authorization, and the TCP framing described here belong to the native server
adapter under `src/zig/apps/`; the Parix/TigerBeetle host implementation is an
opt-in package under `src/zig/plugins/tigerbeetle/parix/`.

## Security boundary

The server listens only on `127.0.0.1` or `localhost`. Its TCP hop is plaintext:
the challenge-response protocol authenticates both endpoints and signs the one
command and response, but it does not encrypt their contents. For remote access,
terminate TLS or mTLS at an operator-owned L4 proxy and forward only to the
loopback listener. Never bind or publish port `7379` on a public or LAN address.

The bundled browser/WASM desk is unrelated to this ingress. Its simulated
clients share one exchange inside one browser tab and never contact the native
server. Browsers cannot speak this raw TCP protocol; a production browser
client needs a separate HTTPS/WebSocket gateway that keeps all ingress and
Parix secrets server-side.

## Auth profile

The ingress profile is strict, bounded, non-secret JSON. Its `instance_id` must
be the same canonical database UUID as both `PARIX_DATABASE_ID` and the
operator-owned Parix settlement profile. Each credential maps to one principal:

```json
{
  "version": 1,
  "instance_id": "0e46f90e-c38b-487a-b9f9-630f7839cc3f",
  "credentials": [
    {
      "credential_id": "trader-1001-a",
      "principal_id": "trader-1001",
      "role": "trader",
      "uid": 1001,
      "secret_file_env": "EXCHANGE_INGRESS_TRADER_1001_KEY_FILE"
    },
    {
      "credential_id": "trader-1002-a",
      "principal_id": "trader-1002",
      "role": "trader",
      "uid": 1002,
      "secret_file_env": "EXCHANGE_INGRESS_TRADER_1002_KEY_FILE"
    },
    {
      "credential_id": "operator-a",
      "principal_id": "local-operator",
      "role": "operator",
      "secret_file_env": "EXCHANGE_INGRESS_OPERATOR_KEY_FILE"
    }
  ]
}
```

The checked-in
[`src/zig/fixtures/ingress-auth-profile.example.json`](../../src/zig/fixtures/ingress-auth-profile.example.json)
contains this non-secret development shape. Copy it to an operator-managed path
before changing identities; do not add key material to it.

The profile contains at most 32 credentials and must include at least one
trader and one operator. Credential IDs are unique. A trader has exactly
one positive `uid`; an operator has no `uid`. Multiple credential IDs may map
to the same principal so a key can be rotated without changing application
identity; repeated `principal_id` values must retain the identical role and
trader UID. `instance_id` is a canonical, non-zero lowercase UUID. Credential
and principal IDs contain 1–64 ASCII letters, digits, `.`, `_`, or `-`.
Environment-variable names contain at most 64 ASCII uppercase letters, digits,
or `_`, and begin with an uppercase letter or `_`. The profile contains
environment-variable names, never key bytes or secret-file paths. Unknown or
duplicate fields, unknown roles, duplicate credential IDs, duplicate
`secret_file_env` names, duplicate decoded keys, and invalid role/UID shapes
fail startup. Every trader UID must also exist in the paired settlement profile.

## Secret files

Each `secret_file_env` variable points to a regular, owner-only file with mode
`0600`. The file contains one 32-byte key encoded as exactly 64 lowercase
hexadecimal characters with an optional single trailing newline. It must not be
a symlink and must not contain spaces, a `0x` prefix, carriage returns, or extra
lines. An all-zero decoded key is rejected.

Generate each credential independently in a private secrets directory:

```sh
umask 077
openssl rand -hex 32 > /run/secrets/exchange-ingress-trader-1001.key
openssl rand -hex 32 > /run/secrets/exchange-ingress-trader-1002.key
openssl rand -hex 32 > /run/secrets/exchange-ingress-operator.key
chmod 600 /run/secrets/exchange-ingress-*.key
```

Configure the server with paths, not key material:

```sh
export EXCHANGE_INGRESS_TRADER_1001_KEY_FILE=/run/secrets/exchange-ingress-trader-1001.key
export EXCHANGE_INGRESS_TRADER_1002_KEY_FILE=/run/secrets/exchange-ingress-trader-1002.key
export EXCHANGE_INGRESS_OPERATOR_KEY_FILE=/run/secrets/exchange-ingress-operator.key
```

The client reads its selected credential through the generic
`EXCHANGE_INGRESS_KEY_FILE` variable. Treat it and the two client flags as one
configuration triad: supply non-empty `EXCHANGE_INGRESS_KEY_FILE`,
`--auth-instance`, and `--auth-credential` together, or omit all three for
local development mode. Set the generic variable only for the client
invocation. Never put a key in command-line arguments, logs, screenshots,
source files, the auth profile, or browser storage.

## Start the server

Load the outbound Parix environment and ingress key-file paths, then start the
server with both profiles and a stable persistence root:

```sh
set -a
source .env
set +a

zig build run-server -- \
  --parix-settlement \
  --parix-profile path/to/operator-settlement-profile.json \
  --ingress-auth-profile path/to/operator-ingress-profile.json \
  --persistence-root zig-out/runtime/parix
```

For the proven first use of this database/profile/persistence-root tuple, add
`--parix-funding-initialize` once. It creates an absent funding store only when
the live core counters match the configured floors and rejects an existing
store. Remove it for restarts; a missing store after initialization is a
recovery incident, not permission to initialize again.

Startup fails before serving requests when the profiles are not paired, the
three database identities differ, a credential entry is invalid, a named
environment variable is absent, or a secret file fails its type, mode, size,
or encoding checks.

## Wire protocol

An authenticated connection carries exactly one command. All nonces and
HMAC-SHA256 values use their fixed lowercase hexadecimal encoding:

```text
client -> server  AUTH 1 <instance_uuid> <credential_id> <client_nonce>
server -> client  CHALLENGE 1 <server_nonce>
client -> server  PROOF 1 <proof>
server -> client  OK AUTH 1 <server_mac>
client -> server  REQ 1 <request_mac> <raw-command>
server -> client  RES 1 <response_mac> <raw-response>
server closes the connection
```

Each nonce and HMAC-SHA256 value is a 32-byte value encoded as exactly 64
lowercase hexadecimal characters. The HMAC/HKDF transcript binds the protocol
version, instance UUID, credential ID, and both fresh nonces; the strict server
profile separately resolves that credential to its principal, role, and trader
UID. Request and response MACs under the derived session key bind sequence `1`
and the exact raw command or response suffix. The suffix excludes its
terminating newline; changing whitespace or any other byte invalidates the
frame. Sequence `1` is the only accepted request sequence, and a second request
on the connection is never accepted. The client verifies the signed
authentication ACK and signed response before printing the inner
newline-delimited response.

The CLI exits nonzero when the handshake, frame, or MAC verification fails. A
verified signed inner `ERR` response is still printed exactly once and exits
zero, so automation must inspect the response payload.

Authentication failures return the deliberately generic
`ERR authentication AuthenticationFailed` line and close the connection.
Authorization and validly signed protocol/execution failures are returned as a
signed `RES` frame. Secrets, proofs, MACs, and derived keys must never be logged.

## Authorization

Authorization occurs before Parix profile admission, read barriers, or exchange
submission:

| Request | Trader | Operator |
| --- | --- | --- |
| `ping`, `book` | allowed | allowed |
| `balance`, `place`, `move`, `reduce`, `cancel` | allowed only for the profile-bound UID | denied |
| `fund-deposit`, `fund-withdraw`, `funding-status`, `clearing-status`, `clearing-operations` | denied | allowed |
| `shutdown` | denied | allowed |
| `add-user`, `deposit` | denied | denied |

Authenticated settlement users and initial balances come from the immutable
Parix settlement profile and its explicit provisioning controls. Operators can
request plugin-owned funding sagas, but cannot bypass them with the legacy
direct `deposit`. A denied request must not
publish a command, run a read barrier, consume exchange sequence, or reveal
another user's state.

Funding mutation syntax is
`fund-deposit UID CURRENCY AMOUNT OPERATION_UUID` or
`fund-withdraw UID CURRENCY AMOUNT OPERATION_UUID`; status lookup is
`funding-status OPERATION_UUID`. The UUID is a strict idempotency key. Exact
duplicates resolve to the retained operation, while a UUID reused with
different fields is rejected. The public states are `accepted`, `pending`,
`completed`, `failed`, and `quarantined`.

Payment clearing adds two observation-only operator commands.
`clearing-status PAYMENT_ID`, where the ID is canonical positive decimal
`u128`, returns the durable CSG1 public state plus authorization and optional
resolution identity. `clearing-operations` returns a clock-anchored aggregate
of capacity, states, ready/in-flight actions, reconciliation work, attempts,
expiry lag, legacy entries, and representative incident payment IDs. Neither
accepts a provider body, performs dependency I/O, or mutates clearing or the
exchange. Use the per-payment status to require
`state=authorized` before sending a provider resolution and `state=completed`
afterward; a TigerBeetle pending row alone can race the local CSG1 completion
commit.

## Health, readiness, and shutdown

A successful TCP connection proves neither authentication nor readiness. Use a
signed `ping` and require both `ready=true` and `settlement=ready` before routing
mutations:

```sh
EXCHANGE_INGRESS_KEY_FILE="$EXCHANGE_INGRESS_OPERATOR_KEY_FILE" \
  zig build run-client -- \
    --auth-instance "$PARIX_DATABASE_ID" \
    --auth-credential operator-a \
    ping
```

Use the same authenticated probe after restart. Configure the TLS proxy with
bounded connection counts, absolute handshake/connection deadlines,
request-rate limits, and a readiness check that runs the authenticated probe
rather than treating an open TCP port as ready.

The native authenticated client applies one five-second absolute deadline from
the start of TCP connection setup through verification of the signed response.
Expiry cancels the attempt, emits no unverified response on stdout, and exits
nonzero. This keeps a readiness probe bounded, but it does not replace the
server-side deadline and rate controls at the TLS proxy.

As loopback defense in depth, the native authenticated listener admits at most
64 concurrent sessions. On POSIX it also applies a five-second idle receive
timeout while waiting for each client frame. This is not an absolute handshake
deadline or a substitute for the proxy controls; exchange execution is
serialized after authentication and authorization.

Only an operator may request shutdown:

```sh
EXCHANGE_INGRESS_KEY_FILE="$EXCHANGE_INGRESS_OPERATOR_KEY_FILE" \
  zig build run-client -- \
    --auth-instance "$PARIX_DATABASE_ID" \
    --auth-credential operator-a \
    shutdown
```

`OK shutdown` acknowledges admission of the shutdown request. The process still
must stop accepting work, close or drain connection handlers, shut down the
exchange core, give the settlement worker its bounded drain window, and join it.
A drain failure produces a nonzero process exit while preserving the durable
outbox for recovery from the same persistence root.

## Trader smoke

Each trader uses its own credential and may name only its bound UID:

```sh
EXCHANGE_INGRESS_KEY_FILE="$EXCHANGE_INGRESS_TRADER_1001_KEY_FILE" \
  zig build run-client -- \
    --auth-instance "$PARIX_DATABASE_ID" \
    --auth-credential trader-1001-a \
    balance 1001 11

EXCHANGE_INGRESS_KEY_FILE="$EXCHANGE_INGRESS_TRADER_1002_KEY_FILE" \
  zig build run-client -- \
    --auth-instance "$PARIX_DATABASE_ID" \
    --auth-credential trader-1002-a \
    book 241 10
```

A foreign UID, an operator trading command, trader funding or clearing
observation command, `add-user`, or legacy `deposit` returns a
signed authorization error without changing exchange state. Do not use a live
shared database for these examples.

## Funding recovery

Funding is an application/plugin-owned saga; it does not change matching or
risk behavior. Deposits create the TigerBeetle treasury-to-user transfer before
the idempotent local credit. Withdrawals apply the local debit before creating
the user-to-treasury transfer; only an explicit authoritative rejection permits
compensation and `failed`. Generic permanent policy/quota/misconfiguration
responses, ambiguous remote evidence, immutable-field mismatch, or an
unprovable local replay finish `quarantined` and block automated repair.

The server stores checksummed `FSG1` funding snapshots and a single-process
lock under the persistence root. The store binds the Parix destination and
profile identity/digest, persists intent before every side effect, uses
deterministic high-bit TigerBeetle transfer IDs, and assigns monotonic local
transaction IDs per UID above the floor derived from configured initial
funding. It also retains confirmed per-UID core frontiers and compares them with
serialized native core reports before startup I/O. On restart it performs exact
transfer lookup or idempotent core replay. The durable lookup phase retains
whether reconciliation followed an ambiguous write or a conflict response, so
an absent conflict lookup quarantines instead of resubmitting or compensating
without proof. Normal startup requires `FSG1` to exist;
`--parix-funding-initialize` is a one-shot first-bootstrap ceremony that
requires it to be absent and rejects an existing store. Keep the same database,
profiles, credentials, and persistence root; do not delete or hand-edit the
snapshot or lock around a quarantine, and never use the initialization flag as
missing-file recovery.

For an incident, stop funding admission, preserve and back up the full
persistence root, record the operation UUID and last state, and use read-only
Parix transfer/account lookup plus local journal evidence to establish which
side effect committed. Do not submit a replacement UUID, manually adjust the
core, or compensate TigerBeetle until the evidence has been reviewed. Version
1 retains terminal records without pruning, so capacity exhaustion requires a
planned offline migration to a larger store rather than deleting history.

## Rotate a credential

1. Generate a new independent key file with mode `0600`.
2. Add a new unique credential ID for the same principal/UID and a new
   `secret_file_env` name to the ingress profile.
3. Make the new key file and environment variable available, then restart the
   server and confirm authenticated readiness.
4. Move the client to the new credential ID and key file and verify a signed
   request.
5. Remove the old credential from the profile, restart, and confirm the old
   credential fails before deleting its key file.

Never overwrite a key file in place while a process is using it. Rotation does
not change the Parix database UUID, publication domain, settlement profile, or
persistence root.

## Credential incident

If a key may be exposed, stop routing the affected principal, remove that
credential ID from the profile, restart the server, and verify that the old
credential fails. Rotate any other credentials that shared the compromised
storage or delivery path. Preserve the journal, outbox, settlement profile, and
persistence root for investigation and recovery; do not delete durable state as
part of credential revocation. Correlate the incident with non-secret
server/proxy telemetry, but never collect proofs, MACs, keys, or raw secret-file
contents.

Challenge nonces and one-command session keys prevent a captured signed request
from being replayed on a new connection. They do not make a stolen long-term key
safe; revocation and rotation are still required.

## Remaining boundaries

- The protocol is raw TCP, not FIX, HTTP, WebSocket, or a browser API.
- TLS/mTLS termination, public certificates, network ACLs, connection caps, and
  rate limiting remain operator-owned proxy responsibilities.
- Parix API credentials remain outbound server secrets and are independent of
  ingress credentials.
- Funding remains separate from payment-provider ingress. The native
  application layer now has the strict clearing profile, typed saga, durable
  `CSG1` store, app-owned runtime, classified TigerBeetle transport boundary,
  typed FSG1 adapter, and versioned HMAC provider adapter described in
  [`PARIX_CLEARING_RUNBOOK.md`](PARIX_CLEARING_RUNBOOK.md). Provider payloads
  use a distinct loopback HTTP port behind an operator-owned TLS/mTLS L7 proxy;
  this authenticated raw TCP ingress does not load or expose them. It exposes
  only the operator-only read-only clearing status; its `ping` readiness also
  includes enabled clearing/provider health. Expiry automation, a named-vendor
  adapter, and live provider-rail calls are not wired yet.
- Browser/WASM clients never receive funding commands, Parix credentials, or
  ingress secrets; a networked browser requires a separate server-side gateway.
