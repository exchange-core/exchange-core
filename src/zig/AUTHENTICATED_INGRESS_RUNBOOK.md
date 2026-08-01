# Authenticated ingress runbook

This runbook covers the authenticated native TCP ingress used with the
Parix/TigerBeetle settlement server. Authentication and settlement are paired:
`--parix-settlement`, `--parix-profile`, and `--ingress-auth-profile` must be
supplied together. Omitting all three keeps the loopback-only, unauthenticated
development server.

The matching core is still an in-process engine. Authentication, request
authorization, and the TCP framing described here belong to the native server
adapter under `src/zig/apps/`.

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
[`fixtures/ingress-auth-profile.example.json`](fixtures/ingress-auth-profile.example.json)
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
| `shutdown` | denied | allowed |
| `add-user`, `deposit` | denied | denied |

Authenticated settlement users and initial balances come from the immutable
Parix settlement profile and its explicit provisioning controls. The ingress
protocol cannot create users or adjust balances. A denied request must not
publish a command, run a read barrier, consume exchange sequence, or reveal
another user's state.

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

A foreign UID, an operator trading command, `add-user`, or `deposit` returns a
signed authorization error without changing exchange state. Do not use a live
shared database for these examples.

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
- Deposits and withdrawals are not mirrored settlement sagas and remain outside
  authenticated ingress.
