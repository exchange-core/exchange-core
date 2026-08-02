# Optional Zig plugins

The matching, risk, command, report, and persistence engine lives in
`src/zig/core/`. Provider-specific integrations live here and depend inward on
the public `exchange_core` module. Core never imports or re-exports a plugin.
This keeps an ordinary core build free of provider transports, credentials,
account models, and delivery state machines.

## TigerBeetle

`src/zig/plugins/tigerbeetle/root.zig` is the generic opt-in package. It owns deterministic
transfer mapping, the durable outbox, destination registry, dispatcher,
transport-neutral facade, exchange integration, and pure Parix response codec.
It depends on the provider-neutral `core/settlement_lifecycle.zig` contract;
the core does not depend back on it.

`src/zig/plugins/tigerbeetle/parix/` is the native host package. It owns Parix profiles, HTTP
transport, account bootstrap, FSG1 funding, CSG1 clearing, provider ingress,
and the provider callback client. Network and secret-bearing modules remain
native-only. The standalone server imports this package, but activates it only
when the operator supplies the existing Parix settlement flags.

`build.zig` registers the generic package as `tigerbeetle` and the native host
package as `tigerbeetle_parix`. Focused checks are:

```sh
zig build -j1 test-tigerbeetle-plugin --summary all
zig build -j1 test-tigerbeetle-plugin-release-safe --summary all
zig build -j1 test-parix-apps --summary all
zig build -j1 wasm-settlement-check --summary all
```

The browser exchange ABI does not import either package. The separate WASM
settlement check imports only the generic package to prove it remains free of
filesystem, socket, credential, and native-thread dependencies.
