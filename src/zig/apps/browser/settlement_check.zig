const std = @import("std");
const exchange_core = @import("exchange_core");
const tigerbeetle = @import("tigerbeetle");
const common = exchange_core.core.common;
const destination_registry = tigerbeetle.destination_registry;
const dispatcher = tigerbeetle.dispatcher;
const outbox = tigerbeetle.outbox;
const parix = tigerbeetle.parix_adapter;
const plugin = tigerbeetle.plugin;
const exchange_integration = tigerbeetle.exchange_integration;
const settlement = tigerbeetle.settlement;

const WasmTransport = struct {
    submit_calls: u32 = 0,
    lookup_calls: u32 = 0,

    fn asTransport(self: *WasmTransport) plugin.Transport {
        return .{ .context = self, .vtable = &vtable };
    }

    fn submit(
        context: *anyopaque,
        destination_id: outbox.DestinationId,
        chain: *const settlement.TransferChain,
    ) dispatcher.SubmitDisposition {
        const self: *WasmTransport = @ptrCast(@alignCast(context));
        self.submit_calls += 1;
        if (chain.count == 0) return .permanent;
        return .{ .success = .{
            .destination_id = destination_id,
            .operation = .create_transfers,
            .persisted = true,
        } };
    }

    fn lookup(
        context: *anyopaque,
        destination_id: outbox.DestinationId,
        chain: *const settlement.TransferChain,
        cause: outbox.ReconciliationCause,
    ) dispatcher.LookupDisposition {
        const self: *WasmTransport = @ptrCast(@alignCast(context));
        self.lookup_calls += 1;
        _ = cause;
        const batch = dispatcher.LookupBatch.init(chain.items()) catch return .permanent;
        return .{ .transfers = .{
            .destination_id = destination_id,
            .operation = .lookup_transfers,
            .persisted = false,
            .batch = batch,
        } };
    }

    const vtable: plugin.Transport.VTable = .{
        .submit = submit,
        .lookup = lookup,
    };
};

/// Compile-only WASM probe for the pure settlement plugin/adapter boundary.
/// Browser builds never own settlement endpoints or credentials; this symbol
/// is built into a separate object and is not exported by the exchange browser
/// ABI.
export fn settlement_dispatcher_wasm_check() u32 {
    const Store = outbox.SettlementOutbox(1);
    const Driver = dispatcher.Dispatcher(Store);
    const destination: outbox.DestinationId = @enumFromInt(1);
    var store = Store.init(destination) catch return 1;
    var chain: settlement.TransferChain = .{
        .group_id = 1,
        .storage = [_]settlement.Transfer{std.mem.zeroes(settlement.Transfer)} ** settlement.max_transfers_per_fill,
        .count = 1,
    };
    chain.storage[0] = .{
        .id = 2,
        .debit_account_id = 3,
        .credit_account_id = 4,
        .amount = 5,
        .user_data_128 = 1,
        .ledger = 840,
        .code = 300,
    };
    var json_storage: [1024]u8 = undefined;
    var json_writer = std.Io.Writer.fixed(&json_storage);
    parix.writeCreateTransfers(&json_writer, &chain) catch return 11;
    if (json_writer.buffered().len == 0) return 12;
    const request: parix.RequestContext = .{
        .destination_id = destination,
        .operation = .create_transfers,
    };
    switch (parix.classifySubmit(request, .definitely_not_sent)) {
        .known_not_sent => {},
        else => return 13,
    }
    _ = store.enqueue(.{ .destination_id = destination, .chain = chain }) catch return 2;
    const policy: dispatcher.RetryPolicy = .{
        .known_not_sent = .{ .initial = .fromInt(1), .maximum = .fromInt(4) },
        .reconciliation = .{ .initial = .fromInt(1), .maximum = .fromInt(4) },
        .confirmed_absent = .{ .initial = .fromInt(1), .maximum = .fromInt(4) },
    };
    var driver = Driver.init(&store, policy) catch return 3;
    const claim_result = driver.claim(.fromInt(0)) catch return 4;
    const submit = switch (claim_result) {
        .submit => |value| value,
        else => return 5,
    };
    _ = driver.completeSubmit(submit, .ambiguous, .fromInt(0)) catch return 6;
    const lookup_result = driver.claim(.fromInt(1)) catch return 7;
    const lookup = switch (lookup_result) {
        .reconcile => |value| value,
        else => return 8,
    };
    const empty = dispatcher.LookupBatch.init(&.{}) catch return 9;
    _ = driver.completeLookup(
        lookup,
        .{ .transfers = .{
            .destination_id = lookup.token.destination_id,
            .operation = .lookup_transfers,
            .persisted = false,
            .batch = empty,
        } },
        .fromInt(1),
    ) catch return 10;
    const plugin_check = settlementPluginWasmCheck(destination, policy);
    if (plugin_check != 0) return plugin_check;
    return 0;
}

fn settlementPluginWasmCheck(
    destination: outbox.DestinationId,
    policy: dispatcher.RetryPolicy,
) u32 {
    const Store = outbox.SettlementOutbox(1);
    const Facade = plugin.Plugin(Store);

    var disabled = Facade.initDisabled();
    if ((disabled.start() catch return 20) != .disabled) return 21;

    const destinations = [_]destination_registry.DestinationSpec{.{ .id = destination }};
    const symbols = [_]destination_registry.SymbolRoute{.{
        .symbol_id = 241,
        .base_currency = 11,
        .quote_currency = 15,
        .destination_id = destination,
    }};
    const currencies = [_]destination_registry.CurrencyRoute{
        .{ .destination_id = destination, .currency = 11, .ledger = 111, .fee_account_id = 50_011 },
        .{ .destination_id = destination, .currency = 15, .ledger = 115, .fee_account_id = 50_015 },
    };
    const accounts = [_]destination_registry.AccountRoute{
        .{ .destination_id = destination, .uid = 1001, .currency = 11, .account_id = 101_011 },
        .{ .destination_id = destination, .uid = 1001, .currency = 15, .account_id = 101_015 },
        .{ .destination_id = destination, .uid = 1002, .currency = 11, .account_id = 102_011 },
        .{ .destination_id = destination, .uid = 1002, .currency = 15, .account_id = 102_015 },
    };
    var registry_storage: [32 * 1024]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&registry_storage);
    var registry = destination_registry.DestinationRegistry.initValidated(
        fixed.allocator(),
        .{
            .destinations = &destinations,
            .symbols = &symbols,
            .currencies = &currencies,
            .accounts = &accounts,
        },
    ) catch return 22;
    defer registry.deinit();

    var store = Store.initPrepared(destination, .{
        .namespace = 7,
        .journal_epoch = 3,
    }) catch return 23;
    var transport: WasmTransport = .{};
    var facade = Facade.initEnabled(
        destination,
        registry,
        &store,
        policy,
        transport.asTransport(),
    ) catch return 24;
    if ((facade.start() catch return 25) != .started) return 26;

    const specification = common.CoreSymbolSpecification.init(.{
        .symbol_id = 241,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    }) catch return 27;
    const capture_input: plugin.CaptureInput = .{
        .identity = .{
            .namespace = 7,
            .journal_epoch = 3,
            .publication_sequence = .fromInt(2),
            .fill_ordinal = 0,
        },
        .taker_uid = 1001,
        .maker_uid = 1002,
        .taker_action = .bid,
        .size = 2,
        .price = 50,
        .specification = specification,
    };
    const prepared = facade.prepareCapture(capture_input) catch return 28;
    switch (prepared) {
        .prepared => |result| if (result.inserted != 1) return 29,
        .disabled => return 30,
    }
    const cursor = capture_input.identity.publicationCursor() catch return 31;
    if (facade.markRiskR2CommittedThrough(cursor) != .marked) return 32;
    const gated = facade.poll(.fromInt(0), 1) catch return 33;
    const gated_summary = switch (gated) {
        .enabled => |value| value,
        .disabled => return 34,
    };
    if (gated_summary.completed != 0 or transport.submit_calls != 0) return 35;
    switch (gated_summary.dispatch) {
        .awaiting_commit => {},
        else => return 36,
    }
    if (facade.markJournalCommittedThrough(cursor) != .marked) return 37;
    const polled = facade.poll(.fromInt(0), 1) catch return 38;
    const summary = switch (polled) {
        .enabled => |value| value,
        .disabled => return 39,
    };
    if (summary.completed != 1 or transport.submit_calls != 1) return 40;

    // Instantiate the production lifecycle bridge on wasm32 as well. This
    // proves its mapper, typed hooks, local synchronization, and explicit poll
    // boundary remain free of sockets, credentials, and OS-only dependencies.
    const Bridge = exchange_integration.Integration(Store, 1);
    var bridge_store = Store.initPrepared(destination, .{
        .namespace = 7,
        .journal_epoch = 3,
    }) catch return 41;
    var bridge_transport: WasmTransport = .{};
    var bridge = Bridge.init(.{
        .destination_id = destination,
        .destination_registry = registry,
        .store = &bridge_store,
        .mapper_policy = .{ .publication_domain = .{
            .namespace = 7,
            .journal_epoch = 3,
        } },
        .retry_policy = policy,
        .transport = bridge_transport.asTransport(),
    }) catch return 42;
    var command: exchange_core.core.cmd.OrderCommand = .{
        .command = .place_order,
        .result_code = .success,
        .uid = 1001,
        .symbol = 241,
        .action = .bid,
    };
    var trade: exchange_core.core.events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 1002,
        .price = 50,
        .size = 2,
    };
    const hooks = bridge.hooks();
    if ((hooks.prepare(.{
        .sequence = .fromInt(3),
        .command = &command,
        .specification = specification,
        .events_head = &trade,
        .mode = .live,
    }) catch return 43) != .prepared) return 44;
    if ((hooks.markRiskR2Committed(.fromInt(3)) catch return 45) != .marked) return 46;
    if ((hooks.markJournalCommitted(.fromInt(3)) catch return 47) != .marked) return 48;
    const bridge_poll = bridge.pollDispatch(.fromInt(0), 1) catch return 49;
    const bridge_summary = switch (bridge_poll) {
        .enabled => |value| value,
        .disabled => return 50,
    };
    if (bridge_summary.completed != 1 or bridge_transport.submit_calls != 1) return 51;
    return 0;
}
