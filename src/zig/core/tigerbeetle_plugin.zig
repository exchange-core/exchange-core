const std = @import("std");
const common = @import("common.zig");
const registry = @import("tigerbeetle_destination_registry.zig");
const dispatcher = @import("tigerbeetle_dispatcher.zig");
const outbox = @import("tigerbeetle_outbox.zig");
const settlement = @import("tigerbeetle_settlement.zig");

/// Lifecycle of one enabled, destination-scoped settlement plugin.
///
/// The facade owns none of its dependencies. Its registry, store, transport
/// context, and transport vtable must remain alive and at stable addresses for
/// the complete enabled lifecycle.
pub const Lifecycle = enum {
    cold,
    running,
    draining,
    quarantined,
    failed,
    stopped,
};

/// Already-identified economic fill accepted by the standalone facade.
/// Automatic extraction from ExchangeCore is deliberately outside this type:
/// production capture first needs a durable prepared/two-frontier handoff.
pub const CaptureInput = struct {
    identity: settlement.FillIdentity,
    taker_uid: i64,
    maker_uid: i64,
    taker_action: common.OrderAction,
    size: i64,
    price: i64,
    specification: common.CoreSymbolSpecification,
};

/// Synchronous semantic transport boundary. Implementations may encode a
/// request and perform I/O during the call, but must not retain `chain` after
/// returning or re-enter the facade. A runtime busy guard rejects accidental
/// re-entry. Every failure is reduced to a conservative dispatcher disposition;
/// credentials, endpoints, and raw responses stay transport-owned.
pub const Transport = struct {
    context: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        submit: *const fn (
            context: *anyopaque,
            destination_id: outbox.DestinationId,
            chain: *const settlement.TransferChain,
        ) dispatcher.SubmitDisposition,
        lookup: *const fn (
            context: *anyopaque,
            destination_id: outbox.DestinationId,
            chain: *const settlement.TransferChain,
            cause: outbox.ReconciliationCause,
        ) dispatcher.LookupDisposition,
    };

    fn submit(
        self: Transport,
        destination_id: outbox.DestinationId,
        chain: *const settlement.TransferChain,
    ) dispatcher.SubmitDisposition {
        return self.vtable.submit(self.context, destination_id, chain);
    }

    fn lookup(
        self: Transport,
        destination_id: outbox.DestinationId,
        chain: *const settlement.TransferChain,
        cause: outbox.ReconciliationCause,
    ) dispatcher.LookupDisposition {
        return self.vtable.lookup(self.context, destination_id, chain, cause);
    }
};

pub const Status = union(enum) {
    disabled,
    enabled: struct {
        lifecycle: Lifecycle,
        dispatch: outbox.DispatchState,
    },
};

pub const StartResult = enum {
    disabled,
    started,
    already_running,
};

pub const StopResult = enum {
    disabled,
    busy,
    draining,
    quarantined,
    failed,
    stopped,
    already_stopped,
};

pub const CaptureResult = union(enum) {
    disabled,
    enqueued: outbox.EnqueueResult,
};

/// Result of one atomic prepared admission. The enabled branch reflects the
/// store's single batch call; mapping failures occur before that call.
pub const PrepareResult = union(enum) {
    disabled,
    prepared: outbox.PrepareBatchResult,
};

/// Publication progress marks are deliberately infallible and allocation-free.
/// The cursor carries the namespace and journal epoch, so a stale domain can
/// never release records in a newer publication domain.
pub const MarkResult = enum {
    disabled,
    marked,
    busy,
    inactive,
};

pub const PollSummary = struct {
    lifecycle: Lifecycle,
    completed: usize,
    dispatch: outbox.DispatchState,
};

pub const PollResult = union(enum) {
    disabled,
    enabled: PollSummary,
};

pub const LifecycleError = error{
    PluginBusy,
    PluginNotRunning,
    PluginDraining,
    PluginQuarantined,
    PluginFailed,
    PluginStopped,
};

pub const InitError = dispatcher.PolicyError || error{
    UnknownPluginDestination,
    StoreDestinationMismatch,
    StoreRecoveryRequired,
};
pub const CaptureError = LifecycleError ||
    registry.ResolveError ||
    settlement.SettlementError ||
    outbox.StoreError ||
    error{WrongPluginDestination};
pub const PrepareError = CaptureError || error{PreparedScratchTooSmall};
pub const PollError = LifecycleError || dispatcher.DispatchError;
const FatalObservationError = PrepareError || PollError;

/// Optional facade over one caller-owned outbox and one external destination.
///
/// `initDisabled` is a zero-dependency value and is the default integration
/// path. `initEnabled` borrows all dependencies. Calls must be serialized; the
/// underlying outbox and dispatcher are intentionally single-owner.
pub fn Plugin(comptime Store: type) type {
    return union(enum) {
        const Self = @This();
        const Driver = dispatcher.Dispatcher(Store);

        disabled,
        enabled: Enabled,

        const Enabled = struct {
            lifecycle: Lifecycle = .cold,
            destination_id: outbox.DestinationId,
            destination_registry: *const registry.DestinationRegistry,
            store: *Store,
            driver: Driver,
            transport: Transport,
            busy: bool = false,
        };

        pub fn initDisabled() Self {
            return .disabled;
        }

        pub fn initEnabled(
            destination_id: outbox.DestinationId,
            destination_registry: *const registry.DestinationRegistry,
            store: *Store,
            policy: dispatcher.RetryPolicy,
            transport: Transport,
        ) InitError!Self {
            _ = destination_registry.lookupDestination(destination_id) orelse
                return error.UnknownPluginDestination;
            if (store.destinationId() != destination_id) {
                return error.StoreDestinationMismatch;
            }
            const initial_lifecycle: Lifecycle = switch (store.dispatchState()) {
                .quarantined => .quarantined,
                .recovery_required => return error.StoreRecoveryRequired,
                else => .cold,
            };
            return .{ .enabled = .{
                .lifecycle = initial_lifecycle,
                .destination_id = destination_id,
                .destination_registry = destination_registry,
                .store = store,
                .driver = try Driver.init(store, policy),
                .transport = transport,
            } };
        }

        pub fn status(self: *const Self) Status {
            return switch (self.*) {
                .disabled => .disabled,
                .enabled => |enabled| .{ .enabled = .{
                    .lifecycle = enabled.lifecycle,
                    .dispatch = enabled.store.dispatchState(),
                } },
            };
        }

        pub fn start(self: *Self) LifecycleError!StartResult {
            return switch (self.*) {
                .disabled => .disabled,
                .enabled => |*enabled| start: {
                    if (enabled.busy) return error.PluginBusy;
                    break :start switch (enabled.lifecycle) {
                        .cold => result: {
                            enabled.lifecycle = .running;
                            break :result .started;
                        },
                        .running => .already_running,
                        .draining => error.PluginDraining,
                        .quarantined => error.PluginQuarantined,
                        .failed => error.PluginFailed,
                        .stopped => error.PluginStopped,
                    };
                },
            };
        }

        /// Fences new captures immediately. Existing work remains pollable
        /// until the outbox drains or reaches terminal quarantine/failure.
        pub fn stop(self: *Self) StopResult {
            return switch (self.*) {
                .disabled => .disabled,
                .enabled => |*enabled| stop: {
                    if (enabled.busy) break :stop .busy;
                    break :stop switch (enabled.lifecycle) {
                        .cold => stopped: {
                            enabled.lifecycle = .stopped;
                            break :stopped .stopped;
                        },
                        .running, .draining => stopFromDispatch(enabled),
                        .quarantined => .quarantined,
                        .failed => .failed,
                        .stopped => .already_stopped,
                    };
                },
            };
        }

        pub fn capture(
            self: *Self,
            input: CaptureInput,
        ) CaptureError!CaptureResult {
            return switch (self.*) {
                .disabled => .disabled,
                .enabled => |*enabled| captureEnabled(enabled, input),
            };
        }

        /// Resolves and maps every fill into caller-owned scratch before making
        /// exactly one atomic store call. `capture` remains the immediate,
        /// already-durable admission API; callers must opt into this prepared
        /// two-frontier path explicitly.
        pub fn prepareBatch(
            self: *Self,
            inputs: []const CaptureInput,
            scratch: []outbox.PreparedSettlementRecord,
        ) PrepareError!PrepareResult {
            return switch (self.*) {
                .disabled => .disabled,
                .enabled => |*enabled| prepareBatchEnabled(enabled, inputs, scratch),
            };
        }

        /// Allocation-free convenience for a single prepared fill.
        pub fn prepareCapture(
            self: *Self,
            input: CaptureInput,
        ) PrepareError!PrepareResult {
            switch (self.*) {
                .disabled => return .disabled,
                .enabled => {},
            }
            var scratch: [1]outbox.PreparedSettlementRecord = undefined;
            return self.prepareBatch(&.{input}, &scratch);
        }

        /// Advances the transient R2 frontier. This method performs no I/O and
        /// must be serialized with every other enabled facade call.
        pub fn markRiskR2CommittedThrough(
            self: *Self,
            cursor: settlement.PublicationCursor,
        ) MarkResult {
            return switch (self.*) {
                .disabled => .disabled,
                .enabled => |*enabled| marked: {
                    if (enabled.busy) break :marked .busy;
                    switch (enabled.lifecycle) {
                        .running, .draining => {},
                        .cold, .quarantined, .failed, .stopped => break :marked .inactive,
                    }
                    enabled.store.markRiskR2CommittedThrough(cursor);
                    break :marked .marked;
                },
            };
        }

        /// Advances the transient journal frontier. This method performs no
        /// I/O and is safe to repeat or call with a regressed/domain-stale mark.
        pub fn markJournalCommittedThrough(
            self: *Self,
            cursor: settlement.PublicationCursor,
        ) MarkResult {
            return switch (self.*) {
                .disabled => .disabled,
                .enabled => |*enabled| marked: {
                    if (enabled.busy) break :marked .busy;
                    switch (enabled.lifecycle) {
                        .running, .draining => {},
                        .cold, .quarantined, .failed, .stopped => break :marked .inactive,
                    }
                    enabled.store.markJournalCommittedThrough(cursor);
                    break :marked .marked;
                },
            };
        }

        /// Completes at most `budget` synchronous transport operations. A zero
        /// budget observes state without claiming work or advancing the logical
        /// clock. Positive-budget ticks must be monotonic within one enabled
        /// instance.
        pub fn poll(
            self: *Self,
            now: dispatcher.RetryTick,
            budget: usize,
        ) PollError!PollResult {
            return switch (self.*) {
                .disabled => .disabled,
                .enabled => |*enabled| .{ .enabled = try pollEnabled(enabled, now, budget) },
            };
        }

        fn captureEnabled(
            enabled: *Enabled,
            input: CaptureInput,
        ) CaptureError!CaptureResult {
            if (enabled.busy) return error.PluginBusy;
            switch (enabled.lifecycle) {
                .running => {},
                .cold => return error.PluginNotRunning,
                .draining => return error.PluginDraining,
                .quarantined => return error.PluginQuarantined,
                .failed => return error.PluginFailed,
                .stopped => return error.PluginStopped,
            }

            const resolved = try enabled.destination_registry.resolve(
                input.specification,
                input.taker_uid,
                input.maker_uid,
            );
            if (resolved.destination_id != enabled.destination_id) {
                return error.WrongPluginDestination;
            }
            const chain = try settlement.mapFill(.{
                .identity = input.identity,
                .taker_uid = input.taker_uid,
                .maker_uid = input.maker_uid,
                .taker_action = input.taker_action,
                .size = input.size,
                .price = input.price,
                .spec = input.specification,
                .ledgers = resolved.ledgers,
                .taker_accounts = resolved.taker_accounts,
                .maker_accounts = resolved.maker_accounts,
                .quote_fee_account_id = resolved.quote_fee_account_id,
                .codes = resolved.codes,
            });
            const enqueue_result = enabled.store.enqueue(resolved.bindChain(chain)) catch |err| {
                markFatalStoreError(enabled, err);
                return err;
            };
            return .{ .enqueued = enqueue_result };
        }

        fn prepareBatchEnabled(
            enabled: *Enabled,
            inputs: []const CaptureInput,
            scratch: []outbox.PreparedSettlementRecord,
        ) PrepareError!PrepareResult {
            if (enabled.busy) return error.PluginBusy;
            switch (enabled.lifecycle) {
                .running => {},
                .cold => return error.PluginNotRunning,
                .draining => return error.PluginDraining,
                .quarantined => return error.PluginQuarantined,
                .failed => return error.PluginFailed,
                .stopped => return error.PluginStopped,
            }
            if (scratch.len < inputs.len) return error.PreparedScratchTooSmall;

            for (inputs, scratch[0..inputs.len]) |input, *record| {
                const resolved = try enabled.destination_registry.resolve(
                    input.specification,
                    input.taker_uid,
                    input.maker_uid,
                );
                if (resolved.destination_id != enabled.destination_id) {
                    return error.WrongPluginDestination;
                }
                const chain = try settlement.mapFill(.{
                    .identity = input.identity,
                    .taker_uid = input.taker_uid,
                    .maker_uid = input.maker_uid,
                    .taker_action = input.taker_action,
                    .size = input.size,
                    .price = input.price,
                    .spec = input.specification,
                    .ledgers = resolved.ledgers,
                    .taker_accounts = resolved.taker_accounts,
                    .maker_accounts = resolved.maker_accounts,
                    .quote_fee_account_id = resolved.quote_fee_account_id,
                    .codes = resolved.codes,
                });
                record.* = .{
                    .destination_id = resolved.destination_id,
                    .publication = try input.identity.publicationCursor(),
                    .chain = chain,
                };
            }

            const result = enabled.store.prepare(scratch[0..inputs.len]) catch |err| {
                markFatalStoreError(enabled, err);
                return err;
            };
            return .{ .prepared = result };
        }

        fn pollEnabled(
            enabled: *Enabled,
            now: dispatcher.RetryTick,
            budget: usize,
        ) PollError!PollSummary {
            if (enabled.busy) return error.PluginBusy;
            switch (enabled.lifecycle) {
                .running, .draining => {},
                .cold => return error.PluginNotRunning,
                .quarantined => return error.PluginQuarantined,
                .failed => return error.PluginFailed,
                .stopped => return error.PluginStopped,
            }

            enabled.busy = true;
            defer enabled.busy = false;

            return pollLoop(enabled, now, budget) catch |err| {
                markFatalStoreError(enabled, err);
                return err;
            };
        }

        fn pollLoop(
            enabled: *Enabled,
            now: dispatcher.RetryTick,
            budget: usize,
        ) PollError!PollSummary {
            var completed: usize = 0;
            while (completed < budget) {
                const claim = try enabled.driver.claim(now);
                switch (claim) {
                    .submit => |value| {
                        const disposition = enabled.transport.submit(
                            enabled.destination_id,
                            value.chain,
                        );
                        _ = try enabled.driver.completeSubmit(value, disposition, now);
                        completed += 1;
                    },
                    .reconcile => |value| {
                        const disposition = enabled.transport.lookup(
                            enabled.destination_id,
                            value.chain,
                            value.cause,
                        );
                        _ = try enabled.driver.completeLookup(value, disposition, now);
                        completed += 1;
                    },
                    .blocked => break,
                }
            }

            const dispatch_state = enabled.store.dispatchState();
            switch (dispatch_state) {
                .drained => if (enabled.lifecycle == .draining) {
                    enabled.lifecycle = .stopped;
                },
                .quarantined => enabled.lifecycle = .quarantined,
                .awaiting_commit, .recovery_required, .waiting_submit, .waiting_reconciliation, .in_flight => {},
            }
            return .{
                .lifecycle = enabled.lifecycle,
                .completed = completed,
                .dispatch = dispatch_state,
            };
        }

        fn stopFromDispatch(enabled: *Enabled) StopResult {
            return switch (enabled.store.dispatchState()) {
                .drained => stopped: {
                    enabled.lifecycle = .stopped;
                    break :stopped .stopped;
                },
                .quarantined => quarantined: {
                    enabled.lifecycle = .quarantined;
                    break :quarantined .quarantined;
                },
                else => draining: {
                    enabled.lifecycle = .draining;
                    break :draining .draining;
                },
            };
        }

        fn markFatalStoreError(enabled: *Enabled, err: FatalObservationError) void {
            switch (err) {
                error.StorageAmbiguous, error.Poisoned => enabled.lifecycle = .failed,
                else => {},
            }
        }
    };
}

fn isDrained(state: outbox.DispatchState) bool {
    return switch (state) {
        .drained => true,
        else => false,
    };
}

fn testDestination(raw: u128) outbox.DestinationId {
    return @enumFromInt(raw);
}

fn testPolicy() dispatcher.RetryPolicy {
    return .{
        .known_not_sent = .{ .initial = .fromInt(2), .maximum = .fromInt(8) },
        .reconciliation = .{ .initial = .fromInt(1), .maximum = .fromInt(8) },
        .confirmed_absent = .{ .initial = .fromInt(2), .maximum = .fromInt(8) },
    };
}

fn testSpecification() common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = 241,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    }) catch unreachable;
}

fn testCapture(sequence: u64) CaptureInput {
    return .{
        .identity = .{
            .namespace = 7,
            .journal_epoch = 3,
            .publication_sequence = .fromInt(sequence),
            .fill_ordinal = 0,
        },
        .taker_uid = 1001,
        .maker_uid = 1002,
        .taker_action = .bid,
        .size = 2,
        .price = 50,
        .specification = testSpecification(),
    };
}

fn initTestRegistry() !*registry.DestinationRegistry {
    const destination_id = testDestination(42);
    const destinations = [_]registry.DestinationSpec{.{ .id = destination_id }};
    const symbols = [_]registry.SymbolRoute{.{
        .symbol_id = 241,
        .base_currency = 11,
        .quote_currency = 15,
        .destination_id = destination_id,
    }};
    const currencies = [_]registry.CurrencyRoute{
        .{ .destination_id = destination_id, .currency = 11, .ledger = 421, .fee_account_id = 42_011 },
        .{ .destination_id = destination_id, .currency = 15, .ledger = 425, .fee_account_id = 42_015 },
    };
    const accounts = [_]registry.AccountRoute{
        .{ .destination_id = destination_id, .uid = 1001, .currency = 11, .account_id = 421_011 },
        .{ .destination_id = destination_id, .uid = 1001, .currency = 15, .account_id = 421_015 },
        .{ .destination_id = destination_id, .uid = 1002, .currency = 11, .account_id = 422_011 },
        .{ .destination_id = destination_id, .uid = 1002, .currency = 15, .account_id = 422_015 },
    };
    return registry.DestinationRegistry.initValidated(std.testing.allocator, .{
        .destinations = &destinations,
        .symbols = &symbols,
        .currencies = &currencies,
        .accounts = &accounts,
    });
}

const TestTransport = struct {
    const SubmitMode = enum { success, ambiguous, permanent };
    const LookupMode = enum { exact, retryable };

    destination_id: outbox.DestinationId,
    submit_mode: SubmitMode = .success,
    lookup_mode: LookupMode = .exact,
    submit_calls: usize = 0,
    lookup_calls: usize = 0,
    last_cause: ?outbox.ReconciliationCause = null,

    fn transport(self: *TestTransport) Transport {
        return .{ .context = self, .vtable = &vtable };
    }

    fn submit(
        context: *anyopaque,
        destination_id: outbox.DestinationId,
        chain: *const settlement.TransferChain,
    ) dispatcher.SubmitDisposition {
        const self: *TestTransport = @ptrCast(@alignCast(context));
        self.submit_calls += 1;
        std.debug.assert(chain.count > 0);
        return switch (self.submit_mode) {
            .success => .{ .success = .{
                .destination_id = destination_id,
                .operation = .create_transfers,
                .persisted = true,
            } },
            .ambiguous => .ambiguous,
            .permanent => .permanent,
        };
    }

    fn lookup(
        context: *anyopaque,
        destination_id: outbox.DestinationId,
        chain: *const settlement.TransferChain,
        cause: outbox.ReconciliationCause,
    ) dispatcher.LookupDisposition {
        const self: *TestTransport = @ptrCast(@alignCast(context));
        self.lookup_calls += 1;
        self.last_cause = cause;
        return switch (self.lookup_mode) {
            .exact => .{ .transfers = .{
                .destination_id = destination_id,
                .operation = .lookup_transfers,
                .persisted = false,
                .batch = dispatcher.LookupBatch.init(chain.items()) catch unreachable,
            } },
            .retryable => .retryable,
        };
    }

    const vtable: Transport.VTable = .{
        .submit = submit,
        .lookup = lookup,
    };
};

fn ReentrantTransport(comptime Facade: type) type {
    return struct {
        const Self = @This();

        plugin: ?*Facade = null,
        saw_busy: bool = false,
        mark_cursor: ?settlement.PublicationCursor = null,
        risk_mark_result: ?MarkResult = null,
        journal_mark_result: ?MarkResult = null,

        fn transport(self: *Self) Transport {
            return .{ .context = self, .vtable = &vtable };
        }

        fn submit(
            context: *anyopaque,
            destination_id: outbox.DestinationId,
            chain: *const settlement.TransferChain,
        ) dispatcher.SubmitDisposition {
            const self: *Self = @ptrCast(@alignCast(context));
            _ = chain;
            if (self.plugin) |plugin_instance| {
                if (self.mark_cursor) |cursor| {
                    self.risk_mark_result = plugin_instance.markRiskR2CommittedThrough(cursor);
                    self.journal_mark_result = plugin_instance.markJournalCommittedThrough(cursor);
                }
                _ = plugin_instance.poll(.fromInt(1), 1) catch |err| {
                    self.saw_busy = err == error.PluginBusy;
                    return .{ .success = .{
                        .destination_id = destination_id,
                        .operation = .create_transfers,
                        .persisted = true,
                    } };
                };
            }
            return .permanent;
        }

        fn lookup(
            context: *anyopaque,
            destination_id: outbox.DestinationId,
            chain: *const settlement.TransferChain,
            cause: outbox.ReconciliationCause,
        ) dispatcher.LookupDisposition {
            _ = context;
            _ = destination_id;
            _ = chain;
            _ = cause;
            return .retryable;
        }

        const vtable: Transport.VTable = .{
            .submit = submit,
            .lookup = lookup,
        };
    };
}

test "disabled settlement plugin is dependency-free and inert" {
    const Store = outbox.SettlementOutbox(1);
    const Facade = Plugin(Store);
    var plugin = Facade.initDisabled();
    const input = testCapture(1);
    const cursor = try input.identity.publicationCursor();

    try std.testing.expectEqual(StartResult.disabled, try plugin.start());
    try std.testing.expectEqual(StopResult.disabled, plugin.stop());
    try std.testing.expectEqualDeep(CaptureResult.disabled, try plugin.capture(input));
    try std.testing.expectEqualDeep(
        PrepareResult.disabled,
        try plugin.prepareBatch(&.{input}, &.{}),
    );
    try std.testing.expectEqual(PrepareResult.disabled, try plugin.prepareCapture(input));
    try std.testing.expectEqual(MarkResult.disabled, plugin.markRiskR2CommittedThrough(cursor));
    try std.testing.expectEqual(MarkResult.disabled, plugin.markJournalCommittedThrough(cursor));
    try std.testing.expectEqualDeep(
        PollResult.disabled,
        try plugin.poll(.fromInt(0), std.math.maxInt(usize)),
    );
    try std.testing.expectEqualDeep(Status.disabled, plugin.status());
}

test "enabled plugin fences capture and drains synchronously on stop" {
    const destination_id = testDestination(42);
    const Store = outbox.SettlementOutbox(2);
    const Facade = Plugin(Store);
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var store = try Store.init(destination_id);
    var fake: TestTransport = .{ .destination_id = destination_id };
    var plugin = try Facade.initEnabled(
        destination_id,
        destination_registry,
        &store,
        testPolicy(),
        fake.transport(),
    );

    try std.testing.expectError(error.PluginNotRunning, plugin.capture(testCapture(1)));
    try std.testing.expectEqual(StartResult.started, try plugin.start());
    try std.testing.expectEqual(StartResult.already_running, try plugin.start());
    const captured = try plugin.capture(testCapture(1));
    switch (captured) {
        .enqueued => |result| try std.testing.expectEqual(outbox.EnqueueResult.inserted, result),
        .disabled => return error.ExpectedEnabledCapture,
    }
    try std.testing.expectEqual(StopResult.draining, plugin.stop());
    try std.testing.expectError(error.PluginDraining, plugin.capture(testCapture(2)));

    const polled = try plugin.poll(.fromInt(0), 1);
    const summary = switch (polled) {
        .enabled => |value| value,
        .disabled => return error.ExpectedEnabledPoll,
    };
    try std.testing.expectEqual(@as(usize, 1), summary.completed);
    try std.testing.expectEqual(Lifecycle.stopped, summary.lifecycle);
    try std.testing.expect(isDrained(summary.dispatch));
    try std.testing.expectEqual(@as(usize, 1), fake.submit_calls);
    try std.testing.expectEqual(StopResult.already_stopped, plugin.stop());
    try std.testing.expectError(error.PluginStopped, plugin.start());
}

test "enabled plugin reconciles an ambiguous write without blind resubmit" {
    const destination_id = testDestination(42);
    const Store = outbox.SettlementOutbox(1);
    const Facade = Plugin(Store);
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var store = try Store.init(destination_id);
    var fake: TestTransport = .{
        .destination_id = destination_id,
        .submit_mode = .ambiguous,
    };
    var plugin = try Facade.initEnabled(
        destination_id,
        destination_registry,
        &store,
        testPolicy(),
        fake.transport(),
    );
    _ = try plugin.start();
    _ = try plugin.capture(testCapture(1));

    const observed = try plugin.poll(.fromInt(99), 0);
    try std.testing.expectEqual(@as(usize, 0), observed.enabled.completed);
    const submitted = try plugin.poll(.fromInt(0), 1);
    try std.testing.expectEqual(@as(usize, 1), submitted.enabled.completed);
    try std.testing.expectEqual(@as(usize, 1), fake.submit_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.lookup_calls);

    const too_early = try plugin.poll(.fromInt(0), 1);
    try std.testing.expectEqual(@as(usize, 0), too_early.enabled.completed);
    try std.testing.expectEqual(@as(usize, 1), fake.submit_calls);

    const reconciled = try plugin.poll(.fromInt(1), 1);
    try std.testing.expectEqual(@as(usize, 1), reconciled.enabled.completed);
    try std.testing.expect(isDrained(reconciled.enabled.dispatch));
    try std.testing.expectEqual(@as(usize, 1), fake.submit_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.lookup_calls);
    try std.testing.expectEqual(
        outbox.ReconciliationCause.ambiguous_submission,
        fake.last_cause.?,
    );
}

test "enabled plugin validates destination and lifecycle without transport calls" {
    const destination_id = testDestination(42);
    const Store = outbox.SettlementOutbox(1);
    const Facade = Plugin(Store);
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var store = try Store.init(destination_id);
    var fake: TestTransport = .{ .destination_id = destination_id };

    try std.testing.expectError(
        error.UnknownPluginDestination,
        Facade.initEnabled(
            testDestination(99),
            destination_registry,
            &store,
            testPolicy(),
            fake.transport(),
        ),
    );
    var wrong_store = try Store.init(testDestination(43));
    try std.testing.expectError(
        error.StoreDestinationMismatch,
        Facade.initEnabled(
            destination_id,
            destination_registry,
            &wrong_store,
            testPolicy(),
            fake.transport(),
        ),
    );

    var plugin = try Facade.initEnabled(
        destination_id,
        destination_registry,
        &store,
        testPolicy(),
        fake.transport(),
    );
    try std.testing.expectError(error.PluginNotRunning, plugin.poll(.fromInt(0), 1));
    try std.testing.expectEqual(StopResult.stopped, plugin.stop());
    try std.testing.expectEqual(@as(usize, 0), fake.submit_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.lookup_calls);
}

test "enabled plugin composes with the durable file outbox" {
    const destination_id = testDestination(42);
    const Store = outbox.DurableFileOutbox(1);
    const Facade = Plugin(Store);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var store = try Store.open(
        std.testing.allocator,
        tmp.dir,
        "plugin-outbox.tso",
        destination_id,
    );
    defer store.deinit();
    var fake: TestTransport = .{ .destination_id = destination_id };
    var plugin = try Facade.initEnabled(
        destination_id,
        destination_registry,
        &store,
        testPolicy(),
        fake.transport(),
    );

    _ = try plugin.start();
    _ = try plugin.capture(testCapture(1));
    const polled = try plugin.poll(.fromInt(0), 1);
    try std.testing.expectEqual(@as(usize, 1), polled.enabled.completed);
    try std.testing.expect(isDrained(polled.enabled.dispatch));
    try std.testing.expectEqual(@as(usize, 0), store.len());
    try std.testing.expectEqual(@as(usize, 1), fake.submit_calls);
}

test "empty settlement chains are finalized without consuming capacity" {
    const destination_id = testDestination(42);
    const Store = outbox.SettlementOutbox(1);
    const Facade = Plugin(Store);
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var store = try Store.init(destination_id);
    var fake: TestTransport = .{ .destination_id = destination_id };
    var plugin = try Facade.initEnabled(
        destination_id,
        destination_registry,
        &store,
        testPolicy(),
        fake.transport(),
    );
    _ = try plugin.start();

    var empty = testCapture(1);
    empty.maker_uid = empty.taker_uid;
    empty.specification.maker_fee = 0;
    empty.specification.taker_fee = 0;
    const finalized = try plugin.capture(empty);
    try std.testing.expectEqual(
        outbox.EnqueueResult.empty_finalized,
        finalized.enqueued,
    );
    try std.testing.expectEqual(@as(usize, 0), store.len());

    const next = try plugin.capture(testCapture(2));
    try std.testing.expectEqual(outbox.EnqueueResult.inserted, next.enqueued);
}

test "durable empty settlement finalization survives reopen atomically" {
    const destination_id = testDestination(42);
    const Store = outbox.DurableFileOutbox(1);
    const Facade = Plugin(Store);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var fake: TestTransport = .{ .destination_id = destination_id };

    {
        var store = try Store.open(
            std.testing.allocator,
            tmp.dir,
            "empty-plugin-outbox.tso",
            destination_id,
        );
        defer store.deinit();
        var plugin = try Facade.initEnabled(
            destination_id,
            destination_registry,
            &store,
            testPolicy(),
            fake.transport(),
        );
        _ = try plugin.start();
        var empty = testCapture(1);
        empty.maker_uid = empty.taker_uid;
        empty.specification.maker_fee = 0;
        empty.specification.taker_fee = 0;
        try std.testing.expectEqual(
            outbox.EnqueueResult.empty_finalized,
            (try plugin.capture(empty)).enqueued,
        );
        try std.testing.expectEqual(@as(usize, 0), store.len());
    }

    var reopened = try Store.open(
        std.testing.allocator,
        tmp.dir,
        "empty-plugin-outbox.tso",
        destination_id,
    );
    defer reopened.deinit();
    try std.testing.expectEqual(@as(usize, 0), reopened.len());
    var plugin = try Facade.initEnabled(
        destination_id,
        destination_registry,
        &reopened,
        testPolicy(),
        fake.transport(),
    );
    _ = try plugin.start();
    try std.testing.expectEqual(
        outbox.EnqueueResult.inserted,
        (try plugin.capture(testCapture(2))).enqueued,
    );
}

test "quarantine is a terminal lifecycle that fences further capture" {
    const destination_id = testDestination(42);
    const Store = outbox.SettlementOutbox(1);
    const Facade = Plugin(Store);
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var store = try Store.init(destination_id);
    var fake: TestTransport = .{
        .destination_id = destination_id,
        .submit_mode = .permanent,
    };
    var plugin = try Facade.initEnabled(
        destination_id,
        destination_registry,
        &store,
        testPolicy(),
        fake.transport(),
    );
    _ = try plugin.start();
    _ = try plugin.capture(testCapture(1));

    const polled = try plugin.poll(.fromInt(0), 1);
    try std.testing.expectEqual(Lifecycle.quarantined, polled.enabled.lifecycle);
    switch (polled.enabled.dispatch) {
        .quarantined => {},
        else => return error.ExpectedQuarantine,
    }
    try std.testing.expectEqual(StopResult.quarantined, plugin.stop());
    try std.testing.expectError(error.PluginQuarantined, plugin.capture(testCapture(2)));
    try std.testing.expectError(error.PluginQuarantined, plugin.poll(.fromInt(1), 1));
}

test "durable quarantine is fenced immediately after reopen" {
    const destination_id = testDestination(42);
    const Store = outbox.DurableFileOutbox(1);
    const Facade = Plugin(Store);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var fake: TestTransport = .{
        .destination_id = destination_id,
        .submit_mode = .permanent,
    };

    {
        var store = try Store.open(
            std.testing.allocator,
            tmp.dir,
            "quarantined-plugin-outbox.tso",
            destination_id,
        );
        defer store.deinit();
        var plugin = try Facade.initEnabled(
            destination_id,
            destination_registry,
            &store,
            testPolicy(),
            fake.transport(),
        );
        _ = try plugin.start();
        _ = try plugin.capture(testCapture(1));
        _ = try plugin.poll(.fromInt(0), 1);
        try std.testing.expectEqual(Lifecycle.quarantined, plugin.status().enabled.lifecycle);
    }

    var reopened = try Store.open(
        std.testing.allocator,
        tmp.dir,
        "quarantined-plugin-outbox.tso",
        destination_id,
    );
    defer reopened.deinit();
    var plugin = try Facade.initEnabled(
        destination_id,
        destination_registry,
        &reopened,
        testPolicy(),
        fake.transport(),
    );
    try std.testing.expectEqual(Lifecycle.quarantined, plugin.status().enabled.lifecycle);
    try std.testing.expectError(error.PluginQuarantined, plugin.start());
    try std.testing.expectError(error.PluginQuarantined, plugin.capture(testCapture(2)));
    try std.testing.expectEqual(StopResult.quarantined, plugin.stop());
}

test "transport reentry is rejected without corrupting the outer claim clock" {
    const destination_id = testDestination(42);
    const Store = outbox.SettlementOutbox(1);
    const Facade = Plugin(Store);
    const Reentrant = ReentrantTransport(Facade);
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var store = try Store.init(destination_id);
    var reentrant: Reentrant = .{};
    var plugin = try Facade.initEnabled(
        destination_id,
        destination_registry,
        &store,
        testPolicy(),
        reentrant.transport(),
    );
    reentrant.plugin = &plugin;
    _ = try plugin.start();
    _ = try plugin.capture(testCapture(1));

    const polled = try plugin.poll(.fromInt(0), 1);
    try std.testing.expect(reentrant.saw_busy);
    try std.testing.expectEqual(@as(usize, 1), polled.enabled.completed);
    try std.testing.expect(isDrained(polled.enabled.dispatch));
}

test "prepared facade waits for both frontier orders before dispatch" {
    const destination_id = testDestination(42);
    const Store = outbox.SettlementOutbox(1);
    const Facade = Plugin(Store);
    const MarkOrder = enum { risk_then_journal, journal_then_risk };
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();

    inline for ([_]MarkOrder{ .risk_then_journal, .journal_then_risk }) |order| {
        const input = testCapture(1);
        const domain = try input.identity.publicationDomain();
        const cursor = try input.identity.publicationCursor();
        var store = try Store.initPrepared(destination_id, domain);
        var fake: TestTransport = .{ .destination_id = destination_id };
        var plugin = try Facade.initEnabled(
            destination_id,
            destination_registry,
            &store,
            testPolicy(),
            fake.transport(),
        );
        try std.testing.expectEqual(
            MarkResult.inactive,
            plugin.markRiskR2CommittedThrough(cursor),
        );
        try std.testing.expectEqual(
            MarkResult.inactive,
            plugin.markJournalCommittedThrough(cursor),
        );
        _ = try plugin.start();
        const prepared = try plugin.prepareCapture(input);
        try std.testing.expectEqual(@as(usize, 1), prepared.prepared.inserted);
        try std.testing.expectEqual(StopResult.draining, plugin.stop());

        switch (order) {
            .risk_then_journal => try std.testing.expectEqual(
                MarkResult.marked,
                plugin.markRiskR2CommittedThrough(cursor),
            ),
            .journal_then_risk => try std.testing.expectEqual(
                MarkResult.marked,
                plugin.markJournalCommittedThrough(cursor),
            ),
        }
        const singly_marked = try plugin.poll(.fromInt(0), 1);
        try std.testing.expectEqual(@as(usize, 0), singly_marked.enabled.completed);
        try std.testing.expectEqual(Lifecycle.draining, singly_marked.enabled.lifecycle);
        switch (singly_marked.enabled.dispatch) {
            .awaiting_commit => {},
            else => return error.ExpectedAwaitingCommit,
        }
        try std.testing.expectEqual(@as(usize, 0), fake.submit_calls);
        try std.testing.expectEqual(@as(usize, 0), fake.lookup_calls);

        switch (order) {
            .risk_then_journal => _ = plugin.markJournalCommittedThrough(cursor),
            .journal_then_risk => _ = plugin.markRiskR2CommittedThrough(cursor),
        }
        const released = try plugin.poll(.fromInt(0), 1);
        try std.testing.expectEqual(@as(usize, 1), released.enabled.completed);
        try std.testing.expectEqual(Lifecycle.stopped, released.enabled.lifecycle);
        try std.testing.expect(isDrained(released.enabled.dispatch));
        try std.testing.expectEqual(@as(usize, 1), fake.submit_calls);
        try std.testing.expectEqual(
            MarkResult.inactive,
            plugin.markRiskR2CommittedThrough(cursor),
        );
        try std.testing.expectEqual(
            MarkResult.inactive,
            plugin.markJournalCommittedThrough(cursor),
        );
    }
}

test "frontier mark reentry is infallible and cannot advance a prepared store" {
    const destination_id = testDestination(42);
    const Store = outbox.SettlementOutbox(2);
    const Facade = Plugin(Store);
    const Reentrant = ReentrantTransport(Facade);
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();

    const first = testCapture(1);
    var second = testCapture(2);
    second.identity.publication_sequence = .fromInt(2);
    const domain = try first.identity.publicationDomain();
    const first_cursor = try first.identity.publicationCursor();
    const second_cursor = try second.identity.publicationCursor();
    var store = try Store.initPrepared(destination_id, domain);
    var reentrant: Reentrant = .{ .mark_cursor = second_cursor };
    var plugin = try Facade.initEnabled(
        destination_id,
        destination_registry,
        &store,
        testPolicy(),
        reentrant.transport(),
    );
    reentrant.plugin = &plugin;
    _ = try plugin.start();
    _ = try plugin.prepareCapture(first);
    try std.testing.expectEqual(MarkResult.marked, plugin.markRiskR2CommittedThrough(first_cursor));
    try std.testing.expectEqual(MarkResult.marked, plugin.markJournalCommittedThrough(first_cursor));

    const polled = try plugin.poll(.fromInt(0), 1);
    try std.testing.expectEqual(@as(usize, 1), polled.enabled.completed);
    try std.testing.expectEqual(MarkResult.busy, reentrant.risk_mark_result.?);
    try std.testing.expectEqual(MarkResult.busy, reentrant.journal_mark_result.?);

    _ = try plugin.prepareCapture(second);
    switch (store.dispatchState()) {
        .awaiting_commit => |waiting| {
            try std.testing.expect(!waiting.risk_r2_committed);
            try std.testing.expect(!waiting.journal_committed);
        },
        else => return error.ExpectedAwaitingCommit,
    }
}

test "prepared facade batch admission is all or nothing on capacity failure" {
    const destination_id = testDestination(42);
    const Store = outbox.SettlementOutbox(1);
    const Facade = Plugin(Store);
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    const first = testCapture(1);
    var second = testCapture(1);
    second.identity.fill_ordinal = 1;
    const domain = try first.identity.publicationDomain();
    var store = try Store.initPrepared(destination_id, domain);
    var fake: TestTransport = .{ .destination_id = destination_id };
    var plugin = try Facade.initEnabled(
        destination_id,
        destination_registry,
        &store,
        testPolicy(),
        fake.transport(),
    );
    _ = try plugin.start();

    var scratch: [2]outbox.PreparedSettlementRecord = undefined;
    try std.testing.expectError(
        error.OutboxFull,
        plugin.prepareBatch(&.{ first, second }, &scratch),
    );
    try std.testing.expectEqual(@as(usize, 0), store.len());
    try std.testing.expect(isDrained(store.dispatchState()));
    try std.testing.expectEqual(@as(usize, 0), fake.submit_calls);

    const retried = try plugin.prepareCapture(first);
    try std.testing.expectEqual(@as(usize, 1), retried.prepared.inserted);
    try std.testing.expectEqual(@as(usize, 1), store.len());
}

test "prepared durable reopen must finish recovery before facade initialization" {
    const destination_id = testDestination(42);
    const Store = outbox.DurableFileOutbox(1);
    const Facade = Plugin(Store);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    const input = testCapture(1);
    const domain = try input.identity.publicationDomain();
    var fake: TestTransport = .{ .destination_id = destination_id };

    {
        var store = try Store.openPrepared(
            std.testing.allocator,
            tmp.dir,
            "prepared-recovery-plugin.tso",
            destination_id,
            domain,
        );
        defer store.deinit();
        var plugin = try Facade.initEnabled(
            destination_id,
            destination_registry,
            &store,
            testPolicy(),
            fake.transport(),
        );
        _ = try plugin.start();
        _ = try plugin.prepareCapture(input);
    }

    var reopened = try Store.openPrepared(
        std.testing.allocator,
        tmp.dir,
        "prepared-recovery-plugin.tso",
        destination_id,
        domain,
    );
    defer reopened.deinit();
    switch (reopened.dispatchState()) {
        .recovery_required => {},
        else => return error.ExpectedRecoveryFence,
    }
    try std.testing.expectError(
        error.StoreRecoveryRequired,
        Facade.initEnabled(
            destination_id,
            destination_registry,
            &reopened,
            testPolicy(),
            fake.transport(),
        ),
    );
    try std.testing.expectEqual(@as(usize, 0), fake.submit_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.lookup_calls);
}

test "durable ambiguity moves the facade to failed until store reopen" {
    const destination_id = testDestination(42);
    const Store = outbox.DurableFileOutbox(1);
    const Facade = Plugin(Store);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var fake: TestTransport = .{ .destination_id = destination_id };

    {
        var store = try Store.open(
            std.testing.allocator,
            tmp.dir,
            "capture-failure.tso",
            destination_id,
        );
        defer store.deinit();
        var plugin = try Facade.initEnabled(
            destination_id,
            destination_registry,
            &store,
            testPolicy(),
            fake.transport(),
        );
        _ = try plugin.start();
        store.injectWriteFailureForTesting();
        try std.testing.expectError(error.StorageAmbiguous, plugin.capture(testCapture(1)));
        try std.testing.expectEqual(Lifecycle.failed, plugin.status().enabled.lifecycle);
        try std.testing.expectEqual(StopResult.failed, plugin.stop());
        try std.testing.expectError(error.PluginFailed, plugin.capture(testCapture(2)));
    }

    {
        var store = try Store.open(
            std.testing.allocator,
            tmp.dir,
            "poll-failure.tso",
            destination_id,
        );
        defer store.deinit();
        var plugin = try Facade.initEnabled(
            destination_id,
            destination_registry,
            &store,
            testPolicy(),
            fake.transport(),
        );
        _ = try plugin.start();
        _ = try plugin.capture(testCapture(1));
        store.injectWriteFailureForTesting();
        try std.testing.expectError(error.StorageAmbiguous, plugin.poll(.fromInt(0), 1));
        try std.testing.expectEqual(Lifecycle.failed, plugin.status().enabled.lifecycle);
        try std.testing.expectEqual(StopResult.failed, plugin.stop());
        try std.testing.expectError(error.PluginFailed, plugin.poll(.fromInt(1), 1));
    }
}
