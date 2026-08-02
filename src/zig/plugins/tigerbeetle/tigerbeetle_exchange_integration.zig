const std = @import("std");
const builtin = @import("builtin");
const core = @import("exchange_core").core;
const cmd = core.cmd;
const common = core.common;
const dispatcher = @import("tigerbeetle_dispatcher.zig");
const events = core.events;
const lifecycle = core.settlement_lifecycle;
const outbox = @import("tigerbeetle_outbox.zig");
const plugin = @import("tigerbeetle_plugin.zig");
const publication = core.publication_sequence;
const registry = @import("tigerbeetle_destination_registry.zig");
const settlement = @import("tigerbeetle_settlement.zig");

/// Stable mapper identity for one journal epoch. All live and replay fills are
/// derived from this domain plus the lifecycle's durable publication sequence.
pub const MapperPolicy = struct {
    publication_domain: settlement.PublicationDomain,

    pub fn validate(self: MapperPolicy) settlement.SettlementError!void {
        try self.publication_domain.validate();
    }
};

pub const InitError = plugin.InitError ||
    plugin.LifecycleError ||
    settlement.SettlementError ||
    error{
        StoreModeMismatch,
        StoreDomainMismatch,
        StoreRecoveryStateMismatch,
    };

pub const DispatchAccessError = error{
    RecoveryNotFinished,
    IntegrationFailed,
};

pub const DispatchStartError = DispatchAccessError || plugin.LifecycleError;
pub const DispatchPollError = DispatchAccessError || plugin.PollError;

pub const IntegrationPhase = enum {
    recovering,
    live,
    failed,
};

pub const IntegrationStatus = union(enum) {
    recovering: outbox.DispatchState,
    live: plugin.Status,
    failed,
};

const BatchError = error{
    EmptyTradeBatch,
    TooManyFills,
    InvalidPublicationSequence,
    FillOrdinalOverflow,
    CommandSymbolMismatch,
    MissingTakerAction,
    WrongIntegrationDestination,
};

/// Blocking process-local mutex for native threaded builds. Freestanding WASM
/// is configured as single-threaded, so its selected type is zero-sized and
/// does not analyze or link any host threading/posix implementation.
const IntegrationMutex = if (builtin.single_threaded) struct {
    fn lock(_: *@This()) void {}
    fn unlock(_: *@This()) void {}
} else struct {
    inner: std.Io.Mutex = .init,

    fn lock(self: *@This()) void {
        std.Io.Threaded.mutexLock(&self.inner);
    }

    fn unlock(self: *@This()) void {
        std.Io.Threaded.mutexUnlock(&self.inner);
    }
};

/// Plug-and-play bridge from ExchangeKernel settlement lifecycle hooks to one
/// destination-bound, two-frontier TigerBeetle plugin.
///
/// `max_fills_per_batch` is the hard allocation-free mapping bound: a lifecycle
/// batch with more trade events is rejected in full before registry resolution
/// or any store mutation. The caller owns the registry, store, transport
/// context, and this integration. After `init` returns, place the value at its
/// final stable address before calling `hooks()`. Never move or copy it after
/// installing those hooks, and retain it until every exchange-core owner has
/// stopped using the hooks.
///
/// Hook callbacks serialize only local mapper/store/plugin mutations. They
/// never poll the dispatcher or invoke transport. `pollDispatch` is the sole
/// transport entry point and must be called explicitly outside exchange locks.
pub fn Integration(
    comptime Store: type,
    comptime max_fills_per_batch: usize,
) type {
    comptime {
        if (max_fills_per_batch == 0) {
            @compileError("TigerBeetle exchange integration requires at least one fill slot");
        }
    }

    return struct {
        const Self = @This();
        const Facade = plugin.Plugin(Store);

        pub const Config = struct {
            destination_id: outbox.DestinationId,
            destination_registry: *const registry.DestinationRegistry,
            store: *Store,
            mapper_policy: MapperPolicy,
            retry_policy: dispatcher.RetryPolicy,
            transport: plugin.Transport,
        };

        mutex: IntegrationMutex = .{},
        destination_id: outbox.DestinationId,
        destination_registry: *const registry.DestinationRegistry,
        store: *Store,
        mapper_policy: MapperPolicy,
        retry_policy: dispatcher.RetryPolicy,
        transport: plugin.Transport,
        facade: ?Facade = null,
        phase: IntegrationPhase,
        capture_scratch: [max_fills_per_batch]plugin.CaptureInput = undefined,
        record_scratch: [max_fills_per_batch]outbox.PreparedSettlementRecord = undefined,

        pub fn init(config: Config) InitError!Self {
            try config.mapper_policy.validate();
            try config.retry_policy.validate();
            _ = config.destination_registry.lookupDestination(config.destination_id) orelse
                return error.UnknownPluginDestination;
            if (config.store.destinationId() != config.destination_id) {
                return error.StoreDestinationMismatch;
            }
            if (config.store.mode() != .two_frontier) {
                return error.StoreModeMismatch;
            }
            const store_domain = config.store.publicationDomain() orelse
                return error.StoreModeMismatch;
            if (!store_domain.eql(config.mapper_policy.publication_domain)) {
                return error.StoreDomainMismatch;
            }

            const recovery_required = config.store.isRecoveryRequired();
            const state_requires_recovery = switch (config.store.dispatchState()) {
                .recovery_required => true,
                else => false,
            };
            if (recovery_required != state_requires_recovery) {
                return error.StoreRecoveryStateMismatch;
            }

            var self: Self = .{
                .destination_id = config.destination_id,
                .destination_registry = config.destination_registry,
                .store = config.store,
                .mapper_policy = config.mapper_policy,
                .retry_policy = config.retry_policy,
                .transport = config.transport,
                .phase = if (recovery_required) .recovering else .live,
            };
            if (!recovery_required) {
                self.facade = try self.createStartedFacade();
            }
            return self;
        }

        /// Returns hooks whose context is this exact address. Calling this is
        /// the point after which the integration must remain pinned in memory.
        pub fn hooks(self: *Self) lifecycle.Hooks {
            return lifecycle.Hooks.enabled(self, &hooks_vtable);
        }

        pub fn currentPhase(self: *Self) IntegrationPhase {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.phase;
        }

        pub fn status(self: *Self) IntegrationStatus {
            self.mutex.lock();
            defer self.mutex.unlock();
            return switch (self.phase) {
                .recovering => .{ .recovering = self.store.dispatchState() },
                .live => if (self.facade) |*facade|
                    .{ .live = facade.status() }
                else
                    .failed,
                .failed => .failed,
            };
        }

        /// Idempotently activates an already-recovered facade. Initialization
        /// and successful recovery activate it automatically so the first live
        /// command can prepare safely; this wrapper remains explicit for hosts.
        pub fn startDispatch(self: *Self) DispatchStartError!plugin.StartResult {
            self.mutex.lock();
            defer self.mutex.unlock();
            const facade = try self.liveFacade();
            return facade.start();
        }

        /// The only method that can call the injected TigerBeetle transport.
        /// It retains the integration mutex across the synchronous transport
        /// call. Invoke it from a dedicated owner outside exchange and journal
        /// locks, with a bounded transport deadline, so hooks cannot be starved.
        pub fn pollDispatch(
            self: *Self,
            now: dispatcher.RetryTick,
            budget: usize,
        ) DispatchPollError!plugin.PollResult {
            self.mutex.lock();
            defer self.mutex.unlock();
            const facade = try self.liveFacade();
            return facade.poll(now, budget);
        }

        pub fn stopDispatch(self: *Self) DispatchAccessError!plugin.StopResult {
            self.mutex.lock();
            defer self.mutex.unlock();
            const facade = try self.liveFacade();
            return facade.stop();
        }

        fn createStartedFacade(self: *Self) (plugin.InitError || plugin.LifecycleError)!Facade {
            var facade = try Facade.initEnabled(
                self.destination_id,
                self.destination_registry,
                self.store,
                self.retry_policy,
                self.transport,
            );
            _ = try facade.start();
            return facade;
        }

        fn liveFacade(self: *Self) DispatchAccessError!*Facade {
            return switch (self.phase) {
                .recovering => error.RecoveryNotFinished,
                .failed => error.IntegrationFailed,
                .live => if (self.facade) |*facade| facade else error.IntegrationFailed,
            };
        }

        fn prepareHook(
            context: *anyopaque,
            batch: lifecycle.PreparedBatch,
        ) lifecycle.PrepareObservation {
            const self: *Self = @ptrCast(@alignCast(context));
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.prepareLocked(batch);
        }

        fn markRiskHook(
            context: *anyopaque,
            sequence: publication.PublicationSequence,
        ) lifecycle.MarkObservation {
            const self: *Self = @ptrCast(@alignCast(context));
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.markLocked(.risk_r2, sequence.toInt());
        }

        fn markJournalHook(
            context: *anyopaque,
            sequence: publication.PublicationSequence,
        ) lifecycle.MarkObservation {
            const self: *Self = @ptrCast(@alignCast(context));
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.markLocked(.journal, sequence.toInt());
        }

        fn finishRecoveryHook(
            context: *anyopaque,
            sequence: publication.PublicationSequence,
        ) lifecycle.RecoveryObservation {
            const self: *Self = @ptrCast(@alignCast(context));
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.finishRecoveryLocked(sequence.toInt());
        }

        const hooks_vtable: lifecycle.Hooks.VTable = .{
            .prepare = prepareHook,
            .mark_risk_r2_committed = markRiskHook,
            .mark_journal_committed = markJournalHook,
            .finish_recovery = finishRecoveryHook,
        };

        fn prepareLocked(self: *Self, batch: lifecycle.PreparedBatch) lifecycle.PrepareObservation {
            const inputs = self.buildCaptureInputs(batch) catch return .rejected;
            return switch (batch.mode) {
                .live => live: {
                    if (self.phase != .live) break :live .rejected;
                    const facade = self.facadePtr() orelse break :live .rejected;
                    const result = facade.prepareBatch(
                        inputs,
                        self.record_scratch[0..inputs.len],
                    ) catch break :live .rejected;
                    break :live switch (result) {
                        .prepared => .prepared,
                        .disabled => .rejected,
                    };
                },
                .replay => replay: {
                    if (self.phase != .recovering or !self.store.isRecoveryRequired()) {
                        break :replay .rejected;
                    }
                    const records = self.mapRecoveryRecords(inputs) catch break :replay .rejected;
                    _ = self.store.prepareRecovery(records) catch break :replay .rejected;
                    break :replay .prepared;
                },
            };
        }

        fn buildCaptureInputs(
            self: *Self,
            batch: lifecycle.PreparedBatch,
        ) BatchError![]const plugin.CaptureInput {
            const trade_count = batch.tradeCount();
            if (trade_count == 0) return error.EmptyTradeBatch;
            if (trade_count > max_fills_per_batch) return error.TooManyFills;
            if (batch.sequence.toInt() == 0) return error.InvalidPublicationSequence;
            if (batch.command.symbol != batch.specification.symbol_id) {
                return error.CommandSymbolMismatch;
            }
            const taker_action = batch.command.action orelse return error.MissingTakerAction;

            var trade_index: usize = 0;
            var event = batch.events_head;
            while (event) |current| : (event = current.next_event) {
                if (current.event_type != .trade) continue;
                const fill_ordinal = std.math.cast(u32, trade_index) orelse
                    return error.FillOrdinalOverflow;
                self.capture_scratch[trade_index] = .{
                    .identity = .{
                        .namespace = self.mapper_policy.publication_domain.namespace,
                        .journal_epoch = self.mapper_policy.publication_domain.journal_epoch,
                        .publication_sequence = batch.sequence,
                        .fill_ordinal = fill_ordinal,
                    },
                    .taker_uid = batch.command.uid,
                    .maker_uid = current.matched_order_uid,
                    .taker_action = taker_action,
                    .size = current.size,
                    .price = current.price,
                    .specification = batch.specification,
                };
                trade_index += 1;
            }
            std.debug.assert(trade_index == trade_count);
            return self.capture_scratch[0..trade_count];
        }

        fn mapRecoveryRecords(
            self: *Self,
            inputs: []const plugin.CaptureInput,
        ) (registry.ResolveError || settlement.SettlementError || BatchError)![]const outbox.PreparedSettlementRecord {
            for (inputs, self.record_scratch[0..inputs.len]) |input, *record| {
                const resolved = try self.destination_registry.resolve(
                    input.specification,
                    input.taker_uid,
                    input.maker_uid,
                );
                if (resolved.destination_id != self.destination_id) {
                    return error.WrongIntegrationDestination;
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
            return self.record_scratch[0..inputs.len];
        }

        const Frontier = enum { risk_r2, journal };

        fn markLocked(
            self: *Self,
            frontier: Frontier,
            sequence: u64,
        ) lifecycle.MarkObservation {
            if (self.phase != .live or sequence == 0) return .rejected;
            const facade = self.facadePtr() orelse return .rejected;
            const cursor: settlement.PublicationCursor = .{
                .domain = self.mapper_policy.publication_domain,
                .publication_sequence = sequence,
            };
            cursor.validate() catch return .rejected;
            const result = switch (frontier) {
                .risk_r2 => facade.markRiskR2CommittedThrough(cursor),
                .journal => facade.markJournalCommittedThrough(cursor),
            };
            return switch (result) {
                .marked => .marked,
                .disabled, .busy, .inactive => .rejected,
            };
        }

        fn finishRecoveryLocked(
            self: *Self,
            sequence: u64,
        ) lifecycle.RecoveryObservation {
            switch (self.phase) {
                .live => {
                    const facade = self.facadePtr() orelse {
                        self.phase = .failed;
                        return .rejected;
                    };
                    const cursor: settlement.PublicationCursor = .{
                        .domain = self.mapper_policy.publication_domain,
                        .publication_sequence = sequence,
                    };
                    self.store.validateRecoveryFrontier(cursor) catch return .rejected;
                    _ = facade.start() catch {
                        self.phase = .failed;
                        return .rejected;
                    };
                    return .not_required;
                },
                .failed => return .rejected,
                .recovering => {},
            }
            if (!self.store.isRecoveryRequired()) return .rejected;
            const cursor: settlement.PublicationCursor = .{
                .domain = self.mapper_policy.publication_domain,
                .publication_sequence = sequence,
            };
            _ = self.store.finishRecovery(cursor) catch return .rejected;

            self.facade = self.createStartedFacade() catch {
                self.phase = .failed;
                return .rejected;
            };
            self.phase = .live;
            return .completed;
        }

        fn facadePtr(self: *Self) ?*Facade {
            return if (self.facade) |*facade| facade else null;
        }
    };
}

fn testDestination(raw: u128) outbox.DestinationId {
    return @enumFromInt(raw);
}

fn testDomain() settlement.PublicationDomain {
    return .{ .namespace = 7, .journal_epoch = 3 };
}

fn testRetryPolicy() dispatcher.RetryPolicy {
    return .{
        .known_not_sent = .{ .initial = .fromInt(1), .maximum = .fromInt(4) },
        .reconciliation = .{ .initial = .fromInt(1), .maximum = .fromInt(4) },
        .confirmed_absent = .{ .initial = .fromInt(1), .maximum = .fromInt(4) },
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

fn testCommand() cmd.OrderCommand {
    return .{
        .command = .place_order,
        .order_id = 9001,
        .symbol = 241,
        .action = .bid,
        .uid = 1001,
        .result_code = .success,
    };
}

fn testBatch(
    command: *const cmd.OrderCommand,
    events_head: ?*const events.MatcherTradeEvent,
    sequence: u64,
    mode: lifecycle.ExecutionMode,
) lifecycle.PreparedBatch {
    return .{
        .sequence = .fromInt(sequence),
        .command = command,
        .specification = testSpecification(),
        .events_head = events_head,
        .mode = mode,
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
        .{ .destination_id = destination_id, .uid = 1003, .currency = 11, .account_id = 423_011 },
        .{ .destination_id = destination_id, .uid = 1003, .currency = 15, .account_id = 423_015 },
    };
    return registry.DestinationRegistry.initValidated(std.testing.allocator, .{
        .destinations = &destinations,
        .symbols = &symbols,
        .currencies = &currencies,
        .accounts = &accounts,
    });
}

const TestTransport = struct {
    submit_calls: usize = 0,
    lookup_calls: usize = 0,
    submitted_groups: [8]u128 = [_]u128{0} ** 8,

    fn asTransport(self: *TestTransport) plugin.Transport {
        return .{ .context = self, .vtable = &vtable };
    }

    fn submit(
        context: *anyopaque,
        destination_id: outbox.DestinationId,
        chain: *const settlement.TransferChain,
    ) dispatcher.SubmitDisposition {
        const self: *TestTransport = @ptrCast(@alignCast(context));
        std.debug.assert(self.submit_calls < self.submitted_groups.len);
        self.submitted_groups[self.submit_calls] = chain.group_id;
        self.submit_calls += 1;
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
        const self: *TestTransport = @ptrCast(@alignCast(context));
        _ = cause;
        self.lookup_calls += 1;
        return .{ .transfers = .{
            .destination_id = destination_id,
            .operation = .lookup_transfers,
            .persisted = false,
            .batch = dispatcher.LookupBatch.init(chain.items()) catch unreachable,
        } };
    }

    const vtable: plugin.Transport.VTable = .{
        .submit = submit,
        .lookup = lookup,
    };
};

/// Instrumented structural Store used to prove which atomic path the bridge
/// selected without weakening the real outbox state machine underneath it.
const FakeStore = struct {
    const Core = outbox.SettlementOutbox(8);

    core: Core,
    prepare_calls: usize = 0,
    prepare_recovery_calls: usize = 0,
    risk_mark_calls: usize = 0,
    journal_mark_calls: usize = 0,
    finish_recovery_calls: usize = 0,

    fn initLive(destination_id: outbox.DestinationId) !FakeStore {
        return .{ .core = try Core.initPrepared(destination_id, testDomain()) };
    }

    fn initRecovering(destination_id: outbox.DestinationId) !FakeStore {
        const initial = try Core.initPrepared(destination_id, testDomain());
        const image = try initial.encodeAlloc(std.testing.allocator);
        defer std.testing.allocator.free(image);
        return .{ .core = try Core.decode(image, destination_id) };
    }

    pub fn destinationId(self: *const FakeStore) outbox.DestinationId {
        return self.core.destinationId();
    }

    pub fn mode(self: *const FakeStore) outbox.OutboxMode {
        return self.core.mode();
    }

    pub fn publicationDomain(self: *const FakeStore) ?outbox.PublicationDomain {
        return self.core.publicationDomain();
    }

    pub fn isRecoveryRequired(self: *const FakeStore) bool {
        return self.core.isRecoveryRequired();
    }

    pub fn recoveryProvenance(self: *const FakeStore) outbox.RecoveryProvenance {
        return self.core.recoveryProvenance();
    }

    pub fn len(self: *const FakeStore) usize {
        return self.core.len();
    }

    pub fn status(self: *const FakeStore, group_id: u128) ?outbox.EntryStatus {
        return self.core.status(group_id);
    }

    pub fn dispatchState(self: *const FakeStore) outbox.DispatchState {
        return self.core.dispatchState();
    }

    pub fn enqueue(
        self: *FakeStore,
        record: outbox.SettlementRecord,
    ) outbox.StoreError!outbox.EnqueueResult {
        return self.core.enqueue(record);
    }

    pub fn prepare(
        self: *FakeStore,
        records: []const outbox.PreparedSettlementRecord,
    ) outbox.StoreError!outbox.PrepareBatchResult {
        self.prepare_calls += 1;
        return self.core.prepare(records);
    }

    pub fn prepareRecovery(
        self: *FakeStore,
        records: []const outbox.PreparedSettlementRecord,
    ) outbox.StoreError!outbox.PrepareBatchResult {
        self.prepare_recovery_calls += 1;
        return self.core.prepareRecovery(records);
    }

    pub fn markRiskR2CommittedThrough(
        self: *FakeStore,
        cursor: outbox.PublicationCursor,
    ) void {
        self.risk_mark_calls += 1;
        self.core.markRiskR2CommittedThrough(cursor);
    }

    pub fn markJournalCommittedThrough(
        self: *FakeStore,
        cursor: outbox.PublicationCursor,
    ) void {
        self.journal_mark_calls += 1;
        self.core.markJournalCommittedThrough(cursor);
    }

    pub fn finishRecovery(
        self: *FakeStore,
        cursor: outbox.PublicationCursor,
    ) outbox.StoreError!usize {
        self.finish_recovery_calls += 1;
        return self.core.finishRecovery(cursor);
    }

    pub fn validateRecoveryFrontier(
        self: *const FakeStore,
        cursor: outbox.PublicationCursor,
    ) outbox.StoreError!void {
        return self.core.validateRecoveryFrontier(cursor);
    }

    pub fn beginNext(
        self: *FakeStore,
        now_tick: u64,
    ) outbox.StoreError!?outbox.DeliveryWork {
        return self.core.beginNext(now_tick);
    }

    pub fn validateClaim(
        self: *const FakeStore,
        work: outbox.DeliveryWork,
    ) outbox.StoreError!outbox.ClaimValidationResult {
        return self.core.validateClaim(work);
    }

    pub fn resolveClaim(
        self: *const FakeStore,
        token: outbox.ClaimToken,
        kind: outbox.WorkKind,
    ) outbox.StoreError!outbox.ClaimResolution {
        return self.core.resolveClaim(token, kind);
    }

    pub fn applySubmitOutcome(
        self: *FakeStore,
        token: outbox.ClaimToken,
        outcome: outbox.SubmitOutcome,
    ) outbox.StoreError!outbox.ApplyResult {
        return self.core.applySubmitOutcome(token, outcome);
    }

    pub fn applyReconcileOutcome(
        self: *FakeStore,
        token: outbox.ClaimToken,
        outcome: outbox.ReconcileOutcome,
    ) outbox.StoreError!outbox.ApplyResult {
        return self.core.applyReconcileOutcome(token, outcome);
    }

    pub fn pruneDeliveredPrefix(self: *FakeStore) outbox.StoreError!usize {
        return self.core.pruneDeliveredPrefix();
    }
};

fn expectPrepared(result: lifecycle.PrepareDisposition) !void {
    try std.testing.expectEqual(lifecycle.PrepareDisposition.prepared, result);
}

test "fresh exchange integration accepts an empty recovery frontier" {
    const Bridge = Integration(FakeStore, 1);
    const destination_id = testDestination(42);
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var store = try FakeStore.initLive(destination_id);
    var transport: TestTransport = .{};
    var integration = try Bridge.init(.{
        .destination_id = destination_id,
        .destination_registry = destination_registry,
        .store = &store,
        .mapper_policy = .{ .publication_domain = testDomain() },
        .retry_policy = testRetryPolicy(),
        .transport = transport.asTransport(),
    });

    try std.testing.expectEqual(outbox.RecoveryProvenance.fresh, store.recoveryProvenance());
    try std.testing.expectEqual(
        lifecycle.RecoveryDisposition.not_required,
        try integration.hooks().finishRecovery(.fromInt(0)),
    );
    try std.testing.expectEqual(IntegrationPhase.live, integration.currentPhase());
}

test "fresh exchange integration proves cold live commands before recovery completion" {
    const Bridge = Integration(FakeStore, 1);
    const destination_id = testDestination(42);
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var store = try FakeStore.initLive(destination_id);
    var transport: TestTransport = .{};
    var integration = try Bridge.init(.{
        .destination_id = destination_id,
        .destination_registry = destination_registry,
        .store = &store,
        .mapper_policy = .{ .publication_domain = testDomain() },
        .retry_policy = testRetryPolicy(),
        .transport = transport.asTransport(),
    });
    const hooks = integration.hooks();
    var command = testCommand();
    var trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 1002,
        .price = 50,
        .size = 2,
    };

    try expectPrepared(try hooks.prepare(testBatch(&command, &trade, 1, .live)));
    _ = try hooks.markRiskR2Committed(.fromInt(1));
    _ = try hooks.markJournalCommitted(.fromInt(1));
    try std.testing.expectEqual(
        lifecycle.RecoveryDisposition.not_required,
        try hooks.finishRecovery(.fromInt(1)),
    );
}

test "missing durable store rejects snapshot and nontrade-only recovered frontiers" {
    const Store = outbox.DurableFileOutbox(1);
    const Bridge = Integration(Store, 1);
    const destination_id = testDestination(42);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var store = try Store.openPrepared(
        std.testing.allocator,
        tmp.dir,
        "missing-settlement-outbox.tso",
        destination_id,
        testDomain(),
    );
    defer store.deinit();
    try std.testing.expectEqual(outbox.RecoveryProvenance.fresh, store.recoveryProvenance());
    var transport: TestTransport = .{};
    var integration = try Bridge.init(.{
        .destination_id = destination_id,
        .destination_registry = destination_registry,
        .store = &store,
        .mapper_policy = .{ .publication_domain = testDomain() },
        .retry_policy = testRetryPolicy(),
        .transport = transport.asTransport(),
    });
    const hooks = integration.hooks();

    // A snapshot-restored prefix emits no settlement replay callbacks.
    try std.testing.expectError(
        error.SettlementRecoveryRejected,
        hooks.finishRecovery(.fromInt(42)),
    );

    // A following non-trade command can publish both live marks, but observing
    // sequence 43 first still cannot prove the skipped snapshot prefix.
    _ = try hooks.markRiskR2Committed(.fromInt(43));
    _ = try hooks.markJournalCommitted(.fromInt(43));
    try std.testing.expectError(
        error.SettlementRecoveryRejected,
        hooks.finishRecovery(.fromInt(43)),
    );
    try std.testing.expectEqual(@as(usize, 0), transport.submit_calls);
}

test "exchange integration maps all live fills before one atomic fake-store prepare" {
    const Bridge = Integration(FakeStore, 4);
    const destination_id = testDestination(42);
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var store = try FakeStore.initLive(destination_id);
    var transport: TestTransport = .{};
    var integration = try Bridge.init(.{
        .destination_id = destination_id,
        .destination_registry = destination_registry,
        .store = &store,
        .mapper_policy = .{ .publication_domain = testDomain() },
        .retry_policy = testRetryPolicy(),
        .transport = transport.asTransport(),
    });
    const hooks = integration.hooks();
    var command = testCommand();
    var first: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 1002,
        .price = 50,
        .size = 2,
    };
    var ignored: events.MatcherTradeEvent = .{ .event_type = .reduce };
    var second: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 1003,
        .price = 51,
        .size = 0,
    };
    first.next_event = &ignored;
    ignored.next_event = &second;

    try std.testing.expectError(
        error.SettlementPreparationRejected,
        hooks.prepare(testBatch(&command, &first, 10, .live)),
    );
    try std.testing.expectEqual(@as(usize, 0), store.prepare_calls);
    try std.testing.expectEqual(@as(usize, 0), store.len());

    second.size = 3;
    try expectPrepared(try hooks.prepare(testBatch(&command, &first, 10, .live)));
    try std.testing.expectEqual(@as(usize, 1), store.prepare_calls);
    try std.testing.expectEqual(@as(usize, 2), store.len());
    const first_group = try (settlement.FillIdentity{
        .namespace = testDomain().namespace,
        .journal_epoch = testDomain().journal_epoch,
        .publication_sequence = .fromInt(10),
        .fill_ordinal = 0,
    }).groupId();
    const second_group = try (settlement.FillIdentity{
        .namespace = testDomain().namespace,
        .journal_epoch = testDomain().journal_epoch,
        .publication_sequence = .fromInt(10),
        .fill_ordinal = 1,
    }).groupId();
    try std.testing.expectEqual(outbox.EntryStatus.ready, store.status(first_group).?);
    try std.testing.expectEqual(outbox.EntryStatus.ready, store.status(second_group).?);
    try std.testing.expectEqual(@as(usize, 0), transport.submit_calls);

    try std.testing.expectEqual(
        lifecycle.MarkDisposition.marked,
        try hooks.markRiskR2Committed(.fromInt(10)),
    );
    const gated = try integration.pollDispatch(.fromInt(0), 2);
    try std.testing.expectEqual(@as(usize, 0), gated.enabled.completed);
    try std.testing.expectEqual(@as(usize, 0), transport.submit_calls);
    try std.testing.expectEqual(
        lifecycle.MarkDisposition.marked,
        try hooks.markJournalCommitted(.fromInt(10)),
    );
    try std.testing.expectEqual(@as(usize, 0), transport.submit_calls);
    const delivered = try integration.pollDispatch(.fromInt(0), 2);
    try std.testing.expectEqual(@as(usize, 2), delivered.enabled.completed);
    try std.testing.expectEqual(@as(usize, 2), transport.submit_calls);
    try std.testing.expectEqual(first_group, transport.submitted_groups[0]);
    try std.testing.expectEqual(second_group, transport.submitted_groups[1]);
}

test "exchange integration rejects a batch above its hard fill bound atomically" {
    const Bridge = Integration(FakeStore, 1);
    const destination_id = testDestination(42);
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var store = try FakeStore.initLive(destination_id);
    var transport: TestTransport = .{};
    var integration = try Bridge.init(.{
        .destination_id = destination_id,
        .destination_registry = destination_registry,
        .store = &store,
        .mapper_policy = .{ .publication_domain = testDomain() },
        .retry_policy = testRetryPolicy(),
        .transport = transport.asTransport(),
    });
    const hooks = integration.hooks();
    var command = testCommand();
    var first: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 1002,
        .price = 50,
        .size = 2,
    };
    var second: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 1003,
        .price = 51,
        .size = 3,
    };
    first.next_event = &second;

    try std.testing.expectError(
        error.SettlementPreparationRejected,
        hooks.prepare(testBatch(&command, &first, 10, .live)),
    );
    try std.testing.expectEqual(@as(usize, 0), store.prepare_calls);
    try std.testing.expectEqual(@as(usize, 0), store.len());
    try std.testing.expectEqual(@as(usize, 0), transport.submit_calls);
}

test "exchange integration real in-memory replay remains fenced until recovery completion" {
    const Store = outbox.SettlementOutbox(2);
    const Bridge = Integration(Store, 2);
    const destination_id = testDestination(42);
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    const initial = try Store.initPrepared(destination_id, testDomain());
    const image = try initial.encodeAlloc(std.testing.allocator);
    defer std.testing.allocator.free(image);
    var store = try Store.decode(image, destination_id);
    var transport: TestTransport = .{};
    var integration = try Bridge.init(.{
        .destination_id = destination_id,
        .destination_registry = destination_registry,
        .store = &store,
        .mapper_policy = .{ .publication_domain = testDomain() },
        .retry_policy = testRetryPolicy(),
        .transport = transport.asTransport(),
    });
    const hooks = integration.hooks();
    var command = testCommand();
    var trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 1002,
        .price = 50,
        .size = 2,
    };

    try expectPrepared(try hooks.prepare(testBatch(&command, &trade, 5, .replay)));
    try std.testing.expectEqual(@as(usize, 1), store.len());
    switch (store.dispatchState()) {
        .recovery_required => {},
        else => return error.ExpectedRecoveryFence,
    }
    try std.testing.expectError(error.RecoveryNotFinished, integration.startDispatch());
    try std.testing.expectError(
        error.RecoveryNotFinished,
        integration.pollDispatch(.fromInt(0), 1),
    );
    try std.testing.expectError(
        error.SettlementRiskR2MarkRejected,
        hooks.markRiskR2Committed(.fromInt(5)),
    );
    try std.testing.expectEqual(@as(usize, 0), transport.submit_calls);

    try std.testing.expectEqual(
        lifecycle.RecoveryDisposition.completed,
        try hooks.finishRecovery(.fromInt(5)),
    );
    try std.testing.expectEqual(IntegrationPhase.live, integration.currentPhase());
    try std.testing.expectEqual(plugin.StartResult.already_running, try integration.startDispatch());
    try std.testing.expectEqual(@as(usize, 0), transport.submit_calls);
    const delivered = try integration.pollDispatch(.fromInt(0), 1);
    try std.testing.expectEqual(@as(usize, 1), delivered.enabled.completed);
    try std.testing.expectEqual(@as(usize, 1), transport.submit_calls);
}

test "exchange integration durable replay reconstructs a missing batch without dispatch before finish" {
    const Store = outbox.DurableFileOutbox(4);
    const Bridge = Integration(Store, 2);
    const destination_id = testDestination(42);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var transport: TestTransport = .{};
    var command = testCommand();
    var trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 1002,
        .price = 50,
        .size = 2,
    };

    {
        var store = try Store.openPrepared(
            std.testing.allocator,
            tmp.dir,
            "exchange-integration.tso",
            destination_id,
            testDomain(),
        );
        defer store.deinit();
        var integration = try Bridge.init(.{
            .destination_id = destination_id,
            .destination_registry = destination_registry,
            .store = &store,
            .mapper_policy = .{ .publication_domain = testDomain() },
            .retry_policy = testRetryPolicy(),
            .transport = transport.asTransport(),
        });
        const hooks = integration.hooks();
        try expectPrepared(try hooks.prepare(testBatch(&command, &trade, 10, .live)));
        try std.testing.expectEqual(@as(usize, 1), store.len());
        try std.testing.expectEqual(@as(usize, 0), transport.submit_calls);
    }

    var reopened = try Store.openPrepared(
        std.testing.allocator,
        tmp.dir,
        "exchange-integration.tso",
        destination_id,
        testDomain(),
    );
    defer reopened.deinit();
    try std.testing.expectEqual(outbox.RecoveryProvenance.reopened, reopened.recoveryProvenance());
    var integration = try Bridge.init(.{
        .destination_id = destination_id,
        .destination_registry = destination_registry,
        .store = &reopened,
        .mapper_policy = .{ .publication_domain = testDomain() },
        .retry_policy = testRetryPolicy(),
        .transport = transport.asTransport(),
    });
    const hooks = integration.hooks();
    try expectPrepared(try hooks.prepare(testBatch(&command, &trade, 10, .replay)));
    trade.price = 51;
    try expectPrepared(try hooks.prepare(testBatch(&command, &trade, 11, .replay)));
    try std.testing.expectEqual(@as(usize, 2), reopened.len());
    try std.testing.expectEqual(@as(usize, 0), transport.submit_calls);
    try std.testing.expectError(
        error.RecoveryNotFinished,
        integration.pollDispatch(.fromInt(0), 2),
    );

    try std.testing.expectEqual(
        lifecycle.RecoveryDisposition.completed,
        try hooks.finishRecovery(.fromInt(11)),
    );
    try std.testing.expectEqual(@as(usize, 0), transport.submit_calls);
    const delivered = try integration.pollDispatch(.fromInt(0), 2);
    try std.testing.expectEqual(@as(usize, 2), delivered.enabled.completed);
    try std.testing.expectEqual(@as(usize, 2), transport.submit_calls);
}

test "exchange integration fails closed on destination mode and publication-domain mismatch" {
    const destination_id = testDestination(42);
    const Store = outbox.SettlementOutbox(1);
    const Bridge = Integration(Store, 1);
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var transport: TestTransport = .{};

    var invalid_policy = testRetryPolicy();
    invalid_policy.known_not_sent.initial = .fromInt(5);
    var valid_store = try Store.initPrepared(destination_id, testDomain());
    try std.testing.expectError(error.InvalidBackoff, Bridge.init(.{
        .destination_id = destination_id,
        .destination_registry = destination_registry,
        .store = &valid_store,
        .mapper_policy = .{ .publication_domain = testDomain() },
        .retry_policy = invalid_policy,
        .transport = transport.asTransport(),
    }));

    var unknown_store = try Store.initPrepared(testDestination(43), testDomain());
    try std.testing.expectError(error.UnknownPluginDestination, Bridge.init(.{
        .destination_id = testDestination(43),
        .destination_registry = destination_registry,
        .store = &unknown_store,
        .mapper_policy = .{ .publication_domain = testDomain() },
        .retry_policy = testRetryPolicy(),
        .transport = transport.asTransport(),
    }));

    var immediate = try Store.init(destination_id);
    try std.testing.expectError(error.StoreModeMismatch, Bridge.init(.{
        .destination_id = destination_id,
        .destination_registry = destination_registry,
        .store = &immediate,
        .mapper_policy = .{ .publication_domain = testDomain() },
        .retry_policy = testRetryPolicy(),
        .transport = transport.asTransport(),
    }));

    var wrong_domain = try Store.initPrepared(destination_id, .{
        .namespace = testDomain().namespace,
        .journal_epoch = testDomain().journal_epoch + 1,
    });
    try std.testing.expectError(error.StoreDomainMismatch, Bridge.init(.{
        .destination_id = destination_id,
        .destination_registry = destination_registry,
        .store = &wrong_domain,
        .mapper_policy = .{ .publication_domain = testDomain() },
        .retry_policy = testRetryPolicy(),
        .transport = transport.asTransport(),
    }));

    var wrong_destination = try Store.initPrepared(testDestination(43), testDomain());
    try std.testing.expectError(error.StoreDestinationMismatch, Bridge.init(.{
        .destination_id = destination_id,
        .destination_registry = destination_registry,
        .store = &wrong_destination,
        .mapper_policy = .{ .publication_domain = testDomain() },
        .retry_policy = testRetryPolicy(),
        .transport = transport.asTransport(),
    }));
}

const ConcurrentMarks = struct {
    fn risk(hooks: lifecycle.Hooks) void {
        var sequence: u64 = 1;
        while (sequence <= 128) : (sequence += 1) {
            _ = hooks.markRiskR2Committed(.fromInt(sequence)) catch @panic("risk mark rejected");
        }
    }

    fn journal(hooks: lifecycle.Hooks) void {
        var sequence: u64 = 1;
        while (sequence <= 128) : (sequence += 1) {
            _ = hooks.markJournalCommitted(.fromInt(sequence)) catch @panic("journal mark rejected");
        }
    }
};

test "exchange integration serializes journal and R2 hook owners without transport" {
    const Bridge = Integration(FakeStore, 1);
    const destination_id = testDestination(42);
    var destination_registry = try initTestRegistry();
    defer destination_registry.deinit();
    var store = try FakeStore.initLive(destination_id);
    var transport: TestTransport = .{};
    var integration = try Bridge.init(.{
        .destination_id = destination_id,
        .destination_registry = destination_registry,
        .store = &store,
        .mapper_policy = .{ .publication_domain = testDomain() },
        .retry_policy = testRetryPolicy(),
        .transport = transport.asTransport(),
    });
    const hooks = integration.hooks();

    const risk_thread = try std.Thread.spawn(.{}, ConcurrentMarks.risk, .{hooks});
    const journal_thread = try std.Thread.spawn(.{}, ConcurrentMarks.journal, .{hooks});
    risk_thread.join();
    journal_thread.join();

    try std.testing.expectEqual(@as(usize, 128), store.risk_mark_calls);
    try std.testing.expectEqual(@as(usize, 128), store.journal_mark_calls);
    try std.testing.expectEqual(@as(usize, 0), transport.submit_calls);
    try std.testing.expectEqual(@as(usize, 0), transport.lookup_calls);
}
