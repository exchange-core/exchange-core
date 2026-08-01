const std = @import("std");
const exchange_core = @import("exchange_core");
const profile_module = @import("parix_settlement_profile.zig");
const saga = @import("parix_funding_saga.zig");
const funding_store = @import("parix_funding_store.zig");

const core = exchange_core.core;
const dispatcher = core.tigerbeetle_dispatcher;
const outbox = core.tigerbeetle_outbox;
const plugin = core.tigerbeetle_plugin;
const settlement = core.tigerbeetle_settlement;

pub const DefaultStore = funding_store.DefaultStore;
pub const OpenPolicy = funding_store.OpenPolicy;

pub const Health = enum(u8) {
    cold,
    ready,
    pending,
    failed,
    quarantined,
    stopped,
};

/// Borrowed synchronous adapter to the exchange-core balance-adjustment API.
/// Implementations must not retain the adjustment or re-enter this runtime.
pub const BalancePort = struct {
    context: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        apply: *const fn (*anyopaque, saga.CoreAdjustment) saga.CoreOutcome,
        observe_adjustment_counter: *const fn (*anyopaque, i64) ?i64,
        is_running: *const fn (*anyopaque) bool,
    };

    pub fn apply(self: BalancePort, adjustment: saga.CoreAdjustment) saga.CoreOutcome {
        return self.vtable.apply(self.context, adjustment);
    }

    pub fn isRunning(self: BalancePort) bool {
        return self.vtable.is_running(self.context);
    }

    pub fn observeAdjustmentCounter(self: BalancePort, uid: i64) ?i64 {
        return self.vtable.observe_adjustment_counter(self.context, uid);
    }
};

pub const ConfigError = error{
    InvalidSnapshotPath,
    InvalidPollInterval,
    TooManyProfileUsers,
    InitialFundingIdOverflow,
};

pub const InitError = ConfigError || std.mem.Allocator.Error || funding_store.StoreError;
pub const SubmitError = profile_module.FundingRouteError || funding_store.StoreError || error{
    RuntimeNotRunning,
    RuntimeFailed,
    RuntimeQuarantined,
};
pub const StatusError = funding_store.StoreError;
pub const StartError = std.Thread.SpawnError || error{
    CoreNotRunning,
    AlreadyStarted,
    RuntimeFailed,
    RuntimeQuarantined,
    RuntimeStopped,
};
pub const StopError = error{CoreNotRunning};
pub const DeinitError = error{ThreadStillRunning};

pub const Config = struct {
    /// Immutable profile and transport dependencies are borrowed for the
    /// runtime's full lifetime.
    profile: *const profile_module.Profile,
    persistence_root: std.Io.Dir,
    transport: plugin.Transport,
    balance_port: BalancePort,
    snapshot_path: []const u8 = "funding-sagas.fsg1",
    open_policy: OpenPolicy = .open_existing,
    poll_interval_ms: u32 = 10,

    pub fn validate(self: Config) ConfigError!void {
        try validateSnapshotPath(self.snapshot_path);
        if (self.poll_interval_ms == 0) return error.InvalidPollInterval;
        if (self.profile.users().len > 16) return error.TooManyProfileUsers;
    }
};

/// App-owned funding orchestrator. The store mutex protects only durable
/// state transitions. External TigerBeetle and core callbacks always execute
/// after it has been released.
pub const Runtime = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    io: std.Io,
    profile: *const profile_module.Profile,
    destination_id: saga.DestinationId,
    transport: plugin.Transport,
    balance_port: BalancePort,
    poll_interval_ms: u32,
    store: *DefaultStore,
    store_mutex: std.Io.Mutex = .init,
    worker: ?std.Thread = null,
    stop_requested: std.atomic.Value(bool) = .init(false),
    accepting: std.atomic.Value(bool) = .init(false),
    wakeup: std.Io.Event = .unset,
    cached_health: std.atomic.Value(u8) = .init(@intFromEnum(Health.cold)),

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        config: Config,
    ) InitError!Self {
        try config.validate();

        var floor_storage: [16]saga.LocalIdFloor = undefined;
        const floors = try deriveLocalIdFloors(config.profile, &floor_storage);
        var observed_storage: [16]saga.LocalIdFloor = undefined;
        const observed = try observeCoreCounters(
            config.balance_port,
            floors,
            &observed_storage,
        );
        const binding: saga.Binding = .{
            .destination_id = config.profile.destinationId(),
            .registry_digest = config.profile.registryDigest(),
            .profile_digest = config.profile.digest(),
        };

        const store = try allocator.create(DefaultStore);
        errdefer allocator.destroy(store);
        try DefaultStore.openInto(
            store,
            allocator,
            io,
            config.persistence_root,
            config.snapshot_path,
            config.open_policy,
            binding,
            floors,
            observed,
        );
        errdefer store.deinit();
        const initial_health = healthFromSummary(try store.summary());

        return .{
            .allocator = allocator,
            .io = io,
            .profile = config.profile,
            .destination_id = config.profile.destinationId(),
            .transport = config.transport,
            .balance_port = config.balance_port,
            .poll_interval_ms = config.poll_interval_ms,
            .store = store,
            .cached_health = .init(@intFromEnum(initial_health)),
        };
    }

    /// Opens admission and starts the only side-effect worker. The caller must
    /// invoke this only after core startup and keep every borrowed dependency
    /// alive at a stable address until `stopAndJoin` returns.
    pub fn start(self: *Self) StartError!void {
        if (!self.balance_port.isRunning()) return error.CoreNotRunning;
        if (self.worker != null) return error.AlreadyStarted;
        if (self.store.isPoisoned()) return error.RuntimeFailed;
        const durable_health = healthFromSummary(self.store.summary() catch {
            self.storeHealth(.failed);
            return error.RuntimeFailed;
        });
        self.storeHealth(durable_health);
        switch (self.health()) {
            .failed => return error.RuntimeFailed,
            .quarantined => return error.RuntimeQuarantined,
            .stopped => return error.RuntimeStopped,
            .cold, .ready, .pending => {},
        }

        self.stop_requested.store(false, .release);
        self.accepting.store(true, .release);
        self.wakeup.reset();
        self.worker = std.Thread.spawn(.{}, workerMain, .{self}) catch |err| {
            self.accepting.store(false, .release);
            self.storeHealth(.failed);
            return err;
        };
        self.wakeup.set(self.io);
    }

    /// Fences admission and joins the worker while the core is still live.
    /// In-flight or queued work stays in FSG1 and is resumed on the next open.
    pub fn stopAndJoin(self: *Self) StopError!void {
        self.accepting.store(false, .release);
        // Pair the admission fence with the mutex used by `submit`, ensuring
        // no already-admitted snapshot publication remains in progress.
        self.store_mutex.lockUncancelable(self.io);
        self.store_mutex.unlock(self.io);
        self.stop_requested.store(true, .release);
        self.wakeup.set(self.io);
        if (self.worker) |worker| {
            worker.join();
            self.worker = null;
        }
        self.storeHealth(.stopped);
        if (!self.balance_port.isRunning()) return error.CoreNotRunning;
    }

    pub fn deinit(self: *Self) DeinitError!void {
        if (self.worker != null) return error.ThreadStillRunning;
        self.store.deinit();
        self.allocator.destroy(self.store);
        self.* = undefined;
    }

    pub fn health(self: *const Self) Health {
        return @enumFromInt(self.cached_health.load(.acquire));
    }

    /// Durable idempotent admission. An exact duplicate returns its current
    /// status; the saga layer rejects any immutable mismatch.
    pub fn submit(
        self: *Self,
        kind: saga.Kind,
        operation_id: u128,
        uid: i64,
        currency: i32,
        amount: i64,
    ) SubmitError!saga.AcceptResult {
        try self.ensureAccepting();
        const route = try self.profile.fundingRoute(uid, currency);
        const request: saga.Request = .{
            .operation_id = operation_id,
            .kind = kind,
            .uid = uid,
            .currency = currency,
            .amount = amount,
        };

        self.store_mutex.lockUncancelable(self.io);
        self.ensureAccepting() catch |err| {
            self.store_mutex.unlock(self.io);
            return err;
        };
        const accepted = self.store.accept(request, routeFromProfile(route)) catch |err| {
            self.store_mutex.unlock(self.io);
            self.observeStoreFailure();
            return err;
        };
        self.store_mutex.unlock(self.io);
        self.wakeup.set(self.io);
        return accepted;
    }

    pub fn status(self: *Self, operation_id: u128) StatusError!saga.Status {
        self.store_mutex.lockUncancelable(self.io);
        defer self.store_mutex.unlock(self.io);
        return self.store.lookup(operation_id) catch |err| {
            self.observeStoreFailure();
            return err;
        };
    }

    /// Executes at most one already-durable claim. This caller-driven entry
    /// point is also useful for deterministic tests and embedded hosts.
    pub fn pollOnce(self: *Self) void {
        if (self.stop_requested.load(.acquire)) return;
        if (isTerminalRuntimeHealth(self.health())) return;
        if (!self.balance_port.isRunning()) {
            self.accepting.store(false, .release);
            self.storeHealth(.failed);
            return;
        }

        const claim = self.claimNext() orelse {
            if (!isTerminalRuntimeHealth(self.health())) self.storeHealth(.ready);
            return;
        };
        self.storeHealth(.pending);

        const completion: saga.Completion = switch (claim.action) {
            .remote_submit => |transfer| .{
                .remote_submit = self.executeRemoteSubmit(transfer),
            },
            .remote_lookup => |lookup| .{
                .remote_lookup = self.executeRemoteLookup(lookup),
            },
            .core_adjust => |adjustment| .{
                .core_adjust = self.balance_port.apply(adjustment),
            },
        };

        self.store_mutex.lockUncancelable(self.io);
        const completed = self.store.complete(claim, completion) catch {
            self.store_mutex.unlock(self.io);
            self.observeStoreFailure();
            return;
        };
        self.store_mutex.unlock(self.io);
        switch (completed.state) {
            .quarantined => {
                self.accepting.store(false, .release);
                self.storeHealth(.quarantined);
            },
            .accepted, .pending, .completed, .failed => {},
        }
    }

    fn workerMain(self: *Self) void {
        while (!self.stop_requested.load(.acquire)) {
            self.pollOnce();
            if (isTerminalRuntimeHealth(self.health())) return;
            self.wakeup.waitTimeout(self.io, .{ .duration = .{
                .raw = .fromMilliseconds(@intCast(self.poll_interval_ms)),
                .clock = .awake,
            } }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => {
                    self.accepting.store(false, .release);
                    self.storeHealth(.failed);
                    return;
                },
            };
        }
    }

    fn claimNext(self: *Self) ?saga.Claim {
        self.store_mutex.lockUncancelable(self.io);
        defer self.store_mutex.unlock(self.io);
        return self.store.claimNext() catch {
            self.observeStoreFailure();
            return null;
        };
    }

    fn executeRemoteSubmit(
        self: *Self,
        transfer: settlement.Transfer,
    ) saga.RemoteSubmitOutcome {
        var chain = oneTransferChain(transfer);
        const disposition = self.transport.vtable.submit(
            self.transport.context,
            self.destination_id,
            &chain,
        );
        return switch (disposition) {
            .success => |receipt| if (receipt.destination_id != self.destination_id)
                .destination_mismatch
            else if (receipt.operation != .create_transfers or !receipt.persisted)
                .ambiguous
            else
                .applied,
            .conflict => |receipt| if (receipt.request_destination_id != self.destination_id)
                .destination_mismatch
            else if (receipt.request_operation != .create_transfers or
                receipt.item_error_count == 0)
                .ambiguous
            else
                .conflict,
            .known_not_sent => .known_not_sent,
            .ambiguous => .ambiguous,
            .definitely_rejected => .definitely_rejected,
            .permanent => .operator_action,
            .destination_mismatch => .destination_mismatch,
        };
    }

    fn executeRemoteLookup(
        self: *Self,
        lookup: saga.RemoteLookup,
    ) saga.RemoteLookupOutcome {
        var chain = oneTransferChain(lookup.transfer);
        const disposition = self.transport.vtable.lookup(
            self.transport.context,
            self.destination_id,
            &chain,
            switch (lookup.cause) {
                .ambiguous_submission => .ambiguous_submission,
                .conflict_response => .rejected_submission,
            },
        );
        return switch (disposition) {
            .transfers => |receipt| if (receipt.destination_id != self.destination_id)
                .destination_mismatch
            else if (receipt.operation != .lookup_transfers or receipt.persisted)
                .permanent_failure
            else switch (dispatcher.classifyLookup(chain, receipt.batch)) {
                .all_present_exact => .exact,
                .all_absent => .absent,
                .partial_application => .partial_application,
                .transfer_mismatch => .transfer_mismatch,
            },
            .retryable => .retryable,
            .permanent => .permanent_failure,
            .destination_mismatch => .destination_mismatch,
        };
    }

    fn ensureAccepting(self: *const Self) SubmitError!void {
        if (!self.accepting.load(.acquire)) return error.RuntimeNotRunning;
        return switch (self.health()) {
            .failed => error.RuntimeFailed,
            .quarantined => error.RuntimeQuarantined,
            .cold, .stopped => error.RuntimeNotRunning,
            .ready, .pending => {},
        };
    }

    fn observeStoreFailure(self: *Self) void {
        if (self.store.isPoisoned()) {
            self.accepting.store(false, .release);
            self.storeHealth(.failed);
        }
    }

    fn storeHealth(self: *Self, value: Health) void {
        self.cached_health.store(@intFromEnum(value), .release);
    }
};

fn routeFromProfile(route: profile_module.FundingRoute) saga.Route {
    return .{
        .destination_id = route.destination_id,
        .ledger = route.ledger,
        .treasury_account_id = route.treasury_account_id,
        .user_account_id = route.user_account_id,
    };
}

fn oneTransferChain(transfer: settlement.Transfer) settlement.TransferChain {
    var chain: settlement.TransferChain = .{
        .group_id = transfer.user_data_128,
        .storage = [_]settlement.Transfer{std.mem.zeroes(settlement.Transfer)} **
            settlement.max_transfers_per_fill,
        .count = 1,
    };
    chain.storage[0] = transfer;
    return chain;
}

fn deriveLocalIdFloors(
    profile: *const profile_module.Profile,
    storage: *[16]saga.LocalIdFloor,
) ConfigError![]const saga.LocalIdFloor {
    const users = profile.users();
    if (users.len > storage.len) return error.TooManyProfileUsers;
    for (users, 0..) |user, index| {
        storage[index] = .{ .uid = user.uid, .last_transaction_id = 0 };
    }
    for (profile.initialFunding()) |funding| {
        const transaction_id = std.math.cast(i64, funding.id) orelse
            return error.InitialFundingIdOverflow;
        for (storage[0..users.len]) |*floor| {
            if (floor.uid == funding.uid) {
                floor.last_transaction_id = @max(floor.last_transaction_id, transaction_id);
                break;
            }
        }
    }
    return storage[0..users.len];
}

fn observeCoreCounters(
    port: BalancePort,
    floors: []const saga.LocalIdFloor,
    storage: *[16]saga.LocalIdFloor,
) funding_store.StoreError![]const saga.LocalIdFloor {
    if (floors.len > storage.len) return error.InvalidFloor;
    for (floors, 0..) |floor, index| {
        storage[index] = .{
            .uid = floor.uid,
            .last_transaction_id = port.observeAdjustmentCounter(floor.uid) orelse
                return error.MissingCoreCounter,
        };
    }
    return storage[0..floors.len];
}

fn validateSnapshotPath(path: []const u8) ConfigError!void {
    if (path.len == 0 or std.mem.eql(u8, path, ".") or std.mem.eql(u8, path, "..")) {
        return error.InvalidSnapshotPath;
    }
    for (path) |byte| {
        if (std.fs.path.isSep(byte)) return error.InvalidSnapshotPath;
    }
}

fn isTerminalRuntimeHealth(health: Health) bool {
    return switch (health) {
        .failed, .quarantined, .stopped => true,
        .cold, .ready, .pending => false,
    };
}

fn healthFromSummary(summary: saga.Summary) Health {
    if (summary.quarantined != 0) return .quarantined;
    if (summary.accepted != 0 or summary.pending != 0) return .pending;
    return .ready;
}

test "transport disposition mapping is conservative and exact" {
    const transfer: settlement.Transfer = .{
        .id = 0x8000_0000_0000_0000_0000_0000_0000_0001,
        .debit_account_id = 10,
        .credit_account_id = 11,
        .amount = 25,
        .user_data_128 = 99,
        .ledger = 421,
        .code = saga.deposit_transfer_code,
    };
    const chain = oneTransferChain(transfer);
    try std.testing.expectEqual(@as(usize, 1), chain.items().len);
    try std.testing.expectEqual(transfer, chain.items()[0]);

    const exact = try dispatcher.LookupBatch.init(&.{transfer});
    try std.testing.expectEqual(
        dispatcher.ReconciliationEvidence.all_present_exact,
        dispatcher.classifyLookup(chain, exact),
    );
    const absent = try dispatcher.LookupBatch.init(&.{});
    try std.testing.expectEqual(
        dispatcher.ReconciliationEvidence.all_absent,
        dispatcher.classifyLookup(chain, absent),
    );
}

const FakeRemote = struct {
    const Mode = enum {
        success,
        ambiguous_once,
        ambiguous_absent_once,
        conflict_absent,
        definitely_rejected,
        permanent,
        destination_mismatch,
    };

    destination_id: saga.DestinationId,
    mode: Mode = .success,
    present: ?settlement.Transfer = null,
    submits: usize = 0,
    lookups: usize = 0,
    economic_effects: usize = 0,
    malformed_request: bool = false,
    last_lookup_cause: ?outbox.ReconciliationCause = null,

    fn transport(self: *FakeRemote) plugin.Transport {
        return .{ .context = self, .vtable = &vtable };
    }

    fn submitCallback(
        context: *anyopaque,
        destination_id: outbox.DestinationId,
        chain: *const settlement.TransferChain,
    ) dispatcher.SubmitDisposition {
        const self: *FakeRemote = @ptrCast(@alignCast(context));
        self.submits += 1;
        if (destination_id != self.destination_id or chain.items().len != 1) {
            self.malformed_request = true;
            return .destination_mismatch;
        }
        if (self.mode == .destination_mismatch) return .destination_mismatch;
        if (self.mode == .definitely_rejected) return .definitely_rejected;
        if (self.mode == .permanent) return .permanent;
        if (self.mode == .conflict_absent) return .{ .conflict = .{
            .request_destination_id = self.destination_id,
            .request_operation = .create_transfers,
            .item_error_count = 1,
        } };
        if (self.mode == .ambiguous_absent_once) {
            self.mode = .success;
            return .ambiguous;
        }

        const transfer = chain.items()[0];
        if (self.present) |existing| {
            if (!std.meta.eql(existing, transfer)) return .permanent;
        } else {
            self.present = transfer;
            self.economic_effects += 1;
        }
        if (self.mode == .ambiguous_once) {
            self.mode = .success;
            return .ambiguous;
        }
        return .{ .success = .{
            .destination_id = self.destination_id,
            .operation = .create_transfers,
            .persisted = true,
        } };
    }

    fn lookupCallback(
        context: *anyopaque,
        destination_id: outbox.DestinationId,
        chain: *const settlement.TransferChain,
        cause: outbox.ReconciliationCause,
    ) dispatcher.LookupDisposition {
        const self: *FakeRemote = @ptrCast(@alignCast(context));
        self.last_lookup_cause = cause;
        self.lookups += 1;
        if (destination_id != self.destination_id or chain.items().len != 1) {
            self.malformed_request = true;
            return .destination_mismatch;
        }
        const batch = if (self.present) |transfer|
            dispatcher.LookupBatch.init(&.{transfer}) catch unreachable
        else
            dispatcher.LookupBatch.init(&.{}) catch unreachable;
        return .{ .transfers = .{
            .destination_id = self.destination_id,
            .operation = .lookup_transfers,
            .persisted = false,
            .batch = batch,
        } };
    }

    const vtable: plugin.Transport.VTable = .{
        .submit = submitCallback,
        .lookup = lookupCallback,
    };
};

const FakeBalance = struct {
    running: bool = true,
    adjustments: [16]saga.CoreAdjustment = undefined,
    adjustment_count: usize = 0,
    economic_effects: usize = 0,
    net: i64 = 0,
    retry_once: bool = false,
    counters: [16]saga.LocalIdFloor = undefined,
    counter_count: usize = 0,
    missing_uid: ?i64 = null,

    fn port(self: *FakeBalance) BalancePort {
        return .{ .context = self, .vtable = &vtable };
    }

    fn applyCallback(context: *anyopaque, adjustment: saga.CoreAdjustment) saga.CoreOutcome {
        const self: *FakeBalance = @ptrCast(@alignCast(context));
        if (self.retry_once) {
            self.retry_once = false;
            return .retryable;
        }
        var counter: ?*saga.LocalIdFloor = null;
        for (self.counters[0..self.counter_count]) |*candidate| {
            if (candidate.uid == adjustment.uid) {
                counter = candidate;
                break;
            }
        }
        const current = counter orelse return .rejected;
        if (current.last_transaction_id == adjustment.transaction_id) return .already_applied_same;
        if (current.last_transaction_id > adjustment.transaction_id) return .already_applied_many;
        if (self.adjustment_count == self.adjustments.len) return .rejected;
        const next_net = std.math.add(i64, self.net, adjustment.amount) catch return .rejected;
        self.adjustments[self.adjustment_count] = adjustment;
        self.adjustment_count += 1;
        self.economic_effects += 1;
        self.net = next_net;
        current.last_transaction_id = adjustment.transaction_id;
        return .applied;
    }

    fn observeCallback(context: *anyopaque, uid: i64) ?i64 {
        const self: *FakeBalance = @ptrCast(@alignCast(context));
        if (self.missing_uid == uid) return null;
        for (self.counters[0..self.counter_count]) |counter| {
            if (counter.uid == uid) return counter.last_transaction_id;
        }
        return null;
    }

    fn isRunningCallback(context: *anyopaque) bool {
        const self: *FakeBalance = @ptrCast(@alignCast(context));
        return self.running;
    }

    const vtable: BalancePort.VTable = .{
        .apply = applyCallback,
        .observe_adjustment_counter = observeCallback,
        .is_running = isRunningCallback,
    };

    fn ensureProfileCounters(
        self: *FakeBalance,
        profile: *const profile_module.Profile,
    ) !void {
        if (self.counter_count != 0) return;
        var storage: [16]saga.LocalIdFloor = undefined;
        const floors = try deriveLocalIdFloors(profile, &storage);
        @memcpy(self.counters[0..floors.len], floors);
        self.counter_count = floors.len;
    }
};

fn enableManualPolling(runtime: *Runtime) void {
    runtime.stop_requested.store(false, .release);
    runtime.accepting.store(true, .release);
    runtime.storeHealth(healthFromSummary(runtime.store.summary() catch unreachable));
}

fn testRuntime(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    profile: *const profile_module.Profile,
    remote: *FakeRemote,
    balance: *FakeBalance,
) !Runtime {
    try balance.ensureProfileCounters(profile);
    return Runtime.init(allocator, std.testing.io, .{
        .profile = profile,
        .persistence_root = dir,
        .transport = remote.transport(),
        .balance_port = balance.port(),
        .open_policy = .initialize,
        .poll_interval_ms = 1,
    });
}

fn reopenTestRuntime(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    profile: *const profile_module.Profile,
    remote: *FakeRemote,
    balance: *FakeBalance,
) !Runtime {
    try balance.ensureProfileCounters(profile);
    return Runtime.init(allocator, std.testing.io, .{
        .profile = profile,
        .persistence_root = dir,
        .transport = remote.transport(),
        .balance_port = balance.port(),
        .open_policy = .open_existing,
        .poll_interval_ms = 1,
    });
}

test "manual deposit is durable idempotent and applies each side effect once" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profile = try profile_module.Profile.developmentExample(allocator);
    defer profile.deinit();
    var remote: FakeRemote = .{ .destination_id = profile.destinationId() };
    var balance: FakeBalance = .{};
    var runtime = try testRuntime(allocator, tmp.dir, profile, &remote, &balance);
    defer runtime.deinit() catch unreachable;
    enableManualPolling(&runtime);

    const accepted = try runtime.submit(.deposit, 6_001, 1_001, 11, 50);
    try std.testing.expectEqual(saga.AcceptDisposition.inserted, accepted.disposition);
    runtime.pollOnce();
    runtime.pollOnce();
    const completed = try runtime.status(6_001);
    try std.testing.expectEqual(saga.PublicState.completed, completed.state);
    try std.testing.expectEqual(@as(usize, 1), remote.economic_effects);
    try std.testing.expectEqual(@as(usize, 1), balance.economic_effects);
    try std.testing.expectEqual(@as(i64, 8_421_012), balance.adjustments[0].transaction_id);

    const duplicate = try runtime.submit(.deposit, 6_001, 1_001, 11, 50);
    try std.testing.expectEqual(saga.AcceptDisposition.existing, duplicate.disposition);
    runtime.pollOnce();
    try std.testing.expectEqual(@as(usize, 1), remote.economic_effects);
    try std.testing.expectEqual(@as(usize, 1), balance.economic_effects);
}

test "withdrawal definite rejection compensates with reserved consecutive id" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profile = try profile_module.Profile.developmentExample(allocator);
    defer profile.deinit();
    var remote: FakeRemote = .{
        .destination_id = profile.destinationId(),
        .mode = .definitely_rejected,
    };
    var balance: FakeBalance = .{};
    var runtime = try testRuntime(allocator, tmp.dir, profile, &remote, &balance);
    defer runtime.deinit() catch unreachable;
    enableManualPolling(&runtime);

    _ = try runtime.submit(.withdrawal, 6_002, 1_001, 11, 75);
    runtime.pollOnce();
    runtime.pollOnce();
    runtime.pollOnce();
    const failed = try runtime.status(6_002);
    try std.testing.expectEqual(saga.PublicState.failed, failed.state);
    try std.testing.expectEqual(saga.FailureReason.external_rejected, failed.failure_reason.?);
    try std.testing.expectEqual(@as(usize, 2), balance.economic_effects);
    try std.testing.expectEqual(@as(i64, -75), balance.adjustments[0].amount);
    try std.testing.expectEqual(@as(i64, 75), balance.adjustments[1].amount);
    try std.testing.expectEqual(
        balance.adjustments[0].transaction_id + 1,
        balance.adjustments[1].transaction_id,
    );
    try std.testing.expectEqual(@as(i64, 0), balance.net);
}

test "generic permanent submit quarantines without compensation" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profile = try profile_module.Profile.developmentExample(allocator);
    defer profile.deinit();
    var remote: FakeRemote = .{
        .destination_id = profile.destinationId(),
        .mode = .permanent,
    };
    var balance: FakeBalance = .{};
    var runtime = try testRuntime(allocator, tmp.dir, profile, &remote, &balance);
    defer runtime.deinit() catch unreachable;
    enableManualPolling(&runtime);

    _ = try runtime.submit(.withdrawal, 6_008, 1_001, 11, 75);
    runtime.pollOnce();
    runtime.pollOnce();
    const quarantined = try runtime.status(6_008);
    try std.testing.expectEqual(saga.PublicState.quarantined, quarantined.state);
    try std.testing.expectEqual(
        saga.QuarantineReason.reconciliation_failed,
        quarantined.quarantine_reason.?,
    );
    try std.testing.expectEqual(@as(usize, 1), balance.economic_effects);
    try std.testing.expectEqual(@as(i64, -75), balance.net);
}

test "ambiguous submit reconciles exact transfer without duplicate effect" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profile = try profile_module.Profile.developmentExample(allocator);
    defer profile.deinit();
    var remote: FakeRemote = .{
        .destination_id = profile.destinationId(),
        .mode = .ambiguous_once,
    };
    var balance: FakeBalance = .{};
    var runtime = try testRuntime(allocator, tmp.dir, profile, &remote, &balance);
    defer runtime.deinit() catch unreachable;
    enableManualPolling(&runtime);

    _ = try runtime.submit(.deposit, 6_003, 1_001, 11, 90);
    runtime.pollOnce();
    runtime.pollOnce();
    runtime.pollOnce();
    try std.testing.expectEqual(saga.PublicState.completed, (try runtime.status(6_003)).state);
    try std.testing.expectEqual(@as(usize, 1), remote.submits);
    try std.testing.expectEqual(@as(usize, 1), remote.lookups);
    try std.testing.expectEqual(@as(usize, 1), remote.economic_effects);
    try std.testing.expectEqual(@as(usize, 1), balance.economic_effects);
}

test "ambiguous absent lookup safely retries the immutable transfer" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profile = try profile_module.Profile.developmentExample(allocator);
    defer profile.deinit();
    var remote: FakeRemote = .{
        .destination_id = profile.destinationId(),
        .mode = .ambiguous_absent_once,
    };
    var balance: FakeBalance = .{};
    var runtime = try testRuntime(allocator, tmp.dir, profile, &remote, &balance);
    defer runtime.deinit() catch unreachable;
    enableManualPolling(&runtime);

    _ = try runtime.submit(.deposit, 6_011, 1_001, 11, 30);
    runtime.pollOnce();
    runtime.pollOnce();
    runtime.pollOnce();
    runtime.pollOnce();
    try std.testing.expectEqual(saga.PublicState.completed, (try runtime.status(6_011)).state);
    try std.testing.expectEqual(@as(usize, 2), remote.submits);
    try std.testing.expectEqual(@as(usize, 1), remote.lookups);
    try std.testing.expectEqual(outbox.ReconciliationCause.ambiguous_submission, remote.last_lookup_cause.?);
    try std.testing.expectEqual(@as(usize, 1), remote.economic_effects);
    try std.testing.expectEqual(@as(usize, 1), balance.economic_effects);
}

test "withdrawal conflict absent quarantines across restart without compensation or resubmit" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profile = try profile_module.Profile.developmentExample(allocator);
    defer profile.deinit();
    var remote: FakeRemote = .{
        .destination_id = profile.destinationId(),
        .mode = .conflict_absent,
    };
    var balance: FakeBalance = .{};

    var before = try testRuntime(allocator, tmp.dir, profile, &remote, &balance);
    enableManualPolling(&before);
    _ = try before.submit(.withdrawal, 6_012, 1_001, 11, 35);
    before.pollOnce();
    before.pollOnce();
    try before.deinit();

    var after = try reopenTestRuntime(allocator, tmp.dir, profile, &remote, &balance);
    defer after.deinit() catch unreachable;
    enableManualPolling(&after);
    after.pollOnce();
    after.pollOnce();
    const quarantined = try after.status(6_012);
    try std.testing.expectEqual(saga.PublicState.quarantined, quarantined.state);
    try std.testing.expectEqual(
        saga.QuarantineReason.reconciliation_failed,
        quarantined.quarantine_reason.?,
    );
    try std.testing.expectEqual(@as(usize, 1), remote.submits);
    try std.testing.expectEqual(@as(usize, 1), remote.lookups);
    try std.testing.expectEqual(outbox.ReconciliationCause.rejected_submission, remote.last_lookup_cause.?);
    try std.testing.expectEqual(@as(usize, 0), remote.economic_effects);
    try std.testing.expectEqual(@as(usize, 1), balance.economic_effects);
    try std.testing.expectEqual(@as(i64, -35), balance.net);
}

test "restart converts durable submit claim to lookup before resend" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profile = try profile_module.Profile.developmentExample(allocator);
    defer profile.deinit();
    var remote: FakeRemote = .{ .destination_id = profile.destinationId() };
    var balance: FakeBalance = .{};

    var before = try testRuntime(allocator, tmp.dir, profile, &remote, &balance);
    enableManualPolling(&before);
    _ = try before.submit(.deposit, 6_004, 1_001, 11, 40);
    const exposed = before.claimNext().?;
    try std.testing.expect(exposed.action == .remote_submit);
    try before.deinit();

    var after = try reopenTestRuntime(allocator, tmp.dir, profile, &remote, &balance);
    defer after.deinit() catch unreachable;
    enableManualPolling(&after);
    runtime_recovery: {
        after.pollOnce(); // authoritative lookup proves absence
        try std.testing.expectEqual(@as(usize, 1), remote.lookups);
        try std.testing.expectEqual(@as(usize, 0), remote.submits);
        after.pollOnce(); // exact immutable transfer is now safe to submit
        after.pollOnce(); // local credit
        break :runtime_recovery;
    }
    try std.testing.expectEqual(saga.PublicState.completed, (try after.status(6_004)).state);
    try std.testing.expectEqual(@as(usize, 1), remote.economic_effects);
    try std.testing.expectEqual(@as(usize, 1), balance.economic_effects);
}

test "ambiguous storage failure poisons admission before external IO" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profile = try profile_module.Profile.developmentExample(allocator);
    defer profile.deinit();
    var remote: FakeRemote = .{ .destination_id = profile.destinationId() };
    var balance: FakeBalance = .{};
    var runtime = try testRuntime(allocator, tmp.dir, profile, &remote, &balance);
    defer runtime.deinit() catch unreachable;
    enableManualPolling(&runtime);

    runtime.store.injectWriteFailureForTesting();
    try std.testing.expectError(
        error.StorageAmbiguous,
        runtime.submit(.deposit, 6_005, 1_001, 11, 10),
    );
    try std.testing.expectEqual(Health.failed, runtime.health());
    runtime.pollOnce();
    try std.testing.expectEqual(@as(usize, 0), remote.submits);
    try std.testing.expectEqual(@as(usize, 0), remote.lookups);
    try std.testing.expectEqual(@as(usize, 0), balance.economic_effects);
}

test "reopened quarantine remains fenced before worker start" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profile = try profile_module.Profile.developmentExample(allocator);
    defer profile.deinit();
    var remote: FakeRemote = .{
        .destination_id = profile.destinationId(),
        .mode = .destination_mismatch,
    };
    var balance: FakeBalance = .{};

    var before = try testRuntime(allocator, tmp.dir, profile, &remote, &balance);
    enableManualPolling(&before);
    _ = try before.submit(.deposit, 6_006, 1_001, 11, 10);
    before.pollOnce();
    try std.testing.expectEqual(Health.quarantined, before.health());
    try before.deinit();

    var after = try reopenTestRuntime(allocator, tmp.dir, profile, &remote, &balance);
    defer after.deinit() catch unreachable;
    try std.testing.expectEqual(Health.quarantined, after.health());
    try std.testing.expectError(error.RuntimeQuarantined, after.start());
    try std.testing.expectEqual(@as(usize, 1), remote.submits);
}

test "worker lifecycle starts only with a running core and joins before stop" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profile = try profile_module.Profile.developmentExample(allocator);
    defer profile.deinit();
    var remote: FakeRemote = .{ .destination_id = profile.destinationId() };
    var balance: FakeBalance = .{ .running = false };
    var runtime = try testRuntime(allocator, tmp.dir, profile, &remote, &balance);
    defer runtime.deinit() catch unreachable;

    try std.testing.expectError(error.CoreNotRunning, runtime.start());
    balance.running = true;
    try runtime.start();
    try std.testing.expectError(error.AlreadyStarted, runtime.start());
    try runtime.stopAndJoin();
    try std.testing.expectEqual(Health.stopped, runtime.health());
}

test "retryable core adjustment replays exact reserved id" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profile = try profile_module.Profile.developmentExample(allocator);
    defer profile.deinit();
    var remote: FakeRemote = .{ .destination_id = profile.destinationId() };
    var balance: FakeBalance = .{ .retry_once = true };
    var runtime = try testRuntime(allocator, tmp.dir, profile, &remote, &balance);
    defer runtime.deinit() catch unreachable;
    enableManualPolling(&runtime);

    _ = try runtime.submit(.deposit, 6_007, 1_001, 11, 12);
    runtime.pollOnce();
    runtime.pollOnce();
    try std.testing.expectEqual(saga.PublicState.pending, (try runtime.status(6_007)).state);
    runtime.pollOnce();
    try std.testing.expectEqual(saga.PublicState.completed, (try runtime.status(6_007)).state);
    try std.testing.expectEqual(@as(usize, 1), remote.economic_effects);
    try std.testing.expectEqual(@as(usize, 1), balance.economic_effects);
    try std.testing.expectEqual(@as(i64, 8_421_012), balance.adjustments[0].transaction_id);
}

test "runtime initialization requires every live core counter" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profile = try profile_module.Profile.developmentExample(allocator);
    defer profile.deinit();
    var remote: FakeRemote = .{ .destination_id = profile.destinationId() };
    var balance: FakeBalance = .{};
    try balance.ensureProfileCounters(profile);
    balance.missing_uid = 1_001;

    try std.testing.expectError(error.MissingCoreCounter, Runtime.init(allocator, std.testing.io, .{
        .profile = profile,
        .persistence_root = tmp.dir,
        .transport = remote.transport(),
        .balance_port = balance.port(),
        .open_policy = .initialize,
    }));
}

test "runtime restart rejects a core counter behind durable confirmation" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profile = try profile_module.Profile.developmentExample(allocator);
    defer profile.deinit();
    var remote: FakeRemote = .{ .destination_id = profile.destinationId() };
    var balance: FakeBalance = .{};

    var before = try testRuntime(allocator, tmp.dir, profile, &remote, &balance);
    enableManualPolling(&before);
    _ = try before.submit(.deposit, 6_009, 1_001, 11, 20);
    before.pollOnce();
    before.pollOnce();
    try std.testing.expectEqual(saga.PublicState.completed, (try before.status(6_009)).state);
    try before.deinit();

    balance.counters[0].last_transaction_id -= 1;
    try std.testing.expectError(
        error.CoreCounterRollback,
        reopenTestRuntime(allocator, tmp.dir, profile, &remote, &balance),
    );
}

test "runtime accepts exact in flight core evidence across repeated restart" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profile = try profile_module.Profile.developmentExample(allocator);
    defer profile.deinit();
    var remote: FakeRemote = .{ .destination_id = profile.destinationId() };
    var balance: FakeBalance = .{};

    var before = try testRuntime(allocator, tmp.dir, profile, &remote, &balance);
    enableManualPolling(&before);
    _ = try before.submit(.withdrawal, 6_010, 1_001, 11, 25);
    const in_flight = before.claimNext().?;
    const adjustment = switch (in_flight.action) {
        .core_adjust => |value| value,
        else => return error.ExpectedCoreAdjustment,
    };
    try std.testing.expectEqual(saga.CoreOutcome.applied, balance.port().apply(adjustment));
    try before.deinit();

    var middle = try reopenTestRuntime(allocator, tmp.dir, profile, &remote, &balance);
    try middle.deinit();

    var after = try reopenTestRuntime(allocator, tmp.dir, profile, &remote, &balance);
    defer after.deinit() catch unreachable;
    enableManualPolling(&after);
    after.pollOnce();
    try std.testing.expectEqual(@as(usize, 1), balance.economic_effects);
    try std.testing.expectEqual(saga.PublicState.pending, (try after.status(6_010)).state);
    try std.testing.expectEqual(@as(i64, adjustment.transaction_id), balance.counters[0].last_transaction_id);
}
