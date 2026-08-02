const std = @import("std");
const exchange_core = @import("exchange_core");
const profile_module = @import("parix_clearing_profile.zig");
const saga = @import("parix_clearing_saga.zig");
const clearing_store = @import("parix_clearing_store.zig");
const funding_runtime = @import("parix_funding_runtime.zig");
const funding_saga = @import("parix_funding_saga.zig");

const core = exchange_core.core;
const dispatcher = core.tigerbeetle_dispatcher;
const outbox = core.tigerbeetle_outbox;
const plugin = core.tigerbeetle_plugin;
const settlement = core.tigerbeetle_settlement;

pub const DefaultStore = clearing_store.DefaultStore;

pub const OpenPolicy = enum {
    initialize,
    open_existing,
};

pub const Clock = union(enum) {
    system,
    fixed: i64,
    callback: Callback,

    pub const Callback = struct {
        context: *const anyopaque,
        now_unix_seconds: *const fn (*const anyopaque) i64,
    };

    pub fn nowUnixSeconds(self: Clock, io: std.Io) i64 {
        return switch (self) {
            .system => std.Io.Clock.real.now(io).toSeconds(),
            .fixed => |seconds| seconds,
            .callback => |callback| callback.now_unix_seconds(callback.context),
        };
    }
};

pub const Health = enum(u8) {
    cold,
    ready,
    pending,
    failed,
    quarantined,
    stopped,
};

pub const OperationsReport = struct {
    health: Health,
    durable: saga.OperationsSnapshot,
};

pub const FundingIdentity = struct {
    destination_id: saga.DestinationId,
    settlement_profile_digest: [profile_module.digest_len]u8,
};

pub const FundingPort = struct {
    context: *anyopaque,
    vtable: *const VTable,
    identity: FundingIdentity,

    pub const VTable = struct {
        submit: *const fn (*anyopaque, saga.FundingRequest) saga.FundingSubmitOutcome,
        status: *const fn (*anyopaque, saga.FundingStatusRequest) saga.FundingStatusOutcome,
        is_running: *const fn (*anyopaque) bool,
    };

    pub fn submit(self: FundingPort, request: saga.FundingRequest) saga.FundingSubmitOutcome {
        return self.vtable.submit(self.context, request);
    }

    pub fn status(self: FundingPort, request: saga.FundingStatusRequest) saga.FundingStatusOutcome {
        return self.vtable.status(self.context, request);
    }

    pub fn isRunning(self: FundingPort) bool {
        return self.vtable.is_running(self.context);
    }
};

pub const FundingRuntimeAdapter = struct {
    runtime: *funding_runtime.Runtime,

    pub fn asPort(self: *FundingRuntimeAdapter) FundingPort {
        const funding_identity = self.runtime.identity();
        return .{
            .context = self,
            .vtable = &vtable,
            .identity = .{
                .destination_id = funding_identity.destination_id,
                .settlement_profile_digest = funding_identity.profile_digest,
            },
        };
    }

    const vtable: FundingPort.VTable = .{
        .submit = submit,
        .status = status,
        .is_running = isRunning,
    };

    fn submit(raw: *anyopaque, request: saga.FundingRequest) saga.FundingSubmitOutcome {
        const self: *FundingRuntimeAdapter = @ptrCast(@alignCast(raw));
        const accepted = self.runtime.submit(
            request.kind,
            request.operation_id,
            request.uid,
            request.currency,
            request.amount,
        ) catch |err| return fundingSubmitError(err);
        return fundingSubmitStatus(request, accepted.status);
    }

    fn status(raw: *anyopaque, request: saga.FundingStatusRequest) saga.FundingStatusOutcome {
        const self: *FundingRuntimeAdapter = @ptrCast(@alignCast(raw));
        const current = self.runtime.status(request.request.operation_id) catch |err| return switch (err) {
            error.NotFound => .not_found,
            error.Poisoned => .quarantined,
            else => .retryable,
        };
        return fundingStatus(request.request, current);
    }

    fn isRunning(raw: *anyopaque) bool {
        const self: *FundingRuntimeAdapter = @ptrCast(@alignCast(raw));
        return self.runtime.isAccepting();
    }
};

pub const ConfigError = error{
    InvalidSnapshotPath,
    InvalidPollInterval,
    InvalidProfileBinding,
    FundingProfileMismatch,
};

pub const InitError = ConfigError || std.mem.Allocator.Error || clearing_store.StoreError;
pub const AdmissionError = profile_module.AdmissionError ||
    profile_module.ResolutionValidationError ||
    clearing_store.StoreError || error{
    RuntimeNotRunning,
    RuntimeFailed,
    RuntimeQuarantined,
};
pub const StatusError = clearing_store.StoreError;
pub const StartError = std.Thread.SpawnError || error{
    FundingNotRunning,
    AlreadyStarted,
    RuntimeFailed,
    RuntimeQuarantined,
    RuntimeStopped,
};
pub const StopError = error{FundingNotRunning};
pub const DeinitError = error{ThreadStillRunning};

pub const Config = struct {
    /// Every dependency is borrowed for the runtime's full lifetime.
    profile: *const profile_module.Profile,
    persistence_root: std.Io.Dir,
    transport: plugin.Transport,
    funding_port: FundingPort,
    snapshot_path: []const u8 = clearing_store.default_path,
    open_policy: OpenPolicy = .open_existing,
    poll_interval_ms: u32 = 10,
    clock: Clock = .system,

    pub fn validate(self: Config) ConfigError!void {
        try validateSnapshotPath(self.snapshot_path);
        if (self.poll_interval_ms == 0) return error.InvalidPollInterval;
        self.profile.binding().validate() catch return error.InvalidProfileBinding;
        const expected_digest = self.profile.settlementProfileDigest();
        if (self.funding_port.identity.destination_id != self.profile.destinationId() or
            !std.mem.eql(
                u8,
                &self.funding_port.identity.settlement_profile_digest,
                &expected_digest,
            ))
        {
            return error.FundingProfileMismatch;
        }
    }
};

/// App-owned clearing orchestrator. The mutex protects only CSG1. TigerBeetle
/// and FSG1 calls always execute after the durable claim is published and the
/// mutex is released, preventing a CSG1-to-FSG1 lock cycle.
pub const Runtime = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    io: std.Io,
    profile: *const profile_module.Profile,
    destination_id: saga.DestinationId,
    transport: plugin.Transport,
    funding_port: FundingPort,
    poll_interval_ms: u32,
    clock: Clock,
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
        const store = try allocator.create(DefaultStore);
        errdefer allocator.destroy(store);
        switch (config.open_policy) {
            .initialize => try DefaultStore.initialize(
                store,
                allocator,
                io,
                config.persistence_root,
                config.snapshot_path,
                config.profile.binding(),
            ),
            .open_existing => try DefaultStore.openExisting(
                store,
                allocator,
                io,
                config.persistence_root,
                config.snapshot_path,
                config.profile.binding(),
            ),
        }
        errdefer store.deinit();
        const initial_health = healthFromSummary(try store.summary());
        return .{
            .allocator = allocator,
            .io = io,
            .profile = config.profile,
            .destination_id = config.profile.destinationId(),
            .transport = config.transport,
            .funding_port = config.funding_port,
            .poll_interval_ms = config.poll_interval_ms,
            .clock = config.clock,
            .store = store,
            .cached_health = .init(@intFromEnum(initial_health)),
        };
    }

    pub fn start(self: *Self) StartError!void {
        if (!self.funding_port.isRunning()) return error.FundingNotRunning;
        if (self.worker != null) return error.AlreadyStarted;
        self.store_mutex.lockUncancelable(self.io);
        if (self.store.isPoisoned()) {
            self.publishFailureLocked();
            self.store_mutex.unlock(self.io);
            return error.RuntimeFailed;
        }
        const durable_health = healthFromSummary(self.store.summary() catch {
            self.publishFailureLocked();
            self.store_mutex.unlock(self.io);
            return error.RuntimeFailed;
        });
        self.storeHealth(durable_health);
        switch (self.health()) {
            .failed => {
                self.store_mutex.unlock(self.io);
                return error.RuntimeFailed;
            },
            .quarantined => {
                self.store_mutex.unlock(self.io);
                return error.RuntimeQuarantined;
            },
            .stopped => {
                self.store_mutex.unlock(self.io);
                return error.RuntimeStopped;
            },
            .cold, .ready, .pending => {},
        }

        self.stop_requested.store(false, .release);
        self.wakeup.reset();
        self.store_mutex.unlock(self.io);
        self.worker = std.Thread.spawn(.{}, workerMain, .{self}) catch |err| {
            self.publishFailure();
            return err;
        };
        // Publish admission only after the worker exists. A worker-side
        // terminal failure takes the same mutex and wins this race.
        self.store_mutex.lockUncancelable(self.io);
        if (isTerminalRuntimeHealth(self.health())) {
            const terminal_health = self.health();
            self.store_mutex.unlock(self.io);
            self.stop_requested.store(true, .release);
            self.wakeup.set(self.io);
            self.worker.?.join();
            self.worker = null;
            return switch (terminal_health) {
                .failed => error.RuntimeFailed,
                .quarantined => error.RuntimeQuarantined,
                .stopped => error.RuntimeStopped,
                .cold, .ready, .pending => unreachable,
            };
        }
        self.accepting.store(true, .release);
        self.store_mutex.unlock(self.io);
        self.wakeup.set(self.io);
    }

    /// Clearing must stop while FSG1 remains live. The worker is joined before
    /// this method verifies the dependency, enforcing the host shutdown order.
    pub fn stopAndJoin(self: *Self) StopError!void {
        self.accepting.store(false, .release);
        self.store_mutex.lockUncancelable(self.io);
        self.accepting.store(false, .release);
        self.stop_requested.store(true, .release);
        self.store_mutex.unlock(self.io);
        self.wakeup.set(self.io);
        if (self.worker) |worker| {
            worker.join();
            self.worker = null;
        }
        if (!self.funding_port.isRunning()) {
            self.publishFailure();
            return error.FundingNotRunning;
        }
        self.storeHealth(.stopped);
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

    /// Returns only after the normalized provider authorization is durable.
    pub fn acceptAuthorization(
        self: *Self,
        authorization: saga.Authorization,
    ) AdmissionError!saga.AcceptResult {
        try self.ensureAccepting();
        self.store_mutex.lockUncancelable(self.io);
        self.ensureAccepting() catch |err| {
            self.store_mutex.unlock(self.io);
            return err;
        };
        if (self.store.preflightAuthorizationEvent(authorization) catch |err| {
            self.observeStoreFailureLocked();
            self.store_mutex.unlock(self.io);
            return err;
        }) |existing| {
            self.store_mutex.unlock(self.io);
            return existing;
        }
        const route = self.profile.authorizationRoute(authorization) catch |err| {
            self.store_mutex.unlock(self.io);
            return err;
        };
        const accepted = self.store.acceptAuthorizationAt(
            authorization,
            route,
            self.clock.nowUnixSeconds(self.io),
        ) catch |err| {
            self.observeStoreFailureLocked();
            self.store_mutex.unlock(self.io);
            return err;
        };
        self.storeHealth(.pending);
        self.store_mutex.unlock(self.io);
        self.wakeup.set(self.io);
        return accepted;
    }

    /// Loads the durable authorization, validates the complete pair against
    /// the trusted profile, and publishes the resolution under one CSG1 lock.
    pub fn acceptResolution(
        self: *Self,
        resolution: saga.Resolution,
    ) AdmissionError!saga.AcceptResult {
        try self.ensureAccepting();
        self.store_mutex.lockUncancelable(self.io);
        self.ensureAccepting() catch |err| {
            self.store_mutex.unlock(self.io);
            return err;
        };
        if (self.store.preflightResolutionEvent(resolution) catch |err| {
            self.observeStoreFailureLocked();
            self.store_mutex.unlock(self.io);
            return err;
        }) |existing| {
            self.store_mutex.unlock(self.io);
            return existing;
        }
        const existing = self.store.lookup(resolution.payment_id) catch |err| {
            self.observeStoreFailureLocked();
            self.store_mutex.unlock(self.io);
            return err;
        };
        self.profile.validateResolution(existing.authorization, resolution) catch |err| {
            self.store_mutex.unlock(self.io);
            return err;
        };
        const accepted = self.store.acceptResolution(resolution) catch |err| {
            self.observeStoreFailureLocked();
            self.store_mutex.unlock(self.io);
            return err;
        };
        self.storeHealth(.pending);
        self.store_mutex.unlock(self.io);
        self.wakeup.set(self.io);
        return accepted;
    }

    pub fn status(self: *Self, payment_id: u128) StatusError!saga.Status {
        self.store_mutex.lockUncancelable(self.io);
        defer self.store_mutex.unlock(self.io);
        return self.store.lookup(payment_id) catch |err| {
            self.observeStoreFailureLocked();
            return err;
        };
    }

    pub fn summary(self: *Self) StatusError!saga.Summary {
        self.store_mutex.lockUncancelable(self.io);
        defer self.store_mutex.unlock(self.io);
        return self.store.summary() catch |err| {
            self.observeStoreFailureLocked();
            return err;
        };
    }

    pub fn operationsReport(self: *Self) StatusError!OperationsReport {
        self.store_mutex.lockUncancelable(self.io);
        defer self.store_mutex.unlock(self.io);
        const durable = self.store.operationsSnapshot(
            self.clock.nowUnixSeconds(self.io),
        ) catch |err| {
            self.observeStoreFailureLocked();
            return err;
        };
        return .{ .health = self.health(), .durable = durable };
    }

    /// Executes at most one durable claim. This is also the deterministic
    /// embedded-host/test entry point.
    pub fn pollOnce(self: *Self) void {
        if (self.stop_requested.load(.acquire)) return;
        if (isTerminalRuntimeHealth(self.health())) return;
        if (!self.funding_port.isRunning()) {
            self.publishFailure();
            return;
        }

        if (self.expireNext()) return;

        const claim = self.claimNext() orelse {
            self.refreshHealth();
            return;
        };
        self.storeHealth(.pending);
        const completion: saga.Completion = switch (claim.action) {
            .remote_submit => |submit| .{
                .remote_submit = self.executeRemoteSubmit(submit),
            },
            .remote_lookup => |lookup| .{
                .remote_lookup = self.executeRemoteLookup(lookup),
            },
            .funding_submit => |request| .{
                .funding_submit = self.funding_port.submit(request),
            },
            .funding_status => |request| .{
                .funding_status = self.funding_port.status(request),
            },
        };

        self.store_mutex.lockUncancelable(self.io);
        const completed = self.store.complete(claim, completion) catch {
            self.publishFailureLocked();
            self.store_mutex.unlock(self.io);
            return;
        };
        const next_health = healthFromSummary(self.store.summary() catch {
            self.publishFailureLocked();
            self.store_mutex.unlock(self.io);
            return;
        });
        if (completed.state == .quarantined or next_health == .quarantined) {
            self.accepting.store(false, .release);
            self.storeHealth(.quarantined);
        } else {
            self.storeHealth(next_health);
        }
        // Publish health and the terminal admission fence while the same lock
        // still excludes admission. This makes the durable state and its
        // externally visible lifecycle one atomic observation.
        self.store_mutex.unlock(self.io);
    }

    fn expireNext(self: *Self) bool {
        const now_unix_seconds = self.clock.nowUnixSeconds(self.io);
        self.store_mutex.lockUncancelable(self.io);
        defer self.store_mutex.unlock(self.io);
        if (self.stop_requested.load(.acquire)) return false;
        _ = self.store.expireNext(now_unix_seconds) catch {
            self.publishFailureLocked();
            return true;
        } orelse return false;
        const next_health = healthFromSummary(self.store.summary() catch {
            self.publishFailureLocked();
            return true;
        });
        self.storeHealth(next_health);
        return true;
    }

    fn workerMain(self: *Self) void {
        while (!self.stop_requested.load(.acquire)) {
            // Event is manual-reset. Reset before polling so durable work set
            // before the reset is observed by the poll, while a concurrent
            // admission set after the reset remains visible to the wait.
            self.wakeup.reset();
            if (self.stop_requested.load(.acquire)) return;
            self.pollOnce();
            if (isTerminalRuntimeHealth(self.health())) return;
            if (self.stop_requested.load(.acquire)) return;
            self.wakeup.waitTimeout(self.io, .{ .duration = .{
                .raw = .fromMilliseconds(@intCast(self.poll_interval_ms)),
                .clock = .awake,
            } }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => {
                    self.publishFailure();
                    return;
                },
            };
        }
    }

    fn claimNext(self: *Self) ?saga.Claim {
        self.store_mutex.lockUncancelable(self.io);
        defer self.store_mutex.unlock(self.io);
        if (self.stop_requested.load(.acquire)) return null;
        return self.store.claimNext() catch {
            self.publishFailureLocked();
            return null;
        };
    }

    fn executeRemoteSubmit(
        self: *Self,
        submit: saga.RemoteSubmit,
    ) saga.RemoteSubmitOutcome {
        var chain = oneTransferChain(submit.transfer);
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
            else switch (classifyClearingLookup(chain, receipt.batch)) {
                .all_present_exact => .exact,
                .all_absent => .absent,
                .partial_application, .transfer_mismatch => .transfer_mismatch,
            },
            .retryable => .retryable,
            .permanent => .permanent_failure,
            .destination_mismatch => .destination_mismatch,
        };
    }

    fn ensureAccepting(self: *const Self) AdmissionError!void {
        switch (self.health()) {
            .failed => return error.RuntimeFailed,
            .quarantined => return error.RuntimeQuarantined,
            .cold, .stopped => return error.RuntimeNotRunning,
            .ready, .pending => {},
        }
        if (!self.accepting.load(.acquire)) return error.RuntimeNotRunning;
    }

    fn refreshHealth(self: *Self) void {
        self.store_mutex.lockUncancelable(self.io);
        const next = healthFromSummary(self.store.summary() catch {
            self.publishFailureLocked();
            self.store_mutex.unlock(self.io);
            return;
        });
        self.storeHealth(next);
        self.store_mutex.unlock(self.io);
    }

    fn observeStoreFailureLocked(self: *Self) void {
        if (self.store.isPoisoned()) {
            self.publishFailureLocked();
        }
    }

    fn publishFailure(self: *Self) void {
        self.store_mutex.lockUncancelable(self.io);
        self.publishFailureLocked();
        self.store_mutex.unlock(self.io);
    }

    fn publishFailureLocked(self: *Self) void {
        self.accepting.store(false, .release);
        self.storeHealth(.failed);
    }

    fn storeHealth(self: *Self, value: Health) void {
        const desired = @intFromEnum(value);
        var current = self.cached_health.load(.acquire);
        while (true) {
            if (isTerminalRuntimeHealth(@enumFromInt(current))) return;
            if (self.cached_health.cmpxchgWeak(
                current,
                desired,
                .acq_rel,
                .acquire,
            )) |observed| {
                current = observed;
            } else return;
        }
    }
};

fn fundingSubmitStatus(
    expected: saga.FundingRequest,
    status: funding_saga.Status,
) saga.FundingSubmitOutcome {
    if (!sameFundingRequest(expected, status.request)) return .idempotency_conflict;
    return switch (status.state) {
        .accepted, .pending => .pending,
        .completed => .completed,
        .failed => .rejected,
        .quarantined => .quarantined,
    };
}

fn fundingSubmitError(err: funding_runtime.SubmitError) saga.FundingSubmitOutcome {
    return switch (err) {
        error.StorageAmbiguous => .ambiguous,
        error.IdempotencyConflict => .idempotency_conflict,
        // These failures happen before FSG1 can durably accept the child.
        error.RuntimeNotRunning, error.OutOfMemory => .known_not_sent,
        // Only an FSG1 status of `.failed` is an authoritative rejection.
        // Configuration, capacity, corruption, and dependency failures need
        // operator action and must never fail the payment silently.
        else => .quarantined,
    };
}

fn fundingStatus(
    expected: saga.FundingRequest,
    status: funding_saga.Status,
) saga.FundingStatusOutcome {
    if (!sameFundingRequest(expected, status.request)) return .idempotency_conflict;
    return switch (status.state) {
        .accepted, .pending => .pending,
        .completed => .completed,
        .failed => .rejected,
        .quarantined => .quarantined,
    };
}

fn sameFundingRequest(expected: saga.FundingRequest, actual: funding_saga.Request) bool {
    return expected.operation_id == actual.operation_id and
        expected.kind == actual.kind and
        expected.uid == actual.uid and
        expected.currency == actual.currency and
        expected.amount == actual.amount;
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

/// Clearing actions are always single-transfer lifecycle operations. Unlike
/// trade chains, the linked bit is immutable here: accepting a transfer that
/// was committed as part of another atomic chain would not be byte-equivalent
/// reconciliation evidence.
fn classifyClearingLookup(
    expected: settlement.TransferChain,
    observed: dispatcher.LookupBatch,
) dispatcher.ReconciliationEvidence {
    const evidence = dispatcher.classifyLookup(expected, observed);
    if (evidence != .all_present_exact) return evidence;
    const actual = observed.items() catch return .transfer_mismatch;
    if (expected.count != 1 or actual.len != 1) return .transfer_mismatch;
    if (actual[0].flags != expected.storage[0].flags) return .transfer_mismatch;
    return .all_present_exact;
}

fn validateSnapshotPath(path: []const u8) ConfigError!void {
    if (path.len == 0 or std.mem.eql(u8, path, ".") or std.mem.eql(u8, path, "..")) {
        return error.InvalidSnapshotPath;
    }
    for (path) |byte| {
        if (!std.ascii.isAlphanumeric(byte) and byte != '-' and byte != '_' and byte != '.') {
            return error.InvalidSnapshotPath;
        }
    }
}

fn healthFromSummary(summary: saga.Summary) Health {
    if (summary.quarantined != 0) return .quarantined;
    if (summary.accepted != 0 or summary.pending != 0 or summary.authorized != 0) {
        return .pending;
    }
    return .ready;
}

fn isTerminalRuntimeHealth(health: Health) bool {
    return switch (health) {
        .failed, .quarantined, .stopped => true,
        .cold, .ready, .pending => false,
    };
}

const FakeRemote = struct {
    destination_id: saga.DestinationId,
    transfers: [8]settlement.Transfer = undefined,
    transfer_count: usize = 0,
    submits: usize = 0,
    lookups: usize = 0,
    economic_effects: usize = 0,
    ambiguous_next: bool = false,
    fail_completion_store: ?*DefaultStore = null,

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
            return .destination_mismatch;
        }
        const transfer = chain.items()[0];
        for (self.transfers[0..self.transfer_count]) |existing| {
            if (existing.id != transfer.id) continue;
            if (!std.meta.eql(existing, transfer)) return .permanent;
            return .{ .success = .{
                .destination_id = self.destination_id,
                .operation = .create_transfers,
                .persisted = true,
            } };
        }
        if (self.transfer_count == self.transfers.len) return .permanent;
        self.transfers[self.transfer_count] = transfer;
        self.transfer_count += 1;
        self.economic_effects += 1;
        if (self.fail_completion_store) |store| {
            self.fail_completion_store = null;
            store.injectPreWriteFailureForTesting();
        }
        if (self.ambiguous_next) {
            self.ambiguous_next = false;
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
        _: outbox.ReconciliationCause,
    ) dispatcher.LookupDisposition {
        const self: *FakeRemote = @ptrCast(@alignCast(context));
        self.lookups += 1;
        if (destination_id != self.destination_id or chain.items().len != 1) {
            return .destination_mismatch;
        }
        const expected = chain.items()[0];
        for (self.transfers[0..self.transfer_count]) |existing| {
            if (existing.id != expected.id) continue;
            return .{ .transfers = .{
                .destination_id = self.destination_id,
                .operation = .lookup_transfers,
                .persisted = false,
                .batch = dispatcher.LookupBatch.init(&.{existing}) catch unreachable,
            } };
        }
        return .{ .transfers = .{
            .destination_id = self.destination_id,
            .operation = .lookup_transfers,
            .persisted = false,
            .batch = dispatcher.LookupBatch.init(&.{}) catch unreachable,
        } };
    }

    const vtable: plugin.Transport.VTable = .{
        .submit = submitCallback,
        .lookup = lookupCallback,
    };
};

const FakeFunding = struct {
    const Mode = enum {
        completed,
        ambiguous_absent_once,
        rejected,
        quarantined,
    };

    running: std.atomic.Value(bool) = .init(true),
    is_running_calls: std.atomic.Value(usize) = .init(0),
    mode: Mode = .completed,
    ambiguous_returned: bool = false,
    absent_returned: bool = false,
    submits: usize = 0,
    statuses: usize = 0,
    last_submit: ?saga.FundingRequest = null,
    last_status: ?saga.FundingStatusRequest = null,

    fn port(
        self: *FakeFunding,
        profile: *const profile_module.Profile,
    ) FundingPort {
        return .{
            .context = self,
            .vtable = &vtable,
            .identity = .{
                .destination_id = profile.destinationId(),
                .settlement_profile_digest = profile.settlementProfileDigest(),
            },
        };
    }

    fn submitCallback(
        context: *anyopaque,
        request: saga.FundingRequest,
    ) saga.FundingSubmitOutcome {
        const self: *FakeFunding = @ptrCast(@alignCast(context));
        self.submits += 1;
        if (self.last_submit) |previous| {
            if (previous.operation_id == request.operation_id and
                !std.meta.eql(previous, request))
            {
                return .idempotency_conflict;
            }
        }
        self.last_submit = request;
        return switch (self.mode) {
            .completed => .completed,
            .ambiguous_absent_once => if (!self.ambiguous_returned) outcome: {
                self.ambiguous_returned = true;
                break :outcome .ambiguous;
            } else .completed,
            .rejected => .rejected,
            .quarantined => .quarantined,
        };
    }

    fn statusCallback(
        context: *anyopaque,
        request: saga.FundingStatusRequest,
    ) saga.FundingStatusOutcome {
        const self: *FakeFunding = @ptrCast(@alignCast(context));
        self.statuses += 1;
        self.last_status = request;
        if (self.last_submit) |submitted| {
            if (!std.meta.eql(submitted, request.request)) return .idempotency_conflict;
        }
        return switch (self.mode) {
            .ambiguous_absent_once => if (!self.absent_returned) outcome: {
                self.absent_returned = true;
                break :outcome .not_found;
            } else .completed,
            .completed => .completed,
            .rejected => .rejected,
            .quarantined => .quarantined,
        };
    }

    fn isRunningCallback(context: *anyopaque) bool {
        const self: *FakeFunding = @ptrCast(@alignCast(context));
        _ = self.is_running_calls.fetchAdd(1, .monotonic);
        return self.running.load(.acquire);
    }

    const vtable: FundingPort.VTable = .{
        .submit = submitCallback,
        .status = statusCallback,
        .is_running = isRunningCallback,
    };
};

const TestProfiles = struct {
    settlement_profile: *profile_module.SettlementProfile,
    clearing_profile: *profile_module.Profile,

    fn init(allocator: std.mem.Allocator) !TestProfiles {
        const settlement_profile = try profile_module.SettlementProfile.developmentExample(allocator);
        errdefer settlement_profile.deinit();
        return .{
            .settlement_profile = settlement_profile,
            .clearing_profile = try profile_module.Profile.developmentExample(
                allocator,
                settlement_profile,
            ),
        };
    }

    fn deinit(self: TestProfiles) void {
        self.clearing_profile.deinit();
        self.settlement_profile.deinit();
    }
};

fn testAuthorization(payment_id: u128, direction: saga.Direction) saga.Authorization {
    return .{
        .event_id = payment_id * 10 + 1,
        .payment_id = payment_id,
        .provider_id = 7_001,
        .revision = 1,
        .direction = direction,
        .uid = 1_001,
        .currency = 11,
        .amount = 100,
        .timeout_seconds = 60,
    };
}

fn testResolution(
    authorization: saga.Authorization,
    kind: saga.ResolutionKind,
    amount: i64,
) saga.Resolution {
    return .{
        .event_id = authorization.payment_id * 10 + 2,
        .payment_id = authorization.payment_id,
        .provider_id = authorization.provider_id,
        .revision = authorization.revision + 1,
        .kind = kind,
        .direction = authorization.direction,
        .uid = authorization.uid,
        .currency = authorization.currency,
        .amount = amount,
    };
}

const TestClock = struct {
    now_unix_seconds: i64,

    fn clock(self: *const TestClock) Clock {
        return .{ .callback = .{
            .context = self,
            .now_unix_seconds = now,
        } };
    }

    fn now(raw: *const anyopaque) i64 {
        const self: *const TestClock = @ptrCast(@alignCast(raw));
        return self.now_unix_seconds;
    }
};

fn testRuntime(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    profile: *const profile_module.Profile,
    remote: *FakeRemote,
    funding: *FakeFunding,
    policy: OpenPolicy,
) !Runtime {
    return testRuntimeWithClock(
        allocator,
        dir,
        profile,
        remote,
        funding,
        policy,
        .{ .fixed = 1_700_000_000 },
    );
}

fn testRuntimeWithClock(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    profile: *const profile_module.Profile,
    remote: *FakeRemote,
    funding: *FakeFunding,
    policy: OpenPolicy,
    clock: Clock,
) !Runtime {
    return Runtime.init(allocator, std.testing.io, .{
        .profile = profile,
        .persistence_root = dir,
        .transport = remote.transport(),
        .funding_port = funding.port(profile),
        .open_policy = policy,
        .poll_interval_ms = 1,
        .clock = clock,
    });
}

fn enableManualPolling(runtime: *Runtime) void {
    runtime.stop_requested.store(false, .release);
    runtime.accepting.store(true, .release);
    runtime.storeHealth(healthFromSummary(runtime.store.summary() catch unreachable));
}

fn driveToState(
    runtime: *Runtime,
    payment_id: u128,
    expected: saga.PublicState,
    maximum_polls: usize,
) !void {
    for (0..maximum_polls) |_| {
        if ((try runtime.status(payment_id)).state == expected) return;
        runtime.pollOnce();
    }
    try std.testing.expectEqual(expected, (try runtime.status(payment_id)).state);
}

test "clearing lookup requires exact lifecycle and linked flags" {
    const expected_transfer: settlement.Transfer = .{
        .id = 101,
        .debit_account_id = 201,
        .credit_account_id = 202,
        .amount = 25,
        .user_data_128 = 301,
        .timeout = 60,
        .ledger = 421,
        .code = 20,
        .flags = settlement.pending_flag,
    };
    const expected = oneTransferChain(expected_transfer);
    const exact = try dispatcher.LookupBatch.init(&.{expected_transfer});
    try std.testing.expectEqual(
        dispatcher.ReconciliationEvidence.all_present_exact,
        classifyClearingLookup(expected, exact),
    );

    var linked = expected_transfer;
    linked.flags |= settlement.linked_flag;
    const wrong_chain = try dispatcher.LookupBatch.init(&.{linked});
    try std.testing.expectEqual(
        dispatcher.ReconciliationEvidence.transfer_mismatch,
        classifyClearingLookup(expected, wrong_chain),
    );
}

test "funding status comparison binds the complete immutable request" {
    const expected: saga.FundingRequest = .{
        .operation_id = 401,
        .kind = .deposit,
        .uid = 1_001,
        .currency = 11,
        .amount = 50,
        .purpose = .deposit_credit,
    };
    const exact: funding_saga.Status = .{
        .request = .{
            .operation_id = expected.operation_id,
            .kind = expected.kind,
            .uid = expected.uid,
            .currency = expected.currency,
            .amount = expected.amount,
        },
        .state = .completed,
    };
    try std.testing.expectEqual(
        saga.FundingSubmitOutcome.completed,
        fundingSubmitStatus(expected, exact),
    );
    try std.testing.expectEqual(
        saga.FundingStatusOutcome.completed,
        fundingStatus(expected, exact),
    );

    var mismatch = exact;
    mismatch.request.amount += 1;
    try std.testing.expectEqual(
        saga.FundingSubmitOutcome.idempotency_conflict,
        fundingSubmitStatus(expected, mismatch),
    );
    try std.testing.expectEqual(
        saga.FundingStatusOutcome.idempotency_conflict,
        fundingStatus(expected, mismatch),
    );
}

test "FSG1 method errors preserve side-effect certainty" {
    try std.testing.expectEqual(
        saga.FundingSubmitOutcome.ambiguous,
        fundingSubmitError(error.StorageAmbiguous),
    );
    try std.testing.expectEqual(
        saga.FundingSubmitOutcome.idempotency_conflict,
        fundingSubmitError(error.IdempotencyConflict),
    );
    try std.testing.expectEqual(
        saga.FundingSubmitOutcome.known_not_sent,
        fundingSubmitError(error.RuntimeNotRunning),
    );
    try std.testing.expectEqual(
        saga.FundingSubmitOutcome.known_not_sent,
        fundingSubmitError(error.OutOfMemory),
    );
    try std.testing.expectEqual(
        saga.FundingSubmitOutcome.quarantined,
        fundingSubmitError(error.CapacityExceeded),
    );
    try std.testing.expectEqual(
        saga.FundingSubmitOutcome.quarantined,
        fundingSubmitError(error.UnknownFundingCurrency),
    );
}

test "runtime expires deposits and funded withdrawals at the durable deadline" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profiles = try TestProfiles.init(allocator);
    defer profiles.deinit();
    var remote: FakeRemote = .{ .destination_id = profiles.clearing_profile.destinationId() };
    var funding: FakeFunding = .{};
    var clock: TestClock = .{ .now_unix_seconds = 1_700_000_000 };
    var runtime = try testRuntimeWithClock(
        allocator,
        tmp.dir,
        profiles.clearing_profile,
        &remote,
        &funding,
        .initialize,
        clock.clock(),
    );
    defer runtime.deinit() catch unreachable;
    enableManualPolling(&runtime);

    const deposit = testAuthorization(490, .deposit);
    const deposit_deadline = clock.now_unix_seconds + @as(i64, deposit.timeout_seconds);
    const admitted = try runtime.acceptAuthorization(deposit);
    try std.testing.expectEqual(deposit_deadline, admitted.status.expires_at_unix_seconds);
    try driveToState(&runtime, deposit.payment_id, .authorized, 2);
    try std.testing.expectEqual(@as(usize, 1), remote.economic_effects);

    clock.now_unix_seconds = deposit_deadline - 1;
    runtime.pollOnce();
    try std.testing.expectEqual(saga.PublicState.authorized, (try runtime.status(deposit.payment_id)).state);
    clock.now_unix_seconds = deposit_deadline;
    runtime.pollOnce();
    try std.testing.expectEqual(@as(usize, 1), remote.economic_effects);
    const expiring = try runtime.status(deposit.payment_id);
    try std.testing.expectEqual(saga.ResolutionSource.expiration, expiring.resolution_source.?);
    try driveToState(&runtime, deposit.payment_id, .completed, 2);
    try std.testing.expectEqual(@as(usize, 2), remote.economic_effects);
    try std.testing.expectEqual(@as(usize, 0), funding.submits);

    clock.now_unix_seconds += 1;
    const withdrawal = testAuthorization(491, .withdrawal);
    const withdrawal_deadline = clock.now_unix_seconds + @as(i64, withdrawal.timeout_seconds);
    _ = try runtime.acceptAuthorization(withdrawal);
    try driveToState(&runtime, withdrawal.payment_id, .authorized, 3);
    clock.now_unix_seconds = withdrawal_deadline;
    runtime.pollOnce();
    try driveToState(&runtime, withdrawal.payment_id, .completed, 3);
    try std.testing.expectEqual(@as(usize, 4), remote.economic_effects);
    try std.testing.expectEqual(@as(usize, 2), funding.submits);
    try std.testing.expectEqual(
        saga.FundingPurpose.withdrawal_refund,
        funding.last_submit.?.purpose,
    );
}

test "operations report uses the runtime clock without mutating CSG1" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profiles = try TestProfiles.init(allocator);
    defer profiles.deinit();
    var remote: FakeRemote = .{ .destination_id = profiles.clearing_profile.destinationId() };
    var funding: FakeFunding = .{};
    var clock: TestClock = .{ .now_unix_seconds = 1_700_000_000 };
    var runtime = try testRuntimeWithClock(
        allocator,
        tmp.dir,
        profiles.clearing_profile,
        &remote,
        &funding,
        .initialize,
        clock.clock(),
    );
    defer runtime.deinit() catch unreachable;
    enableManualPolling(&runtime);

    const authorization = testAuthorization(492, .deposit);
    const deadline = clock.now_unix_seconds + @as(i64, authorization.timeout_seconds);
    _ = try runtime.acceptAuthorization(authorization);
    const initial = try runtime.operationsReport();
    try std.testing.expectEqual(Health.pending, initial.health);
    try std.testing.expectEqual(@as(usize, 1), initial.durable.total);
    try std.testing.expectEqual(@as(usize, 1), initial.durable.ready_actions);
    try std.testing.expectEqual(@as(usize, 0), initial.durable.expired_unresolved);

    clock.now_unix_seconds = deadline + 5;
    const overdue = try runtime.operationsReport();
    try std.testing.expectEqual(@as(usize, 1), overdue.durable.expired_unresolved);
    try std.testing.expectEqual(@as(u64, 5), overdue.durable.max_deadline_lag_seconds);
    try std.testing.expectEqual(
        authorization.payment_id,
        overdue.durable.oldest_expired_payment_id.?,
    );
    try std.testing.expectEqual(@as(usize, 0), remote.economic_effects);

    runtime.pollOnce();
    const expired = try runtime.operationsReport();
    try std.testing.expectEqual(Health.ready, expired.health);
    try std.testing.expectEqual(@as(usize, 0), expired.durable.expired_unresolved);
    try std.testing.expectEqual(@as(usize, 1), expired.durable.expiration_resolutions);
    try std.testing.expectEqual(@as(usize, 1), expired.durable.states.completed);
    try std.testing.expectEqual(@as(usize, 0), remote.economic_effects);
}

test "expiry persistence failure fences before an automatic void" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profiles = try TestProfiles.init(allocator);
    defer profiles.deinit();
    var remote: FakeRemote = .{ .destination_id = profiles.clearing_profile.destinationId() };
    var funding: FakeFunding = .{};
    var clock: TestClock = .{ .now_unix_seconds = 1_700_000_000 };
    var runtime = try testRuntimeWithClock(
        allocator,
        tmp.dir,
        profiles.clearing_profile,
        &remote,
        &funding,
        .initialize,
        clock.clock(),
    );
    defer runtime.deinit() catch unreachable;
    enableManualPolling(&runtime);

    const authorization = testAuthorization(492, .deposit);
    _ = try runtime.acceptAuthorization(authorization);
    try driveToState(&runtime, authorization.payment_id, .authorized, 2);
    const effects_before = remote.economic_effects;
    runtime.store.injectPreWriteFailureForTesting();
    clock.now_unix_seconds += @as(i64, authorization.timeout_seconds);
    runtime.pollOnce();
    try std.testing.expectEqual(Health.failed, runtime.health());
    try std.testing.expectEqual(effects_before, remote.economic_effects);
}

test "manual deposit clears only after provider post and exact FSG1 credit" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profiles = try TestProfiles.init(allocator);
    defer profiles.deinit();
    var remote: FakeRemote = .{ .destination_id = profiles.clearing_profile.destinationId() };
    var funding: FakeFunding = .{};
    var runtime = try testRuntime(
        allocator,
        tmp.dir,
        profiles.clearing_profile,
        &remote,
        &funding,
        .initialize,
    );
    defer runtime.deinit() catch unreachable;
    enableManualPolling(&runtime);

    const authorization = testAuthorization(501, .deposit);
    _ = try runtime.acceptAuthorization(authorization);
    try driveToState(&runtime, authorization.payment_id, .authorized, 4);
    try std.testing.expectEqual(@as(usize, 0), funding.submits);

    _ = try runtime.acceptResolution(testResolution(authorization, .post, 100));
    try driveToState(&runtime, authorization.payment_id, .completed, 6);
    try std.testing.expectEqual(@as(usize, 2), remote.economic_effects);
    try std.testing.expectEqual(@as(usize, 1), funding.submits);
    try std.testing.expectEqual(saga.FundingPurpose.deposit_credit, funding.last_submit.?.purpose);
    try std.testing.expectEqual(@as(i64, 100), funding.last_submit.?.amount);
}

test "partial withdrawal debits before pending and refunds only released remainder" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profiles = try TestProfiles.init(allocator);
    defer profiles.deinit();
    var remote: FakeRemote = .{ .destination_id = profiles.clearing_profile.destinationId() };
    var funding: FakeFunding = .{};
    var runtime = try testRuntime(
        allocator,
        tmp.dir,
        profiles.clearing_profile,
        &remote,
        &funding,
        .initialize,
    );
    defer runtime.deinit() catch unreachable;
    enableManualPolling(&runtime);

    const authorization = testAuthorization(502, .withdrawal);
    _ = try runtime.acceptAuthorization(authorization);
    runtime.pollOnce();
    try std.testing.expectEqual(@as(usize, 1), funding.submits);
    try std.testing.expectEqual(@as(usize, 0), remote.submits);
    try driveToState(&runtime, authorization.payment_id, .authorized, 4);

    _ = try runtime.acceptResolution(testResolution(authorization, .post, 60));
    try driveToState(&runtime, authorization.payment_id, .completed, 6);
    try std.testing.expectEqual(@as(usize, 2), funding.submits);
    try std.testing.expectEqual(saga.FundingPurpose.withdrawal_refund, funding.last_submit.?.purpose);
    try std.testing.expectEqual(@as(i64, 40), funding.last_submit.?.amount);
    try std.testing.expectEqual(@as(usize, 2), remote.economic_effects);
}

test "ambiguous FSG1 absence retries the exact full child request" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profiles = try TestProfiles.init(allocator);
    defer profiles.deinit();
    var remote: FakeRemote = .{ .destination_id = profiles.clearing_profile.destinationId() };
    var funding: FakeFunding = .{ .mode = .ambiguous_absent_once };
    var runtime = try testRuntime(
        allocator,
        tmp.dir,
        profiles.clearing_profile,
        &remote,
        &funding,
        .initialize,
    );
    defer runtime.deinit() catch unreachable;
    enableManualPolling(&runtime);

    const authorization = testAuthorization(503, .withdrawal);
    _ = try runtime.acceptAuthorization(authorization);
    try driveToState(&runtime, authorization.payment_id, .authorized, 8);
    try std.testing.expectEqual(@as(usize, 2), funding.submits);
    try std.testing.expectEqual(@as(usize, 1), funding.statuses);
    try std.testing.expect(std.meta.eql(funding.last_submit.?, funding.last_status.?.request));
}

test "restart reconciles an exposed remote claim before any resubmit" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profiles = try TestProfiles.init(allocator);
    defer profiles.deinit();
    var remote: FakeRemote = .{
        .destination_id = profiles.clearing_profile.destinationId(),
        .ambiguous_next = true,
    };
    var funding: FakeFunding = .{};

    var before = try testRuntime(
        allocator,
        tmp.dir,
        profiles.clearing_profile,
        &remote,
        &funding,
        .initialize,
    );
    enableManualPolling(&before);
    const authorization = testAuthorization(504, .deposit);
    _ = try before.acceptAuthorization(authorization);
    const exposed = before.claimNext().?;
    const submit = switch (exposed.action) {
        .remote_submit => |value| value,
        else => return error.ExpectedRemoteSubmit,
    };
    try std.testing.expectEqual(
        saga.RemoteSubmitOutcome.ambiguous,
        before.executeRemoteSubmit(submit),
    );
    try before.deinit();

    var after = try testRuntime(
        allocator,
        tmp.dir,
        profiles.clearing_profile,
        &remote,
        &funding,
        .open_existing,
    );
    defer after.deinit() catch unreachable;
    enableManualPolling(&after);
    after.pollOnce();
    try std.testing.expectEqual(@as(usize, 1), remote.submits);
    try std.testing.expectEqual(@as(usize, 1), remote.lookups);
    try std.testing.expectEqual(saga.PublicState.authorized, (try after.status(504)).state);
}

test "ambiguous CSG1 publication fences admission before external effects" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profiles = try TestProfiles.init(allocator);
    defer profiles.deinit();
    var remote: FakeRemote = .{ .destination_id = profiles.clearing_profile.destinationId() };
    var funding: FakeFunding = .{};
    var runtime = try testRuntime(
        allocator,
        tmp.dir,
        profiles.clearing_profile,
        &remote,
        &funding,
        .initialize,
    );
    defer runtime.deinit() catch unreachable;
    enableManualPolling(&runtime);

    runtime.store.injectPostReplaceFailureForTesting();
    try std.testing.expectError(
        error.StorageAmbiguous,
        runtime.acceptAuthorization(testAuthorization(505, .deposit)),
    );
    try std.testing.expectEqual(Health.failed, runtime.health());
    runtime.pollOnce();
    try std.testing.expectEqual(@as(usize, 0), remote.submits);
    try std.testing.expectEqual(@as(usize, 0), funding.submits);
}

test "worker lifecycle requires FSG1 admission through clearing shutdown" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profiles = try TestProfiles.init(allocator);
    defer profiles.deinit();
    var remote: FakeRemote = .{ .destination_id = profiles.clearing_profile.destinationId() };
    var funding: FakeFunding = .{};
    funding.running.store(false, .release);
    var runtime = try testRuntime(
        allocator,
        tmp.dir,
        profiles.clearing_profile,
        &remote,
        &funding,
        .initialize,
    );
    defer runtime.deinit() catch unreachable;

    try std.testing.expectError(error.FundingNotRunning, runtime.start());
    funding.running.store(true, .release);
    try runtime.start();
    try std.testing.expectError(error.AlreadyStarted, runtime.start());
    funding.running.store(false, .release);
    try std.testing.expectError(error.FundingNotRunning, runtime.stopAndJoin());
    try std.testing.expectEqual(Health.failed, runtime.health());
}

test "idle clearing worker honors the configured poll interval" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profiles = try TestProfiles.init(allocator);
    defer profiles.deinit();
    var remote: FakeRemote = .{ .destination_id = profiles.clearing_profile.destinationId() };
    var funding: FakeFunding = .{};
    var runtime = try Runtime.init(allocator, std.testing.io, .{
        .profile = profiles.clearing_profile,
        .persistence_root = tmp.dir,
        .transport = remote.transport(),
        .funding_port = funding.port(profiles.clearing_profile),
        .open_policy = .initialize,
        .poll_interval_ms = 100,
    });
    defer runtime.deinit() catch unreachable;

    try runtime.start();
    try (std.Io.Timeout{ .duration = .{
        .raw = .fromMilliseconds(20),
        .clock = .awake,
    } }).sleep(std.testing.io);
    const calls_before_stop = funding.is_running_calls.load(.acquire);
    try runtime.stopAndJoin();
    try std.testing.expect(calls_before_stop <= 8);
}

test "runtime rejects an FSG1 port from another settlement profile" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profiles = try TestProfiles.init(allocator);
    defer profiles.deinit();
    var remote: FakeRemote = .{ .destination_id = profiles.clearing_profile.destinationId() };
    var funding: FakeFunding = .{};
    var wrong_port = funding.port(profiles.clearing_profile);
    wrong_port.identity.settlement_profile_digest[0] ^= 1;

    try std.testing.expectError(error.FundingProfileMismatch, Runtime.init(
        allocator,
        std.testing.io,
        .{
            .profile = profiles.clearing_profile,
            .persistence_root = tmp.dir,
            .transport = remote.transport(),
            .funding_port = wrong_port,
            .open_policy = .initialize,
        },
    ));
}

test "provider event identity precedes route and payment validation" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profiles = try TestProfiles.init(allocator);
    defer profiles.deinit();
    var remote: FakeRemote = .{ .destination_id = profiles.clearing_profile.destinationId() };
    var funding: FakeFunding = .{};
    var runtime = try testRuntime(
        allocator,
        tmp.dir,
        profiles.clearing_profile,
        &remote,
        &funding,
        .initialize,
    );
    defer runtime.deinit() catch unreachable;
    enableManualPolling(&runtime);

    const authorization = testAuthorization(506, .deposit);
    _ = try runtime.acceptAuthorization(authorization);

    var reused_authorization = authorization;
    reused_authorization.payment_id += 1;
    reused_authorization.currency = 999;
    try std.testing.expectError(
        error.IdempotencyConflict,
        runtime.acceptAuthorization(reused_authorization),
    );

    var reused_resolution = testResolution(authorization, .post, authorization.amount);
    reused_resolution.event_id = authorization.event_id;
    reused_resolution.payment_id += 999;
    try std.testing.expectError(
        error.IdempotencyConflict,
        runtime.acceptResolution(reused_resolution),
    );
}

test "pre-write completion failure fences until restart reconciliation" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const profiles = try TestProfiles.init(allocator);
    defer profiles.deinit();
    var remote: FakeRemote = .{ .destination_id = profiles.clearing_profile.destinationId() };
    var funding: FakeFunding = .{};

    var before = try testRuntime(
        allocator,
        tmp.dir,
        profiles.clearing_profile,
        &remote,
        &funding,
        .initialize,
    );
    enableManualPolling(&before);
    remote.fail_completion_store = before.store;
    const authorization = testAuthorization(507, .deposit);
    _ = try before.acceptAuthorization(authorization);
    before.pollOnce();
    try std.testing.expectEqual(@as(usize, 1), remote.economic_effects);
    try std.testing.expectEqual(Health.failed, before.health());
    try std.testing.expectError(
        error.RuntimeFailed,
        before.acceptAuthorization(testAuthorization(508, .deposit)),
    );
    try before.deinit();

    var after = try testRuntime(
        allocator,
        tmp.dir,
        profiles.clearing_profile,
        &remote,
        &funding,
        .open_existing,
    );
    defer after.deinit() catch unreachable;
    enableManualPolling(&after);
    after.pollOnce();
    try std.testing.expectEqual(@as(usize, 1), remote.submits);
    try std.testing.expectEqual(@as(usize, 1), remote.lookups);
    try std.testing.expectEqual(saga.PublicState.authorized, (try after.status(507)).state);
}
