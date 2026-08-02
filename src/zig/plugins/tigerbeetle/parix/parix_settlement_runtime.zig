//! Parix host integration for the opt-in TigerBeetle plugin.
const std = @import("std");
const exchange_core = @import("exchange_core");
const tigerbeetle = @import("tigerbeetle");
const profile = @import("parix_settlement_profile.zig");
const profile_manifest = @import("parix_settlement_profile_manifest.zig");

const core = exchange_core.core;
const dispatcher = tigerbeetle.dispatcher;
const exchange_integration = tigerbeetle.exchange_integration;
const outbox = tigerbeetle.outbox;
const plugin = tigerbeetle.plugin;
const registry = tigerbeetle.destination_registry;
const settlement = tigerbeetle.settlement;

pub const Outbox = outbox.DurableFileOutbox(1024);
pub const Integration = exchange_integration.Integration(Outbox, 1024);

pub const Health = enum(u8) {
    cold,
    recovering,
    ready,
    settling,
    draining,
    quarantined,
    failed,
    stopped,
};

pub const ConfigError = dispatcher.PolicyError || error{
    UnsafePersistenceRoot,
    UnsafeOutboxFileName,
    UnsafeProfileManifestFileName,
    InvalidDestinationId,
    InvalidPublicationNamespace,
    InvalidPollInterval,
    InvalidPollBudget,
    InvalidShutdownTimeout,
};

pub const InitError = ConfigError ||
    std.Io.Dir.CreateDirPathOpenError ||
    std.mem.Allocator.Error ||
    registry.RegistryInitError ||
    profile_manifest.EnsureError ||
    outbox.StoreError ||
    exchange_integration.InitError;

pub const InstallError = core.exchange_core.SettlementHooksConfigError || error{
    AlreadyInstalled,
};

pub const StartError = exchange_integration.DispatchStartError || std.Thread.SpawnError || error{
    NotInstalled,
    CoreNotRunning,
    AlreadyStarted,
};

pub const StopError = error{
    CoreStillRunning,
    DrainTimedOut,
    SettlementFailed,
    SettlementQuarantined,
};

const WorkerStopOutcome = enum(u8) {
    pending,
    stopped,
    drain_timed_out,
    failed,
    quarantined,
};

pub const DetachError = error{
    NotInstalled,
    CoreStillRunning,
    ThreadStillRunning,
};

pub const DeinitError = error{
    CoreStillRunning,
    ThreadStillRunning,
};

pub const Config = struct {
    /// Generated runtime state stays under Zig's ignored output tree unless
    /// the host deliberately selects another safe relative directory.
    persistence_root: []const u8 = "zig-out/runtime/parix",
    outbox_path: []const u8 = "settlement-outbox.tso1",
    profile_manifest_path: []const u8 = "settlement-profile.spf1",
    /// Validated immutable product configuration, borrowed for `init` only.
    /// Runtime state owns the derived destination registry thereafter.
    profile: *const profile.Profile,
    transport: plugin.Transport,
    poll_interval_ms: u32 = 10,
    /// Exactly one synchronous transport call per loop keeps shutdown
    /// interruptible between every externally bounded HTTP operation.
    poll_budget: usize = 1,
    shutdown_timeout_ms: u32 = 30_000,
    retry_policy: dispatcher.RetryPolicy = defaultRetryPolicy(),

    pub fn validate(self: Config) ConfigError!void {
        try validateSafeRelativePath(self.persistence_root);
        try validateSafeFileName(self.outbox_path);
        try validateSafeManifestFileName(self.profile_manifest_path);
        if (@intFromEnum(self.profile.destinationId()) == 0) return error.InvalidDestinationId;
        if (self.profile.publicationDomain().namespace == 0) {
            return error.InvalidPublicationNamespace;
        }
        if (self.poll_interval_ms == 0) return error.InvalidPollInterval;
        if (self.poll_budget != 1) return error.InvalidPollBudget;
        if (self.shutdown_timeout_ms == 0) return error.InvalidShutdownTimeout;
        try self.retry_policy.validate();
    }
};

pub const Runtime = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    io: std.Io,
    persistence_root: std.Io.Dir,
    destination_registry: *registry.DestinationRegistry,
    store: *Outbox,
    integration: *Integration,
    poll_interval_ms: u32,
    poll_budget: usize,
    shutdown_timeout_ms: u32,
    installed_core: ?*core.exchange_core.ExchangeCore = null,
    worker: ?std.Thread = null,
    stop_requested: std.atomic.Value(bool) = .init(false),
    stop_wakeup: std.Io.Event = .unset,
    shutdown_deadline_ns: std.atomic.Value(i64) = .init(0),
    worker_stop_outcome: std.atomic.Value(u8) =
        .init(@intFromEnum(WorkerStopOutcome.pending)),
    mutation_in_progress: std.atomic.Mutex = .unlocked,
    cached_health: std.atomic.Value(u8) = .init(@intFromEnum(Health.cold)),

    /// Initializes owned filesystem, registry, outbox, and integration state.
    /// The transport context and vtable are borrowed and must outlive `deinit`.
    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        config: Config,
    ) InitError!Self {
        try config.validate();

        const persistence_root = try std.Io.Dir.cwd().createDirPathOpen(
            io,
            config.persistence_root,
            .{},
        );
        errdefer persistence_root.close(io);

        const destination_registry = try config.profile.createRegistry(allocator);
        errdefer destination_registry.deinit();

        try profile_manifest.ensureBound(
            io,
            persistence_root,
            config.profile_manifest_path,
            .{
                .destination_id = config.profile.destinationId(),
                .publication_domain = config.profile.publicationDomain(),
                .registry_digest = destination_registry.digest(),
                .profile_digest = config.profile.digest(),
            },
        );

        const store = try allocator.create(Outbox);
        errdefer allocator.destroy(store);
        try Outbox.openPreparedInto(
            store,
            allocator,
            persistence_root,
            config.outbox_path,
            config.profile.destinationId(),
            config.profile.publicationDomain(),
        );
        errdefer store.deinit();
        try store.ensureDurableRecoveryAnchor();

        const integration = try allocator.create(Integration);
        errdefer allocator.destroy(integration);
        integration.* = try Integration.init(.{
            .destination_id = config.profile.destinationId(),
            .destination_registry = destination_registry,
            .store = store,
            .mapper_policy = .{ .publication_domain = config.profile.publicationDomain() },
            .retry_policy = config.retry_policy,
            .transport = config.transport,
        });

        return .{
            .allocator = allocator,
            .io = io,
            .persistence_root = persistence_root,
            .destination_registry = destination_registry,
            .store = store,
            .integration = integration,
            .poll_interval_ms = config.poll_interval_ms,
            .poll_budget = config.poll_budget,
            .shutdown_timeout_ms = config.shutdown_timeout_ms,
        };
    }

    /// Borrows the runtime-owned persistence root for durable core
    /// construction. The caller must not close this handle.
    pub fn persistenceRoot(self: *const Self) std.Io.Dir {
        return self.persistence_root;
    }

    /// Installs hooks while the durable exchange core is still cold. This
    /// runtime must remain at a stable address after a successful install.
    pub fn install(
        self: *Self,
        exchange: *core.exchange_core.ExchangeCore,
    ) InstallError!void {
        if (self.installed_core != null) return error.AlreadyInstalled;
        try exchange.setSettlementHooks(self.integration.hooks());
        self.installed_core = exchange;
        self.storeHealth(switch (self.integration.currentPhase()) {
            .recovering => .recovering,
            .live => .cold,
            .failed => .failed,
        });
    }

    /// Starts the sole transport owner after exchange startup has completed.
    /// The runtime must remain pinned until `stopAndJoin` returns.
    pub fn start(self: *Self) StartError!void {
        const exchange = self.installed_core orelse return error.NotInstalled;
        if (!exchange.isStarted() or exchange.isStopped()) return error.CoreNotRunning;
        if (self.worker != null) return error.AlreadyStarted;

        _ = try self.integration.startDispatch();
        self.stop_requested.store(false, .release);
        self.stop_wakeup.reset();
        self.shutdown_deadline_ns.store(0, .release);
        self.storeWorkerStopOutcome(.pending);
        self.cacheIntegrationHealth();
        self.worker = std.Thread.spawn(.{}, pollMain, .{self}) catch |err| {
            self.storeHealth(.failed);
            return err;
        };
    }

    /// Stops capture after the exchange is no longer accepting work, drains
    /// durable work to a terminal state, and joins the sole transport owner.
    pub fn stopAndJoin(self: *Self) StopError!void {
        if (self.installed_core) |exchange| {
            if (exchange.isStarted() and !exchange.isStopped()) {
                return error.CoreStillRunning;
            }
        }

        if (self.worker) |worker| {
            self.publishShutdownDeadline();
            self.stop_requested.store(true, .release);
            self.stop_wakeup.set(self.io);
            worker.join();
            self.worker = null;
            return self.checkWorkerStopOutcome();
        }

        self.publishShutdownDeadline();
        self.stop_requested.store(true, .release);
        self.stop_wakeup.set(self.io);
        self.storeWorkerStopOutcome(.pending);
        self.pollMain();
        return self.checkWorkerStopOutcome();
    }

    /// Releases the runtime's borrowed core pointer after both owners have
    /// stopped. The caller may deinitialize the core only after this succeeds;
    /// the runtime can then release its hook backing state without retaining a
    /// dangling core pointer.
    pub fn detachStoppedCore(self: *Self) DetachError!void {
        if (self.worker != null) return error.ThreadStillRunning;
        const exchange = self.installed_core orelse return error.NotInstalled;
        if (!exchange.isStopped()) return error.CoreStillRunning;
        self.installed_core = null;
    }

    /// Returns the last worker-published, non-secret health state without
    /// taking the integration mutex or waiting for transport I/O.
    pub fn health(self: *const Self) Health {
        return @enumFromInt(self.cached_health.load(.acquire));
    }

    /// Atomically admits one core mutation only while the live integration is
    /// running and fully drained. While held, readiness remains `settling`
    /// even if the poll worker observes the pre-mutation drained state.
    pub fn tryBeginMutation(self: *Self) bool {
        if (!self.mutation_in_progress.tryLock()) return false;

        const actual = healthFromIntegrationStatus(self.integration.status());
        if (actual != .ready) {
            self.storeHealth(actual);
            self.mutation_in_progress.unlock();
            return false;
        }
        self.storeHealth(.settling);
        return true;
    }

    /// Releases a successful mutation admission after the synchronous core
    /// submission has completed, then refreshes the true dispatch state.
    pub fn endMutation(self: *Self) void {
        self.cacheIntegrationHealthOwned();
        self.mutation_in_progress.unlock();
    }

    /// Releases only runtime-owned state. The exchange must already be stopped
    /// and a started worker must already have been joined.
    pub fn deinit(self: *Self) DeinitError!void {
        if (self.worker != null) return error.ThreadStillRunning;
        if (self.installed_core) |exchange| {
            if (exchange.isStarted() and !exchange.isStopped()) {
                return error.CoreStillRunning;
            }
        }

        self.allocator.destroy(self.integration);
        self.store.deinit();
        self.allocator.destroy(self.store);
        self.destination_registry.deinit();
        self.persistence_root.close(self.io);
        self.* = undefined;
    }

    fn pollMain(self: *Self) void {
        var tick: u64 = 0;
        while (true) {
            const stopping = self.stop_requested.load(.acquire);
            if (stopping) {
                const stop_result = self.integration.stopDispatch() catch {
                    self.storeHealth(.failed);
                    self.storeWorkerStopOutcome(.failed);
                    return;
                };
                self.cacheStopResult(stop_result);
                switch (stop_result) {
                    .stopped, .already_stopped => {
                        self.storeWorkerStopOutcome(.stopped);
                        return;
                    },
                    .quarantined => {
                        self.storeWorkerStopOutcome(.quarantined);
                        return;
                    },
                    .failed => {
                        self.storeWorkerStopOutcome(.failed);
                        return;
                    },
                    .disabled => {
                        self.storeHealth(.failed);
                        self.storeWorkerStopOutcome(.failed);
                        return;
                    },
                    .busy, .draining => {},
                }
            }

            tick = std.math.add(u64, tick, 1) catch {
                self.storeHealth(.failed);
                self.storeWorkerStopOutcome(.failed);
                return;
            };
            _ = self.integration.pollDispatch(
                .fromInt(tick),
                self.poll_budget,
            ) catch {
                self.cacheIntegrationHealth();
                if (!isTerminal(self.health())) self.storeHealth(.failed);
                self.storeWorkerStopOutcome(workerOutcomeFromHealth(self.health()));
                return;
            };
            self.cacheIntegrationHealth();
            if (stopping and isTerminal(self.health())) {
                self.storeWorkerStopOutcome(workerOutcomeFromHealth(self.health()));
                return;
            }

            var sleep_duration = std.Io.Duration.fromMilliseconds(
                @intCast(self.poll_interval_ms),
            );
            if (stopping) {
                const deadline_ns = self.shutdown_deadline_ns.load(.acquire);
                const now_ns = awakeNowNanoseconds(self.io);
                if (deadline_ns == 0 or now_ns >= deadline_ns) {
                    self.storeHealth(.draining);
                    self.storeWorkerStopOutcome(.drain_timed_out);
                    return;
                }
                const remaining_ns: i64 = deadline_ns - now_ns;
                if (remaining_ns < sleep_duration.nanoseconds) {
                    sleep_duration = .fromNanoseconds(remaining_ns);
                }
            }

            if (stopping) {
                std.Io.sleep(self.io, sleep_duration, .awake) catch {
                    self.storeHealth(.failed);
                    self.storeWorkerStopOutcome(.failed);
                    return;
                };
            } else {
                self.stop_wakeup.waitTimeout(self.io, .{ .duration = .{
                    .raw = sleep_duration,
                    .clock = .awake,
                } }) catch |err| switch (err) {
                    error.Timeout => {},
                    error.Canceled => {
                        self.storeHealth(.failed);
                        self.storeWorkerStopOutcome(.failed);
                        return;
                    },
                };
            }
        }
    }

    fn cacheIntegrationHealth(self: *Self) void {
        if (!self.mutation_in_progress.tryLock()) {
            self.storeHealth(.settling);
            return;
        }
        defer self.mutation_in_progress.unlock();
        self.cacheIntegrationHealthOwned();
    }

    fn cacheIntegrationHealthOwned(self: *Self) void {
        self.storeHealth(healthFromIntegrationStatus(self.integration.status()));
    }

    fn cacheStopResult(self: *Self, result: plugin.StopResult) void {
        self.storeHealth(switch (result) {
            .disabled, .failed => .failed,
            .busy, .draining => .draining,
            .quarantined => .quarantined,
            .stopped, .already_stopped => .stopped,
        });
    }

    fn storeHealth(self: *Self, value: Health) void {
        self.cached_health.store(@intFromEnum(value), .release);
    }

    fn storeWorkerStopOutcome(self: *Self, value: WorkerStopOutcome) void {
        self.worker_stop_outcome.store(@intFromEnum(value), .release);
    }

    fn publishShutdownDeadline(self: *Self) void {
        const now_ns = awakeNowNanoseconds(self.io);
        const timeout_ns = @as(i64, self.shutdown_timeout_ms) * std.time.ns_per_ms;
        const deadline_ns = std.math.add(i64, now_ns, timeout_ns) catch
            std.math.maxInt(i64);
        self.shutdown_deadline_ns.store(deadline_ns, .release);
    }

    fn checkWorkerStopOutcome(self: *const Self) StopError!void {
        return stopErrorForOutcome(@enumFromInt(
            self.worker_stop_outcome.load(.acquire),
        ));
    }
};

fn awakeNowNanoseconds(io: std.Io) i64 {
    return std.math.cast(i64, std.Io.Clock.awake.now(io).nanoseconds) orelse
        std.math.maxInt(i64);
}

fn workerOutcomeFromHealth(value: Health) WorkerStopOutcome {
    return switch (value) {
        .stopped => .stopped,
        .quarantined => .quarantined,
        .failed => .failed,
        .cold, .recovering, .ready, .settling, .draining => .failed,
    };
}

fn stopErrorForOutcome(outcome: WorkerStopOutcome) StopError!void {
    return switch (outcome) {
        .stopped => {},
        .drain_timed_out => error.DrainTimedOut,
        .failed, .pending => error.SettlementFailed,
        .quarantined => error.SettlementQuarantined,
    };
}

pub fn defaultRetryPolicy() dispatcher.RetryPolicy {
    return .{
        .known_not_sent = .{ .initial = .fromInt(2), .maximum = .fromInt(8) },
        .reconciliation = .{ .initial = .fromInt(1), .maximum = .fromInt(8) },
        .confirmed_absent = .{ .initial = .fromInt(2), .maximum = .fromInt(8) },
    };
}

pub fn healthFromIntegrationStatus(status: exchange_integration.IntegrationStatus) Health {
    return switch (status) {
        .recovering => |dispatch| switch (dispatch) {
            .quarantined => .quarantined,
            else => .recovering,
        },
        .failed => .failed,
        .live => |plugin_status| switch (plugin_status) {
            .disabled => .failed,
            .enabled => |enabled| healthFromEnabledStatus(
                enabled.lifecycle,
                enabled.dispatch,
            ),
        },
    };
}

fn healthFromEnabledStatus(
    lifecycle: plugin.Lifecycle,
    dispatch: outbox.DispatchState,
) Health {
    return switch (lifecycle) {
        .cold => .cold,
        .running => switch (dispatch) {
            .drained => .ready,
            .awaiting_commit,
            .waiting_submit,
            .waiting_reconciliation,
            .in_flight,
            => .settling,
            .recovery_required => .recovering,
            .quarantined => .quarantined,
        },
        .draining => .draining,
        .quarantined => .quarantined,
        .failed => .failed,
        .stopped => .stopped,
    };
}

fn isTerminal(value: Health) bool {
    return switch (value) {
        .quarantined, .failed, .stopped => true,
        .cold, .recovering, .ready, .settling, .draining => false,
    };
}

fn validateSafeRelativePath(path: []const u8) ConfigError!void {
    if (path.len == 0 or std.fs.path.isAbsolute(path)) {
        return error.UnsafePersistenceRoot;
    }
    var components = std.mem.splitScalar(u8, path, '/');
    var count: usize = 0;
    while (components.next()) |component| {
        if (!isSafePathComponent(component)) return error.UnsafePersistenceRoot;
        count += 1;
    }
    if (count == 0) return error.UnsafePersistenceRoot;
}

fn validateSafeFileName(path: []const u8) ConfigError!void {
    if (!isSafePathComponent(path)) return error.UnsafeOutboxFileName;
}

fn validateSafeManifestFileName(path: []const u8) ConfigError!void {
    if (!isSafePathComponent(path)) return error.UnsafeProfileManifestFileName;
}

fn isSafePathComponent(component: []const u8) bool {
    if (component.len == 0 or
        std.mem.eql(u8, component, ".") or
        std.mem.eql(u8, component, ".."))
    {
        return false;
    }
    for (component) |byte| {
        if (!std.ascii.isAlphanumeric(byte) and
            byte != '-' and byte != '_' and byte != '.')
        {
            return false;
        }
    }
    return true;
}

const FakeTransport = struct {
    const Mode = enum {
        retry_forever,
        quarantine,
    };

    mode: Mode = .retry_forever,
    submit_calls: std.atomic.Value(usize) = .init(0),

    fn asTransport(self: *FakeTransport) plugin.Transport {
        return .{ .context = self, .vtable = &vtable };
    }

    fn submit(
        context: *anyopaque,
        _: outbox.DestinationId,
        _: *const settlement.TransferChain,
    ) dispatcher.SubmitDisposition {
        const self: *FakeTransport = @ptrCast(@alignCast(context));
        _ = self.submit_calls.fetchAdd(1, .monotonic);
        return switch (self.mode) {
            .retry_forever => .known_not_sent,
            .quarantine => .permanent,
        };
    }

    fn lookup(
        _: *anyopaque,
        _: outbox.DestinationId,
        _: *const settlement.TransferChain,
        _: outbox.ReconciliationCause,
    ) dispatcher.LookupDisposition {
        return .retryable;
    }

    const vtable: plugin.Transport.VTable = .{
        .submit = submit,
        .lookup = lookup,
    };
};

fn stagePendingSettlement(store: *Outbox, settlement_profile: *const profile.Profile) !void {
    const publication_domain = settlement_profile.publicationDomain();
    const identity: settlement.FillIdentity = .{
        .namespace = publication_domain.namespace,
        .journal_epoch = publication_domain.journal_epoch,
        .publication_sequence = core.publication_sequence.PublicationSequence.fromInt(1),
        .fill_ordinal = 0,
    };
    const group_id = try identity.groupId();
    var chain: settlement.TransferChain = .{
        .group_id = group_id,
        .count = 1,
    };
    chain.storage[0] = .{
        .id = try identity.transferId(.quote_principal),
        .debit_account_id = 101,
        .credit_account_id = 102,
        .amount = 10,
        .user_data_128 = group_id,
        .ledger = 421,
        .code = 300,
    };
    const cursor = try identity.publicationCursor();
    const prepared = try store.prepare(&.{.{
        .destination_id = settlement_profile.destinationId(),
        .publication = cursor,
        .chain = chain,
    }});
    try std.testing.expectEqual(@as(usize, 1), prepared.inserted);
    store.markJournalCommittedThrough(cursor);
    store.markRiskR2CommittedThrough(cursor);
}

fn expectTerminalStop(
    mode: FakeTransport.Mode,
    inject_storage_failure: bool,
    expected_error: StopError,
) !void {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const relative_root = try std.fmt.allocPrint(
        allocator,
        ".zig-cache/tmp/{s}/parix-terminal-stop",
        .{tmp.sub_path},
    );
    defer allocator.free(relative_root);

    const settlement_profile = try profile.Profile.developmentExample(allocator);
    defer settlement_profile.deinit();

    var fake: FakeTransport = .{ .mode = mode };
    var runtime = try Runtime.init(allocator, std.testing.io, .{
        .persistence_root = relative_root,
        .outbox_path = "settlement.tso1",
        .profile = settlement_profile,
        .transport = fake.asTransport(),
        .poll_interval_ms = 1,
        .poll_budget = 1,
        .shutdown_timeout_ms = 100,
    });
    errdefer runtime.deinit() catch {};

    var exchange = try core.exchange_core.ExchangeCore.initWithIoAndPersistenceRoot(
        allocator,
        std.testing.io,
        runtime.persistenceRoot(),
        .{
            .initial_state = core.config.InitialStateConfig.cleanStart("PARIX_TERMINAL_STOP_TEST"),
            .matching = .{ .orderbook_kind = .direct },
            .serialization = .{
                .enable_journaling = true,
                .backend = .disk,
                .storage_folder = "core",
            },
        },
    );
    try runtime.install(&exchange);
    try exchange.startup();
    try stagePendingSettlement(runtime.store, settlement_profile);
    if (inject_storage_failure) runtime.store.injectWriteFailureForTesting();
    try runtime.start();

    exchange.shutdown();
    try std.testing.expectError(expected_error, runtime.stopAndJoin());
    try std.testing.expect(runtime.worker == null);
    try runtime.detachStoppedCore();
    exchange.deinit();
    try runtime.deinit();
}

test "settlement health requires a running and drained dispatch queue" {
    try std.testing.expectEqual(
        Health.ready,
        healthFromIntegrationStatus(.{ .live = .{ .enabled = .{
            .lifecycle = .running,
            .dispatch = .drained,
        } } }),
    );

    const settling_states = [_]outbox.DispatchState{
        .{ .awaiting_commit = undefined },
        .{ .waiting_submit = undefined },
        .{ .waiting_reconciliation = undefined },
        .{ .in_flight = undefined },
    };
    for (settling_states) |dispatch| {
        try std.testing.expectEqual(
            Health.settling,
            healthFromIntegrationStatus(.{ .live = .{ .enabled = .{
                .lifecycle = .running,
                .dispatch = dispatch,
            } } }),
        );
    }
}

test "settlement health exposes recovery and quarantine dispatch states" {
    try std.testing.expectEqual(
        Health.recovering,
        healthFromIntegrationStatus(.{ .live = .{ .enabled = .{
            .lifecycle = .running,
            .dispatch = .{ .recovery_required = undefined },
        } } }),
    );
    try std.testing.expectEqual(
        Health.recovering,
        healthFromIntegrationStatus(.{ .recovering = .drained }),
    );
    try std.testing.expectEqual(
        Health.quarantined,
        healthFromIntegrationStatus(.{ .live = .{ .enabled = .{
            .lifecycle = .running,
            .dispatch = .{ .quarantined = undefined },
        } } }),
    );
    try std.testing.expectEqual(
        Health.quarantined,
        healthFromIntegrationStatus(.{ .recovering = .{ .quarantined = undefined } }),
    );
    try std.testing.expectEqual(
        Health.failed,
        healthFromIntegrationStatus(.{ .live = .{ .enabled = .{
            .lifecycle = .failed,
            .dispatch = .{ .recovery_required = undefined },
        } } }),
    );
    try std.testing.expectEqual(
        Health.stopped,
        healthFromIntegrationStatus(.{ .live = .{ .enabled = .{
            .lifecycle = .stopped,
            .dispatch = .{ .quarantined = undefined },
        } } }),
    );
}

test "settlement runtime config rejects unsafe paths and zero poll controls" {
    const allocator = std.testing.allocator;
    const settlement_profile = try profile.Profile.developmentExample(allocator);
    defer settlement_profile.deinit();

    var fake: FakeTransport = .{};
    const base: Config = .{
        .profile = settlement_profile,
        .transport = fake.asTransport(),
    };

    try base.validate();

    var invalid = base;
    invalid.persistence_root = "/private/tmp/exchange-core";
    try std.testing.expectError(error.UnsafePersistenceRoot, invalid.validate());
    invalid.persistence_root = "safe/../escape";
    try std.testing.expectError(error.UnsafePersistenceRoot, invalid.validate());
    invalid.persistence_root = "safe";
    invalid.outbox_path = "nested/outbox.tso1";
    try std.testing.expectError(error.UnsafeOutboxFileName, invalid.validate());
    invalid.outbox_path = "outbox.tso1";
    invalid.profile_manifest_path = "nested/profile.spf1";
    try std.testing.expectError(error.UnsafeProfileManifestFileName, invalid.validate());
    invalid.profile_manifest_path = "profile.spf1";
    invalid.poll_interval_ms = 0;
    try std.testing.expectError(error.InvalidPollInterval, invalid.validate());
    invalid.poll_interval_ms = 1;
    invalid.poll_budget = 0;
    try std.testing.expectError(error.InvalidPollBudget, invalid.validate());
    invalid.poll_budget = 2;
    try std.testing.expectError(error.InvalidPollBudget, invalid.validate());
    invalid.poll_budget = 1;
    invalid.shutdown_timeout_ms = 0;
    try std.testing.expectError(error.InvalidShutdownTimeout, invalid.validate());
}

test "settlement runtime reopens only the exact bound profile without transport calls" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const relative_root = try std.fmt.allocPrint(
        allocator,
        ".zig-cache/tmp/{s}/parix-profile-reopen",
        .{tmp.sub_path},
    );
    defer allocator.free(relative_root);

    const settlement_profile = try profile.Profile.developmentExample(allocator);
    defer settlement_profile.deinit();

    var fake: FakeTransport = .{};
    var runtime = try Runtime.init(allocator, std.testing.io, .{
        .persistence_root = relative_root,
        .profile = settlement_profile,
        .transport = fake.asTransport(),
    });
    try std.testing.expectEqual(@as(usize, 0), fake.submit_calls.load(.acquire));
    try std.testing.expectEqual(outbox.RecoveryProvenance.reopened, runtime.store.recoveryProvenance());
    try std.testing.expect(runtime.store.isRecoveryRequired());
    try runtime.deinit();

    var reopened = try Runtime.init(allocator, std.testing.io, .{
        .persistence_root = relative_root,
        .profile = settlement_profile,
        .transport = fake.asTransport(),
    });
    defer reopened.deinit() catch {};
    try std.testing.expectEqual(@as(usize, 0), fake.submit_calls.load(.acquire));
    try std.testing.expectEqual(outbox.RecoveryProvenance.reopened, reopened.store.recoveryProvenance());
    try std.testing.expect(reopened.store.isRecoveryRequired());
}

test "settlement runtime recovers a legacy nontrade-only journal without an outbox snapshot" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const relative_root = try std.fmt.allocPrint(
        allocator,
        ".zig-cache/tmp/{s}/parix-empty-anchor-restart",
        .{tmp.sub_path},
    );
    defer allocator.free(relative_root);

    const settlement_profile = try profile.Profile.developmentExample(allocator);
    defer settlement_profile.deinit();
    var fake: FakeTransport = .{};

    {
        var runtime = try Runtime.init(allocator, std.testing.io, .{
            .persistence_root = relative_root,
            .profile = settlement_profile,
            .transport = fake.asTransport(),
        });
        var exchange = try core.exchange_core.ExchangeCore.initWithIoAndPersistenceRoot(
            allocator,
            std.testing.io,
            runtime.persistenceRoot(),
            .{
                .initial_state = core.config.InitialStateConfig.lastKnownStateFromJournal(
                    "exchange-core",
                    0,
                    0,
                ),
                .matching = .{ .orderbook_kind = .direct },
                .serialization = .{
                    .enable_journaling = true,
                    .backend = .disk,
                    .storage_folder = "core",
                },
            },
        );
        var cleaned = false;
        defer if (!cleaned) {
            if (!exchange.isStopped()) exchange.shutdown();
            runtime.stopAndJoin() catch {};
            runtime.detachStoppedCore() catch {};
            exchange.deinit();
            runtime.deinit() catch {};
        };

        // Simulate a root produced before the runtime seeded an empty outbox:
        // the profile binding exists and the in-memory integration can run,
        // but only the subsequent core journal survives the first shutdown.
        try runtime.persistenceRoot().deleteFile(
            std.testing.io,
            "settlement-outbox.tso1",
        );
        try runtime.install(&exchange);
        const startup = try exchange.startupFromConfiguredPersistence(
            runtime.persistenceRoot(),
            .matching_engine_router,
            0,
            0,
        );
        try std.testing.expectEqual(@as(usize, 0), startup.journal_replayed_commands);
        {
            var api = core.exchange_api.ExchangeApi.init(allocator, &exchange);
            defer api.deinit();
            var result = try api.submitCommandSync(.{ .add_user = 88_001 });
            defer result.deinit(allocator);
            switch (result) {
                .response => |response| try std.testing.expectEqual(
                    core.cmd.CommandResultCode.success,
                    response.result_code,
                ),
                .persist_state => return error.UnexpectedPersistState,
            }
        }

        exchange.shutdown();
        try runtime.stopAndJoin();
        try runtime.detachStoppedCore();
        exchange.deinit();
        try runtime.deinit();
        cleaned = true;
    }

    {
        var runtime = try Runtime.init(allocator, std.testing.io, .{
            .persistence_root = relative_root,
            .profile = settlement_profile,
            .transport = fake.asTransport(),
        });
        var exchange = try core.exchange_core.ExchangeCore.initWithIoAndPersistenceRoot(
            allocator,
            std.testing.io,
            runtime.persistenceRoot(),
            .{
                .initial_state = core.config.InitialStateConfig.lastKnownStateFromJournal(
                    "exchange-core",
                    0,
                    0,
                ),
                .matching = .{ .orderbook_kind = .direct },
                .serialization = .{
                    .enable_journaling = true,
                    .backend = .disk,
                    .storage_folder = "core",
                },
            },
        );
        var cleaned = false;
        defer if (!cleaned) {
            if (!exchange.isStopped()) exchange.shutdown();
            runtime.stopAndJoin() catch {};
            runtime.detachStoppedCore() catch {};
            exchange.deinit();
            runtime.deinit() catch {};
        };

        try runtime.install(&exchange);
        const startup = try exchange.startupFromConfiguredPersistence(
            runtime.persistenceRoot(),
            .matching_engine_router,
            0,
            0,
        );
        try std.testing.expectEqual(@as(usize, 1), startup.journal_replayed_commands);

        exchange.shutdown();
        try runtime.stopAndJoin();
        try runtime.detachStoppedCore();
        exchange.deinit();
        try runtime.deinit();
        cleaned = true;
    }

    try std.testing.expectEqual(@as(usize, 0), fake.submit_calls.load(.acquire));
}

test "settlement runtime rejects a changed profile manifest before transport" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const relative_root = try std.fmt.allocPrint(
        allocator,
        ".zig-cache/tmp/{s}/parix-profile-mismatch",
        .{tmp.sub_path},
    );
    defer allocator.free(relative_root);

    const settlement_profile = try profile.Profile.developmentExample(allocator);
    defer settlement_profile.deinit();
    const destination_registry = try settlement_profile.createRegistry(allocator);
    defer destination_registry.deinit();

    var wrong_profile_digest = settlement_profile.digest();
    wrong_profile_digest[0] ^= 0xff;

    {
        const persistence_root = try std.Io.Dir.cwd().createDirPathOpen(
            std.testing.io,
            relative_root,
            .{},
        );
        defer persistence_root.close(std.testing.io);
        try profile_manifest.ensureBound(
            std.testing.io,
            persistence_root,
            "settlement-profile.spf1",
            .{
                .destination_id = settlement_profile.destinationId(),
                .publication_domain = settlement_profile.publicationDomain(),
                .registry_digest = destination_registry.digest(),
                .profile_digest = wrong_profile_digest,
            },
        );
    }

    var fake: FakeTransport = .{};
    try std.testing.expectError(
        error.ProfileDigestMismatch,
        Runtime.init(allocator, std.testing.io, .{
            .persistence_root = relative_root,
            .profile = settlement_profile,
            .transport = fake.asTransport(),
        }),
    );
    try std.testing.expectEqual(@as(usize, 0), fake.submit_calls.load(.acquire));
}

test "settlement shutdown times out with durable work under retryable outage" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const relative_root = try std.fmt.allocPrint(
        allocator,
        ".zig-cache/tmp/{s}/parix-timeout",
        .{tmp.sub_path},
    );
    defer allocator.free(relative_root);

    const settlement_profile = try profile.Profile.developmentExample(allocator);
    defer settlement_profile.deinit();

    var fake: FakeTransport = .{};
    var runtime = try Runtime.init(allocator, std.testing.io, .{
        .persistence_root = relative_root,
        .outbox_path = "settlement.tso1",
        .profile = settlement_profile,
        .transport = fake.asTransport(),
        .poll_interval_ms = 60_000,
        .poll_budget = 1,
        .shutdown_timeout_ms = 3,
    });
    errdefer runtime.deinit() catch {};

    var exchange = try core.exchange_core.ExchangeCore.initWithIoAndPersistenceRoot(
        allocator,
        std.testing.io,
        runtime.persistenceRoot(),
        .{
            .initial_state = core.config.InitialStateConfig.cleanStart("PARIX_TIMEOUT_TEST"),
            .matching = .{ .orderbook_kind = .direct },
            .serialization = .{
                .enable_journaling = true,
                .backend = .disk,
                .storage_folder = "core",
            },
        },
    );
    try runtime.install(&exchange);
    try exchange.startup();
    try stagePendingSettlement(runtime.store, settlement_profile);
    try runtime.start();
    while (fake.submit_calls.load(.acquire) == 0) {
        std.Thread.yield() catch {};
    }

    exchange.shutdown();
    try std.testing.expectError(error.DrainTimedOut, runtime.stopAndJoin());
    try std.testing.expectEqual(Health.draining, runtime.health());
    try std.testing.expectEqual(@as(usize, 1), runtime.store.len());
    try runtime.detachStoppedCore();
    exchange.deinit();
    try runtime.deinit();

    var reopened = try Runtime.init(allocator, std.testing.io, .{
        .persistence_root = relative_root,
        .outbox_path = "settlement.tso1",
        .profile = settlement_profile,
        .transport = fake.asTransport(),
    });
    try std.testing.expectEqual(@as(usize, 1), reopened.store.len());
    try reopened.deinit();
}

test "settlement shutdown reports failed and quarantined terminal outcomes" {
    try expectTerminalStop(.retry_forever, true, error.SettlementFailed);
    try expectTerminalStop(.quarantine, false, error.SettlementQuarantined);
}

test "settlement runtime owns stable state and drains its poll thread" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const relative_root = try std.fmt.allocPrint(
        allocator,
        ".zig-cache/tmp/{s}/parix-runtime",
        .{tmp.sub_path},
    );
    defer allocator.free(relative_root);

    const settlement_profile = try profile.Profile.developmentExample(allocator);
    defer settlement_profile.deinit();

    var fake: FakeTransport = .{};
    var runtime = try Runtime.init(allocator, std.testing.io, .{
        .persistence_root = relative_root,
        .outbox_path = "settlement.tso1",
        .profile = settlement_profile,
        .transport = fake.asTransport(),
        .poll_interval_ms = 1,
        .poll_budget = 1,
    });
    errdefer runtime.deinit() catch {};

    var exchange = try core.exchange_core.ExchangeCore.initWithIoAndPersistenceRoot(
        allocator,
        std.testing.io,
        runtime.persistenceRoot(),
        .{
            .initial_state = core.config.InitialStateConfig.cleanStart("PARIX_RUNTIME_TEST"),
            .matching = .{ .orderbook_kind = .direct },
            .serialization = .{
                .enable_journaling = true,
                .backend = .disk,
                .storage_folder = "core",
            },
        },
    );
    try runtime.install(&exchange);
    try std.testing.expectEqual(Health.recovering, runtime.health());
    try exchange.startup();
    try std.testing.expectError(error.CoreStillRunning, runtime.detachStoppedCore());
    try runtime.start();
    try std.testing.expectEqual(Health.ready, runtime.health());
    try std.testing.expect(runtime.tryBeginMutation());
    try std.testing.expectEqual(Health.settling, runtime.health());
    try std.testing.expect(!runtime.tryBeginMutation());
    runtime.cacheIntegrationHealth();
    try std.testing.expectEqual(Health.settling, runtime.health());
    runtime.endMutation();
    try std.testing.expectEqual(Health.ready, runtime.health());

    exchange.shutdown();
    try std.testing.expectError(error.ThreadStillRunning, runtime.detachStoppedCore());
    try runtime.stopAndJoin();
    try std.testing.expectEqual(Health.stopped, runtime.health());
    try runtime.detachStoppedCore();
    try std.testing.expectError(error.NotInstalled, runtime.detachStoppedCore());
    exchange.deinit();
    try runtime.deinit();
}
