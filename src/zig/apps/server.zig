const std = @import("std");
const builtin = @import("builtin");
const exchange_core = @import("exchange_core");
const protocol = @import("exchange_protocol");
const tigerbeetle_parix = @import("tigerbeetle_parix");
const ingress_auth = @import("ingress_auth.zig");
const parix_account_bootstrap = tigerbeetle_parix.account_bootstrap;
const parix_clearing_http_ingress = tigerbeetle_parix.clearing_http_ingress;
const parix_clearing_profile = tigerbeetle_parix.clearing_profile;
const parix_clearing_provider_adapter = tigerbeetle_parix.clearing_provider_adapter;
const parix_clearing_runtime = tigerbeetle_parix.clearing_runtime;
const parix_clearing_saga = tigerbeetle_parix.clearing_saga;
const parix_funding_runtime = tigerbeetle_parix.funding_runtime;
const parix_funding_saga = tigerbeetle_parix.funding_saga;
const parix_http_transport = tigerbeetle_parix.http_transport;
const parix_profile = tigerbeetle_parix.settlement_profile;
const parix_settlement_runtime = tigerbeetle_parix.settlement_runtime;

const core = exchange_core.core;
const log = std.log.scoped(.exchange_server);

pub const DefaultHost = "127.0.0.1";
pub const DefaultPort: u16 = 7379;
pub const DefaultProviderPort: u16 = 7380;
pub const DefaultSymbolId: i32 = 241;
pub const DefaultBaseCurrency: i32 = 11;
pub const DefaultQuoteCurrency: i32 = 15;
pub const DefaultParixBaseUrl = "https://dev.parix.io";
pub const DefaultPersistenceRoot = "zig-out/runtime/parix";

const MaxRequestLineBytes = 64 * 1024;
const MaxResponseBytes = 4096;
const MaxAuthenticatedWriteBufferBytes = MaxResponseBytes + 128;
const AuthenticatedReceiveTimeoutSeconds = 5;
const MaxAuthenticatedSessions = 64;
const ProviderIngressStartupPollAttempts = 5_000;
const BootstrapTransferId: i32 = 1;

const AuthSessionState = enum {
    awaiting_auth,
    awaiting_proof,
    awaiting_request,
    finished,

    fn advance(self: *AuthSessionState, next: AuthSessionState) error{InvalidAuthStateTransition}!void {
        const valid = switch (self.*) {
            .awaiting_auth => next == .awaiting_proof,
            .awaiting_proof => next == .awaiting_request,
            .awaiting_request => next == .finished,
            .finished => false,
        };
        if (!valid) return error.InvalidAuthStateTransition;
        self.* = next;
    }
};

const ExecutionAttempt = union(enum) {
    completed: bool,
    failed: anyerror,
};

const ArgumentError = error{
    InvalidArguments,
    MissingArgumentValue,
    InvalidPersistenceRoot,
    InvalidPort,
    NonLoopbackHost,
    MissingParixProfile,
    ParixProfileWithoutSettlement,
    MissingIngressAuthProfile,
    IngressAuthProfileWithoutSettlement,
    ParixProvisioningWithoutSettlement,
    ParixFundingInitializationWithoutSettlement,
    ClearingProfileWithoutSettlement,
    MissingProviderProfile,
    ProviderProfileWithoutClearing,
    ClearingInitializationWithoutClearing,
    ClearingAccountAttestationWithoutClearing,
    MissingClearingAccountAttestation,
    ProviderPortWithoutClearing,
    DuplicateListenerPort,
};

const RequestMapError = error{
    NonCommandRequest,
};

const ParixEnvironmentError = error{
    MissingParixDatabaseId,
    MissingParixCredential,
    MultipleParixCredentials,
    ParixDatabaseIdMismatch,
};

const ParixRequestError = error{
    ParixDepositUnsupported,
    ParixProfileUnavailable,
    UnknownParixProfileUser,
    UnsupportedParixSymbol,
};

const IngressProfileError = error{
    IngressInstanceMismatch,
    IngressTraderMissingFromSettlement,
};

const ClearingProfileError = error{
    ProviderProfileMismatch,
};

const ParixEnvironment = struct {
    base_url: []const u8,
    database_id: []const u8,
    auth: parix_http_transport.Auth,
};

const FundingBalanceContext = struct {
    allocator: std.mem.Allocator,
    api: *core.exchange_api.ExchangeApi,

    const vtable: parix_funding_runtime.BalancePort.VTable = .{
        .apply = apply,
        .is_running = isRunning,
        .observe_adjustment_counter = observeAdjustmentCounter,
    };

    fn asPort(self: *@This()) parix_funding_runtime.BalancePort {
        return .{ .context = self, .vtable = &vtable };
    }

    fn apply(raw_context: *anyopaque, adjustment: parix_funding_saga.CoreAdjustment) parix_funding_saga.CoreOutcome {
        const self: *@This() = @ptrCast(@alignCast(raw_context));
        var result = self.api.submitCommandSync(.{ .adjust_user_balance = .{
            .uid = adjustment.uid,
            .transaction_id = adjustment.transaction_id,
            .currency = adjustment.currency,
            .amount = adjustment.amount,
            .adjustment_type = .adjustment,
        } }) catch return .retryable;
        defer result.deinit(self.allocator);

        return switch (result) {
            .response => |response| fundingCoreOutcome(response.result_code),
            .persist_state => .rejected,
        };
    }

    fn isRunning(raw_context: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(raw_context));
        return self.api.isStarted() and !self.api.isStopped();
    }

    fn observeAdjustmentCounter(raw_context: *anyopaque, uid: i64) ?i64 {
        const self: *@This() = @ptrCast(@alignCast(raw_context));
        return self.api.queryUserAdjustmentCounter(uid) catch null;
    }
};

const ClearingAdmissionContext = struct {
    runtime: *parix_clearing_runtime.Runtime,

    const vtable: parix_clearing_http_ingress.AdmissionPort.VTable = .{
        .admit = admit,
        .is_ready = isReady,
    };

    fn asPort(self: *@This()) parix_clearing_http_ingress.AdmissionPort {
        return .{ .context = self, .vtable = &vtable };
    }

    fn admit(
        raw_context: *anyopaque,
        event: parix_clearing_provider_adapter.VerifiedEvent,
    ) parix_clearing_http_ingress.AdmissionOutcome {
        const self: *@This() = @ptrCast(@alignCast(raw_context));
        const accepted = switch (event) {
            .authorization => |authorization| self.runtime.acceptAuthorization(authorization),
            .resolution => |resolution| self.runtime.acceptResolution(resolution),
        } catch |err| return clearingAdmissionError(err);
        return .{ .durable = accepted };
    }

    fn isReady(raw_context: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(raw_context));
        return switch (self.runtime.health()) {
            .ready, .pending => true,
            .cold, .failed, .quarantined, .stopped => false,
        };
    }
};

fn clearingAdmissionError(
    err: parix_clearing_runtime.AdmissionError,
) parix_clearing_http_ingress.AdmissionOutcome {
    return switch (err) {
        error.InvalidEventId,
        error.InvalidPaymentId,
        error.InvalidProviderId,
        error.InvalidRevision,
        error.InvalidUserId,
        error.InvalidCurrency,
        error.InvalidAmount,
        error.InvalidTimeout,
        error.UnknownUser,
        error.UnknownCurrency,
        error.TimeoutOutOfBounds,
        => .invalid,

        error.IdempotencyConflict,
        error.PaymentNotFound,
        error.AuthorizationNotReady,
        error.ImmutableFieldMismatch,
        error.StaleRevision,
        error.OutOfOrderRevision,
        error.AmountExceedsAuthorization,
        error.VoidAmountMismatch,
        error.TerminalConflict,
        => .conflict,

        error.ProviderMismatch,
        error.InvalidDestination,
        error.InvalidLedger,
        error.InvalidAccount,
        error.InvalidTransferCode,
        error.InvalidBinding,
        error.CapacityExceeded,
        error.NoPendingAction,
        error.StaleClaim,
        error.CompletionKindMismatch,
        error.InvalidState,
        error.InvalidClock,
        error.ExpirationOverflow,
        error.GenerationOverflow,
        error.AttemptOverflow,
        error.InvalidPath,
        error.Truncated,
        error.TrailingBytes,
        error.MagicMismatch,
        error.UnsupportedVersion,
        error.InvalidFlags,
        error.CapacityMismatch,
        error.DestinationMismatch,
        error.SettlementProfileMismatch,
        error.ClearingProfileMismatch,
        error.EntryCountExceeded,
        error.DigestMismatch,
        error.InvalidEnum,
        error.InvalidOptional,
        error.OutOfMemory,
        error.AlreadyOpen,
        error.AlreadyInitialized,
        error.NotInitialized,
        error.Poisoned,
        error.InsecurePermissions,
        error.StorageReadFailed,
        error.StorageAmbiguous,
        error.RuntimeNotRunning,
        error.RuntimeFailed,
        error.RuntimeQuarantined,
        => .unavailable,
    };
}

fn fundingCoreOutcome(result_code: core.cmd.CommandResultCode) parix_funding_saga.CoreOutcome {
    return switch (result_code) {
        .success => .applied,
        .user_mgmt_account_balance_adjustment_already_applied_same => .already_applied_same,
        .user_mgmt_account_balance_adjustment_already_applied_many => .already_applied_many,
        else => .rejected,
    };
}

pub const Options = struct {
    host: []const u8 = DefaultHost,
    port: u16 = DefaultPort,
    parix_settlement: bool = false,
    parix_profile_path: ?[]const u8 = null,
    ingress_auth_profile_path: ?[]const u8 = null,
    parix_create_accounts: bool = false,
    parix_development_seed: bool = false,
    parix_funding_initialize: bool = false,
    parix_clearing_profile_path: ?[]const u8 = null,
    parix_provider_profile_path: ?[]const u8 = null,
    parix_clearing_initialize: bool = false,
    parix_clearing_accounts_verified: bool = false,
    provider_port: u16 = DefaultProviderPort,
    provider_port_explicit: bool = false,
    persistence_root: []const u8 = DefaultPersistenceRoot,
    owned_host: ?[]u8 = null,
    owned_parix_profile_path: ?[]u8 = null,
    owned_ingress_auth_profile_path: ?[]u8 = null,
    owned_parix_clearing_profile_path: ?[]u8 = null,
    owned_parix_provider_profile_path: ?[]u8 = null,
    owned_persistence_root: ?[]u8 = null,

    fn deinit(self: *Options, allocator: std.mem.Allocator) void {
        if (self.owned_host) |owned| allocator.free(owned);
        if (self.owned_parix_profile_path) |owned| allocator.free(owned);
        if (self.owned_ingress_auth_profile_path) |owned| allocator.free(owned);
        if (self.owned_parix_clearing_profile_path) |owned| allocator.free(owned);
        if (self.owned_parix_provider_profile_path) |owned| allocator.free(owned);
        if (self.owned_persistence_root) |owned| allocator.free(owned);
        self.* = undefined;
    }

    fn setIngressAuthProfilePath(
        self: *Options,
        allocator: std.mem.Allocator,
        path: []const u8,
    ) (std.mem.Allocator.Error || ArgumentError)!void {
        if (path.len == 0) return error.InvalidArguments;
        const replacement = try allocator.dupe(u8, path);
        if (self.owned_ingress_auth_profile_path) |owned| allocator.free(owned);
        self.owned_ingress_auth_profile_path = replacement;
        self.ingress_auth_profile_path = replacement;
    }

    fn setParixProfilePath(
        self: *Options,
        allocator: std.mem.Allocator,
        path: []const u8,
    ) (std.mem.Allocator.Error || ArgumentError)!void {
        if (path.len == 0) return error.InvalidArguments;
        const replacement = try allocator.dupe(u8, path);
        if (self.owned_parix_profile_path) |owned| allocator.free(owned);
        self.owned_parix_profile_path = replacement;
        self.parix_profile_path = replacement;
    }

    fn setParixClearingProfilePath(
        self: *Options,
        allocator: std.mem.Allocator,
        path: []const u8,
    ) (std.mem.Allocator.Error || ArgumentError)!void {
        if (path.len == 0) return error.InvalidArguments;
        const replacement = try allocator.dupe(u8, path);
        if (self.owned_parix_clearing_profile_path) |owned| allocator.free(owned);
        self.owned_parix_clearing_profile_path = replacement;
        self.parix_clearing_profile_path = replacement;
    }

    fn setParixProviderProfilePath(
        self: *Options,
        allocator: std.mem.Allocator,
        path: []const u8,
    ) (std.mem.Allocator.Error || ArgumentError)!void {
        if (path.len == 0) return error.InvalidArguments;
        const replacement = try allocator.dupe(u8, path);
        if (self.owned_parix_provider_profile_path) |owned| allocator.free(owned);
        self.owned_parix_provider_profile_path = replacement;
        self.parix_provider_profile_path = replacement;
    }

    fn setHost(
        self: *Options,
        allocator: std.mem.Allocator,
        host: []const u8,
    ) (std.mem.Allocator.Error || ArgumentError)!void {
        if (host.len == 0) return error.InvalidArguments;
        const replacement = try allocator.dupe(u8, host);
        if (self.owned_host) |owned| allocator.free(owned);
        self.owned_host = replacement;
        self.host = replacement;
    }

    fn setPersistenceRoot(
        self: *Options,
        allocator: std.mem.Allocator,
        persistence_root: []const u8,
    ) (std.mem.Allocator.Error || ArgumentError)!void {
        try validatePersistenceRoot(persistence_root);
        const replacement = try allocator.dupe(u8, persistence_root);
        if (self.owned_persistence_root) |owned| allocator.free(owned);
        self.owned_persistence_root = replacement;
        self.persistence_root = replacement;
    }
};

const ParseOutcome = union(enum) {
    help,
    options: Options,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var parsed = parseOptions(init.minimal.args, allocator) catch |err| {
        try writeArgumentError(init.io, err);
        return err;
    };

    switch (parsed) {
        .help => {
            var stdout_buffer: [2048]u8 = undefined;
            var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
            try writeUsage(&stdout_writer.interface);
            try stdout_writer.interface.flush();
            return;
        },
        .options => |*options| {
            defer options.deinit(allocator);
            try run(init.io, allocator, init.environ_map, options.*);
        },
    }
}

fn run(
    io: std.Io,
    allocator: std.mem.Allocator,
    environment: *const std.process.Environ.Map,
    options: Options,
) !void {
    try validateOptions(options);
    if (options.parix_settlement) {
        return runWithParixSettlement(io, allocator, environment, options);
    }
    return runInMemory(io, allocator, options);
}

fn runInMemory(io: std.Io, allocator: std.mem.Allocator, options: Options) !void {
    const engine = try allocator.create(core.exchange_core.ExchangeCore);
    defer allocator.destroy(engine);

    engine.* = try core.exchange_core.ExchangeCore.initWithIo(allocator, io, .{
        .matching = .{ .orderbook_kind = .direct },
    });
    defer engine.deinit();

    var api = core.exchange_api.ExchangeApi.init(allocator, engine);
    defer api.deinit();

    try engine.startup();
    defer engine.shutdown();

    try bootstrapDefaultSymbol(&api, allocator);

    try serve(io, allocator, options, &api, null, null, null, null, null, null);
}

fn runWithParixSettlement(
    io: std.Io,
    allocator: std.mem.Allocator,
    environment: *const std.process.Environ.Map,
    options: Options,
) !void {
    const profile_path = options.parix_profile_path orelse
        return error.MissingParixProfile;
    const configured_profile = try parix_profile.Profile.loadFile(
        allocator,
        io,
        profile_path,
    );
    defer configured_profile.deinit();

    const ingress_profile_path = options.ingress_auth_profile_path orelse
        return error.MissingIngressAuthProfile;
    const authenticated_ingress = try ingress_auth.Profile.load(
        io,
        allocator,
        environment,
        ingress_profile_path,
    );
    defer authenticated_ingress.deinit();

    const configured_clearing_profile = if (options.parix_clearing_profile_path) |path|
        try parix_clearing_profile.Profile.loadFile(
            allocator,
            io,
            path,
            configured_profile,
        )
    else
        null;
    defer if (configured_clearing_profile) |profile| profile.deinit();

    const configured_provider_profile = if (options.parix_provider_profile_path) |path|
        try parix_clearing_provider_adapter.Profile.load(
            io,
            allocator,
            environment,
            path,
        )
    else
        null;
    defer if (configured_provider_profile) |profile| profile.deinit();
    if ((configured_clearing_profile != null) != (configured_provider_profile != null)) {
        return error.ProviderProfileMismatch;
    }
    if (configured_clearing_profile) |clearing_profile| {
        if (configured_provider_profile.?.providerId() != clearing_profile.providerId()) {
            return error.ProviderProfileMismatch;
        }
    }

    const parix_environment = try readParixEnvironment(environment);
    try validateParixDatabaseId(parix_environment.database_id, configured_profile);
    try validateIngressProfile(
        authenticated_ingress,
        configured_profile,
        parix_environment.database_id,
    );
    const destination_id = configured_profile.destinationId();

    var gateway_client = try parix_http_transport.GatewayClient.init(
        allocator,
        io,
        .{
            .api_origin = parix_environment.base_url,
            .database_id = parix_environment.database_id,
            .expected_destination_id = destination_id,
            .auth = parix_environment.auth,
        },
    );
    var gateway_safe_to_deinit = true;
    defer {
        if (gateway_safe_to_deinit) {
            gateway_client.deinit();
        } else {
            log.err("Parix gateway retained because settlement cleanup was incomplete", .{});
        }
    }

    var settlement_runtime = try parix_settlement_runtime.Runtime.init(
        allocator,
        io,
        .{
            .persistence_root = options.persistence_root,
            .profile = configured_profile,
            .transport = gateway_client.asTransport(),
        },
    );
    gateway_safe_to_deinit = false;

    const engine = allocator.create(core.exchange_core.ExchangeCore) catch |err| {
        settlement_runtime.deinit() catch |cleanup_err| {
            log.err("Parix settlement cleanup failed: {s}", .{@errorName(cleanup_err)});
            return err;
        };
        gateway_safe_to_deinit = true;
        return err;
    };
    defer allocator.destroy(engine);

    engine.* = core.exchange_core.ExchangeCore.initWithIoAndPersistenceRoot(
        allocator,
        io,
        settlement_runtime.persistenceRoot(),
        .{
            .matching = .{ .orderbook_kind = .direct },
            .initial_state = core.config.InitialStateConfig.lastKnownStateFromJournal(
                "exchange-core",
                0,
                0,
            ),
            .serialization = .{
                .enable_journaling = true,
                .backend = .disk,
                .storage_folder = "core",
            },
        },
    ) catch |err| {
        settlement_runtime.deinit() catch |cleanup_err| {
            log.err("Parix settlement cleanup failed: {s}", .{@errorName(cleanup_err)});
            return err;
        };
        gateway_safe_to_deinit = true;
        return err;
    };

    var runtime_stopped = false;
    var runtime_detached = false;
    var engine_deinitialized = false;
    var runtime_deinitialized = false;
    defer {
        if (!engine_deinitialized and !engine.isStopped()) engine.shutdown();
        if (!runtime_stopped) {
            stop_attempt: {
                settlement_runtime.stopAndJoin() catch |err| {
                    log.err("Parix settlement shutdown failed: {s}", .{@errorName(err)});
                    switch (err) {
                        error.CoreStillRunning => break :stop_attempt,
                        error.DrainTimedOut,
                        error.SettlementFailed,
                        error.SettlementQuarantined,
                        => runtime_stopped = true,
                    }
                };
                if (!runtime_stopped) {
                    runtime_stopped = true;
                }
            }
        }
        if (runtime_stopped and !runtime_detached) {
            detach_attempt: {
                settlement_runtime.detachStoppedCore() catch |err| switch (err) {
                    error.NotInstalled => {},
                    else => {
                        log.err("Parix settlement detach failed: {s}", .{@errorName(err)});
                        break :detach_attempt;
                    },
                };
                runtime_detached = true;
            }
        }
        if (!engine_deinitialized) {
            engine.deinit();
            engine_deinitialized = true;
        }
        if (runtime_detached and !runtime_deinitialized) {
            runtime_deinit_attempt: {
                settlement_runtime.deinit() catch |err| {
                    log.err("Parix settlement cleanup failed: {s}", .{@errorName(err)});
                    break :runtime_deinit_attempt;
                };
                runtime_deinitialized = true;
                gateway_safe_to_deinit = true;
            }
        }
    }

    var api = core.exchange_api.ExchangeApi.init(allocator, engine);
    var api_deinitialized = false;
    defer if (!api_deinitialized) api.deinit();
    var funding_balance_context: FundingBalanceContext = .{
        .allocator = allocator,
        .api = &api,
    };
    var funding_runtime: parix_funding_runtime.Runtime = undefined;
    var funding_runtime_initialized = false;
    var funding_runtime_started = false;
    defer {
        if (funding_runtime_started) {
            funding_runtime.stopAndJoin() catch |err| {
                log.err("Parix funding shutdown failed: {s}", .{@errorName(err)});
            };
            funding_runtime_started = false;
        }
        if (funding_runtime_initialized) {
            funding_runtime.deinit() catch |err| {
                log.err("Parix funding cleanup failed: {s}", .{@errorName(err)});
            };
            funding_runtime_initialized = false;
        }
    }
    var funding_runtime_adapter: parix_clearing_runtime.FundingRuntimeAdapter = .{
        .runtime = &funding_runtime,
    };
    var clearing_runtime: parix_clearing_runtime.Runtime = undefined;
    var clearing_runtime_initialized = false;
    var clearing_runtime_started = false;
    defer {
        if (clearing_runtime_started) {
            clearing_runtime.stopAndJoin() catch |err| {
                log.err("Parix clearing shutdown failed: {s}", .{@errorName(err)});
            };
            clearing_runtime_started = false;
        }
        if (clearing_runtime_initialized) {
            clearing_runtime.deinit() catch |err| {
                log.err("Parix clearing cleanup failed: {s}", .{@errorName(err)});
            };
            clearing_runtime_initialized = false;
        }
    }

    try settlement_runtime.install(engine);
    _ = try engine.startupFromConfiguredPersistence(
        settlement_runtime.persistenceRoot(),
        .matching_engine_router,
        0,
        0,
    );

    try bootstrapParixLocalProfileProvisionally(&api, allocator, configured_profile);
    funding_runtime = try parix_funding_runtime.Runtime.init(
        allocator,
        io,
        .{
            .profile = configured_profile,
            .persistence_root = settlement_runtime.persistenceRoot(),
            .transport = gateway_client.asTransport(),
            .balance_port = funding_balance_context.asPort(),
            .open_policy = fundingOpenPolicy(options),
        },
    );
    funding_runtime_initialized = true;
    _ = try parix_account_bootstrap.ensureProfile(
        &gateway_client,
        configured_profile,
        parixBootstrapPolicy(options),
    );
    try settlement_runtime.start();
    switch (settlement_runtime.health()) {
        .failed => return error.ParixSettlementFailed,
        .quarantined => return error.ParixSettlementQuarantined,
        else => {},
    }
    try funding_runtime.start();
    funding_runtime_started = true;

    if (configured_clearing_profile) |clearing_profile| {
        // This is an explicit operator gate for the reviewed exact account-
        // lookup ceremony. The clearing profile intentionally does not pretend
        // that ID and ledger alone describe an exact account.
        if (!options.parix_clearing_accounts_verified) {
            return error.MissingClearingAccountAttestation;
        }
        clearing_runtime = try parix_clearing_runtime.Runtime.init(
            allocator,
            io,
            .{
                .profile = clearing_profile,
                .persistence_root = settlement_runtime.persistenceRoot(),
                .transport = gateway_client.asTransport(),
                .funding_port = funding_runtime_adapter.asPort(),
                .open_policy = clearingOpenPolicy(options),
            },
        );
        clearing_runtime_initialized = true;
        try clearing_runtime.start();
        clearing_runtime_started = true;
        switch (clearing_runtime.health()) {
            .ready, .pending => {},
            .failed => return error.ParixClearingFailed,
            .quarantined => return error.ParixClearingQuarantined,
            .cold, .stopped => return error.ParixClearingNotReady,
        }
    }

    try serve(
        io,
        allocator,
        options,
        &api,
        &settlement_runtime,
        &funding_runtime,
        configured_profile,
        authenticated_ingress,
        if (clearing_runtime_started) &clearing_runtime else null,
        if (configured_provider_profile) |profile| profile.adapter() else null,
    );

    if (clearing_runtime_started) {
        try clearing_runtime.stopAndJoin();
        clearing_runtime_started = false;
        try clearing_runtime.deinit();
        clearing_runtime_initialized = false;
    }
    try funding_runtime.stopAndJoin();
    funding_runtime_started = false;
    try funding_runtime.deinit();
    funding_runtime_initialized = false;
    engine.shutdown();
    var stop_failure: ?parix_settlement_runtime.StopError = null;
    settlement_runtime.stopAndJoin() catch |err| switch (err) {
        error.CoreStillRunning => return err,
        error.DrainTimedOut,
        error.SettlementFailed,
        error.SettlementQuarantined,
        => stop_failure = err,
    };
    runtime_stopped = true;
    try settlement_runtime.detachStoppedCore();
    runtime_detached = true;
    api.deinit();
    api_deinitialized = true;
    engine.deinit();
    engine_deinitialized = true;
    try settlement_runtime.deinit();
    runtime_deinitialized = true;
    gateway_safe_to_deinit = true;
    if (stop_failure) |err| return err;
}

fn serve(
    io: std.Io,
    allocator: std.mem.Allocator,
    options: Options,
    api: *core.exchange_api.ExchangeApi,
    settlement_runtime: ?*parix_settlement_runtime.Runtime,
    funding_runtime: ?*parix_funding_runtime.Runtime,
    configured_profile: ?*const parix_profile.Profile,
    authenticated_ingress: ?*const ingress_auth.Profile,
    clearing_runtime: ?*parix_clearing_runtime.Runtime,
    provider_adapter: ?parix_clearing_provider_adapter.Adapter,
) !void {
    std.debug.assert((settlement_runtime != null) == (configured_profile != null));
    std.debug.assert((settlement_runtime != null) == (authenticated_ingress != null));
    std.debug.assert((settlement_runtime != null) == (funding_runtime != null));
    std.debug.assert((clearing_runtime != null) == (provider_adapter != null));
    std.debug.assert(clearing_runtime == null or authenticated_ingress != null);
    const address = try std.Io.net.IpAddress.resolve(io, options.host, options.port);
    var server = try address.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    if (authenticated_ingress) |ingress_profile| {
        if (clearing_runtime) |runtime| {
            const provider_address = try std.Io.net.IpAddress.resolve(
                io,
                options.host,
                options.provider_port,
            );
            var provider_server = try provider_address.listen(io, .{ .reuse_address = true });
            defer provider_server.deinit(io);
            return serveAuthenticatedConnections(
                io,
                allocator,
                options,
                &server,
                api,
                settlement_runtime.?,
                funding_runtime.?,
                configured_profile.?,
                ingress_profile,
                runtime,
                provider_adapter.?,
                &provider_server,
            );
        }
        return serveAuthenticatedConnections(
            io,
            allocator,
            options,
            &server,
            api,
            settlement_runtime.?,
            funding_runtime.?,
            configured_profile.?,
            ingress_profile,
            null,
            null,
            null,
        );
    }

    try writeServerReady(
        io,
        options,
        api,
        settlement_runtime,
        funding_runtime,
        configured_profile,
        clearing_runtime,
        null,
    );

    while (true) {
        const stream = server.accept(io) catch |err| switch (err) {
            error.ConnectionAborted => continue,
            else => return err,
        };
        const shutdown_requested = serveLegacyConnection(
            io,
            allocator,
            stream,
            api,
            settlement_runtime,
            funding_runtime,
            configured_profile,
        ) catch |err| {
            log.warn("client connection failed: {s}", .{@errorName(err)});
            continue;
        };
        if (shutdown_requested) break;
    }
}

fn writeServerReady(
    io: std.Io,
    options: Options,
    api: *const core.exchange_api.ExchangeApi,
    settlement_runtime: ?*const parix_settlement_runtime.Runtime,
    funding_runtime: ?*const parix_funding_runtime.Runtime,
    configured_profile: ?*const parix_profile.Profile,
    clearing_runtime: ?*const parix_clearing_runtime.Runtime,
    provider_host: ?*const parix_clearing_http_ingress.Host,
) !void {
    std.debug.assert((clearing_runtime != null) == (provider_host != null));
    if (provider_host != null and !serverIsReady(
        api,
        settlement_runtime,
        funding_runtime,
        clearing_runtime,
        provider_host,
    )) {
        return error.ProviderIngressNotReady;
    }

    var stdout_buffer: [2048]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buffer);
    if (settlement_runtime) |runtime| {
        const symbol = configured_profile.?.symbolSpecification();
        if (clearing_runtime) |clearing| {
            try stdout_writer.interface.print(
                "exchange-core server ready on {s}:{d} symbol={d} base={d} quote={d} settlement={s} funding={s} clearing={s} provider_ingress={s} provider_port={d} ingress=authenticated max_sessions={d} receive_timeout_seconds={d}\n",
                .{
                    options.host,
                    options.port,
                    symbol.symbol_id,
                    symbol.base_currency,
                    symbol.quote_currency,
                    @tagName(runtime.health()),
                    @tagName(funding_runtime.?.health()),
                    @tagName(clearing.health()),
                    @tagName(provider_host.?.health()),
                    options.provider_port,
                    MaxAuthenticatedSessions,
                    AuthenticatedReceiveTimeoutSeconds,
                },
            );
        } else {
            try stdout_writer.interface.print(
                "exchange-core server ready on {s}:{d} symbol={d} base={d} quote={d} settlement={s} funding={s} ingress=authenticated max_sessions={d} receive_timeout_seconds={d}\n",
                .{
                    options.host,
                    options.port,
                    symbol.symbol_id,
                    symbol.base_currency,
                    symbol.quote_currency,
                    @tagName(runtime.health()),
                    @tagName(funding_runtime.?.health()),
                    MaxAuthenticatedSessions,
                    AuthenticatedReceiveTimeoutSeconds,
                },
            );
        }
    } else {
        try stdout_writer.interface.print(
            "exchange-core server ready on {s}:{d} symbol={d} base={d} quote={d}\n",
            .{
                options.host,
                options.port,
                DefaultSymbolId,
                DefaultBaseCurrency,
                DefaultQuoteCurrency,
            },
        );
    }
    try stdout_writer.interface.flush();
}

const AuthenticatedStreamQueue = std.Io.Queue(std.Io.net.Stream);

const AuthenticatedServerContext = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    api: *core.exchange_api.ExchangeApi,
    settlement_runtime: *parix_settlement_runtime.Runtime,
    funding_runtime: *parix_funding_runtime.Runtime,
    clearing_runtime: ?*parix_clearing_runtime.Runtime,
    provider_host: ?*parix_clearing_http_ingress.Host,
    configured_profile: *const parix_profile.Profile,
    authenticated_ingress: *const ingress_auth.Profile,
    execution_mutex: *std.Io.Mutex,
    session_slots: *std.Io.Semaphore,
    accepted_streams: *AuthenticatedStreamQueue,
    shutdown_requested: *std.atomic.Value(bool),
};

const AuthenticatedAcceptorContext = struct {
    io: std.Io,
    server: *std.Io.net.Server,
    accepted_streams: *AuthenticatedStreamQueue,
    session_slots: *std.Io.Semaphore,
    shutdown_requested: *std.atomic.Value(bool),
    accept_failed: *std.atomic.Value(bool),
};

const ProviderIngressFailureContext = struct {
    io: std.Io,
    accepted_streams: *AuthenticatedStreamQueue,
    shutdown_requested: *std.atomic.Value(bool),
    failed: *std.atomic.Value(bool),

    fn callback(self: *@This()) parix_clearing_http_ingress.FailureCallback {
        return .{ .context = self, .notify_fn = notify };
    }

    fn publish(self: *@This()) void {
        if (self.failed.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return;
        self.shutdown_requested.store(true, .release);
        self.accepted_streams.close(self.io);
    }

    fn notify(
        raw_context: *anyopaque,
        reason: parix_clearing_http_ingress.FailureReason,
    ) void {
        const self: *@This() = @ptrCast(@alignCast(raw_context));
        switch (reason) {
            .listener_failed => self.publish(),
        }
    }
};

const ProviderIngressServerContext = struct {
    host: *parix_clearing_http_ingress.Host,
    listener: *std.Io.net.Server,
    failure: *ProviderIngressFailureContext,
    shutdown_requested: *const std.atomic.Value(bool),
};

fn serveAuthenticatedConnections(
    io: std.Io,
    allocator: std.mem.Allocator,
    options: Options,
    server: *std.Io.net.Server,
    api: *core.exchange_api.ExchangeApi,
    settlement_runtime: *parix_settlement_runtime.Runtime,
    funding_runtime: *parix_funding_runtime.Runtime,
    configured_profile: *const parix_profile.Profile,
    authenticated_ingress: *const ingress_auth.Profile,
    clearing_runtime: ?*parix_clearing_runtime.Runtime,
    provider_adapter: ?parix_clearing_provider_adapter.Adapter,
    provider_listener: ?*std.Io.net.Server,
) !void {
    std.debug.assert((clearing_runtime != null) == (provider_adapter != null));
    std.debug.assert((clearing_runtime != null) == (provider_listener != null));
    var sessions: std.Io.Group = .init;
    errdefer sessions.cancel(io);
    var acceptors: std.Io.Group = .init;
    errdefer acceptors.cancel(io);
    var provider_tasks: std.Io.Group = .init;
    var execution_mutex: std.Io.Mutex = .init;
    var session_slots: std.Io.Semaphore = .{ .permits = MaxAuthenticatedSessions };
    var accepted_storage: [1]std.Io.net.Stream = undefined;
    var accepted_streams = AuthenticatedStreamQueue.init(&accepted_storage);
    errdefer accepted_streams.close(io);
    var shutdown_requested: std.atomic.Value(bool) = .init(false);
    var accept_failed: std.atomic.Value(bool) = .init(false);
    var provider_failed: std.atomic.Value(bool) = .init(false);
    var provider_failure_context: ProviderIngressFailureContext = .{
        .io = io,
        .accepted_streams = &accepted_streams,
        .shutdown_requested = &shutdown_requested,
        .failed = &provider_failed,
    };
    var clearing_admission_context: ClearingAdmissionContext = undefined;
    var provider_host: parix_clearing_http_ingress.Host = undefined;
    var provider_server_context: ProviderIngressServerContext = undefined;
    var provider_task_started = false;
    errdefer if (provider_task_started) {
        provider_host.requestStop();
        provider_tasks.await(io) catch {};
    };

    const provider_host_ptr: ?*parix_clearing_http_ingress.Host = if (clearing_runtime) |runtime| provider: {
        clearing_admission_context = .{ .runtime = runtime };
        provider_host = parix_clearing_http_ingress.Host.init(io, .{
            .handler = .{
                .adapter = provider_adapter.?,
                .admission = clearing_admission_context.asPort(),
            },
            .failure_callback = provider_failure_context.callback(),
        });
        provider_server_context = .{
            .host = &provider_host,
            .listener = provider_listener.?,
            .failure = &provider_failure_context,
            .shutdown_requested = &shutdown_requested,
        };
        try provider_tasks.concurrent(io, serveProviderIngress, .{&provider_server_context});
        provider_task_started = true;
        try waitForProviderIngressReady(io, &provider_host, &provider_failed);
        break :provider &provider_host;
    } else null;

    var context: AuthenticatedServerContext = .{
        .io = io,
        .allocator = allocator,
        .api = api,
        .settlement_runtime = settlement_runtime,
        .funding_runtime = funding_runtime,
        .clearing_runtime = clearing_runtime,
        .provider_host = provider_host_ptr,
        .configured_profile = configured_profile,
        .authenticated_ingress = authenticated_ingress,
        .execution_mutex = &execution_mutex,
        .session_slots = &session_slots,
        .accepted_streams = &accepted_streams,
        .shutdown_requested = &shutdown_requested,
    };
    var acceptor_context: AuthenticatedAcceptorContext = .{
        .io = io,
        .server = server,
        .accepted_streams = &accepted_streams,
        .session_slots = &session_slots,
        .shutdown_requested = &shutdown_requested,
        .accept_failed = &accept_failed,
    };
    try acceptors.concurrent(io, acceptAuthenticatedConnections, .{&acceptor_context});
    try writeServerReady(
        io,
        options,
        api,
        settlement_runtime,
        funding_runtime,
        configured_profile,
        clearing_runtime,
        provider_host_ptr,
    );

    while (true) {
        const stream = accepted_streams.getOne(io) catch |err| switch (err) {
            error.Closed => break,
            error.Canceled => return error.Canceled,
        };
        if (shutdown_requested.load(.acquire)) {
            stream.close(io);
            session_slots.post(io);
            break;
        }

        sessions.concurrent(
            io,
            serveAuthenticatedSession,
            .{ &context, stream },
        ) catch {
            stream.close(io);
            session_slots.post(io);
            log.warn("ingress_auth event=overloaded outcome=connection_closed", .{});
            continue;
        };
    }

    if (provider_host_ptr) |host| host.requestStop();
    acceptors.cancel(io);
    if (shutdown_requested.load(.acquire) or
        accept_failed.load(.acquire) or
        provider_failed.load(.acquire))
    {
        sessions.cancel(io);
    } else {
        try sessions.await(io);
    }
    if (provider_task_started) try provider_tasks.await(io);
    if (provider_failed.load(.acquire)) return error.ProviderIngressFailed;
    if (accept_failed.load(.acquire)) return error.AuthenticatedAcceptFailed;
}

fn serveProviderIngress(
    context: *ProviderIngressServerContext,
) std.Io.Cancelable!void {
    context.host.serve(context.listener) catch |err| {
        if (!context.shutdown_requested.load(.acquire)) {
            log.err("provider ingress stopped unexpectedly error={s}", .{@errorName(err)});
            context.failure.publish();
        }
    };
}

fn waitForProviderIngressReady(
    io: std.Io,
    host: *const parix_clearing_http_ingress.Host,
    provider_failed: *const std.atomic.Value(bool),
) !void {
    for (0..ProviderIngressStartupPollAttempts) |_| {
        if (provider_failed.load(.acquire)) return error.ProviderIngressFailed;
        switch (host.health()) {
            .cold => try std.Io.sleep(io, .fromMilliseconds(1), .awake),
            .ready => {
                if (!host.isAccepting()) return error.ProviderIngressNotReady;
                return;
            },
            .failed, .stopped => return error.ProviderIngressFailed,
        }
    }
    return error.ProviderIngressStartTimedOut;
}

fn acceptAuthenticatedConnections(
    context: *AuthenticatedAcceptorContext,
) std.Io.Cancelable!void {
    while (!context.shutdown_requested.load(.acquire)) {
        try context.session_slots.wait(context.io);
        if (context.shutdown_requested.load(.acquire)) {
            context.session_slots.post(context.io);
            return;
        }

        const stream = context.server.accept(context.io) catch |err| {
            context.session_slots.post(context.io);
            switch (err) {
                error.ConnectionAborted => continue,
                error.Canceled => return error.Canceled,
                else => {
                    context.accept_failed.store(true, .release);
                    log.err("authenticated listener accept failed error={s}", .{@errorName(err)});
                    context.accepted_streams.close(context.io);
                    return;
                },
            }
        };
        context.accepted_streams.putOne(context.io, stream) catch |err| {
            stream.close(context.io);
            context.session_slots.post(context.io);
            switch (err) {
                error.Closed => return,
                error.Canceled => return error.Canceled,
            }
        };
    }
}

fn serveAuthenticatedSession(
    context: *AuthenticatedServerContext,
    stream: std.Io.net.Stream,
) std.Io.Cancelable!void {
    defer context.session_slots.post(context.io);
    defer if (context.shutdown_requested.load(.acquire)) {
        context.accepted_streams.close(context.io);
    };
    const shutdown_requested = serveAuthenticatedConnection(
        context.io,
        context.allocator,
        stream,
        context.api,
        context.settlement_runtime,
        context.funding_runtime,
        context.clearing_runtime,
        context.provider_host,
        context.configured_profile,
        context.authenticated_ingress,
        context.execution_mutex,
        context.shutdown_requested,
    ) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => {
            log.warn("authenticated client connection failed error={s}", .{@errorName(err)});
            return;
        },
    };
    if (!shutdown_requested) return;

    context.accepted_streams.close(context.io);
}

fn serveAuthenticatedConnection(
    io: std.Io,
    allocator: std.mem.Allocator,
    stream: std.Io.net.Stream,
    api: *core.exchange_api.ExchangeApi,
    settlement_runtime: *parix_settlement_runtime.Runtime,
    funding_runtime: *parix_funding_runtime.Runtime,
    clearing_runtime: ?*parix_clearing_runtime.Runtime,
    provider_host: ?*parix_clearing_http_ingress.Host,
    configured_profile: *const parix_profile.Profile,
    authenticated_ingress: *const ingress_auth.Profile,
    execution_mutex: *std.Io.Mutex,
    shutdown_state: *std.atomic.Value(bool),
) !bool {
    defer stream.close(io);
    try installAuthenticatedReceiveTimeout(stream);

    var read_buffer: [ingress_auth.max_request_envelope_bytes + 1]u8 = undefined;
    var write_buffer: [MaxAuthenticatedWriteBufferBytes]u8 = undefined;
    var stream_reader = stream.reader(io, &read_buffer);
    var stream_writer = stream.writer(io, &write_buffer);
    const reader = &stream_reader.interface;
    const writer = &stream_writer.interface;
    var state: AuthSessionState = .awaiting_auth;

    const auth_line = takeAuthenticatedLine(reader) catch |err|
        return rejectAuthentication(writer, .awaiting_auth, err);
    const parsed_auth = ingress_auth.parseAuth(auth_line) catch |err|
        return rejectAuthentication(writer, .awaiting_auth, err);

    var server_nonce: ingress_auth.Nonce = undefined;
    io.randomSecure(&server_nonce) catch |err|
        return rejectAuthentication(writer, .awaiting_auth, err);
    const challenge: ingress_auth.Challenge = .{ .server_nonce = server_nonce };
    const transcript = ingress_auth.Transcript.init(parsed_auth, challenge);
    try state.advance(.awaiting_proof);
    try ingress_auth.writeChallenge(writer, challenge);
    try writer.flush();

    const proof_line = takeAuthenticatedLine(reader) catch |err|
        return rejectAuthentication(writer, .awaiting_proof, err);
    const proof = ingress_auth.parseProof(proof_line) catch |err|
        return rejectAuthentication(writer, .awaiting_proof, err);
    var authenticated = authenticated_ingress.verifyProof(
        transcript,
        proof.mac,
    ) catch |err| return rejectAuthentication(writer, .awaiting_proof, err);
    defer authenticated.deinit();
    try state.advance(.awaiting_request);

    {
        var server_mac = ingress_auth.computeServerMac(
            &authenticated.session_key,
            transcript,
        );
        defer std.crypto.secureZero(u8, &server_mac);
        try ingress_auth.writeAuthAck(writer, .{ .server_mac = server_mac });
    }
    try writer.flush();
    logIngressAuthenticationAccepted(authenticated.identity);

    const request_line = takeAuthenticatedLine(reader) catch |err|
        return rejectAuthentication(writer, .awaiting_request, err);
    const envelope = ingress_auth.parseRequestEnvelope(request_line) catch |err|
        return rejectAuthentication(writer, .awaiting_request, err);
    ingress_auth.verifyRequestMac(
        &authenticated.session_key,
        transcript.instance_id.slice(),
        transcript.credential_id.slice(),
        envelope.sequence,
        envelope.raw_command,
        envelope.request_mac,
    ) catch |err| return rejectAuthentication(writer, .awaiting_request, err);
    try state.advance(.finished);

    const request = protocol.parseLine(envelope.raw_command) catch |err| {
        logIngressCommandResult(authenticated.identity, null, "malformed");
        try writeSignedIngressError(
            writer,
            &authenticated,
            transcript,
            "protocol",
            @errorName(err),
        );
        return false;
    };
    const request_kind = ingress_auth.requestKind(request);
    ingress_auth.authorize(authenticated.identity.principal, request) catch |err| {
        logIngressAuthorizationDenied(authenticated.identity, request_kind, err);
        try writeSignedIngressError(
            writer,
            &authenticated,
            transcript,
            "authorization",
            @errorName(err),
        );
        return false;
    };
    logIngressAuthorizationAccepted(authenticated.identity, request_kind);

    var response_storage: [MaxResponseBytes]u8 = undefined;
    var response_writer = std.Io.Writer.fixed(&response_storage);
    const attempt: ExecutionAttempt = execution: {
        try execution_mutex.lock(io);
        defer execution_mutex.unlock(io);
        ensureAuthenticatedCommandAdmission(shutdown_state) catch |err|
            break :execution .{ .failed = err };
        const request_shutdown = executeRequest(
            allocator,
            api,
            settlement_runtime,
            funding_runtime,
            clearing_runtime,
            provider_host,
            configured_profile,
            request,
            &response_writer,
        ) catch |err| break :execution .{ .failed = err };
        if (request_shutdown) {
            markAuthenticatedShutdown(shutdown_state, provider_host);
        }
        break :execution .{ .completed = request_shutdown };
    };

    const shutdown_requested = switch (attempt) {
        .completed => |value| value,
        .failed => |err| failed: {
            response_writer = std.Io.Writer.fixed(&response_storage);
            try writeError(&response_writer, "execution", @errorName(err));
            break :failed false;
        },
    };
    const response = try stagedResponsePayload(response_writer.buffered());
    const outcome: []const u8 = switch (attempt) {
        .completed => "ok",
        .failed => "execution_error",
    };
    logIngressCommandResult(authenticated.identity, request_kind, outcome);
    try writeSignedIngressResponse(
        writer,
        &authenticated,
        transcript,
        response,
    );
    return shutdown_requested;
}

fn ensureAuthenticatedCommandAdmission(
    shutdown_state: *const std.atomic.Value(bool),
) error{ServerShuttingDown}!void {
    if (shutdown_state.load(.acquire)) return error.ServerShuttingDown;
}

fn markAuthenticatedShutdown(
    shutdown_state: *std.atomic.Value(bool),
    provider_host: ?*parix_clearing_http_ingress.Host,
) void {
    // The provider fence is part of admitting the serialized shutdown command,
    // not deferred to coordinator wakeup. Once the raw shutdown state becomes
    // visible, no new provider session can acquire an admission lease.
    if (provider_host) |host| host.requestStop();
    shutdown_state.store(true, .release);
}

fn takeAuthenticatedLine(reader: *std.Io.Reader) ![]const u8 {
    return (try reader.takeDelimiter('\n')) orelse error.EndOfStream;
}

fn rejectAuthentication(
    writer: *std.Io.Writer,
    state: AuthSessionState,
    err: anyerror,
) !bool {
    log.warn(
        "ingress_auth event=rejected stage={s} reason={s}",
        .{ @tagName(state), @errorName(err) },
    );
    try ingress_auth.writeAuthenticationFailure(writer);
    try writer.flush();
    return false;
}

fn writeSignedIngressError(
    writer: *std.Io.Writer,
    authenticated: *const ingress_auth.Authenticated,
    transcript: ingress_auth.Transcript,
    kind: []const u8,
    message: []const u8,
) !void {
    var response_storage: [MaxResponseBytes]u8 = undefined;
    var response_writer = std.Io.Writer.fixed(&response_storage);
    try writeError(&response_writer, kind, message);
    try writeSignedIngressResponse(
        writer,
        authenticated,
        transcript,
        try stagedResponsePayload(response_writer.buffered()),
    );
}

fn writeSignedIngressResponse(
    writer: *std.Io.Writer,
    authenticated: *const ingress_auth.Authenticated,
    transcript: ingress_auth.Transcript,
    raw_response: []const u8,
) !void {
    var response_mac = try ingress_auth.computeResponseMac(
        &authenticated.session_key,
        transcript.instance_id.slice(),
        transcript.credential_id.slice(),
        ingress_auth.sequence_one,
        raw_response,
    );
    defer std.crypto.secureZero(u8, &response_mac);
    try ingress_auth.writeResponseEnvelope(writer, .{
        .sequence = ingress_auth.sequence_one,
        .response_mac = response_mac,
        .raw_response = raw_response,
    });
    try writer.flush();
}

fn stagedResponsePayload(response: []const u8) error{InvalidStagedResponse}![]const u8 {
    if (response.len == 0 or response[response.len - 1] != '\n') {
        return error.InvalidStagedResponse;
    }
    const payload = response[0 .. response.len - 1];
    if (std.mem.findScalar(u8, payload, '\n') != null or
        std.mem.findScalar(u8, payload, '\r') != null)
    {
        return error.InvalidStagedResponse;
    }
    return payload;
}

fn logIngressAuthenticationAccepted(identity: ingress_auth.Identity) void {
    log.info(
        "ingress_auth event=accepted credential={s} principal={s} role={s}",
        .{
            identity.credential_id.slice(),
            identity.principal_id.slice(),
            @tagName(identity.principal),
        },
    );
}

fn logIngressAuthorizationDenied(
    identity: ingress_auth.Identity,
    request_kind: ingress_auth.RequestKind,
    err: anyerror,
) void {
    log.warn(
        "ingress_auth event=authorization_denied credential={s} principal={s} request={s} reason={s}",
        .{
            identity.credential_id.slice(),
            identity.principal_id.slice(),
            @tagName(request_kind),
            @errorName(err),
        },
    );
}

fn logIngressAuthorizationAccepted(
    identity: ingress_auth.Identity,
    request_kind: ingress_auth.RequestKind,
) void {
    log.info(
        "ingress_auth event=authorization_allowed credential={s} principal={s} request={s}",
        .{
            identity.credential_id.slice(),
            identity.principal_id.slice(),
            @tagName(request_kind),
        },
    );
}

fn logIngressCommandResult(
    identity: ingress_auth.Identity,
    request_kind: ?ingress_auth.RequestKind,
    outcome: []const u8,
) void {
    log.info(
        "ingress_auth event=command_result credential={s} principal={s} request={s} outcome={s}",
        .{
            identity.credential_id.slice(),
            identity.principal_id.slice(),
            if (request_kind) |kind| @tagName(kind) else "invalid",
            outcome,
        },
    );
}

fn installAuthenticatedReceiveTimeout(
    stream: std.Io.net.Stream,
) (std.posix.SetSockOptError || error{AuthenticatedReceiveTimeoutUnsupported})!void {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .wasi) {
        return error.AuthenticatedReceiveTimeoutUnsupported;
    }

    const timeout: std.posix.timeval = .{
        .sec = AuthenticatedReceiveTimeoutSeconds,
        .usec = 0,
    };
    try std.posix.setsockopt(
        stream.socket.handle,
        std.posix.SOL.SOCKET,
        std.posix.SO.RCVTIMEO,
        std.mem.asBytes(&timeout),
    );
}

fn serveLegacyConnection(
    io: std.Io,
    allocator: std.mem.Allocator,
    stream: std.Io.net.Stream,
    api: *core.exchange_api.ExchangeApi,
    settlement_runtime: ?*parix_settlement_runtime.Runtime,
    funding_runtime: ?*parix_funding_runtime.Runtime,
    configured_profile: ?*const parix_profile.Profile,
) !bool {
    defer stream.close(io);

    var read_buffer: [MaxRequestLineBytes]u8 = undefined;
    var write_buffer: [4096]u8 = undefined;
    var stream_reader = stream.reader(io, &read_buffer);
    var stream_writer = stream.writer(io, &write_buffer);
    const reader = &stream_reader.interface;
    const writer = &stream_writer.interface;

    while (true) {
        const line_optional = reader.takeDelimiter('\n') catch |err| switch (err) {
            error.StreamTooLong => {
                const discarded: ?usize = reader.discardDelimiterInclusive('\n') catch |discard_err| switch (discard_err) {
                    error.EndOfStream => null,
                    error.ReadFailed => return error.ReadFailed,
                };
                try writeError(writer, "protocol", "RequestLineTooLong");
                try writer.flush();
                if (discarded == null) return false;
                continue;
            },
            error.ReadFailed => return error.ReadFailed,
        };
        const raw_line = line_optional orelse return false;
        const line = std.mem.trimEnd(u8, raw_line, "\r");
        const request = protocol.parseLine(line) catch |err| {
            try writeError(writer, "protocol", @errorName(err));
            try writer.flush();
            continue;
        };

        const shutdown_requested = executeRequest(
            allocator,
            api,
            settlement_runtime,
            funding_runtime,
            null,
            null,
            configured_profile,
            request,
            writer,
        ) catch |err| {
            try writeError(writer, "execution", @errorName(err));
            try writer.flush();
            continue;
        };
        try writer.flush();
        if (shutdown_requested) return true;
    }
}

fn executeRequest(
    allocator: std.mem.Allocator,
    api: *core.exchange_api.ExchangeApi,
    settlement_runtime: ?*parix_settlement_runtime.Runtime,
    funding_runtime: ?*parix_funding_runtime.Runtime,
    clearing_runtime: ?*parix_clearing_runtime.Runtime,
    provider_host: ?*parix_clearing_http_ingress.Host,
    configured_profile: ?*const parix_profile.Profile,
    request: protocol.Request,
    writer: *std.Io.Writer,
) !bool {
    switch (request) {
        .ping => {
            const ready = if (serverIsReady(
                api,
                settlement_runtime,
                funding_runtime,
                clearing_runtime,
                provider_host,
            )) "true" else "false";
            if (settlement_runtime) |runtime| {
                try writer.print(
                    "OK ready={s} settlement={s} funding={s}\n",
                    .{
                        ready,
                        @tagName(runtime.health()),
                        @tagName(funding_runtime.?.health()),
                    },
                );
            } else {
                try writer.print("OK ready={s}\n", .{ready});
            }
            return false;
        },
        .fund_deposit => |command| {
            try executeFundingSubmit(funding_runtime, .deposit, command, writer);
            return false;
        },
        .fund_withdraw => |command| {
            try executeFundingSubmit(funding_runtime, .withdrawal, command, writer);
            return false;
        },
        .funding_status => |operation_id| {
            try executeFundingStatus(funding_runtime, operation_id, writer);
            return false;
        },
        .clearing_status => |payment_id| {
            try executeClearingStatus(clearing_runtime, payment_id, writer);
            return false;
        },
        .clearing_operations => {
            try executeClearingOperations(clearing_runtime, writer);
            return false;
        },
        .balance => |query| {
            try settleReadBarrier(api, allocator);
            const balance = try api.queryUserBalance(query.uid, query.currency);
            try writer.print(
                "OK balance uid={d} currency={d} value=",
                .{ query.uid, query.currency },
            );
            if (balance) |value| {
                try writer.print("{d}\n", .{value});
            } else {
                try writer.writeAll("null\n");
            }
            return false;
        },
        .book => |query| {
            try settleReadBarrier(api, allocator);
            var snapshot = try api.queryOrderBook(query.symbol, query.depth);
            defer snapshot.deinit(allocator);

            try writer.print(
                "OK book symbol={d} asks={d} bids={d} best_ask=",
                .{ query.symbol, snapshot.asks.items.len, snapshot.bids.items.len },
            );
            if (snapshot.asks.items.len == 0) {
                try writer.writeAll("null");
            } else {
                try writer.print("{d}", .{snapshot.asks.items[0].price});
            }
            try writer.writeAll(" best_bid=");
            if (snapshot.bids.items.len == 0) {
                try writer.writeAll("null\n");
            } else {
                try writer.print("{d}\n", .{snapshot.bids.items[0].price});
            }
            return false;
        },
        .shutdown => {
            try writer.writeAll("OK shutdown\n");
            return true;
        },
        else => {
            var result = try submitMutationRequest(
                api,
                settlement_runtime,
                funding_runtime,
                configured_profile,
                request,
            );
            defer result.deinit(allocator);
            try writeCommandResult(writer, result);
            return false;
        },
    }
}

fn executeFundingSubmit(
    funding_runtime: ?*parix_funding_runtime.Runtime,
    kind: parix_funding_saga.Kind,
    command: protocol.FundingMutation,
    writer: *std.Io.Writer,
) std.Io.Writer.Error!void {
    const runtime = funding_runtime orelse {
        try writeError(writer, "funding", "FundingUnavailable");
        return;
    };
    const accepted = runtime.submit(
        kind,
        command.operation_id.value,
        command.uid,
        command.currency,
        command.amount,
    ) catch |err| {
        try writeError(writer, "funding", fundingErrorMessage(err));
        return;
    };
    try writeFundingStatus(writer, accepted.status);
}

fn executeFundingStatus(
    funding_runtime: ?*parix_funding_runtime.Runtime,
    operation_id: protocol.FundingOperationId,
    writer: *std.Io.Writer,
) std.Io.Writer.Error!void {
    const runtime = funding_runtime orelse {
        try writeError(writer, "funding", "FundingUnavailable");
        return;
    };
    const status = runtime.status(operation_id.value) catch |err| {
        try writeError(writer, "funding", fundingErrorMessage(err));
        return;
    };
    try writeFundingStatus(writer, status);
}

fn writeFundingStatus(
    writer: *std.Io.Writer,
    status: parix_funding_saga.Status,
) std.Io.Writer.Error!void {
    try writer.writeAll("OK funding operation=");
    try (protocol.FundingOperationId{ .value = status.request.operation_id }).write(writer);
    try writer.print(
        " kind={s} uid={d} currency={d} amount={d} state={s} reason={s}\n",
        .{
            @tagName(status.request.kind),
            status.request.uid,
            status.request.currency,
            status.request.amount,
            @tagName(status.state),
            fundingReason(status),
        },
    );
}

fn fundingReason(status: parix_funding_saga.Status) []const u8 {
    if (status.failure_reason) |reason| return @tagName(reason);
    if (status.quarantine_reason) |reason| return @tagName(reason);
    return "none";
}

fn executeClearingStatus(
    clearing_runtime: ?*parix_clearing_runtime.Runtime,
    payment_id: u128,
    writer: *std.Io.Writer,
) std.Io.Writer.Error!void {
    const runtime = clearing_runtime orelse {
        try writeError(writer, "clearing", "ClearingUnavailable");
        return;
    };
    const status = runtime.status(payment_id) catch |err| {
        try writeError(writer, "clearing", clearingErrorMessage(err));
        return;
    };
    try writeClearingStatus(writer, status);
}

fn writeClearingStatus(
    writer: *std.Io.Writer,
    status: parix_clearing_saga.Status,
) std.Io.Writer.Error!void {
    try writer.print(
        "OK clearing payment={d} state={s} authorization_event={d} " ++
            "authorization_revision={d} expires_at={d} resolution=",
        .{
            status.authorization.payment_id,
            @tagName(status.state),
            status.authorization.event_id,
            status.authorization.revision,
            status.expires_at_unix_seconds,
        },
    );
    if (status.resolution) |resolution| {
        try writer.print(
            "{s} resolution_event={d} resolution_revision={d} resolution_source={s}",
            .{
                @tagName(resolution.kind),
                resolution.event_id,
                resolution.revision,
                if (status.resolution_source) |source| @tagName(source) else "unknown",
            },
        );
    } else {
        try writer.writeAll(
            "none resolution_event=0 resolution_revision=0 resolution_source=none",
        );
    }
    try writer.print(" reason={s}\n", .{clearingReason(status)});
}

fn executeClearingOperations(
    clearing_runtime: ?*parix_clearing_runtime.Runtime,
    writer: *std.Io.Writer,
) std.Io.Writer.Error!void {
    const runtime = clearing_runtime orelse {
        try writeError(writer, "clearing", "ClearingUnavailable");
        return;
    };
    const report = runtime.operationsReport() catch |err| {
        try writeError(writer, "clearing", clearingErrorMessage(err));
        return;
    };
    try writeClearingOperations(writer, report);
}

fn writeClearingOperations(
    writer: *std.Io.Writer,
    report: parix_clearing_runtime.OperationsReport,
) std.Io.Writer.Error!void {
    const durable = report.durable;
    try writer.print(
        "OK clearing-operations health={s} observed_at={d} capacity={d} total={d} " ++
            "accepted={d} pending={d} authorized={d} completed={d} failed={d} " ++
            "quarantined={d} ready={d} in_flight={d} remote_reconcile={d} " ++
            "funding_reconcile={d} provider_resolutions={d} expiration_resolutions={d} " ++
            "remote_attempts={d} funding_attempts={d} expired_unresolved={d} " ++
            "legacy_manual_expiry={d} max_deadline_lag={d} oldest_open_age={d} " ++
            "oldest_expired_payment={d} first_reconciliation_payment={d} " ++
            "first_failed_payment={d} first_quarantined_payment={d}\n",
        .{
            @tagName(report.health),
            durable.observed_at_unix_seconds,
            durable.capacity,
            durable.total,
            durable.states.accepted,
            durable.states.pending,
            durable.states.authorized,
            durable.states.completed,
            durable.states.failed,
            durable.states.quarantined,
            durable.ready_actions,
            durable.in_flight_actions,
            durable.remote_reconciliations,
            durable.funding_reconciliations,
            durable.provider_resolutions,
            durable.expiration_resolutions,
            durable.remote_attempts,
            durable.funding_attempts,
            durable.expired_unresolved,
            durable.legacy_manual_expiry,
            durable.max_deadline_lag_seconds,
            durable.oldest_open_age_seconds,
            durable.oldest_expired_payment_id orelse 0,
            durable.first_reconciliation_payment_id orelse 0,
            durable.first_failed_payment_id orelse 0,
            durable.first_quarantined_payment_id orelse 0,
        },
    );
}

fn clearingReason(status: parix_clearing_saga.Status) []const u8 {
    if (status.failure_reason) |reason| return @tagName(reason);
    if (status.quarantine_reason) |reason| return @tagName(reason);
    return "none";
}

fn clearingErrorMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.PaymentNotFound => "ClearingNotFound",
        error.Poisoned,
        error.StorageReadFailed,
        error.StorageAmbiguous,
        => "ClearingUnavailable",
        else => "ClearingFailed",
    };
}

fn fundingErrorMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.NotFound => "FundingNotFound",
        error.IdempotencyConflict => "IdempotencyConflict",
        error.CapacityExceeded => "FundingCapacityExceeded",
        error.RuntimeNotRunning,
        error.RuntimeFailed,
        error.RuntimeQuarantined,
        error.Poisoned,
        error.StorageReadFailed,
        error.StorageAmbiguous,
        => "FundingUnavailable",
        else => "FundingFailed",
    };
}

fn serverIsReady(
    api: *const core.exchange_api.ExchangeApi,
    settlement_runtime: ?*const parix_settlement_runtime.Runtime,
    funding_runtime: ?*const parix_funding_runtime.Runtime,
    clearing_runtime: ?*const parix_clearing_runtime.Runtime,
    provider_host: ?*const parix_clearing_http_ingress.Host,
) bool {
    if (!api.isStarted() or api.isStopped()) return false;
    const runtime = settlement_runtime orelse return funding_runtime == null and
        clearing_runtime == null and provider_host == null;
    if (runtime.health() != .ready) return false;
    const funding = funding_runtime orelse return false;
    if (!fundingHealthIsReady(funding.health())) return false;
    return clearingIngressHealthIsReady(
        if (clearing_runtime) |clearing| clearing.health() else null,
        if (provider_host) |host| host.health() else null,
        if (provider_host) |host| host.isAccepting() else false,
    );
}

fn clearingIngressHealthIsReady(
    clearing_health: ?parix_clearing_runtime.Health,
    provider_health: ?parix_clearing_http_ingress.Health,
    provider_accepting: bool,
) bool {
    if (clearing_health == null or provider_health == null) {
        return clearing_health == null and provider_health == null;
    }
    const clearing_ready = switch (clearing_health.?) {
        .ready, .pending => true,
        .cold, .failed, .quarantined, .stopped => false,
    };
    return clearing_ready and provider_health.? == .ready and provider_accepting;
}

fn fundingHealthIsReady(health: parix_funding_runtime.Health) bool {
    return switch (health) {
        .ready, .pending => true,
        .cold, .failed, .quarantined, .stopped => false,
    };
}

fn submitMutationRequest(
    api: *core.exchange_api.ExchangeApi,
    settlement_runtime: ?*parix_settlement_runtime.Runtime,
    funding_runtime: ?*parix_funding_runtime.Runtime,
    configured_profile: ?*const parix_profile.Profile,
    request: protocol.Request,
) !core.api_command.ApiCommandResult {
    var mutation_runtime: ?*parix_settlement_runtime.Runtime = null;
    defer if (mutation_runtime) |runtime| runtime.endMutation();

    if (settlement_runtime) |runtime| {
        const profile = configured_profile orelse
            return error.ParixProfileUnavailable;
        try validateParixRequest(profile, request);
        const funding = funding_runtime orelse return error.ParixFundingNotReady;
        if (!fundingHealthAllowsCoreMutation(funding.health())) {
            return error.ParixFundingNotReady;
        }
        if (!runtime.tryBeginMutation()) return error.ParixSettlementNotReady;
        mutation_runtime = runtime;
    }
    return api.submitCommandSync(try apiCommandForRequest(request));
}

fn fundingHealthAllowsCoreMutation(health: parix_funding_runtime.Health) bool {
    return switch (health) {
        .ready, .pending => true,
        .cold, .failed, .quarantined, .stopped => false,
    };
}

fn validateParixRequest(
    configured_profile: *const parix_profile.Profile,
    request: protocol.Request,
) ParixRequestError!void {
    switch (request) {
        .deposit => return error.ParixDepositUnsupported,
        .add_user => |command| try validateParixMutation(configured_profile, command.uid, null),
        .place => |command| try validateParixMutation(configured_profile, command.uid, command.symbol),
        .move => |command| try validateParixMutation(configured_profile, command.uid, command.symbol),
        .reduce => |command| try validateParixMutation(configured_profile, command.uid, command.symbol),
        .cancel => |command| try validateParixMutation(configured_profile, command.uid, command.symbol),
        .ping,
        .fund_deposit,
        .fund_withdraw,
        .funding_status,
        .clearing_status,
        .clearing_operations,
        .balance,
        .book,
        .shutdown,
        => return,
    }
}

fn validateParixMutation(
    configured_profile: *const parix_profile.Profile,
    uid: i64,
    symbol: ?i32,
) ParixRequestError!void {
    if (!configured_profile.isConfiguredUser(uid)) {
        return error.UnknownParixProfileUser;
    }
    if (symbol) |symbol_id| {
        if (symbol_id != configured_profile.symbolSpecification().symbol_id) {
            return error.UnsupportedParixSymbol;
        }
    }
}

fn readParixEnvironment(
    environment: *const std.process.Environ.Map,
) ParixEnvironmentError!ParixEnvironment {
    const database_id = nonEmptyEnvironmentValue(
        environment.get("PARIX_DATABASE_ID"),
    ) orelse return error.MissingParixDatabaseId;
    const base_url = nonEmptyEnvironmentValue(
        environment.get("PARIX_BASE_URL"),
    ) orelse DefaultParixBaseUrl;
    const api_key = nonEmptyEnvironmentValue(environment.get("PARIX_API_KEY"));
    const bearer = nonEmptyEnvironmentValue(environment.get("PARIX_BEARER_TOKEN"));

    if (api_key != null and bearer != null) return error.MultipleParixCredentials;
    const auth: parix_http_transport.Auth = if (api_key) |secret|
        .{ .api_key = secret }
    else if (bearer) |secret|
        .{ .bearer = secret }
    else
        return error.MissingParixCredential;

    return .{
        .base_url = base_url,
        .database_id = database_id,
        .auth = auth,
    };
}

fn validateParixDatabaseId(
    database_id: []const u8,
    configured_profile: *const parix_profile.Profile,
) ParixEnvironmentError!void {
    if (!std.mem.eql(u8, database_id, configured_profile.databaseUuid())) {
        return error.ParixDatabaseIdMismatch;
    }
}

fn validateIngressProfile(
    authenticated_ingress: *const ingress_auth.Profile,
    configured_profile: *const parix_profile.Profile,
    parix_database_id: []const u8,
) IngressProfileError!void {
    if (!std.mem.eql(
        u8,
        authenticated_ingress.instanceId(),
        configured_profile.databaseUuid(),
    ) or !std.mem.eql(
        u8,
        authenticated_ingress.instanceId(),
        parix_database_id,
    )) return error.IngressInstanceMismatch;

    for (0..authenticated_ingress.credentialCount()) |index| {
        const identity = authenticated_ingress.identityAt(index) orelse unreachable;
        switch (identity.principal) {
            .operator => {},
            .trader => |uid| {
                if (!configured_profile.isConfiguredUser(uid)) {
                    return error.IngressTraderMissingFromSettlement;
                }
            },
        }
    }
}

fn parixBootstrapPolicy(options: Options) parix_account_bootstrap.Policy {
    return .{
        .allow_account_creation = options.parix_create_accounts,
        .allow_initial_funding = options.parix_development_seed,
    };
}

fn fundingOpenPolicy(options: Options) parix_funding_runtime.OpenPolicy {
    return if (options.parix_funding_initialize) .initialize else .open_existing;
}

fn clearingOpenPolicy(options: Options) parix_clearing_runtime.OpenPolicy {
    return if (options.parix_clearing_initialize) .initialize else .open_existing;
}

fn nonEmptyEnvironmentValue(value: ?[]const u8) ?[]const u8 {
    const present = value orelse return null;
    return if (present.len == 0) null else present;
}

/// A completed matching response can precede the risk-release stage that
/// publishes post-trade balances. A settlement-safe NOP places both balance
/// and order-book reads after every stage of the preceding command stream.
fn settleReadBarrier(
    api: *core.exchange_api.ExchangeApi,
    allocator: std.mem.Allocator,
) !void {
    var result = try api.submitCommandSync(.nop);
    defer result.deinit(allocator);
    const result_code = switch (result) {
        .response => |response| response.result_code,
        .persist_state => return error.UnexpectedReadBarrierResult,
    };
    if (result_code != .success) return error.ReadBarrierRejected;
}

fn apiCommandForRequest(request: protocol.Request) RequestMapError!core.api_command.ApiCommand {
    return switch (request) {
        .add_user => |command| .{ .add_user = command.uid },
        .deposit => |command| .{
            .adjust_user_balance = .{
                .uid = command.uid,
                .transaction_id = command.transaction_id,
                .currency = command.currency,
                .amount = command.amount,
                .adjustment_type = .adjustment,
            },
        },
        .place => |command| .{
            .place_order = .{
                .order_id = command.order_id,
                .uid = command.uid,
                .symbol = command.symbol,
                .price = command.price,
                .reserve_bid_price = command.price,
                .size = command.size,
                .action = switch (command.side) {
                    .bid => .bid,
                    .ask => .ask,
                },
                .order_type = switch (command.time_in_force) {
                    .gtc => .gtc,
                    .ioc => .ioc,
                },
                .user_cookie = 0,
            },
        },
        .move => |command| .{
            .move_order = .{
                .order_id = command.order_id,
                .uid = command.uid,
                .symbol = command.symbol,
                .new_price = command.new_price,
            },
        },
        .reduce => |command| .{
            .reduce_order = .{
                .order_id = command.order_id,
                .uid = command.uid,
                .symbol = command.symbol,
                .reduce_size = command.size,
            },
        },
        .cancel => |command| .{
            .cancel_order = .{
                .order_id = command.order_id,
                .uid = command.uid,
                .symbol = command.symbol,
            },
        },
        .ping,
        .fund_deposit,
        .fund_withdraw,
        .funding_status,
        .clearing_status,
        .clearing_operations,
        .balance,
        .book,
        .shutdown,
        => error.NonCommandRequest,
    };
}

fn writeCommandResult(
    writer: *std.Io.Writer,
    result: core.api_command.ApiCommandResult,
) std.Io.Writer.Error!void {
    switch (result) {
        .response => |response| try writer.print(
            "OK tag=response code={d} sequence={d} events={d}\n",
            .{
                response.result_code.code(),
                response.sequence,
                core.events.listSize(response.matcher_events_head),
            },
        ),
        .persist_state => |result_code| try writer.print(
            "OK tag=persist_state code={d} sequence=0 events=0\n",
            .{result_code.code()},
        ),
    }
}

fn writeError(
    writer: *std.Io.Writer,
    kind: []const u8,
    message: []const u8,
) std.Io.Writer.Error!void {
    try writer.print("ERR kind={s} message={s}\n", .{ kind, message });
}

fn bootstrapDefaultSymbol(
    api: *core.exchange_api.ExchangeApi,
    allocator: std.mem.Allocator,
) !void {
    try bootstrapSymbol(api, allocator, try defaultSymbolSpecification());
}

fn bootstrapSymbol(
    api: *core.exchange_api.ExchangeApi,
    allocator: std.mem.Allocator,
    symbol: core.common.CoreSymbolSpecification,
) !void {
    var result = try api.submitCommandSync(.{
        .binary_add_symbols = .{
            .transfer_id = BootstrapTransferId,
            .timestamp = 0,
            .symbols = &.{symbol},
        },
    });
    defer result.deinit(allocator);

    const result_code = switch (result) {
        .response => |response| response.result_code,
        .persist_state => return error.UnexpectedBootstrapResult,
    };
    if (result_code != .success and
        result_code != .symbol_mgmt_symbol_already_exists)
    {
        return error.DefaultSymbolBootstrapRejected;
    }
}

/// Replays immutable profile bootstrap commands. An advanced balance counter is
/// admitted only provisionally: production startup must call `Runtime.init`
/// immediately afterward so FSG/core alignment is proven before side effects.
fn bootstrapParixLocalProfileProvisionally(
    api: *core.exchange_api.ExchangeApi,
    allocator: std.mem.Allocator,
    configured_profile: *const parix_profile.Profile,
) !void {
    try bootstrapSymbol(api, allocator, configured_profile.symbolSpecification());
    for (configured_profile.users()) |user| {
        try bootstrapParixUser(api, allocator, user.uid);
    }
    for (configured_profile.initialFunding()) |funding| {
        try bootstrapParixBalance(
            api,
            allocator,
            funding.uid,
            funding.currency,
            @intCast(funding.amount),
            @intCast(funding.id),
        );
    }
}

fn bootstrapParixUser(
    api: *core.exchange_api.ExchangeApi,
    allocator: std.mem.Allocator,
    uid: i64,
) !void {
    var result = try api.submitCommandSync(.{ .add_user = uid });
    defer result.deinit(allocator);

    const result_code = switch (result) {
        .response => |response| response.result_code,
        .persist_state => return error.UnexpectedBootstrapResult,
    };
    if (result_code != .success and
        result_code != .user_mgmt_user_already_exists)
    {
        return error.ParixUserBootstrapRejected;
    }
}

fn bootstrapParixBalance(
    api: *core.exchange_api.ExchangeApi,
    allocator: std.mem.Allocator,
    uid: i64,
    currency: i32,
    amount: i64,
    transaction_id: i64,
) !void {
    var result = try api.submitCommandSync(.{ .adjust_user_balance = .{
        .uid = uid,
        .transaction_id = transaction_id,
        .currency = currency,
        .amount = amount,
        .adjustment_type = .adjustment,
    } });
    defer result.deinit(allocator);

    const result_code = switch (result) {
        .response => |response| response.result_code,
        .persist_state => return error.UnexpectedBootstrapResult,
    };
    // `already_applied_many` is only provisional restart admission. The Parix
    // startup path must immediately open the destination-bound FSG store and
    // validate the live core counter against its phase-aware saga state before
    // either worker or ingress can start.
    if (result_code != .success and
        result_code != .user_mgmt_account_balance_adjustment_already_applied_same and
        result_code != .user_mgmt_account_balance_adjustment_already_applied_many)
    {
        return error.ParixBalanceBootstrapRejected;
    }
}

fn defaultSymbolSpecification() core.common.CoreSymbolSpecification.ValidationError!core.common.CoreSymbolSpecification {
    return core.common.CoreSymbolSpecification.init(.{
        .symbol_id = DefaultSymbolId,
        .symbol_type = .currency_exchange_pair,
        .base_currency = DefaultBaseCurrency,
        .quote_currency = DefaultQuoteCurrency,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    });
}

fn parseOptions(
    process_args: std.process.Args,
    allocator: std.mem.Allocator,
) (std.process.Args.Iterator.InitError || std.mem.Allocator.Error || ArgumentError)!ParseOutcome {
    var args = try std.process.Args.Iterator.initAllocator(process_args, allocator);
    defer args.deinit();
    _ = args.next();

    var options: Options = .{};
    errdefer options.deinit(allocator);

    while (args.next()) |argument| {
        if (std.mem.eql(u8, argument, "--help") or std.mem.eql(u8, argument, "-h")) {
            options.deinit(allocator);
            return .help;
        }
        if (std.mem.eql(u8, argument, "--host")) {
            try options.setHost(
                allocator,
                args.next() orelse return error.MissingArgumentValue,
            );
            continue;
        }
        if (std.mem.eql(u8, argument, "--port")) {
            options.port = parsePort(args.next() orelse return error.MissingArgumentValue) catch
                return error.InvalidPort;
            continue;
        }
        if (std.mem.eql(u8, argument, "--parix-settlement")) {
            options.parix_settlement = true;
            continue;
        }
        if (std.mem.eql(u8, argument, "--parix-profile")) {
            try options.setParixProfilePath(
                allocator,
                args.next() orelse return error.MissingArgumentValue,
            );
            continue;
        }
        if (std.mem.eql(u8, argument, "--ingress-auth-profile")) {
            try options.setIngressAuthProfilePath(
                allocator,
                args.next() orelse return error.MissingArgumentValue,
            );
            continue;
        }
        if (std.mem.eql(u8, argument, "--parix-create-accounts")) {
            options.parix_create_accounts = true;
            continue;
        }
        if (std.mem.eql(u8, argument, "--parix-development-seed")) {
            options.parix_development_seed = true;
            continue;
        }
        if (std.mem.eql(u8, argument, "--parix-funding-initialize")) {
            options.parix_funding_initialize = true;
            continue;
        }
        if (std.mem.eql(u8, argument, "--parix-clearing-profile")) {
            try options.setParixClearingProfilePath(
                allocator,
                args.next() orelse return error.MissingArgumentValue,
            );
            continue;
        }
        if (std.mem.eql(u8, argument, "--parix-provider-profile")) {
            try options.setParixProviderProfilePath(
                allocator,
                args.next() orelse return error.MissingArgumentValue,
            );
            continue;
        }
        if (std.mem.eql(u8, argument, "--parix-clearing-initialize")) {
            options.parix_clearing_initialize = true;
            continue;
        }
        if (std.mem.eql(u8, argument, "--parix-clearing-accounts-verified")) {
            options.parix_clearing_accounts_verified = true;
            continue;
        }
        if (std.mem.eql(u8, argument, "--provider-port")) {
            options.provider_port = parsePort(
                args.next() orelse return error.MissingArgumentValue,
            ) catch return error.InvalidPort;
            options.provider_port_explicit = true;
            continue;
        }
        if (std.mem.eql(u8, argument, "--persistence-root")) {
            try options.setPersistenceRoot(
                allocator,
                args.next() orelse return error.MissingArgumentValue,
            );
            continue;
        }
        return error.InvalidArguments;
    }

    try validateOptions(options);

    return .{ .options = options };
}

fn validateOptions(options: Options) ArgumentError!void {
    if (!isLoopbackHost(options.host)) return error.NonLoopbackHost;
    if (options.parix_settlement and options.parix_profile_path == null) {
        return error.MissingParixProfile;
    }
    if (!options.parix_settlement and options.parix_profile_path != null) {
        return error.ParixProfileWithoutSettlement;
    }
    if (options.parix_settlement and options.ingress_auth_profile_path == null) {
        return error.MissingIngressAuthProfile;
    }
    if (!options.parix_settlement and options.ingress_auth_profile_path != null) {
        return error.IngressAuthProfileWithoutSettlement;
    }
    if (!options.parix_settlement and
        (options.parix_create_accounts or options.parix_development_seed))
    {
        return error.ParixProvisioningWithoutSettlement;
    }
    if (!options.parix_settlement and options.parix_funding_initialize) {
        return error.ParixFundingInitializationWithoutSettlement;
    }
    const clearing_enabled = options.parix_clearing_profile_path != null;
    if (clearing_enabled and !options.parix_settlement) {
        return error.ClearingProfileWithoutSettlement;
    }
    if (!clearing_enabled and options.parix_provider_profile_path != null) {
        return error.ProviderProfileWithoutClearing;
    }
    if (clearing_enabled and options.parix_provider_profile_path == null) {
        return error.MissingProviderProfile;
    }
    if (!clearing_enabled and options.parix_clearing_initialize) {
        return error.ClearingInitializationWithoutClearing;
    }
    if (!clearing_enabled and options.parix_clearing_accounts_verified) {
        return error.ClearingAccountAttestationWithoutClearing;
    }
    if (clearing_enabled and !options.parix_clearing_accounts_verified) {
        return error.MissingClearingAccountAttestation;
    }
    if (!clearing_enabled and options.provider_port_explicit) {
        return error.ProviderPortWithoutClearing;
    }
    if (clearing_enabled and options.provider_port == options.port) {
        return error.DuplicateListenerPort;
    }
}

fn isLoopbackHost(host: []const u8) bool {
    return std.mem.eql(u8, host, "127.0.0.1") or
        std.mem.eql(u8, host, "localhost");
}

fn parsePort(raw: []const u8) ArgumentError!u16 {
    const port = std.fmt.parseInt(u16, raw, 10) catch return error.InvalidPort;
    if (port == 0) return error.InvalidPort;
    return port;
}

fn validatePersistenceRoot(path: []const u8) ArgumentError!void {
    if (path.len == 0 or std.fs.path.isAbsolute(path)) {
        return error.InvalidPersistenceRoot;
    }

    var components = std.mem.splitScalar(u8, path, '/');
    var count: usize = 0;
    while (components.next()) |component| {
        if (!isSafePathComponent(component)) return error.InvalidPersistenceRoot;
        count += 1;
    }
    if (count == 0) return error.InvalidPersistenceRoot;
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

fn writeArgumentError(io: std.Io, err: anyerror) !void {
    var stderr_buffer: [2048]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writer(io, &stderr_buffer);
    try stderr_writer.interface.print("argument error: {s}\n\n", .{@errorName(err)});
    try writeUsage(&stderr_writer.interface);
    try stderr_writer.interface.flush();
}

fn writeUsage(writer: *std.Io.Writer) std.Io.Writer.Error!void {
    try writer.writeAll(
        \\Usage: exchange-core-server [--host LOOPBACK_HOST] [--port PORT]
        \\                            [--parix-settlement --parix-profile PATH
        \\                             --ingress-auth-profile PATH]
        \\                            [--parix-create-accounts] [--parix-development-seed]
        \\                            [--parix-funding-initialize]
        \\                            [--parix-clearing-profile PATH
        \\                             --parix-provider-profile PATH
        \\                             --parix-clearing-accounts-verified]
        \\                            [--parix-clearing-initialize] [--provider-port PORT]
        \\                            [--persistence-root RELATIVE_PATH]
        \\
        \\Starts a loopback-only newline-delimited TCP exchange server.
        \\
        \\Options:
        \\  --host LOOPBACK_HOST         Bind 127.0.0.1 or localhost (default: 127.0.0.1)
        \\  --port PORT                  Bind TCP port (default: 7379)
        \\  --parix-settlement           Enable native Parix/TigerBeetle settlement
        \\  --parix-profile PATH          Required operator-owned settlement profile
        \\  --ingress-auth-profile PATH  Required non-secret authenticated-ingress profile
        \\  --parix-create-accounts       Create missing external accounts (default: validate only)
        \\  --parix-development-seed      Seed configured development funding transfers
        \\  --parix-funding-initialize    Create an absent funding saga store once; fail if present
        \\  --parix-clearing-profile PATH Required strict payment-clearing profile
        \\  --parix-provider-profile PATH Required non-secret provider HMAC profile
        \\  --parix-clearing-accounts-verified
        \\                                Attest completion of the reviewed provider-account check
        \\  --parix-clearing-initialize   Create an absent clearing saga store once; fail if present
        \\  --provider-port PORT          Loopback provider HTTP port (default: 7380)
        \\  --persistence-root PATH      Safe relative durable root (default: zig-out/runtime/parix)
        \\  -h, --help                   Show this help
        \\
        \\Settlement environment:
        \\  PARIX_DATABASE_ID            Required database UUID
        \\  PARIX_BASE_URL               Optional API origin (default: https://dev.parix.io)
        \\  PARIX_API_KEY                API key credential (exclusive with bearer token)
        \\  PARIX_BEARER_TOKEN            Bearer credential (exclusive with API key)
        \\
        \\Ingress security:
        \\  Authenticated Parix ingress is plaintext on loopback. Terminate TLS/mTLS
        \\  at an operator-owned proxy and never expose this TCP port directly.
        \\  Native ingress caps sessions at 64 and applies a 5-second POSIX idle
        \\  receive timeout. The proxy must enforce absolute deadlines and rate limits.
        \\  Profile secret_file_env values name environment variables containing
        \\  paths to owner-only (0600) key files; never place keys in arguments.
        \\  Provider HTTPS terminates at an operator-owned L7 proxy. The backend
        \\  listener is loopback-only HTTP and still requires the provider HMAC.
        \\
    );
}

const test_parix_profile_json =
    \\{
    \\  "version": 1,
    \\  "database_uuid": "11111111-2222-3333-4444-555555555555",
    \\  "publication": {"namespace": 9, "journal_epoch": 7},
    \\  "symbol": {"symbol_id": 9001, "symbol_type": "currency_exchange_pair", "base_currency": 31, "quote_currency": 32, "base_scale_k": 10, "quote_scale_k": 20, "taker_fee": 3, "maker_fee": 4, "margin_buy": 0, "margin_sell": 0},
    \\  "currencies": [{"currency": 31, "ledger": 501, "treasury_account_id": "900031", "fee_account_id": "910031"}, {"currency": 32, "ledger": 502, "treasury_account_id": "900032", "fee_account_id": "910032"}],
    \\  "users": [{"uid": 7001, "base_account_id": "920031", "quote_account_id": "920032"}, {"uid": 7002, "base_account_id": "930031", "quote_account_id": "930032"}],
    \\  "initial_funding": [{"id": "940031", "uid": 7001, "currency": 31, "amount": "1234"}, {"id": "940032", "uid": 7002, "currency": 32, "amount": "5678"}]
    \\}
;

fn testParixProfile(allocator: std.mem.Allocator) !*parix_profile.Profile {
    return parix_profile.Profile.parseJson(allocator, test_parix_profile_json);
}

fn writeTestIngressFile(
    dir: std.Io.Dir,
    path: []const u8,
    bytes: []const u8,
) !void {
    const file = try dir.createFile(std.testing.io, path, .{
        .truncate = true,
        .permissions = .fromMode(0o600),
    });
    defer file.close(std.testing.io);
    var buffer: [2048]u8 = undefined;
    var writer = file.writer(std.testing.io, &buffer);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
}

fn loadTestIngressProfile(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    instance_id: []const u8,
    trader_uid: i64,
) !*ingress_auth.Profile {
    var profile_storage: [2048]u8 = undefined;
    var profile_writer = std.Io.Writer.fixed(&profile_storage);
    try profile_writer.print(
        "{{\"version\":1,\"instance_id\":\"{s}\",\"credentials\":[" ++
            "{{\"credential_id\":\"trader-a\",\"principal_id\":\"trader\",\"role\":\"trader\",\"uid\":{d},\"secret_file_env\":\"TEST_TRADER_KEY_FILE\"}}," ++
            "{{\"credential_id\":\"operator-a\",\"principal_id\":\"operator\",\"role\":\"operator\",\"secret_file_env\":\"TEST_OPERATOR_KEY_FILE\"}}]}}",
        .{ instance_id, trader_uid },
    );
    try writeTestIngressFile(dir, "ingress.json", profile_writer.buffered());
    try writeTestIngressFile(
        dir,
        "trader.key",
        "1111111111111111111111111111111111111111111111111111111111111111",
    );
    try writeTestIngressFile(
        dir,
        "operator.key",
        "2222222222222222222222222222222222222222222222222222222222222222",
    );

    var environment = std.process.Environ.Map.init(allocator);
    defer environment.deinit();
    try environment.put("TEST_TRADER_KEY_FILE", "trader.key");
    try environment.put("TEST_OPERATOR_KEY_FILE", "operator.key");
    return ingress_auth.Profile.loadFromDir(
        std.testing.io,
        allocator,
        dir,
        &environment,
        "ingress.json",
    );
}

test "server defaults to loopback-only development mode" {
    const options: Options = .{};
    try std.testing.expectEqualStrings(DefaultHost, options.host);
    try std.testing.expectEqual(DefaultPort, options.port);
    try std.testing.expect(!options.parix_settlement);
    try std.testing.expectEqual(@as(?[]const u8, null), options.parix_profile_path);
    try std.testing.expectEqual(@as(?[]const u8, null), options.ingress_auth_profile_path);
    try std.testing.expect(!options.parix_create_accounts);
    try std.testing.expect(!options.parix_development_seed);
    try std.testing.expect(!options.parix_funding_initialize);
    try std.testing.expectEqual(@as(?[]const u8, null), options.parix_clearing_profile_path);
    try std.testing.expectEqual(@as(?[]const u8, null), options.parix_provider_profile_path);
    try std.testing.expect(!options.parix_clearing_initialize);
    try std.testing.expect(!options.parix_clearing_accounts_verified);
    try std.testing.expectEqual(DefaultProviderPort, options.provider_port);
    try std.testing.expect(!options.provider_port_explicit);
    try std.testing.expectEqualStrings(DefaultPersistenceRoot, options.persistence_root);

    const symbol = try defaultSymbolSpecification();
    try symbol.validate();
    try std.testing.expectEqual(DefaultSymbolId, symbol.symbol_id);
    try std.testing.expectEqual(DefaultBaseCurrency, symbol.base_currency);
    try std.testing.expectEqual(DefaultQuoteCurrency, symbol.quote_currency);
    try std.testing.expectEqual(@as(i64, 100), symbol.base_scale_k);
    try std.testing.expectEqual(@as(i64, 10), symbol.quote_scale_k);
    try std.testing.expectEqual(@as(i64, 2), symbol.taker_fee);
    try std.testing.expectEqual(@as(i64, 1), symbol.maker_fee);
}

test "Parix and ingress profiles are required exactly with settlement" {
    try validateOptions(.{});
    try std.testing.expectEqual(
        parix_funding_runtime.OpenPolicy.open_existing,
        fundingOpenPolicy(.{}),
    );
    try std.testing.expectError(
        error.MissingParixProfile,
        validateOptions(.{ .parix_settlement = true }),
    );
    try std.testing.expectError(
        error.ParixProfileWithoutSettlement,
        validateOptions(.{ .parix_profile_path = "profile.json" }),
    );
    try std.testing.expectError(
        error.MissingIngressAuthProfile,
        validateOptions(.{
            .parix_settlement = true,
            .parix_profile_path = "profile.json",
        }),
    );
    try std.testing.expectError(
        error.IngressAuthProfileWithoutSettlement,
        validateOptions(.{ .ingress_auth_profile_path = "ingress.json" }),
    );
    try std.testing.expectError(
        error.ParixProvisioningWithoutSettlement,
        validateOptions(.{ .parix_create_accounts = true }),
    );
    try std.testing.expectError(
        error.ParixFundingInitializationWithoutSettlement,
        validateOptions(.{ .parix_funding_initialize = true }),
    );

    const validate_only: Options = .{
        .parix_settlement = true,
        .parix_profile_path = "profile.json",
        .ingress_auth_profile_path = "ingress.json",
        .parix_funding_initialize = true,
    };
    try validateOptions(validate_only);
    const validate_policy = parixBootstrapPolicy(validate_only);
    try std.testing.expect(!validate_policy.allow_account_creation);
    try std.testing.expect(!validate_policy.allow_initial_funding);
    try std.testing.expect(validate_only.parix_funding_initialize);
    try std.testing.expectEqual(
        parix_funding_runtime.OpenPolicy.initialize,
        fundingOpenPolicy(validate_only),
    );

    const funding_only: Options = .{
        .parix_settlement = true,
        .parix_profile_path = "profile.json",
        .ingress_auth_profile_path = "ingress.json",
        .parix_development_seed = true,
    };
    try validateOptions(funding_only);
    const funding_policy = parixBootstrapPolicy(funding_only);
    try std.testing.expect(!funding_policy.allow_account_creation);
    try std.testing.expect(funding_policy.allow_initial_funding);

    const provision_all: Options = .{
        .parix_settlement = true,
        .parix_profile_path = "profile.json",
        .ingress_auth_profile_path = "ingress.json",
        .parix_create_accounts = true,
        .parix_development_seed = true,
    };
    try validateOptions(provision_all);
    const provision_policy = parixBootstrapPolicy(provision_all);
    try std.testing.expect(provision_policy.allow_account_creation);
    try std.testing.expect(provision_policy.allow_initial_funding);
}

test "Parix CLI owns profile paths and enforces settlement pairing" {
    const allocator = std.testing.allocator;

    const missing_profile_argv = [_][*:0]const u8{
        "exchange-core-server",
        "--parix-settlement",
    };
    try std.testing.expectError(
        error.MissingParixProfile,
        parseOptions(.{ .vector = &missing_profile_argv }, allocator),
    );

    const unpaired_profile_argv = [_][*:0]const u8{
        "exchange-core-server",
        "--parix-profile",
        "/operator/profile.json",
    };
    try std.testing.expectError(
        error.ParixProfileWithoutSettlement,
        parseOptions(.{ .vector = &unpaired_profile_argv }, allocator),
    );

    const configured_argv = [_][*:0]const u8{
        "exchange-core-server",
        "--parix-settlement",
        "--parix-profile",
        "/operator/profile.json",
        "--ingress-auth-profile",
        "/operator/ingress.json",
        "--parix-development-seed",
        "--parix-funding-initialize",
    };
    var parsed = try parseOptions(.{ .vector = &configured_argv }, allocator);
    switch (parsed) {
        .help => return error.UnexpectedHelp,
        .options => |*options| {
            defer options.deinit(allocator);
            try std.testing.expect(options.parix_settlement);
            try std.testing.expectEqualStrings(
                "/operator/profile.json",
                options.parix_profile_path.?,
            );
            try std.testing.expect(options.owned_parix_profile_path != null);
            try std.testing.expectEqualStrings(
                "/operator/ingress.json",
                options.ingress_auth_profile_path.?,
            );
            try std.testing.expect(options.owned_ingress_auth_profile_path != null);
            try std.testing.expect(!options.parix_create_accounts);
            try std.testing.expect(options.parix_development_seed);
            try std.testing.expect(options.parix_funding_initialize);
        },
    }
}

test "clearing CLI requires provider profile account attestation and distinct port" {
    const parix_only: Options = .{
        .parix_settlement = true,
        .parix_profile_path = "settlement.json",
        .ingress_auth_profile_path = "ingress.json",
    };
    try validateOptions(parix_only);
    try std.testing.expectError(
        error.ClearingProfileWithoutSettlement,
        validateOptions(.{ .parix_clearing_profile_path = "clearing.json" }),
    );
    try std.testing.expectError(
        error.ProviderProfileWithoutClearing,
        validateOptions(.{ .parix_provider_profile_path = "provider.json" }),
    );
    try std.testing.expectError(
        error.MissingProviderProfile,
        validateOptions(.{
            .parix_settlement = true,
            .parix_profile_path = "settlement.json",
            .ingress_auth_profile_path = "ingress.json",
            .parix_clearing_profile_path = "clearing.json",
        }),
    );
    try std.testing.expectError(
        error.MissingClearingAccountAttestation,
        validateOptions(.{
            .parix_settlement = true,
            .parix_profile_path = "settlement.json",
            .ingress_auth_profile_path = "ingress.json",
            .parix_clearing_profile_path = "clearing.json",
            .parix_provider_profile_path = "provider.json",
        }),
    );
    try std.testing.expectError(
        error.ClearingInitializationWithoutClearing,
        validateOptions(.{ .parix_clearing_initialize = true }),
    );
    try std.testing.expectError(
        error.ClearingAccountAttestationWithoutClearing,
        validateOptions(.{ .parix_clearing_accounts_verified = true }),
    );
    try std.testing.expectError(
        error.ProviderPortWithoutClearing,
        validateOptions(.{ .provider_port_explicit = true }),
    );

    var enabled = parix_only;
    enabled.parix_clearing_profile_path = "clearing.json";
    enabled.parix_provider_profile_path = "provider.json";
    enabled.parix_clearing_accounts_verified = true;
    try validateOptions(enabled);
    enabled.provider_port = enabled.port;
    try std.testing.expectError(error.DuplicateListenerPort, validateOptions(enabled));
}

test "clearing CLI owns provider paths and one-shot initialization" {
    const allocator = std.testing.allocator;
    const configured_argv = [_][*:0]const u8{
        "exchange-core-server",
        "--parix-settlement",
        "--parix-profile",
        "/operator/settlement.json",
        "--ingress-auth-profile",
        "/operator/ingress.json",
        "--parix-clearing-profile",
        "/operator/clearing.json",
        "--parix-provider-profile",
        "/operator/provider.json",
        "--parix-clearing-accounts-verified",
        "--parix-clearing-initialize",
        "--provider-port",
        "7443",
    };
    var parsed = try parseOptions(.{ .vector = &configured_argv }, allocator);
    switch (parsed) {
        .help => return error.UnexpectedHelp,
        .options => |*options| {
            defer options.deinit(allocator);
            try std.testing.expectEqualStrings(
                "/operator/clearing.json",
                options.parix_clearing_profile_path.?,
            );
            try std.testing.expect(options.owned_parix_clearing_profile_path != null);
            try std.testing.expectEqualStrings(
                "/operator/provider.json",
                options.parix_provider_profile_path.?,
            );
            try std.testing.expect(options.owned_parix_provider_profile_path != null);
            try std.testing.expect(options.parix_clearing_accounts_verified);
            try std.testing.expect(options.parix_clearing_initialize);
            try std.testing.expectEqual(@as(u16, 7443), options.provider_port);
            try std.testing.expect(options.provider_port_explicit);
        },
    }
}

test "server accepts only literal loopback hosts before bind" {
    try validateOptions(.{ .host = "127.0.0.1" });
    try validateOptions(.{ .host = "localhost" });
    try std.testing.expectError(
        error.NonLoopbackHost,
        validateOptions(.{ .host = "0.0.0.0" }),
    );
    try std.testing.expectError(
        error.NonLoopbackHost,
        validateOptions(.{ .host = "::1" }),
    );
    try std.testing.expectError(
        error.NonLoopbackHost,
        validateOptions(.{ .host = "exchange.internal" }),
    );
}

test "authenticated connection state permits only AUTH proof request order" {
    var state: AuthSessionState = .awaiting_auth;
    try std.testing.expectError(
        error.InvalidAuthStateTransition,
        state.advance(.awaiting_request),
    );
    try state.advance(.awaiting_proof);
    try state.advance(.awaiting_request);
    try state.advance(.finished);
    try std.testing.expectError(
        error.InvalidAuthStateTransition,
        state.advance(.awaiting_auth),
    );
}

test "shutdown admission fence rejects commands queued behind shutdown" {
    const Context = struct {
        io: std.Io,
        mutex: *std.Io.Mutex,
        shutdown_state: *std.atomic.Value(bool),
        shutdown_admitted: *std.Io.Event,
        later_command_rejected: *std.atomic.Value(bool),

        fn admitShutdown(context: *@This()) std.Io.Cancelable!void {
            try context.mutex.lock(context.io);
            markAuthenticatedShutdown(context.shutdown_state, null);
            context.mutex.unlock(context.io);
            context.shutdown_admitted.set(context.io);
        }

        fn attemptLaterCommand(context: *@This()) std.Io.Cancelable!void {
            try context.shutdown_admitted.wait(context.io);
            try context.mutex.lock(context.io);
            defer context.mutex.unlock(context.io);
            ensureAuthenticatedCommandAdmission(context.shutdown_state) catch {
                context.later_command_rejected.store(true, .release);
                return;
            };
            return;
        }
    };

    var mutex: std.Io.Mutex = .init;
    var shutdown_state: std.atomic.Value(bool) = .init(false);
    var shutdown_admitted: std.Io.Event = .unset;
    var later_command_rejected: std.atomic.Value(bool) = .init(false);
    var context: Context = .{
        .io = std.testing.io,
        .mutex = &mutex,
        .shutdown_state = &shutdown_state,
        .shutdown_admitted = &shutdown_admitted,
        .later_command_rejected = &later_command_rejected,
    };
    var tasks: std.Io.Group = .init;
    try tasks.concurrent(std.testing.io, Context.attemptLaterCommand, .{&context});
    try tasks.concurrent(std.testing.io, Context.admitShutdown, .{&context});
    try tasks.await(std.testing.io);
    try std.testing.expect(shutdown_state.load(.acquire));
    try std.testing.expect(later_command_rejected.load(.acquire));
}

test "authenticated shutdown fences provider admission before publishing state" {
    const Stub = struct {
        fn verify(
            _: *const anyopaque,
            _: parix_clearing_provider_adapter.RequestView,
        ) parix_clearing_provider_adapter.VerifyError!parix_clearing_provider_adapter.VerifiedEvent {
            return error.AuthenticationFailed;
        }

        fn admit(
            _: *anyopaque,
            _: parix_clearing_provider_adapter.VerifiedEvent,
        ) parix_clearing_http_ingress.AdmissionOutcome {
            return .unavailable;
        }

        fn isReady(_: *anyopaque) bool {
            return false;
        }

        const adapter_vtable: parix_clearing_provider_adapter.Adapter.VTable = .{
            .verify = verify,
        };
        const admission_vtable: parix_clearing_http_ingress.AdmissionPort.VTable = .{
            .admit = admit,
            .is_ready = isReady,
        };
    };

    var context: u8 = 0;
    var provider_host = parix_clearing_http_ingress.Host.init(std.testing.io, .{
        .handler = .{
            .adapter = .{ .context = &context, .vtable = &Stub.adapter_vtable },
            .admission = .{ .context = &context, .vtable = &Stub.admission_vtable },
        },
    });
    var shutdown_state: std.atomic.Value(bool) = .init(false);

    markAuthenticatedShutdown(&shutdown_state, &provider_host);

    try std.testing.expectEqual(parix_clearing_http_ingress.Health.stopped, provider_host.health());
    try std.testing.expect(!provider_host.isAccepting());
    try std.testing.expect(shutdown_state.load(.acquire));
}

test "authenticated acceptor cancellation interrupts blocked accept safely" {
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var server = try address.listen(std.testing.io, .{ .reuse_address = true });
    defer server.deinit(std.testing.io);

    var accepted_storage: [1]std.Io.net.Stream = undefined;
    var accepted_streams = AuthenticatedStreamQueue.init(&accepted_storage);
    defer accepted_streams.close(std.testing.io);
    var session_slots: std.Io.Semaphore = .{ .permits = MaxAuthenticatedSessions };
    var shutdown_requested: std.atomic.Value(bool) = .init(false);
    var accept_failed: std.atomic.Value(bool) = .init(false);
    var context: AuthenticatedAcceptorContext = .{
        .io = std.testing.io,
        .server = &server,
        .accepted_streams = &accepted_streams,
        .session_slots = &session_slots,
        .shutdown_requested = &shutdown_requested,
        .accept_failed = &accept_failed,
    };
    var acceptors: std.Io.Group = .init;
    try acceptors.concurrent(
        std.testing.io,
        acceptAuthenticatedConnections,
        .{&context},
    );
    acceptors.cancel(std.testing.io);
    try std.testing.expect(!accept_failed.load(.acquire));
}

test "authenticated POSIX streams install five second receive timeout" {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi) return error.SkipZigTest;
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var server = try address.listen(std.testing.io, .{ .reuse_address = true });
    defer server.deinit(std.testing.io);
    const client = try server.socket.address.connect(
        std.testing.io,
        .{ .mode = .stream },
    );
    defer client.close(std.testing.io);
    const accepted = try server.accept(std.testing.io);
    defer accepted.close(std.testing.io);
    try installAuthenticatedReceiveTimeout(accepted);
}

test "authenticated signed responses stage exactly one outer newline" {
    const auth: ingress_auth.Auth = .{
        .instance_id = try ingress_auth.validateInstanceId("11111111-2222-3333-4444-555555555555"),
        .credential_id = try ingress_auth.Identifier.init("trader-a"),
        .client_nonce = [_]u8{0x11} ** ingress_auth.nonce_bytes,
    };
    const transcript = ingress_auth.Transcript.init(auth, .{
        .server_nonce = [_]u8{0x22} ** ingress_auth.nonce_bytes,
    });
    var authenticated: ingress_auth.Authenticated = .{
        .identity = .{
            .credential_id = auth.credential_id,
            .principal_id = try ingress_auth.Identifier.init("trader"),
            .principal = .{ .trader = 7001 },
        },
        .session_key = .{ .bytes = [_]u8{0x33} ** ingress_auth.secret_key_bytes },
    };
    defer authenticated.deinit();

    const raw_response = try stagedResponsePayload("OK ready=true\n");
    var wire_storage: [MaxAuthenticatedWriteBufferBytes]u8 = undefined;
    var wire_writer = std.Io.Writer.fixed(&wire_storage);
    try writeSignedIngressResponse(
        &wire_writer,
        &authenticated,
        transcript,
        raw_response,
    );
    const wire = wire_writer.buffered();
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, wire, "\n"));
    try std.testing.expect(wire[wire.len - 1] == '\n');
    const response = try ingress_auth.parseResponseEnvelope(wire[0 .. wire.len - 1]);
    try std.testing.expectEqualStrings("OK ready=true", response.raw_response);
    try ingress_auth.verifyResponseMac(
        &authenticated.session_key,
        transcript.instance_id.slice(),
        transcript.credential_id.slice(),
        response.sequence,
        response.raw_response,
        response.response_mac,
    );

    try std.testing.expectError(error.InvalidStagedResponse, stagedResponsePayload(""));
    try std.testing.expectError(error.InvalidStagedResponse, stagedResponsePayload("OK no-newline"));
    try std.testing.expectError(error.InvalidStagedResponse, stagedResponsePayload("OK\nextra\n"));
}

test "authentication failures are exact generic and secret free" {
    var storage: [128]u8 = undefined;
    var writer = std.Io.Writer.fixed(&storage);
    try ingress_auth.writeAuthenticationFailure(&writer);
    try std.testing.expectEqualStrings(
        "ERR authentication AuthenticationFailed\n",
        writer.buffered(),
    );
    try std.testing.expect(std.mem.indexOf(u8, writer.buffered(), "trader-a") == null);
    try std.testing.expect(std.mem.indexOf(u8, writer.buffered(), "111111") == null);
}

test "ingress profile matches Parix database and configured trader set" {
    const allocator = std.testing.allocator;
    const configured_profile = try testParixProfile(allocator);
    defer configured_profile.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const valid = try loadTestIngressProfile(
        allocator,
        tmp.dir,
        configured_profile.databaseUuid(),
        7001,
    );
    defer valid.deinit();
    try validateIngressProfile(
        valid,
        configured_profile,
        configured_profile.databaseUuid(),
    );
    try std.testing.expectError(
        error.IngressInstanceMismatch,
        validateIngressProfile(
            valid,
            configured_profile,
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        ),
    );

    const mismatched_instance = try loadTestIngressProfile(
        allocator,
        tmp.dir,
        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        7001,
    );
    defer mismatched_instance.deinit();
    try std.testing.expectError(
        error.IngressInstanceMismatch,
        validateIngressProfile(
            mismatched_instance,
            configured_profile,
            configured_profile.databaseUuid(),
        ),
    );

    const missing_trader = try loadTestIngressProfile(
        allocator,
        tmp.dir,
        configured_profile.databaseUuid(),
        7999,
    );
    defer missing_trader.deinit();
    try std.testing.expectError(
        error.IngressTraderMissingFromSettlement,
        validateIngressProfile(
            missing_trader,
            configured_profile,
            configured_profile.databaseUuid(),
        ),
    );
}

test "Parix persistence roots stay repository relative" {
    try validatePersistenceRoot("zig-out/runtime/parix");
    try validatePersistenceRoot("state.v1/parix_outbox");
    try std.testing.expectError(error.InvalidPersistenceRoot, validatePersistenceRoot(""));
    try std.testing.expectError(error.InvalidPersistenceRoot, validatePersistenceRoot("/tmp/parix"));
    try std.testing.expectError(error.InvalidPersistenceRoot, validatePersistenceRoot("../parix"));
    try std.testing.expectError(error.InvalidPersistenceRoot, validatePersistenceRoot("zig-out//parix"));
    try std.testing.expectError(error.InvalidPersistenceRoot, validatePersistenceRoot("zig-out/parix dump"));
}

test "Parix environment requires one environment-only credential" {
    const allocator = std.testing.allocator;
    var environment = std.process.Environ.Map.init(allocator);
    defer environment.deinit();

    try std.testing.expectError(
        error.MissingParixDatabaseId,
        readParixEnvironment(&environment),
    );
    try environment.put("PARIX_DATABASE_ID", "0e46f90e-c38b-487a-b9f9-630f7839cc3f");
    try std.testing.expectError(
        error.MissingParixCredential,
        readParixEnvironment(&environment),
    );

    try environment.put("PARIX_API_KEY", "test-api-key");
    const api_key_config = try readParixEnvironment(&environment);
    try std.testing.expectEqualStrings(DefaultParixBaseUrl, api_key_config.base_url);
    switch (api_key_config.auth) {
        .api_key => |secret| try std.testing.expectEqualStrings("test-api-key", secret),
        .bearer => return error.UnexpectedCredentialKind,
    }

    try environment.put("PARIX_BEARER_TOKEN", "test-bearer-token");
    try std.testing.expectError(
        error.MultipleParixCredentials,
        readParixEnvironment(&environment),
    );
}

test "Parix database identity must exactly match profile before transport" {
    const configured_profile = try testParixProfile(std.testing.allocator);
    defer configured_profile.deinit();

    try validateParixDatabaseId(
        "11111111-2222-3333-4444-555555555555",
        configured_profile,
    );
    try std.testing.expectError(
        error.ParixDatabaseIdMismatch,
        validateParixDatabaseId(
            "0e46f90e-c38b-487a-b9f9-630f7839cc3f",
            configured_profile,
        ),
    );
}

test "Parix mode uses non-default profile for mutation admission" {
    const configured_profile = try testParixProfile(std.testing.allocator);
    defer configured_profile.deinit();
    const operation_id = try protocol.FundingOperationId.parse(
        "123e4567-e89b-12d3-a456-426614174000",
    );

    try std.testing.expectError(
        error.ParixDepositUnsupported,
        validateParixRequest(configured_profile, .{ .deposit = .{
            .uid = 7001,
            .currency = 31,
            .amount = 1,
            .transaction_id = 1,
        } }),
    );
    try std.testing.expectError(
        error.UnknownParixProfileUser,
        validateParixRequest(configured_profile, .{ .add_user = .{ .uid = 99 } }),
    );
    try validateParixRequest(configured_profile, .{ .place = .{
        .order_id = 1,
        .uid = 7002,
        .symbol = 9001,
        .price = 5,
        .size = 1,
        .side = .bid,
        .time_in_force = .gtc,
    } });
    try std.testing.expectError(
        error.UnsupportedParixSymbol,
        validateParixRequest(configured_profile, .{ .place = .{
            .order_id = 2,
            .uid = 7002,
            .symbol = 9002,
            .price = 5,
            .size = 1,
            .side = .bid,
            .time_in_force = .gtc,
        } }),
    );
    try std.testing.expectError(
        error.UnsupportedParixSymbol,
        validateParixRequest(configured_profile, .{ .cancel = .{
            .order_id = 1,
            .uid = 7001,
            .symbol = 9002,
        } }),
    );
    try validateParixRequest(configured_profile, .{ .fund_deposit = .{
        .uid = 7001,
        .currency = 31,
        .amount = 1,
        .operation_id = operation_id,
    } });
    try validateParixRequest(configured_profile, .{ .fund_withdraw = .{
        .uid = 7001,
        .currency = 31,
        .amount = 1,
        .operation_id = operation_id,
    } });
    try validateParixRequest(configured_profile, .{ .funding_status = operation_id });
    try validateParixRequest(configured_profile, .{ .clearing_status = 91 });
    try validateParixRequest(configured_profile, .clearing_operations);
}

test "server port parser rejects zero, overflow, and non-numeric input" {
    try std.testing.expectEqual(@as(u16, 7379), try parsePort("7379"));
    try std.testing.expectError(error.InvalidPort, parsePort("0"));
    try std.testing.expectError(error.InvalidPort, parsePort("65536"));
    try std.testing.expectError(error.InvalidPort, parsePort("tcp"));
}

test "server maps wire place orders to settlement-safe API commands" {
    const mapped = try apiCommandForRequest(.{ .place = .{
        .order_id = 71,
        .uid = 81,
        .symbol = DefaultSymbolId,
        .price = 1_001,
        .size = 9,
        .side = .bid,
        .time_in_force = .ioc,
    } });
    switch (mapped) {
        .place_order => |command| {
            try std.testing.expectEqual(@as(i64, 71), command.order_id);
            try std.testing.expectEqual(@as(i64, 81), command.uid);
            try std.testing.expectEqual(DefaultSymbolId, command.symbol);
            try std.testing.expectEqual(@as(i64, 1_001), command.reserve_bid_price);
            try std.testing.expectEqual(core.common.OrderAction.bid, command.action);
            try std.testing.expectEqual(core.common.OrderType.ioc, command.order_type);
        },
        else => return error.UnexpectedApiCommand,
    }

    const operation_id = try protocol.FundingOperationId.parse(
        "123e4567-e89b-12d3-a456-426614174000",
    );
    try std.testing.expectError(
        error.NonCommandRequest,
        apiCommandForRequest(.{ .fund_deposit = .{
            .uid = 81,
            .currency = DefaultQuoteCurrency,
            .amount = 1,
            .operation_id = operation_id,
        } }),
    );
    try std.testing.expectError(
        error.NonCommandRequest,
        apiCommandForRequest(.{ .fund_withdraw = .{
            .uid = 81,
            .currency = DefaultQuoteCurrency,
            .amount = 1,
            .operation_id = operation_id,
        } }),
    );
    try std.testing.expectError(
        error.NonCommandRequest,
        apiCommandForRequest(.{ .funding_status = operation_id }),
    );
    try std.testing.expectError(
        error.NonCommandRequest,
        apiCommandForRequest(.{ .clearing_status = 91 }),
    );
    try std.testing.expectError(
        error.NonCommandRequest,
        apiCommandForRequest(.clearing_operations),
    );
}

test "funding balance adapter maps only exact idempotent core results" {
    try std.testing.expectEqual(
        parix_funding_saga.CoreOutcome.applied,
        fundingCoreOutcome(.success),
    );
    try std.testing.expectEqual(
        parix_funding_saga.CoreOutcome.already_applied_same,
        fundingCoreOutcome(.user_mgmt_account_balance_adjustment_already_applied_same),
    );
    try std.testing.expectEqual(
        parix_funding_saga.CoreOutcome.already_applied_many,
        fundingCoreOutcome(.user_mgmt_account_balance_adjustment_already_applied_many),
    );
    try std.testing.expectEqual(
        parix_funding_saga.CoreOutcome.rejected,
        fundingCoreOutcome(.user_mgmt_account_balance_adjustment_nsf),
    );
    try std.testing.expectEqual(
        parix_funding_saga.CoreOutcome.rejected,
        fundingCoreOutcome(.user_mgmt_user_not_found),
    );
}

test "funding requests fail closed without the authenticated Parix runtime" {
    const operation_id = try protocol.FundingOperationId.parse(
        "123e4567-e89b-12d3-a456-426614174000",
    );
    const command: protocol.FundingMutation = .{
        .uid = 7001,
        .currency = 31,
        .amount = 50,
        .operation_id = operation_id,
    };

    var submit_storage: [128]u8 = undefined;
    var submit_writer = std.Io.Writer.fixed(&submit_storage);
    try executeFundingSubmit(null, .deposit, command, &submit_writer);
    try std.testing.expectEqualStrings(
        "ERR kind=funding message=FundingUnavailable\n",
        submit_writer.buffered(),
    );

    var status_storage: [128]u8 = undefined;
    var status_writer = std.Io.Writer.fixed(&status_storage);
    try executeFundingStatus(null, operation_id, &status_writer);
    try std.testing.expectEqualStrings(
        "ERR kind=funding message=FundingUnavailable\n",
        status_writer.buffered(),
    );
}

test "funding status is canonical and error details stay bounded" {
    const operation_id = try protocol.FundingOperationId.parse(
        "123e4567-e89b-12d3-a456-426614174000",
    );
    const request: parix_funding_saga.Request = .{
        .operation_id = operation_id.value,
        .kind = .withdrawal,
        .uid = 7002,
        .currency = 32,
        .amount = 125,
    };

    var accepted_storage: [256]u8 = undefined;
    var accepted_writer = std.Io.Writer.fixed(&accepted_storage);
    try writeFundingStatus(&accepted_writer, .{
        .request = request,
        .state = .accepted,
    });
    try std.testing.expectEqualStrings(
        "OK funding operation=123e4567-e89b-12d3-a456-426614174000 kind=withdrawal uid=7002 currency=32 amount=125 state=accepted reason=none\n",
        accepted_writer.buffered(),
    );

    var quarantined_storage: [256]u8 = undefined;
    var quarantined_writer = std.Io.Writer.fixed(&quarantined_storage);
    try writeFundingStatus(&quarantined_writer, .{
        .request = request,
        .state = .quarantined,
        .quarantine_reason = .transfer_mismatch,
    });
    try std.testing.expectEqualStrings(
        "OK funding operation=123e4567-e89b-12d3-a456-426614174000 kind=withdrawal uid=7002 currency=32 amount=125 state=quarantined reason=transfer_mismatch\n",
        quarantined_writer.buffered(),
    );

    try std.testing.expectEqualStrings("FundingNotFound", fundingErrorMessage(error.NotFound));
    try std.testing.expectEqualStrings("IdempotencyConflict", fundingErrorMessage(error.IdempotencyConflict));
    try std.testing.expectEqualStrings("FundingCapacityExceeded", fundingErrorMessage(error.CapacityExceeded));
    try std.testing.expectEqualStrings("FundingUnavailable", fundingErrorMessage(error.RuntimeFailed));
    try std.testing.expectEqualStrings("FundingUnavailable", fundingErrorMessage(error.StorageAmbiguous));
    try std.testing.expectEqualStrings("FundingFailed", fundingErrorMessage(error.UnknownFundingUser));
}

test "clearing status is operator-readable canonical evidence" {
    const authorization: parix_clearing_saga.Authorization = .{
        .event_id = 101,
        .payment_id = 91,
        .provider_id = 7_001,
        .revision = 1,
        .direction = .deposit,
        .uid = 1_001,
        .currency = 11,
        .amount = 25,
        .timeout_seconds = 60,
    };

    var authorized_storage: [256]u8 = undefined;
    var authorized_writer = std.Io.Writer.fixed(&authorized_storage);
    try writeClearingStatus(&authorized_writer, .{
        .authorization = authorization,
        .expires_at_unix_seconds = 1_700_000_060,
        .resolution = null,
        .state = .authorized,
    });
    try std.testing.expectEqualStrings(
        "OK clearing payment=91 state=authorized authorization_event=101 " ++
            "authorization_revision=1 expires_at=1700000060 resolution=none " ++
            "resolution_event=0 resolution_revision=0 resolution_source=none " ++
            "reason=none\n",
        authorized_writer.buffered(),
    );

    var completed_storage: [256]u8 = undefined;
    var completed_writer = std.Io.Writer.fixed(&completed_storage);
    try writeClearingStatus(&completed_writer, .{
        .authorization = authorization,
        .expires_at_unix_seconds = 1_700_000_060,
        .resolution = .{
            .event_id = 102,
            .payment_id = authorization.payment_id,
            .provider_id = authorization.provider_id,
            .revision = 2,
            .kind = .post,
            .direction = authorization.direction,
            .uid = authorization.uid,
            .currency = authorization.currency,
            .amount = authorization.amount,
        },
        .resolution_source = .provider,
        .state = .completed,
    });
    try std.testing.expectEqualStrings(
        "OK clearing payment=91 state=completed authorization_event=101 " ++
            "authorization_revision=1 expires_at=1700000060 resolution=post " ++
            "resolution_event=102 resolution_revision=2 resolution_source=provider " ++
            "reason=none\n",
        completed_writer.buffered(),
    );

    var expired_storage: [256]u8 = undefined;
    var expired_writer = std.Io.Writer.fixed(&expired_storage);
    try writeClearingStatus(&expired_writer, .{
        .authorization = authorization,
        .expires_at_unix_seconds = 1_700_000_060,
        .resolution = parix_clearing_saga.expirationResolution(authorization),
        .resolution_source = .expiration,
        .state = .completed,
    });
    var expected_expired_storage: [256]u8 = undefined;
    const expected_expired = try std.fmt.bufPrint(
        &expected_expired_storage,
        "OK clearing payment=91 state=completed authorization_event=101 " ++
            "authorization_revision=1 expires_at=1700000060 resolution=void " ++
            "resolution_event={d} resolution_revision=2 resolution_source=expiration " ++
            "reason=none\n",
        .{parix_clearing_saga.expirationResolution(authorization).event_id},
    );
    try std.testing.expectEqualStrings(expected_expired, expired_writer.buffered());

    var unavailable_storage: [128]u8 = undefined;
    var unavailable_writer = std.Io.Writer.fixed(&unavailable_storage);
    try executeClearingStatus(null, authorization.payment_id, &unavailable_writer);
    try std.testing.expectEqualStrings(
        "ERR kind=clearing message=ClearingUnavailable\n",
        unavailable_writer.buffered(),
    );
    try std.testing.expectEqualStrings(
        "ClearingNotFound",
        clearingErrorMessage(error.PaymentNotFound),
    );
    try std.testing.expectEqualStrings(
        "ClearingUnavailable",
        clearingErrorMessage(error.StorageAmbiguous),
    );
    try std.testing.expectEqualStrings(
        "ClearingFailed",
        clearingErrorMessage(error.InvalidState),
    );
}

test "clearing operations are canonical bounded incident evidence" {
    var storage: [1024]u8 = undefined;
    var writer = std.Io.Writer.fixed(&storage);
    try writeClearingOperations(&writer, .{
        .health = .pending,
        .durable = .{
            .observed_at_unix_seconds = 1_700_000_100,
            .capacity = 4_096,
            .total = 8,
            .states = .{
                .accepted = 1,
                .pending = 2,
                .authorized = 1,
                .completed = 2,
                .failed = 1,
                .quarantined = 1,
            },
            .ready_actions = 2,
            .in_flight_actions = 1,
            .remote_reconciliations = 1,
            .funding_reconciliations = 1,
            .provider_resolutions = 2,
            .expiration_resolutions = 1,
            .remote_attempts = 7,
            .funding_attempts = 3,
            .expired_unresolved = 1,
            .legacy_manual_expiry = 1,
            .max_deadline_lag_seconds = 5,
            .oldest_open_age_seconds = 65,
            .oldest_expired_payment_id = 91,
            .first_reconciliation_payment_id = 92,
            .first_failed_payment_id = 93,
            .first_quarantined_payment_id = 94,
        },
    });
    try std.testing.expectEqualStrings(
        "OK clearing-operations health=pending observed_at=1700000100 capacity=4096 total=8 " ++
            "accepted=1 pending=2 authorized=1 completed=2 failed=1 quarantined=1 " ++
            "ready=2 in_flight=1 remote_reconcile=1 funding_reconcile=1 " ++
            "provider_resolutions=2 expiration_resolutions=1 remote_attempts=7 " ++
            "funding_attempts=3 expired_unresolved=1 legacy_manual_expiry=1 " ++
            "max_deadline_lag=5 oldest_open_age=65 oldest_expired_payment=91 " ++
            "first_reconciliation_payment=92 first_failed_payment=93 " ++
            "first_quarantined_payment=94\n",
        writer.buffered(),
    );

    var unavailable_storage: [128]u8 = undefined;
    var unavailable_writer = std.Io.Writer.fixed(&unavailable_storage);
    try executeClearingOperations(null, &unavailable_writer);
    try std.testing.expectEqualStrings(
        "ERR kind=clearing message=ClearingUnavailable\n",
        unavailable_writer.buffered(),
    );
}

test "funding health is ready during work and fails closed otherwise" {
    inline for (.{ parix_funding_runtime.Health.ready, .pending }) |health| {
        try std.testing.expect(fundingHealthIsReady(health));
        try std.testing.expect(fundingHealthAllowsCoreMutation(health));
    }
    inline for (.{
        parix_funding_runtime.Health.cold,
        .failed,
        .quarantined,
        .stopped,
    }) |health| {
        try std.testing.expect(!fundingHealthIsReady(health));
        try std.testing.expect(!fundingHealthAllowsCoreMutation(health));
    }
}

test "clearing and provider ingress readiness is paired and fail closed" {
    try std.testing.expect(clearingIngressHealthIsReady(null, null, false));
    try std.testing.expect(!clearingIngressHealthIsReady(.ready, null, true));
    try std.testing.expect(!clearingIngressHealthIsReady(null, .ready, true));

    inline for (.{ parix_clearing_runtime.Health.ready, .pending }) |health| {
        try std.testing.expect(clearingIngressHealthIsReady(health, .ready, true));
        try std.testing.expect(!clearingIngressHealthIsReady(health, .ready, false));
        try std.testing.expect(!clearingIngressHealthIsReady(health, .failed, true));
    }
    inline for (.{
        parix_clearing_runtime.Health.cold,
        .failed,
        .quarantined,
        .stopped,
    }) |health| {
        try std.testing.expect(!clearingIngressHealthIsReady(health, .ready, true));
    }
}

test "clearing admission errors map to closed HTTP outcome classes" {
    switch (clearingAdmissionError(error.InvalidAmount)) {
        .invalid => {},
        else => return error.UnexpectedAdmissionOutcome,
    }
    switch (clearingAdmissionError(error.IdempotencyConflict)) {
        .conflict => {},
        else => return error.UnexpectedAdmissionOutcome,
    }
    switch (clearingAdmissionError(error.StorageAmbiguous)) {
        .unavailable => {},
        else => return error.UnexpectedAdmissionOutcome,
    }
}

test "provider listener failure fences commands and wakes authenticated accept" {
    const io = std.testing.io;
    var accepted_storage: [1]std.Io.net.Stream = undefined;
    var accepted_streams = AuthenticatedStreamQueue.init(&accepted_storage);
    var shutdown_requested: std.atomic.Value(bool) = .init(false);
    var provider_failed: std.atomic.Value(bool) = .init(false);
    var failure_context: ProviderIngressFailureContext = .{
        .io = io,
        .accepted_streams = &accepted_streams,
        .shutdown_requested = &shutdown_requested,
        .failed = &provider_failed,
    };

    failure_context.callback().notify(.listener_failed);
    failure_context.callback().notify(.listener_failed);

    try std.testing.expect(shutdown_requested.load(.acquire));
    try std.testing.expect(provider_failed.load(.acquire));
    try std.testing.expectError(error.Closed, accepted_streams.getOne(io));
}

test "running server core bootstraps symbol and exposes settled balances" {
    const allocator = std.testing.allocator;
    const engine = try allocator.create(core.exchange_core.ExchangeCore);
    defer allocator.destroy(engine);

    engine.* = try core.exchange_core.ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .matching = .{ .orderbook_kind = .direct },
    });
    defer engine.deinit();

    var api = core.exchange_api.ExchangeApi.init(allocator, engine);
    defer api.deinit();
    try engine.startup();
    defer engine.shutdown();

    try bootstrapDefaultSymbol(&api, allocator);
    var snapshot = try api.queryOrderBook(DefaultSymbolId, 20);
    defer snapshot.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len);
    try std.testing.expectEqual(@as(usize, 0), snapshot.bids.items.len);

    var add_user = try api.submitCommandSync(.{ .add_user = 1_001 });
    defer add_user.deinit(allocator);
    var deposit = try api.submitCommandSync(.{ .adjust_user_balance = .{
        .uid = 1_001,
        .transaction_id = 1,
        .currency = DefaultQuoteCurrency,
        .amount = 25_000,
        .adjustment_type = .adjustment,
    } });
    defer deposit.deinit(allocator);
    try settleReadBarrier(&api, allocator);
    try std.testing.expectEqual(
        @as(?i64, 25_000),
        try api.queryUserBalance(1_001, DefaultQuoteCurrency),
    );
}

test "Parix local bootstrap uses dynamic profile and is idempotent" {
    const allocator = std.testing.allocator;
    const configured_profile = try testParixProfile(allocator);
    defer configured_profile.deinit();
    const engine = try allocator.create(core.exchange_core.ExchangeCore);
    defer allocator.destroy(engine);

    engine.* = try core.exchange_core.ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .matching = .{ .orderbook_kind = .direct },
    });
    defer engine.deinit();

    var api = core.exchange_api.ExchangeApi.init(allocator, engine);
    defer api.deinit();
    try engine.startup();
    defer engine.shutdown();

    try bootstrapParixLocalProfileProvisionally(&api, allocator, configured_profile);
    try bootstrapParixLocalProfileProvisionally(&api, allocator, configured_profile);
    try settleReadBarrier(&api, allocator);

    var snapshot = try api.queryOrderBook(9001, 20);
    defer snapshot.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len);
    try std.testing.expectEqual(@as(usize, 0), snapshot.bids.items.len);

    try std.testing.expectEqual(
        @as(?i64, 1_234),
        try api.queryUserBalance(7_001, 31),
    );
    try std.testing.expectEqual(
        @as(?i64, 5_678),
        try api.queryUserBalance(7_002, 32),
    );
}

test "Parix local bootstrap provisionally tolerates a saga-advanced counter" {
    const allocator = std.testing.allocator;
    const configured_profile = try testParixProfile(allocator);
    defer configured_profile.deinit();
    const engine = try allocator.create(core.exchange_core.ExchangeCore);
    defer allocator.destroy(engine);

    engine.* = try core.exchange_core.ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .matching = .{ .orderbook_kind = .direct },
    });
    defer engine.deinit();

    var api = core.exchange_api.ExchangeApi.init(allocator, engine);
    defer api.deinit();
    try engine.startup();
    defer engine.shutdown();

    try bootstrapParixLocalProfileProvisionally(&api, allocator, configured_profile);
    const seed_id: i64 = @intCast(configured_profile.initialFunding()[0].id);
    var saga_adjustment = try api.submitCommandSync(.{ .adjust_user_balance = .{
        .uid = 7_001,
        .transaction_id = seed_id + 1,
        .currency = 31,
        .amount = 25,
        .adjustment_type = .adjustment,
    } });
    defer saga_adjustment.deinit(allocator);
    switch (saga_adjustment) {
        .response => |response| try std.testing.expectEqual(
            core.cmd.CommandResultCode.success,
            response.result_code,
        ),
        .persist_state => return error.UnexpectedSagaAdjustmentResult,
    }

    // Restart replays the profile bootstrap before FSG validation. The old
    // seed now reports already_applied_many and must remain side-effect free.
    try bootstrapParixLocalProfileProvisionally(&api, allocator, configured_profile);
    try std.testing.expectEqual(
        @as(?i64, seed_id + 1),
        try api.queryUserAdjustmentCounter(7_001),
    );
    try std.testing.expectEqual(
        @as(?i64, 1_259),
        try api.queryUserBalance(7_001, 31),
    );
}
