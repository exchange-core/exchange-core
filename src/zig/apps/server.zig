const std = @import("std");
const exchange_core = @import("exchange_core");
const protocol = @import("exchange_protocol");
const parix_account_bootstrap = @import("parix_account_bootstrap.zig");
const parix_http_transport = @import("parix_http_transport.zig");
const parix_profile = @import("parix_settlement_profile.zig");
const parix_settlement_runtime = @import("parix_settlement_runtime.zig");

const core = exchange_core.core;
const log = std.log.scoped(.exchange_server);

pub const DefaultHost = "127.0.0.1";
pub const DefaultPort: u16 = 7379;
pub const DefaultSymbolId: i32 = 241;
pub const DefaultBaseCurrency: i32 = 11;
pub const DefaultQuoteCurrency: i32 = 15;
pub const DefaultParixBaseUrl = "https://dev.parix.io";
pub const DefaultPersistenceRoot = "zig-out/runtime/parix";

const MaxRequestLineBytes = 64 * 1024;
const BootstrapTransferId: i32 = 1;

const ArgumentError = error{
    InvalidArguments,
    MissingArgumentValue,
    InvalidPersistenceRoot,
    InvalidPort,
    MissingParixProfile,
    ParixProfileWithoutSettlement,
    ParixProvisioningWithoutSettlement,
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

const ParixEnvironment = struct {
    base_url: []const u8,
    database_id: []const u8,
    auth: parix_http_transport.Auth,
};

pub const Options = struct {
    host: []const u8 = DefaultHost,
    port: u16 = DefaultPort,
    parix_settlement: bool = false,
    parix_profile_path: ?[]const u8 = null,
    parix_create_accounts: bool = false,
    parix_development_seed: bool = false,
    persistence_root: []const u8 = DefaultPersistenceRoot,
    owned_host: ?[]u8 = null,
    owned_parix_profile_path: ?[]u8 = null,
    owned_persistence_root: ?[]u8 = null,

    fn deinit(self: *Options, allocator: std.mem.Allocator) void {
        if (self.owned_host) |owned| allocator.free(owned);
        if (self.owned_parix_profile_path) |owned| allocator.free(owned);
        if (self.owned_persistence_root) |owned| allocator.free(owned);
        self.* = undefined;
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

    try serve(io, allocator, options, &api, null, null);
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

    const parix_environment = try readParixEnvironment(environment);
    try validateParixDatabaseId(parix_environment.database_id, configured_profile);
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

    try settlement_runtime.install(engine);
    _ = try engine.startupFromConfiguredPersistence(
        settlement_runtime.persistenceRoot(),
        .matching_engine_router,
        0,
        0,
    );

    _ = try parix_account_bootstrap.ensureProfile(
        &gateway_client,
        configured_profile,
        parixBootstrapPolicy(options),
    );
    try bootstrapParixLocalProfile(&api, allocator, configured_profile);
    try settlement_runtime.start();
    switch (settlement_runtime.health()) {
        .failed => return error.ParixSettlementFailed,
        .quarantined => return error.ParixSettlementQuarantined,
        else => {},
    }

    try serve(
        io,
        allocator,
        options,
        &api,
        &settlement_runtime,
        configured_profile,
    );

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
    configured_profile: ?*const parix_profile.Profile,
) !void {
    std.debug.assert((settlement_runtime != null) == (configured_profile != null));
    const address = try std.Io.net.IpAddress.resolve(io, options.host, options.port);
    var server = try address.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    var stdout_buffer: [2048]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buffer);
    if (settlement_runtime) |runtime| {
        const symbol = configured_profile.?.symbolSpecification();
        try stdout_writer.interface.print(
            "exchange-core server ready on {s}:{d} symbol={d} base={d} quote={d} settlement={s}\n",
            .{
                options.host,
                options.port,
                symbol.symbol_id,
                symbol.base_currency,
                symbol.quote_currency,
                @tagName(runtime.health()),
            },
        );
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

    while (true) {
        const stream = server.accept(io) catch |err| switch (err) {
            error.ConnectionAborted => continue,
            else => return err,
        };
        const shutdown_requested = serveConnection(
            io,
            allocator,
            stream,
            api,
            settlement_runtime,
            configured_profile,
        ) catch |err| {
            log.warn("client connection failed: {s}", .{@errorName(err)});
            continue;
        };
        if (shutdown_requested) break;
    }
}

fn serveConnection(
    io: std.Io,
    allocator: std.mem.Allocator,
    stream: std.Io.net.Stream,
    api: *core.exchange_api.ExchangeApi,
    settlement_runtime: ?*parix_settlement_runtime.Runtime,
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
    configured_profile: ?*const parix_profile.Profile,
    request: protocol.Request,
    writer: *std.Io.Writer,
) !bool {
    switch (request) {
        .ping => {
            const ready = if (isReady(api, settlement_runtime)) "true" else "false";
            if (settlement_runtime) |runtime| {
                try writer.print(
                    "OK ready={s} settlement={s}\n",
                    .{ ready, @tagName(runtime.health()) },
                );
            } else {
                try writer.print("OK ready={s}\n", .{ready});
            }
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
                configured_profile,
                request,
            );
            defer result.deinit(allocator);
            try writeCommandResult(writer, result);
            return false;
        },
    }
}

fn isReady(
    api: *const core.exchange_api.ExchangeApi,
    settlement_runtime: ?*const parix_settlement_runtime.Runtime,
) bool {
    if (!api.isStarted() or api.isStopped()) return false;
    const runtime = settlement_runtime orelse return true;
    return runtime.health() == .ready;
}

fn submitMutationRequest(
    api: *core.exchange_api.ExchangeApi,
    settlement_runtime: ?*parix_settlement_runtime.Runtime,
    configured_profile: ?*const parix_profile.Profile,
    request: protocol.Request,
) !core.api_command.ApiCommandResult {
    var mutation_runtime: ?*parix_settlement_runtime.Runtime = null;
    defer if (mutation_runtime) |runtime| runtime.endMutation();

    if (settlement_runtime) |runtime| {
        const profile = configured_profile orelse
            return error.ParixProfileUnavailable;
        try validateParixRequest(profile, request);
        if (!runtime.tryBeginMutation()) return error.ParixSettlementNotReady;
        mutation_runtime = runtime;
    }
    return api.submitCommandSync(try apiCommandForRequest(request));
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
        .ping, .balance, .book, .shutdown => return,
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

fn parixBootstrapPolicy(options: Options) parix_account_bootstrap.Policy {
    return .{
        .allow_account_creation = options.parix_create_accounts,
        .allow_initial_funding = options.parix_development_seed,
    };
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
        .ping, .balance, .book, .shutdown => error.NonCommandRequest,
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

fn bootstrapParixLocalProfile(
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
    if (result_code != .success and
        result_code != .user_mgmt_account_balance_adjustment_already_applied_same)
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
        if (std.mem.eql(u8, argument, "--parix-create-accounts")) {
            options.parix_create_accounts = true;
            continue;
        }
        if (std.mem.eql(u8, argument, "--parix-development-seed")) {
            options.parix_development_seed = true;
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
    if (options.parix_settlement and options.parix_profile_path == null) {
        return error.MissingParixProfile;
    }
    if (!options.parix_settlement and options.parix_profile_path != null) {
        return error.ParixProfileWithoutSettlement;
    }
    if (!options.parix_settlement and
        (options.parix_create_accounts or options.parix_development_seed))
    {
        return error.ParixProvisioningWithoutSettlement;
    }
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
        \\Usage: exchange-core-server [--host HOST] [--port PORT]
        \\                            [--parix-settlement --parix-profile PATH]
        \\                            [--parix-create-accounts] [--parix-development-seed]
        \\                            [--persistence-root RELATIVE_PATH]
        \\
        \\Starts a newline-delimited TCP exchange server.
        \\
        \\Options:
        \\  --host HOST                  Bind host or IP address (default: 127.0.0.1)
        \\  --port PORT                  Bind TCP port (default: 7379)
        \\  --parix-settlement           Enable native Parix/TigerBeetle settlement
        \\  --parix-profile PATH          Required operator-owned settlement profile
        \\  --parix-create-accounts       Create missing external accounts (default: validate only)
        \\  --parix-development-seed      Seed configured development funding transfers
        \\  --persistence-root PATH      Safe relative durable root (default: zig-out/runtime/parix)
        \\  -h, --help                   Show this help
        \\
        \\Settlement environment:
        \\  PARIX_DATABASE_ID            Required database UUID
        \\  PARIX_BASE_URL               Optional API origin (default: https://dev.parix.io)
        \\  PARIX_API_KEY                API key credential (exclusive with bearer token)
        \\  PARIX_BEARER_TOKEN            Bearer credential (exclusive with API key)
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

test "server defaults and bootstrap symbol are production-ready" {
    const options: Options = .{};
    try std.testing.expectEqualStrings(DefaultHost, options.host);
    try std.testing.expectEqual(DefaultPort, options.port);
    try std.testing.expect(!options.parix_settlement);
    try std.testing.expectEqual(@as(?[]const u8, null), options.parix_profile_path);
    try std.testing.expect(!options.parix_create_accounts);
    try std.testing.expect(!options.parix_development_seed);
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

test "Parix profile is required exactly with settlement and provisioning defaults fail closed" {
    try validateOptions(.{});
    try std.testing.expectError(
        error.MissingParixProfile,
        validateOptions(.{ .parix_settlement = true }),
    );
    try std.testing.expectError(
        error.ParixProfileWithoutSettlement,
        validateOptions(.{ .parix_profile_path = "profile.json" }),
    );
    try std.testing.expectError(
        error.ParixProvisioningWithoutSettlement,
        validateOptions(.{ .parix_create_accounts = true }),
    );

    const validate_only: Options = .{
        .parix_settlement = true,
        .parix_profile_path = "profile.json",
    };
    try validateOptions(validate_only);
    const validate_policy = parixBootstrapPolicy(validate_only);
    try std.testing.expect(!validate_policy.allow_account_creation);
    try std.testing.expect(!validate_policy.allow_initial_funding);

    const funding_only: Options = .{
        .parix_settlement = true,
        .parix_profile_path = "profile.json",
        .parix_development_seed = true,
    };
    try validateOptions(funding_only);
    const funding_policy = parixBootstrapPolicy(funding_only);
    try std.testing.expect(!funding_policy.allow_account_creation);
    try std.testing.expect(funding_policy.allow_initial_funding);

    const provision_all: Options = .{
        .parix_settlement = true,
        .parix_profile_path = "profile.json",
        .parix_create_accounts = true,
        .parix_development_seed = true,
    };
    try validateOptions(provision_all);
    const provision_policy = parixBootstrapPolicy(provision_all);
    try std.testing.expect(provision_policy.allow_account_creation);
    try std.testing.expect(provision_policy.allow_initial_funding);
}

test "Parix CLI owns profile path and enforces settlement pairing" {
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
        "--parix-development-seed",
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
            try std.testing.expect(!options.parix_create_accounts);
            try std.testing.expect(options.parix_development_seed);
        },
    }
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

    try bootstrapParixLocalProfile(&api, allocator, configured_profile);
    try bootstrapParixLocalProfile(&api, allocator, configured_profile);
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
