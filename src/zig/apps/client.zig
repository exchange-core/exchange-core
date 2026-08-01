const std = @import("std");
const protocol = @import("exchange_protocol");
const ingress_auth = @import("ingress_auth.zig");

pub const default_host = "127.0.0.1";
pub const default_port: u16 = 7379;
pub const max_response_line_bytes: usize = 64 * 1024;
pub const authenticated_exchange_deadline: std.Io.Duration = .fromSeconds(5);

const ingress_key_file_environment = "EXCHANGE_INGRESS_KEY_FILE";
const max_cli_arguments = protocol.max_tokens + 8;

const CollectArgumentsError = error{
    ArgumentIterationFailed,
    TooManyArguments,
};

pub const ClientConfig = struct {
    address: std.Io.net.IpAddress,
    request: protocol.Request,
    auth: ?ClientAuth = null,
};

pub const ClientAuth = struct {
    instance_id: ingress_auth.Identifier,
    credential_id: ingress_auth.Identifier,
};

pub const CliError = protocol.ParseError || error{
    MissingCommand,
    MissingOptionValue,
    UnknownOption,
    DuplicateOption,
    InvalidHost,
    InvalidPort,
    InvalidAuthInstance,
    InvalidAuthCredential,
    IncompleteAuthentication,
    TooManyArguments,
};

const EnvironmentError = error{
    MissingIngressKeyFile,
    IngressKeyFileWithoutAuthentication,
};

const AuthenticatedExchangeError = std.Io.Reader.Error ||
    std.Io.Writer.Error ||
    ingress_auth.WireError ||
    ingress_auth.AuthenticationError ||
    ingress_auth.IntegrityError ||
    error{
        EmptyResponse,
        ResponseTooLong,
        InvalidCanonicalRequest,
    };

const RunError = std.Io.net.IpAddress.ConnectError ||
    std.Io.net.Stream.Reader.Error ||
    std.Io.net.Stream.Writer.Error ||
    std.Io.Writer.Error ||
    std.Io.RandomSecureError ||
    std.Io.ConcurrentError ||
    std.Io.Cancelable ||
    ingress_auth.SecretFileError ||
    AuthenticatedExchangeError ||
    EnvironmentError ||
    error{
        AuthenticatedExchangeDeadlineExceeded,
        EmptyResponse,
        ResponseTooLong,
    };

const AuthenticatedAttemptOutcome = union(enum) {
    response_len: usize,
    failed: RunError,
};

const AuthenticatedDeadlineOutcome = enum {
    expired,
    canceled,
};

const AuthenticatedRaceResult = union(enum) {
    attempt: AuthenticatedAttemptOutcome,
    deadline: AuthenticatedDeadlineOutcome,
};

const AuthenticatedRace = std.Io.Select(AuthenticatedRaceResult);

pub fn main(init: std.process.Init) !u8 {
    var argument_storage: [max_cli_arguments][]const u8 = undefined;
    const argument_count = collectArguments(
        init.minimal.args,
        init.gpa,
        &argument_storage,
    ) catch |err| {
        switch (err) {
            error.TooManyArguments => {
                try writeCliError(init.io, error.TooManyArguments);
                return 2;
            },
            error.ArgumentIterationFailed => return err,
        }
    };

    if (isHelpRequest(argument_storage[0..argument_count])) {
        try writeHelp(init.io);
        return 0;
    }

    const config = parseCli(argument_storage[0..argument_count]) catch |err| {
        try writeCliError(init.io, err);
        return 2;
    };

    run(init.io, init.environ_map, config) catch |err| {
        try writeRuntimeError(init.io, err);
        return 1;
    };
    return 0;
}

pub fn parseCli(args: []const []const u8) CliError!ClientConfig {
    var host: []const u8 = default_host;
    var port = default_port;
    var host_set = false;
    var port_set = false;
    var auth_instance: ?[]const u8 = null;
    var auth_credential: ?[]const u8 = null;
    var index: usize = 0;

    while (index < args.len and std.mem.startsWith(u8, args[index], "--")) {
        const option = args[index];
        if (std.mem.eql(u8, option, "--host")) {
            if (host_set) return error.DuplicateOption;
            index += 1;
            if (index == args.len) return error.MissingOptionValue;
            host = args[index];
            host_set = true;
        } else if (std.mem.eql(u8, option, "--port")) {
            if (port_set) return error.DuplicateOption;
            index += 1;
            if (index == args.len) return error.MissingOptionValue;
            port = parsePort(args[index]) catch return error.InvalidPort;
            port_set = true;
        } else if (std.mem.eql(u8, option, "--auth-instance")) {
            if (auth_instance != null) return error.DuplicateOption;
            index += 1;
            if (index == args.len) return error.MissingOptionValue;
            auth_instance = args[index];
        } else if (std.mem.eql(u8, option, "--auth-credential")) {
            if (auth_credential != null) return error.DuplicateOption;
            index += 1;
            if (index == args.len) return error.MissingOptionValue;
            auth_credential = args[index];
        } else {
            return error.UnknownOption;
        }
        index += 1;
    }

    if (index == args.len) return error.MissingCommand;
    if ((auth_instance == null) != (auth_credential == null)) {
        return error.IncompleteAuthentication;
    }

    const auth: ?ClientAuth = if (auth_instance) |instance_text| auth: {
        const instance_id = ingress_auth.validateInstanceId(instance_text) catch
            return error.InvalidAuthInstance;
        const credential_id = ingress_auth.Identifier.init(auth_credential.?) catch
            return error.InvalidAuthCredential;
        break :auth .{
            .instance_id = instance_id,
            .credential_id = credential_id,
        };
    } else null;

    const address = std.Io.net.IpAddress.parseIp4(host, port) catch return error.InvalidHost;
    return .{
        .address = address,
        .request = try protocol.parseArgs(args[index..]),
        .auth = auth,
    };
}

fn ingressKeyFilePath(
    environment: *const std.process.Environ.Map,
    authenticated: bool,
) EnvironmentError!?[]const u8 {
    const configured = environment.get(ingress_key_file_environment);
    if (!authenticated) {
        if (configured) |path| {
            if (path.len != 0) return error.IngressKeyFileWithoutAuthentication;
        }
        return null;
    }
    const path = configured orelse return error.MissingIngressKeyFile;
    if (path.len == 0) return error.MissingIngressKeyFile;
    return path;
}

fn collectArguments(
    process_args: std.process.Args,
    allocator: std.mem.Allocator,
    storage: *[max_cli_arguments][]const u8,
) CollectArgumentsError!usize {
    var iterator = std.process.Args.Iterator.initAllocator(process_args, allocator) catch
        return error.ArgumentIterationFailed;
    defer iterator.deinit();

    _ = iterator.next();
    var count: usize = 0;
    while (iterator.next()) |argument| {
        if (count == storage.len) return error.TooManyArguments;
        storage[count] = argument;
        count += 1;
    }
    return count;
}

fn isHelpRequest(args: []const []const u8) bool {
    return args.len == 1 and
        (std.mem.eql(u8, args[0], "-h") or std.mem.eql(u8, args[0], "--help"));
}

fn parsePort(raw: []const u8) error{InvalidPort}!u16 {
    const port = std.fmt.parseInt(u16, raw, 10) catch return error.InvalidPort;
    if (port == 0) return error.InvalidPort;
    return port;
}

fn run(
    io: std.Io,
    environment: *const std.process.Environ.Map,
    config: ClientConfig,
) RunError!void {
    const key_path = try ingressKeyFilePath(environment, config.auth != null);
    if (config.auth) |auth| {
        var secret_key = try ingress_auth.loadSecretKey(
            io,
            std.Io.Dir.cwd(),
            key_path.?,
        );
        defer secret_key.deinit();

        var client_nonce: ingress_auth.Nonce = undefined;
        try io.randomSecure(&client_nonce);
        return runAuthenticated(io, config.address, config.request, auth, &secret_key, client_nonce);
    }

    return runDevelopment(io, config);
}

fn runDevelopment(io: std.Io, config: ClientConfig) RunError!void {
    const stream = try config.address.connect(io, .{
        .mode = .stream,
        .protocol = .tcp,
    });
    defer stream.close(io);

    var request_buffer: [protocol.max_line_bytes + 1]u8 = undefined;
    var stream_writer = stream.writer(io, &request_buffer);
    protocol.writeRequest(&stream_writer.interface, config.request) catch |err| switch (err) {
        error.WriteFailed => return stream_writer.err.?,
    };
    stream_writer.interface.flush() catch |err| switch (err) {
        error.WriteFailed => return stream_writer.err.?,
    };

    var response_buffer: [max_response_line_bytes + 1]u8 = undefined;
    var stream_reader = stream.reader(io, &response_buffer);
    const maybe_response = stream_reader.interface.takeDelimiter('\n') catch |err| switch (err) {
        error.ReadFailed => return stream_reader.err.?,
        error.StreamTooLong => return error.ResponseTooLong,
    };
    const response = maybe_response orelse return error.EmptyResponse;
    if (response.len > max_response_line_bytes) return error.ResponseTooLong;

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buffer);
    try stdout_writer.interface.writeAll(response);
    try stdout_writer.interface.writeAll("\n");
    try stdout_writer.interface.flush();
}

fn runAuthenticated(
    io: std.Io,
    address: std.Io.net.IpAddress,
    request: protocol.Request,
    auth: ClientAuth,
    secret_key: *const ingress_auth.SecretKey,
    client_nonce: ingress_auth.Nonce,
) RunError!void {
    return runAuthenticatedWithin(
        io,
        address,
        request,
        auth,
        secret_key,
        client_nonce,
        authenticated_exchange_deadline,
    );
}

fn runAuthenticatedWithin(
    io: std.Io,
    address: std.Io.net.IpAddress,
    request: protocol.Request,
    auth: ClientAuth,
    secret_key: *const ingress_auth.SecretKey,
    client_nonce: ingress_auth.Nonce,
    deadline_duration: std.Io.Duration,
) RunError!void {
    const absolute_deadline = (std.Io.Timeout{ .duration = .{
        .raw = deadline_duration,
        .clock = .boot,
    } }).toDeadline(io);
    var response_storage: [ingress_auth.max_response_bytes + 1]u8 = undefined;
    var result_storage: [2]AuthenticatedRaceResult = undefined;
    var race = AuthenticatedRace.init(io, &result_storage);
    try race.concurrent(
        .attempt,
        performAuthenticatedAttempt,
        .{ io, address, request, auth, secret_key, client_nonce, &response_storage },
    );
    defer race.cancelDiscard();
    try race.concurrent(
        .deadline,
        waitForAuthenticatedDeadline,
        .{ absolute_deadline, io },
    );

    const first = try race.await();

    const response_len = switch (first) {
        .attempt => |outcome| switch (outcome) {
            .response_len => |length| length,
            .failed => |err| return err,
        },
        .deadline => |outcome| return switch (outcome) {
            .expired => error.AuthenticatedExchangeDeadlineExceeded,
            .canceled => error.Canceled,
        },
    };

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buffer);
    try stdout_writer.interface.writeAll(response_storage[0..response_len]);
    try stdout_writer.interface.flush();
}

fn performAuthenticatedAttempt(
    io: std.Io,
    address: std.Io.net.IpAddress,
    request: protocol.Request,
    auth: ClientAuth,
    secret_key: *const ingress_auth.SecretKey,
    client_nonce: ingress_auth.Nonce,
    response_storage: []u8,
) AuthenticatedAttemptOutcome {
    const response_len = runAuthenticatedAttempt(
        io,
        address,
        request,
        auth,
        secret_key,
        client_nonce,
        response_storage,
    ) catch |err| return .{ .failed = err };
    return .{ .response_len = response_len };
}

fn runAuthenticatedAttempt(
    io: std.Io,
    address: std.Io.net.IpAddress,
    request: protocol.Request,
    auth: ClientAuth,
    secret_key: *const ingress_auth.SecretKey,
    client_nonce: ingress_auth.Nonce,
    response_storage: []u8,
) RunError!usize {
    var raw_command_storage: [protocol.max_line_bytes + 1]u8 = undefined;
    const raw_command = try canonicalRequest(request, &raw_command_storage);

    const stream = try address.connect(io, .{
        .mode = .stream,
        .protocol = .tcp,
    });
    defer stream.close(io);

    var request_buffer: [ingress_auth.max_request_envelope_bytes + 1]u8 = undefined;
    var stream_writer = stream.writer(io, &request_buffer);
    var response_buffer: [ingress_auth.max_response_envelope_bytes + 1]u8 = undefined;
    var stream_reader = stream.reader(io, &response_buffer);
    var response_writer = std.Io.Writer.fixed(response_storage);

    try exchangeAuthenticated(
        &stream_reader.interface,
        &stream_writer.interface,
        &response_writer,
        auth,
        secret_key,
        client_nonce,
        raw_command,
    );
    return response_writer.buffered().len;
}

fn waitForAuthenticatedDeadline(
    absolute_deadline: std.Io.Timeout,
    io: std.Io,
) AuthenticatedDeadlineOutcome {
    absolute_deadline.sleep(io) catch return .canceled;
    return .expired;
}

fn canonicalRequest(
    request: protocol.Request,
    storage: *[protocol.max_line_bytes + 1]u8,
) error{InvalidCanonicalRequest}![]const u8 {
    var writer = std.Io.Writer.fixed(storage);
    protocol.writeRequest(&writer, request) catch return error.InvalidCanonicalRequest;
    const line = writer.buffered();
    if (line.len == 0 or line[line.len - 1] != '\n') {
        return error.InvalidCanonicalRequest;
    }
    return line[0 .. line.len - 1];
}

fn exchangeAuthenticated(
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    output: *std.Io.Writer,
    config: ClientAuth,
    secret_key: *const ingress_auth.SecretKey,
    client_nonce: ingress_auth.Nonce,
    raw_command: []const u8,
) AuthenticatedExchangeError!void {
    const auth: ingress_auth.Auth = .{
        .instance_id = config.instance_id,
        .credential_id = config.credential_id,
        .client_nonce = client_nonce,
    };
    try ingress_auth.writeAuth(writer, auth);
    try writer.flush();

    const challenge = try ingress_auth.parseChallenge(try readRequiredLine(reader));
    const transcript = ingress_auth.Transcript.init(auth, challenge);

    var proof_mac = ingress_auth.computeProof(secret_key, transcript);
    defer std.crypto.secureZero(u8, &proof_mac);
    try ingress_auth.writeProof(writer, .{ .mac = proof_mac });
    try writer.flush();

    const ack = try ingress_auth.parseAuthAck(try readRequiredLine(reader));
    var session_key = ingress_auth.deriveSessionKey(secret_key, transcript);
    defer session_key.deinit();
    try ingress_auth.verifyServerMac(&session_key, transcript, ack.server_mac);

    var request_mac = try ingress_auth.computeRequestMac(
        &session_key,
        config.instance_id.slice(),
        config.credential_id.slice(),
        ingress_auth.sequence_one,
        raw_command,
    );
    defer std.crypto.secureZero(u8, &request_mac);
    try ingress_auth.writeRequestEnvelope(writer, .{
        .sequence = ingress_auth.sequence_one,
        .request_mac = request_mac,
        .raw_command = raw_command,
    });
    try writer.flush();

    const response = try ingress_auth.parseResponseEnvelope(try readRequiredLine(reader));
    try ingress_auth.verifyResponseMac(
        &session_key,
        config.instance_id.slice(),
        config.credential_id.slice(),
        response.sequence,
        response.raw_response,
        response.response_mac,
    );
    try output.writeAll(response.raw_response);
    try output.writeByte('\n');
}

fn readRequiredLine(reader: *std.Io.Reader) AuthenticatedExchangeError![]const u8 {
    const line = reader.takeDelimiter('\n') catch |err| switch (err) {
        error.ReadFailed => return error.ReadFailed,
        error.StreamTooLong => return error.ResponseTooLong,
    };
    return line orelse error.EmptyResponse;
}

fn writeCliError(io: std.Io, err: CliError) std.Io.Writer.Error!void {
    var buffer: [4096]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writer(io, &buffer);
    try writeCliErrorMessage(&stderr_writer.interface, err);
    try stderr_writer.interface.flush();
}

fn writeCliErrorMessage(writer: *std.Io.Writer, err: CliError) std.Io.Writer.Error!void {
    try writer.print("error: invalid command line ({s})\n\n", .{@errorName(err)});
    try writeUsage(writer);
}

fn writeHelp(io: std.Io) std.Io.Writer.Error!void {
    var buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &buffer);
    try writeUsage(&stdout_writer.interface);
    try stdout_writer.interface.flush();
}

pub fn writeUsage(writer: *std.Io.Writer) std.Io.Writer.Error!void {
    try writer.print(
        "Usage: exchange-core-client [--host IPv4] [--port PORT] " ++
            "[--auth-instance UUID --auth-credential ID] COMMAND...\n" ++
            "Authenticated mode requires the key file path in " ++
            ingress_key_file_environment ++ "; supply it with both auth options, " ++
            "or omit all three.\n{s}",
        .{protocol.usage},
    );
}

fn writeRuntimeError(io: std.Io, err: RunError) std.Io.Writer.Error!void {
    var buffer: [512]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writer(io, &buffer);
    try stderr_writer.interface.print("error: request failed ({s})\n", .{@errorName(err)});
    try stderr_writer.interface.flush();
}

test "parseCli uses the loopback defaults" {
    try std.testing.expectEqual(protocol.max_tokens + 8, max_cli_arguments);
    const config = try parseCli(&.{ "add-user", "7" });
    try std.testing.expectEqualDeep(
        std.Io.net.IpAddress{ .ip4 = std.Io.net.Ip4Address.loopback(default_port) },
        config.address,
    );
    try std.testing.expectEqualDeep(
        protocol.Request{ .add_user = .{ .uid = 7 } },
        config.request,
    );
    try std.testing.expect(config.auth == null);
}

test "parseCli accepts an explicit IPv4 address and port" {
    const config = try parseCli(&.{
        "--host",
        "192.0.2.10",
        "--port",
        "9000",
        "book",
        "241",
        "5",
    });
    try std.testing.expectEqual(@as(u16, 9000), config.address.getPort());
    try std.testing.expectEqualDeep(
        protocol.Request{ .book = .{ .symbol = 241, .depth = 5 } },
        config.request,
    );
}

test "parseCli rejects invalid options and endpoints" {
    try std.testing.expectError(error.MissingCommand, parseCli(&.{}));
    try std.testing.expectError(error.MissingOptionValue, parseCli(&.{"--host"}));
    try std.testing.expectError(error.UnknownOption, parseCli(&.{ "--hostname", "localhost", "ping" }));
    try std.testing.expectError(error.DuplicateOption, parseCli(&.{ "--port", "1", "--port", "2", "ping" }));
    try std.testing.expectError(error.InvalidPort, parseCli(&.{ "--port", "0", "ping" }));
    try std.testing.expectError(error.InvalidPort, parseCli(&.{ "--port", "65536", "ping" }));
    try std.testing.expectError(error.InvalidHost, parseCli(&.{ "--host", "localhost", "ping" }));
    try std.testing.expectError(error.InvalidHost, parseCli(&.{ "--host", "127.0.0.999", "ping" }));
}

test "parseCli validates the command through the shared protocol" {
    try std.testing.expectError(error.InvalidArgumentCount, parseCli(&.{"add-user"}));
    try std.testing.expectError(error.InvalidSide, parseCli(&.{ "place", "1", "7", "241", "100", "2", "buy" }));
}

test "parseCli requires paired public authentication identifiers" {
    const instance = "11111111-2222-3333-4444-555555555555";
    const config = try parseCli(&.{
        "--auth-credential",
        "trader-7",
        "--auth-instance",
        instance,
        "ping",
    });
    try std.testing.expectEqualStrings(instance, config.auth.?.instance_id.slice());
    try std.testing.expectEqualStrings("trader-7", config.auth.?.credential_id.slice());

    try std.testing.expectError(
        error.IncompleteAuthentication,
        parseCli(&.{ "--auth-instance", instance, "ping" }),
    );
    try std.testing.expectError(
        error.IncompleteAuthentication,
        parseCli(&.{ "--auth-credential", "trader-7", "ping" }),
    );
    try std.testing.expectError(
        error.DuplicateOption,
        parseCli(&.{
            "--auth-instance",
            instance,
            "--auth-instance",
            instance,
            "--auth-credential",
            "trader-7",
            "ping",
        }),
    );
    try std.testing.expectError(
        error.DuplicateOption,
        parseCli(&.{
            "--auth-instance",
            instance,
            "--auth-credential",
            "trader-7",
            "--auth-credential",
            "operator",
            "ping",
        }),
    );
}

test "parseCli rejects noncanonical authentication identities" {
    const credential_flag = [_][]const u8{ "--auth-credential", "trader-7", "ping" };
    try std.testing.expectError(
        error.InvalidAuthInstance,
        parseCli(&.{
            "--auth-instance",
            "11111111-2222-3333-4444-55555555555",
            credential_flag[0],
            credential_flag[1],
            credential_flag[2],
        }),
    );
    try std.testing.expectError(
        error.InvalidAuthInstance,
        parseCli(&.{
            "--auth-instance",
            "11111111-2222-3333-4444-55555555555A",
            credential_flag[0],
            credential_flag[1],
            credential_flag[2],
        }),
    );
    try std.testing.expectError(
        error.InvalidAuthInstance,
        parseCli(&.{
            "--auth-instance",
            "00000000-0000-0000-0000-000000000000",
            credential_flag[0],
            credential_flag[1],
            credential_flag[2],
        }),
    );
    try std.testing.expectError(
        error.InvalidAuthCredential,
        parseCli(&.{
            "--auth-instance",
            "11111111-2222-3333-4444-555555555555",
            "--auth-credential",
            "not valid",
            "ping",
        }),
    );
}

test "authentication flags and environment key path form a fail-closed triad" {
    const allocator = std.testing.allocator;
    var environment = std.process.Environ.Map.init(allocator);
    defer environment.deinit();

    try std.testing.expectEqual(
        @as(?[]const u8, null),
        try ingressKeyFilePath(&environment, false),
    );
    try std.testing.expectError(
        error.MissingIngressKeyFile,
        ingressKeyFilePath(&environment, true),
    );

    try environment.put(ingress_key_file_environment, "");
    try std.testing.expectError(
        error.MissingIngressKeyFile,
        ingressKeyFilePath(&environment, true),
    );
    try std.testing.expectEqual(
        @as(?[]const u8, null),
        try ingressKeyFilePath(&environment, false),
    );

    try environment.put(ingress_key_file_environment, "/run/secrets/exchange-ingress.key");
    try std.testing.expectEqualStrings(
        "/run/secrets/exchange-ingress.key",
        (try ingressKeyFilePath(&environment, true)).?,
    );
    try std.testing.expectError(
        error.IngressKeyFileWithoutAuthentication,
        ingressKeyFilePath(&environment, false),
    );
}

fn testClientAuth() !ClientAuth {
    return .{
        .instance_id = try ingress_auth.validateInstanceId(
            "11111111-2222-3333-4444-555555555555",
        ),
        .credential_id = try ingress_auth.Identifier.init("trader-7"),
    };
}

fn testSecretKey() ingress_auth.SecretKey {
    var key: ingress_auth.SecretKey = undefined;
    for (&key.bytes, 0..) |*byte, index| byte.* = @intCast(index + 1);
    return key;
}

fn testNonce(seed: u8) ingress_auth.Nonce {
    var nonce: ingress_auth.Nonce = undefined;
    for (&nonce, 0..) |*byte, index| byte.* = seed +% @as(u8, @intCast(index));
    return nonce;
}

fn writeTestServerTranscript(
    storage: *[1024]u8,
    secret_key: *const ingress_auth.SecretKey,
    config: ClientAuth,
    client_nonce: ingress_auth.Nonce,
    server_nonce: ingress_auth.Nonce,
    raw_response: []const u8,
) ![]u8 {
    const auth: ingress_auth.Auth = .{
        .instance_id = config.instance_id,
        .credential_id = config.credential_id,
        .client_nonce = client_nonce,
    };
    const challenge: ingress_auth.Challenge = .{ .server_nonce = server_nonce };
    const transcript = ingress_auth.Transcript.init(auth, challenge);
    var session_key = ingress_auth.deriveSessionKey(secret_key, transcript);
    defer session_key.deinit();
    var server_mac = ingress_auth.computeServerMac(&session_key, transcript);
    defer std.crypto.secureZero(u8, &server_mac);
    var response_mac = try ingress_auth.computeResponseMac(
        &session_key,
        config.instance_id.slice(),
        config.credential_id.slice(),
        ingress_auth.sequence_one,
        raw_response,
    );
    defer std.crypto.secureZero(u8, &response_mac);

    var writer = std.Io.Writer.fixed(storage);
    try ingress_auth.writeChallenge(&writer, challenge);
    try ingress_auth.writeAuthAck(&writer, .{ .server_mac = server_mac });
    try ingress_auth.writeResponseEnvelope(&writer, .{
        .sequence = ingress_auth.sequence_one,
        .response_mac = response_mac,
        .raw_response = raw_response,
    });
    return writer.buffered();
}

const StallingPeerStage = enum(u8) {
    starting,
    auth_received,
    challenge_sent,
    proof_received,
    ack_sent,
    request_received,
    response_sent,
};

const StallingPeerContext = struct {
    io: std.Io,
    listener: *std.Io.net.Server,
    secret_key: *const ingress_auth.SecretKey,
    server_nonce: ingress_auth.Nonce,
    first_stall: std.Io.Duration,
    second_stall: std.Io.Duration,
    stage: std.atomic.Value(u8) = .init(@intFromEnum(StallingPeerStage.starting)),

    fn record(self: *StallingPeerContext, stage: StallingPeerStage) void {
        self.stage.store(@intFromEnum(stage), .release);
    }

    fn observed(self: *const StallingPeerContext) StallingPeerStage {
        return @enumFromInt(self.stage.load(.acquire));
    }
};

fn runCumulativeStallingPeer(context: *StallingPeerContext) !void {
    const stream = try context.listener.accept(context.io);
    defer stream.close(context.io);

    var read_storage: [ingress_auth.max_request_envelope_bytes + 1]u8 = undefined;
    var stream_reader = stream.reader(context.io, &read_storage);
    var write_storage: [ingress_auth.max_response_envelope_bytes + 1]u8 = undefined;
    var stream_writer = stream.writer(context.io, &write_storage);
    const reader = &stream_reader.interface;
    const writer = &stream_writer.interface;

    const auth_line = (try reader.takeDelimiter('\n')) orelse return error.EndOfStream;
    const auth = try ingress_auth.parseAuth(auth_line);
    context.record(.auth_received);

    try std.Io.sleep(context.io, context.first_stall, .boot);
    const challenge: ingress_auth.Challenge = .{ .server_nonce = context.server_nonce };
    const transcript = ingress_auth.Transcript.init(auth, challenge);
    try ingress_auth.writeChallenge(writer, challenge);
    try writer.flush();
    context.record(.challenge_sent);

    const proof_line = (try reader.takeDelimiter('\n')) orelse return error.EndOfStream;
    const proof = try ingress_auth.parseProof(proof_line);
    var expected_proof = ingress_auth.computeProof(context.secret_key, transcript);
    defer std.crypto.secureZero(u8, &expected_proof);
    if (!std.crypto.timing_safe.eql(ingress_auth.Mac, expected_proof, proof.mac)) {
        return error.InvalidTestProof;
    }
    context.record(.proof_received);

    try std.Io.sleep(context.io, context.second_stall, .boot);
    var session_key = ingress_auth.deriveSessionKey(context.secret_key, transcript);
    defer session_key.deinit();
    var server_mac = ingress_auth.computeServerMac(&session_key, transcript);
    defer std.crypto.secureZero(u8, &server_mac);
    try ingress_auth.writeAuthAck(writer, .{ .server_mac = server_mac });
    try writer.flush();
    context.record(.ack_sent);

    const request_line = (try reader.takeDelimiter('\n')) orelse return error.EndOfStream;
    const request = try ingress_auth.parseRequestEnvelope(request_line);
    try ingress_auth.verifyRequestMac(
        &session_key,
        auth.instance_id.slice(),
        auth.credential_id.slice(),
        request.sequence,
        request.raw_command,
        request.request_mac,
    );
    context.record(.request_received);

    const raw_response = "OK readiness=true";
    var response_mac = try ingress_auth.computeResponseMac(
        &session_key,
        auth.instance_id.slice(),
        auth.credential_id.slice(),
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
    context.record(.response_sent);
}

test "authenticated readiness uses one absolute deadline across cumulative peer stalls" {
    const io = std.testing.io;
    const listen_address: std.Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var listener = try listen_address.listen(io, .{});
    defer listener.deinit(io);

    var secret_key = testSecretKey();
    defer secret_key.deinit();
    var peer_context: StallingPeerContext = .{
        .io = io,
        .listener = &listener,
        .secret_key = &secret_key,
        .server_nonce = testNonce(0x80),
        .first_stall = .fromMilliseconds(100),
        .second_stall = .fromMilliseconds(200),
    };
    var peer = try io.concurrent(runCumulativeStallingPeer, .{&peer_context});
    defer peer.cancel(io) catch {};

    try std.testing.expectError(
        error.AuthenticatedExchangeDeadlineExceeded,
        runAuthenticatedWithin(
            io,
            listener.socket.address,
            .ping,
            try testClientAuth(),
            &secret_key,
            testNonce(0x20),
            .fromMilliseconds(250),
        ),
    );
    const observed = peer_context.observed();
    try std.testing.expect(@intFromEnum(observed) >= @intFromEnum(StallingPeerStage.proof_received));
    try std.testing.expect(@intFromEnum(observed) < @intFromEnum(StallingPeerStage.ack_sent));
}

test "authenticated exchange orders frames and prints only verified raw response" {
    const config = try testClientAuth();
    var secret_key = testSecretKey();
    defer secret_key.deinit();
    const client_nonce = testNonce(0x20);
    const server_nonce = testNonce(0x80);

    var server_storage: [1024]u8 = undefined;
    const server_transcript = try writeTestServerTranscript(
        &server_storage,
        &secret_key,
        config,
        client_nonce,
        server_nonce,
        "OK pong value=ready",
    );
    var server_reader = std.Io.Reader.fixed(server_transcript);
    var wire_storage: [2048]u8 = undefined;
    var wire_writer = std.Io.Writer.fixed(&wire_storage);
    var output_storage: [256]u8 = undefined;
    var output_writer = std.Io.Writer.fixed(&output_storage);

    try exchangeAuthenticated(
        &server_reader,
        &wire_writer,
        &output_writer,
        config,
        &secret_key,
        client_nonce,
        "ping",
    );
    try std.testing.expectEqualStrings("OK pong value=ready\n", output_writer.buffered());

    var wire_reader = std.Io.Reader.fixed(wire_writer.buffered());
    const auth = try ingress_auth.parseAuth((try wire_reader.takeDelimiter('\n')).?);
    try std.testing.expectEqualStrings(config.instance_id.slice(), auth.instance_id.slice());
    try std.testing.expectEqualStrings(config.credential_id.slice(), auth.credential_id.slice());
    try std.testing.expectEqualSlices(u8, &client_nonce, &auth.client_nonce);

    const challenge: ingress_auth.Challenge = .{ .server_nonce = server_nonce };
    const transcript = ingress_auth.Transcript.init(auth, challenge);
    const proof = try ingress_auth.parseProof((try wire_reader.takeDelimiter('\n')).?);
    var expected_proof = ingress_auth.computeProof(&secret_key, transcript);
    defer std.crypto.secureZero(u8, &expected_proof);
    try std.testing.expectEqualSlices(u8, &expected_proof, &proof.mac);

    const request = try ingress_auth.parseRequestEnvelope(
        (try wire_reader.takeDelimiter('\n')).?,
    );
    var session_key = ingress_auth.deriveSessionKey(&secret_key, transcript);
    defer session_key.deinit();
    try ingress_auth.verifyRequestMac(
        &session_key,
        config.instance_id.slice(),
        config.credential_id.slice(),
        request.sequence,
        request.raw_command,
        request.request_mac,
    );
    try std.testing.expectEqualStrings("ping", request.raw_command);
    try std.testing.expectEqual(null, try wire_reader.takeDelimiter('\n'));
}

test "verified signed application errors retain legacy output behavior" {
    const config = try testClientAuth();
    var secret_key = testSecretKey();
    defer secret_key.deinit();
    const client_nonce = testNonce(0x20);

    var server_storage: [1024]u8 = undefined;
    const server_transcript = try writeTestServerTranscript(
        &server_storage,
        &secret_key,
        config,
        client_nonce,
        testNonce(0x80),
        "ERR kind=authorization message=Forbidden",
    );
    var server_reader = std.Io.Reader.fixed(server_transcript);
    var wire_storage: [2048]u8 = undefined;
    var wire_writer = std.Io.Writer.fixed(&wire_storage);
    var output_storage: [256]u8 = undefined;
    var output_writer = std.Io.Writer.fixed(&output_storage);

    try exchangeAuthenticated(
        &server_reader,
        &wire_writer,
        &output_writer,
        config,
        &secret_key,
        client_nonce,
        "ping",
    );
    try std.testing.expectEqualStrings(
        "ERR kind=authorization message=Forbidden\n",
        output_writer.buffered(),
    );
}

test "authenticated exchange rejects malformed challenge transcript" {
    const config = try testClientAuth();
    var secret_key = testSecretKey();
    defer secret_key.deinit();

    var challenge_storage: [128]u8 = undefined;
    var challenge_writer = std.Io.Writer.fixed(&challenge_storage);
    try ingress_auth.writeChallenge(&challenge_writer, .{ .server_nonce = testNonce(0x80) });
    const malformed = challenge_writer.buffered();
    malformed["CHALLENGE ".len] = '2';

    var reader = std.Io.Reader.fixed(malformed);
    var wire_storage: [512]u8 = undefined;
    var wire_writer = std.Io.Writer.fixed(&wire_storage);
    var output_storage: [64]u8 = undefined;
    var output_writer = std.Io.Writer.fixed(&output_storage);
    try std.testing.expectError(
        error.UnsupportedVersion,
        exchangeAuthenticated(
            &reader,
            &wire_writer,
            &output_writer,
            config,
            &secret_key,
            testNonce(0x20),
            "ping",
        ),
    );
    try std.testing.expectEqual(@as(usize, 0), output_writer.buffered().len);
}

test "authenticated exchange verifies mutual ack and response MAC" {
    const config = try testClientAuth();
    var secret_key = testSecretKey();
    defer secret_key.deinit();
    const client_nonce = testNonce(0x20);
    const server_nonce = testNonce(0x80);

    var ack_storage: [1024]u8 = undefined;
    const bad_ack = try writeTestServerTranscript(
        &ack_storage,
        &secret_key,
        config,
        client_nonce,
        server_nonce,
        "OK pong",
    );
    const ack_start = std.mem.find(u8, bad_ack, "OK AUTH 1 ").? + "OK AUTH 1 ".len;
    bad_ack[ack_start] = if (bad_ack[ack_start] == '0') '1' else '0';
    var ack_reader = std.Io.Reader.fixed(bad_ack);
    var ack_wire_storage: [2048]u8 = undefined;
    var ack_wire_writer = std.Io.Writer.fixed(&ack_wire_storage);
    var ack_output_storage: [64]u8 = undefined;
    var ack_output_writer = std.Io.Writer.fixed(&ack_output_storage);
    try std.testing.expectError(
        error.AuthenticationFailed,
        exchangeAuthenticated(
            &ack_reader,
            &ack_wire_writer,
            &ack_output_writer,
            config,
            &secret_key,
            client_nonce,
            "ping",
        ),
    );
    var rejected_wire = std.Io.Reader.fixed(ack_wire_writer.buffered());
    _ = try ingress_auth.parseAuth((try rejected_wire.takeDelimiter('\n')).?);
    _ = try ingress_auth.parseProof((try rejected_wire.takeDelimiter('\n')).?);
    try std.testing.expectEqual(null, try rejected_wire.takeDelimiter('\n'));

    var response_storage: [1024]u8 = undefined;
    const bad_response = try writeTestServerTranscript(
        &response_storage,
        &secret_key,
        config,
        client_nonce,
        server_nonce,
        "OK pong",
    );
    const response_start = std.mem.find(u8, bad_response, "RES 1 ").? + "RES 1 ".len;
    bad_response[response_start] = if (bad_response[response_start] == '0') '1' else '0';
    var response_reader = std.Io.Reader.fixed(bad_response);
    var response_wire_storage: [2048]u8 = undefined;
    var response_wire_writer = std.Io.Writer.fixed(&response_wire_storage);
    var response_output_storage: [64]u8 = undefined;
    var response_output_writer = std.Io.Writer.fixed(&response_output_storage);
    try std.testing.expectError(
        error.IntegrityFailed,
        exchangeAuthenticated(
            &response_reader,
            &response_wire_writer,
            &response_output_writer,
            config,
            &secret_key,
            client_nonce,
            "ping",
        ),
    );
    try std.testing.expectEqual(@as(usize, 0), response_output_writer.buffered().len);
}

test "authenticated canonical command excludes the legacy line feed" {
    var storage: [protocol.max_line_bytes + 1]u8 = undefined;
    try std.testing.expectEqualStrings(
        "place 91 7 241 100 2 bid gtc",
        try canonicalRequest(
            .{ .place = .{
                .order_id = 91,
                .uid = 7,
                .symbol = 241,
                .price = 100,
                .size = 2,
                .side = .bid,
                .time_in_force = .gtc,
            } },
            &storage,
        ),
    );
}

test "authentication help and errors never render argument key material" {
    const marker = "super-sensitive-key-material";
    var storage: [4096]u8 = undefined;
    var writer = std.Io.Writer.fixed(&storage);
    if (parseCli(&.{ "--auth-key", marker, "ping" })) |_| {
        return error.ExpectedUnknownOption;
    } else |err| {
        try writeCliErrorMessage(&writer, err);
    }
    const rendered = writer.buffered();
    try std.testing.expect(std.mem.find(u8, rendered, marker) == null);
    try std.testing.expect(std.mem.find(u8, rendered, "--auth-key") == null);
    try std.testing.expect(std.mem.find(u8, rendered, ingress_key_file_environment) != null);
}

test "help aliases render successful client usage" {
    try std.testing.expect(isHelpRequest(&.{"-h"}));
    try std.testing.expect(isHelpRequest(&.{"--help"}));
    try std.testing.expect(!isHelpRequest(&.{ "--help", "ping" }));

    var storage: [4096]u8 = undefined;
    var writer = std.Io.Writer.fixed(&storage);
    try writeUsage(&writer);
    const rendered = writer.buffered();
    try std.testing.expect(std.mem.startsWith(
        u8,
        rendered,
        "Usage: exchange-core-client [--host IPv4] [--port PORT] " ++
            "[--auth-instance UUID --auth-credential ID] COMMAND...\n",
    ));
    try std.testing.expect(std.mem.find(u8, rendered, "  shutdown\n") != null);
}
