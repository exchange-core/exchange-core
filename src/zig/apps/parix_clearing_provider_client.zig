const std = @import("std");
const builtin = @import("builtin");
const provider = @import("parix_clearing_provider_adapter.zig");

comptime {
    if (builtin.target.cpu.arch.isWasm()) {
        @compileError("the Parix clearing provider client is native-only");
    }
}

pub const default_host = "127.0.0.1";
pub const default_port: u16 = 7380;
pub const exchange_deadline: std.Io.Duration = .fromSeconds(5);
pub const max_arguments: usize = 10;
pub const max_argument_bytes: usize = 4 * 1024;
pub const max_response_head_bytes: usize = 8 * 1024;
pub const max_response_body_bytes: usize = 4 * 1024;
pub const max_response_bytes: usize = max_response_head_bytes + max_response_body_bytes + 4;
pub const max_request_bytes: usize = provider.max_body_bytes + 1024;

pub const Config = struct {
    profile_path: []const u8,
    body_file_path: []const u8,
    host: []const u8 = default_host,
    port: u16 = default_port,
    timestamp: ?i64 = null,
};

pub const CliError = error{
    MissingOptionValue,
    MissingRequiredOption,
    DuplicateOption,
    UnknownOption,
    InvalidHost,
    InvalidPort,
    InvalidTimestamp,
    ArgumentTooLong,
    TooManyArguments,
};

pub const ResponseError = error{
    EmptyResponse,
    ResponseTooLarge,
    MalformedResponse,
    DuplicateContentLength,
    ConflictingContentLength,
    MissingContentLength,
    ResponseBodyTooLarge,
    UnexpectedResponseBodyLength,
    RequestRejected,
};

const ResponseHead = struct {
    status: u16,
    body_offset: usize,
    content_length: usize,
};

const CollectArgumentsError = error{ ArgumentIterationFailed, TooManyArguments };

pub fn main(init: std.process.Init) !u8 {
    var argument_storage: [max_arguments][]const u8 = undefined;
    const argument_count = collectArguments(init.minimal.args, init.gpa, &argument_storage) catch |err| {
        if (err == error.TooManyArguments) {
            try writeStderr(init.io, "error: invalid arguments\n");
            return 2;
        }
        return err;
    };
    if (argument_count == 1 and
        (std.mem.eql(u8, argument_storage[0], "-h") or
            std.mem.eql(u8, argument_storage[0], "--help")))
    {
        try writeStdout(init.io,
            \\Usage: exchange-core-provider-client --profile PATH --body-file PATH [--host 127.0.0.1] [--port PORT] [--timestamp UNIX_SECONDS]
            \\
        );
        return 0;
    }
    const config = parseCli(argument_storage[0..argument_count]) catch {
        try writeStderr(init.io, "error: invalid arguments\n");
        return 2;
    };
    run(init.io, init.gpa, init.environ_map, config) catch {
        try writeStderr(init.io, "error: callback failed\n");
        return 1;
    };
    return 0;
}

pub fn parseCli(args: []const []const u8) CliError!Config {
    if (args.len > max_arguments) return error.TooManyArguments;
    for (args) |argument| if (argument.len > max_argument_bytes) return error.ArgumentTooLong;
    var config: Config = .{ .profile_path = "", .body_file_path = "" };
    var profile_set = false;
    var body_set = false;
    var host_set = false;
    var port_set = false;
    var timestamp_set = false;
    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const option = args[index];
        index += 1;
        if (index == args.len) return error.MissingOptionValue;
        const value = args[index];
        if (std.mem.eql(u8, option, "--profile")) {
            if (profile_set) return error.DuplicateOption;
            if (value.len == 0) return error.MissingOptionValue;
            config.profile_path = value;
            profile_set = true;
        } else if (std.mem.eql(u8, option, "--body-file")) {
            if (body_set) return error.DuplicateOption;
            if (value.len == 0) return error.MissingOptionValue;
            config.body_file_path = value;
            body_set = true;
        } else if (std.mem.eql(u8, option, "--host")) {
            if (host_set) return error.DuplicateOption;
            if (!std.mem.eql(u8, value, default_host)) return error.InvalidHost;
            config.host = value;
            host_set = true;
        } else if (std.mem.eql(u8, option, "--port")) {
            if (port_set) return error.DuplicateOption;
            config.port = parseCanonicalPositive(u16, value) catch return error.InvalidPort;
            port_set = true;
        } else if (std.mem.eql(u8, option, "--timestamp")) {
            if (timestamp_set) return error.DuplicateOption;
            config.timestamp = parseCanonicalPositive(i64, value) catch return error.InvalidTimestamp;
            timestamp_set = true;
        } else {
            return error.UnknownOption;
        }
    }
    if (!profile_set or !body_set) return error.MissingRequiredOption;
    return config;
}

pub fn run(
    io: std.Io,
    allocator: std.mem.Allocator,
    environment: *const std.process.Environ.Map,
    config: Config,
) !void {
    if (!std.mem.eql(u8, config.host, default_host)) return error.InvalidHost;
    const profile = try provider.Profile.load(io, allocator, environment, config.profile_path);
    defer profile.deinit();
    const body = std.Io.Dir.cwd().readFileAlloc(
        io,
        config.body_file_path,
        allocator,
        .limited(provider.max_body_bytes + 1),
    ) catch return error.BodyReadFailed;
    defer allocator.free(body);
    if (body.len == 0 or body.len > provider.max_body_bytes) return error.InvalidBody;

    const timestamp = config.timestamp orelse std.Io.Clock.real.now(io).toSeconds();
    if (timestamp <= 0) return error.InvalidTimestamp;
    var timestamp_storage: [20]u8 = undefined;
    const timestamp_text = std.fmt.bufPrint(&timestamp_storage, "{d}", .{timestamp}) catch unreachable;
    const signature = try profile.sign(timestamp_text, body);

    var request_storage: [max_request_bytes]u8 = undefined;
    const request = try frameRequest(
        &request_storage,
        profile.path(),
        config.host,
        config.port,
        timestamp_text,
        &signature,
        body,
    );
    const address = std.Io.net.IpAddress.parseIp4(default_host, config.port) catch unreachable;
    const status = try exchangeWithin(io, address, request, exchange_deadline);
    var status_storage: [32]u8 = undefined;
    const output = std.fmt.bufPrint(&status_storage, "{{\"status\":{d}}}\n", .{status}) catch unreachable;
    try writeStdout(io, output);
}

pub fn frameRequest(
    storage: []u8,
    path: []const u8,
    host: []const u8,
    port: u16,
    timestamp: []const u8,
    signature: []const u8,
    body: []const u8,
) error{ InvalidRequest, NoSpaceLeft }![]const u8 {
    if (path.len == 0 or path.len > provider.max_path_bytes or path[0] != '/' or
        !std.mem.eql(u8, host, default_host) or port == 0 or body.len == 0 or
        body.len > provider.max_body_bytes or signature.len != 64 or
        parseCanonicalPositive(i64, timestamp) catch null == null)
    {
        return error.InvalidRequest;
    }
    for (signature) |byte| {
        if (!std.ascii.isDigit(byte) and (byte < 'a' or byte > 'f')) return error.InvalidRequest;
    }
    var writer = std.Io.Writer.fixed(storage);
    writer.print(
        "POST {s} HTTP/1.1\r\n" ++
            "Host: {s}:{d}\r\n" ++
            "Content-Type: application/json\r\n" ++
            "Content-Length: {d}\r\n" ++
            "x-exchange-provider-timestamp: {s}\r\n" ++
            "x-exchange-provider-signature: {s}\r\n" ++
            "Connection: close\r\n\r\n{s}",
        .{ path, host, port, body.len, timestamp, signature, body },
    ) catch return error.NoSpaceLeft;
    return writer.buffered();
}

pub fn parseResponse(bytes: []const u8) ResponseError!u16 {
    if (bytes.len == 0) return error.EmptyResponse;
    const parsed = (try parseResponseHead(bytes)) orelse return error.MalformedResponse;
    const body_length = bytes.len - parsed.body_offset;
    if (parsed.content_length != body_length) return error.UnexpectedResponseBodyLength;
    if (parsed.status != 200 and parsed.status != 202) return error.RequestRejected;
    return parsed.status;
}

fn parseResponseHead(bytes: []const u8) ResponseError!?ResponseHead {
    if (bytes.len > max_response_bytes) return error.ResponseTooLarge;
    const delimiter = std.mem.find(u8, bytes, "\r\n\r\n") orelse {
        if (bytes.len > max_response_head_bytes + 3) return error.ResponseTooLarge;
        return null;
    };
    if (delimiter > max_response_head_bytes) return error.ResponseTooLarge;
    const head = bytes[0..delimiter];
    var lines = std.mem.splitSequence(u8, head, "\r\n");
    const status_line = lines.next() orelse return error.MalformedResponse;
    var status_parts = std.mem.splitScalar(u8, status_line, ' ');
    if (!std.mem.eql(u8, status_parts.next() orelse return error.MalformedResponse, "HTTP/1.1")) {
        return error.MalformedResponse;
    }
    const status_text = status_parts.next() orelse return error.MalformedResponse;
    if (status_text.len != 3) return error.MalformedResponse;
    const status = std.fmt.parseInt(u16, status_text, 10) catch return error.MalformedResponse;
    const reason = status_parts.rest();
    if (reason.len == 0) return error.MalformedResponse;
    for (reason) |byte| if (byte < 0x20 or byte == 0x7f) return error.MalformedResponse;

    var content_length: ?usize = null;
    while (lines.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse return error.MalformedResponse;
        const name = line[0..colon];
        const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
        if (name.len == 0) return error.MalformedResponse;
        for (name) |byte| {
            if (!std.ascii.isAlphanumeric(byte) and byte != '-') return error.MalformedResponse;
        }
        for (value) |byte| if (byte < 0x20 or byte == 0x7f) return error.MalformedResponse;
        if (std.ascii.eqlIgnoreCase(name, "content-length")) {
            const parsed = parseCanonicalNonnegative(usize, value) catch return error.ConflictingContentLength;
            if (content_length != null) {
                if (content_length.? != parsed) return error.ConflictingContentLength;
                return error.DuplicateContentLength;
            }
            content_length = parsed;
        } else if (std.ascii.eqlIgnoreCase(name, "transfer-encoding")) {
            return error.MalformedResponse;
        }
    }
    const expected = content_length orelse return error.MissingContentLength;
    if (expected > max_response_body_bytes) return error.ResponseBodyTooLarge;
    return .{
        .status = status,
        .body_offset = delimiter + 4,
        .content_length = expected,
    };
}

const AttemptOutcome = union(enum) { status: u16, failed: anyerror };
const DeadlineOutcome = enum { expired, canceled };
const RaceResult = union(enum) { attempt: AttemptOutcome, deadline: DeadlineOutcome };
const Race = std.Io.Select(RaceResult);

fn exchangeWithin(
    io: std.Io,
    address: std.Io.net.IpAddress,
    request: []const u8,
    duration: std.Io.Duration,
) !u16 {
    const deadline = (std.Io.Timeout{ .duration = .{ .raw = duration, .clock = .boot } }).toDeadline(io);
    var response_storage: [max_response_bytes + 1]u8 = undefined;
    var result_storage: [2]RaceResult = undefined;
    var race = Race.init(io, &result_storage);
    try race.concurrent(.attempt, performExchange, .{ io, address, request, &response_storage });
    defer race.cancelDiscard();
    try race.concurrent(.deadline, waitForDeadline, .{ deadline, io });
    return switch (try race.await()) {
        .attempt => |outcome| switch (outcome) {
            .status => |status| status,
            .failed => |err| return err,
        },
        .deadline => |outcome| switch (outcome) {
            .expired => error.ExchangeDeadlineExceeded,
            .canceled => error.Canceled,
        },
    };
}

fn performExchange(
    io: std.Io,
    address: std.Io.net.IpAddress,
    request: []const u8,
    response_storage: []u8,
) AttemptOutcome {
    const status = exchange(io, address, request, response_storage) catch |err| return .{ .failed = err };
    return .{ .status = status };
}

fn exchange(io: std.Io, address: std.Io.net.IpAddress, request: []const u8, response_storage: []u8) !u16 {
    const stream = try address.connect(io, .{ .mode = .stream, .protocol = .tcp });
    defer stream.close(io);
    var write_buffer: [4096]u8 = undefined;
    var writer = stream.writer(io, &write_buffer);
    try writer.interface.writeAll(request);
    try writer.interface.flush();
    var read_buffer: [4096]u8 = undefined;
    var reader = stream.reader(io, &read_buffer);
    var length: usize = 0;
    while (length < response_storage.len) {
        var slices: [1][]u8 = .{response_storage[length..]};
        const read = reader.interface.readVec(&slices) catch |err| switch (err) {
            error.EndOfStream => break,
            error.ReadFailed => return reader.err.?,
        };
        length += read;
        if (try parseResponseHead(response_storage[0..length])) |head| {
            const response_length = head.body_offset + head.content_length;
            if (length >= response_length) return parseResponse(response_storage[0..length]);
        }
        if (read == 0) break;
    }
    if (length == response_storage.len) return error.ResponseTooLarge;
    return parseResponse(response_storage[0..length]);
}

fn waitForDeadline(deadline: std.Io.Timeout, io: std.Io) DeadlineOutcome {
    deadline.sleep(io) catch return .canceled;
    return .expired;
}

fn collectArguments(
    process_args: std.process.Args,
    allocator: std.mem.Allocator,
    storage: *[max_arguments][]const u8,
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

fn parseCanonicalPositive(comptime T: type, text: []const u8) error{InvalidInteger}!T {
    const result = try parseCanonicalNonnegative(T, text);
    if (result == 0) return error.InvalidInteger;
    return result;
}

fn parseCanonicalNonnegative(comptime T: type, text: []const u8) error{InvalidInteger}!T {
    if (text.len == 0 or (text.len > 1 and text[0] == '0')) return error.InvalidInteger;
    for (text) |byte| if (!std.ascii.isDigit(byte)) return error.InvalidInteger;
    return std.fmt.parseInt(T, text, 10) catch error.InvalidInteger;
}

fn writeStdout(io: std.Io, bytes: []const u8) !void {
    var buffer: [256]u8 = undefined;
    var writer = std.Io.File.stdout().writer(io, &buffer);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
}

fn writeStderr(io: std.Io, bytes: []const u8) !void {
    var buffer: [256]u8 = undefined;
    var writer = std.Io.File.stderr().writer(io, &buffer);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
}

fn serveTestResponse(
    io: std.Io,
    listener: *std.Io.net.Server,
    status: u16,
    close_gate: ?*std.Io.Event,
) !void {
    const stream = try listener.accept(io);
    defer stream.close(io);
    var read_buffer: [2048]u8 = undefined;
    var write_buffer: [1024]u8 = undefined;
    var reader = stream.reader(io, &read_buffer);
    var writer = stream.writer(io, &write_buffer);
    var server = std.http.Server.init(&reader.interface, &writer.interface);
    var request = try server.receiveHead();
    try std.testing.expectEqual(std.http.Method.POST, request.head.method);
    const body_length: usize = @intCast(request.head.content_length orelse return error.MissingBody);
    var body: [provider.max_body_bytes]u8 = undefined;
    var transfer: [1024]u8 = undefined;
    const body_reader = request.readerExpectNone(&transfer);
    try body_reader.readSliceAll(body[0..body_length]);
    try std.testing.expectEqualStrings("{}", body[0..body_length]);
    const phrase = if (status == 200) "OK" else "Accepted";
    try writer.interface.print(
        "HTTP/1.1 {d} {s}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        .{ status, phrase },
    );
    try writer.interface.flush();
    if (close_gate) |gate| try gate.wait(io);
}

fn runLoopbackStatus(status: u16) !void {
    const io = std.testing.io;
    const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var listener = try address.listen(io, .{});
    defer listener.deinit(io);
    var server_task = try io.concurrent(serveTestResponse, .{ io, &listener, status, null });
    defer server_task.cancel(io) catch {};
    var request_storage: [1024]u8 = undefined;
    const request = try frameRequest(
        &request_storage,
        "/v1/clearing/events",
        default_host,
        listener.socket.address.getPort(),
        "1700000000",
        "01" ** 32,
        "{}",
    );
    try std.testing.expectEqual(
        status,
        try exchangeWithin(io, listener.socket.address, request, .fromSeconds(1)),
    );
    try server_task.await(io);
}

fn runLoopbackPersistentConnection() !void {
    const io = std.testing.io;
    const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var listener = try address.listen(io, .{});
    defer listener.deinit(io);
    var close_gate: std.Io.Event = .unset;
    var server_task = try io.concurrent(
        serveTestResponse,
        .{ io, &listener, @as(u16, 202), &close_gate },
    );
    defer server_task.cancel(io) catch {};
    var request_storage: [1024]u8 = undefined;
    const request = try frameRequest(
        &request_storage,
        "/v1/clearing/events",
        default_host,
        listener.socket.address.getPort(),
        "1700000000",
        "01" ** 32,
        "{}",
    );
    const response_status = try exchangeWithin(
        io,
        listener.socket.address,
        request,
        .fromSeconds(1),
    );
    try std.testing.expectEqual(@as(u16, 202), response_status);
    close_gate.set(io);
    try server_task.await(io);
}

fn serveStalledConnection(
    io: std.Io,
    listener: *std.Io.net.Server,
    release: *std.Io.Event,
) !void {
    const stream = try listener.accept(io);
    defer stream.close(io);
    try release.wait(io);
}

fn runLoopbackDeadline() !void {
    const io = std.testing.io;
    const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var listener = try address.listen(io, .{});
    defer listener.deinit(io);
    var release: std.Io.Event = .unset;
    var server_task = try io.concurrent(serveStalledConnection, .{ io, &listener, &release });
    defer server_task.cancel(io) catch {};
    var request_storage: [1024]u8 = undefined;
    const request = try frameRequest(
        &request_storage,
        "/v1/clearing/events",
        default_host,
        listener.socket.address.getPort(),
        "1700000000",
        "01" ** 32,
        "{}",
    );
    try std.testing.expectError(
        error.ExchangeDeadlineExceeded,
        exchangeWithin(io, listener.socket.address, request, .fromMilliseconds(50)),
    );
    release.set(io);
    try server_task.await(io);
}

test "CLI accepts only bounded explicit loopback configuration" {
    const config = try parseCli(&.{
        "--profile",   "provider.json", "--body-file", "event.json",
        "--host",      default_host,    "--port",      "7381",
        "--timestamp", "1700000000",
    });
    try std.testing.expectEqualStrings("provider.json", config.profile_path);
    try std.testing.expectEqualStrings("event.json", config.body_file_path);
    try std.testing.expectEqual(@as(u16, 7381), config.port);
    try std.testing.expectEqual(@as(?i64, 1_700_000_000), config.timestamp);
    try std.testing.expectError(error.InvalidHost, parseCli(&.{ "--profile", "p", "--body-file", "b", "--host", "localhost" }));
    try std.testing.expectError(error.InvalidPort, parseCli(&.{ "--profile", "p", "--body-file", "b", "--port", "07380" }));
    try std.testing.expectError(error.InvalidTimestamp, parseCli(&.{ "--profile", "p", "--body-file", "b", "--timestamp", "0" }));
    try std.testing.expectError(error.DuplicateOption, parseCli(&.{ "--profile", "p", "--profile", "q", "--body-file", "b" }));
    try std.testing.expectError(error.MissingRequiredOption, parseCli(&.{ "--profile", "p" }));
}

test "request framing is exact and body preserving" {
    var storage: [2048]u8 = undefined;
    const request = try frameRequest(
        &storage,
        "/v1/clearing/events",
        default_host,
        7380,
        "1700000000",
        "ab" ** 32,
        "{\"version\":1}",
    );
    try std.testing.expectEqualStrings(
        "POST /v1/clearing/events HTTP/1.1\r\n" ++
            "Host: 127.0.0.1:7380\r\n" ++
            "Content-Type: application/json\r\n" ++
            "Content-Length: 13\r\n" ++
            "x-exchange-provider-timestamp: 1700000000\r\n" ++
            "x-exchange-provider-signature: " ++ ("ab" ** 32) ++ "\r\n" ++
            "Connection: close\r\n\r\n{\"version\":1}",
        request,
    );
}

test "response parser maps only strict 200 and 202 responses" {
    try std.testing.expectEqual(@as(u16, 200), try parseResponse("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"));
    try std.testing.expectEqual(@as(u16, 202), try parseResponse("HTTP/1.1 202 Accepted\r\ncontent-length: 0\r\n\r\n"));
    try std.testing.expectError(error.RequestRejected, parseResponse("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n"));
    try std.testing.expectError(error.DuplicateContentLength, parseResponse("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n"));
    try std.testing.expectError(error.ConflictingContentLength, parseResponse("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Length: 1\r\n\r\n"));
    try std.testing.expectError(error.UnexpectedResponseBodyLength, parseResponse("HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n{}"));
    try std.testing.expectError(error.MalformedResponse, parseResponse("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 0\r\n\r\n"));
}

test "loopback exchange accepts 202 and 200" {
    try runLoopbackStatus(202);
    try runLoopbackStatus(200);
}

test "loopback exchange completes at content length without peer EOF" {
    try runLoopbackPersistentConnection();
}

test "loopback exchange enforces one absolute deadline" {
    try runLoopbackDeadline();
}
