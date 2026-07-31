const std = @import("std");
const protocol = @import("exchange_protocol");

pub const default_host = "127.0.0.1";
pub const default_port: u16 = 7379;
pub const max_response_line_bytes: usize = 64 * 1024;

const max_cli_arguments = protocol.max_tokens + 4;

const CollectArgumentsError = error{
    ArgumentIterationFailed,
    TooManyArguments,
};

pub const ClientConfig = struct {
    address: std.Io.net.IpAddress,
    request: protocol.Request,
};

pub const CliError = protocol.ParseError || error{
    MissingCommand,
    MissingOptionValue,
    UnknownOption,
    DuplicateOption,
    InvalidHost,
    InvalidPort,
    TooManyArguments,
};

const RunError = std.Io.net.IpAddress.ConnectError ||
    std.Io.net.Stream.Reader.Error ||
    std.Io.net.Stream.Writer.Error ||
    std.Io.Writer.Error ||
    error{
        EmptyResponse,
        ResponseTooLong,
    };

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

    run(init.io, config) catch |err| {
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
        } else {
            return error.UnknownOption;
        }
        index += 1;
    }

    if (index == args.len) return error.MissingCommand;
    const address = std.Io.net.IpAddress.parseIp4(host, port) catch return error.InvalidHost;
    return .{
        .address = address,
        .request = try protocol.parseArgs(args[index..]),
    };
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

fn run(io: std.Io, config: ClientConfig) RunError!void {
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

fn writeCliError(io: std.Io, err: CliError) std.Io.Writer.Error!void {
    var buffer: [4096]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writer(io, &buffer);
    try stderr_writer.interface.print(
        "error: invalid command line ({s})\n\nUsage: exchange-core-client [--host IPv4] [--port PORT] COMMAND...\n{s}",
        .{ @errorName(err), protocol.usage },
    );
    try stderr_writer.interface.flush();
}

fn writeHelp(io: std.Io) std.Io.Writer.Error!void {
    var buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &buffer);
    try writeUsage(&stdout_writer.interface);
    try stdout_writer.interface.flush();
}

pub fn writeUsage(writer: *std.Io.Writer) std.Io.Writer.Error!void {
    try writer.print(
        "Usage: exchange-core-client [--host IPv4] [--port PORT] COMMAND...\n{s}",
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
    const config = try parseCli(&.{ "add-user", "7" });
    try std.testing.expectEqualDeep(
        std.Io.net.IpAddress{ .ip4 = std.Io.net.Ip4Address.loopback(default_port) },
        config.address,
    );
    try std.testing.expectEqualDeep(
        protocol.Request{ .add_user = .{ .uid = 7 } },
        config.request,
    );
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
        "Usage: exchange-core-client [--host IPv4] [--port PORT] COMMAND...\n",
    ));
    try std.testing.expect(std.mem.find(u8, rendered, "  shutdown\n") != null);
}
