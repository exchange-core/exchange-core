const builtin = @import("builtin");
const std = @import("std");

/// Returns the process I/O implementation used by persistence operations.
/// Tests must use the runner-provided implementation; library consumers that
/// do not own a process `Init` use Zig's synchronous threaded implementation.
pub fn current() std.Io {
    return if (builtin.is_test)
        std.testing.io
    else
        std.Io.Threaded.global_single_threaded.io();
}

pub const ReadToEndAllocError = std.mem.Allocator.Error ||
    std.Io.File.Reader.Error ||
    error{StreamTooLong};

pub fn readToEndAlloc(
    file: std.Io.File,
    allocator: std.mem.Allocator,
    limit: usize,
) ReadToEndAllocError![]u8 {
    var file_reader = file.reader(current(), &.{});
    return file_reader.interface.allocRemaining(allocator, .limited(limit)) catch |err| switch (err) {
        error.ReadFailed => return file_reader.err.?,
        else => |e| return e,
    };
}

pub fn readAll(file: std.Io.File, buffer: []u8) std.Io.File.Reader.Error!usize {
    var file_reader = file.reader(current(), &.{});
    return file_reader.interface.readSliceShort(buffer) catch return file_reader.err.?;
}

pub const AppendAllError = std.Io.File.StatError ||
    std.Io.File.Writer.SeekError ||
    std.Io.Writer.Error;

pub fn appendAll(file: std.Io.File, bytes: []const u8) AppendAllError!void {
    const end = (try file.stat(current())).size;
    var buffer: [4096]u8 = undefined;
    var file_writer = file.writer(current(), &buffer);
    try file_writer.seekTo(end);
    try file_writer.interface.writeAll(bytes);
    try file_writer.interface.flush();
}

/// Process wall-clock reads for call sites that do not yet own a
/// `runtime.WallClock`. Prefer the injectable `WallClock` on
/// `RuntimeConfig` / `ExchangeCore` for command timestamps and any state that
/// participates in hashes or journals so tests can freeze time.
pub fn realTimestampNs() u64 {
    const nanoseconds = std.Io.Clock.real.now(current()).nanoseconds;
    if (nanoseconds <= 0) return 0;
    return std.math.cast(u64, nanoseconds) orelse std.math.maxInt(u64);
}

pub fn realTimestampMs() u64 {
    return realTimestampNs() / std.time.ns_per_ms;
}
