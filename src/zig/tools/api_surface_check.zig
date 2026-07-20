const std = @import("std");

const SurfaceCheckError = error{
    MissingFunctionName,
    ApiSurfaceMismatch,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const core_source = try std.fs.cwd().readFileAlloc(
        allocator,
        "src/zig/core/exchange_core.zig",
        std.math.maxInt(usize),
    );
    defer allocator.free(core_source);

    const api_source = try std.fs.cwd().readFileAlloc(
        allocator,
        "src/zig/core/exchange_api.zig",
        std.math.maxInt(usize),
    );
    defer allocator.free(api_source);

    var core_names: std.StringHashMapUnmanaged(void) = .empty;
    defer core_names.deinit(allocator);

    var api_names: std.StringHashMapUnmanaged(void) = .empty;
    defer api_names.deinit(allocator);

    try collectPublicFunctionNames(allocator, core_source, &core_names);
    try collectPublicFunctionNames(allocator, api_source, &api_names);

    var missing: std.ArrayListUnmanaged([]const u8) = .empty;
    defer missing.deinit(allocator);

    var it = core_names.keyIterator();
    while (it.next()) |name_ptr| {
        if (!api_names.contains(name_ptr.*)) {
            try missing.append(allocator, name_ptr.*);
        }
    }

    if (missing.items.len == 0) {
        return;
    }

    std.sort.heap([]const u8, missing.items, {}, lessThanName);
    const stderr = std.fs.File.stderr();
    var err_buffer: [2048]u8 = undefined;
    var err_writer = stderr.writer(&err_buffer);
    const err = &err_writer.interface;
    try err.print(
        "ExchangeApi is missing {d} public ExchangeCore methods:\n",
        .{missing.items.len},
    );
    for (missing.items) |name| {
        try err.print("  - {s}\n", .{name});
    }
    try err.flush();
    return SurfaceCheckError.ApiSurfaceMismatch;
}

fn collectPublicFunctionNames(
    allocator: std.mem.Allocator,
    source: []const u8,
    out: *std.StringHashMapUnmanaged(void),
) (std.mem.Allocator.Error || SurfaceCheckError)!void {
    var lines = std.mem.splitScalar(u8, source, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trimLeft(u8, line, " \t");
        if (!std.mem.startsWith(u8, trimmed, "pub fn ")) continue;
        const tail = trimmed["pub fn ".len..];
        const end = std.mem.indexOfAny(u8, tail, " (\t") orelse return SurfaceCheckError.MissingFunctionName;
        if (end == 0) return SurfaceCheckError.MissingFunctionName;
        try out.put(allocator, tail[0..end], {});
    }
}

fn lessThanName(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs) == .lt;
}
