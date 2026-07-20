const std = @import("std");

const JavaSourcePath = "src/main/java/exchange/core2/core/ExchangeApi.java";
const ZigSourcePath = "src/zig/core/exchange_api.zig";
const JsonOutputPath = "src/zig/EXCHANGE_API_SURFACE_DIFF.json";
const MarkdownOutputPath = "src/zig/EXCHANGE_API_SURFACE_DIFF.md";

const SurfaceDiffError = error{
    MissingMethodName,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const java_source = try std.fs.cwd().readFileAlloc(
        allocator,
        JavaSourcePath,
        std.math.maxInt(usize),
    );
    defer allocator.free(java_source);

    const zig_source = try std.fs.cwd().readFileAlloc(
        allocator,
        ZigSourcePath,
        std.math.maxInt(usize),
    );
    defer allocator.free(zig_source);

    var java_methods: std.StringHashMapUnmanaged(void) = .empty;
    defer java_methods.deinit(allocator);

    var zig_methods: std.StringHashMapUnmanaged(void) = .empty;
    defer zig_methods.deinit(allocator);

    try collectJavaPublicMethodNames(allocator, java_source, &java_methods);
    try collectZigPublicMethodNames(allocator, zig_source, &zig_methods);

    var java_only: std.ArrayListUnmanaged([]const u8) = .empty;
    defer java_only.deinit(allocator);
    var zig_only: std.ArrayListUnmanaged([]const u8) = .empty;
    defer zig_only.deinit(allocator);
    var shared: std.ArrayListUnmanaged([]const u8) = .empty;
    defer shared.deinit(allocator);

    var java_it = java_methods.keyIterator();
    while (java_it.next()) |name_ptr| {
        if (zig_methods.contains(name_ptr.*)) {
            try shared.append(allocator, name_ptr.*);
        } else {
            try java_only.append(allocator, name_ptr.*);
        }
    }

    var zig_it = zig_methods.keyIterator();
    while (zig_it.next()) |name_ptr| {
        if (!java_methods.contains(name_ptr.*)) {
            try zig_only.append(allocator, name_ptr.*);
        }
    }

    std.sort.heap([]const u8, java_only.items, {}, lessThanName);
    std.sort.heap([]const u8, zig_only.items, {}, lessThanName);
    std.sort.heap([]const u8, shared.items, {}, lessThanName);

    try writeJsonReport(
        java_methods.count(),
        zig_methods.count(),
        shared.items,
        java_only.items,
        zig_only.items,
    );
    try writeMarkdownReport(
        java_methods.count(),
        zig_methods.count(),
        shared.items,
        java_only.items,
        zig_only.items,
    );
}

fn collectJavaPublicMethodNames(
    allocator: std.mem.Allocator,
    source: []const u8,
    out: *std.StringHashMapUnmanaged(void),
) (std.mem.Allocator.Error || SurfaceDiffError)!void {
    var lines = std.mem.splitScalar(u8, source, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trimLeft(u8, line, " \t");
        if (!std.mem.startsWith(u8, trimmed, "public ")) continue;
        if (std.mem.startsWith(u8, trimmed, "public final class ")) continue;
        if (std.mem.indexOfScalar(u8, trimmed, '(') == null) continue;

        const method_name = extractMethodNameFromSignature(trimmed) orelse
            return SurfaceDiffError.MissingMethodName;
        if (std.mem.eql(u8, method_name, "ExchangeApi")) continue;
        try out.put(allocator, method_name, {});
    }
}

fn collectZigPublicMethodNames(
    allocator: std.mem.Allocator,
    source: []const u8,
    out: *std.StringHashMapUnmanaged(void),
) (std.mem.Allocator.Error || SurfaceDiffError)!void {
    var lines = std.mem.splitScalar(u8, source, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trimLeft(u8, line, " \t");
        if (!std.mem.startsWith(u8, trimmed, "pub fn ")) continue;

        const tail = trimmed["pub fn ".len..];
        const open_paren = std.mem.indexOfScalar(u8, tail, '(') orelse
            return SurfaceDiffError.MissingMethodName;
        const signature = tail[0..open_paren];
        const method_name = std.mem.trimRight(u8, signature, " \t");
        if (method_name.len == 0) return SurfaceDiffError.MissingMethodName;
        try out.put(allocator, method_name, {});
    }
}

fn extractMethodNameFromSignature(signature_line: []const u8) ?[]const u8 {
    const open_paren = std.mem.indexOfScalar(u8, signature_line, '(') orelse return null;
    var end = open_paren;
    while (end > 0 and std.ascii.isWhitespace(signature_line[end - 1])) : (end -= 1) {}
    if (end == 0) return null;

    var start = end;
    while (start > 0 and isIdentifierChar(signature_line[start - 1])) : (start -= 1) {}
    if (start == end) return null;

    return signature_line[start..end];
}

fn isIdentifierChar(ch: u8) bool {
    return std.ascii.isAlphabetic(ch) or
        std.ascii.isDigit(ch) or
        ch == '_';
}

fn writeJsonReport(
    java_count: usize,
    zig_count: usize,
    shared: []const []const u8,
    java_only: []const []const u8,
    zig_only: []const []const u8,
) !void {
    const file = try std.fs.cwd().createFile(JsonOutputPath, .{ .truncate = true });
    defer file.close();

    var buffer: [8192]u8 = undefined;
    var file_writer = file.writer(&buffer);
    const writer = &file_writer.interface;

    try writer.writeAll("{\n");
    try writer.print("  \"java_source\": \"{s}\",\n", .{JavaSourcePath});
    try writer.print("  \"zig_source\": \"{s}\",\n", .{ZigSourcePath});
    try writer.print("  \"java_method_count\": {d},\n", .{java_count});
    try writer.print("  \"zig_method_count\": {d},\n", .{zig_count});
    try writer.print("  \"shared_method_count\": {d},\n", .{shared.len});
    try writer.print("  \"java_only_method_count\": {d},\n", .{java_only.len});
    try writer.print("  \"zig_only_method_count\": {d},\n", .{zig_only.len});
    try writeJsonStringArray(writer, "shared_methods", shared, true);
    try writeJsonStringArray(writer, "java_only_methods", java_only, true);
    try writeJsonStringArray(writer, "zig_only_methods", zig_only, false);
    try writer.writeAll("}\n");
    try writer.flush();
}

fn writeJsonStringArray(
    writer: *std.Io.Writer,
    key: []const u8,
    values: []const []const u8,
    trailing_comma: bool,
) !void {
    try writer.print("  \"{s}\": [", .{key});
    for (values, 0..) |value, idx| {
        if (idx == 0) {
            try writer.writeByte('\n');
        }
        try writer.print("    \"{s}\"", .{value});
        if (idx + 1 != values.len) {
            try writer.writeAll(",\n");
        } else {
            try writer.writeByte('\n');
        }
    }
    if (values.len == 0) {
        try writer.writeByte(']');
    } else {
        try writer.writeAll("  ]");
    }
    if (trailing_comma) {
        try writer.writeAll(",\n");
    } else {
        try writer.writeByte('\n');
    }
}

fn writeMarkdownReport(
    java_count: usize,
    zig_count: usize,
    shared: []const []const u8,
    java_only: []const []const u8,
    zig_only: []const []const u8,
) !void {
    const file = try std.fs.cwd().createFile(MarkdownOutputPath, .{ .truncate = true });
    defer file.close();

    var buffer: [8192]u8 = undefined;
    var file_writer = file.writer(&buffer);
    const writer = &file_writer.interface;

    try writer.writeAll("# ExchangeApi Surface Diff (Java vs Zig)\n\n");
    try writer.print("- Java public methods (unique): **{d}**\n", .{java_count});
    try writer.print("- Zig public methods (unique): **{d}**\n", .{zig_count});
    try writer.print("- Shared method names: **{d}**\n", .{shared.len});
    try writer.print("- Java-only method names: **{d}**\n", .{java_only.len});
    try writer.print("- Zig-only method names: **{d}**\n\n", .{zig_only.len});
    try writer.writeAll("Generated by `src/zig/tools/java_exchange_api_surface_diff.zig`.\n\n");

    try writeMarkdownSection(writer, "Java-only method names", java_only);
    try writeMarkdownSection(writer, "Shared method names", shared);
    try writeMarkdownSection(writer, "Zig-only method names", zig_only);
    try writer.flush();
}

fn writeMarkdownSection(
    writer: *std.Io.Writer,
    title: []const u8,
    values: []const []const u8,
) !void {
    try writer.print("## {s}\n\n", .{title});
    if (values.len == 0) {
        try writer.writeAll("_None._\n\n");
        return;
    }

    for (values) |value| {
        try writer.print("- `{s}`\n", .{value});
    }
    try writer.writeByte('\n');
}

fn lessThanName(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs) == .lt;
}

test "extract method name from java signature line" {
    try std.testing.expectEqualStrings(
        "submitCommandAsync",
        extractMethodNameFromSignature(
            "public CompletableFuture<CommandResultCode> submitCommandAsync(ApiCommand cmd) {",
        ).?,
    );
    try std.testing.expectEqualStrings(
        "processReport",
        extractMethodNameFromSignature(
            "public <Q extends ReportQuery<R>, R extends ReportResult> CompletableFuture<R> processReport(final Q query, final int transferId) {",
        ).?,
    );
    try std.testing.expectEqual(@as(?[]const u8, null), extractMethodNameFromSignature("public static final int LONGS_PER_MESSAGE = 5;"));
}
