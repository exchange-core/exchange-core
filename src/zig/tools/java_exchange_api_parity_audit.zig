const std = @import("std");

const JavaSourcePath = "src/main/java/exchange/core2/core/ExchangeApi.java";
const ZigSourcePath = "src/zig/core/exchange_api.zig";
const JsonOutputPath = "src/zig/EXCHANGE_API_PARITY_AUDIT.json";
const MarkdownOutputPath = "src/zig/EXCHANGE_API_PARITY_AUDIT.md";

const AuditError = error{
    MissingMethodName,
    MissingSameNameCoverage,
};

const MethodDecl = struct {
    name: []const u8,
    signature: []const u8,
    line: usize,
};

const CoverageRow = struct {
    name: []const u8,
    java_overloads: usize,
    zig_same_name_count: usize,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var arena_state = std.heap.ArenaAllocator.init(gpa.allocator());
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const java_source = try std.fs.cwd().readFileAlloc(
        arena,
        JavaSourcePath,
        std.math.maxInt(usize),
    );
    const zig_source = try std.fs.cwd().readFileAlloc(
        arena,
        ZigSourcePath,
        std.math.maxInt(usize),
    );

    var java_decls: std.ArrayListUnmanaged(MethodDecl) = .empty;
    var zig_decls: std.ArrayListUnmanaged(MethodDecl) = .empty;
    try collectJavaPublicMethodDecls(arena, java_source, &java_decls);
    try collectZigPublicMethodDecls(arena, zig_source, &zig_decls);

    var java_counts: std.StringHashMapUnmanaged(usize) = .empty;
    var zig_counts: std.StringHashMapUnmanaged(usize) = .empty;

    for (java_decls.items) |decl| {
        try incrementCount(arena, &java_counts, decl.name);
    }
    for (zig_decls.items) |decl| {
        try incrementCount(arena, &zig_counts, decl.name);
    }

    const java_names = try collectSortedKeys(arena, &java_counts);
    const zig_names = try collectSortedKeys(arena, &zig_counts);
    const coverage_rows = try buildCoverageRows(arena, java_names, &java_counts, &zig_counts);
    const java_without_same_name = try buildJavaWithoutSameName(
        arena,
        coverage_rows,
    );

    try writeJsonReport(
        java_decls.items,
        zig_decls.items,
        java_names,
        zig_names,
        coverage_rows,
        java_without_same_name,
    );
    try writeMarkdownReport(
        java_decls.items,
        zig_decls.items,
        java_names,
        zig_names,
        coverage_rows,
        java_without_same_name,
    );

    if (java_without_same_name.len != 0) {
        const stderr = std.fs.File.stderr();
        var err_buffer: [2048]u8 = undefined;
        var err_writer = stderr.writer(&err_buffer);
        const err = &err_writer.interface;
        try err.print(
            "ExchangeApi parity audit failed: {d} Java method family(ies) lack same-name Zig coverage.\n",
            .{java_without_same_name.len},
        );
        for (java_without_same_name) |name| {
            try err.print("- {s}\n", .{name});
        }
        try err.flush();
        return error.MissingSameNameCoverage;
    }
}

fn collectJavaPublicMethodDecls(
    allocator: std.mem.Allocator,
    source: []const u8,
    out: *std.ArrayListUnmanaged(MethodDecl),
) (std.mem.Allocator.Error || AuditError)!void {
    var lines = std.mem.splitScalar(u8, source, '\n');
    var line_no: usize = 1;
    while (lines.next()) |line| : (line_no += 1) {
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (!std.mem.startsWith(u8, trimmed, "public ")) continue;
        if (std.mem.startsWith(u8, trimmed, "public final class ")) continue;
        if (std.mem.indexOfScalar(u8, trimmed, '(') == null) continue;

        const method_name = extractMethodNameFromSignature(trimmed) orelse
            return AuditError.MissingMethodName;
        if (std.mem.eql(u8, method_name, "ExchangeApi")) continue;

        try out.append(allocator, .{
            .name = try allocator.dupe(u8, method_name),
            .signature = try allocator.dupe(u8, trimmed),
            .line = line_no,
        });
    }
}

fn collectZigPublicMethodDecls(
    allocator: std.mem.Allocator,
    source: []const u8,
    out: *std.ArrayListUnmanaged(MethodDecl),
) (std.mem.Allocator.Error || AuditError)!void {
    var lines = std.mem.splitScalar(u8, source, '\n');
    var line_no: usize = 1;
    while (lines.next()) |line| : (line_no += 1) {
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (!std.mem.startsWith(u8, trimmed, "pub fn ")) continue;

        const tail = trimmed["pub fn ".len..];
        const open_paren = std.mem.indexOfScalar(u8, tail, '(') orelse
            return AuditError.MissingMethodName;
        const method_name = std.mem.trimRight(u8, tail[0..open_paren], " \t");
        if (method_name.len == 0) return AuditError.MissingMethodName;

        try out.append(allocator, .{
            .name = try allocator.dupe(u8, method_name),
            .signature = try allocator.dupe(u8, trimmed),
            .line = line_no,
        });
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
    return std.ascii.isAlphabetic(ch) or std.ascii.isDigit(ch) or ch == '_';
}

fn incrementCount(
    allocator: std.mem.Allocator,
    counts: *std.StringHashMapUnmanaged(usize),
    key: []const u8,
) !void {
    const entry = try counts.getOrPut(allocator, key);
    if (!entry.found_existing) {
        entry.value_ptr.* = 1;
    } else {
        entry.value_ptr.* += 1;
    }
}

fn collectSortedKeys(
    allocator: std.mem.Allocator,
    counts: *const std.StringHashMapUnmanaged(usize),
) ![]const []const u8 {
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    var it = counts.keyIterator();
    while (it.next()) |name_ptr| {
        try names.append(allocator, name_ptr.*);
    }
    std.sort.heap([]const u8, names.items, {}, lessThanName);
    return names.toOwnedSlice(allocator);
}

fn buildCoverageRows(
    allocator: std.mem.Allocator,
    java_names: []const []const u8,
    java_counts: *const std.StringHashMapUnmanaged(usize),
    zig_counts: *const std.StringHashMapUnmanaged(usize),
) ![]const CoverageRow {
    var rows: std.ArrayListUnmanaged(CoverageRow) = .empty;
    for (java_names) |name| {
        const java_overloads = java_counts.get(name) orelse 0;
        const zig_same_name_count = zig_counts.get(name) orelse 0;
        try rows.append(allocator, .{
            .name = name,
            .java_overloads = java_overloads,
            .zig_same_name_count = zig_same_name_count,
        });
    }
    return rows.toOwnedSlice(allocator);
}

fn buildJavaWithoutSameName(
    allocator: std.mem.Allocator,
    rows: []const CoverageRow,
) ![]const []const u8 {
    var missing: std.ArrayListUnmanaged([]const u8) = .empty;
    for (rows) |row| {
        if (row.zig_same_name_count == 0) {
            try missing.append(allocator, row.name);
        }
    }
    return missing.toOwnedSlice(allocator);
}

fn writeJsonReport(
    java_decls: []const MethodDecl,
    zig_decls: []const MethodDecl,
    java_names: []const []const u8,
    zig_names: []const []const u8,
    coverage_rows: []const CoverageRow,
    java_without_same_name: []const []const u8,
) !void {
    const file = try std.fs.cwd().createFile(JsonOutputPath, .{ .truncate = true });
    defer file.close();

    var buffer: [8192]u8 = undefined;
    var fw = file.writer(&buffer);
    const writer = &fw.interface;

    var covered_count: usize = 0;
    for (coverage_rows) |row| {
        if (row.zig_same_name_count > 0) covered_count += 1;
    }

    try writer.writeAll("{\n");
    try writer.print("  \"java_source\": \"{s}\",\n", .{JavaSourcePath});
    try writer.print("  \"zig_source\": \"{s}\",\n", .{ZigSourcePath});
    try writer.print("  \"java_public_declaration_count\": {d},\n", .{java_decls.len});
    try writer.print("  \"zig_public_declaration_count\": {d},\n", .{zig_decls.len});
    try writer.print("  \"java_method_family_count\": {d},\n", .{java_names.len});
    try writer.print("  \"zig_method_family_count\": {d},\n", .{zig_names.len});
    try writer.print("  \"java_method_families_with_same_name_coverage\": {d},\n", .{covered_count});
    try writer.print(
        "  \"java_method_families_without_same_name_coverage\": {d},\n",
        .{java_without_same_name.len},
    );

    try writer.writeAll("  \"coverage_rows\": [\n");
    for (coverage_rows, 0..) |row, idx| {
        try writer.writeAll("    {\n");
        try writer.writeAll("      \"name\": ");
        try writeJsonEscapedString(writer, row.name);
        try writer.writeAll(",\n");
        try writer.print("      \"java_overloads\": {d},\n", .{row.java_overloads});
        try writer.print("      \"zig_same_name_count\": {d},\n", .{row.zig_same_name_count});
        try writer.print(
            "      \"same_name_covered\": {s}\n",
            .{if (row.zig_same_name_count > 0) "true" else "false"},
        );
        try writer.writeAll("    }");
        if (idx + 1 != coverage_rows.len) try writer.writeAll(",");
        try writer.writeAll("\n");
    }
    try writer.writeAll("  ],\n");

    try writer.writeAll("  \"java_without_same_name\": [");
    for (java_without_same_name, 0..) |name, idx| {
        if (idx == 0) try writer.writeAll("\n");
        try writer.writeAll("    ");
        try writeJsonEscapedString(writer, name);
        if (idx + 1 != java_without_same_name.len) try writer.writeAll(",\n") else try writer.writeAll("\n");
    }
    if (java_without_same_name.len == 0) {
        try writer.writeAll("],\n");
    } else {
        try writer.writeAll("  ],\n");
    }

    try writer.writeAll("  \"java_declarations\": [\n");
    for (java_decls, 0..) |decl, idx| {
        try writeDeclJson(writer, decl, idx + 1 == java_decls.len);
    }
    try writer.writeAll("  ],\n");

    try writer.writeAll("  \"zig_declarations\": [\n");
    for (zig_decls, 0..) |decl, idx| {
        try writeDeclJson(writer, decl, idx + 1 == zig_decls.len);
    }
    try writer.writeAll("  ]\n");
    try writer.writeAll("}\n");
    try writer.flush();
}

fn writeDeclJson(writer: *std.Io.Writer, decl: MethodDecl, is_last: bool) !void {
    try writer.writeAll("    {\n");
    try writer.writeAll("      \"name\": ");
    try writeJsonEscapedString(writer, decl.name);
    try writer.writeAll(",\n");
    try writer.print("      \"line\": {d},\n", .{decl.line});
    try writer.writeAll("      \"signature\": ");
    try writeJsonEscapedString(writer, decl.signature);
    try writer.writeAll("\n");
    try writer.writeAll("    }");
    if (!is_last) try writer.writeAll(",");
    try writer.writeAll("\n");
}

fn writeMarkdownReport(
    java_decls: []const MethodDecl,
    zig_decls: []const MethodDecl,
    java_names: []const []const u8,
    zig_names: []const []const u8,
    coverage_rows: []const CoverageRow,
    java_without_same_name: []const []const u8,
) !void {
    const file = try std.fs.cwd().createFile(MarkdownOutputPath, .{ .truncate = true });
    defer file.close();

    var buffer: [8192]u8 = undefined;
    var fw = file.writer(&buffer);
    const writer = &fw.interface;

    var covered_count: usize = 0;
    for (coverage_rows) |row| {
        if (row.zig_same_name_count > 0) covered_count += 1;
    }

    try writer.writeAll("# ExchangeApi Class-by-Class Parity Audit (Java vs Zig)\n\n");
    try writer.print("- Java public declarations: **{d}**\n", .{java_decls.len});
    try writer.print("- Zig public declarations: **{d}**\n", .{zig_decls.len});
    try writer.print("- Java method families (unique names): **{d}**\n", .{java_names.len});
    try writer.print("- Zig method families (unique names): **{d}**\n", .{zig_names.len});
    try writer.print("- Java families with same-name Zig coverage: **{d}**\n", .{covered_count});
    try writer.print("- Java families without same-name Zig coverage: **{d}**\n\n", .{java_without_same_name.len});
    try writer.writeAll("Generated by `src/zig/tools/java_exchange_api_parity_audit.zig`.\n\n");

    try writer.writeAll("## Coverage Matrix\n\n");
    try writer.writeAll("| Method family | Java overloads | Zig same-name fns | Same-name coverage |\n");
    try writer.writeAll("| --- | ---: | ---: | --- |\n");
    for (coverage_rows) |row| {
        try writer.print(
            "| `{s}` | {d} | {d} | {s} |\n",
            .{
                row.name,
                row.java_overloads,
                row.zig_same_name_count,
                if (row.zig_same_name_count > 0) "yes" else "no",
            },
        );
    }
    try writer.writeAll("\n");

    try writer.writeAll("## Java Families Without Same-Name Coverage\n\n");
    if (java_without_same_name.len == 0) {
        try writer.writeAll("_None._\n\n");
    } else {
        for (java_without_same_name) |name| {
            try writer.print("- `{s}`\n", .{name});
        }
        try writer.writeAll("\n");
    }

    try writer.writeAll("## Java Public Declarations\n\n");
    for (java_names) |name| {
        try writer.print("### `{s}`\n\n", .{name});
        for (java_decls) |decl| {
            if (!std.mem.eql(u8, decl.name, name)) continue;
            try writer.print("- `{s}` (`{s}:{d}`)\n", .{
                decl.signature,
                JavaSourcePath,
                decl.line,
            });
        }
        try writer.writeAll("\n");
    }

    try writer.writeAll("## Zig Same-Name Declaration Coverage\n\n");
    for (java_names) |name| {
        try writer.print("### `{s}`\n\n", .{name});
        var found: usize = 0;
        for (zig_decls) |decl| {
            if (!std.mem.eql(u8, decl.name, name)) continue;
            found += 1;
            try writer.print("- `{s}` (`{s}:{d}`)\n", .{
                decl.signature,
                ZigSourcePath,
                decl.line,
            });
        }
        if (found == 0) {
            try writer.writeAll("- _No same-name Zig declaration._\n");
        }
        try writer.writeAll("\n");
    }

    try writer.flush();
}

fn writeJsonEscapedString(writer: *std.Io.Writer, value: []const u8) !void {
    try writer.writeByte('"');
    for (value) |ch| {
        switch (ch) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => try writer.writeByte(ch),
        }
    }
    try writer.writeByte('"');
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
}
