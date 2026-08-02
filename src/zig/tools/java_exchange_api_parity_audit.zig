const std = @import("std");

const JavaSourcePath = "src/main/java/exchange/core2/core/ExchangeApi.java";
const ZigSourcePath = "src/zig/core/exchange_api.zig";
const JsonOutputPath = "docs/zig/evidence/EXCHANGE_API_PARITY_AUDIT.json";
const MarkdownOutputPath = "docs/zig/evidence/EXCHANGE_API_PARITY_AUDIT.md";

const AuditError = error{
    MissingMethodName,
    MissingSameNameCoverage,
    MissingSemanticMapping,
    MissingSemanticEntrypoint,
    MissingSemanticEvidence,
    MissingSemanticEvidenceCall,
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

const SemanticMapping = struct {
    name: []const u8,
    zig_entrypoint: []const u8,
    observable_contract: []const u8,
    evidence_file: []const u8,
    evidence_test: []const u8,
    evidence_call: []const u8,
};

const semantic_mappings = [_]SemanticMapping{
    .{ .name = "balanceAdjustment", .zig_entrypoint = "balanceAdjustment", .observable_contract = "The same-name detached entrypoint maps balance fields and service, group, and timestamp metadata into the raw command carrier.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api metadata entrypoints map to raw command fields in journaled input", .evidence_call = "api.balanceAdjustment(" },
    .{ .name = "binaryData", .zig_entrypoint = "binaryData", .observable_contract = "The same-name detached entrypoint preserves all five carrier words, the last-frame marker, and service, group, and timestamp metadata.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api metadata entrypoints map to raw command fields in journaled input", .evidence_call = "api.binaryData(" },
    .{ .name = "cancelOrder", .zig_entrypoint = "cancelOrder", .observable_contract = "The same-name detached entrypoint preserves order, symbol, uid, and service, group, and timestamp metadata.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api metadata entrypoints map to raw command fields in journaled input", .evidence_call = "api.cancelOrder(" },
    .{ .name = "createUser", .zig_entrypoint = "createUser", .observable_contract = "The same-name detached entrypoint preserves uid and service, group, and timestamp metadata.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api metadata entrypoints map to raw command fields in journaled input", .evidence_call = "api.createUser(" },
    .{ .name = "groupingControl", .zig_entrypoint = "groupingControl", .observable_contract = "The same-name detached entrypoint preserves the grouping mode and timestamp in the raw command carrier.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api metadata entrypoints map to raw command fields in journaled input", .evidence_call = "api.groupingControl(" },
    .{ .name = "moveOrder", .zig_entrypoint = "moveOrder", .observable_contract = "The same-name detached entrypoint preserves new price, order, symbol, uid, and service, group, and timestamp metadata.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api metadata entrypoints map to raw command fields in journaled input", .evidence_call = "api.moveOrder(" },
    .{ .name = "orderBookRequest", .zig_entrypoint = "orderBookRequest", .observable_contract = "The callback entrypoint returns one completed command with an owned L2 snapshot for the requested symbol and depth.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api orderBookRequest callback wrapper exposes snapshot response", .evidence_call = "api.orderBookRequest(" },
    .{ .name = "placeNewOrder", .zig_entrypoint = "placeNewOrder", .observable_contract = "The same-name detached entrypoint preserves order fields and service, group, and timestamp metadata in the raw carrier.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api metadata entrypoints map to raw command fields in journaled input", .evidence_call = "api.placeNewOrder(" },
    .{ .name = "processReport", .zig_entrypoint = "processJavaReport", .observable_contract = "The explicitly named semantic entrypoint executes the report query and returns its owned exact-Java reduced payload.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api processJavaReport returns exact Java report payload in one step", .evidence_call = "api.processJavaReport(" },
    .{ .name = "processResult", .zig_entrypoint = "processResult", .observable_contract = "The same-name entrypoint invokes the callback only after readiness and consumes the completed result exactly once.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api processResult consumes callback result after drain", .evidence_call = "api.processResult(" },
    .{ .name = "publishBinaryData", .zig_entrypoint = "publishBinaryData", .observable_contract = "The alias frames an encoded add-symbols payload, applies it, and resolves the terminal async handle successfully.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api publishBinaryData alias resolves encoded add-symbols payload", .evidence_call = "api.publishBinaryData(" },
    .{ .name = "publishQuery", .zig_entrypoint = "publishQuery", .observable_contract = "The alias publishes a state-hash query and returns the owned exact-Java report payload.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api publishQuery alias resolves report payload", .evidence_call = "api.publishQuery(" },
    .{ .name = "reduceOrder", .zig_entrypoint = "reduceOrder", .observable_contract = "The same-name detached entrypoint preserves reduce size, order, symbol, uid, and service, group, and timestamp metadata.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api metadata entrypoints map to raw command fields in journaled input", .evidence_call = "api.reduceOrder(" },
    .{ .name = "requestOrderBookAsync", .zig_entrypoint = "requestOrderBookAsync", .observable_contract = "The async entrypoint retains symbol and depth and transfers ownership of the terminal L2 snapshot to the caller.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api order-book async helper returns typed snapshot ownership", .evidence_call = "api.requestOrderBookAsync(" },
    .{ .name = "reset", .zig_entrypoint = "reset", .observable_contract = "The same-name detached entrypoint clears live matching, risk, user, symbol, and in-flight binary state at settlement.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api reset entrypoint clears live state and in-flight binary assembly", .evidence_call = "api.reset(" },
    .{ .name = "resumeUser", .zig_entrypoint = "resumeUser", .observable_contract = "The same-name detached entrypoint preserves uid and service, group, and timestamp metadata.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api metadata entrypoints map to raw command fields in journaled input", .evidence_call = "api.resumeUser(" },
    .{ .name = "submitBinaryCommandAsync", .zig_entrypoint = "submitBinaryCommandAsync", .observable_contract = "The alias frames and applies an encoded add-symbols payload and resolves its terminal async handle once with success.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api generic submitBinaryCommandAsync alias resolves encoded add-symbols payload", .evidence_call = "api.submitBinaryCommandAsync(" },
    .{ .name = "submitBinaryDataAsync", .zig_entrypoint = "submitBinaryDataAsync", .observable_contract = "The same-name entrypoint frames and applies an encoded add-symbols payload and resolves its terminal async handle once with success.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api submitBinaryDataAsync entrypoint resolves encoded add-symbols payload", .evidence_call = "api.submitBinaryDataAsync(" },
    .{ .name = "submitCommand", .zig_entrypoint = "submitCommand", .observable_contract = "Detached publication advances the global sequence without retaining a result slot, and a later synchronous barrier settles it.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api detached submit and Java list and stream barriers preserve timing", .evidence_call = "api.submitCommand(" },
    .{ .name = "submitCommandAsync", .zig_entrypoint = "submitCommandAsync", .observable_contract = "Async publication becomes ready only after drain and transfers one successful owned result to the caller.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api async handle flow drains and returns result payload", .evidence_call = "api.submitCommandAsync(" },
    .{ .name = "submitCommandAsyncFullResponse", .zig_entrypoint = "submitCommandAsyncFullResponse", .observable_contract = "A supported command resolves to an owned completed command response after drain.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api async full-response helper returns command response", .evidence_call = "api.submitCommandAsyncFullResponse(" },
    .{ .name = "submitCommandsSync", .zig_entrypoint = "submitCommandsSync", .observable_contract = "A nonempty slice settles in order before return, while an empty slice is a no-op and neither path retains result slots.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api detached submit and Java list and stream barriers preserve timing", .evidence_call = "api.submitCommandsSync(" },
    .{ .name = "submitQueryAsync", .zig_entrypoint = "submitQueryAsync", .observable_contract = "The alias publishes a state-hash query and transfers its owned exact-Java report payload exactly once.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api generic submitQueryAsync alias resolves report payload", .evidence_call = "api.submitQueryAsync(" },
    .{ .name = "suspendUser", .zig_entrypoint = "suspendUser", .observable_contract = "The same-name detached entrypoint preserves uid and service, group, and timestamp metadata.", .evidence_file = "src/zig/core/exchange_api.zig", .evidence_test = "exchange api metadata entrypoints map to raw command fields in journaled input", .evidence_call = "api.suspendUser(" },
};

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();

    var arena_state = std.heap.ArenaAllocator.init(gpa.allocator());
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const java_source = try std.Io.Dir.cwd().readFileAlloc(
        init.io,
        JavaSourcePath,
        arena,
        .unlimited,
    );
    const zig_source = try std.Io.Dir.cwd().readFileAlloc(
        init.io,
        ZigSourcePath,
        arena,
        .unlimited,
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
    try validateSemanticMappings(
        arena,
        init.io,
        java_names,
        zig_source,
    );

    try writeJsonReport(
        init.io,
        java_decls.items,
        zig_decls.items,
        java_names,
        zig_names,
        coverage_rows,
        java_without_same_name,
        semantic_mappings[0..],
    );
    try writeMarkdownReport(
        init.io,
        java_decls.items,
        zig_decls.items,
        java_names,
        zig_names,
        coverage_rows,
        java_without_same_name,
        semantic_mappings[0..],
    );

    if (java_without_same_name.len != 0) {
        const stderr = std.Io.File.stderr();
        var err_buffer: [2048]u8 = undefined;
        var err_writer = stderr.writer(init.io, &err_buffer);
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

fn findSemanticMapping(name: []const u8) ?SemanticMapping {
    for (semantic_mappings) |mapping| {
        if (std.mem.eql(u8, mapping.name, name)) return mapping;
    }
    return null;
}

fn validateSemanticMappings(
    allocator: std.mem.Allocator,
    io: std.Io,
    java_names: []const []const u8,
    zig_source: []const u8,
) !void {
    for (java_names) |name| {
        const mapping = findSemanticMapping(name) orelse
            return error.MissingSemanticMapping;
        const entrypoint_needle = try std.fmt.allocPrint(
            allocator,
            "pub fn {s}(",
            .{mapping.zig_entrypoint},
        );
        if (std.mem.indexOf(u8, zig_source, entrypoint_needle) == null) {
            return error.MissingSemanticEntrypoint;
        }

        const evidence_source = try std.Io.Dir.cwd().readFileAlloc(
            io,
            mapping.evidence_file,
            allocator,
            .unlimited,
        );
        const evidence_body = try findTestBody(
            allocator,
            evidence_source,
            mapping.evidence_test,
        ) orelse {
            return error.MissingSemanticEvidence;
        };
        if (std.mem.indexOf(u8, evidence_body, mapping.evidence_call) == null) {
            return error.MissingSemanticEvidenceCall;
        }
    }

    for (semantic_mappings, 0..) |mapping, index| {
        var matches_java = false;
        for (java_names) |name| {
            if (std.mem.eql(u8, mapping.name, name)) {
                matches_java = true;
                break;
            }
        }
        if (!matches_java) return error.MissingSemanticMapping;
        for (semantic_mappings[index + 1 ..]) |later| {
            if (std.mem.eql(u8, mapping.name, later.name)) {
                return error.MissingSemanticMapping;
            }
        }
    }
}

fn findTestBody(
    allocator: std.mem.Allocator,
    source: []const u8,
    title: []const u8,
) std.mem.Allocator.Error!?[]const u8 {
    const marker = try std.fmt.allocPrint(allocator, "test \"{s}\"", .{title});
    defer allocator.free(marker);
    const marker_index = std.mem.indexOf(u8, source, marker) orelse return null;
    const after_marker = marker_index + marker.len;
    const relative_open = std.mem.indexOfScalar(u8, source[after_marker..], '{') orelse return null;
    const open_index = after_marker + relative_open;
    const close_index = findMatchingBrace(source, open_index) orelse return null;
    return source[open_index + 1 .. close_index];
}

fn findMatchingBrace(source: []const u8, open_index: usize) ?usize {
    if (open_index >= source.len or source[open_index] != '{') return null;

    const ScanState = enum {
        normal,
        string,
        character,
        line_comment,
        block_comment,
    };
    var state: ScanState = .normal;
    var block_comment_depth: usize = 0;
    var depth: usize = 0;
    var escaped = false;
    var index = open_index;
    while (index < source.len) : (index += 1) {
        const ch = source[index];
        const next = if (index + 1 < source.len) source[index + 1] else 0;
        switch (state) {
            .normal => {
                if (ch == '/' and next == '/') {
                    state = .line_comment;
                    index += 1;
                } else if (ch == '/' and next == '*') {
                    state = .block_comment;
                    block_comment_depth = 1;
                    index += 1;
                } else if (ch == '"') {
                    state = .string;
                    escaped = false;
                } else if (ch == '\'') {
                    state = .character;
                    escaped = false;
                } else if (ch == '{') {
                    depth += 1;
                } else if (ch == '}') {
                    if (depth == 0) return null;
                    depth -= 1;
                    if (depth == 0) return index;
                }
            },
            .string => {
                if (escaped) {
                    escaped = false;
                } else if (ch == '\\') {
                    escaped = true;
                } else if (ch == '"') {
                    state = .normal;
                }
            },
            .character => {
                if (escaped) {
                    escaped = false;
                } else if (ch == '\\') {
                    escaped = true;
                } else if (ch == '\'') {
                    state = .normal;
                }
            },
            .line_comment => {
                if (ch == '\n') state = .normal;
            },
            .block_comment => {
                if (ch == '/' and next == '*') {
                    block_comment_depth += 1;
                    index += 1;
                } else if (ch == '*' and next == '/') {
                    block_comment_depth -= 1;
                    index += 1;
                    if (block_comment_depth == 0) state = .normal;
                }
            },
        }
    }
    return null;
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
        const method_name = std.mem.trimEnd(u8, tail[0..open_paren], " \t");
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
    io: std.Io,
    java_decls: []const MethodDecl,
    zig_decls: []const MethodDecl,
    java_names: []const []const u8,
    zig_names: []const []const u8,
    coverage_rows: []const CoverageRow,
    java_without_same_name: []const []const u8,
    semantic_rows: []const SemanticMapping,
) !void {
    const file = try std.Io.Dir.cwd().createFile(io, JsonOutputPath, .{ .truncate = true });
    defer file.close(io);

    var buffer: [8192]u8 = undefined;
    var fw = file.writer(io, &buffer);
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
    try writer.print(
        "  \"java_method_families_with_semantic_evidence\": {d},\n",
        .{semantic_rows.len},
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

    try writer.writeAll("  \"semantic_rows\": [\n");
    for (semantic_rows, 0..) |row, idx| {
        try writer.writeAll("    {\n");
        try writer.writeAll("      \"name\": ");
        try writeJsonEscapedString(writer, row.name);
        try writer.writeAll(",\n      \"zig_entrypoint\": ");
        try writeJsonEscapedString(writer, row.zig_entrypoint);
        try writer.writeAll(",\n      \"observable_contract\": ");
        try writeJsonEscapedString(writer, row.observable_contract);
        try writer.writeAll(",\n      \"evidence_file\": ");
        try writeJsonEscapedString(writer, row.evidence_file);
        try writer.writeAll(",\n      \"evidence_test\": ");
        try writeJsonEscapedString(writer, row.evidence_test);
        try writer.writeAll(",\n      \"evidence_call\": ");
        try writeJsonEscapedString(writer, row.evidence_call);
        try writer.writeAll("\n    }");
        if (idx + 1 != semantic_rows.len) try writer.writeAll(",");
        try writer.writeAll("\n");
    }
    try writer.writeAll("  ],\n");

    try writer.writeAll("  \"java_declarations\": [\n");
    for (java_decls, 0..) |decl, idx| {
        const mapping = findSemanticMapping(decl.name) orelse
            return error.MissingSemanticMapping;
        try writeJavaDeclJson(
            writer,
            decl,
            mapping,
            idx + 1 == java_decls.len,
        );
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

fn writeJavaDeclJson(
    writer: *std.Io.Writer,
    decl: MethodDecl,
    mapping: SemanticMapping,
    is_last: bool,
) !void {
    try writer.writeAll("    {\n");
    try writer.writeAll("      \"name\": ");
    try writeJsonEscapedString(writer, decl.name);
    try writer.writeAll(",\n");
    try writer.print("      \"line\": {d},\n", .{decl.line});
    try writer.writeAll("      \"signature\": ");
    try writeJsonEscapedString(writer, decl.signature);
    try writer.writeAll(",\n      \"zig_entrypoint\": ");
    try writeJsonEscapedString(writer, mapping.zig_entrypoint);
    try writer.writeAll(",\n      \"observable_contract\": ");
    try writeJsonEscapedString(writer, mapping.observable_contract);
    try writer.writeAll(",\n      \"evidence_file\": ");
    try writeJsonEscapedString(writer, mapping.evidence_file);
    try writer.writeAll(",\n      \"evidence_test\": ");
    try writeJsonEscapedString(writer, mapping.evidence_test);
    try writer.writeAll(",\n      \"evidence_call\": ");
    try writeJsonEscapedString(writer, mapping.evidence_call);
    try writer.writeAll("\n");
    try writer.writeAll("    }");
    if (!is_last) try writer.writeAll(",");
    try writer.writeAll("\n");
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
    io: std.Io,
    java_decls: []const MethodDecl,
    zig_decls: []const MethodDecl,
    java_names: []const []const u8,
    zig_names: []const []const u8,
    coverage_rows: []const CoverageRow,
    java_without_same_name: []const []const u8,
    semantic_rows: []const SemanticMapping,
) !void {
    const file = try std.Io.Dir.cwd().createFile(io, MarkdownOutputPath, .{ .truncate = true });
    defer file.close(io);

    var buffer: [8192]u8 = undefined;
    var fw = file.writer(io, &buffer);
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
    try writer.print("- Java families with scoped executable semantic evidence: **{d}**\n\n", .{semantic_rows.len});
    try writer.writeAll("Generated by `src/zig/tools/java_exchange_api_parity_audit.zig`.\n\n");
    try writer.writeAll("Same-name coverage is structural only. Each semantic row is narrower: the generator locates the exact cited test body, requires the recorded direct-call token inside that body, and reports only the observable contract asserted there. A row is not evidence that every Java overload or every callback/helper variant is behaviorally identical.\n\n");

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

    try writer.writeAll("## Observable-Semantics Matrix\n\n");
    try writer.writeAll("| Java family | Zig entrypoint | Observable contract | Direct-call token | Executable evidence |\n");
    try writer.writeAll("| --- | --- | --- | --- | --- |\n");
    for (semantic_rows) |row| {
        try writer.print(
            "| `{s}` | `{s}` | {s} | `{s}` | `{s}` in `{s}` |\n",
            .{
                row.name,
                row.zig_entrypoint,
                row.observable_contract,
                row.evidence_call,
                row.evidence_test,
                row.evidence_file,
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

test "semantic evidence call must occur inside the exact cited test body" {
    const source =
        "test \"first\" {\n" ++
        "    // A comment containing } must not terminate the body.\n" ++
        "    const text = \"another } brace\";\n" ++
        "    _ = text;\n" ++
        "}\n" ++
        "test \"second\" {\n" ++
        "    try api.target();\n" ++
        "}\n";

    const first = (try findTestBody(std.testing.allocator, source, "first")) orelse
        return error.MissingSemanticEvidence;
    try std.testing.expect(std.mem.indexOf(u8, first, "api.target(") == null);

    const second = (try findTestBody(std.testing.allocator, source, "second")) orelse
        return error.MissingSemanticEvidence;
    try std.testing.expect(std.mem.indexOf(u8, second, "api.target(") != null);
}
