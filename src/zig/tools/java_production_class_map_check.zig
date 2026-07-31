const std = @import("std");

const JavaRoot = "src/main/java/exchange/core2/core";
const ZigRoot = "src/zig";
const MapPath = "src/zig/JAVA_PRODUCTION_CLASS_MAP.md";
const ExpectedJavaFiles = 91;

const CheckError = error{
    DuplicateJavaSource,
    DuplicateMapSource,
    EmptyEvidence,
    InvalidOwnerFormat,
    InvalidRow,
    InvalidStatus,
    MissingJavaSource,
    MissingOwner,
    PlaceholderEvidence,
    UnexpectedJavaFileCount,
    UnknownJavaSource,
};

const PathList = std.ArrayListUnmanaged([]const u8);
const PathSet = std.StringHashMapUnmanaged(void);

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const map = try std.Io.Dir.cwd().readFileAlloc(init.io, MapPath, allocator, .unlimited);
    defer allocator.free(map);

    var java_paths: PathList = .empty;
    defer freePaths(allocator, &java_paths);
    try collectFiles(allocator, init.io, JavaRoot, JavaRoot, ".java", &java_paths);
    if (java_paths.items.len != ExpectedJavaFiles) {
        return CheckError.UnexpectedJavaFileCount;
    }

    var zig_paths: PathList = .empty;
    defer freePaths(allocator, &zig_paths);
    try collectFiles(allocator, init.io, ZigRoot, "", ".zig", &zig_paths);

    validateMap(allocator, java_paths.items, zig_paths.items, map) catch |err| {
        var stderr_buffer: [1024]u8 = undefined;
        var stderr_writer = std.Io.File.stderr().writer(init.io, &stderr_buffer);
        try stderr_writer.interface.print("invalid Java production class map: {s}\n", .{@errorName(err)});
        try stderr_writer.interface.flush();
        return err;
    };
}

fn collectFiles(
    allocator: std.mem.Allocator,
    io: std.Io,
    directory_path: []const u8,
    relative_root: []const u8,
    extension: []const u8,
    paths: *PathList,
) !void {
    var dir = try std.Io.Dir.cwd().openDir(io, directory_path, .{ .iterate = true });
    defer dir.close(io);

    var iterator = dir.iterate();
    while (try iterator.next(io)) |entry| {
        const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ directory_path, entry.name });
        switch (entry.kind) {
            .directory => {
                defer allocator.free(path);
                try collectFiles(allocator, io, path, relative_root, extension, paths);
            },
            .file => {
                if (!std.mem.endsWith(u8, entry.name, extension)) {
                    allocator.free(path);
                    continue;
                }
                const relative_path = if (relative_root.len == 0)
                    path
                else blk: {
                    const prefix_len = relative_root.len + 1;
                    const copy = try allocator.dupe(u8, path[prefix_len..]);
                    allocator.free(path);
                    break :blk copy;
                };
                errdefer allocator.free(relative_path);
                try paths.append(allocator, relative_path);
            },
            else => allocator.free(path),
        }
    }
}

fn freePaths(allocator: std.mem.Allocator, paths: *PathList) void {
    for (paths.items) |path| allocator.free(path);
    paths.deinit(allocator);
}

fn validateMap(
    allocator: std.mem.Allocator,
    java_paths: []const []const u8,
    owner_paths: []const []const u8,
    map: []const u8,
) (std.mem.Allocator.Error || CheckError)!void {
    var java_set: PathSet = .empty;
    defer java_set.deinit(allocator);
    for (java_paths) |path| {
        const result = try java_set.getOrPut(allocator, path);
        if (result.found_existing) return CheckError.DuplicateJavaSource;
    }

    var owner_set: PathSet = .empty;
    defer owner_set.deinit(allocator);
    for (owner_paths) |path| try owner_set.put(allocator, path, {});

    var mapped_sources: PathSet = .empty;
    defer mapped_sources.deinit(allocator);

    var lines = std.mem.splitScalar(u8, map, '\n');
    while (lines.next()) |line| {
        if (!isMapRow(line)) continue;
        const row = try parseRow(line);

        if (!java_set.contains(row.java_source)) return CheckError.UnknownJavaSource;
        const source_result = try mapped_sources.getOrPut(allocator, row.java_source);
        if (source_result.found_existing) return CheckError.DuplicateMapSource;

        try validateOwners(&owner_set, row.owners);
        if (!isAllowedStatus(row.status)) return CheckError.InvalidStatus;
        try validateEvidence(row.evidence);
    }

    if (mapped_sources.count() != java_set.count()) return CheckError.MissingJavaSource;
    var java_iterator = java_set.keyIterator();
    while (java_iterator.next()) |path| {
        if (!mapped_sources.contains(path.*)) return CheckError.MissingJavaSource;
    }
}

const Row = struct {
    java_source: []const u8,
    owners: []const u8,
    status: []const u8,
    evidence: []const u8,
};

fn isMapRow(line: []const u8) bool {
    return std.mem.startsWith(u8, line, "| `") and std.mem.indexOf(u8, line, ".java` |") != null;
}

fn parseRow(line: []const u8) CheckError!Row {
    if (line.len < 2 or line[0] != '|' or line[line.len - 1] != '|') {
        return CheckError.InvalidRow;
    }
    var fields: [4][]const u8 = undefined;
    var field_count: usize = 0;
    var pieces = std.mem.splitScalar(u8, line[1 .. line.len - 1], '|');
    while (pieces.next()) |piece| {
        if (field_count == fields.len) return CheckError.InvalidRow;
        fields[field_count] = std.mem.trim(u8, piece, " \t");
        field_count += 1;
    }
    if (field_count != fields.len) return CheckError.InvalidRow;

    return .{
        .java_source = unwrapCode(fields[0]) orelse return CheckError.InvalidRow,
        .owners = fields[1],
        .status = fields[2],
        .evidence = fields[3],
    };
}

fn validateOwners(owner_set: *const PathSet, owners: []const u8) CheckError!void {
    if (owners.len == 0) return CheckError.InvalidOwnerFormat;
    var owner_iterator = std.mem.splitSequence(u8, owners, ", ");
    var count: usize = 0;
    while (owner_iterator.next()) |owner_field| {
        const owner = unwrapCode(owner_field) orelse return CheckError.InvalidOwnerFormat;
        if (!std.mem.startsWith(u8, owner, ZigRoot ++ "/") or
            !std.mem.endsWith(u8, owner, ".zig"))
        {
            return CheckError.InvalidOwnerFormat;
        }
        if (!owner_set.contains(owner)) return CheckError.MissingOwner;
        count += 1;
    }
    if (count == 0) return CheckError.InvalidOwnerFormat;
}

fn unwrapCode(value: []const u8) ?[]const u8 {
    if (value.len < 3 or value[0] != '`' or value[value.len - 1] != '`') return null;
    const contents = value[1 .. value.len - 1];
    if (std.mem.indexOfScalar(u8, contents, '`') != null) return null;
    return contents;
}

fn isAllowedStatus(status: []const u8) bool {
    return std.mem.eql(u8, status, "Closed") or
        std.mem.eql(u8, status, "Intentional native boundary") or
        std.mem.eql(u8, status, "Intentional capability boundary");
}

fn validateEvidence(evidence: []const u8) CheckError!void {
    if (std.mem.trim(u8, evidence, " \t").len == 0) return CheckError.EmptyEvidence;
    const forbidden = [_][]const u8{ "partial", "placeholder", "notimplemented" };
    for (forbidden) |needle| {
        if (containsIgnoreCase(evidence, needle)) return CheckError.PlaceholderEvidence;
    }
}

fn containsIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len > haystack.len) return false;
    var start: usize = 0;
    while (start + needle.len <= haystack.len) : (start += 1) {
        if (std.ascii.eqlIgnoreCase(haystack[start .. start + needle.len], needle)) return true;
    }
    return false;
}

const complete_map =
    "| Java production source | Zig owners | Status | Responsibility evidence |\n" ++
    "| --- | --- | --- | --- |\n" ++
    "| `a/Foo.java` | `src/zig/core/foo.zig` | Closed | Owns Foo behavior. |\n" ++
    "| `b/Foo.java` | `src/zig/core/foo.zig` | Intentional native boundary | Native traversal replaces the Java mechanism. |\n";
const test_java_paths = [_][]const u8{ "a/Foo.java", "b/Foo.java" };
const test_owner_paths = [_][]const u8{"src/zig/core/foo.zig"};

test "complete map accepts duplicate basenames in different directories" {
    try validateMap(std.testing.allocator, &test_java_paths, &test_owner_paths, complete_map);
}

test "missing owner is rejected" {
    const map = "| `a/Foo.java` | `src/zig/core/missing.zig` | Closed | Owns Foo behavior. |\n";
    try std.testing.expectError(error.MissingOwner, validateMap(std.testing.allocator, test_java_paths[0..1], &test_owner_paths, map));
}

test "invalid status is rejected" {
    const map = "| `a/Foo.java` | `src/zig/core/foo.zig` | Partial | Some behavior. |\n";
    try std.testing.expectError(error.InvalidStatus, validateMap(std.testing.allocator, test_java_paths[0..1], &test_owner_paths, map));
}

test "missing source path is rejected" {
    const map = "| `a/Foo.java` | `src/zig/core/foo.zig` | Closed | Owns Foo behavior. |\n";
    try std.testing.expectError(error.MissingJavaSource, validateMap(std.testing.allocator, &test_java_paths, &test_owner_paths, map));
}

test "duplicate source path is rejected" {
    const map =
        "| `a/Foo.java` | `src/zig/core/foo.zig` | Closed | Owns Foo behavior. |\n" ++
        "| `a/Foo.java` | `src/zig/core/foo.zig` | Closed | Duplicate claim. |\n";
    try std.testing.expectError(error.DuplicateMapSource, validateMap(std.testing.allocator, test_java_paths[0..1], &test_owner_paths, map));
}

test "missing and placeholder evidence are rejected" {
    const missing = "| `a/Foo.java` | `src/zig/core/foo.zig` | Closed | |\n";
    try std.testing.expectError(error.EmptyEvidence, validateMap(std.testing.allocator, test_java_paths[0..1], &test_owner_paths, missing));

    const placeholder = "| `a/Foo.java` | `src/zig/core/foo.zig` | Closed | Placeholder responsibility. |\n";
    try std.testing.expectError(error.PlaceholderEvidence, validateMap(std.testing.allocator, test_java_paths[0..1], &test_owner_paths, placeholder));

    const not_implemented = "| `a/Foo.java` | `src/zig/core/foo.zig` | Closed | NotImplemented. |\n";
    try std.testing.expectError(error.PlaceholderEvidence, validateMap(std.testing.allocator, test_java_paths[0..1], &test_owner_paths, not_implemented));
}
