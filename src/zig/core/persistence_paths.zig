const std = @import("std");

pub const ModuleType = enum {
    risk_engine,
    matching_engine_router,
};

pub const FileNameError = std.mem.Allocator.Error || error{
    EmptyExchangeId,
};

pub const ResolveError = std.mem.Allocator.Error || error{
    EmptyStorageFolder,
};

pub const PathError = FileNameError || ResolveError;

pub fn moduleCode(module_type: ModuleType) []const u8 {
    return switch (module_type) {
        .risk_engine => "RE",
        .matching_engine_router => "ME",
    };
}

pub fn snapshotFileName(
    allocator: std.mem.Allocator,
    exchange_id: []const u8,
    snapshot_id: u64,
    module_type: ModuleType,
    instance_id: u32,
) FileNameError![]u8 {
    if (exchange_id.len == 0) return error.EmptyExchangeId;
    return std.fmt.allocPrint(
        allocator,
        "{s}_snapshot_{d}_{s}{d}.ecs",
        .{ exchange_id, snapshot_id, moduleCode(module_type), instance_id },
    );
}

pub fn journalFileName(
    allocator: std.mem.Allocator,
    exchange_id: []const u8,
    snapshot_id: u64,
    partition_id: u16,
) FileNameError![]u8 {
    if (exchange_id.len == 0) return error.EmptyExchangeId;
    return std.fmt.allocPrint(
        allocator,
        "{s}_journal_{d}_{X:0>4}.ecj",
        .{ exchange_id, snapshot_id, partition_id },
    );
}

pub fn mainLogFileName(
    allocator: std.mem.Allocator,
    exchange_id: []const u8,
) FileNameError![]u8 {
    if (exchange_id.len == 0) return error.EmptyExchangeId;
    return std.fmt.allocPrint(allocator, "{s}.eca", .{exchange_id});
}

pub fn resolveInFolder(
    allocator: std.mem.Allocator,
    storage_folder: []const u8,
    file_name: []const u8,
) ResolveError![]u8 {
    if (storage_folder.len == 0) return error.EmptyStorageFolder;
    return std.fs.path.join(allocator, &.{ storage_folder, file_name });
}

pub fn snapshotPath(
    allocator: std.mem.Allocator,
    storage_folder: []const u8,
    exchange_id: []const u8,
    snapshot_id: u64,
    module_type: ModuleType,
    instance_id: u32,
) PathError![]u8 {
    const file_name = try snapshotFileName(
        allocator,
        exchange_id,
        snapshot_id,
        module_type,
        instance_id,
    );
    defer allocator.free(file_name);
    return resolveInFolder(allocator, storage_folder, file_name);
}

pub fn journalPath(
    allocator: std.mem.Allocator,
    storage_folder: []const u8,
    exchange_id: []const u8,
    snapshot_id: u64,
    partition_id: u16,
) PathError![]u8 {
    const file_name = try journalFileName(
        allocator,
        exchange_id,
        snapshot_id,
        partition_id,
    );
    defer allocator.free(file_name);
    return resolveInFolder(allocator, storage_folder, file_name);
}

pub fn mainLogPath(
    allocator: std.mem.Allocator,
    storage_folder: []const u8,
    exchange_id: []const u8,
) PathError![]u8 {
    const file_name = try mainLogFileName(allocator, exchange_id);
    defer allocator.free(file_name);
    return resolveInFolder(allocator, storage_folder, file_name);
}

test "module code parity matches Java constants" {
    try std.testing.expectEqualStrings("RE", moduleCode(.risk_engine));
    try std.testing.expectEqualStrings("ME", moduleCode(.matching_engine_router));
}

test "snapshot filename parity uses module code and instance" {
    const allocator = std.testing.allocator;
    const path = try snapshotFileName(allocator, "EX0", 77, .matching_engine_router, 3);
    defer allocator.free(path);

    try std.testing.expectEqualStrings("EX0_snapshot_77_ME3.ecs", path);
}

test "journal filename parity uses uppercase zero-padded hex partition id" {
    const allocator = std.testing.allocator;
    const path = try journalFileName(allocator, "EX0", 77, 15);
    defer allocator.free(path);
    try std.testing.expectEqualStrings("EX0_journal_77_000F.ecj", path);
}

test "main log filename parity uses eca suffix" {
    const allocator = std.testing.allocator;
    const path = try mainLogFileName(allocator, "EX0");
    defer allocator.free(path);
    try std.testing.expectEqualStrings("EX0.eca", path);
}

test "path helpers resolve within configured storage folder" {
    const allocator = std.testing.allocator;

    const snapshot = try snapshotPath(allocator, "./dumps", "EX0", 10, .risk_engine, 1);
    defer allocator.free(snapshot);
    try std.testing.expect(std.mem.startsWith(u8, snapshot, "./dumps"));
    try std.testing.expect(std.mem.endsWith(u8, snapshot, "EX0_snapshot_10_RE1.ecs"));

    const journal = try journalPath(allocator, "./dumps", "EX0", 10, 0x00AF);
    defer allocator.free(journal);
    try std.testing.expect(std.mem.startsWith(u8, journal, "./dumps"));
    try std.testing.expect(std.mem.endsWith(u8, journal, "EX0_journal_10_00AF.ecj"));

    const main_log = try mainLogPath(allocator, "./dumps", "EX0");
    defer allocator.free(main_log);
    try std.testing.expect(std.mem.startsWith(u8, main_log, "./dumps"));
    try std.testing.expect(std.mem.endsWith(u8, main_log, "EX0.eca"));
}

test "helpers reject empty exchange id and storage folder" {
    const allocator = std.testing.allocator;

    try std.testing.expectError(
        error.EmptyExchangeId,
        snapshotFileName(allocator, "", 1, .risk_engine, 0),
    );
    try std.testing.expectError(
        error.EmptyExchangeId,
        journalFileName(allocator, "", 1, 1),
    );
    try std.testing.expectError(
        error.EmptyExchangeId,
        mainLogFileName(allocator, ""),
    );
    try std.testing.expectError(
        error.EmptyStorageFolder,
        resolveInFolder(allocator, "", "name.ext"),
    );
}
