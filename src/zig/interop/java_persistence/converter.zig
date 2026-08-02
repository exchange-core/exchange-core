const std = @import("std");
const exchange_core = @import("exchange_core");
const io_context = exchange_core.core.io_context;
const java_body = @import("snapshot_body.zig");
const java_import = @import("snapshot_import.zig");
const java_wire = @import("snapshot_wire.zig");
const kernel_mod = exchange_core.core.exchange_kernel;
const legacy = @import("legacy_lz4.zig");
const paths = exchange_core.core.persistence_paths;
const container = exchange_core.core.sharded_snapshot;
const store = exchange_core.core.sharded_snapshot_store;

const log = std.log.scoped(.java_persistence_converter);

pub const RequestValidationError = error{
    EmptyInputStorageFolder,
    EmptyOutputStorageFolder,
    EmptyExchangeId,
    InvalidMatchingShardCount,
    InvalidRiskShardCount,
    TooManySnapshotModules,
    InvalidSequence,
};

/// Explicit offline conversion request for one complete Java snapshot family.
///
/// Java `.ecs` files do not embed the snapshot id, command boundary, timestamp,
/// topology, or runtime configuration. The caller must therefore supply those
/// values from the migration inventory (or a separately validated `.eca` log).
/// `input_storage_folder` and `output_storage_folder` should be disjoint because
/// Java and XCM2 module payloads intentionally share the same `.ecs` filenames.
///
/// No execution mode is needed for snapshot-only conversion. Routed/split mode
/// becomes relevant only when a later journal converter executes Java records.
pub const Request = struct {
    input_dir: std.Io.Dir,
    output_dir: std.Io.Dir,
    input_storage_folder: []const u8,
    output_storage_folder: []const u8,
    exchange_id: []const u8,
    snapshot_id: u64,
    matching_count: u32,
    risk_count: u32,
    base_config: kernel_mod.Config,
    matching_command_seq: u64,
    risk_command_seq: u64,
    timestamp_ns: u64,
    frame_limits: legacy.LegacyLz4DecodeLimits = .{},
    wire_config: java_wire.Config = .{},
    body_limits: java_body.Limits = .{},
    store_options: store.StoreOptions = .{},

    pub fn validate(self: Request) RequestValidationError!void {
        if (self.input_storage_folder.len == 0) return error.EmptyInputStorageFolder;
        if (self.output_storage_folder.len == 0) return error.EmptyOutputStorageFolder;
        if (self.exchange_id.len == 0) return error.EmptyExchangeId;
        if (!validShardCount(self.matching_count)) return error.InvalidMatchingShardCount;
        if (!validShardCount(self.risk_count)) return error.InvalidRiskShardCount;

        const module_count = std.math.add(u32, self.matching_count, self.risk_count) catch
            return error.TooManySnapshotModules;
        if (module_count > self.store_options.container_limits.max_entries) {
            return error.TooManySnapshotModules;
        }

        const expected_risk = std.math.add(u64, self.matching_command_seq, 1) catch
            return error.InvalidSequence;
        if (self.risk_command_seq != expected_risk) return error.InvalidSequence;
    }

    pub fn bundleIdentity(self: Request) store.BundleIdentity {
        return .{
            .snapshot_id = self.snapshot_id,
            .matching_command_seq = self.matching_command_seq,
            .risk_command_seq = self.risk_command_seq,
            .timestamp_ns = self.timestamp_ns,
            .config_sha256 = store.kernelConfigSha256(
                self.base_config,
                self.matching_count,
                self.risk_count,
            ),
        };
    }
};

pub const ConversionSummary = struct {
    source_file_count: u32,
    commit: store.CommitSummary,
};

/// Converts one complete Java `DiskSerializationProcessor` snapshot family into
/// an atomically advertised XCM2 bundle. All Java files are decoded and imported
/// into a private staged topology before the native store is allowed to publish.
pub fn convertSnapshotFamily(
    allocator: std.mem.Allocator,
    request: Request,
) !ConversionSummary {
    try request.validate();

    var staged = try loadSnapshotFamilyFresh(allocator, request);
    defer staged.deinit();

    const commit = try store.captureAndCommit(
        allocator,
        request.output_dir,
        request.output_storage_folder,
        request.exchange_id,
        &staged,
        request.bundleIdentity(),
        request.store_options,
    );
    const source_file_count = std.math.add(u32, request.matching_count, request.risk_count) catch
        unreachable;
    log.info(
        "converted Java snapshot {d}: {d} source modules -> XCM2 bundle",
        .{ request.snapshot_id, source_file_count },
    );
    return .{ .source_file_count = source_file_count, .commit = commit };
}

fn loadSnapshotFamilyFresh(
    allocator: std.mem.Allocator,
    request: Request,
) !kernel_mod.MultiShardKernelSet {
    try request.validate();

    const matching_count: usize = request.matching_count;
    const risk_count: usize = request.risk_count;
    const matching_states = try allocator.alloc(java_body.MatchingEngineState, matching_count);
    defer allocator.free(matching_states);
    var matching_initialized: usize = 0;
    defer for (matching_states[0..matching_initialized]) |*state| state.deinit(allocator);

    for (matching_states, 0..) |*state, shard_id| {
        state.* = try loadMatchingState(allocator, request, shard_id);
        matching_initialized += 1;
    }

    const risk_states = try allocator.alloc(java_body.RiskEngineState, risk_count);
    defer allocator.free(risk_states);
    var risk_initialized: usize = 0;
    defer for (risk_states[0..risk_initialized]) |*state| state.deinit(allocator);

    for (risk_states, 0..) |*state, shard_id| {
        state.* = try loadRiskState(allocator, request, shard_id);
        risk_initialized += 1;
    }

    return java_import.importFresh(allocator, .{
        .matching_states = matching_states,
        .risk_states = risk_states,
        .matching_shard_count = matching_count,
        .risk_shard_count = risk_count,
        .base_config = request.base_config,
        .publication_frontier = request.risk_command_seq,
    });
}

fn loadMatchingState(
    allocator: std.mem.Allocator,
    request: Request,
    shard_id: usize,
) !java_body.MatchingEngineState {
    const path = try paths.snapshotPath(
        allocator,
        request.input_storage_folder,
        request.exchange_id,
        request.snapshot_id,
        .matching_engine_router,
        @intCast(shard_id),
    );
    defer allocator.free(path);

    const frame = try container.readBoundedFileAlloc(
        allocator,
        request.input_dir,
        path,
        request.frame_limits.max_file_size,
    );
    defer allocator.free(frame);
    var decompressed = try legacy.decodeLegacyLz4SnapshotFrame(
        allocator,
        frame,
        request.frame_limits,
    );
    defer decompressed.deinit(allocator);
    const document = try java_wire.unwrapRawDocument(decompressed.bytes, request.wire_config);

    return java_body.decodeMatchingEngineBody(
        allocator,
        document.body,
        .{
            .shard_id = @intCast(shard_id),
            .shard_mask = @intCast(request.matching_count - 1),
        },
        request.body_limits,
    );
}

fn loadRiskState(
    allocator: std.mem.Allocator,
    request: Request,
    shard_id: usize,
) !java_body.RiskEngineState {
    const path = try paths.snapshotPath(
        allocator,
        request.input_storage_folder,
        request.exchange_id,
        request.snapshot_id,
        .risk_engine,
        @intCast(shard_id),
    );
    defer allocator.free(path);

    const frame = try container.readBoundedFileAlloc(
        allocator,
        request.input_dir,
        path,
        request.frame_limits.max_file_size,
    );
    defer allocator.free(frame);
    var decompressed = try legacy.decodeLegacyLz4SnapshotFrame(
        allocator,
        frame,
        request.frame_limits,
    );
    defer decompressed.deinit(allocator);
    const document = try java_wire.unwrapRawDocument(decompressed.bytes, request.wire_config);

    return java_body.decodeRiskEngineBody(
        allocator,
        document.body,
        .{
            .shard_id = @intCast(shard_id),
            .shard_mask = @intCast(request.risk_count - 1),
        },
        request.body_limits,
    );
}

fn validShardCount(count: u32) bool {
    return count != 0 and @popCount(count) == 1;
}

const TestModule = enum {
    matching,
    risk,
};

const TestTarget = struct {
    module: TestModule,
    shard_id: u32,
};

const TestFamilyOptions = struct {
    omit: ?TestTarget = null,
    wrong_shard: ?TestTarget = null,
    truncate: ?TestTarget = null,
    corrupt_frame_header: ?TestTarget = null,
};

const test_exchange_id = "JAVA-CONVERT";
const test_snapshot_id: u64 = 77;
const test_matching_seq: u64 = 400;
const test_risk_seq: u64 = 401;
const test_timestamp_ns: u64 = 987_654_321;
const test_symbol: i32 = 100;
const test_uid: i64 = 40;
const test_base_currency: i32 = 11;
const test_quote_currency: i32 = 15;

fn testRequest(
    dir: std.Io.Dir,
    matching_count: u32,
    risk_count: u32,
    output_folder: []const u8,
) Request {
    return .{
        .input_dir = dir,
        .output_dir = dir,
        .input_storage_folder = "java-input",
        .output_storage_folder = output_folder,
        .exchange_id = test_exchange_id,
        .snapshot_id = test_snapshot_id,
        .matching_count = matching_count,
        .risk_count = risk_count,
        .base_config = .{ .orderbook_kind = .direct },
        .matching_command_seq = test_matching_seq,
        .risk_command_seq = test_risk_seq,
        .timestamp_ns = test_timestamp_ns,
    };
}

fn targetMatches(target: ?TestTarget, module: TestModule, shard_id: u32) bool {
    const value = target orelse return false;
    return value.module == module and value.shard_id == shard_id;
}

fn appendIntLe(
    bytes: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    comptime T: type,
    value: T,
) !void {
    var encoded: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &encoded, value, .little);
    try bytes.appendSlice(allocator, &encoded);
}

fn appendIntBe(
    bytes: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    comptime T: type,
    value: T,
) !void {
    var encoded: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &encoded, value, .big);
    try bytes.appendSlice(allocator, &encoded);
}

fn appendTestSpecification(
    bytes: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
) !void {
    try appendIntLe(bytes, allocator, i32, test_symbol);
    try bytes.append(allocator, 0); // CURRENCY_EXCHANGE_PAIR
    try appendIntLe(bytes, allocator, i32, test_base_currency);
    try appendIntLe(bytes, allocator, i32, test_quote_currency);
    try appendIntLe(bytes, allocator, i64, 100);
    try appendIntLe(bytes, allocator, i64, 10);
    try appendIntLe(bytes, allocator, i64, 3);
    try appendIntLe(bytes, allocator, i64, 2);
    try appendIntLe(bytes, allocator, i64, 0);
    try appendIntLe(bytes, allocator, i64, 0);
}

fn buildMatchingBody(
    allocator: std.mem.Allocator,
    shard_id: u32,
    shard_mask: u32,
) ![]u8 {
    var bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer bytes.deinit(allocator);

    try appendIntLe(&bytes, allocator, i32, @intCast(shard_id));
    try appendIntLe(&bytes, allocator, i64, shard_mask);
    try appendIntLe(&bytes, allocator, i32, 0); // BinaryCommandsProcessor
    try appendIntLe(&bytes, allocator, i32, 1); // orderBooks
    try appendIntLe(&bytes, allocator, i32, test_symbol); // map key
    try bytes.append(allocator, 2); // DIRECT
    try appendTestSpecification(&bytes, allocator);

    const owns_symbol = (@as(u32, @bitCast(test_symbol)) & shard_mask) == shard_id;
    try appendIntLe(&bytes, allocator, i32, if (owns_symbol) 1 else 0);
    if (owns_symbol) {
        try appendIntLe(&bytes, allocator, i64, 9_001); // order id
        try appendIntLe(&bytes, allocator, i64, 101); // price
        try appendIntLe(&bytes, allocator, i64, 10); // size
        try appendIntLe(&bytes, allocator, i64, 2); // filled
        try appendIntLe(&bytes, allocator, i64, 0); // reserve bid price
        try bytes.append(allocator, 0); // ASK
        try appendIntLe(&bytes, allocator, i64, test_uid);
        try appendIntLe(&bytes, allocator, i64, 1_234); // timestamp
    }
    return bytes.toOwnedSlice(allocator);
}

fn buildRiskBody(
    allocator: std.mem.Allocator,
    shard_id: u32,
    shard_mask: u32,
) ![]u8 {
    var bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer bytes.deinit(allocator);

    try appendIntLe(&bytes, allocator, i32, @intCast(shard_id));
    try appendIntLe(&bytes, allocator, i64, shard_mask);

    try appendIntLe(&bytes, allocator, i32, 1); // symbol specifications
    try appendIntLe(&bytes, allocator, i32, test_symbol); // map key
    try appendTestSpecification(&bytes, allocator);

    const owns_uid = (@as(u64, @bitCast(test_uid)) & shard_mask) == shard_id;
    try appendIntLe(&bytes, allocator, i32, if (owns_uid) 1 else 0); // users
    if (owns_uid) {
        try appendIntLe(&bytes, allocator, i64, test_uid); // map key
        try appendIntLe(&bytes, allocator, i64, test_uid); // embedded uid
        try appendIntLe(&bytes, allocator, i32, 0); // positions
        try appendIntLe(&bytes, allocator, i64, 5); // adjustments counter
        try appendIntLe(&bytes, allocator, i32, 1); // accounts
        try appendIntLe(&bytes, allocator, i32, test_quote_currency);
        try appendIntLe(&bytes, allocator, i64, 10_000);
        try bytes.append(allocator, 0); // ACTIVE
    }

    try appendIntLe(&bytes, allocator, i32, 0); // BinaryCommandsProcessor
    try appendIntLe(&bytes, allocator, i32, 1); // last prices
    try appendIntLe(&bytes, allocator, i32, test_symbol);
    try appendIntLe(&bytes, allocator, i64, 105);
    try appendIntLe(&bytes, allocator, i64, 99);

    try appendIntLe(&bytes, allocator, i32, if (shard_id == 0) 1 else 0); // fees
    if (shard_id == 0) {
        try appendIntLe(&bytes, allocator, i32, test_quote_currency);
        try appendIntLe(&bytes, allocator, i64, 7);
    }
    try appendIntLe(&bytes, allocator, i32, if (shard_id == 0) 1 else 0); // adjustments
    if (shard_id == 0) {
        try appendIntLe(&bytes, allocator, i32, test_quote_currency);
        try appendIntLe(&bytes, allocator, i64, -10_000);
    }
    try appendIntLe(&bytes, allocator, i32, 0); // suspends
    return bytes.toOwnedSlice(allocator);
}

fn buildJavaSnapshotFrame(
    allocator: std.mem.Allocator,
    body: []const u8,
) ![]u8 {
    const body_len = std.math.cast(u32, body.len) orelse return error.TestBodyTooLarge;
    const outer_len = std.math.add(u32, body_len, @sizeOf(u32)) catch
        return error.TestBodyTooLarge;

    var document: std.ArrayListUnmanaged(u8) = .empty;
    defer document.deinit(allocator);
    try appendIntBe(&document, allocator, u32, outer_len);
    try appendIntLe(&document, allocator, u32, body_len);
    try document.appendSlice(allocator, body);

    var frame: std.ArrayListUnmanaged(u8) = .empty;
    defer frame.deinit(allocator);
    try appendIntLe(&frame, allocator, u32, 0x184D2204);
    try frame.append(allocator, 0x60); // v1 + independent blocks
    try frame.append(allocator, 0x70); // 4 MiB blocks
    try frame.append(
        allocator,
        @truncate(std.hash.XxHash32.hash(0, frame.items[4..6]) >> 8),
    );
    const raw_block_len = std.math.cast(u32, document.items.len) orelse
        return error.TestBodyTooLarge;
    try appendIntLe(&frame, allocator, u32, raw_block_len | 0x8000_0000);
    try frame.appendSlice(allocator, document.items);
    try appendIntLe(&frame, allocator, u32, 0);
    return frame.toOwnedSlice(allocator);
}

fn writeTestFile(dir: std.Io.Dir, path: []const u8, bytes: []const u8) !void {
    const file = try dir.createFile(io_context.current(), path, .{ .truncate = true });
    defer file.close(io_context.current());
    try file.writeStreamingAll(io_context.current(), bytes);
}

fn writeTestFamily(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    matching_count: u32,
    risk_count: u32,
    options: TestFamilyOptions,
) !void {
    try dir.createDirPath(io_context.current(), "java-input");

    for (0..matching_count) |shard_index| {
        const shard_id: u32 = @intCast(shard_index);
        if (targetMatches(options.omit, .matching, shard_id)) continue;
        const encoded_id = if (targetMatches(options.wrong_shard, .matching, shard_id))
            shard_id + 1
        else
            shard_id;
        const body = try buildMatchingBody(allocator, encoded_id, matching_count - 1);
        defer allocator.free(body);
        const frame = try buildJavaSnapshotFrame(allocator, body);
        defer allocator.free(frame);
        if (targetMatches(options.corrupt_frame_header, .matching, shard_id)) frame[6] ^= 1;
        const contents = if (targetMatches(options.truncate, .matching, shard_id))
            frame[0 .. frame.len - 1]
        else
            frame;
        const path = try paths.snapshotPath(
            allocator,
            "java-input",
            test_exchange_id,
            test_snapshot_id,
            .matching_engine_router,
            shard_id,
        );
        defer allocator.free(path);
        try writeTestFile(dir, path, contents);
    }

    for (0..risk_count) |shard_index| {
        const shard_id: u32 = @intCast(shard_index);
        if (targetMatches(options.omit, .risk, shard_id)) continue;
        const encoded_id = if (targetMatches(options.wrong_shard, .risk, shard_id))
            shard_id + 1
        else
            shard_id;
        const body = try buildRiskBody(allocator, encoded_id, risk_count - 1);
        defer allocator.free(body);
        const frame = try buildJavaSnapshotFrame(allocator, body);
        defer allocator.free(frame);
        if (targetMatches(options.corrupt_frame_header, .risk, shard_id)) frame[6] ^= 1;
        const contents = if (targetMatches(options.truncate, .risk, shard_id))
            frame[0 .. frame.len - 1]
        else
            frame;
        const path = try paths.snapshotPath(
            allocator,
            "java-input",
            test_exchange_id,
            test_snapshot_id,
            .risk_engine,
            shard_id,
        );
        defer allocator.free(path);
        try writeTestFile(dir, path, contents);
    }
}

fn restoreConverted(
    allocator: std.mem.Allocator,
    request: Request,
) !kernel_mod.MultiShardKernelSet {
    return store.restoreFresh(
        allocator,
        request.output_dir,
        request.output_storage_folder,
        request.exchange_id,
        .{
            .identity = request.bundleIdentity(),
            .matching_count = request.matching_count,
            .risk_count = request.risk_count,
            .base_config = request.base_config,
            .container_limits = request.store_options.container_limits,
            .body_limits = request.store_options.body_limits,
        },
    );
}

fn expectMapEqual(
    expected: *const std.AutoHashMapUnmanaged(i32, i64),
    actual: *const std.AutoHashMapUnmanaged(i32, i64),
) !void {
    try std.testing.expectEqual(expected.count(), actual.count());
    var iterator = expected.iterator();
    while (iterator.next()) |entry| {
        try std.testing.expectEqual(entry.value_ptr.*, actual.get(entry.key_ptr.*));
    }
}

fn expectStateAndReportsEqual(
    expected: *kernel_mod.MultiShardKernelSet,
    actual: *kernel_mod.MultiShardKernelSet,
) !void {
    try std.testing.expectEqual(expected.next_seq, actual.next_seq);
    try std.testing.expectEqual(
        try expected.detailedHashFingerprint(),
        try actual.detailedHashFingerprint(),
    );
    try std.testing.expectEqual(
        try expected.queryUserBalance(test_uid, test_quote_currency),
        try actual.queryUserBalance(test_uid, test_quote_currency),
    );

    var expected_report = try expected.aggregateTotalCurrencyBalance();
    defer expected_report.deinit(expected.allocator);
    var actual_report = try actual.aggregateTotalCurrencyBalance();
    defer actual_report.deinit(actual.allocator);
    try expectMapEqual(&expected_report.account_balances, &actual_report.account_balances);
    try expectMapEqual(&expected_report.orders_balances, &actual_report.orders_balances);
    try expectMapEqual(&expected_report.fees, &actual_report.fees);
    try expectMapEqual(&expected_report.adjustments, &actual_report.adjustments);
    try expectMapEqual(&expected_report.suspends, &actual_report.suspends);
    try expectMapEqual(&expected_report.open_interest_long, &actual_report.open_interest_long);
    try expectMapEqual(&expected_report.open_interest_short, &actual_report.open_interest_short);
}

fn expectManifestMissing(allocator: std.mem.Allocator, request: Request) !void {
    const path = try paths.snapshotManifestPath(
        allocator,
        request.output_storage_folder,
        request.exchange_id,
        request.snapshot_id,
    );
    defer allocator.free(path);
    try std.testing.expectError(
        error.FileNotFound,
        request.output_dir.openFile(io_context.current(), path, .{}),
    );
}

fn runSuccessfulConversionCase(matching_count: u32, risk_count: u32) !void {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeTestFamily(allocator, tmp.dir, matching_count, risk_count, .{});
    const request = testRequest(tmp.dir, matching_count, risk_count, "native-output");

    var expected = try loadSnapshotFamilyFresh(allocator, request);
    defer expected.deinit();
    const summary = try convertSnapshotFamily(allocator, request);
    try std.testing.expectEqual(matching_count + risk_count, summary.source_file_count);
    try std.testing.expectEqual(request.snapshot_id, summary.commit.snapshot_id);
    try std.testing.expectEqual(request.risk_command_seq, summary.commit.publication_high_water);

    var restored = try restoreConverted(allocator, request);
    defer restored.deinit();
    try expectStateAndReportsEqual(&expected, &restored);
    try std.testing.expectEqual(@as(?i64, 10_000), restored.queryUserBalance(test_uid, test_quote_currency));

    var wrong_identity = request.bundleIdentity();
    wrong_identity.timestamp_ns += 1;
    try std.testing.expectError(
        error.TimestampMismatch,
        store.restoreFresh(
            allocator,
            request.output_dir,
            request.output_storage_folder,
            request.exchange_id,
            .{
                .identity = wrong_identity,
                .matching_count = request.matching_count,
                .risk_count = request.risk_count,
                .base_config = request.base_config,
            },
        ),
    );
}

test "Java snapshot-family converter roundtrips populated 1ME 1RE through XCM2" {
    try runSuccessfulConversionCase(1, 1);
}

test "Java snapshot-family converter roundtrips populated asymmetric 4ME 2RE through XCM2" {
    try runSuccessfulConversionCase(4, 2);
}

test "Java snapshot-family converter restores committed DiskSerializationProcessor fixture" {
    const fixtures = @import("java_persistence_test_fixtures");
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io_context.current(), "java-input");

    const me_name = "JAVAPERSISTV1_snapshot_4242424242_ME0.ecs";
    const re_name = "JAVAPERSISTV1_snapshot_4242424242_RE0.ecs";
    try writeTestFile(tmp.dir, "java-input/" ++ me_name, fixtures.matching_snapshot);
    try writeTestFile(tmp.dir, "java-input/" ++ re_name, fixtures.risk_snapshot);

    const request: Request = .{
        .input_dir = tmp.dir,
        .output_dir = tmp.dir,
        .input_storage_folder = "java-input",
        .output_storage_folder = "native-output",
        .exchange_id = "JAVAPERSISTV1",
        .snapshot_id = 4_242_424_242,
        .matching_count = 1,
        .risk_count = 1,
        .base_config = .{
            .orderbook_kind = .direct,
            .send_l2_for_every_cmd = true,
            .risk = .{ .margin_trading_enabled = true },
        },
        .matching_command_seq = 16,
        .risk_command_seq = 17,
        .timestamp_ns = 1_700_000_000_000_000_020,
    };
    const summary = try convertSnapshotFamily(allocator, request);
    try std.testing.expectEqual(@as(u32, 2), summary.source_file_count);
    try std.testing.expectEqual(@as(u64, 17), summary.commit.publication_high_water);

    var restored = try restoreConverted(allocator, request);
    defer restored.deinit();
    try std.testing.expectEqual(@as(u64, 17), restored.next_seq);
    try std.testing.expectEqual(@as(?i64, 239_940), try restored.queryUserBalance(7_001, 392));
    try std.testing.expectEqual(@as(?i64, 149_910), try restored.queryUserBalance(7_002, 392));

    var totals = try restored.aggregateTotalCurrencyBalance();
    defer totals.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 389_850), totals.account_balances.get(392));
    try std.testing.expectEqual(@as(?i64, -390_000), totals.adjustments.get(392));
    try std.testing.expectEqual(@as(?i64, 150), totals.fees.get(392));
    try std.testing.expectEqual(@as(?i64, 30), totals.open_interest_long.get(5_991));
    try std.testing.expectEqual(@as(?i64, 30), totals.open_interest_short.get(5_991));

    var maker_response = try restored.queryReport(.{ .single_user = 7_001 });
    defer maker_response.deinit(allocator);
    const maker = maker_response.single_user;
    try std.testing.expectEqual(@as(usize, 1), maker.orders.items.len);
    try std.testing.expectEqual(@as(i64, 101), maker.orders.items[0].order_id);
    try std.testing.expectEqual(@as(i64, 30), maker.orders.items[0].filled);

    var book_response = try restored.queryReport(.{
        .order_book = .{ .symbol = 5_991, .depth = 8 },
    });
    defer book_response.deinit(allocator);
    const book = book_response.order_book;
    try std.testing.expectEqual(@as(usize, 1), book.asks.items.len);
    try std.testing.expectEqual(@as(usize, 0), book.bids.items.len);
    try std.testing.expectEqual(@as(i64, 10_770), book.asks.items[0].price);
    try std.testing.expectEqual(@as(i64, 10), book.asks.items[0].volume);
}

test "Java snapshot-family converter rejects missing wrong truncated and corrupt modules without a manifest" {
    const cases = [_]struct {
        options: TestFamilyOptions,
        expected_error: anyerror,
    }{
        .{
            .options = .{ .omit = .{ .module = .risk, .shard_id = 1 } },
            .expected_error = error.FileNotFound,
        },
        .{
            .options = .{ .wrong_shard = .{ .module = .matching, .shard_id = 0 } },
            .expected_error = error.ShardIdMismatch,
        },
        .{
            .options = .{ .truncate = .{ .module = .risk, .shard_id = 0 } },
            .expected_error = error.TruncatedBlockHeader,
        },
        .{
            .options = .{ .corrupt_frame_header = .{ .module = .matching, .shard_id = 1 } },
            .expected_error = error.InvalidHeaderChecksum,
        },
    };

    for (cases) |case| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try writeTestFamily(std.testing.allocator, tmp.dir, 4, 2, case.options);
        const request = testRequest(tmp.dir, 4, 2, "failed-output");
        try std.testing.expectError(
            case.expected_error,
            convertSnapshotFamily(std.testing.allocator, request),
        );
        try expectManifestMissing(std.testing.allocator, request);
    }
}

test "Java snapshot-family converter rejects topology identity and sequence mismatch before commit" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeTestFamily(allocator, tmp.dir, 1, 1, .{});

    var topology = testRequest(tmp.dir, 2, 1, "topology-output");
    try std.testing.expectError(error.ShardMaskMismatch, convertSnapshotFamily(allocator, topology));
    try expectManifestMissing(allocator, topology);

    var identity = testRequest(tmp.dir, 1, 1, "identity-output");
    identity.snapshot_id += 1;
    try std.testing.expectError(error.FileNotFound, convertSnapshotFamily(allocator, identity));
    try expectManifestMissing(allocator, identity);

    var sequence = testRequest(tmp.dir, 1, 1, "sequence-output");
    sequence.risk_command_seq += 1;
    try std.testing.expectError(error.InvalidSequence, convertSnapshotFamily(allocator, sequence));
    try expectManifestMissing(allocator, sequence);

    topology.matching_count = 3;
    try std.testing.expectError(error.InvalidMatchingShardCount, topology.validate());
}

test "Java snapshot-family converter never advertises an injected late commit failure" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeTestFamily(allocator, tmp.dir, 4, 2, .{});
    var request = testRequest(tmp.dir, 4, 2, "late-failure-output");
    request.store_options.fault_point = .before_manifest_publish;
    try std.testing.expectError(
        error.InjectedPersistenceFault,
        convertSnapshotFamily(allocator, request),
    );
    try expectManifestMissing(allocator, request);
}

fn conversionAllocationCampaign(allocator: std.mem.Allocator) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeTestFamily(std.testing.allocator, tmp.dir, 1, 1, .{});
    const request = testRequest(tmp.dir, 1, 1, "oom-output");
    errdefer expectManifestMissing(std.testing.allocator, request) catch
        @panic("failed to verify absent XCM2 manifest");
    _ = try convertSnapshotFamily(allocator, request);
}

test "Java snapshot-family converter releases staged state across every allocation failure" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        conversionAllocationCampaign,
        .{},
    );
}
