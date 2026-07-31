const std = @import("std");
const io_context = @import("io_context.zig");
const binary_frames = @import("binary_frames.zig");
const cmd = @import("cmd.zig");
const common = @import("common.zig");
const kernel_mod = @import("exchange_kernel.zig");
const paths = @import("persistence_paths.zig");
const report_registry = @import("report_registry.zig");
const snapshot = @import("snapshot.zig");
const container = @import("sharded_snapshot.zig");
const state = @import("sharded_snapshot_state.zig");
const user = @import("user_profile.zig");

const Sha256 = std.crypto.hash.sha2.Sha256;

const bundle_domain = "exchange-core/XCM2/bundle/v1";
const module_domain = "exchange-core/XCM2/native-module/v1";
const aggregate_domain = "exchange-core/XCM2/native-aggregate/v1";
const kernel_config_v1_domain = "exchange-core/XCM2/kernel-config/v1";
const kernel_config_v2_domain = "exchange-core/XCM2/kernel-config/v2";

pub const StoreError = error{
    InvalidSequence,
    PublicationFrontierMismatch,
};

pub const RestoreError = error{
    InvalidSequence,
    TimestampMismatch,
    BundleMismatch,
    ModuleStateHashMismatch,
    AggregateStateHashMismatch,
    RestoredFrontierMismatch,
};

pub const LegacyManifestError = error{
    LegacyXcm1Manifest,
};

/// The snapshot identity supplied by the persistence barrier. Matching is
/// captured by command S and risk by command S+1. Both module families must
/// already observe the globally restorable frontier S+1 before capture starts.
pub const BundleIdentity = struct {
    snapshot_id: u64,
    matching_command_seq: u64,
    risk_command_seq: u64,
    timestamp_ns: u64,
    config_sha256: [container.sha256_len]u8,
};

pub const FaultPoint = enum {
    before_manifest_publish,
};

pub const StoreOptions = struct {
    container_limits: container.Limits = .{},
    body_limits: state.Limits = .{},
    /// Deterministic integration-test seam. Production callers leave this
    /// null; the injected failure occurs after every immutable payload is
    /// linked but before the manifest advertises the bundle.
    fault_point: ?FaultPoint = null,
};

pub const RestoreRequest = struct {
    identity: BundleIdentity,
    matching_count: u32,
    risk_count: u32,
    base_config: kernel_mod.Config,
    container_limits: container.Limits = .{},
    body_limits: state.Limits = .{},
};

pub const CommitSummary = struct {
    snapshot_id: u64,
    matching_count: u32,
    risk_count: u32,
    publication_high_water: u64,
    aggregate_state_hash: u64,
    bundle_id: [16]u8,
    manifest_file_sha256: [container.sha256_len]u8,
};

pub const CommittedSnapshotInfo = struct {
    identity: BundleIdentity,
    matching_count: u32,
    risk_count: u32,
    publication_high_water: u64,
    aggregate_state_hash: u64,
    bundle_id: [16]u8,
};

pub const CommittedSnapshotInventory = struct {
    latest_xcm1: ?u64 = null,
    latest_xcm2: ?u64 = null,
};

/// Hashes the exact exchange identifier bytes used in the Java-compatible
/// filenames. No normalization is performed.
pub fn exchangeIdSha256(exchange_id: []const u8) [container.sha256_len]u8 {
    return container.sha256(exchange_id);
}

/// Canonical identity for the kernel semantics that must remain compatible
/// across an XCM2 restore. Per-instance shard ids are intentionally excluded:
/// the manifest binds the independent matching/risk topology and restoration
/// derives each instance id from that topology.
pub fn kernelConfigSha256(
    config: kernel_mod.Config,
    matching_count: u32,
    risk_count: u32,
) [container.sha256_len]u8 {
    const custom_registry = config.report_queries.custom_registry;
    const has_custom_reports = custom_registry.len() != 0;

    var hasher = Sha256.init(.{});
    hasher.update(
        if (has_custom_reports)
            kernel_config_v2_domain
        else
            kernel_config_v1_domain,
    );
    hashInt(u32, &hasher, matching_count);
    hashInt(u32, &hasher, risk_count);
    hasher.update(&.{@intCast(@intFromEnum(config.orderbook_kind))});
    hashBool(&hasher, config.send_l2_for_every_cmd);
    hashInt(u64, &hasher, @intCast(config.l2_refresh_depth));
    hashBool(&hasher, config.risk.ignore_risk_processing);
    hashBool(&hasher, config.risk.margin_trading_enabled);
    hashBool(&hasher, config.report_queries.state_hash);
    hashBool(&hasher, config.report_queries.single_user);
    hashBool(&hasher, config.report_queries.total_currency_balance);
    hashBool(&hasher, config.report_queries.add_accounts_binary);
    hashBool(&hasher, config.report_queries.add_symbols_binary);
    if (has_custom_reports) {
        const registry_digest = custom_registry.digest();
        hasher.update(&registry_digest);
    }
    var digest: [container.sha256_len]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

/// XCM2 bundle IDs are deterministic and architecture-independent. They are
/// intentionally distinct from native state fingerprints and legacy Java
/// `hashCode()` values.
pub fn deriveBundleId(
    identity: BundleIdentity,
    exchange_id_sha256: [container.sha256_len]u8,
) [16]u8 {
    var hasher = Sha256.init(.{});
    hasher.update(bundle_domain);
    hashInt(u64, &hasher, identity.snapshot_id);
    hashInt(u64, &hasher, identity.matching_command_seq);
    hashInt(u64, &hasher, identity.risk_command_seq);
    hashInt(u64, &hasher, identity.timestamp_ns);
    hasher.update(&identity.config_sha256);
    hasher.update(&exchange_id_sha256);
    var digest: [container.sha256_len]u8 = undefined;
    hasher.final(&digest);
    return digest[0..16].*;
}

/// Combines the matching semantic fingerprint with its pending binary-frame
/// fingerprint. This is an XCM2-native integrity value, not a Java hash.
pub fn matchingModuleStateHash(
    shard_id: u32,
    books_state_hash: u64,
    binary_state_hash: u64,
) u64 {
    return moduleStateHash(.matching_engine, shard_id, books_state_hash, binary_state_hash);
}

/// Combines the risk semantic fingerprint with its pending binary-frame
/// fingerprint. This is an XCM2-native integrity value, not a Java hash.
pub fn riskModuleStateHash(
    shard_id: u32,
    risk_state_hash: u64,
    binary_state_hash: u64,
) u64 {
    return moduleStateHash(.risk_engine, shard_id, risk_state_hash, binary_state_hash);
}

/// Canonical aggregate order is ME0..ME(N-1), then RE0..RE(N-1). Module kind
/// and shard id are hashed explicitly so equal module hashes cannot be swapped.
pub fn aggregateStateHash(
    matching_hashes: []const u64,
    risk_hashes: []const u64,
) u64 {
    var hasher = Sha256.init(.{});
    hasher.update(aggregate_domain);
    hashInt(u64, &hasher, matching_hashes.len);
    hashInt(u64, &hasher, risk_hashes.len);
    for (matching_hashes, 0..) |module_hash, shard_id| {
        hasher.update(&.{@intFromEnum(container.ModuleKind.matching_engine)});
        hashInt(u64, &hasher, shard_id);
        hashInt(u64, &hasher, module_hash);
    }
    for (risk_hashes, 0..) |module_hash, shard_id| {
        hasher.update(&.{@intFromEnum(container.ModuleKind.risk_engine)});
        hashInt(u64, &hasher, shard_id);
        hashInt(u64, &hasher, module_hash);
    }
    var digest: [container.sha256_len]u8 = undefined;
    hasher.final(&digest);
    return std.mem.readInt(u64, digest[0..8], .little);
}

/// Captures all module bodies before publishing any file. Immutable payloads
/// are linked in canonical order and the manifest is linked last as the sole
/// commit advertisement. Existing paths are rejected by the underlying atomic
/// no-replace publisher; no committed or mismatching file is overwritten.
pub fn captureAndCommit(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    storage_folder: []const u8,
    exchange_id: []const u8,
    source: *const kernel_mod.MultiShardKernelSet,
    identity: BundleIdentity,
    options: StoreOptions,
) !CommitSummary {
    try validateIdentity(identity);
    try validateCaptureFrontier(source, identity.risk_command_seq);

    // Validate both path components before creating anything on disk.
    const manifest_path = try paths.snapshotManifestPath(
        allocator,
        storage_folder,
        exchange_id,
        identity.snapshot_id,
    );
    defer allocator.free(manifest_path);
    try paths.ensureDirPathDurable(io_context.current(), dir, storage_folder);

    const matching_count = std.math.cast(u32, source.matchingShardCount()) orelse
        return error.LimitExceeded;
    const risk_count = std.math.cast(u32, source.riskShardCount()) orelse
        return error.LimitExceeded;

    const matching_states = try allocator.alloc(state.MatchingState, matching_count);
    defer allocator.free(matching_states);
    var matching_initialized: usize = 0;
    defer for (matching_states[0..matching_initialized]) |*captured| captured.deinit();
    for (matching_states, 0..) |*captured, shard_id| {
        captured.* = try state.MatchingState.capture(
            allocator,
            &source.matching_shards[shard_id],
            options.body_limits,
        );
        matching_initialized += 1;
    }

    const risk_states = try allocator.alloc(state.RiskState, risk_count);
    defer allocator.free(risk_states);
    var risk_initialized: usize = 0;
    defer for (risk_states[0..risk_initialized]) |*captured| captured.deinit();
    for (risk_states, 0..) |*captured, shard_id| {
        const shard = &source.risk_shards[shard_id];
        captured.* = try state.RiskState.capture(
            allocator,
            shard.engine,
            &shard.binary_frame_assembler,
            options.body_limits,
        );
        risk_initialized += 1;
    }
    // A persistence integration must hold a live barrier. Rechecking before
    // the first disk publication turns an accidentally released barrier into
    // a clean orphan-free failure instead of committing a stale frontier.
    try validateCaptureFrontier(source, identity.risk_command_seq);

    const matching_hashes = try allocator.alloc(u64, matching_count);
    defer allocator.free(matching_hashes);
    for (matching_states, matching_hashes, 0..) |captured, *module_hash, shard_id| {
        module_hash.* = matchingModuleStateHash(
            @intCast(shard_id),
            captured.books_state_hash,
            captured.binary_state_hash,
        );
    }
    const risk_hashes = try allocator.alloc(u64, risk_count);
    defer allocator.free(risk_hashes);
    for (risk_states, risk_hashes, 0..) |captured, *module_hash, shard_id| {
        module_hash.* = riskModuleStateHash(
            @intCast(shard_id),
            captured.risk_state_hash,
            captured.binary_state_hash,
        );
    }

    const exchange_digest = exchangeIdSha256(exchange_id);
    const bundle_id = deriveBundleId(identity, exchange_digest);
    const aggregate_hash = aggregateStateHash(matching_hashes, risk_hashes);
    const entry_count = std.math.add(u32, matching_count, risk_count) catch
        return error.LimitExceeded;
    const entries = try allocator.alloc(container.ManifestEntry, entry_count);
    defer allocator.free(entries);

    for (matching_states, 0..) |*captured, shard_id| {
        const body = try captured.encodeAlloc(allocator, options.body_limits);
        defer allocator.free(body);
        entries[shard_id] = try publishPayload(
            allocator,
            dir,
            storage_folder,
            exchange_id,
            identity,
            .matching_engine,
            @intCast(shard_id),
            matching_count,
            source.matchingShardMask(),
            matching_hashes[shard_id],
            body,
            options.container_limits,
        );
    }
    for (risk_states, 0..) |*captured, shard_id| {
        const body = try captured.encodeAlloc(allocator, options.body_limits);
        defer allocator.free(body);
        entries[@as(usize, matching_count) + shard_id] = try publishPayload(
            allocator,
            dir,
            storage_folder,
            exchange_id,
            identity,
            .risk_engine,
            @intCast(shard_id),
            risk_count,
            source.riskShardMask(),
            risk_hashes[shard_id],
            body,
            options.container_limits,
        );
    }

    if (options.fault_point == .before_manifest_publish) {
        return error.InjectedPersistenceFault;
    }

    const header: container.ManifestHeader = .{
        .snapshot_id = identity.snapshot_id,
        .matching_command_seq = identity.matching_command_seq,
        .risk_command_seq = identity.risk_command_seq,
        .publication_high_water = identity.risk_command_seq,
        .timestamp_ns = identity.timestamp_ns,
        .matching_count = matching_count,
        .risk_count = risk_count,
        .matching_mask = source.matchingShardMask(),
        .risk_mask = source.riskShardMask(),
        .aggregate_state_hash = aggregate_hash,
        .config_sha256 = identity.config_sha256,
        .exchange_id_sha256 = exchange_digest,
        .bundle_id = bundle_id,
        .entry_count = entry_count,
    };
    const manifest_bytes = try container.encodeManifestAlloc(
        allocator,
        header,
        entries,
        options.container_limits,
    );
    defer allocator.free(manifest_bytes);
    try container.publishBytesAtomicNoReplace(dir, manifest_path, manifest_bytes);

    return .{
        .snapshot_id = identity.snapshot_id,
        .matching_count = matching_count,
        .risk_count = risk_count,
        .publication_high_water = identity.risk_command_seq,
        .aggregate_state_hash = aggregate_hash,
        .bundle_id = bundle_id,
        .manifest_file_sha256 = container.sha256(manifest_bytes),
    };
}

/// Validates the committed manifest identity needed to stage a fresh live
/// topology. Payloads are deliberately not read here; `restoreFresh` performs
/// their complete digest/body/native-state validation before returning.
pub fn readCommittedSnapshotInfo(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    storage_folder: []const u8,
    exchange_id: []const u8,
    snapshot_id: u64,
    matching_count: u32,
    risk_count: u32,
    base_config: kernel_mod.Config,
    limits: container.Limits,
) !CommittedSnapshotInfo {
    const matching_mask = try maskForCount(matching_count);
    const risk_mask = try maskForCount(risk_count);
    const config_sha256 = kernelConfigSha256(base_config, matching_count, risk_count);
    const exchange_digest = exchangeIdSha256(exchange_id);
    const manifest_path = try paths.snapshotManifestPath(
        allocator,
        storage_folder,
        exchange_id,
        snapshot_id,
    );
    defer allocator.free(manifest_path);

    switch (try classifyManifestFile(dir, manifest_path)) {
        .xcm1 => {
            const legacy_manifest = try snapshot.readManifest(dir, manifest_path);
            if (legacy_manifest.snapshot_id != snapshot_id) {
                return error.SnapshotIdMismatch;
            }
            return error.LegacyXcm1Manifest;
        },
        .xcm2 => {},
        .unrelated => return error.InvalidMagic,
    }

    var manifest = container.readManifestFileAlloc(
        allocator,
        dir,
        manifest_path,
        limits,
        .{
            .snapshot_id = snapshot_id,
            .matching_count = matching_count,
            .risk_count = risk_count,
            .matching_mask = matching_mask,
            .risk_mask = risk_mask,
            .config_sha256 = config_sha256,
            .exchange_id_sha256 = exchange_digest,
        },
    ) catch |err| return err;
    defer manifest.deinit();

    const identity: BundleIdentity = .{
        .snapshot_id = manifest.header.snapshot_id,
        .matching_command_seq = manifest.header.matching_command_seq,
        .risk_command_seq = manifest.header.risk_command_seq,
        .timestamp_ns = manifest.header.timestamp_ns,
        .config_sha256 = manifest.header.config_sha256,
    };
    try validateIdentity(identity);
    const expected_bundle = deriveBundleId(identity, exchange_digest);
    if (!std.mem.eql(u8, &manifest.header.bundle_id, &expected_bundle)) {
        return error.BundleMismatch;
    }

    const matching_hashes = try allocator.alloc(u64, matching_count);
    defer allocator.free(matching_hashes);
    const risk_hashes = try allocator.alloc(u64, risk_count);
    defer allocator.free(risk_hashes);
    for (matching_hashes, 0..) |*module_hash, shard_id| {
        module_hash.* = manifest.entries[shard_id].module_state_hash;
    }
    for (risk_hashes, 0..) |*module_hash, shard_id| {
        module_hash.* = manifest.entries[@as(usize, matching_count) + shard_id].module_state_hash;
    }
    if (aggregateStateHash(matching_hashes, risk_hashes) != manifest.header.aggregate_state_hash) {
        return error.AggregateStateHashMismatch;
    }

    return .{
        .identity = identity,
        .matching_count = matching_count,
        .risk_count = risk_count,
        .publication_high_water = manifest.header.publication_high_water,
        .aggregate_state_hash = manifest.header.aggregate_state_hash,
        .bundle_id = manifest.header.bundle_id,
    };
}

/// Inventories valid committed manifests across the shared `.ecm` namespace.
/// Immutable `.ecs` payload orphans and files with unrelated magic are not
/// candidates. A recognized malformed manifest remains a discovery error.
pub fn discoverCommittedSnapshotInventory(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    storage_folder: []const u8,
    exchange_id: []const u8,
    limits: container.Limits,
) !CommittedSnapshotInventory {
    var storage_dir = dir.openDir(
        io_context.current(),
        storage_folder,
        .{ .iterate = true },
    ) catch |err| switch (err) {
        error.FileNotFound => return .{},
        else => return err,
    };
    defer storage_dir.close(io_context.current());

    var iterator = storage_dir.iterate();
    var inventory: CommittedSnapshotInventory = .{};
    while (try iterator.next(io_context.current())) |entry| {
        const snapshot_id = parseManifestSnapshotId(entry.name, exchange_id) orelse continue;
        {
            const manifest_path = try paths.snapshotManifestPath(
                allocator,
                storage_folder,
                exchange_id,
                snapshot_id,
            );
            defer allocator.free(manifest_path);
            switch (try classifyManifestFile(dir, manifest_path)) {
                .xcm1 => {
                    const legacy_manifest = try snapshot.readManifest(
                        dir,
                        manifest_path,
                    );
                    if (legacy_manifest.snapshot_id != snapshot_id) {
                        return error.SnapshotIdMismatch;
                    }
                    updateLatestSnapshotId(&inventory.latest_xcm1, snapshot_id);
                    continue;
                },
                .xcm2 => {},
                .unrelated => continue,
            }
            var manifest = container.readManifestFileAlloc(
                allocator,
                dir,
                manifest_path,
                limits,
                .{
                    .snapshot_id = snapshot_id,
                    .exchange_id_sha256 = exchangeIdSha256(exchange_id),
                },
            ) catch |err| switch (err) {
                error.InvalidMagic, error.UnsupportedVersion => continue,
                else => return err,
            };
            defer manifest.deinit();
            updateLatestSnapshotId(&inventory.latest_xcm2, snapshot_id);
        }
    }
    return inventory;
}

/// Compatibility API for callers that intentionally restore only XCM2.
pub fn discoverLatestCommittedSnapshotId(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    storage_folder: []const u8,
    exchange_id: []const u8,
    limits: container.Limits,
) !?u64 {
    return (try discoverCommittedSnapshotInventory(
        allocator,
        dir,
        storage_folder,
        exchange_id,
        limits,
    )).latest_xcm2;
}

/// Reads one committed manifest, stages every module into a newly allocated
/// topology, and returns that owned topology by value. No caller/live set is
/// accepted or mutated. Publication sequence is restored only after every
/// payload, body, native module hash, and aggregate hash has validated.
pub fn restoreFresh(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    storage_folder: []const u8,
    exchange_id: []const u8,
    request: RestoreRequest,
) !kernel_mod.MultiShardKernelSet {
    try validateIdentity(request.identity);
    const matching_mask = try maskForCount(request.matching_count);
    const risk_mask = try maskForCount(request.risk_count);
    const exchange_digest = exchangeIdSha256(exchange_id);

    const manifest_path = try paths.snapshotManifestPath(
        allocator,
        storage_folder,
        exchange_id,
        request.identity.snapshot_id,
    );
    defer allocator.free(manifest_path);
    var manifest = try container.readManifestFileAlloc(
        allocator,
        dir,
        manifest_path,
        request.container_limits,
        .{
            .snapshot_id = request.identity.snapshot_id,
            .matching_command_seq = request.identity.matching_command_seq,
            .risk_command_seq = request.identity.risk_command_seq,
            .publication_high_water = request.identity.risk_command_seq,
            .matching_count = request.matching_count,
            .risk_count = request.risk_count,
            .matching_mask = matching_mask,
            .risk_mask = risk_mask,
            .config_sha256 = request.identity.config_sha256,
            .exchange_id_sha256 = exchange_digest,
        },
    );
    defer manifest.deinit();
    if (manifest.header.timestamp_ns != request.identity.timestamp_ns) {
        return error.TimestampMismatch;
    }
    const expected_bundle = deriveBundleId(request.identity, exchange_digest);
    if (!std.mem.eql(u8, &manifest.header.bundle_id, &expected_bundle)) {
        return error.BundleMismatch;
    }

    const expected_matching_hashes = try allocator.alloc(u64, request.matching_count);
    defer allocator.free(expected_matching_hashes);
    const expected_risk_hashes = try allocator.alloc(u64, request.risk_count);
    defer allocator.free(expected_risk_hashes);
    for (expected_matching_hashes, 0..) |*module_hash, shard_id| {
        module_hash.* = manifest.entries[shard_id].module_state_hash;
    }
    for (expected_risk_hashes, 0..) |*module_hash, shard_id| {
        module_hash.* = manifest.entries[@as(usize, request.matching_count) + shard_id].module_state_hash;
    }
    if (aggregateStateHash(expected_matching_hashes, expected_risk_hashes) !=
        manifest.header.aggregate_state_hash)
    {
        return error.AggregateStateHashMismatch;
    }

    var staged = try kernel_mod.MultiShardKernelSet.initWithTopology(
        allocator,
        request.matching_count,
        request.risk_count,
        request.base_config,
    );
    errdefer staged.deinit();

    const actual_matching_hashes = try allocator.alloc(u64, request.matching_count);
    defer allocator.free(actual_matching_hashes);
    const actual_risk_hashes = try allocator.alloc(u64, request.risk_count);
    defer allocator.free(actual_risk_hashes);

    for (0..request.matching_count) |shard_id| {
        const entry_value = manifest.entries[shard_id];
        var payload = try readPayload(
            allocator,
            dir,
            storage_folder,
            exchange_id,
            &manifest,
            entry_value,
            request.container_limits,
        );
        defer payload.deinit();
        var decoded = try state.MatchingState.decodeAlloc(
            allocator,
            payload.body,
            request.body_limits,
        );
        defer decoded.deinit();
        const decoded_hash = matchingModuleStateHash(
            @intCast(shard_id),
            decoded.books_state_hash,
            decoded.binary_state_hash,
        );
        if (decoded_hash != entry_value.module_state_hash) return error.ModuleStateHashMismatch;
        const target = &staged.matching_shards[shard_id];
        try decoded.restoreIntoFreshKernel(target);
        const actual_hash = matchingModuleStateHash(
            @intCast(shard_id),
            try state.matchingBooksStateHash(target),
            try target.binary_frame_assembler.stateHash(),
        );
        if (actual_hash != entry_value.module_state_hash) return error.ModuleStateHashMismatch;
        actual_matching_hashes[shard_id] = actual_hash;
    }

    for (0..request.risk_count) |shard_id| {
        const entry_value = manifest.entries[@as(usize, request.matching_count) + shard_id];
        var payload = try readPayload(
            allocator,
            dir,
            storage_folder,
            exchange_id,
            &manifest,
            entry_value,
            request.container_limits,
        );
        defer payload.deinit();
        var decoded = try state.RiskState.decodeAlloc(
            allocator,
            payload.body,
            request.body_limits,
        );
        defer decoded.deinit();
        const decoded_hash = riskModuleStateHash(
            @intCast(shard_id),
            decoded.risk_state_hash,
            decoded.binary_state_hash,
        );
        if (decoded_hash != entry_value.module_state_hash) return error.ModuleStateHashMismatch;
        const target = &staged.risk_shards[shard_id];
        try decoded.restoreIntoFreshRisk(target.engine, &target.binary_frame_assembler);
        const actual_hash = riskModuleStateHash(
            @intCast(shard_id),
            try target.engine.stateHash(),
            try target.binary_frame_assembler.stateHash(),
        );
        if (actual_hash != entry_value.module_state_hash) return error.ModuleStateHashMismatch;
        actual_risk_hashes[shard_id] = actual_hash;
    }

    if (aggregateStateHash(actual_matching_hashes, actual_risk_hashes) !=
        manifest.header.aggregate_state_hash)
    {
        return error.AggregateStateHashMismatch;
    }
    staged.restorePublicationSequence(manifest.header.publication_high_water);
    try validateRestoredFrontier(&staged, manifest.header.publication_high_water);
    return staged;
}

fn validateIdentity(identity: BundleIdentity) StoreError!void {
    const expected_risk = std.math.add(u64, identity.matching_command_seq, 1) catch
        return error.InvalidSequence;
    if (identity.risk_command_seq != expected_risk) return error.InvalidSequence;
}

fn validateCaptureFrontier(
    source: *const kernel_mod.MultiShardKernelSet,
    expected: u64,
) StoreError!void {
    if (source.next_seq != expected) return error.PublicationFrontierMismatch;
    for (0..source.matchingShardCount()) |shard_id| {
        if (source.matchingObservedSequence(shard_id) != expected) {
            return error.PublicationFrontierMismatch;
        }
    }
    for (0..source.riskShardCount()) |shard_id| {
        if (source.riskObservedSequence(shard_id) != expected) {
            return error.PublicationFrontierMismatch;
        }
    }
}

fn validateRestoredFrontier(
    restored: *const kernel_mod.MultiShardKernelSet,
    expected: u64,
) RestoreError!void {
    if (restored.next_seq != expected) return error.RestoredFrontierMismatch;
    for (0..restored.matchingShardCount()) |shard_id| {
        if (restored.matchingObservedSequence(shard_id) != expected) {
            return error.RestoredFrontierMismatch;
        }
    }
    for (0..restored.riskShardCount()) |shard_id| {
        if (restored.riskObservedSequence(shard_id) != expected) {
            return error.RestoredFrontierMismatch;
        }
    }
}

fn publishPayload(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    storage_folder: []const u8,
    exchange_id: []const u8,
    identity: BundleIdentity,
    kind: container.ModuleKind,
    shard_id: u32,
    shard_count: u32,
    shard_mask: u64,
    module_state_hash: u64,
    body: []const u8,
    limits: container.Limits,
) !container.ManifestEntry {
    const header: container.PayloadHeader = .{
        .snapshot_id = identity.snapshot_id,
        .command_seq = switch (kind) {
            .matching_engine => identity.matching_command_seq,
            .risk_engine => identity.risk_command_seq,
        },
        .observed_high_water = identity.risk_command_seq,
        .timestamp_ns = identity.timestamp_ns,
        .shard_id = shard_id,
        .shard_count = shard_count,
        .shard_mask = shard_mask,
        .config_sha256 = identity.config_sha256,
        .module_state_hash = module_state_hash,
    };
    const bytes = try container.encodePayloadAlloc(allocator, kind, header, body, limits);
    defer allocator.free(bytes);
    const path = try paths.snapshotPath(
        allocator,
        storage_folder,
        exchange_id,
        identity.snapshot_id,
        switch (kind) {
            .matching_engine => .matching_engine_router,
            .risk_engine => .risk_engine,
        },
        shard_id,
    );
    defer allocator.free(path);
    try container.publishBytesAtomicNoReplace(dir, path, bytes);
    return .{
        .module_kind = kind,
        .shard_id = shard_id,
        .command_seq = header.command_seq,
        .byte_len = @intCast(bytes.len),
        .payload_sha256 = container.sha256(bytes),
        .module_state_hash = module_state_hash,
    };
}

fn readPayload(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    storage_folder: []const u8,
    exchange_id: []const u8,
    manifest: *const container.Manifest,
    entry_value: container.ManifestEntry,
    limits: container.Limits,
) !container.PayloadFile {
    const path = try paths.snapshotPath(
        allocator,
        storage_folder,
        exchange_id,
        manifest.header.snapshot_id,
        switch (entry_value.module_kind) {
            .matching_engine => .matching_engine_router,
            .risk_engine => .risk_engine,
        },
        entry_value.shard_id,
    );
    defer allocator.free(path);
    return container.readPayloadFileAlloc(
        allocator,
        dir,
        path,
        limits,
        container.PayloadExpectations.fromManifest(manifest, entry_value),
    );
}

fn maskForCount(count: u32) error{TopologyMismatch}!u64 {
    if (count == 0 or @popCount(count) != 1) return error.TopologyMismatch;
    return @as(u64, count) - 1;
}

fn parseManifestSnapshotId(file_name: []const u8, exchange_id: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, file_name, exchange_id)) return null;
    const marker = "_snapshot_";
    const suffix = ".ecm";
    const remainder = file_name[exchange_id.len..];
    if (!std.mem.startsWith(u8, remainder, marker) or
        !std.mem.endsWith(u8, remainder, suffix))
    {
        return null;
    }
    const digits = remainder[marker.len .. remainder.len - suffix.len];
    if (digits.len == 0) return null;
    return std.fmt.parseInt(u64, digits, 10) catch null;
}

const ManifestKind = enum {
    xcm1,
    xcm2,
    unrelated,
};

fn classifyManifestFile(
    dir: std.Io.Dir,
    path: []const u8,
) !ManifestKind {
    const file = try dir.openFile(io_context.current(), path, .{});
    defer file.close(io_context.current());
    const stat = try file.stat(io_context.current());
    if (stat.size < 4) return .unrelated;

    var buffer: [4]u8 = undefined;
    var file_reader = file.reader(io_context.current(), &buffer);
    const magic = try file_reader.interface.takeArray(4);
    if (std.mem.eql(u8, magic, "XCM1")) return .xcm1;
    if (std.mem.eql(u8, magic, container.manifest_magic)) return .xcm2;
    return .unrelated;
}

fn updateLatestSnapshotId(latest: *?u64, candidate: u64) void {
    if (latest.* == null or candidate > latest.*.?) {
        latest.* = candidate;
    }
}

fn moduleStateHash(
    kind: container.ModuleKind,
    shard_id: u32,
    semantic_state_hash: u64,
    binary_state_hash: u64,
) u64 {
    var hasher = Sha256.init(.{});
    hasher.update(module_domain);
    hasher.update(&.{@intFromEnum(kind)});
    hashInt(u32, &hasher, shard_id);
    hashInt(u64, &hasher, semantic_state_hash);
    hashInt(u64, &hasher, binary_state_hash);
    var digest: [container.sha256_len]u8 = undefined;
    hasher.final(&digest);
    return std.mem.readInt(u64, digest[0..8], .little);
}

fn hashInt(comptime T: type, hasher: *Sha256, value: T) void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    hasher.update(&bytes);
}

fn hashBool(hasher: *Sha256, value: bool) void {
    hasher.update(&.{@intFromBool(value)});
}

fn testDigest(seed: u8) [container.sha256_len]u8 {
    var digest: [container.sha256_len]u8 = undefined;
    for (&digest, 0..) |*byte, index| byte.* = seed +% @as(u8, @truncate(index));
    return digest;
}

const StoreTestReportValue = struct {
    marker: u8 = 0,
};

fn StoreTestReportSpec(
    comptime report_code: i32,
    comptime schema_byte: u8,
    comptime report_schema_version: u32,
) type {
    return struct {
        pub const code: i32 = report_code;
        pub const schema_id: [report_registry.schema_id_len]u8 =
            [_]u8{schema_byte} ** report_registry.schema_id_len;
        pub const schema_version: u32 = report_schema_version;
        pub const Query = StoreTestReportValue;
        pub const Section = StoreTestReportValue;
        pub const Result = StoreTestReportValue;
        pub const MatchingView = kernel_mod.MatchingReportView;
        pub const RiskView = kernel_mod.RiskReportView;

        pub fn cloneQuery(
            _: std.mem.Allocator,
            query: *const Query,
        ) report_registry.ResourceError!Query {
            return query.*;
        }

        pub fn deinitQuery(_: std.mem.Allocator, _: *Query) void {}

        pub fn encodeQuery(
            _: std.mem.Allocator,
            _: *report_registry.EncodingWriter,
            _: *const Query,
        ) report_registry.EncodingError!void {}

        pub fn decodeQuery(
            _: std.mem.Allocator,
            _: []const u8,
        ) report_registry.QueryConstructionError!Query {
            return .{};
        }

        pub fn processMatching(
            _: std.mem.Allocator,
            _: *const Query,
            _: report_registry.MatchingContext(MatchingView),
        ) report_registry.HandlerError!?Section {
            return null;
        }

        pub fn processRisk(
            _: std.mem.Allocator,
            _: *const Query,
            _: report_registry.RiskContext(RiskView),
        ) report_registry.HandlerError!?Section {
            return null;
        }

        pub fn encodeSection(
            _: std.mem.Allocator,
            _: *report_registry.EncodingWriter,
            _: *const Section,
        ) report_registry.EncodingError!void {}

        pub fn deinitSection(_: std.mem.Allocator, _: *Section) void {}

        pub fn reduce(
            _: std.mem.Allocator,
            _: *const Query,
            _: []const report_registry.SectionBytes,
        ) report_registry.ReductionError!Result {
            return .{};
        }

        pub fn deinitResult(_: std.mem.Allocator, _: *Result) void {}
    };
}

const StoreCustomReportA = StoreTestReportSpec(43_001, 0x41, 1);
const StoreCustomReportB = StoreTestReportSpec(43_002, 0x42, 1);
const StoreCustomReportASchemaChanged = StoreTestReportSpec(43_001, 0x43, 1);
const StoreCustomReportAVersionChanged = StoreTestReportSpec(43_001, 0x41, 2);

fn testIdentity(snapshot_id: u64) BundleIdentity {
    const matching_seq = 700 + snapshot_id;
    return .{
        .snapshot_id = snapshot_id,
        .matching_command_seq = matching_seq,
        .risk_command_seq = matching_seq + 1,
        .timestamp_ns = 9_000_000_000 + snapshot_id,
        .config_sha256 = testDigest(@truncate(snapshot_id)),
    };
}

fn testSpec(symbol: i32) !common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    });
}

fn addPendingTransfer(
    assembler: *binary_frames.BinaryFrameAssembler,
    cookie: i32,
    seed: i64,
) !void {
    const command: cmd.OrderCommand = .{
        .command = .binary_data_command,
        .order_id = seed,
        .price = seed + 1,
        .reserve_bid_price = seed + 2,
        .size = seed + 3,
        .uid = seed + 4,
        .symbol = 0,
        .user_cookie = cookie,
    };
    try std.testing.expectEqual(
        binary_frames.AcceptResult.accepted,
        try assembler.acceptFrame(&command),
    );
}

fn makePopulatedSet(
    allocator: std.mem.Allocator,
    matching_count: usize,
    risk_count: usize,
    identity: BundleIdentity,
    base_config: kernel_mod.Config,
) !kernel_mod.MultiShardKernelSet {
    var result = try kernel_mod.MultiShardKernelSet.initWithTopology(
        allocator,
        matching_count,
        risk_count,
        base_config,
    );
    errdefer result.deinit();

    for (0..matching_count) |shard_id| {
        const symbol: i32 = 8_000 + @as(i32, @intCast(shard_id));
        try result.addSymbolAll(try testSpec(symbol));
    }
    for (0..matching_count) |shard_id| {
        const symbol: i32 = 8_000 + @as(i32, @intCast(shard_id));
        const order = [_]kernel_mod.RestingOrder{.{
            .symbol = symbol,
            .order_id = 10_000 + @as(i64, @intCast(shard_id)),
            .price = 100 + @as(i64, @intCast(shard_id)),
            .size = 10,
            .filled = @intCast(shard_id),
            .reserve_bid_price = 0,
            .action = .ask,
            .uid = 30_000 + @as(i64, @intCast(shard_id)),
            .timestamp = 40_000 + @as(i64, @intCast(shard_id)),
        }};
        try result.matchingShardForSymbol(symbol).restoreOrdersForSymbol(symbol, &order);
        try addPendingTransfer(
            &result.matching_shards[shard_id].binary_frame_assembler,
            1_000 + @as(i32, @intCast(shard_id)),
            50_000 + @as(i64, @intCast(shard_id * 10)),
        );
    }

    for (0..risk_count) |shard_id| {
        const uid: i64 = 20_000 + @as(i64, @intCast(shard_id));
        const engine = result.risk_shards[shard_id].engine;
        try std.testing.expect(engine.uidForThisHandler(uid));
        try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(uid));
        const profile = engine.getUserProfile(uid) orelse return error.TestExpectedUser;
        profile.user_status = .active;
        profile.adjustments_counter = @intCast(100 + shard_id);
        try profile.putAccount(allocator, 15, 1_000 + @as(i64, @intCast(shard_id)));
        try engine.last_price_cache.put(allocator, 8_000, .{
            .ask_price = 200 + @as(i64, @intCast(shard_id)),
            .bid_price = 190 + @as(i64, @intCast(shard_id)),
        });
        try engine.fees.put(allocator, 15, @intCast(shard_id + 1));
        try engine.fees_history.recordPut(allocator, 15);
        try addPendingTransfer(
            &result.risk_shards[shard_id].binary_frame_assembler,
            2_000 + @as(i32, @intCast(shard_id)),
            60_000 + @as(i64, @intCast(shard_id * 10)),
        );
    }
    result.restorePublicationSequence(identity.risk_command_seq);
    return result;
}

fn expectEquivalentSets(
    allocator: std.mem.Allocator,
    expected: *const kernel_mod.MultiShardKernelSet,
    actual: *const kernel_mod.MultiShardKernelSet,
) !void {
    try std.testing.expectEqual(expected.matchingShardCount(), actual.matchingShardCount());
    try std.testing.expectEqual(expected.riskShardCount(), actual.riskShardCount());
    try std.testing.expectEqual(expected.next_seq, actual.next_seq);
    for (0..expected.matchingShardCount()) |shard_id| {
        var lhs = try state.MatchingState.capture(
            allocator,
            &expected.matching_shards[shard_id],
            .{},
        );
        defer lhs.deinit();
        var rhs = try state.MatchingState.capture(
            allocator,
            &actual.matching_shards[shard_id],
            .{},
        );
        defer rhs.deinit();
        const lhs_bytes = try lhs.encodeAlloc(allocator, .{});
        defer allocator.free(lhs_bytes);
        const rhs_bytes = try rhs.encodeAlloc(allocator, .{});
        defer allocator.free(rhs_bytes);
        try std.testing.expectEqualSlices(u8, lhs_bytes, rhs_bytes);
        try std.testing.expectEqual(expected.next_seq, actual.matchingObservedSequence(shard_id));
    }
    for (0..expected.riskShardCount()) |shard_id| {
        const expected_shard = &expected.risk_shards[shard_id];
        const actual_shard = &actual.risk_shards[shard_id];
        var lhs = try state.RiskState.capture(
            allocator,
            expected_shard.engine,
            &expected_shard.binary_frame_assembler,
            .{},
        );
        defer lhs.deinit();
        var rhs = try state.RiskState.capture(
            allocator,
            actual_shard.engine,
            &actual_shard.binary_frame_assembler,
            .{},
        );
        defer rhs.deinit();
        const lhs_bytes = try lhs.encodeAlloc(allocator, .{});
        defer allocator.free(lhs_bytes);
        const rhs_bytes = try rhs.encodeAlloc(allocator, .{});
        defer allocator.free(rhs_bytes);
        try std.testing.expectEqualSlices(u8, lhs_bytes, rhs_bytes);
        try std.testing.expectEqual(expected.next_seq, actual.riskObservedSequence(shard_id));
    }
}

fn expectFileExists(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    path: []const u8,
) !void {
    const bytes = try container.readBoundedFileAlloc(allocator, dir, path, 8 * 1024 * 1024);
    defer allocator.free(bytes);
    try std.testing.expect(bytes.len > 0);
}

fn writeFile(
    dir: std.Io.Dir,
    path: []const u8,
    bytes: []const u8,
) !void {
    const file = try dir.createFile(io_context.current(), path, .{ .truncate = true });
    defer file.close(io_context.current());
    var buffer: [4096]u8 = undefined;
    var writer = file.writer(io_context.current(), &buffer);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
    try file.sync(io_context.current());
}

fn writeLegacyManifestForTest(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    storage_folder: []const u8,
    exchange_id: []const u8,
    file_snapshot_id: u64,
    embedded_snapshot_id: u64,
) !void {
    try paths.ensureDirPathDurable(io_context.current(), dir, storage_folder);
    const manifest_path = try paths.snapshotManifestPath(
        allocator,
        storage_folder,
        exchange_id,
        file_snapshot_id,
    );
    defer allocator.free(manifest_path);
    try snapshot.writeManifestAtomic(allocator, dir, manifest_path, .{
        .snapshot_id = embedded_snapshot_id,
        .matching_seq = 100 + embedded_snapshot_id,
        .risk_seq = 101 + embedded_snapshot_id,
        .next_seq = 101 + embedded_snapshot_id,
        .timestamp_ns = 1_000 + embedded_snapshot_id,
        .matching_shards = 1,
        .risk_shards = 1,
        .matching = .{ .byte_len = 11, .hash = 12 },
        .risk = .{ .byte_len = 13, .hash = 14 },
    });
}

fn writeXcm2SnapshotForTest(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    storage_folder: []const u8,
    exchange_id: []const u8,
    snapshot_id: u64,
) !void {
    const identity = testIdentity(snapshot_id);
    var source = try makePopulatedSet(allocator, 1, 1, identity, .{});
    defer source.deinit();
    _ = try captureAndCommit(
        allocator,
        dir,
        storage_folder,
        exchange_id,
        &source,
        identity,
        .{},
    );
}

test "XCM discovery inventories only XCM1 while latest XCM2 compatibility stays empty" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const folder = "only-xcm1";
    const exchange_id = "EXLEGACY";
    const snapshot_id: u64 = 10;
    try writeLegacyManifestForTest(
        allocator,
        tmp.dir,
        folder,
        exchange_id,
        snapshot_id,
        snapshot_id,
    );

    const inventory = try discoverCommittedSnapshotInventory(
        allocator,
        tmp.dir,
        folder,
        exchange_id,
        .{},
    );
    try std.testing.expectEqual(@as(?u64, snapshot_id), inventory.latest_xcm1);
    try std.testing.expectEqual(@as(?u64, null), inventory.latest_xcm2);
    try std.testing.expectEqual(
        @as(?u64, null),
        try discoverLatestCommittedSnapshotId(
            allocator,
            tmp.dir,
            folder,
            exchange_id,
            .{},
        ),
    );
    try std.testing.expectError(
        error.LegacyXcm1Manifest,
        readCommittedSnapshotInfo(
            allocator,
            tmp.dir,
            folder,
            exchange_id,
            snapshot_id,
            1,
            1,
            .{},
            .{},
        ),
    );
}

test "XCM discovery compares newest valid XCM1 and XCM2 snapshot ids" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const exchange_id = "EXORDER";

    const legacy_newer_folder = "legacy-newer";
    try writeXcm2SnapshotForTest(
        allocator,
        tmp.dir,
        legacy_newer_folder,
        exchange_id,
        9,
    );
    try writeLegacyManifestForTest(
        allocator,
        tmp.dir,
        legacy_newer_folder,
        exchange_id,
        10,
        10,
    );
    const legacy_newer = try discoverCommittedSnapshotInventory(
        allocator,
        tmp.dir,
        legacy_newer_folder,
        exchange_id,
        .{},
    );
    try std.testing.expectEqual(@as(?u64, 10), legacy_newer.latest_xcm1);
    try std.testing.expectEqual(@as(?u64, 9), legacy_newer.latest_xcm2);

    const sharded_newer_folder = "sharded-newer";
    try writeLegacyManifestForTest(
        allocator,
        tmp.dir,
        sharded_newer_folder,
        exchange_id,
        10,
        10,
    );
    try writeXcm2SnapshotForTest(
        allocator,
        tmp.dir,
        sharded_newer_folder,
        exchange_id,
        11,
    );
    const sharded_newer = try discoverCommittedSnapshotInventory(
        allocator,
        tmp.dir,
        sharded_newer_folder,
        exchange_id,
        .{},
    );
    try std.testing.expectEqual(@as(?u64, 10), sharded_newer.latest_xcm1);
    try std.testing.expectEqual(@as(?u64, 11), sharded_newer.latest_xcm2);
}

test "XCM discovery preserves unknown and invalid manifest policy" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const exchange_id = "EXINVALID";

    const unknown_folder = "unknown-magic";
    try tmp.dir.createDirPath(io_context.current(), unknown_folder);
    const unknown_path = try paths.snapshotManifestPath(
        allocator,
        unknown_folder,
        exchange_id,
        20,
    );
    defer allocator.free(unknown_path);
    try writeFile(tmp.dir, unknown_path, "JUNK");
    const unknown = try discoverCommittedSnapshotInventory(
        allocator,
        tmp.dir,
        unknown_folder,
        exchange_id,
        .{},
    );
    try std.testing.expectEqual(@as(?u64, null), unknown.latest_xcm1);
    try std.testing.expectEqual(@as(?u64, null), unknown.latest_xcm2);

    const unsupported_folder = "unsupported-xcm2";
    try writeXcm2SnapshotForTest(
        allocator,
        tmp.dir,
        unsupported_folder,
        exchange_id,
        21,
    );
    const unsupported_path = try paths.snapshotManifestPath(
        allocator,
        unsupported_folder,
        exchange_id,
        21,
    );
    defer allocator.free(unsupported_path);
    const unsupported_bytes = try container.readBoundedFileAlloc(
        allocator,
        tmp.dir,
        unsupported_path,
        4 * 1024 * 1024,
    );
    defer allocator.free(unsupported_bytes);
    std.mem.writeInt(u16, unsupported_bytes[4..6], container.format_version + 1, .little);
    try writeFile(tmp.dir, unsupported_path, unsupported_bytes);
    const unsupported = try discoverCommittedSnapshotInventory(
        allocator,
        tmp.dir,
        unsupported_folder,
        exchange_id,
        .{},
    );
    try std.testing.expectEqual(@as(?u64, null), unsupported.latest_xcm1);
    try std.testing.expectEqual(@as(?u64, null), unsupported.latest_xcm2);

    const malformed_folder = "malformed-xcm2";
    try tmp.dir.createDirPath(io_context.current(), malformed_folder);
    const malformed_path = try paths.snapshotManifestPath(
        allocator,
        malformed_folder,
        exchange_id,
        22,
    );
    defer allocator.free(malformed_path);
    try writeFile(tmp.dir, malformed_path, "XCM2");
    try std.testing.expectError(
        error.Truncated,
        discoverCommittedSnapshotInventory(
            allocator,
            tmp.dir,
            malformed_folder,
            exchange_id,
            .{},
        ),
    );

    const mismatch_folder = "mismatched-xcm1";
    try writeLegacyManifestForTest(
        allocator,
        tmp.dir,
        mismatch_folder,
        exchange_id,
        23,
        24,
    );
    try std.testing.expectError(
        error.SnapshotIdMismatch,
        discoverCommittedSnapshotInventory(
            allocator,
            tmp.dir,
            mismatch_folder,
            exchange_id,
            .{},
        ),
    );
}

test "XCM2 store roundtrips independent 4ME/2RE and 1ME/4RE with canonical filenames" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base_config: kernel_mod.Config = .{ .orderbook_kind = .direct };

    const cases = [_]struct {
        matching_count: usize,
        risk_count: usize,
        snapshot_id: u64,
        storage_folder: []const u8,
    }{
        .{ .matching_count = 4, .risk_count = 2, .snapshot_id = 101, .storage_folder = "case-4-2" },
        .{ .matching_count = 1, .risk_count = 4, .snapshot_id = 102, .storage_folder = "case-1-4" },
    };
    for (cases) |case| {
        const identity = testIdentity(case.snapshot_id);
        var source = try makePopulatedSet(
            allocator,
            case.matching_count,
            case.risk_count,
            identity,
            base_config,
        );
        defer source.deinit();
        const summary = try captureAndCommit(
            allocator,
            tmp.dir,
            case.storage_folder,
            "EXCHANGE",
            &source,
            identity,
            .{},
        );
        try std.testing.expectEqual(@as(u32, @intCast(case.matching_count)), summary.matching_count);
        try std.testing.expectEqual(@as(u32, @intCast(case.risk_count)), summary.risk_count);
        try std.testing.expectEqual(identity.risk_command_seq, summary.publication_high_water);

        for (0..case.matching_count) |shard_id| {
            const path = try paths.snapshotPath(
                allocator,
                case.storage_folder,
                "EXCHANGE",
                identity.snapshot_id,
                .matching_engine_router,
                @intCast(shard_id),
            );
            defer allocator.free(path);
            try expectFileExists(allocator, tmp.dir, path);
        }
        for (0..case.risk_count) |shard_id| {
            const path = try paths.snapshotPath(
                allocator,
                case.storage_folder,
                "EXCHANGE",
                identity.snapshot_id,
                .risk_engine,
                @intCast(shard_id),
            );
            defer allocator.free(path);
            try expectFileExists(allocator, tmp.dir, path);
        }
        const manifest_path = try paths.snapshotManifestPath(
            allocator,
            case.storage_folder,
            "EXCHANGE",
            identity.snapshot_id,
        );
        defer allocator.free(manifest_path);
        try expectFileExists(allocator, tmp.dir, manifest_path);

        var storage_dir = try tmp.dir.openDir(
            io_context.current(),
            case.storage_folder,
            .{ .iterate = true },
        );
        defer storage_dir.close(io_context.current());
        var iterator = storage_dir.iterate();
        var file_count: usize = 0;
        while (try iterator.next(io_context.current())) |_| file_count += 1;
        try std.testing.expectEqual(case.matching_count + case.risk_count + 1, file_count);

        var manifest = try container.readManifestFileAlloc(
            allocator,
            tmp.dir,
            manifest_path,
            .{},
            .{ .snapshot_id = identity.snapshot_id },
        );
        defer manifest.deinit();
        for (0..case.matching_count) |shard_id| {
            try std.testing.expectEqual(container.ModuleKind.matching_engine, manifest.entries[shard_id].module_kind);
            try std.testing.expectEqual(@as(u32, @intCast(shard_id)), manifest.entries[shard_id].shard_id);
        }
        for (0..case.risk_count) |shard_id| {
            const entry_value = manifest.entries[case.matching_count + shard_id];
            try std.testing.expectEqual(container.ModuleKind.risk_engine, entry_value.module_kind);
            try std.testing.expectEqual(@as(u32, @intCast(shard_id)), entry_value.shard_id);
        }

        var restored = try restoreFresh(allocator, tmp.dir, case.storage_folder, "EXCHANGE", .{
            .identity = identity,
            .matching_count = @intCast(case.matching_count),
            .risk_count = @intCast(case.risk_count),
            .base_config = base_config,
        });
        defer restored.deinit();
        try expectEquivalentSets(allocator, &source, &restored);
    }
}

test "XCM2 store rejects invalid persist sequence and any stale module frontier before disk mutation" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var identity = testIdentity(201);
    var source = try makePopulatedSet(allocator, 1, 1, identity, .{});
    defer source.deinit();

    identity.risk_command_seq += 1;
    try std.testing.expectError(
        error.InvalidSequence,
        captureAndCommit(allocator, tmp.dir, "invalid-seq", "EX", &source, identity, .{}),
    );
    identity = testIdentity(201);
    source.matching_shards[0].next_seq -= 1;
    try std.testing.expectError(
        error.PublicationFrontierMismatch,
        captureAndCommit(allocator, tmp.dir, "stale-frontier", "EX", &source, identity, .{}),
    );
    try std.testing.expectError(
        error.FileNotFound,
        tmp.dir.openFile(io_context.current(), "stale-frontier/EX_snapshot_201.ecm", .{}),
    );
}

test "XCM2 payload publication is canonical and manifest remains absent after a late collision" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const identity = testIdentity(301);
    var source = try makePopulatedSet(allocator, 4, 2, identity, .{});
    defer source.deinit();
    const folder = "collision";
    try tmp.dir.createDirPath(io_context.current(), folder);
    const collision_path = try paths.snapshotPath(
        allocator,
        folder,
        "EX",
        identity.snapshot_id,
        .risk_engine,
        1,
    );
    defer allocator.free(collision_path);
    try writeFile(tmp.dir, collision_path, "sentinel");

    try std.testing.expectError(
        error.PathAlreadyExists,
        captureAndCommit(allocator, tmp.dir, folder, "EX", &source, identity, .{}),
    );
    for (0..4) |shard_id| {
        const path = try paths.snapshotPath(
            allocator,
            folder,
            "EX",
            identity.snapshot_id,
            .matching_engine_router,
            @intCast(shard_id),
        );
        defer allocator.free(path);
        try expectFileExists(allocator, tmp.dir, path);
    }
    const risk_zero = try paths.snapshotPath(
        allocator,
        folder,
        "EX",
        identity.snapshot_id,
        .risk_engine,
        0,
    );
    defer allocator.free(risk_zero);
    try expectFileExists(allocator, tmp.dir, risk_zero);
    const sentinel = try container.readBoundedFileAlloc(allocator, tmp.dir, collision_path, 32);
    defer allocator.free(sentinel);
    try std.testing.expectEqualStrings("sentinel", sentinel);

    const manifest_path = try paths.snapshotManifestPath(allocator, folder, "EX", identity.snapshot_id);
    defer allocator.free(manifest_path);
    try std.testing.expectError(
        error.FileNotFound,
        tmp.dir.openFile(io_context.current(), manifest_path, .{}),
    );
    try std.testing.expectError(
        error.FileNotFound,
        restoreFresh(allocator, tmp.dir, folder, "EX", .{
            .identity = identity,
            .matching_count = 4,
            .risk_count = 2,
            .base_config = .{},
        }),
    );
}

fn rewriteManifest(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    path: []const u8,
    header: container.ManifestHeader,
    entries: []const container.ManifestEntry,
) !void {
    const bytes = try container.encodeManifestAlloc(allocator, header, entries, .{});
    defer allocator.free(bytes);
    try writeFile(dir, path, bytes);
}

fn commitFolder(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    folder: []const u8,
    source: *const kernel_mod.MultiShardKernelSet,
    identity: BundleIdentity,
) !void {
    _ = try captureAndCommit(allocator, dir, folder, "EX", source, identity, .{});
}

fn requestFor(
    identity: BundleIdentity,
    matching_count: u32,
    risk_count: u32,
) RestoreRequest {
    return .{
        .identity = identity,
        .matching_count = matching_count,
        .risk_count = risk_count,
        .base_config = .{},
    };
}

fn manifestPath(
    allocator: std.mem.Allocator,
    folder: []const u8,
    identity: BundleIdentity,
) ![]u8 {
    return paths.snapshotManifestPath(allocator, folder, "EX", identity.snapshot_id);
}

fn payloadPath(
    allocator: std.mem.Allocator,
    folder: []const u8,
    identity: BundleIdentity,
    module_type: paths.ModuleType,
    shard_id: u32,
) ![]u8 {
    return paths.snapshotPath(
        allocator,
        folder,
        "EX",
        identity.snapshot_id,
        module_type,
        shard_id,
    );
}

test "XCM2 restore rejects wrong snapshot config topology sequence timestamp exchange bundle and aggregate identity" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const identity = testIdentity(401);
    var source = try makePopulatedSet(allocator, 4, 2, identity, .{});
    defer source.deinit();
    try commitFolder(allocator, tmp.dir, "identity", &source, identity);

    var wrong_config = identity;
    wrong_config.config_sha256[0] ^= 1;
    try std.testing.expectError(
        error.ConfigMismatch,
        restoreFresh(allocator, tmp.dir, "identity", "EX", requestFor(wrong_config, 4, 2)),
    );
    try std.testing.expectError(
        error.TopologyMismatch,
        restoreFresh(allocator, tmp.dir, "identity", "EX", requestFor(identity, 2, 2)),
    );
    var wrong_sequence = identity;
    wrong_sequence.matching_command_seq += 1;
    wrong_sequence.risk_command_seq += 1;
    try std.testing.expectError(
        error.SequenceMismatch,
        restoreFresh(allocator, tmp.dir, "identity", "EX", requestFor(wrong_sequence, 4, 2)),
    );
    var wrong_timestamp = identity;
    wrong_timestamp.timestamp_ns += 1;
    try std.testing.expectError(
        error.TimestampMismatch,
        restoreFresh(allocator, tmp.dir, "identity", "EX", requestFor(wrong_timestamp, 4, 2)),
    );

    try commitFolder(allocator, tmp.dir, "snapshot-id", &source, identity);
    const snapshot_manifest_path = try manifestPath(allocator, "snapshot-id", identity);
    defer allocator.free(snapshot_manifest_path);
    var snapshot_manifest = try container.readManifestFileAlloc(
        allocator,
        tmp.dir,
        snapshot_manifest_path,
        .{},
        .{},
    );
    defer snapshot_manifest.deinit();
    var snapshot_header = snapshot_manifest.header;
    snapshot_header.snapshot_id += 1;
    try rewriteManifest(
        allocator,
        tmp.dir,
        snapshot_manifest_path,
        snapshot_header,
        snapshot_manifest.entries,
    );
    try std.testing.expectError(
        error.SnapshotIdMismatch,
        restoreFresh(allocator, tmp.dir, "snapshot-id", "EX", requestFor(identity, 4, 2)),
    );

    try commitFolder(allocator, tmp.dir, "exchange", &source, identity);
    const exchange_manifest_path = try manifestPath(allocator, "exchange", identity);
    defer allocator.free(exchange_manifest_path);
    var exchange_manifest = try container.readManifestFileAlloc(
        allocator,
        tmp.dir,
        exchange_manifest_path,
        .{},
        .{},
    );
    defer exchange_manifest.deinit();
    var exchange_header = exchange_manifest.header;
    exchange_header.exchange_id_sha256[0] ^= 1;
    try rewriteManifest(
        allocator,
        tmp.dir,
        exchange_manifest_path,
        exchange_header,
        exchange_manifest.entries,
    );
    try std.testing.expectError(
        error.ExchangeMismatch,
        restoreFresh(allocator, tmp.dir, "exchange", "EX", requestFor(identity, 4, 2)),
    );

    try commitFolder(allocator, tmp.dir, "bundle", &source, identity);
    const bundle_manifest_path = try manifestPath(allocator, "bundle", identity);
    defer allocator.free(bundle_manifest_path);
    var bundle_manifest = try container.readManifestFileAlloc(
        allocator,
        tmp.dir,
        bundle_manifest_path,
        .{},
        .{},
    );
    defer bundle_manifest.deinit();
    var bundle_header = bundle_manifest.header;
    bundle_header.bundle_id[0] ^= 1;
    try rewriteManifest(
        allocator,
        tmp.dir,
        bundle_manifest_path,
        bundle_header,
        bundle_manifest.entries,
    );
    try std.testing.expectError(
        error.BundleMismatch,
        restoreFresh(allocator, tmp.dir, "bundle", "EX", requestFor(identity, 4, 2)),
    );

    try commitFolder(allocator, tmp.dir, "aggregate", &source, identity);
    const aggregate_manifest_path = try manifestPath(allocator, "aggregate", identity);
    defer allocator.free(aggregate_manifest_path);
    var aggregate_manifest = try container.readManifestFileAlloc(
        allocator,
        tmp.dir,
        aggregate_manifest_path,
        .{},
        .{},
    );
    defer aggregate_manifest.deinit();
    var aggregate_header = aggregate_manifest.header;
    aggregate_header.aggregate_state_hash ^= 1;
    try rewriteManifest(
        allocator,
        tmp.dir,
        aggregate_manifest_path,
        aggregate_header,
        aggregate_manifest.entries,
    );
    try std.testing.expectError(
        error.AggregateStateHashMismatch,
        restoreFresh(allocator, tmp.dir, "aggregate", "EX", requestFor(identity, 4, 2)),
    );
}

test "XCM2 restore rejects missing tampered swapped truncated and trailing payload files" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const identity = testIdentity(501);
    var source = try makePopulatedSet(allocator, 4, 2, identity, .{});
    defer source.deinit();
    const request = requestFor(identity, 4, 2);

    try commitFolder(allocator, tmp.dir, "missing", &source, identity);
    const missing_path = try payloadPath(allocator, "missing", identity, .matching_engine_router, 0);
    defer allocator.free(missing_path);
    try tmp.dir.deleteFile(io_context.current(), missing_path);
    try std.testing.expectError(
        error.FileNotFound,
        restoreFresh(allocator, tmp.dir, "missing", "EX", request),
    );

    try commitFolder(allocator, tmp.dir, "tampered", &source, identity);
    const tampered_path = try payloadPath(allocator, "tampered", identity, .matching_engine_router, 0);
    defer allocator.free(tampered_path);
    const tampered = try container.readBoundedFileAlloc(allocator, tmp.dir, tampered_path, 8 * 1024 * 1024);
    defer allocator.free(tampered);
    tampered[container.payload_header_len] ^= 1;
    try writeFile(tmp.dir, tampered_path, tampered);
    try std.testing.expectError(
        error.PayloadDigestMismatch,
        restoreFresh(allocator, tmp.dir, "tampered", "EX", request),
    );

    try commitFolder(allocator, tmp.dir, "swapped", &source, identity);
    const swap_zero_path = try payloadPath(allocator, "swapped", identity, .matching_engine_router, 0);
    defer allocator.free(swap_zero_path);
    const swap_one_path = try payloadPath(allocator, "swapped", identity, .matching_engine_router, 1);
    defer allocator.free(swap_one_path);
    const swap_zero = try container.readBoundedFileAlloc(allocator, tmp.dir, swap_zero_path, 8 * 1024 * 1024);
    defer allocator.free(swap_zero);
    const swap_one = try container.readBoundedFileAlloc(allocator, tmp.dir, swap_one_path, 8 * 1024 * 1024);
    defer allocator.free(swap_one);
    try writeFile(tmp.dir, swap_zero_path, swap_one);
    try writeFile(tmp.dir, swap_one_path, swap_zero);
    try std.testing.expectError(
        error.PayloadShardMismatch,
        restoreFresh(allocator, tmp.dir, "swapped", "EX", request),
    );

    try commitFolder(allocator, tmp.dir, "truncated", &source, identity);
    const truncated_path = try payloadPath(allocator, "truncated", identity, .matching_engine_router, 0);
    defer allocator.free(truncated_path);
    const truncated = try container.readBoundedFileAlloc(allocator, tmp.dir, truncated_path, 8 * 1024 * 1024);
    defer allocator.free(truncated);
    try writeFile(tmp.dir, truncated_path, truncated[0 .. truncated.len - 1]);
    try std.testing.expectError(
        error.Truncated,
        restoreFresh(allocator, tmp.dir, "truncated", "EX", request),
    );

    try commitFolder(allocator, tmp.dir, "trailing", &source, identity);
    const trailing_path = try payloadPath(allocator, "trailing", identity, .matching_engine_router, 0);
    defer allocator.free(trailing_path);
    const original = try container.readBoundedFileAlloc(allocator, tmp.dir, trailing_path, 8 * 1024 * 1024);
    defer allocator.free(original);
    const trailing = try allocator.alloc(u8, original.len + 1);
    defer allocator.free(trailing);
    @memcpy(trailing[0..original.len], original);
    trailing[original.len] = 0;
    try writeFile(tmp.dir, trailing_path, trailing);
    try std.testing.expectError(
        error.TrailingBytes,
        restoreFresh(allocator, tmp.dir, "trailing", "EX", request),
    );
}

test "XCM2 restore recomputes decoded module hashes and never exposes a partially staged set" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const identity = testIdentity(601);
    var live = try makePopulatedSet(allocator, 1, 1, identity, .{});
    defer live.deinit();
    const live_matching_hash = try state.matchingBooksStateHash(&live.matching_shards[0]);
    const live_matching_binary_hash = try live.matching_shards[0].binary_frame_assembler.stateHash();
    const live_risk_hash = try live.risk_shards[0].engine.stateHash();
    const live_risk_binary_hash = try live.risk_shards[0].binary_frame_assembler.stateHash();
    const live_frontier = live.next_seq;

    try commitFolder(allocator, tmp.dir, "module-hash", &live, identity);
    const payload_path = try payloadPath(allocator, "module-hash", identity, .matching_engine_router, 0);
    defer allocator.free(payload_path);
    const payload = try container.readBoundedFileAlloc(allocator, tmp.dir, payload_path, 8 * 1024 * 1024);
    defer allocator.free(payload);
    // Change the embedded matching semantic fingerprint while preserving a
    // structurally decodable body and then honestly advertise the new digest.
    payload[container.payload_header_len + 8] ^= 1;
    try writeFile(tmp.dir, payload_path, payload);

    const manifest_path_value = try manifestPath(allocator, "module-hash", identity);
    defer allocator.free(manifest_path_value);
    var manifest = try container.readManifestFileAlloc(
        allocator,
        tmp.dir,
        manifest_path_value,
        .{},
        .{},
    );
    defer manifest.deinit();
    manifest.entries[0].payload_sha256 = container.sha256(payload);
    try rewriteManifest(
        allocator,
        tmp.dir,
        manifest_path_value,
        manifest.header,
        manifest.entries,
    );
    try std.testing.expectError(
        error.ModuleStateHashMismatch,
        restoreFresh(allocator, tmp.dir, "module-hash", "EX", requestFor(identity, 1, 1)),
    );

    // restoreFresh accepts no live target; a failed staged restore cannot have
    // changed the caller-owned instance.
    try std.testing.expectEqual(live_matching_hash, try state.matchingBooksStateHash(&live.matching_shards[0]));
    try std.testing.expectEqual(live_matching_binary_hash, try live.matching_shards[0].binary_frame_assembler.stateHash());
    try std.testing.expectEqual(live_risk_hash, try live.risk_shards[0].engine.stateHash());
    try std.testing.expectEqual(live_risk_binary_hash, try live.risk_shards[0].binary_frame_assembler.stateHash());
    try std.testing.expectEqual(live_frontier, live.next_seq);
}

fn restoreAllocationTest(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    folder: []const u8,
    request: RestoreRequest,
) !void {
    var restored = try restoreFresh(allocator, dir, folder, "EX", request);
    defer restored.deinit();
}

test "XCM2 fresh restore releases all staged modules across every allocation failure" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const identity = testIdentity(701);
    var source = try makePopulatedSet(allocator, 1, 1, identity, .{});
    defer source.deinit();
    try commitFolder(allocator, tmp.dir, "oom", &source, identity);
    try std.testing.checkAllAllocationFailures(
        allocator,
        restoreAllocationTest,
        .{ tmp.dir, "oom", requestFor(identity, 1, 1) },
    );
}

test "XCM2 native module and aggregate hashes bind pending state kind shard and canonical order" {
    const matching_a = matchingModuleStateHash(0, 11, 22);
    const matching_pending_changed = matchingModuleStateHash(0, 11, 23);
    const matching_shard_changed = matchingModuleStateHash(1, 11, 22);
    const risk_same_fields = riskModuleStateHash(0, 11, 22);
    try std.testing.expect(matching_a != matching_pending_changed);
    try std.testing.expect(matching_a != matching_shard_changed);
    try std.testing.expect(matching_a != risk_same_fields);
    try std.testing.expect(
        aggregateStateHash(&.{ matching_a, matching_shard_changed }, &.{risk_same_fields}) !=
            aggregateStateHash(&.{ matching_shard_changed, matching_a }, &.{risk_same_fields}),
    );
}

test "XCM2 kernel config digest binds topology and every restored semantic option" {
    const base: kernel_mod.Config = .{};
    const digest = kernelConfigSha256(base, 4, 2);
    const legacy_v1_digest = [_]u8{
        0x81, 0x5c, 0xe8, 0xce, 0x87, 0x2e, 0xc9, 0xc8,
        0x95, 0x97, 0x37, 0x3e, 0x10, 0x5c, 0xe2, 0x4f,
        0x47, 0x85, 0xf3, 0x3e, 0x11, 0x79, 0x00, 0xe8,
        0x12, 0x4b, 0x09, 0xcb, 0xd7, 0x53, 0x1b, 0x97,
    };
    try std.testing.expectEqualSlices(u8, &legacy_v1_digest, &digest);
    try std.testing.expectEqual(digest, kernelConfigSha256(base, 4, 2));
    try std.testing.expect(!std.mem.eql(u8, &digest, &kernelConfigSha256(base, 2, 2)));
    try std.testing.expect(!std.mem.eql(u8, &digest, &kernelConfigSha256(base, 4, 1)));

    inline for (.{
        kernel_mod.Config{ .orderbook_kind = .naive },
        kernel_mod.Config{ .send_l2_for_every_cmd = true },
        kernel_mod.Config{ .l2_refresh_depth = 9 },
        kernel_mod.Config{ .risk = .{ .ignore_risk_processing = true } },
        kernel_mod.Config{ .risk = .{ .margin_trading_enabled = true } },
        kernel_mod.Config{ .report_queries = .{ .state_hash = false } },
        kernel_mod.Config{ .report_queries = .{ .single_user = false } },
        kernel_mod.Config{ .report_queries = .{ .total_currency_balance = false } },
        kernel_mod.Config{ .report_queries = .{ .add_accounts_binary = false } },
        kernel_mod.Config{ .report_queries = .{ .add_symbols_binary = false } },
    }) |changed| {
        const changed_digest = kernelConfigSha256(changed, 4, 2);
        try std.testing.expect(!std.mem.eql(u8, &digest, &changed_digest));
    }
}

test "XCM2 custom report config digest is canonical and binds registry metadata" {
    const forward_registry = report_registry.Registry.fromSpecs(.{
        StoreCustomReportA,
        StoreCustomReportB,
    });
    const reverse_registry = report_registry.Registry.fromSpecs(.{
        StoreCustomReportB,
        StoreCustomReportA,
    });
    const code_changed_registry = report_registry.Registry.fromSpecs(.{
        StoreCustomReportB,
    });
    const schema_changed_registry = report_registry.Registry.fromSpecs(.{
        StoreCustomReportASchemaChanged,
        StoreCustomReportB,
    });
    const version_changed_registry = report_registry.Registry.fromSpecs(.{
        StoreCustomReportAVersionChanged,
        StoreCustomReportB,
    });

    var custom_config: kernel_mod.Config = .{};
    custom_config.report_queries.custom_registry = forward_registry;
    const forward_digest = kernelConfigSha256(custom_config, 4, 2);
    try std.testing.expect(!std.mem.eql(
        u8,
        &forward_digest,
        &kernelConfigSha256(.{}, 4, 2),
    ));

    custom_config.report_queries.custom_registry = reverse_registry;
    try std.testing.expectEqual(
        forward_digest,
        kernelConfigSha256(custom_config, 4, 2),
    );

    custom_config.report_queries.custom_registry = code_changed_registry;
    try std.testing.expect(!std.mem.eql(
        u8,
        &forward_digest,
        &kernelConfigSha256(custom_config, 4, 2),
    ));

    custom_config.report_queries.custom_registry = schema_changed_registry;
    try std.testing.expect(!std.mem.eql(
        u8,
        &forward_digest,
        &kernelConfigSha256(custom_config, 4, 2),
    ));

    custom_config.report_queries.custom_registry = version_changed_registry;
    try std.testing.expect(!std.mem.eql(
        u8,
        &forward_digest,
        &kernelConfigSha256(custom_config, 4, 2),
    ));
}
