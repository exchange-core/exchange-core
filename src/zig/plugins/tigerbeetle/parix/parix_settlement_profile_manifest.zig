//! Parix host integration for the opt-in TigerBeetle plugin.
const std = @import("std");
const tigerbeetle = @import("tigerbeetle");

const registry = tigerbeetle.destination_registry;
const settlement = tigerbeetle.settlement;

const Sha256 = std.crypto.hash.sha2.Sha256;

pub const digest_len: usize = Sha256.digest_length;
pub const encoded_len: usize = 123;

const magic = "PSM1";
const version: u16 = 1;
const flags: u16 = 0;
const integrity_domain = "exchange-core/parix-settlement-profile-manifest/v1";

const magic_offset: usize = 0;
const version_offset: usize = magic_offset + magic.len;
const flags_offset: usize = version_offset + @sizeOf(u16);
const destination_offset: usize = flags_offset + @sizeOf(u16);
const namespace_offset: usize = destination_offset + @sizeOf(u128);
const journal_epoch_offset: usize = namespace_offset + @sizeOf(u8);
const registry_digest_offset: usize = journal_epoch_offset + @sizeOf(u16);
const profile_digest_offset: usize = registry_digest_offset + digest_len;
const integrity_offset: usize = profile_digest_offset + digest_len;

comptime {
    std.debug.assert(integrity_offset + digest_len == encoded_len);
    std.debug.assert(registry.registry_digest_len == digest_len);
}

/// Non-secret compatibility identity which must remain stable for one
/// persistence root. The manifest deliberately excludes transport endpoints
/// and credentials.
pub const Expected = struct {
    destination_id: registry.DestinationId,
    publication_domain: settlement.PublicationDomain,
    registry_digest: [digest_len]u8,
    profile_digest: [digest_len]u8,
};

pub const EnsureError = error{
    InvalidManifestPath,
    InvalidDestinationId,
    InvalidPublicationDomain,
    LegacyRootNotEmpty,
    Truncated,
    TrailingBytes,
    MagicMismatch,
    UnsupportedVersion,
    InvalidFlags,
    IntegrityMismatch,
    DestinationMismatch,
    PublicationDomainMismatch,
    RegistryDigestMismatch,
    ProfileDigestMismatch,
    StorageReadFailed,
    StorageWriteFailed,
    StorageSyncFailed,
};

/// Establishes or verifies the durable profile identity for `dir`.
///
/// Creation is allowed only when the manifest is absent and the persistence
/// root contains no other artifact. Publication is atomic and never replaces
/// an existing path. A concurrent identical creator is accepted only after
/// the winner's complete manifest has been reopened and validated.
pub fn ensureBound(
    io: std.Io,
    dir: std.Io.Dir,
    manifest_path: []const u8,
    expected: Expected,
) EnsureError!void {
    try validateManifestPath(manifest_path);
    try validateExpected(expected);

    if (try readExisting(io, dir, manifest_path)) |actual| {
        try expectEqual(expected, actual);
        return;
    }

    try requireEmptyRoot(io, dir);

    const bytes = encode(expected);
    var atomic_file = dir.createFileAtomic(io, manifest_path, .{
        .replace = false,
    }) catch return error.StorageWriteFailed;
    defer atomic_file.deinit(io);

    var write_buffer: [encoded_len]u8 = undefined;
    var file_writer = atomic_file.file.writer(io, &write_buffer);
    file_writer.interface.writeAll(&bytes) catch return error.StorageWriteFailed;
    file_writer.interface.flush() catch return error.StorageWriteFailed;
    atomic_file.file.sync(io) catch return error.StorageSyncFailed;

    atomic_file.link(io) catch |err| switch (err) {
        error.PathAlreadyExists => {
            const winner = (try readExisting(io, dir, manifest_path)) orelse
                return error.StorageReadFailed;
            try expectEqual(expected, winner);
            return;
        },
        else => return error.StorageWriteFailed,
    };

    syncDirectory(io, dir) catch return error.StorageSyncFailed;
}

fn validateManifestPath(path: []const u8) EnsureError!void {
    if (path.len == 0 or
        std.mem.eql(u8, path, ".") or
        std.mem.eql(u8, path, ".."))
    {
        return error.InvalidManifestPath;
    }
    for (path) |byte| {
        if (!std.ascii.isAlphanumeric(byte) and
            byte != '-' and
            byte != '_' and
            byte != '.')
        {
            return error.InvalidManifestPath;
        }
    }
}

fn validateExpected(expected: Expected) EnsureError!void {
    if (@intFromEnum(expected.destination_id) == 0) {
        return error.InvalidDestinationId;
    }
    expected.publication_domain.validate() catch {
        return error.InvalidPublicationDomain;
    };
}

fn readExisting(
    io: std.Io,
    dir: std.Io.Dir,
    path: []const u8,
) EnsureError!?Expected {
    const file = dir.openFile(io, path, .{
        .allow_directory = false,
        .follow_symlinks = false,
        .resolve_beneath = true,
    }) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return error.StorageReadFailed,
    };
    defer file.close(io);

    const file_len = file.length(io) catch return error.StorageReadFailed;
    if (file_len < encoded_len) return error.Truncated;
    if (file_len > encoded_len) return error.TrailingBytes;

    var bytes: [encoded_len]u8 = undefined;
    const read_len = file.readPositionalAll(io, &bytes, 0) catch
        return error.StorageReadFailed;
    if (read_len != encoded_len) return error.Truncated;
    return try decode(bytes);
}

fn requireEmptyRoot(io: std.Io, dir: std.Io.Dir) EnsureError!void {
    var iterable = dir.openDir(io, ".", .{ .iterate = true }) catch
        return error.StorageReadFailed;
    defer iterable.close(io);

    var iterator = iterable.iterate();
    if ((iterator.next(io) catch return error.StorageReadFailed) != null) {
        return error.LegacyRootNotEmpty;
    }
}

fn syncDirectory(io: std.Io, dir: std.Io.Dir) !void {
    const directory = try dir.openFile(io, ".", .{
        .mode = .read_only,
        .allow_directory = true,
        .path_only = false,
        .follow_symlinks = false,
        .resolve_beneath = true,
    });
    defer directory.close(io);
    try directory.sync(io);
}

fn encode(expected: Expected) [encoded_len]u8 {
    var bytes: [encoded_len]u8 = undefined;
    @memcpy(bytes[magic_offset..version_offset], magic);
    putInt(u16, &bytes, version_offset, version);
    putInt(u16, &bytes, flags_offset, flags);
    putInt(u128, &bytes, destination_offset, @intFromEnum(expected.destination_id));
    bytes[namespace_offset] = expected.publication_domain.namespace;
    putInt(u16, &bytes, journal_epoch_offset, expected.publication_domain.journal_epoch);
    @memcpy(
        bytes[registry_digest_offset..profile_digest_offset],
        &expected.registry_digest,
    );
    @memcpy(
        bytes[profile_digest_offset..integrity_offset],
        &expected.profile_digest,
    );
    const checksum = integrityDigest(bytes[0..integrity_offset]);
    @memcpy(bytes[integrity_offset..encoded_len], &checksum);
    return bytes;
}

fn decode(bytes: [encoded_len]u8) EnsureError!Expected {
    if (!std.mem.eql(u8, bytes[magic_offset..version_offset], magic)) {
        return error.MagicMismatch;
    }
    if (getInt(u16, &bytes, version_offset) != version) {
        return error.UnsupportedVersion;
    }
    if (getInt(u16, &bytes, flags_offset) != flags) {
        return error.InvalidFlags;
    }

    const wanted_integrity = integrityDigest(bytes[0..integrity_offset]);
    if (!std.mem.eql(u8, &wanted_integrity, bytes[integrity_offset..encoded_len])) {
        return error.IntegrityMismatch;
    }

    const destination_raw = getInt(u128, &bytes, destination_offset);
    if (destination_raw == 0) return error.InvalidDestinationId;
    const publication_domain: settlement.PublicationDomain = .{
        .namespace = bytes[namespace_offset],
        .journal_epoch = getInt(u16, &bytes, journal_epoch_offset),
    };
    publication_domain.validate() catch return error.InvalidPublicationDomain;

    var registry_digest: [digest_len]u8 = undefined;
    @memcpy(&registry_digest, bytes[registry_digest_offset..profile_digest_offset]);
    var profile_digest: [digest_len]u8 = undefined;
    @memcpy(&profile_digest, bytes[profile_digest_offset..integrity_offset]);

    return .{
        .destination_id = @enumFromInt(destination_raw),
        .publication_domain = publication_domain,
        .registry_digest = registry_digest,
        .profile_digest = profile_digest,
    };
}

fn expectEqual(expected: Expected, actual: Expected) EnsureError!void {
    if (expected.destination_id != actual.destination_id) {
        return error.DestinationMismatch;
    }
    if (!expected.publication_domain.eql(actual.publication_domain)) {
        return error.PublicationDomainMismatch;
    }
    if (!std.mem.eql(u8, &expected.registry_digest, &actual.registry_digest)) {
        return error.RegistryDigestMismatch;
    }
    if (!std.mem.eql(u8, &expected.profile_digest, &actual.profile_digest)) {
        return error.ProfileDigestMismatch;
    }
}

fn integrityDigest(payload: []const u8) [digest_len]u8 {
    var hasher = Sha256.init(.{});
    hasher.update(integrity_domain);
    hasher.update(payload);
    var digest: [digest_len]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn putInt(
    comptime T: type,
    bytes: *[encoded_len]u8,
    offset: usize,
    value: T,
) void {
    std.mem.writeInt(T, bytes[offset..][0..@sizeOf(T)], value, .little);
}

fn getInt(
    comptime T: type,
    bytes: *const [encoded_len]u8,
    offset: usize,
) T {
    return std.mem.readInt(T, bytes[offset..][0..@sizeOf(T)], .little);
}

fn testDigest(seed: u8) [digest_len]u8 {
    var digest: [digest_len]u8 = undefined;
    for (&digest, 0..) |*byte, index| {
        byte.* = seed +% @as(u8, @truncate(index));
    }
    return digest;
}

fn testExpected() Expected {
    return .{
        .destination_id = @enumFromInt(@as(u128, 0x0123456789abcdef)),
        .publication_domain = .{ .namespace = 7, .journal_epoch = 19 },
        .registry_digest = testDigest(0x20),
        .profile_digest = testDigest(0x80),
    };
}

fn writeTestBytes(dir: std.Io.Dir, path: []const u8, bytes: []const u8) !void {
    const io = std.testing.io;
    const file = try dir.createFile(io, path, .{ .truncate = true });
    defer file.close(io);
    var buffer: [encoded_len]u8 = undefined;
    var writer = file.writer(io, &buffer);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
}

test "fresh root publishes a fixed durable profile manifest" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try ensureBound(std.testing.io, tmp.dir, "profile.psm1", testExpected());
    const stat = try tmp.dir.statFile(std.testing.io, "profile.psm1", .{});
    try std.testing.expectEqual(@as(u64, encoded_len), stat.size);
}

test "exact profile manifest restart succeeds with existing artifacts" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const expected = testExpected();
    try ensureBound(std.testing.io, tmp.dir, "profile.psm1", expected);
    try writeTestBytes(tmp.dir, "settlement-outbox.tso1", "durable artifact");
    try ensureBound(std.testing.io, tmp.dir, "profile.psm1", expected);
}

test "profile manifest rejects changed durable identities" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const expected = testExpected();
    try ensureBound(std.testing.io, tmp.dir, "profile.psm1", expected);

    var changed = expected;
    changed.destination_id = @enumFromInt(@intFromEnum(expected.destination_id) + 1);
    try std.testing.expectError(
        error.DestinationMismatch,
        ensureBound(std.testing.io, tmp.dir, "profile.psm1", changed),
    );

    changed = expected;
    changed.publication_domain.journal_epoch += 1;
    try std.testing.expectError(
        error.PublicationDomainMismatch,
        ensureBound(std.testing.io, tmp.dir, "profile.psm1", changed),
    );

    changed = expected;
    changed.registry_digest[0] ^= 1;
    try std.testing.expectError(
        error.RegistryDigestMismatch,
        ensureBound(std.testing.io, tmp.dir, "profile.psm1", changed),
    );

    changed = expected;
    changed.profile_digest[0] ^= 1;
    try std.testing.expectError(
        error.ProfileDigestMismatch,
        ensureBound(std.testing.io, tmp.dir, "profile.psm1", changed),
    );
}

test "profile manifest rejects corrupt and truncated records" {
    var corrupt_tmp = std.testing.tmpDir(.{});
    defer corrupt_tmp.cleanup();
    var corrupt = encode(testExpected());
    corrupt[registry_digest_offset] ^= 1;
    try writeTestBytes(corrupt_tmp.dir, "profile.psm1", &corrupt);
    try std.testing.expectError(
        error.IntegrityMismatch,
        ensureBound(std.testing.io, corrupt_tmp.dir, "profile.psm1", testExpected()),
    );

    var truncated_tmp = std.testing.tmpDir(.{});
    defer truncated_tmp.cleanup();
    const full = encode(testExpected());
    try writeTestBytes(truncated_tmp.dir, "profile.psm1", full[0 .. encoded_len - 1]);
    try std.testing.expectError(
        error.Truncated,
        ensureBound(std.testing.io, truncated_tmp.dir, "profile.psm1", testExpected()),
    );
}

test "non-empty legacy root is rejected without mutation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const legacy = "legacy settlement state";
    try writeTestBytes(tmp.dir, "settlement-outbox.tso1", legacy);
    try std.testing.expectError(
        error.LegacyRootNotEmpty,
        ensureBound(std.testing.io, tmp.dir, "profile.psm1", testExpected()),
    );
    try std.testing.expectError(
        error.FileNotFound,
        tmp.dir.access(std.testing.io, "profile.psm1", .{}),
    );

    const legacy_file = try tmp.dir.openFile(std.testing.io, "settlement-outbox.tso1", .{});
    defer legacy_file.close(std.testing.io);
    var actual: [legacy.len]u8 = undefined;
    const read_len = try legacy_file.readPositionalAll(std.testing.io, &actual, 0);
    try std.testing.expectEqual(legacy.len, read_len);
    try std.testing.expectEqualStrings(legacy, &actual);
}

test "profile manifest path is one safe file name" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    for ([_][]const u8{
        "",
        ".",
        "..",
        "nested/profile.psm1",
        "nested\\profile.psm1",
        "profile\x00.psm1",
        "profile?.psm1",
    }) |invalid| {
        try std.testing.expectError(
            error.InvalidManifestPath,
            ensureBound(std.testing.io, tmp.dir, invalid, testExpected()),
        );
    }
}
