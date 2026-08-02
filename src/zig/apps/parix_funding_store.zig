const std = @import("std");
const exchange_core = @import("exchange_core");
const saga = @import("parix_funding_saga.zig");

const persistence_paths = exchange_core.core.persistence_paths;

const snapshot_magic = "FSG1";
const snapshot_version: u16 = 1;
const snapshot_flags: u16 = 0;
const snapshot_digest_len: usize = 32;
const snapshot_header_len: usize = 112;
const snapshot_counter_len: usize = 24;
const snapshot_entry_len: usize = 251;
const encoded_transfer_len: usize = 128;
const integrity_domain = "exchange-core/parix-funding-snapshot/v1";

pub const CodecError = error{
    InvalidPath,
    Truncated,
    TrailingBytes,
    MagicMismatch,
    UnsupportedVersion,
    InvalidFlags,
    CapacityMismatch,
    MaxUsersMismatch,
    DestinationMismatch,
    RegistryDigestMismatch,
    ProfileDigestMismatch,
    CounterCountExceeded,
    EntryCountExceeded,
    DigestMismatch,
    InvalidEnum,
    InvalidState,
    IdentityDrift,
};

pub const StoreError = saga.LedgerError || CodecError || std.mem.Allocator.Error || error{
    AlreadyOpen,
    AlreadyInitialized,
    StoreMissing,
    Poisoned,
    InsecurePermissions,
    StorageReadFailed,
    StorageAmbiguous,
};

pub const OpenPolicy = enum {
    open_existing,
    initialize,
};

/// Durable, destination-bound wrapper around the pure funding saga.
///
/// The handle is intentionally single-threaded. Its owning runtime must
/// serialize calls. Every mutator commits a complete candidate snapshot before
/// publishing it in memory, so a returned claim proves its in-flight marker is
/// durable.
pub fn DurableStore(comptime capacity: usize, comptime max_users: usize) type {
    const Ledger = saga.Ledger(capacity, max_users);

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        io: std.Io,
        dir: std.Io.Dir,
        path: []u8,
        lock_path: []u8,
        lock_file: std.Io.File,
        ledger: Ledger,
        poisoned: bool = false,
        fail_next_pre_write_for_testing: bool = false,
        fail_next_write_for_testing: bool = false,
        fail_next_post_replace_for_testing: bool = false,

        pub fn openInto(
            out: *Self,
            allocator: std.mem.Allocator,
            io: std.Io,
            dir: std.Io.Dir,
            path: []const u8,
            open_policy: OpenPolicy,
            binding: saga.Binding,
            initial_floors: []const saga.LocalIdFloor,
            observed_core_counters: []const saga.LocalIdFloor,
        ) StoreError!void {
            try validatePath(path);
            binding.validate() catch return error.InvalidBinding;

            const path_copy = try allocator.dupe(u8, path);
            const lock_path = std.fmt.allocPrint(allocator, "{s}.lock", .{path}) catch |err| {
                allocator.free(path_copy);
                return err;
            };
            const lock_file = acquireLock(io, dir, lock_path) catch |err| {
                allocator.free(lock_path);
                allocator.free(path_copy);
                return err;
            };

            out.* = .{
                .allocator = allocator,
                .io = io,
                .dir = dir,
                .path = path_copy,
                .lock_path = lock_path,
                .lock_file = lock_file,
                .ledger = undefined,
            };
            errdefer {
                out.lock_file.close(io);
                allocator.free(lock_path);
                allocator.free(path_copy);
                out.* = undefined;
            }

            var must_commit = false;
            const file = dir.openFile(io, path, secureOpenOptions(.read_only)) catch |err| switch (err) {
                error.FileNotFound => switch (open_policy) {
                    .open_existing => return error.StoreMissing,
                    .initialize => null,
                },
                else => return error.StorageReadFailed,
            };
            if (file) |opened| {
                defer opened.close(io);
                if (open_policy == .initialize) return error.AlreadyInitialized;
                try requirePrivateRegularFile(io, opened);
                const max_size = encodedSize(capacity, max_users);
                const bytes = readToEndAlloc(io, allocator, opened, max_size + 1) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.StorageReadFailed,
                };
                defer allocator.free(bytes);
                if (bytes.len > max_size) return error.TrailingBytes;
                try decodeInto(
                    capacity,
                    max_users,
                    &out.ledger,
                    allocator,
                    bytes,
                    binding,
                    initial_floors,
                );
                const before_recovery_generation = out.ledger.snapshot_generation;
                _ = try out.ledger.reconcileCoreCounters(observed_core_counters);
                _ = try out.ledger.recover();
                must_commit = out.ledger.snapshot_generation != before_recovery_generation;
            } else {
                try Ledger.initInto(&out.ledger, binding, initial_floors);
                _ = try out.ledger.reconcileCoreCounters(observed_core_counters);
                must_commit = true;
            }

            if (must_commit) {
                try out.commit(&out.ledger);
            }
        }

        pub fn deinit(self: *Self) void {
            self.lock_file.close(self.io);
            self.allocator.free(self.lock_path);
            self.allocator.free(self.path);
            self.* = undefined;
        }

        pub fn isPoisoned(self: *const Self) bool {
            return self.poisoned;
        }

        pub fn injectWriteFailureForTesting(self: *Self) void {
            self.fail_next_write_for_testing = true;
        }

        pub fn injectPreWriteFailureForTesting(self: *Self) void {
            self.fail_next_pre_write_for_testing = true;
        }

        pub fn injectPostReplaceFailureForTesting(self: *Self) void {
            self.fail_next_post_replace_for_testing = true;
        }

        pub fn accept(
            self: *Self,
            request: saga.Request,
            route: saga.Route,
        ) StoreError!saga.AcceptResult {
            try self.ensureHealthy();
            const candidate = try self.candidateAlloc();
            defer self.allocator.destroy(candidate);
            const result = try candidate.accept(request, route);
            if (result.disposition == .inserted) try self.commit(candidate);
            return result;
        }

        pub fn lookup(self: *const Self, operation_id: u128) StoreError!saga.Status {
            try self.ensureHealthy();
            return self.ledger.lookup(operation_id);
        }

        pub fn summary(self: *const Self) StoreError!saga.Summary {
            try self.ensureHealthy();
            return self.ledger.summary();
        }

        pub fn claimNext(self: *Self) StoreError!?saga.Claim {
            try self.ensureHealthy();
            const candidate = try self.candidateAlloc();
            defer self.allocator.destroy(candidate);
            const claim = try candidate.claimNext() orelse return null;
            try self.commit(candidate);
            return claim;
        }

        pub fn complete(
            self: *Self,
            claim: saga.Claim,
            completion: saga.Completion,
        ) StoreError!saga.Status {
            try self.ensureHealthy();
            const candidate = try self.candidateAlloc();
            defer self.allocator.destroy(candidate);
            const status = try candidate.complete(claim, completion);
            try self.commit(candidate);
            return status;
        }

        fn ensureHealthy(self: *const Self) StoreError!void {
            if (self.poisoned) return error.Poisoned;
        }

        fn candidateAlloc(self: *Self) std.mem.Allocator.Error!*Ledger {
            const candidate = try self.allocator.create(Ledger);
            candidate.* = self.ledger;
            return candidate;
        }

        fn commit(self: *Self, candidate: *const Ledger) StoreError!void {
            if (self.fail_next_pre_write_for_testing) {
                self.fail_next_pre_write_for_testing = false;
                return error.OutOfMemory;
            }
            const bytes = try encodeAlloc(capacity, max_users, self.allocator, candidate);
            defer self.allocator.free(bytes);

            if (self.fail_next_write_for_testing) {
                self.fail_next_write_for_testing = false;
                self.poisoned = true;
                return error.StorageAmbiguous;
            }
            const fail_after_replace = self.fail_next_post_replace_for_testing;
            self.fail_next_post_replace_for_testing = false;
            writeBytesAtomic(self.io, self.dir, self.path, bytes, fail_after_replace) catch {
                self.poisoned = true;
                return error.StorageAmbiguous;
            };
            if (candidate != &self.ledger) self.ledger = candidate.*;
        }
    };
}

pub const DefaultStore = DurableStore(4_096, 16);

fn validatePath(path: []const u8) CodecError!void {
    if (path.len == 0 or std.mem.eql(u8, path, ".") or std.mem.eql(u8, path, "..")) {
        return error.InvalidPath;
    }
    for (path) |byte| {
        if (!std.ascii.isAlphanumeric(byte) and byte != '-' and byte != '_' and byte != '.') {
            return error.InvalidPath;
        }
    }
}

fn secureOpenOptions(mode: std.Io.Dir.OpenFileOptions.Mode) std.Io.Dir.OpenFileOptions {
    return .{
        .mode = mode,
        .allow_directory = false,
        .follow_symlinks = false,
        .resolve_beneath = true,
    };
}

fn acquireLock(io: std.Io, dir: std.Io.Dir, path: []const u8) StoreError!std.Io.File {
    const private_permissions: std.Io.File.Permissions = .fromMode(0o600);
    const created = dir.createFile(io, path, .{
        .read = true,
        .truncate = false,
        .exclusive = true,
        .lock = .exclusive,
        .lock_nonblocking = true,
        .permissions = private_permissions,
        .resolve_beneath = true,
    }) catch |err| switch (err) {
        error.PathAlreadyExists => null,
        error.WouldBlock => return error.AlreadyOpen,
        else => return error.StorageReadFailed,
    };
    const file = created orelse dir.openFile(io, path, .{
        .mode = .read_write,
        .allow_directory = false,
        .lock = .exclusive,
        .lock_nonblocking = true,
        .follow_symlinks = false,
        .resolve_beneath = true,
    }) catch |err| switch (err) {
        error.WouldBlock => return error.AlreadyOpen,
        else => return error.StorageReadFailed,
    };
    errdefer file.close(io);
    try requirePrivateRegularFile(io, file);
    return file;
}

fn requirePrivateRegularFile(io: std.Io, file: std.Io.File) StoreError!void {
    const stat = file.stat(io) catch return error.StorageReadFailed;
    if (stat.kind != .file) return error.StorageReadFailed;
    if (comptime @hasDecl(std.Io.File.Permissions, "toMode")) {
        if ((stat.permissions.toMode() & 0o7777) != 0o600) return error.InsecurePermissions;
    }
}

fn encodedSize(comptime capacity: usize, comptime max_users: usize) usize {
    return snapshot_header_len +
        snapshot_counter_len * max_users +
        snapshot_entry_len * capacity +
        snapshot_digest_len;
}

fn actualEncodedSize(counter_count: usize, entry_count: usize) usize {
    return snapshot_header_len +
        snapshot_counter_len * counter_count +
        snapshot_entry_len * entry_count +
        snapshot_digest_len;
}

fn encodeAlloc(
    comptime capacity: usize,
    comptime max_users: usize,
    allocator: std.mem.Allocator,
    ledger: *const saga.Ledger(capacity, max_users),
) (std.mem.Allocator.Error || CodecError)![]u8 {
    ledger.validate() catch return error.InvalidState;
    const total = actualEncodedSize(ledger.counterCount(), ledger.entriesSlice().len);
    const bytes = try allocator.alloc(u8, total);
    errdefer allocator.free(bytes);
    var index: usize = 0;

    @memcpy(bytes[index..][0..snapshot_magic.len], snapshot_magic);
    index += snapshot_magic.len;
    putInt(u16, bytes, &index, snapshot_version);
    putInt(u16, bytes, &index, snapshot_flags);
    putInt(u32, bytes, &index, @intCast(capacity));
    putInt(u32, bytes, &index, @intCast(max_users));
    putInt(u128, bytes, &index, @intFromEnum(ledger.binding.destination_id));
    @memcpy(bytes[index..][0..32], &ledger.binding.registry_digest);
    index += 32;
    @memcpy(bytes[index..][0..32], &ledger.binding.profile_digest);
    index += 32;
    putInt(u32, bytes, &index, @intCast(ledger.counterCount()));
    putInt(u32, bytes, &index, @intCast(ledger.entriesSlice().len));
    putInt(u64, bytes, &index, ledger.snapshot_generation);
    std.debug.assert(index == snapshot_header_len);

    for (0..ledger.counterCount()) |counter_index| {
        const counter = ledger.counterAt(counter_index);
        putInt(i64, bytes, &index, counter.uid);
        putInt(i64, bytes, &index, counter.last_reserved);
        putInt(i64, bytes, &index, counter.last_confirmed);
    }
    for (ledger.entriesSlice()) |entry| encodeEntry(bytes, &index, entry);

    const digest_offset = total - snapshot_digest_len;
    std.debug.assert(index == digest_offset);
    const digest = integrityDigest(bytes[0..digest_offset]);
    @memcpy(bytes[digest_offset..], &digest);
    return bytes;
}

fn decodeInto(
    comptime capacity: usize,
    comptime max_users: usize,
    out: *saga.Ledger(capacity, max_users),
    allocator: std.mem.Allocator,
    bytes: []const u8,
    binding: saga.Binding,
    initial_floors: []const saga.LocalIdFloor,
) StoreError!void {
    if (bytes.len < snapshot_header_len + snapshot_digest_len) return error.Truncated;
    const digest_offset = bytes.len - snapshot_digest_len;
    const wanted_digest = integrityDigest(bytes[0..digest_offset]);
    if (!std.mem.eql(u8, &wanted_digest, bytes[digest_offset..])) return error.DigestMismatch;

    var index: usize = 0;
    if (!std.mem.eql(u8, bytes[0..snapshot_magic.len], snapshot_magic)) return error.MagicMismatch;
    index += snapshot_magic.len;
    if (try takeInt(u16, bytes, &index, digest_offset) != snapshot_version) return error.UnsupportedVersion;
    if (try takeInt(u16, bytes, &index, digest_offset) != snapshot_flags) return error.InvalidFlags;
    if (try takeInt(u32, bytes, &index, digest_offset) != capacity) return error.CapacityMismatch;
    if (try takeInt(u32, bytes, &index, digest_offset) != max_users) return error.MaxUsersMismatch;
    if (try takeInt(u128, bytes, &index, digest_offset) != @intFromEnum(binding.destination_id)) {
        return error.DestinationMismatch;
    }
    if (!std.mem.eql(u8, bytes[index..][0..32], &binding.registry_digest)) return error.RegistryDigestMismatch;
    index += 32;
    if (!std.mem.eql(u8, bytes[index..][0..32], &binding.profile_digest)) return error.ProfileDigestMismatch;
    index += 32;
    const counter_count = try takeInt(u32, bytes, &index, digest_offset);
    const entry_count = try takeInt(u32, bytes, &index, digest_offset);
    const snapshot_generation = try takeInt(u64, bytes, &index, digest_offset);
    if (counter_count > max_users) return error.CounterCountExceeded;
    if (entry_count > capacity) return error.EntryCountExceeded;
    const expected_len = actualEncodedSize(counter_count, entry_count);
    if (bytes.len < expected_len) return error.Truncated;
    if (bytes.len > expected_len) return error.TrailingBytes;

    const persisted_counters = try allocator.alloc(saga.LocalIdCounter, counter_count);
    defer allocator.free(persisted_counters);
    for (persisted_counters) |*counter| {
        counter.* = .{
            .uid = try takeInt(i64, bytes, &index, digest_offset),
            .last_reserved = try takeInt(i64, bytes, &index, digest_offset),
            .last_confirmed = try takeInt(i64, bytes, &index, digest_offset),
        };
    }
    if (initial_floors.len != persisted_counters.len) return error.IdentityDrift;
    for (persisted_counters) |persisted| {
        const initial = findFloor(initial_floors, persisted.uid) orelse return error.IdentityDrift;
        if (initial.last_transaction_id < 0) return error.InvalidFloor;
        if (initial.last_transaction_id > persisted.last_confirmed) {
            return error.IdentityDrift;
        }
    }

    const restored_entries = try allocator.alloc(saga.Entry, entry_count);
    defer allocator.free(restored_entries);
    for (restored_entries) |*entry| entry.* = try decodeEntry(bytes, &index, digest_offset);
    if (index != digest_offset) return error.TrailingBytes;

    saga.Ledger(capacity, max_users).restoreInto(
        out,
        binding,
        persisted_counters,
        restored_entries,
        snapshot_generation,
    ) catch return error.InvalidState;
}

fn findFloor(floors: []const saga.LocalIdFloor, uid: i64) ?saga.LocalIdFloor {
    for (floors) |floor| if (floor.uid == uid) return floor;
    return null;
}

fn encodeEntry(bytes: []u8, index: *usize, entry: saga.Entry) void {
    const start = index.*;
    putInt(u128, bytes, index, entry.request.operation_id);
    putInt(u8, bytes, index, @intFromEnum(entry.request.kind));
    putInt(i64, bytes, index, entry.request.uid);
    putInt(i32, bytes, index, entry.request.currency);
    putInt(i64, bytes, index, entry.request.amount);
    putInt(u128, bytes, index, @intFromEnum(entry.route.destination_id));
    putInt(u32, bytes, index, entry.route.ledger);
    putInt(u128, bytes, index, entry.route.treasury_account_id);
    putInt(u128, bytes, index, entry.route.user_account_id);
    encodeTransfer(bytes, index, entry.transfer);
    putInt(i64, bytes, index, entry.primary_transaction_id);
    putInt(i64, bytes, index, entry.compensation_transaction_id);
    const encoded_phase = encodePhase(entry.phase);
    putInt(u8, bytes, index, encoded_phase.tag);
    putInt(u8, bytes, index, encoded_phase.payload);
    putInt(u64, bytes, index, entry.generation);
    putInt(u32, bytes, index, entry.external_attempts);
    putInt(u32, bytes, index, entry.core_attempts);
    std.debug.assert(index.* - start == snapshot_entry_len);
}

fn decodeEntry(bytes: []const u8, index: *usize, limit: usize) CodecError!saga.Entry {
    return .{
        .request = .{
            .operation_id = try takeInt(u128, bytes, index, limit),
            .kind = std.enums.fromInt(saga.Kind, try takeInt(u8, bytes, index, limit)) orelse return error.InvalidEnum,
            .uid = try takeInt(i64, bytes, index, limit),
            .currency = try takeInt(i32, bytes, index, limit),
            .amount = try takeInt(i64, bytes, index, limit),
        },
        .route = .{
            .destination_id = @enumFromInt(try takeInt(u128, bytes, index, limit)),
            .ledger = try takeInt(u32, bytes, index, limit),
            .treasury_account_id = try takeInt(u128, bytes, index, limit),
            .user_account_id = try takeInt(u128, bytes, index, limit),
        },
        .transfer = try decodeTransfer(bytes, index, limit),
        .primary_transaction_id = try takeInt(i64, bytes, index, limit),
        .compensation_transaction_id = try takeInt(i64, bytes, index, limit),
        .phase = try decodePhase(
            try takeInt(u8, bytes, index, limit),
            try takeInt(u8, bytes, index, limit),
        ),
        .generation = try takeInt(u64, bytes, index, limit),
        .external_attempts = try takeInt(u32, bytes, index, limit),
        .core_attempts = try takeInt(u32, bytes, index, limit),
    };
}

const EncodedPhase = struct { tag: u8, payload: u8 };

fn encodePhase(phase: saga.Phase) EncodedPhase {
    return switch (phase) {
        .accepted => .{ .tag = 1, .payload = 0 },
        .remote_submit_ready => .{ .tag = 2, .payload = 0 },
        .remote_submit_in_flight => .{ .tag = 3, .payload = 0 },
        .remote_lookup_ready => |cause| .{ .tag = 4, .payload = @intFromEnum(cause) },
        .remote_lookup_in_flight => |cause| .{ .tag = 5, .payload = @intFromEnum(cause) },
        .core_ready => |purpose| .{ .tag = 6, .payload = @intFromEnum(purpose) },
        .core_in_flight => |purpose| .{ .tag = 7, .payload = @intFromEnum(purpose) },
        .completed => .{ .tag = 8, .payload = 0 },
        .failed => |reason| .{ .tag = 9, .payload = @intFromEnum(reason) },
        .quarantined => |reason| .{ .tag = 10, .payload = @intFromEnum(reason) },
        .core_replay_ready => |purpose| .{ .tag = 11, .payload = @intFromEnum(purpose) },
    };
}

fn decodePhase(tag: u8, payload: u8) CodecError!saga.Phase {
    return switch (tag) {
        1 => if (payload == 0) .accepted else error.InvalidEnum,
        2 => if (payload == 0) .remote_submit_ready else error.InvalidEnum,
        3 => if (payload == 0) .remote_submit_in_flight else error.InvalidEnum,
        4 => .{ .remote_lookup_ready = std.enums.fromInt(saga.ReconciliationCause, payload) orelse return error.InvalidEnum },
        5 => .{ .remote_lookup_in_flight = std.enums.fromInt(saga.ReconciliationCause, payload) orelse return error.InvalidEnum },
        6 => .{ .core_ready = std.enums.fromInt(saga.CorePurpose, payload) orelse return error.InvalidEnum },
        7 => .{ .core_in_flight = std.enums.fromInt(saga.CorePurpose, payload) orelse return error.InvalidEnum },
        8 => if (payload == 0) .completed else error.InvalidEnum,
        9 => .{ .failed = std.enums.fromInt(saga.FailureReason, payload) orelse return error.InvalidEnum },
        10 => .{ .quarantined = std.enums.fromInt(saga.QuarantineReason, payload) orelse return error.InvalidEnum },
        11 => .{ .core_replay_ready = std.enums.fromInt(saga.CorePurpose, payload) orelse return error.InvalidEnum },
        else => error.InvalidEnum,
    };
}

fn encodeTransfer(bytes: []u8, index: *usize, transfer: saga.Transfer) void {
    const start = index.*;
    putInt(u128, bytes, index, transfer.id);
    putInt(u128, bytes, index, transfer.debit_account_id);
    putInt(u128, bytes, index, transfer.credit_account_id);
    putInt(u128, bytes, index, transfer.amount);
    putInt(u128, bytes, index, transfer.pending_id);
    putInt(u128, bytes, index, transfer.user_data_128);
    putInt(u64, bytes, index, transfer.user_data_64);
    putInt(u32, bytes, index, transfer.user_data_32);
    putInt(u32, bytes, index, transfer.timeout);
    putInt(u32, bytes, index, transfer.ledger);
    putInt(u16, bytes, index, transfer.code);
    putInt(u16, bytes, index, transfer.flags);
    putInt(u64, bytes, index, transfer.timestamp);
    std.debug.assert(index.* - start == encoded_transfer_len);
}

fn decodeTransfer(bytes: []const u8, index: *usize, limit: usize) CodecError!saga.Transfer {
    return .{
        .id = try takeInt(u128, bytes, index, limit),
        .debit_account_id = try takeInt(u128, bytes, index, limit),
        .credit_account_id = try takeInt(u128, bytes, index, limit),
        .amount = try takeInt(u128, bytes, index, limit),
        .pending_id = try takeInt(u128, bytes, index, limit),
        .user_data_128 = try takeInt(u128, bytes, index, limit),
        .user_data_64 = try takeInt(u64, bytes, index, limit),
        .user_data_32 = try takeInt(u32, bytes, index, limit),
        .timeout = try takeInt(u32, bytes, index, limit),
        .ledger = try takeInt(u32, bytes, index, limit),
        .code = try takeInt(u16, bytes, index, limit),
        .flags = try takeInt(u16, bytes, index, limit),
        .timestamp = try takeInt(u64, bytes, index, limit),
    };
}

fn putInt(comptime T: type, bytes: []u8, index: *usize, value: T) void {
    std.mem.writeInt(T, bytes[index.*..][0..@sizeOf(T)], value, .little);
    index.* += @sizeOf(T);
}

fn takeInt(
    comptime T: type,
    bytes: []const u8,
    index: *usize,
    limit: usize,
) CodecError!T {
    if (index.* > limit or limit - index.* < @sizeOf(T)) return error.Truncated;
    const value = std.mem.readInt(T, bytes[index.*..][0..@sizeOf(T)], .little);
    index.* += @sizeOf(T);
    return value;
}

fn integrityDigest(bytes: []const u8) [snapshot_digest_len]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(integrity_domain);
    hasher.update(bytes);
    var digest: [snapshot_digest_len]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn refreshDigest(bytes: []u8) void {
    std.debug.assert(bytes.len >= snapshot_digest_len);
    const digest_offset = bytes.len - snapshot_digest_len;
    const digest = integrityDigest(bytes[0..digest_offset]);
    @memcpy(bytes[digest_offset..], &digest);
}

fn readToEndAlloc(
    io: std.Io,
    allocator: std.mem.Allocator,
    file: std.Io.File,
    limit: usize,
) ![]u8 {
    const initial = try file.stat(io);
    if (initial.kind != .file) return error.NotAFile;
    if (initial.size > limit) return error.StreamTooLong;
    const size: usize = @intCast(initial.size);
    const bytes = try allocator.alloc(u8, size);
    errdefer allocator.free(bytes);
    const read_len = try file.readPositionalAll(io, bytes, 0);
    if (read_len != size) return error.UnexpectedEndOfStream;
    const final = try file.stat(io);
    if (final.inode != initial.inode or
        final.size != initial.size or
        final.kind != initial.kind or
        final.permissions != initial.permissions or
        final.nlink != initial.nlink or
        !std.meta.eql(final.mtime, initial.mtime) or
        !std.meta.eql(final.ctime, initial.ctime))
    {
        return error.FileChanged;
    }
    return bytes;
}

fn writeBytesAtomic(
    io: std.Io,
    dir: std.Io.Dir,
    path: []const u8,
    bytes: []const u8,
    fail_after_replace: bool,
) !void {
    var atomic_file = try dir.createFileAtomic(io, path, .{
        .permissions = .fromMode(0o600),
        .replace = true,
    });
    defer atomic_file.deinit(io);
    var buffer: [4096]u8 = undefined;
    var writer = atomic_file.file.writer(io, &buffer);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
    try atomic_file.file.sync(io);
    try atomic_file.replace(io);
    if (fail_after_replace) return error.InjectedPostReplaceFailure;
    try persistence_paths.syncParentDirectory(io, dir, path);
}

fn testBinding() saga.Binding {
    return .{
        .destination_id = @enumFromInt(77),
        .registry_digest = [_]u8{0xA5} ** 32,
        .profile_digest = [_]u8{0x5A} ** 32,
    };
}

fn testFloors() [2]saga.LocalIdFloor {
    return .{
        .{ .uid = 101, .last_transaction_id = 10 },
        .{ .uid = 202, .last_transaction_id = 20 },
    };
}

fn testRequest(operation_id: u128, kind: saga.Kind, uid: i64) saga.Request {
    return .{
        .operation_id = operation_id,
        .kind = kind,
        .uid = uid,
        .currency = 31,
        .amount = 500,
    };
}

fn testRoute(uid: i64) saga.Route {
    return .{
        .destination_id = testBinding().destination_id,
        .ledger = 700,
        .treasury_account_id = 8_000,
        .user_account_id = if (uid == 101) 8_101 else 8_202,
    };
}

fn rewriteTestFile(dir: std.Io.Dir, path: []const u8, bytes: []const u8) !void {
    const file = try dir.createFile(std.testing.io, path, .{
        .truncate = true,
        .permissions = .fromMode(0o600),
    });
    defer file.close(std.testing.io);
    var buffer: [4096]u8 = undefined;
    var writer = file.writer(std.testing.io, &buffer);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
    try file.sync(std.testing.io);
}

test "open policy requires explicit initialization at the exact live floors" {
    const Store = DurableStore(4, 2);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const floors = testFloors();
    var store: Store = undefined;

    try std.testing.expectError(
        error.StoreMissing,
        Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "ceremony.fsg1", .open_existing, testBinding(), &floors, &floors),
    );
    var advanced = floors;
    advanced[0].last_transaction_id += 1;
    try std.testing.expectError(
        error.CoreCounterMismatch,
        Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "ceremony.fsg1", .initialize, testBinding(), &floors, &advanced),
    );

    try Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "ceremony.fsg1", .initialize, testBinding(), &floors, &floors);
    store.deinit();
    try std.testing.expectError(
        error.CoreCounterMismatch,
        Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "ceremony.fsg1", .open_existing, testBinding(), &floors, &advanced),
    );
    try std.testing.expectError(
        error.AlreadyInitialized,
        Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "ceremony.fsg1", .initialize, testBinding(), &floors, &floors),
    );
}

test "absent store is published and exact duplicate survives reopen" {
    const Store = DurableStore(4, 2);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const floors = testFloors();
    const request = testRequest(1, .deposit, 101);
    const route = testRoute(101);

    {
        var store: Store = undefined;
        try Store.openInto(
            &store,
            std.testing.allocator,
            std.testing.io,
            tmp.dir,
            "funding.fsg1",
            .initialize,
            testBinding(),
            &floors,
            &floors,
        );
        defer store.deinit();
        if (comptime @hasDecl(std.Io.File.Permissions, "toMode")) {
            const snapshot_stat = try tmp.dir.statFile(std.testing.io, "funding.fsg1", .{ .follow_symlinks = false });
            const lock_stat = try tmp.dir.statFile(std.testing.io, "funding.fsg1.lock", .{ .follow_symlinks = false });
            try std.testing.expectEqual(@as(u32, 0o600), snapshot_stat.permissions.toMode() & 0o7777);
            try std.testing.expectEqual(@as(u32, 0o600), lock_stat.permissions.toMode() & 0o7777);
        }
        try std.testing.expectEqual(
            saga.AcceptDisposition.inserted,
            (try store.accept(request, route)).disposition,
        );
        try std.testing.expectEqual(
            saga.AcceptDisposition.existing,
            (try store.accept(request, route)).disposition,
        );
        var conflict = request;
        conflict.amount += 1;
        try std.testing.expectError(error.IdempotencyConflict, store.accept(conflict, route));
    }

    {
        var store: Store = undefined;
        try Store.openInto(
            &store,
            std.testing.allocator,
            std.testing.io,
            tmp.dir,
            "funding.fsg1",
            .open_existing,
            testBinding(),
            &floors,
            &floors,
        );
        defer store.deinit();
        try std.testing.expectEqual(saga.PublicState.accepted, (try store.lookup(1)).state);
        try std.testing.expectEqual(
            saga.AcceptDisposition.existing,
            (try store.accept(request, route)).disposition,
        );
    }
}

test "codec rejects truncation unknown phase and duplicate durable identity" {
    const Ledger = saga.Ledger(4, 2);
    const floors = testFloors();
    var ledger = try Ledger.init(testBinding(), &floors);
    _ = try ledger.accept(testRequest(30, .deposit, 101), testRoute(101));
    _ = try ledger.accept(testRequest(31, .deposit, 202), testRoute(202));
    const encoded = try encodeAlloc(4, 2, std.testing.allocator, &ledger);
    defer std.testing.allocator.free(encoded);
    var decoded: Ledger = undefined;

    try std.testing.expectError(
        error.Truncated,
        decodeInto(4, 2, &decoded, std.testing.allocator, encoded[0..100], testBinding(), &floors),
    );

    const first_entry_offset = snapshot_header_len + 2 * snapshot_counter_len;
    const phase_tag_offset = first_entry_offset + 233;
    var unknown_phase = try std.testing.allocator.dupe(u8, encoded);
    defer std.testing.allocator.free(unknown_phase);
    unknown_phase[phase_tag_offset] = 0xFF;
    refreshDigest(unknown_phase);
    try std.testing.expectError(
        error.InvalidEnum,
        decodeInto(4, 2, &decoded, std.testing.allocator, unknown_phase, testBinding(), &floors),
    );

    var duplicate = try std.testing.allocator.dupe(u8, encoded);
    defer std.testing.allocator.free(duplicate);
    @memcpy(
        duplicate[first_entry_offset + snapshot_entry_len ..][0..snapshot_entry_len],
        duplicate[first_entry_offset..][0..snapshot_entry_len],
    );
    refreshDigest(duplicate);
    try std.testing.expectError(
        error.InvalidState,
        decodeInto(4, 2, &decoded, std.testing.allocator, duplicate, testBinding(), &floors),
    );
}

test "claim is durable and reopen maps external in flight to lookup" {
    const Store = DurableStore(4, 2);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const floors = testFloors();
    const request = testRequest(2, .deposit, 101);

    {
        var store: Store = undefined;
        try Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "claim.fsg1", .initialize, testBinding(), &floors, &floors);
        defer store.deinit();
        _ = try store.accept(request, testRoute(101));
        const claim = (try store.claimNext()).?;
        switch (claim.action) {
            .remote_submit => {},
            else => return error.ExpectedRemoteSubmit,
        }
    }

    {
        var store: Store = undefined;
        try Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "claim.fsg1", .open_existing, testBinding(), &floors, &floors);
        defer store.deinit();
        const recovered = (try store.claimNext()).?;
        switch (recovered.action) {
            .remote_lookup => {},
            else => return error.ExpectedRemoteLookup,
        }
    }
}

test "conflict reconciliation cause survives codec and repeated crash" {
    const Store = DurableStore(4, 2);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const floors = testFloors();

    {
        var store: Store = undefined;
        try Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "conflict.fsg1", .initialize, testBinding(), &floors, &floors);
        defer store.deinit();
        _ = try store.accept(testRequest(40, .withdrawal, 101), testRoute(101));
        const debit = (try store.claimNext()).?;
        _ = try store.complete(debit, .{ .core_adjust = .applied });
        const submit = (try store.claimNext()).?;
        _ = try store.complete(submit, .{ .remote_submit = .conflict });
    }

    var applied = floors;
    applied[0].last_transaction_id = 11;
    {
        var store: Store = undefined;
        try Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "conflict.fsg1", .open_existing, testBinding(), &floors, &applied);
        defer store.deinit();
        const lookup = (try store.claimNext()).?;
        try std.testing.expectEqual(
            saga.ReconciliationCause.conflict_response,
            lookup.action.remote_lookup.cause,
        );
    }

    {
        var store: Store = undefined;
        try Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "conflict.fsg1", .open_existing, testBinding(), &floors, &applied);
        defer store.deinit();
        const replay = (try store.claimNext()).?;
        try std.testing.expectEqual(
            saga.ReconciliationCause.conflict_response,
            replay.action.remote_lookup.cause,
        );
        const quarantined = try store.complete(replay, .{ .remote_lookup = .absent });
        try std.testing.expectEqual(saga.PublicState.quarantined, quarantined.state);
        try std.testing.expect((try store.claimNext()) == null);
    }
}

test "reopen replays a durable local in flight adjustment" {
    const Store = DurableStore(4, 2);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const floors = testFloors();
    const request = testRequest(3, .withdrawal, 101);

    {
        var store: Store = undefined;
        try Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "local.fsg1", .initialize, testBinding(), &floors, &floors);
        defer store.deinit();
        _ = try store.accept(request, testRoute(101));
        const claim = (try store.claimNext()).?;
        switch (claim.action) {
            .core_adjust => |adjustment| {
                try std.testing.expectEqual(saga.CorePurpose.withdrawal_debit, adjustment.purpose);
                try std.testing.expectEqual(@as(i64, 11), adjustment.transaction_id);
            },
            else => return error.ExpectedCoreAdjustment,
        }
    }

    {
        var advanced_floors = floors;
        advanced_floors[0].last_transaction_id = 13;
        var rejected: Store = undefined;
        try std.testing.expectError(
            error.CoreCounterMismatch,
            Store.openInto(&rejected, std.testing.allocator, std.testing.io, tmp.dir, "local.fsg1", .open_existing, testBinding(), &floors, &advanced_floors),
        );
    }

    {
        var applied = floors;
        applied[0].last_transaction_id = 11;
        var store: Store = undefined;
        try Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "local.fsg1", .open_existing, testBinding(), &floors, &applied);
        defer store.deinit();
        try std.testing.expectEqual(@as(i64, 11), store.ledger.counterAt(0).last_confirmed);
        const replay = (try store.claimNext()).?;
        switch (replay.action) {
            .core_adjust => |adjustment| try std.testing.expectEqual(@as(i64, 11), adjustment.transaction_id),
            else => return error.ExpectedCoreReplay,
        }
        try std.testing.expect(store.ledger.snapshot_generation >= 3);
    }

    {
        var applied = floors;
        applied[0].last_transaction_id = 11;
        var store: Store = undefined;
        try Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "local.fsg1", .open_existing, testBinding(), &floors, &applied);
        defer store.deinit();
        const replay = (try store.claimNext()).?;
        switch (replay.action) {
            .core_adjust => |adjustment| try std.testing.expectEqual(@as(i64, 11), adjustment.transaction_id),
            else => return error.ExpectedCoreReplay,
        }
    }
}

test "reopen rejects rollback behind the last confirmed core adjustment" {
    const Store = DurableStore(4, 2);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const floors = testFloors();

    {
        var store: Store = undefined;
        try Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "rollback.fsg1", .initialize, testBinding(), &floors, &floors);
        defer store.deinit();
        _ = try store.accept(testRequest(4, .deposit, 101), testRoute(101));
        const remote = (try store.claimNext()).?;
        _ = try store.complete(remote, .{ .remote_submit = .applied });
        const local = (try store.claimNext()).?;
        _ = try store.complete(local, .{ .core_adjust = .applied });
        try std.testing.expectEqual(@as(i64, 11), store.ledger.counterAt(0).last_confirmed);
    }

    var rejected: Store = undefined;
    try std.testing.expectError(
        error.CoreCounterRollback,
        Store.openInto(&rejected, std.testing.allocator, std.testing.io, tmp.dir, "rollback.fsg1", .open_existing, testBinding(), &floors, &floors),
    );
}

test "snapshot rejects identity drift corruption and authenticated trailing bytes" {
    const Store = DurableStore(4, 2);
    const Ledger = saga.Ledger(4, 2);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const floors = testFloors();

    {
        var store: Store = undefined;
        try Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "strict.fsg1", .initialize, testBinding(), &floors, &floors);
        store.deinit();
    }

    var wrong_binding = testBinding();
    wrong_binding.profile_digest[0] ^= 1;
    var rejected: Store = undefined;
    try std.testing.expectError(
        error.ProfileDigestMismatch,
        Store.openInto(&rejected, std.testing.allocator, std.testing.io, tmp.dir, "strict.fsg1", .open_existing, wrong_binding, &floors, &floors),
    );

    const original_file = try tmp.dir.openFile(std.testing.io, "strict.fsg1", secureOpenOptions(.read_only));
    const original = try readToEndAlloc(std.testing.io, std.testing.allocator, original_file, 4096);
    original_file.close(std.testing.io);
    defer std.testing.allocator.free(original);

    var corrupted = try std.testing.allocator.dupe(u8, original);
    defer std.testing.allocator.free(corrupted);
    corrupted[snapshot_header_len] ^= 1;
    try rewriteTestFile(tmp.dir, "strict.fsg1", corrupted);
    try std.testing.expectError(
        error.DigestMismatch,
        Store.openInto(&rejected, std.testing.allocator, std.testing.io, tmp.dir, "strict.fsg1", .open_existing, testBinding(), &floors, &floors),
    );

    var authenticated_trailing = try std.testing.allocator.alloc(u8, original.len + 1);
    defer std.testing.allocator.free(authenticated_trailing);
    const old_digest_offset = original.len - snapshot_digest_len;
    @memcpy(authenticated_trailing[0..old_digest_offset], original[0..old_digest_offset]);
    authenticated_trailing[old_digest_offset] = 0xCC;
    const digest = integrityDigest(authenticated_trailing[0 .. authenticated_trailing.len - snapshot_digest_len]);
    @memcpy(authenticated_trailing[authenticated_trailing.len - snapshot_digest_len ..], &digest);
    var decoded: Ledger = undefined;
    try std.testing.expectError(
        error.TrailingBytes,
        decodeInto(4, 2, &decoded, std.testing.allocator, authenticated_trailing, testBinding(), &floors),
    );
}

test "exclusive lock rejects a second store handle" {
    const Store = DurableStore(2, 2);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const floors = testFloors();
    var first: Store = undefined;
    try Store.openInto(&first, std.testing.allocator, std.testing.io, tmp.dir, "locked.fsg1", .initialize, testBinding(), &floors, &floors);
    defer first.deinit();
    var second: Store = undefined;
    try std.testing.expectError(
        error.AlreadyOpen,
        Store.openInto(&second, std.testing.allocator, std.testing.io, tmp.dir, "locked.fsg1", .open_existing, testBinding(), &floors, &floors),
    );
}

test "ambiguous writes poison and reopen observes only old or new state" {
    const Store = DurableStore(4, 2);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const floors = testFloors();

    {
        var store: Store = undefined;
        try Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "fault.fsg1", .initialize, testBinding(), &floors, &floors);
        defer store.deinit();
        store.injectWriteFailureForTesting();
        try std.testing.expectError(
            error.StorageAmbiguous,
            store.accept(testRequest(10, .deposit, 101), testRoute(101)),
        );
        try std.testing.expect(store.isPoisoned());
        try std.testing.expectError(error.Poisoned, store.lookup(10));
    }
    {
        var store: Store = undefined;
        try Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "fault.fsg1", .open_existing, testBinding(), &floors, &floors);
        defer store.deinit();
        try std.testing.expectError(error.NotFound, store.lookup(10));
        store.injectPostReplaceFailureForTesting();
        try std.testing.expectError(
            error.StorageAmbiguous,
            store.accept(testRequest(11, .deposit, 101), testRoute(101)),
        );
        try std.testing.expect(store.isPoisoned());
    }
    {
        var store: Store = undefined;
        try Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "fault.fsg1", .open_existing, testBinding(), &floors, &floors);
        defer store.deinit();
        try std.testing.expectEqual(saga.PublicState.accepted, (try store.lookup(11)).state);
    }
}

test "terminal operations are retained and consume bounded capacity" {
    const Store = DurableStore(1, 1);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const floor = [_]saga.LocalIdFloor{.{ .uid = 101, .last_transaction_id = 10 }};
    var store: Store = undefined;
    try Store.openInto(&store, std.testing.allocator, std.testing.io, tmp.dir, "capacity.fsg1", .initialize, testBinding(), &floor, &floor);
    defer store.deinit();

    _ = try store.accept(testRequest(20, .deposit, 101), testRoute(101));
    const remote = (try store.claimNext()).?;
    _ = try store.complete(remote, .{ .remote_submit = .applied });
    const local = (try store.claimNext()).?;
    const completed = try store.complete(local, .{ .core_adjust = .applied });
    try std.testing.expectEqual(saga.PublicState.completed, completed.state);
    try std.testing.expect((try store.claimNext()) == null);
    try std.testing.expectError(
        error.CapacityExceeded,
        store.accept(testRequest(21, .deposit, 101), testRoute(101)),
    );
    try std.testing.expectEqual(saga.PublicState.completed, (try store.lookup(20)).state);
}
