const std = @import("std");
const exchange_core = @import("exchange_core");
const saga = @import("parix_clearing_saga.zig");

const persistence_paths = exchange_core.core.persistence_paths;

const magic = "CSG1";
const version: u16 = 1;
const flags: u16 = 0;
const digest_len: usize = 32;
const header_len: usize = 112;
const authorization_len: usize = 73;
const route_len: usize = 54;
const transfer_len: usize = 128;
const resolution_len: usize = 70;
const phase_len: usize = 3;
const entry_len: usize = authorization_len + route_len + transfer_len +
    1 + resolution_len + 1 + transfer_len + phase_len + 8 + 4 + 4;
const integrity_domain = "exchange-core/parix-clearing-snapshot/v1";

comptime {
    std.debug.assert(entry_len == 474);
}

pub const CodecError = error{
    InvalidPath,
    Truncated,
    TrailingBytes,
    MagicMismatch,
    UnsupportedVersion,
    InvalidFlags,
    CapacityMismatch,
    DestinationMismatch,
    SettlementProfileMismatch,
    ClearingProfileMismatch,
    ProviderMismatch,
    EntryCountExceeded,
    DigestMismatch,
    InvalidEnum,
    InvalidOptional,
    InvalidState,
};

pub const StoreError = saga.LedgerError || CodecError || std.mem.Allocator.Error || error{
    AlreadyOpen,
    AlreadyInitialized,
    NotInitialized,
    Poisoned,
    InsecurePermissions,
    StorageReadFailed,
    StorageAmbiguous,
};

pub const default_path = "clearing-sagas.csg1";

/// Strict CSG1 wrapper. The runtime must serialize calls; the store keeps two
/// heap-owned ledgers so production-capacity candidate commits never copy a
/// multi-megabyte ledger through the thread stack.
pub fn DurableStore(comptime capacity: usize) type {
    const Core = saga.Ledger(capacity);

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        io: std.Io,
        dir: std.Io.Dir,
        path: []u8,
        lock_path: []u8,
        lock_file: std.Io.File,
        ledger: *Core,
        candidate: *Core,
        poisoned: bool = false,
        fail_next_write_for_testing: bool = false,
        fail_next_post_replace_for_testing: bool = false,

        pub fn initialize(
            out: *Self,
            allocator: std.mem.Allocator,
            io: std.Io,
            dir: std.Io.Dir,
            path: []const u8,
            binding: saga.Binding,
        ) StoreError!void {
            try prepareHandle(out, allocator, io, dir, path);
            errdefer releasePrepared(out);
            binding.validate() catch return error.InvalidBinding;

            const existing = dir.openFile(io, path, secureOpenOptions(.read_only)) catch |err| switch (err) {
                error.FileNotFound => null,
                else => return error.StorageReadFailed,
            };
            if (existing) |file| {
                file.close(io);
                return error.AlreadyInitialized;
            }
            try Core.initInto(out.ledger, binding);
            const bytes = try encodeAlloc(capacity, allocator, out.ledger);
            defer allocator.free(bytes);
            writeBytesAtomic(io, dir, path, bytes, false, false) catch
                return error.StorageAmbiguous;
        }

        pub fn openExisting(
            out: *Self,
            allocator: std.mem.Allocator,
            io: std.Io,
            dir: std.Io.Dir,
            path: []const u8,
            binding: saga.Binding,
        ) StoreError!void {
            try prepareHandle(out, allocator, io, dir, path);
            errdefer releasePrepared(out);
            binding.validate() catch return error.InvalidBinding;

            const file = dir.openFile(io, path, secureOpenOptions(.read_only)) catch |err| switch (err) {
                error.FileNotFound => return error.NotInitialized,
                else => return error.StorageReadFailed,
            };
            defer file.close(io);
            try requirePrivateRegularFile(io, file);
            const max_size = maxEncodedSize(capacity);
            const bytes = readToEndAlloc(io, allocator, file, max_size + 1) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.StorageReadFailed,
            };
            defer allocator.free(bytes);
            if (bytes.len > max_size) return error.TrailingBytes;
            try decodeInto(capacity, out.ledger, allocator, bytes, binding);

            const before = out.ledger.snapshot_generation;
            _ = try out.ledger.recover();
            if (out.ledger.snapshot_generation != before) {
                const recovered = try encodeAlloc(capacity, allocator, out.ledger);
                defer allocator.free(recovered);
                writeBytesAtomic(io, dir, path, recovered, true, false) catch
                    return error.StorageAmbiguous;
            }
        }

        pub fn deinit(self: *Self) void {
            self.lock_file.close(self.io);
            self.allocator.destroy(self.candidate);
            self.allocator.destroy(self.ledger);
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

        pub fn injectPostReplaceFailureForTesting(self: *Self) void {
            self.fail_next_post_replace_for_testing = true;
        }

        pub fn acceptAuthorization(
            self: *Self,
            authorization: saga.Authorization,
            route: saga.Route,
        ) StoreError!saga.AcceptResult {
            try self.ensureHealthy();
            self.copyCandidate();
            const result = try self.candidate.acceptAuthorization(authorization, route);
            if (result.disposition == .inserted) try self.commitCandidate();
            return result;
        }

        pub fn acceptResolution(
            self: *Self,
            resolution: saga.Resolution,
        ) StoreError!saga.AcceptResult {
            try self.ensureHealthy();
            self.copyCandidate();
            const result = try self.candidate.acceptResolution(resolution);
            if (result.disposition == .inserted) try self.commitCandidate();
            return result;
        }

        pub fn lookup(self: *const Self, payment_id: u128) StoreError!saga.Status {
            try self.ensureHealthy();
            return self.ledger.lookup(payment_id);
        }

        pub fn summary(self: *const Self) StoreError!saga.Summary {
            try self.ensureHealthy();
            return self.ledger.summary();
        }

        pub fn claimNext(self: *Self) StoreError!?saga.Claim {
            try self.ensureHealthy();
            self.copyCandidate();
            const claim = try self.candidate.claimNext() orelse return null;
            try self.commitCandidate();
            return claim;
        }

        pub fn complete(
            self: *Self,
            claim: saga.Claim,
            completion: saga.Completion,
        ) StoreError!saga.Status {
            try self.ensureHealthy();
            self.copyCandidate();
            const status = try self.candidate.complete(claim, completion);
            try self.commitCandidate();
            return status;
        }

        fn prepareHandle(
            out: *Self,
            allocator: std.mem.Allocator,
            io: std.Io,
            dir: std.Io.Dir,
            path: []const u8,
        ) StoreError!void {
            try validatePath(path);
            const path_copy = try allocator.dupe(u8, path);
            errdefer allocator.free(path_copy);
            const lock_path = try std.fmt.allocPrint(allocator, "{s}.lock", .{path});
            errdefer allocator.free(lock_path);
            const lock_file = try acquireLock(io, dir, lock_path);
            errdefer lock_file.close(io);
            const ledger = try allocator.create(Core);
            errdefer allocator.destroy(ledger);
            const candidate = try allocator.create(Core);
            errdefer allocator.destroy(candidate);
            out.* = .{
                .allocator = allocator,
                .io = io,
                .dir = dir,
                .path = path_copy,
                .lock_path = lock_path,
                .lock_file = lock_file,
                .ledger = ledger,
                .candidate = candidate,
            };
        }

        fn releasePrepared(self: *Self) void {
            self.lock_file.close(self.io);
            self.allocator.destroy(self.candidate);
            self.allocator.destroy(self.ledger);
            self.allocator.free(self.lock_path);
            self.allocator.free(self.path);
            self.* = undefined;
        }

        fn ensureHealthy(self: *const Self) StoreError!void {
            if (self.poisoned) return error.Poisoned;
        }

        fn copyCandidate(self: *Self) void {
            self.candidate.* = self.ledger.*;
        }

        fn commitCandidate(self: *Self) StoreError!void {
            const bytes = try encodeAlloc(capacity, self.allocator, self.candidate);
            defer self.allocator.free(bytes);
            if (self.fail_next_write_for_testing) {
                self.fail_next_write_for_testing = false;
                self.poisoned = true;
                return error.StorageAmbiguous;
            }
            const fail_after_replace = self.fail_next_post_replace_for_testing;
            self.fail_next_post_replace_for_testing = false;
            writeBytesAtomic(self.io, self.dir, self.path, bytes, true, fail_after_replace) catch {
                self.poisoned = true;
                return error.StorageAmbiguous;
            };
            const old = self.ledger;
            self.ledger = self.candidate;
            self.candidate = old;
        }
    };
}

pub const DefaultStore = DurableStore(4_096);

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
    const created = dir.createFile(io, path, .{
        .read = true,
        .truncate = false,
        .exclusive = true,
        .lock = .exclusive,
        .lock_nonblocking = true,
        .permissions = .fromMode(0o600),
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

fn maxEncodedSize(comptime capacity: usize) usize {
    return header_len + entry_len * capacity + digest_len;
}

fn actualEncodedSize(entry_count: usize) usize {
    return header_len + entry_len * entry_count + digest_len;
}

fn encodeAlloc(
    comptime capacity: usize,
    allocator: std.mem.Allocator,
    ledger: *const saga.Ledger(capacity),
) (std.mem.Allocator.Error || CodecError)![]u8 {
    ledger.validate() catch return error.InvalidState;
    const total = actualEncodedSize(ledger.entriesSlice().len);
    const bytes = try allocator.alloc(u8, total);
    errdefer allocator.free(bytes);
    var index: usize = 0;
    @memcpy(bytes[index..][0..magic.len], magic);
    index += magic.len;
    putInt(u16, bytes, &index, version);
    putInt(u16, bytes, &index, flags);
    putInt(u32, bytes, &index, @intCast(capacity));
    putInt(u128, bytes, &index, @intFromEnum(ledger.binding.destination_id));
    @memcpy(bytes[index..][0..32], &ledger.binding.settlement_profile_digest);
    index += 32;
    @memcpy(bytes[index..][0..32], &ledger.binding.clearing_profile_digest);
    index += 32;
    putInt(u64, bytes, &index, ledger.binding.provider_id);
    putInt(u64, bytes, &index, ledger.snapshot_generation);
    putInt(u32, bytes, &index, @intCast(ledger.entriesSlice().len));
    std.debug.assert(index == header_len);
    for (ledger.entriesSlice()) |entry| encodeEntry(bytes, &index, entry);
    const digest_offset = total - digest_len;
    std.debug.assert(index == digest_offset);
    const digest = integrityDigest(bytes[0..digest_offset]);
    @memcpy(bytes[digest_offset..], &digest);
    return bytes;
}

fn decodeInto(
    comptime capacity: usize,
    out: *saga.Ledger(capacity),
    allocator: std.mem.Allocator,
    bytes: []const u8,
    binding: saga.Binding,
) StoreError!void {
    if (bytes.len < header_len + digest_len) return error.Truncated;
    const digest_offset = bytes.len - digest_len;
    const wanted = integrityDigest(bytes[0..digest_offset]);
    if (!std.mem.eql(u8, &wanted, bytes[digest_offset..])) return error.DigestMismatch;
    var index: usize = 0;
    if (!std.mem.eql(u8, bytes[0..magic.len], magic)) return error.MagicMismatch;
    index += magic.len;
    if (try takeInt(u16, bytes, &index, digest_offset) != version) return error.UnsupportedVersion;
    if (try takeInt(u16, bytes, &index, digest_offset) != flags) return error.InvalidFlags;
    if (try takeInt(u32, bytes, &index, digest_offset) != capacity) return error.CapacityMismatch;
    if (try takeInt(u128, bytes, &index, digest_offset) != @intFromEnum(binding.destination_id)) return error.DestinationMismatch;
    if (!std.mem.eql(u8, bytes[index..][0..32], &binding.settlement_profile_digest)) return error.SettlementProfileMismatch;
    index += 32;
    if (!std.mem.eql(u8, bytes[index..][0..32], &binding.clearing_profile_digest)) return error.ClearingProfileMismatch;
    index += 32;
    if (try takeInt(u64, bytes, &index, digest_offset) != binding.provider_id) return error.ProviderMismatch;
    const snapshot_generation = try takeInt(u64, bytes, &index, digest_offset);
    const entry_count = try takeInt(u32, bytes, &index, digest_offset);
    if (entry_count > capacity) return error.EntryCountExceeded;
    const expected_len = actualEncodedSize(entry_count);
    if (bytes.len < expected_len) return error.Truncated;
    if (bytes.len > expected_len) return error.TrailingBytes;

    const entries = try allocator.alloc(saga.Entry, entry_count);
    defer allocator.free(entries);
    for (entries) |*entry| entry.* = try decodeEntry(bytes, &index, digest_offset);
    if (index != digest_offset) return error.TrailingBytes;
    saga.Ledger(capacity).restoreInto(out, binding, entries, snapshot_generation) catch
        return error.InvalidState;
}

fn encodeEntry(bytes: []u8, index: *usize, entry: saga.Entry) void {
    const start = index.*;
    encodeAuthorization(bytes, index, entry.authorization);
    encodeRoute(bytes, index, entry.route);
    encodeTransfer(bytes, index, entry.pending_transfer);
    if (entry.resolution) |resolution| {
        putInt(u8, bytes, index, 1);
        encodeResolution(bytes, index, resolution);
    } else {
        putInt(u8, bytes, index, 0);
        @memset(bytes[index.*..][0..resolution_len], 0);
        index.* += resolution_len;
    }
    if (entry.resolution_transfer) |transfer| {
        putInt(u8, bytes, index, 1);
        encodeTransfer(bytes, index, transfer);
    } else {
        putInt(u8, bytes, index, 0);
        @memset(bytes[index.*..][0..transfer_len], 0);
        index.* += transfer_len;
    }
    const phase = encodePhase(entry.phase);
    putInt(u8, bytes, index, phase.tag);
    putInt(u8, bytes, index, phase.a);
    putInt(u8, bytes, index, phase.b);
    putInt(u64, bytes, index, entry.generation);
    putInt(u32, bytes, index, entry.remote_attempts);
    putInt(u32, bytes, index, entry.funding_attempts);
    std.debug.assert(index.* - start == entry_len);
}

fn decodeEntry(bytes: []const u8, index: *usize, limit: usize) CodecError!saga.Entry {
    const authorization = try decodeAuthorization(bytes, index, limit);
    const route = try decodeRoute(bytes, index, limit);
    const pending_transfer = try decodeTransfer(bytes, index, limit);
    const resolution_present = try takeInt(u8, bytes, index, limit);
    const resolution: ?saga.Resolution = switch (resolution_present) {
        0 => blk: {
            try requireZero(bytes, index.*, resolution_len, limit);
            index.* += resolution_len;
            break :blk null;
        },
        1 => try decodeResolution(bytes, index, limit),
        else => return error.InvalidOptional,
    };
    const transfer_present = try takeInt(u8, bytes, index, limit);
    const resolution_transfer: ?saga.Transfer = switch (transfer_present) {
        0 => blk: {
            try requireZero(bytes, index.*, transfer_len, limit);
            index.* += transfer_len;
            break :blk null;
        },
        1 => try decodeTransfer(bytes, index, limit),
        else => return error.InvalidOptional,
    };
    return .{
        .authorization = authorization,
        .route = route,
        .pending_transfer = pending_transfer,
        .resolution = resolution,
        .resolution_transfer = resolution_transfer,
        .phase = try decodePhase(
            try takeInt(u8, bytes, index, limit),
            try takeInt(u8, bytes, index, limit),
            try takeInt(u8, bytes, index, limit),
        ),
        .generation = try takeInt(u64, bytes, index, limit),
        .remote_attempts = try takeInt(u32, bytes, index, limit),
        .funding_attempts = try takeInt(u32, bytes, index, limit),
    };
}

fn encodeAuthorization(bytes: []u8, index: *usize, value: saga.Authorization) void {
    putInt(u128, bytes, index, value.event_id);
    putInt(u128, bytes, index, value.payment_id);
    putInt(u64, bytes, index, value.provider_id);
    putInt(u64, bytes, index, value.revision);
    putInt(u8, bytes, index, @intFromEnum(value.direction));
    putInt(i64, bytes, index, value.uid);
    putInt(i32, bytes, index, value.currency);
    putInt(i64, bytes, index, value.amount);
    putInt(u32, bytes, index, value.timeout_seconds);
}

fn decodeAuthorization(bytes: []const u8, index: *usize, limit: usize) CodecError!saga.Authorization {
    return .{
        .event_id = try takeInt(u128, bytes, index, limit),
        .payment_id = try takeInt(u128, bytes, index, limit),
        .provider_id = try takeInt(u64, bytes, index, limit),
        .revision = try takeInt(u64, bytes, index, limit),
        .direction = std.enums.fromInt(saga.Direction, try takeInt(u8, bytes, index, limit)) orelse return error.InvalidEnum,
        .uid = try takeInt(i64, bytes, index, limit),
        .currency = try takeInt(i32, bytes, index, limit),
        .amount = try takeInt(i64, bytes, index, limit),
        .timeout_seconds = try takeInt(u32, bytes, index, limit),
    };
}

fn encodeRoute(bytes: []u8, index: *usize, value: saga.Route) void {
    putInt(u128, bytes, index, @intFromEnum(value.destination_id));
    putInt(u32, bytes, index, value.ledger);
    putInt(u128, bytes, index, value.treasury_account_id);
    putInt(u128, bytes, index, value.provider_control_account_id);
    putInt(u16, bytes, index, value.transfer_code);
}

fn decodeRoute(bytes: []const u8, index: *usize, limit: usize) CodecError!saga.Route {
    return .{
        .destination_id = @enumFromInt(try takeInt(u128, bytes, index, limit)),
        .ledger = try takeInt(u32, bytes, index, limit),
        .treasury_account_id = try takeInt(u128, bytes, index, limit),
        .provider_control_account_id = try takeInt(u128, bytes, index, limit),
        .transfer_code = try takeInt(u16, bytes, index, limit),
    };
}

fn encodeResolution(bytes: []u8, index: *usize, value: saga.Resolution) void {
    putInt(u128, bytes, index, value.event_id);
    putInt(u128, bytes, index, value.payment_id);
    putInt(u64, bytes, index, value.provider_id);
    putInt(u64, bytes, index, value.revision);
    putInt(u8, bytes, index, @intFromEnum(value.kind));
    putInt(u8, bytes, index, @intFromEnum(value.direction));
    putInt(i64, bytes, index, value.uid);
    putInt(i32, bytes, index, value.currency);
    putInt(i64, bytes, index, value.amount);
}

fn decodeResolution(bytes: []const u8, index: *usize, limit: usize) CodecError!saga.Resolution {
    return .{
        .event_id = try takeInt(u128, bytes, index, limit),
        .payment_id = try takeInt(u128, bytes, index, limit),
        .provider_id = try takeInt(u64, bytes, index, limit),
        .revision = try takeInt(u64, bytes, index, limit),
        .kind = std.enums.fromInt(saga.ResolutionKind, try takeInt(u8, bytes, index, limit)) orelse return error.InvalidEnum,
        .direction = std.enums.fromInt(saga.Direction, try takeInt(u8, bytes, index, limit)) orelse return error.InvalidEnum,
        .uid = try takeInt(i64, bytes, index, limit),
        .currency = try takeInt(i32, bytes, index, limit),
        .amount = try takeInt(i64, bytes, index, limit),
    };
}

const EncodedPhase = struct { tag: u8, a: u8 = 0, b: u8 = 0 };

fn encodePhase(value: saga.Phase) EncodedPhase {
    return switch (value) {
        .accepted => .{ .tag = 1 },
        .funding_submit_ready => |purpose| .{ .tag = 2, .a = @intFromEnum(purpose) },
        .funding_submit_in_flight => |purpose| .{ .tag = 3, .a = @intFromEnum(purpose) },
        .funding_status_ready => |status_phase| .{
            .tag = 4,
            .a = @intFromEnum(status_phase.purpose),
            .b = @intFromEnum(status_phase.cause),
        },
        .funding_status_in_flight => |status_phase| .{
            .tag = 5,
            .a = @intFromEnum(status_phase.purpose),
            .b = @intFromEnum(status_phase.cause),
        },
        .transfer_submit_ready => |stage| .{ .tag = 6, .a = @intFromEnum(stage) },
        .transfer_submit_in_flight => |stage| .{ .tag = 7, .a = @intFromEnum(stage) },
        .transfer_lookup_ready => |lookup| .{ .tag = 8, .a = @intFromEnum(lookup.stage), .b = @intFromEnum(lookup.cause) },
        .transfer_lookup_in_flight => |lookup| .{ .tag = 9, .a = @intFromEnum(lookup.stage), .b = @intFromEnum(lookup.cause) },
        .authorized => .{ .tag = 10 },
        .completed => |kind| .{ .tag = 11, .a = @intFromEnum(kind) },
        .failed => |reason| .{ .tag = 12, .a = @intFromEnum(reason) },
        .quarantined => |reason| .{ .tag = 13, .a = @intFromEnum(reason) },
    };
}

fn decodePhase(tag: u8, a: u8, b: u8) CodecError!saga.Phase {
    return switch (tag) {
        1 => if (a == 0 and b == 0) .accepted else error.InvalidEnum,
        2 => if (b == 0) .{ .funding_submit_ready = try fundingPurpose(a) } else error.InvalidEnum,
        3 => if (b == 0) .{ .funding_submit_in_flight = try fundingPurpose(a) } else error.InvalidEnum,
        4 => .{ .funding_status_ready = .{
            .purpose = try fundingPurpose(a),
            .cause = try fundingReconciliationCause(b),
        } },
        5 => .{ .funding_status_in_flight = .{
            .purpose = try fundingPurpose(a),
            .cause = try fundingReconciliationCause(b),
        } },
        6 => if (b == 0) .{ .transfer_submit_ready = try transferStage(a) } else error.InvalidEnum,
        7 => if (b == 0) .{ .transfer_submit_in_flight = try transferStage(a) } else error.InvalidEnum,
        8 => .{ .transfer_lookup_ready = .{ .stage = try transferStage(a), .cause = try reconciliationCause(b) } },
        9 => .{ .transfer_lookup_in_flight = .{ .stage = try transferStage(a), .cause = try reconciliationCause(b) } },
        10 => if (a == 0 and b == 0) .authorized else error.InvalidEnum,
        11 => if (b == 0) .{ .completed = std.enums.fromInt(saga.ResolutionKind, a) orelse return error.InvalidEnum } else error.InvalidEnum,
        12 => if (b == 0) .{ .failed = std.enums.fromInt(saga.FailureReason, a) orelse return error.InvalidEnum } else error.InvalidEnum,
        13 => if (b == 0) .{ .quarantined = std.enums.fromInt(saga.QuarantineReason, a) orelse return error.InvalidEnum } else error.InvalidEnum,
        else => error.InvalidEnum,
    };
}

fn fundingPurpose(raw: u8) CodecError!saga.FundingPurpose {
    return std.enums.fromInt(saga.FundingPurpose, raw) orelse error.InvalidEnum;
}

fn fundingReconciliationCause(raw: u8) CodecError!saga.FundingReconciliationCause {
    return std.enums.fromInt(saga.FundingReconciliationCause, raw) orelse error.InvalidEnum;
}

fn transferStage(raw: u8) CodecError!saga.TransferStage {
    return std.enums.fromInt(saga.TransferStage, raw) orelse error.InvalidEnum;
}

fn reconciliationCause(raw: u8) CodecError!saga.ReconciliationCause {
    return std.enums.fromInt(saga.ReconciliationCause, raw) orelse error.InvalidEnum;
}

fn encodeTransfer(bytes: []u8, index: *usize, transfer: saga.Transfer) void {
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

fn requireZero(bytes: []const u8, offset: usize, count: usize, limit: usize) CodecError!void {
    if (offset > limit or limit - offset < count) return error.Truncated;
    for (bytes[offset..][0..count]) |byte| if (byte != 0) return error.InvalidOptional;
}

fn putInt(comptime T: type, bytes: []u8, index: *usize, value: T) void {
    std.mem.writeInt(T, bytes[index.*..][0..@sizeOf(T)], value, .little);
    index.* += @sizeOf(T);
}

fn takeInt(comptime T: type, bytes: []const u8, index: *usize, limit: usize) CodecError!T {
    if (index.* > limit or limit - index.* < @sizeOf(T)) return error.Truncated;
    const value = std.mem.readInt(T, bytes[index.*..][0..@sizeOf(T)], .little);
    index.* += @sizeOf(T);
    return value;
}

fn integrityDigest(bytes: []const u8) [digest_len]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(integrity_domain);
    hasher.update(bytes);
    var digest: [digest_len]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn readToEndAlloc(io: std.Io, allocator: std.mem.Allocator, file: std.Io.File, limit: usize) ![]u8 {
    const initial = try file.stat(io);
    if (initial.kind != .file) return error.NotAFile;
    if (initial.size > limit) return error.StreamTooLong;
    const size: usize = @intCast(initial.size);
    const bytes = try allocator.alloc(u8, size);
    errdefer allocator.free(bytes);
    const read_len = try file.readPositionalAll(io, bytes, 0);
    if (read_len != size) return error.UnexpectedEndOfStream;
    const final = try file.stat(io);
    if (final.inode != initial.inode or final.size != initial.size or
        final.kind != initial.kind or final.permissions != initial.permissions or
        final.nlink != initial.nlink or !std.meta.eql(final.mtime, initial.mtime) or
        !std.meta.eql(final.ctime, initial.ctime)) return error.FileChanged;
    return bytes;
}

fn writeBytesAtomic(
    io: std.Io,
    dir: std.Io.Dir,
    path: []const u8,
    bytes: []const u8,
    replace: bool,
    fail_after_replace: bool,
) !void {
    var atomic_file = try dir.createFileAtomic(io, path, .{
        .permissions = .fromMode(0o600),
        .replace = replace,
    });
    defer atomic_file.deinit(io);
    var buffer: [4096]u8 = undefined;
    var writer = atomic_file.file.writer(io, &buffer);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
    try atomic_file.file.sync(io);
    if (replace) try atomic_file.replace(io) else try atomic_file.link(io);
    if (fail_after_replace) return error.InjectedPostReplaceFailure;
    try persistence_paths.syncParentDirectory(io, dir, path);
}

fn testBinding() saga.Binding {
    return .{
        .destination_id = @enumFromInt(77),
        .settlement_profile_digest = [_]u8{0x11} ** 32,
        .clearing_profile_digest = [_]u8{0x22} ** 32,
        .provider_id = 91,
    };
}

fn testAuthorization(payment_id: u128, direction: saga.Direction) saga.Authorization {
    return .{
        .event_id = payment_id + 1_000,
        .payment_id = payment_id,
        .provider_id = testBinding().provider_id,
        .revision = 7,
        .direction = direction,
        .uid = 1_001,
        .currency = 11,
        .amount = 500,
        .timeout_seconds = 30,
    };
}

fn testRoute() saga.Route {
    return .{
        .destination_id = testBinding().destination_id,
        .ledger = 421,
        .treasury_account_id = 8_000,
        .provider_control_account_id = 8_001,
        .transfer_code = 55,
    };
}

fn testResolution(payment_id: u128, direction: saga.Direction) saga.Resolution {
    return .{
        .event_id = payment_id + 2_000,
        .payment_id = payment_id,
        .provider_id = testBinding().provider_id,
        .revision = 8,
        .kind = .post,
        .direction = direction,
        .uid = 1_001,
        .currency = 11,
        .amount = 500,
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

test "initialize exact duplicate and reopen preserve CSG1 state" {
    const Store = DurableStore(4);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const authorization = testAuthorization(10, .deposit);

    {
        var store: Store = undefined;
        try Store.initialize(&store, std.testing.allocator, std.testing.io, tmp.dir, "clearing.csg1", testBinding());
        defer store.deinit();
        if (comptime @hasDecl(std.Io.File.Permissions, "toMode")) {
            const stat = try tmp.dir.statFile(std.testing.io, "clearing.csg1", .{ .follow_symlinks = false });
            try std.testing.expectEqual(@as(u32, 0o600), stat.permissions.toMode() & 0o7777);
        }
        try std.testing.expectEqual(
            saga.AcceptDisposition.inserted,
            (try store.acceptAuthorization(authorization, testRoute())).disposition,
        );
        try std.testing.expectEqual(
            saga.AcceptDisposition.existing,
            (try store.acceptAuthorization(authorization, testRoute())).disposition,
        );
        try std.testing.expectEqual(@as(usize, 1), (try store.summary()).accepted);
    }

    {
        var store: Store = undefined;
        try Store.openExisting(&store, std.testing.allocator, std.testing.io, tmp.dir, "clearing.csg1", testBinding());
        defer store.deinit();
        try std.testing.expectEqual(saga.PublicState.accepted, (try store.lookup(10)).state);
        try std.testing.expectEqual(
            saga.AcceptDisposition.existing,
            (try store.acceptAuthorization(authorization, testRoute())).disposition,
        );
    }
}

test "reopen converts exposed transfer and funding submits to reconciliation" {
    const Store = DurableStore(4);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var store: Store = undefined;
        try Store.initialize(&store, std.testing.allocator, std.testing.io, tmp.dir, "remote.csg1", testBinding());
        defer store.deinit();
        _ = try store.acceptAuthorization(testAuthorization(20, .deposit), testRoute());
        const claim = (try store.claimNext()).?;
        try std.testing.expect(claim.action == .remote_submit);
    }
    {
        var store: Store = undefined;
        try Store.openExisting(&store, std.testing.allocator, std.testing.io, tmp.dir, "remote.csg1", testBinding());
        defer store.deinit();
        const claim = (try store.claimNext()).?;
        try std.testing.expect(claim.action == .remote_lookup);
        try std.testing.expectEqual(saga.TransferStage.authorization, claim.action.remote_lookup.stage);
    }

    {
        var store: Store = undefined;
        try Store.initialize(&store, std.testing.allocator, std.testing.io, tmp.dir, "funding.csg1", testBinding());
        defer store.deinit();
        _ = try store.acceptAuthorization(testAuthorization(21, .withdrawal), testRoute());
        const claim = (try store.claimNext()).?;
        try std.testing.expect(claim.action == .funding_submit);
    }
    {
        var store: Store = undefined;
        try Store.openExisting(&store, std.testing.allocator, std.testing.io, tmp.dir, "funding.csg1", testBinding());
        defer store.deinit();
        const claim = (try store.claimNext()).?;
        try std.testing.expect(claim.action == .funding_status);
        try std.testing.expectEqual(saga.FundingPurpose.withdrawal_debit, claim.action.funding_status.purpose);
        try std.testing.expectEqual(
            saga.FundingReconciliationCause.ambiguous_submission,
            claim.action.funding_status.cause,
        );
    }
}

test "codec rejects digest corruption identity drift and authenticated trailing bytes" {
    const Store = DurableStore(4);
    const Ledger = saga.Ledger(4);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        var store: Store = undefined;
        try Store.initialize(&store, std.testing.allocator, std.testing.io, tmp.dir, "strict.csg1", testBinding());
        store.deinit();
    }

    var wrong = testBinding();
    wrong.clearing_profile_digest[0] ^= 1;
    var rejected: Store = undefined;
    try std.testing.expectError(
        error.ClearingProfileMismatch,
        Store.openExisting(&rejected, std.testing.allocator, std.testing.io, tmp.dir, "strict.csg1", wrong),
    );

    const file = try tmp.dir.openFile(std.testing.io, "strict.csg1", secureOpenOptions(.read_only));
    const original = try readToEndAlloc(std.testing.io, std.testing.allocator, file, 4096);
    file.close(std.testing.io);
    defer std.testing.allocator.free(original);
    var corrupted = try std.testing.allocator.dupe(u8, original);
    defer std.testing.allocator.free(corrupted);
    corrupted[20] ^= 1;
    try rewriteTestFile(tmp.dir, "strict.csg1", corrupted);
    try std.testing.expectError(
        error.DigestMismatch,
        Store.openExisting(&rejected, std.testing.allocator, std.testing.io, tmp.dir, "strict.csg1", testBinding()),
    );

    var trailing = try std.testing.allocator.alloc(u8, original.len + 1);
    defer std.testing.allocator.free(trailing);
    const payload_len = original.len - digest_len;
    @memcpy(trailing[0..payload_len], original[0..payload_len]);
    trailing[payload_len] = 0xCC;
    const digest = integrityDigest(trailing[0 .. trailing.len - digest_len]);
    @memcpy(trailing[trailing.len - digest_len ..], &digest);
    var decoded: Ledger = undefined;
    try std.testing.expectError(
        error.TrailingBytes,
        decodeInto(4, &decoded, std.testing.allocator, trailing, testBinding()),
    );
}

test "exclusive lock rejects a second CSG1 handle" {
    const Store = DurableStore(2);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var first: Store = undefined;
    try Store.initialize(&first, std.testing.allocator, std.testing.io, tmp.dir, "locked.csg1", testBinding());
    defer first.deinit();
    var second: Store = undefined;
    try std.testing.expectError(
        error.AlreadyOpen,
        Store.openExisting(&second, std.testing.allocator, std.testing.io, tmp.dir, "locked.csg1", testBinding()),
    );
}

test "terminal entries are retained and bounded capacity fails closed" {
    const Store = DurableStore(1);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store: Store = undefined;
    try Store.initialize(&store, std.testing.allocator, std.testing.io, tmp.dir, "capacity.csg1", testBinding());
    defer store.deinit();
    _ = try store.acceptAuthorization(testAuthorization(30, .deposit), testRoute());
    const auth_claim = (try store.claimNext()).?;
    _ = try store.complete(auth_claim, .{ .remote_submit = .applied });
    _ = try store.acceptResolution(testResolution(30, .deposit));
    const resolution_claim = (try store.claimNext()).?;
    _ = try store.complete(resolution_claim, .{ .remote_submit = .applied });
    const funding_claim = (try store.claimNext()).?;
    const completed = try store.complete(funding_claim, .{ .funding_submit = .completed });
    try std.testing.expectEqual(saga.PublicState.completed, completed.state);
    try std.testing.expectError(
        error.CapacityExceeded,
        store.acceptAuthorization(testAuthorization(31, .deposit), testRoute()),
    );
    try std.testing.expectEqual(@as(usize, 1), (try store.summary()).completed);
}

test "ambiguous CSG1 writes poison and reopen observes old or new snapshot" {
    const Store = DurableStore(4);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        var store: Store = undefined;
        try Store.initialize(&store, std.testing.allocator, std.testing.io, tmp.dir, "fault.csg1", testBinding());
        defer store.deinit();
        store.injectWriteFailureForTesting();
        try std.testing.expectError(
            error.StorageAmbiguous,
            store.acceptAuthorization(testAuthorization(40, .deposit), testRoute()),
        );
        try std.testing.expect(store.isPoisoned());
        try std.testing.expectError(error.Poisoned, store.lookup(40));
    }
    {
        var store: Store = undefined;
        try Store.openExisting(&store, std.testing.allocator, std.testing.io, tmp.dir, "fault.csg1", testBinding());
        defer store.deinit();
        try std.testing.expectError(error.PaymentNotFound, store.lookup(40));
        store.injectPostReplaceFailureForTesting();
        try std.testing.expectError(
            error.StorageAmbiguous,
            store.acceptAuthorization(testAuthorization(41, .deposit), testRoute()),
        );
        try std.testing.expect(store.isPoisoned());
    }
    {
        var store: Store = undefined;
        try Store.openExisting(&store, std.testing.allocator, std.testing.io, tmp.dir, "fault.csg1", testBinding());
        defer store.deinit();
        try std.testing.expectEqual(saga.PublicState.accepted, (try store.lookup(41)).state);
    }
}

test "default capacity candidate commit stays off the thread stack" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store: DefaultStore = undefined;
    try DefaultStore.initialize(
        &store,
        std.testing.allocator,
        std.testing.io,
        tmp.dir,
        "default.csg1",
        testBinding(),
    );
    defer store.deinit();
    _ = try store.acceptAuthorization(testAuthorization(50, .deposit), testRoute());
    try std.testing.expectEqual(saga.PublicState.accepted, (try store.lookup(50)).state);
}
