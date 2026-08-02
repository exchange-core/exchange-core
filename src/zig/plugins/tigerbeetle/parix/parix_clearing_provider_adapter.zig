//! Parix host integration for the opt-in TigerBeetle plugin.
const std = @import("std");
const saga = @import("parix_clearing_saga.zig");

const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;

pub const profile_version: u16 = 1;
pub const max_profile_bytes: usize = 16 * 1024;
pub const max_body_bytes: usize = 16 * 1024;
pub const max_json_nesting: usize = 8;
pub const max_path_bytes: usize = 256;
pub const max_environment_name_bytes: usize = 64;
pub const max_replay_window_seconds: u32 = 86_400;

/// HMAC transcript for version 1. Each variable field is encoded as a four-byte
/// big-endian length followed by its exact bytes:
///
///   domain, provider ID, method, request target, canonical timestamp header,
///   raw body
///
/// The domain includes the protocol version, preventing cross-protocol use.
pub const transcript_domain = "exchange-core/parix-clearing-provider/hmac-sha256/v1";
pub const timestamp_header_name = "x-exchange-provider-timestamp";
pub const signature_header_name = "x-exchange-provider-signature";

pub const Signature = [HmacSha256.mac_length * 2]u8;

pub const SignError = std.mem.Allocator.Error || error{
    InvalidTimestamp,
    InvalidBody,
};

pub const RequestView = struct {
    method: []const u8,
    target: []const u8,
    content_type: []const u8,
    timestamp_header: []const u8,
    signature_header: []const u8,
    body: []const u8,
    received_unix_seconds: i64,
};

pub const VerifiedEvent = union(enum) {
    authorization: saga.Authorization,
    resolution: saga.Resolution,
};

pub const VerifyError = std.mem.Allocator.Error || error{
    InvalidRequest,
    AuthenticationFailed,
    ReplayRejected,
    MalformedEvent,
};

/// Type-erased verifier used by HTTP ingress.
pub const Adapter = struct {
    context: *const anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        verify: *const fn (*const anyopaque, RequestView) VerifyError!VerifiedEvent,
    };

    pub fn verify(self: Adapter, request: RequestView) VerifyError!VerifiedEvent {
        return self.vtable.verify(self.context, request);
    }
};

pub const ProfileParseError = std.mem.Allocator.Error || error{
    EmptyProfile,
    ProfileTooLarge,
    JsonNestingTooDeep,
    InvalidJson,
    InvalidRoot,
    InvalidObjectShape,
    MissingField,
    InvalidFieldType,
    InvalidInteger,
    InvalidString,
    UnsupportedVersion,
    InvalidProviderId,
    InvalidPath,
    InvalidReplayWindow,
    InvalidEnvironmentName,
};

pub const SecretFileError = error{
    SecretFileOpenFailed,
    SecretFileStatFailed,
    SecretFileNotRegular,
    SecretFilePermissions,
    SecretFileLength,
    SecretFileReadFailed,
    SecretFileChanged,
    InvalidSecretEncoding,
    AllZeroSecret,
};

pub const ProfileLoadError = ProfileParseError || SecretFileError || error{
    ProfileReadFailed,
    MissingSecretFileEnvironment,
    EmptySecretFilePath,
};

const BoundedPath = struct {
    bytes: [max_path_bytes]u8 = [_]u8{0} ** max_path_bytes,
    len: u16 = 0,

    fn init(text: []const u8) ProfileParseError!BoundedPath {
        if (text.len == 0 or text.len > max_path_bytes or text[0] != '/') {
            return error.InvalidPath;
        }
        for (text) |byte| {
            if (byte <= 0x20 or byte == 0x7f or byte == '#' or byte == '?') {
                return error.InvalidPath;
            }
        }
        var result: BoundedPath = .{};
        @memcpy(result.bytes[0..text.len], text);
        result.len = @intCast(text.len);
        return result;
    }

    fn slice(self: *const BoundedPath) []const u8 {
        return self.bytes[0..self.len];
    }
};

const EnvironmentName = struct {
    bytes: [max_environment_name_bytes]u8 = [_]u8{0} ** max_environment_name_bytes,
    len: u8 = 0,

    fn init(text: []const u8) ProfileParseError!EnvironmentName {
        if (!isEnvironmentName(text)) return error.InvalidEnvironmentName;
        var result: EnvironmentName = .{};
        @memcpy(result.bytes[0..text.len], text);
        result.len = @intCast(text.len);
        return result;
    }

    fn slice(self: *const EnvironmentName) []const u8 {
        return self.bytes[0..self.len];
    }
};

const ProfileDocument = struct {
    provider_id: u64,
    path: BoundedPath,
    replay_window_seconds: u32,
    secret_file_env: EnvironmentName,
};

const Secret = struct {
    bytes: [32]u8,

    fn deinit(self: *Secret) void {
        std.crypto.secureZero(u8, &self.bytes);
    }
};

/// Heap-owned HMAC-SHA256 v1 provider profile. The profile JSON names an
/// environment variable whose value is the secret file path; it never embeds
/// secret material.
pub const Profile = opaque {
    pub fn load(
        io: std.Io,
        allocator: std.mem.Allocator,
        environment: *const std.process.Environ.Map,
        profile_path: []const u8,
    ) ProfileLoadError!*Profile {
        return loadFromDir(io, allocator, std.Io.Dir.cwd(), environment, profile_path);
    }

    pub fn loadFromDir(
        io: std.Io,
        allocator: std.mem.Allocator,
        dir: std.Io.Dir,
        environment: *const std.process.Environ.Map,
        profile_path: []const u8,
    ) ProfileLoadError!*Profile {
        const bytes = dir.readFileAlloc(
            io,
            profile_path,
            allocator,
            .limited(max_profile_bytes + 1),
        ) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.StreamTooLong => return error.ProfileTooLarge,
            else => return error.ProfileReadFailed,
        };
        defer allocator.free(bytes);

        const document = try parseProfile(allocator, bytes);
        const secret_path = environment.get(document.secret_file_env.slice()) orelse
            return error.MissingSecretFileEnvironment;
        if (secret_path.len == 0) return error.EmptySecretFilePath;
        var secret = try loadSecret(io, dir, secret_path);
        defer secret.deinit();

        const impl = try allocator.create(ProfileImpl);
        impl.* = .{
            .allocator = allocator,
            .provider_id = document.provider_id,
            .path = document.path,
            .replay_window_seconds = document.replay_window_seconds,
            .secret = secret,
        };
        return profileToken(impl);
    }

    pub fn deinit(self: *Profile) void {
        const impl = profileImplMut(self);
        const allocator = impl.allocator;
        impl.secret.deinit();
        std.crypto.secureZero(u8, std.mem.asBytes(impl));
        allocator.destroy(impl);
    }

    pub fn adapter(self: *const Profile) Adapter {
        return .{ .context = self, .vtable = &adapter_vtable };
    }

    pub fn providerId(self: *const Profile) u64 {
        return profileImpl(self).provider_id;
    }

    pub fn path(self: *const Profile) []const u8 {
        return profileImpl(self).path.slice();
    }

    pub fn replayWindowSeconds(self: *const Profile) u32 {
        return profileImpl(self).replay_window_seconds;
    }

    /// Signs one canonical provider event for loopback rehearsal tooling.
    /// Validation happens before the secret is used so an operator cannot
    /// accidentally emit an authenticated request that this profile would
    /// reject as malformed.
    pub fn sign(
        self: *const Profile,
        timestamp_header: []const u8,
        body: []const u8,
    ) SignError!Signature {
        _ = parseCanonicalPositive(i64, timestamp_header) catch
            return error.InvalidTimestamp;
        if (body.len == 0 or body.len > max_body_bytes) return error.InvalidBody;

        const impl = profileImpl(self);
        _ = parseEvent(impl.allocator, impl.provider_id, body) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.InvalidBody,
        };
        const request: RequestView = .{
            .method = "POST",
            .target = impl.path.slice(),
            .content_type = "application/json",
            .timestamp_header = timestamp_header,
            .signature_header = "",
            .body = body,
            .received_unix_seconds = 0,
        };
        var mac = computeMac(&impl.secret.bytes, impl.provider_id, request);
        defer std.crypto.secureZero(u8, &mac);
        return std.fmt.bytesToHex(mac, .lower);
    }
};

const ProfileImpl = struct {
    allocator: std.mem.Allocator,
    provider_id: u64,
    path: BoundedPath,
    replay_window_seconds: u32,
    secret: Secret,
};

const adapter_vtable: Adapter.VTable = .{ .verify = verifyErased };

fn profileToken(impl: *ProfileImpl) *Profile {
    return @ptrCast(impl);
}

fn profileImpl(profile: *const Profile) *const ProfileImpl {
    return @ptrCast(@alignCast(profile));
}

fn profileImplMut(profile: *Profile) *ProfileImpl {
    return @ptrCast(@alignCast(profile));
}

fn verifyErased(raw: *const anyopaque, request: RequestView) VerifyError!VerifiedEvent {
    const profile: *const Profile = @ptrCast(@alignCast(raw));
    const impl = profileImpl(profile);

    if (!std.mem.eql(u8, request.method, "POST") or
        !std.mem.eql(u8, request.target, impl.path.slice()) or
        !std.mem.eql(u8, request.content_type, "application/json") or
        request.body.len == 0 or request.body.len > max_body_bytes)
    {
        return error.InvalidRequest;
    }

    var supplied: [32]u8 = undefined;
    parseLowerHex32(request.signature_header, &supplied) catch
        return error.AuthenticationFailed;
    defer std.crypto.secureZero(u8, &supplied);
    var expected = computeMac(&impl.secret.bytes, impl.provider_id, request);
    defer std.crypto.secureZero(u8, &expected);
    if (!std.crypto.timing_safe.eql([32]u8, expected, supplied)) {
        return error.AuthenticationFailed;
    }

    const timestamp = parseCanonicalPositive(i64, request.timestamp_header) catch
        return error.ReplayRejected;
    const age = std.math.sub(i64, request.received_unix_seconds, timestamp) catch
        return error.ReplayRejected;
    const window: i64 = @intCast(impl.replay_window_seconds);
    if (age < -window or age > window) return error.ReplayRejected;

    return parseEvent(impl.allocator, impl.provider_id, request.body) catch |err| switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.MalformedEvent,
    };
}

fn computeMac(secret: *const [32]u8, provider_id: u64, request: RequestView) [32]u8 {
    var hmac = HmacSha256.init(secret);
    updateField(&hmac, transcript_domain);
    var provider_id_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &provider_id_bytes, provider_id, .big);
    updateField(&hmac, &provider_id_bytes);
    updateField(&hmac, request.method);
    updateField(&hmac, request.target);
    updateField(&hmac, request.timestamp_header);
    updateField(&hmac, request.body);
    var result: [32]u8 = undefined;
    hmac.final(&result);
    return result;
}

fn updateField(hmac: *HmacSha256, bytes: []const u8) void {
    var length: [4]u8 = undefined;
    std.mem.writeInt(u32, &length, @intCast(bytes.len), .big);
    hmac.update(&length);
    hmac.update(bytes);
}

fn parseProfile(allocator: std.mem.Allocator, bytes: []const u8) ProfileParseError!ProfileDocument {
    if (bytes.len == 0) return error.EmptyProfile;
    if (bytes.len > max_profile_bytes) return error.ProfileTooLarge;
    if (!jsonNestingWithinLimit(bytes, max_json_nesting)) return error.JsonNestingTooDeep;
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, bytes, .{
        .duplicate_field_behavior = .@"error",
        .parse_numbers = false,
        .max_value_len = max_path_bytes,
    }) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return error.InvalidJson;
    };
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidRoot,
    };
    try exactFields(root, &.{ "version", "provider_id", "path", "replay_window_seconds", "secret_file_env" });
    if (try requiredInt(u16, root, "version") != profile_version) return error.UnsupportedVersion;
    const provider_id = parseCanonicalPositive(u64, try requiredString(root, "provider_id")) catch
        return error.InvalidProviderId;
    const replay_window = try requiredInt(u32, root, "replay_window_seconds");
    if (replay_window == 0 or replay_window > max_replay_window_seconds) {
        return error.InvalidReplayWindow;
    }
    return .{
        .provider_id = provider_id,
        .path = try BoundedPath.init(try requiredString(root, "path")),
        .replay_window_seconds = replay_window,
        .secret_file_env = try EnvironmentName.init(try requiredString(root, "secret_file_env")),
    };
}

const EventParseError = std.mem.Allocator.Error || error{MalformedEvent};

fn parseEvent(allocator: std.mem.Allocator, provider_id: u64, bytes: []const u8) EventParseError!VerifiedEvent {
    if (!jsonNestingWithinLimit(bytes, max_json_nesting)) return error.MalformedEvent;
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, bytes, .{
        .duplicate_field_behavior = .@"error",
        .parse_numbers = false,
        .max_value_len = max_body_bytes,
    }) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return error.MalformedEvent;
    };
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.MalformedEvent,
    };
    const kind = eventString(root, "kind") catch return error.MalformedEvent;
    const direction_text = eventString(root, "direction") catch return error.MalformedEvent;
    const direction: saga.Direction = if (std.mem.eql(u8, direction_text, "deposit"))
        .deposit
    else if (std.mem.eql(u8, direction_text, "withdrawal"))
        .withdrawal
    else
        return error.MalformedEvent;
    const event_id = parseCanonicalPositive(u128, eventString(root, "event_id") catch return error.MalformedEvent) catch return error.MalformedEvent;
    const payment_id = parseCanonicalPositive(u128, eventString(root, "payment_id") catch return error.MalformedEvent) catch return error.MalformedEvent;
    const revision = eventPositiveInt(u64, root, "revision") catch return error.MalformedEvent;
    const uid = eventPositiveInt(i64, root, "uid") catch return error.MalformedEvent;
    const currency = eventPositiveInt(i32, root, "currency") catch return error.MalformedEvent;
    const amount = eventPositiveInt(i64, root, "amount") catch return error.MalformedEvent;

    if (std.mem.eql(u8, kind, "authorize")) {
        exactFields(root, &.{ "version", "event_id", "payment_id", "revision", "kind", "direction", "uid", "currency", "amount", "timeout_seconds" }) catch return error.MalformedEvent;
        if ((eventInt(u16, root, "version") catch return error.MalformedEvent) != 1) return error.MalformedEvent;
        const result: saga.Authorization = .{
            .event_id = event_id,
            .payment_id = payment_id,
            .provider_id = provider_id,
            .revision = revision,
            .direction = direction,
            .uid = uid,
            .currency = currency,
            .amount = amount,
            .timeout_seconds = eventPositiveInt(u32, root, "timeout_seconds") catch return error.MalformedEvent,
        };
        result.validate() catch return error.MalformedEvent;
        return .{ .authorization = result };
    }
    if (std.mem.eql(u8, kind, "post") or std.mem.eql(u8, kind, "void")) {
        exactFields(root, &.{ "version", "event_id", "payment_id", "revision", "kind", "direction", "uid", "currency", "amount" }) catch return error.MalformedEvent;
        if ((eventInt(u16, root, "version") catch return error.MalformedEvent) != 1) return error.MalformedEvent;
        const result: saga.Resolution = .{
            .event_id = event_id,
            .payment_id = payment_id,
            .provider_id = provider_id,
            .revision = revision,
            .kind = if (std.mem.eql(u8, kind, "post")) .post else .void,
            .direction = direction,
            .uid = uid,
            .currency = currency,
            .amount = amount,
        };
        result.validate() catch return error.MalformedEvent;
        return .{ .resolution = result };
    }
    return error.MalformedEvent;
}

fn exactFields(object: std.json.ObjectMap, expected: []const []const u8) ProfileParseError!void {
    if (object.count() != expected.len) return error.InvalidObjectShape;
    for (expected) |field| if (!object.contains(field)) return error.InvalidObjectShape;
}

fn requiredString(object: std.json.ObjectMap, key: []const u8) ProfileParseError![]const u8 {
    const value = object.get(key) orelse return error.MissingField;
    return switch (value) {
        .string => |text| text,
        else => error.InvalidFieldType,
    };
}

fn requiredInt(comptime T: type, object: std.json.ObjectMap, key: []const u8) ProfileParseError!T {
    const value = object.get(key) orelse return error.MissingField;
    const text = switch (value) {
        .number_string => |number| number,
        else => return error.InvalidFieldType,
    };
    return std.fmt.parseInt(T, text, 10) catch error.InvalidInteger;
}

fn eventString(object: std.json.ObjectMap, key: []const u8) error{MalformedEvent}![]const u8 {
    const value = object.get(key) orelse return error.MalformedEvent;
    return switch (value) {
        .string => |text| text,
        else => error.MalformedEvent,
    };
}

fn eventInt(comptime T: type, object: std.json.ObjectMap, key: []const u8) error{MalformedEvent}!T {
    const value = object.get(key) orelse return error.MalformedEvent;
    const text = switch (value) {
        .number_string => |number| number,
        else => return error.MalformedEvent,
    };
    return std.fmt.parseInt(T, text, 10) catch error.MalformedEvent;
}

fn eventPositiveInt(comptime T: type, object: std.json.ObjectMap, key: []const u8) error{MalformedEvent}!T {
    const result = try eventInt(T, object, key);
    if (result <= 0) return error.MalformedEvent;
    return result;
}

fn parseCanonicalPositive(comptime T: type, text: []const u8) error{InvalidInteger}!T {
    if (text.len == 0 or text[0] == '0') return error.InvalidInteger;
    for (text) |byte| if (!std.ascii.isDigit(byte)) return error.InvalidInteger;
    const result = std.fmt.parseInt(T, text, 10) catch return error.InvalidInteger;
    if (result <= 0) return error.InvalidInteger;
    return result;
}

fn parseLowerHex32(text: []const u8, out: *[32]u8) error{InvalidSignature}!void {
    if (text.len != 64) return error.InvalidSignature;
    for (text) |byte| {
        if (!std.ascii.isDigit(byte) and (byte < 'a' or byte > 'f')) return error.InvalidSignature;
    }
    _ = std.fmt.hexToBytes(out, text) catch return error.InvalidSignature;
}

fn loadSecret(io: std.Io, dir: std.Io.Dir, path: []const u8) SecretFileError!Secret {
    const file = dir.openFile(io, path, .{ .allow_directory = false, .follow_symlinks = false }) catch
        return error.SecretFileOpenFailed;
    defer file.close(io);
    const initial = file.stat(io) catch return error.SecretFileStatFailed;
    if (initial.kind != .file) return error.SecretFileNotRegular;
    if (comptime @hasDecl(std.Io.File.Permissions, "toMode")) {
        if ((initial.permissions.toMode() & 0o7777) != 0o600) return error.SecretFilePermissions;
    }
    if (initial.size != 64) return error.SecretFileLength;
    var encoded: [64]u8 = undefined;
    defer std.crypto.secureZero(u8, &encoded);
    if ((file.readPositionalAll(io, &encoded, 0) catch return error.SecretFileReadFailed) != encoded.len) {
        return error.SecretFileChanged;
    }
    const final = file.stat(io) catch return error.SecretFileStatFailed;
    if (final.inode != initial.inode or final.size != initial.size or final.kind != initial.kind or
        final.permissions != initial.permissions or final.nlink != initial.nlink or
        !std.meta.eql(final.mtime, initial.mtime) or !std.meta.eql(final.ctime, initial.ctime))
    {
        return error.SecretFileChanged;
    }
    var secret: Secret = undefined;
    defer secret.deinit();
    parseLowerHex32(&encoded, &secret.bytes) catch return error.InvalidSecretEncoding;
    var nonzero = false;
    for (secret.bytes) |byte| nonzero = nonzero or byte != 0;
    if (!nonzero) return error.AllZeroSecret;
    return secret;
}

fn isEnvironmentName(text: []const u8) bool {
    if (text.len == 0 or text.len > max_environment_name_bytes) return false;
    if (text[0] != '_' and !std.ascii.isUpper(text[0])) return false;
    for (text[1..]) |byte| {
        if (byte != '_' and !std.ascii.isUpper(byte) and !std.ascii.isDigit(byte)) return false;
    }
    return true;
}

fn jsonNestingWithinLimit(bytes: []const u8, maximum: usize) bool {
    var depth: usize = 0;
    var in_string = false;
    var escaped = false;
    for (bytes) |byte| {
        if (in_string) {
            if (escaped) escaped = false else if (byte == '\\') escaped = true else if (byte == '"') in_string = false;
            continue;
        }
        switch (byte) {
            '"' => in_string = true,
            '{', '[' => {
                depth += 1;
                if (depth > maximum) return false;
            },
            '}', ']' => {
                if (depth == 0) return false;
                depth -= 1;
            },
            else => {},
        }
    }
    return !in_string and depth == 0;
}

const test_authorize_body =
    \\{"version":1,"event_id":"340282366920938463463374607431768211455","payment_id":"2","revision":1,"kind":"authorize","direction":"deposit","uid":41,"currency":11,"amount":900,"timeout_seconds":30}
;
const test_post_body =
    \\{"version":1,"event_id":"3","payment_id":"2","revision":2,"kind":"post","direction":"withdrawal","uid":41,"currency":11,"amount":900}
;
const test_void_body =
    \\{"version":1,"event_id":"4","payment_id":"2","revision":2,"kind":"void","direction":"deposit","uid":41,"currency":11,"amount":900}
;
const test_profile_json =
    \\{"version":1,"provider_id":"7001","path":"/v1/clearing/events","replay_window_seconds":300,"secret_file_env":"PARIX_CLEARING_PROVIDER_SECRET_FILE"}
;

fn testProfile(allocator: std.mem.Allocator, provider_id: u64, secret_byte: u8) !*Profile {
    const impl = try allocator.create(ProfileImpl);
    impl.* = .{
        .allocator = allocator,
        .provider_id = provider_id,
        .path = try BoundedPath.init("/v1/clearing/events"),
        .replay_window_seconds = 300,
        .secret = .{ .bytes = [_]u8{secret_byte} ** 32 },
    };
    return profileToken(impl);
}

const SignedRequest = struct {
    request: RequestView,
    signature: Signature,

    fn view(self: *const SignedRequest) RequestView {
        var request = self.request;
        request.signature_header = &self.signature;
        return request;
    }
};

fn signedRequest(profile: *const Profile, body: []const u8, timestamp: []const u8) SignedRequest {
    const request: RequestView = .{
        .method = "POST",
        .target = "/v1/clearing/events",
        .content_type = "application/json",
        .timestamp_header = timestamp,
        .signature_header = "",
        .body = body,
        .received_unix_seconds = 1_700_000_000,
    };
    const impl = profileImpl(profile);
    var mac = computeMac(&impl.secret.bytes, impl.provider_id, request);
    defer std.crypto.secureZero(u8, &mac);
    const signature = std.fmt.bytesToHex(mac, .lower);
    return .{ .request = request, .signature = signature };
}

fn writeTestFile(
    dir: std.Io.Dir,
    path: []const u8,
    bytes: []const u8,
    permissions: std.Io.Dir.Permissions,
) !void {
    const file = try dir.createFile(std.testing.io, path, .{
        .truncate = true,
        .permissions = permissions,
    });
    defer file.close(std.testing.io);
    var buffer: [1024]u8 = undefined;
    var writer = file.writer(std.testing.io, &buffer);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
}

fn testFilePermissions(mode: u16) std.Io.Dir.Permissions {
    return if (std.Io.Dir.Permissions.has_executable_bit) @enumFromInt(mode) else .default_file;
}

test "fixture is the exact non-secret profile schema" {
    const fixture = try std.Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        "src/zig/fixtures/parix-clearing-provider.example.json",
        std.testing.allocator,
        .limited(max_profile_bytes),
    );
    defer std.testing.allocator.free(fixture);
    const document = try parseProfile(std.testing.allocator, fixture);
    try std.testing.expectEqual(@as(u64, 7_001), document.provider_id);
    try std.testing.expectEqualStrings("/v1/clearing/events", document.path.slice());
    try std.testing.expectEqual(@as(u32, 300), document.replay_window_seconds);
    try std.testing.expectEqualStrings(
        "PARIX_CLEARING_PROVIDER_SECRET_FILE",
        document.secret_file_env.slice(),
    );
    try std.testing.expect(std.mem.find(u8, fixture, "secret\"") == null);

    const unknown =
        \\{"version":1,"provider_id":"7001","path":"/events","replay_window_seconds":300,"secret_file_env":"KEY_FILE","secret":"no"}
    ;
    try std.testing.expectError(error.InvalidObjectShape, parseProfile(std.testing.allocator, unknown));
    const duplicate =
        \\{"version":1,"provider_id":"7001","provider_id":"7002","path":"/events","replay_window_seconds":300,"secret_file_env":"KEY_FILE"}
    ;
    try std.testing.expectError(error.InvalidJson, parseProfile(std.testing.allocator, duplicate));
    const noncanonical =
        \\{"version":1,"provider_id":"07001","path":"/events","replay_window_seconds":300,"secret_file_env":"KEY_FILE"}
    ;
    try std.testing.expectError(error.InvalidProviderId, parseProfile(std.testing.allocator, noncanonical));
}

test "profile loads only an exact owner-only lowercase secret file" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const private = testFilePermissions(0o600);
    try writeTestFile(tmp.dir, "provider.key", "01" ** 32, private);
    try writeTestFile(tmp.dir, "profile.json", test_profile_json, private);
    var environment = std.process.Environ.Map.init(std.testing.allocator);
    defer environment.deinit();
    try environment.put("PARIX_CLEARING_PROVIDER_SECRET_FILE", "provider.key");
    const profile = try Profile.loadFromDir(std.testing.io, std.testing.allocator, tmp.dir, &environment, "profile.json");
    defer profile.deinit();
    try std.testing.expectEqual(@as(u64, 7_001), profile.providerId());

    try writeTestFile(tmp.dir, "uppercase.key", "AA" ** 32, private);
    try std.testing.expectError(error.InvalidSecretEncoding, loadSecret(std.testing.io, tmp.dir, "uppercase.key"));
    try writeTestFile(tmp.dir, "short.key", "01" ** 31, private);
    try std.testing.expectError(error.SecretFileLength, loadSecret(std.testing.io, tmp.dir, "short.key"));
    try writeTestFile(tmp.dir, "zero.key", "00" ** 32, private);
    try std.testing.expectError(error.AllZeroSecret, loadSecret(std.testing.io, tmp.dir, "zero.key"));
    if (std.Io.Dir.Permissions.has_executable_bit) {
        try writeTestFile(tmp.dir, "public.key", "01" ** 32, testFilePermissions(0o644));
        try std.testing.expectError(error.SecretFilePermissions, loadSecret(std.testing.io, tmp.dir, "public.key"));
    }
}

test "HMAC v1 transcript has a stable known answer" {
    const profile = try testProfile(std.testing.allocator, 7_001, 0x01);
    defer profile.deinit();
    const signed = signedRequest(profile, test_authorize_body, "1700000000");
    try std.testing.expectEqualStrings(
        "ebe2a7c8b8583695bc6f056c618250de973a048e9981915dce5b7985d8de03d4",
        &signed.signature,
    );
}

test "public signer validates canonical timestamp and event before using the profile" {
    const profile = try testProfile(std.testing.allocator, 7_001, 0x01);
    defer profile.deinit();
    const signature = try profile.sign("1700000000", test_authorize_body);
    try std.testing.expectEqualStrings(
        "ebe2a7c8b8583695bc6f056c618250de973a048e9981915dce5b7985d8de03d4",
        &signature,
    );
    try std.testing.expectError(
        error.InvalidTimestamp,
        profile.sign("01700000000", test_authorize_body),
    );
    try std.testing.expectError(error.InvalidBody, profile.sign("1700000000", "{}"));
}

test "checked-in rehearsal events are strict signable provider payloads" {
    const profile = try testProfile(std.testing.allocator, 7_001, 0x01);
    defer profile.deinit();
    const paths = [_][]const u8{
        "src/zig/fixtures/parix-clearing-authorization.example.json",
        "src/zig/fixtures/parix-clearing-post.example.json",
    };
    for (paths) |path| {
        const body = try std.Io.Dir.cwd().readFileAlloc(
            std.testing.io,
            path,
            std.testing.allocator,
            .limited(max_body_bytes),
        );
        defer std.testing.allocator.free(body);
        const signature = try profile.sign("1700000000", body);
        const verified = try profile.adapter().verify(.{
            .method = "POST",
            .target = profile.path(),
            .content_type = "application/json",
            .timestamp_header = "1700000000",
            .signature_header = &signature,
            .body = body,
            .received_unix_seconds = 1_700_000_000,
        });
        switch (verified) {
            .authorization, .resolution => {},
        }
    }
}

test "all signed fields provider and key changes are rejected" {
    const profile = try testProfile(std.testing.allocator, 7_001, 0x01);
    defer profile.deinit();
    const signed = signedRequest(profile, test_authorize_body, "1700000000");

    var changed = signed.view();
    changed.method = "PUT";
    try std.testing.expectError(error.InvalidRequest, profile.adapter().verify(changed));
    changed = signed.view();
    changed.target = "/v1/clearing/other";
    try std.testing.expectError(error.InvalidRequest, profile.adapter().verify(changed));
    changed = signed.view();
    changed.timestamp_header = "1700000001";
    try std.testing.expectError(error.AuthenticationFailed, profile.adapter().verify(changed));
    changed = signed.view();
    changed.body = test_post_body;
    try std.testing.expectError(error.AuthenticationFailed, profile.adapter().verify(changed));

    const other_provider = try testProfile(std.testing.allocator, 7_002, 0x01);
    defer other_provider.deinit();
    try std.testing.expectError(error.AuthenticationFailed, other_provider.adapter().verify(signed.view()));
    const other_key = try testProfile(std.testing.allocator, 7_001, 0x02);
    defer other_key.deinit();
    try std.testing.expectError(error.AuthenticationFailed, other_key.adapter().verify(signed.view()));
}

test "signature verification precedes replay and JSON parsing" {
    const profile = try testProfile(std.testing.allocator, 7_001, 0x01);
    defer profile.deinit();
    var malformed = signedRequest(profile, "{", "1699990000");
    malformed.signature[0] = if (malformed.signature[0] == '0') '1' else '0';
    malformed.request.signature_header = &malformed.signature;
    try std.testing.expectError(error.AuthenticationFailed, profile.adapter().verify(malformed.view()));

    const authenticated = signedRequest(profile, "{", "1700000000");
    try std.testing.expectError(error.MalformedEvent, profile.adapter().verify(authenticated.view()));
}

test "replay window includes exact past and future boundaries" {
    const profile = try testProfile(std.testing.allocator, 7_001, 0x01);
    defer profile.deinit();
    const past = signedRequest(profile, test_post_body, "1699999700");
    _ = try profile.adapter().verify(past.view());
    const future = signedRequest(profile, test_post_body, "1700000300");
    _ = try profile.adapter().verify(future.view());
    const stale = signedRequest(profile, test_post_body, "1699999699");
    try std.testing.expectError(error.ReplayRejected, profile.adapter().verify(stale.view()));
    const too_future = signedRequest(profile, test_post_body, "1700000301");
    try std.testing.expectError(error.ReplayRejected, profile.adapter().verify(too_future.view()));
    const noncanonical = signedRequest(profile, test_post_body, "01700000000");
    try std.testing.expectError(error.ReplayRejected, profile.adapter().verify(noncanonical.view()));
}

test "authorize post and void normalize strictly with pinned provider" {
    const profile = try testProfile(std.testing.allocator, 7_001, 0x01);
    defer profile.deinit();
    const signed_authorization = signedRequest(profile, test_authorize_body, "1700000000");
    const authorization = try profile.adapter().verify(signed_authorization.view());
    try std.testing.expectEqual(@as(u128, std.math.maxInt(u128)), authorization.authorization.event_id);
    try std.testing.expectEqual(@as(u64, 7_001), authorization.authorization.provider_id);
    try std.testing.expectEqual(saga.Direction.deposit, authorization.authorization.direction);
    const signed_post = signedRequest(profile, test_post_body, "1700000000");
    const post = try profile.adapter().verify(signed_post.view());
    try std.testing.expectEqual(saga.ResolutionKind.post, post.resolution.kind);
    try std.testing.expectEqual(saga.Direction.withdrawal, post.resolution.direction);
    const signed_void = signedRequest(profile, test_void_body, "1700000000");
    const voided = try profile.adapter().verify(signed_void.view());
    try std.testing.expectEqual(saga.ResolutionKind.void, voided.resolution.kind);
}

test "event schema rejects duplicates unknown coercions and invalid domain values" {
    const bad_bodies = [_][]const u8{
        \\{"version":1,"event_id":"3","event_id":"4","payment_id":"2","revision":2,"kind":"post","direction":"deposit","uid":41,"currency":11,"amount":900}
        ,
        \\{"version":1,"event_id":"3","payment_id":"2","revision":2,"kind":"post","direction":"deposit","uid":41,"currency":11,"amount":900,"extra":1}
        ,
        \\{"version":1,"event_id":3,"payment_id":"2","revision":2,"kind":"post","direction":"deposit","uid":41,"currency":11,"amount":900}
        ,
        \\{"version":1,"event_id":"03","payment_id":"2","revision":2,"kind":"post","direction":"deposit","uid":41,"currency":11,"amount":900}
        ,
        \\{"version":1,"event_id":"3","payment_id":"2","revision":"2","kind":"post","direction":"deposit","uid":41,"currency":11,"amount":900}
        ,
        \\{"version":1,"event_id":"3","payment_id":"2","revision":2,"kind":"authorize","direction":"deposit","uid":41,"currency":11,"amount":900}
        ,
        \\{"version":1,"event_id":"3","payment_id":"2","revision":2,"kind":"post","direction":"sideways","uid":41,"currency":11,"amount":900}
        ,
        \\{"version":1,"event_id":"3","payment_id":"2","revision":2,"kind":"refund","direction":"deposit","uid":41,"currency":11,"amount":900}
        ,
        \\{"version":1,"event_id":"3","payment_id":"2","revision":2,"kind":"authorize","direction":"deposit","uid":41,"currency":11,"amount":900,"timeout_seconds":0}
        ,
    };
    for (bad_bodies) |body| {
        try std.testing.expectError(error.MalformedEvent, parseEvent(std.testing.allocator, 7_001, body));
    }
    var oversized: [max_body_bytes + 1]u8 = undefined;
    @memset(&oversized, 'x');
    const profile = try testProfile(std.testing.allocator, 7_001, 0x01);
    defer profile.deinit();
    const request = signedRequest(profile, &oversized, "1700000000");
    try std.testing.expectError(error.InvalidRequest, profile.adapter().verify(request.view()));
}

test "profile and verification propagate allocator exhaustion without leaks" {
    var profile_failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(
        error.OutOfMemory,
        parseProfile(profile_failing.allocator(), test_profile_json),
    );

    var event_failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    const impl = try std.testing.allocator.create(ProfileImpl);
    impl.* = .{
        .allocator = event_failing.allocator(),
        .provider_id = 7_001,
        .path = try BoundedPath.init("/v1/clearing/events"),
        .replay_window_seconds = 300,
        .secret = .{ .bytes = [_]u8{0x01} ** 32 },
    };
    const profile = profileToken(impl);
    defer {
        impl.allocator = std.testing.allocator;
        profile.deinit();
    }
    const request = signedRequest(profile, test_authorize_body, "1700000000");
    try std.testing.expectError(error.OutOfMemory, profile.adapter().verify(request.view()));
}
