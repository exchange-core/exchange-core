const std = @import("std");
const builtin = @import("builtin");
const protocol = @import("exchange_protocol");

const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
const HkdfSha256 = std.crypto.kdf.hkdf.HkdfSha256;

pub const protocol_version: u8 = 1;
pub const sequence_one: u64 = 1;
pub const max_profile_bytes: usize = 64 * 1024;
pub const max_identifier_bytes: usize = 64;
pub const max_environment_name_bytes: usize = 64;
pub const max_credentials: usize = 32;
pub const secret_key_bytes: usize = 32;
pub const nonce_bytes: usize = 32;
pub const mac_bytes: usize = 32;
pub const hex_32_bytes: usize = 64;
pub const max_response_bytes: usize = 64 * 1024;
pub const max_request_envelope_bytes: usize = "REQ 1 ".len + hex_32_bytes + 1 + protocol.max_line_bytes;
pub const max_response_envelope_bytes: usize = "RES 1 ".len + hex_32_bytes + 1 + max_response_bytes;
pub const max_wire_line_bytes: usize = @max(max_request_envelope_bytes, max_response_envelope_bytes);

pub const authentication_failure_line = "ERR authentication AuthenticationFailed\n";

// Cryptographic encoding is intentionally centralized here. Every variable
// field is encoded as a four-byte big-endian length followed by exact bytes.
// Handshake MACs encode field(domain), one version byte, then fields for the
// instance UUID, credential ID, client nonce, and server nonce. HKDF-SHA256
// extracts with session_salt_domain as salt and the credential key as IKM,
// then expands over that same transcript with session_domain. Request/response
// MACs encode field(domain), version, instance, credential, the sequence as an
// eight-byte big-endian field, and the exact raw suffix as the final field.
const proof_domain = "exchange-core/ingress-auth/proof";
const session_salt_domain = "exchange-core/ingress-auth/session-salt";
const session_domain = "exchange-core/ingress-auth/session";
const server_mac_domain = "exchange-core/ingress-auth/server";
const request_mac_domain = "exchange-core/ingress-auth/request";
const response_mac_domain = "exchange-core/ingress-auth/response";

const fake_key_bytes = [_]u8{
    0x8d, 0x2b, 0x6f, 0x31, 0xae, 0x47, 0xc9, 0x55,
    0x0c, 0xf0, 0x73, 0x99, 0x5a, 0x14, 0xe6, 0x82,
    0x7b, 0x3d, 0x10, 0xc4, 0x68, 0xf2, 0xa1, 0xde,
    0x44, 0x90, 0xbb, 0x26, 0x5f, 0x83, 0xd7, 0x0a,
};

pub const IdentifierError = error{InvalidIdentifier};

pub const WireError = IdentifierError || error{
    EmptyLine,
    LineTooLong,
    ControlCharacter,
    InvalidFrame,
    UnsupportedVersion,
    InvalidSequence,
    InvalidHex,
    InvalidPayloadLength,
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
    UnsupportedVersion,
    InvalidCredentialCount,
    TooManyCredentials,
    InvalidIdentifier,
    InvalidEnvironmentName,
    InvalidRole,
    InvalidUid,
    DuplicateCredentialId,
    InconsistentPrincipal,
    DuplicateSecretFileEnvironment,
    MissingTrader,
    MissingOperator,
};

pub const ProfileLoadError = ProfileParseError || SecretFileError || error{
    ProfileReadFailed,
    MissingSecretFileEnvironment,
    EmptySecretFilePath,
    DuplicateCredentialSecret,
};

pub const AuthenticationError = error{AuthenticationFailed};
pub const IntegrityError = error{
    InvalidSequence,
    InvalidPayload,
    IntegrityFailed,
};
pub const AuthorizationError = error{
    Forbidden,
    CrossUid,
};

pub const Identifier = struct {
    bytes: [max_identifier_bytes]u8 = [_]u8{0} ** max_identifier_bytes,
    len: u8 = 0,

    pub fn init(text: []const u8) IdentifierError!Identifier {
        if (!isSafeToken(text)) return error.InvalidIdentifier;
        var result: Identifier = .{};
        @memcpy(result.bytes[0..text.len], text);
        result.len = @intCast(text.len);
        return result;
    }

    pub fn slice(self: *const Identifier) []const u8 {
        return self.bytes[0..self.len];
    }
};

/// Parses the canonical, lowercase textual UUID used to pin an ingress
/// connection to one exchange instance. The nil UUID is never a valid instance.
pub fn validateInstanceId(text: []const u8) IdentifierError!Identifier {
    if (text.len != 36) return error.InvalidIdentifier;
    var any_nonzero = false;
    for (text, 0..) |byte, index| {
        if (index == 8 or index == 13 or index == 18 or index == 23) {
            if (byte != '-') return error.InvalidIdentifier;
            continue;
        }
        if (!std.ascii.isDigit(byte) and (byte < 'a' or byte > 'f')) {
            return error.InvalidIdentifier;
        }
        any_nonzero = any_nonzero or byte != '0';
    }
    if (!any_nonzero) return error.InvalidIdentifier;
    return Identifier.init(text);
}

pub const Principal = union(enum) {
    trader: i64,
    operator,
};

pub const Identity = struct {
    credential_id: Identifier,
    principal_id: Identifier,
    principal: Principal,
};

pub const SecretKey = struct {
    bytes: [secret_key_bytes]u8,

    pub fn deinit(self: *SecretKey) void {
        std.crypto.secureZero(u8, &self.bytes);
    }
};

pub const SessionKey = struct {
    bytes: [secret_key_bytes]u8,

    pub fn deinit(self: *SessionKey) void {
        std.crypto.secureZero(u8, &self.bytes);
    }
};

pub const Nonce = [nonce_bytes]u8;
pub const Mac = [mac_bytes]u8;

pub const Auth = struct {
    instance_id: Identifier,
    credential_id: Identifier,
    client_nonce: Nonce,
};

pub const Challenge = struct {
    server_nonce: Nonce,
};

pub const Proof = struct {
    mac: Mac,
};

pub const AuthAck = struct {
    server_mac: Mac,
};

pub const Transcript = struct {
    instance_id: Identifier,
    credential_id: Identifier,
    client_nonce: Nonce,
    server_nonce: Nonce,

    pub fn init(auth: Auth, challenge: Challenge) Transcript {
        return .{
            .instance_id = auth.instance_id,
            .credential_id = auth.credential_id,
            .client_nonce = auth.client_nonce,
            .server_nonce = challenge.server_nonce,
        };
    }
};

pub const Authenticated = struct {
    identity: Identity,
    session_key: SessionKey,

    pub fn deinit(self: *Authenticated) void {
        self.session_key.deinit();
    }
};

pub const RequestEnvelope = struct {
    sequence: u64,
    request_mac: Mac,
    raw_command: []const u8,
};

pub const ResponseEnvelope = struct {
    sequence: u64,
    response_mac: Mac,
    raw_response: []const u8,
};

pub const RequestKind = enum(u8) {
    ping = 0,
    add_user = 1,
    deposit = 2,
    fund_deposit = 3,
    fund_withdraw = 4,
    funding_status = 5,
    place = 6,
    move = 7,
    reduce = 8,
    cancel = 9,
    balance = 10,
    book = 11,
    shutdown = 12,
    clearing_status = 13,
    clearing_operations = 14,
};

const Credential = struct {
    identity: Identity,
    key: SecretKey,
};

/// Heap-owned authentication profile. Its representation is intentionally not
/// part of the public API; callers retain only this handle and copied Identity values.
pub const Profile = opaque {
    pub fn load(
        io: std.Io,
        allocator: std.mem.Allocator,
        environment: *const std.process.Environ.Map,
        path: []const u8,
    ) ProfileLoadError!*Profile {
        return loadFromDir(io, allocator, std.Io.Dir.cwd(), environment, path);
    }

    pub fn loadFromDir(
        io: std.Io,
        allocator: std.mem.Allocator,
        dir: std.Io.Dir,
        environment: *const std.process.Environ.Map,
        path: []const u8,
    ) ProfileLoadError!*Profile {
        const bytes = dir.readFileAlloc(
            io,
            path,
            allocator,
            .limited(max_profile_bytes + 1),
        ) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.StreamTooLong => return error.ProfileTooLarge,
            else => return error.ProfileReadFailed,
        };
        defer allocator.free(bytes);
        const document = try parseProfile(bytes, allocator);

        const impl = try allocator.create(ProfileImpl);
        impl.* = .{
            .allocator = allocator,
            .instance_id = document.instance_id,
            .credentials = undefined,
            .credential_count = 0,
        };
        @memset(std.mem.asBytes(&impl.credentials), 0);
        const profile = profileToken(impl);
        errdefer profile.destroy();

        for (document.credentials[0..document.credential_count], 0..) |spec, index| {
            const secret_path = environment.get(spec.secret_file_env.slice()) orelse
                return error.MissingSecretFileEnvironment;
            if (secret_path.len == 0) return error.EmptySecretFilePath;
            var key = try loadSecretKey(io, dir, secret_path);
            // Assignment below deliberately copies the key into profile-owned
            // storage; wipe the unavoidable stack copy at this iteration's end.
            defer key.deinit();
            for (impl.credentials[0..impl.credential_count]) |*prior| {
                if (std.crypto.timing_safe.eql(
                    [secret_key_bytes]u8,
                    prior.key.bytes,
                    key.bytes,
                )) {
                    return error.DuplicateCredentialSecret;
                }
            }
            impl.credentials[index] = .{
                .identity = spec.identity,
                .key = key,
            };
            impl.credential_count += 1;
        }
        return profile;
    }

    pub fn deinit(self: *Profile) void {
        self.destroy();
    }

    fn destroy(self: *Profile) void {
        const impl = profileImplMut(self);
        const allocator = impl.allocator;
        for (impl.credentials[0..impl.credential_count]) |*credential| {
            credential.key.deinit();
        }
        std.crypto.secureZero(u8, std.mem.asBytes(&impl.credentials));
        allocator.destroy(impl);
    }

    pub fn instanceId(self: *const Profile) []const u8 {
        return profileImpl(self).instance_id.slice();
    }

    pub fn credentialCount(self: *const Profile) usize {
        return profileImpl(self).credential_count;
    }

    pub fn identityAt(self: *const Profile, index: usize) ?Identity {
        const impl = profileImpl(self);
        if (index >= impl.credential_count) return null;
        return impl.credentials[index].identity;
    }

    pub fn findIdentity(self: *const Profile, credential_id: []const u8) ?Identity {
        const credential = self.findCredential(credential_id) orelse return null;
        return credential.identity;
    }

    pub fn containsCredential(self: *const Profile, credential_id: []const u8) bool {
        return self.findCredential(credential_id) != null;
    }

    fn findCredential(self: *const Profile, credential_id: []const u8) ?*const Credential {
        const impl = profileImpl(self);
        for (impl.credentials[0..impl.credential_count]) |*credential| {
            if (std.mem.eql(u8, credential.identity.credential_id.slice(), credential_id)) {
                return credential;
            }
        }
        return null;
    }

    pub fn verifyProof(
        self: *const Profile,
        transcript: Transcript,
        supplied: Mac,
    ) AuthenticationError!Authenticated {
        const credential = self.findCredential(transcript.credential_id.slice());
        var selected_key = SecretKey{ .bytes = fake_key_bytes };
        defer selected_key.deinit();
        if (credential) |known| selected_key.bytes = known.key.bytes;

        var expected = computeProof(&selected_key, transcript);
        defer std.crypto.secureZero(u8, &expected);
        const instance_matches = std.mem.eql(
            u8,
            self.instanceId(),
            transcript.instance_id.slice(),
        );
        const mac_matches = std.crypto.timing_safe.eql(Mac, expected, supplied);
        if (credential == null or !instance_matches or !mac_matches) {
            return error.AuthenticationFailed;
        }
        return .{
            .identity = credential.?.identity,
            .session_key = deriveSessionKey(&selected_key, transcript),
        };
    }
};

const ProfileImpl = struct {
    allocator: std.mem.Allocator,
    instance_id: Identifier,
    credentials: [max_credentials]Credential,
    credential_count: usize,
};

fn profileToken(impl: *ProfileImpl) *Profile {
    return @ptrCast(impl);
}

fn profileImpl(profile: *const Profile) *const ProfileImpl {
    return @ptrCast(@alignCast(profile));
}

fn profileImplMut(profile: *Profile) *ProfileImpl {
    return @ptrCast(@alignCast(profile));
}

pub fn loadSecretKey(
    io: std.Io,
    dir: std.Io.Dir,
    path: []const u8,
) SecretFileError!SecretKey {
    const file = dir.openFile(io, path, .{
        .allow_directory = false,
        .follow_symlinks = false,
    }) catch return error.SecretFileOpenFailed;
    defer file.close(io);

    const initial = file.stat(io) catch return error.SecretFileStatFailed;
    if (initial.kind != .file) return error.SecretFileNotRegular;
    if (comptime @hasDecl(std.Io.File.Permissions, "toMode")) {
        if ((initial.permissions.toMode() & 0o7777) != 0o600) {
            return error.SecretFilePermissions;
        }
    }
    if (initial.size != hex_32_bytes and initial.size != hex_32_bytes + 1) {
        return error.SecretFileLength;
    }

    var encoded: [hex_32_bytes + 1]u8 = undefined;
    defer std.crypto.secureZero(u8, &encoded);
    const length: usize = @intCast(initial.size);
    const read_length = file.readPositionalAll(io, encoded[0..length], 0) catch
        return error.SecretFileReadFailed;
    if (read_length != length) return error.SecretFileChanged;
    const final = file.stat(io) catch return error.SecretFileStatFailed;
    if (final.inode != initial.inode or
        final.size != initial.size or
        final.kind != initial.kind or
        final.permissions != initial.permissions or
        final.nlink != initial.nlink or
        !std.meta.eql(final.mtime, initial.mtime) or
        !std.meta.eql(final.ctime, initial.ctime))
    {
        return error.SecretFileChanged;
    }
    if (length == hex_32_bytes + 1 and encoded[hex_32_bytes] != '\n') {
        return error.InvalidSecretEncoding;
    }

    var key: SecretKey = undefined;
    // Returning copies into the result slot before deferred cleanup; wipe this
    // decoder-local copy on both success and failure paths.
    defer key.deinit();
    decodeLowerHex32(encoded[0..hex_32_bytes], &key.bytes) catch
        return error.InvalidSecretEncoding;
    var any_nonzero = false;
    for (key.bytes) |byte| any_nonzero = any_nonzero or byte != 0;
    if (!any_nonzero) return error.AllZeroSecret;
    return key;
}

pub fn parseAuth(line: []const u8) WireError!Auth {
    const fields = try exactFields(5, line, max_wire_line_bytes);
    if (!std.mem.eql(u8, fields[0], "AUTH")) return error.InvalidFrame;
    try requireVersion(fields[1]);
    return .{
        .instance_id = try validateInstanceId(fields[2]),
        .credential_id = try Identifier.init(fields[3]),
        .client_nonce = try parseLowerHex32(fields[4]),
    };
}

pub fn writeAuth(writer: *std.Io.Writer, auth: Auth) std.Io.Writer.Error!void {
    const nonce_hex = std.fmt.bytesToHex(auth.client_nonce, .lower);
    try writer.print(
        "AUTH 1 {s} {s} {s}\n",
        .{ auth.instance_id.slice(), auth.credential_id.slice(), &nonce_hex },
    );
}

pub fn parseChallenge(line: []const u8) WireError!Challenge {
    const fields = try exactFields(3, line, max_wire_line_bytes);
    if (!std.mem.eql(u8, fields[0], "CHALLENGE")) return error.InvalidFrame;
    try requireVersion(fields[1]);
    return .{ .server_nonce = try parseLowerHex32(fields[2]) };
}

pub fn writeChallenge(writer: *std.Io.Writer, challenge: Challenge) std.Io.Writer.Error!void {
    const nonce_hex = std.fmt.bytesToHex(challenge.server_nonce, .lower);
    try writer.print("CHALLENGE 1 {s}\n", .{&nonce_hex});
}

pub fn parseProof(line: []const u8) WireError!Proof {
    const fields = try exactFields(3, line, max_wire_line_bytes);
    if (!std.mem.eql(u8, fields[0], "PROOF")) return error.InvalidFrame;
    try requireVersion(fields[1]);
    return .{ .mac = try parseLowerHex32(fields[2]) };
}

pub fn writeProof(writer: *std.Io.Writer, proof: Proof) std.Io.Writer.Error!void {
    const mac_hex = std.fmt.bytesToHex(proof.mac, .lower);
    try writer.print("PROOF 1 {s}\n", .{&mac_hex});
}

pub fn parseAuthAck(line: []const u8) WireError!AuthAck {
    const fields = try exactFields(4, line, max_wire_line_bytes);
    if (!std.mem.eql(u8, fields[0], "OK") or
        !std.mem.eql(u8, fields[1], "AUTH")) return error.InvalidFrame;
    try requireVersion(fields[2]);
    return .{ .server_mac = try parseLowerHex32(fields[3]) };
}

pub fn writeAuthAck(writer: *std.Io.Writer, ack: AuthAck) std.Io.Writer.Error!void {
    const mac_hex = std.fmt.bytesToHex(ack.server_mac, .lower);
    try writer.print("OK AUTH 1 {s}\n", .{&mac_hex});
}

pub fn parseRequestEnvelope(line: []const u8) WireError!RequestEnvelope {
    const outer = try parseOuter("REQ", line, protocol.max_line_bytes, max_request_envelope_bytes);
    return .{
        .sequence = outer.sequence,
        .request_mac = outer.mac,
        .raw_command = outer.raw,
    };
}

pub fn writeRequestEnvelope(
    writer: *std.Io.Writer,
    envelope: RequestEnvelope,
) (WireError || std.Io.Writer.Error)!void {
    try validatePayload(envelope.sequence, envelope.raw_command, protocol.max_line_bytes);
    const mac_hex = std.fmt.bytesToHex(envelope.request_mac, .lower);
    try writer.print("REQ 1 {s} {s}\n", .{ &mac_hex, envelope.raw_command });
}

pub fn parseResponseEnvelope(line: []const u8) WireError!ResponseEnvelope {
    const outer = try parseOuter("RES", line, max_response_bytes, max_response_envelope_bytes);
    return .{
        .sequence = outer.sequence,
        .response_mac = outer.mac,
        .raw_response = outer.raw,
    };
}

pub fn writeResponseEnvelope(
    writer: *std.Io.Writer,
    envelope: ResponseEnvelope,
) (WireError || std.Io.Writer.Error)!void {
    try validatePayload(envelope.sequence, envelope.raw_response, max_response_bytes);
    const mac_hex = std.fmt.bytesToHex(envelope.response_mac, .lower);
    try writer.print("RES 1 {s} {s}\n", .{ &mac_hex, envelope.raw_response });
}

pub fn writeAuthenticationFailure(writer: *std.Io.Writer) std.Io.Writer.Error!void {
    try writer.writeAll(authentication_failure_line);
}

pub fn computeProof(key: *const SecretKey, transcript: Transcript) Mac {
    var hmac = transcriptHmac(&key.bytes, proof_domain, transcript);
    var result: Mac = undefined;
    hmac.final(&result);
    return result;
}

pub fn deriveSessionKey(key: *const SecretKey, transcript: Transcript) SessionKey {
    var prk = HkdfSha256.extract(session_salt_domain, &key.bytes);
    defer std.crypto.secureZero(u8, &prk);
    var info: [512]u8 = undefined;
    defer std.crypto.secureZero(u8, &info);
    const info_len = encodeTranscript(&info, session_domain, transcript);
    var result: SessionKey = undefined;
    HkdfSha256.expand(&result.bytes, info[0..info_len], prk);
    return result;
}

pub fn computeServerMac(session_key: *const SessionKey, transcript: Transcript) Mac {
    var hmac = transcriptHmac(&session_key.bytes, server_mac_domain, transcript);
    var result: Mac = undefined;
    hmac.final(&result);
    return result;
}

pub fn verifyServerMac(
    session_key: *const SessionKey,
    transcript: Transcript,
    supplied: Mac,
) AuthenticationError!void {
    var expected = computeServerMac(session_key, transcript);
    defer std.crypto.secureZero(u8, &expected);
    if (!std.crypto.timing_safe.eql(Mac, expected, supplied)) {
        return error.AuthenticationFailed;
    }
}

pub fn computeRequestMac(
    session_key: *const SessionKey,
    instance_id: []const u8,
    credential_id: []const u8,
    sequence: u64,
    raw_command: []const u8,
) IntegrityError!Mac {
    return computePayloadMac(
        &session_key.bytes,
        request_mac_domain,
        instance_id,
        credential_id,
        sequence,
        raw_command,
        protocol.max_line_bytes,
    );
}

pub fn verifyRequestMac(
    session_key: *const SessionKey,
    instance_id: []const u8,
    credential_id: []const u8,
    sequence: u64,
    raw_command: []const u8,
    supplied: Mac,
) IntegrityError!void {
    var expected = try computeRequestMac(
        session_key,
        instance_id,
        credential_id,
        sequence,
        raw_command,
    );
    defer std.crypto.secureZero(u8, &expected);
    if (!std.crypto.timing_safe.eql(Mac, expected, supplied)) {
        return error.IntegrityFailed;
    }
}

pub fn computeResponseMac(
    session_key: *const SessionKey,
    instance_id: []const u8,
    credential_id: []const u8,
    sequence: u64,
    raw_response: []const u8,
) IntegrityError!Mac {
    return computePayloadMac(
        &session_key.bytes,
        response_mac_domain,
        instance_id,
        credential_id,
        sequence,
        raw_response,
        max_response_bytes,
    );
}

pub fn verifyResponseMac(
    session_key: *const SessionKey,
    instance_id: []const u8,
    credential_id: []const u8,
    sequence: u64,
    raw_response: []const u8,
    supplied: Mac,
) IntegrityError!void {
    var expected = try computeResponseMac(
        session_key,
        instance_id,
        credential_id,
        sequence,
        raw_response,
    );
    defer std.crypto.secureZero(u8, &expected);
    if (!std.crypto.timing_safe.eql(Mac, expected, supplied)) {
        return error.IntegrityFailed;
    }
}

pub fn requestKind(request: protocol.Request) RequestKind {
    return switch (request) {
        .ping => .ping,
        .add_user => .add_user,
        .deposit => .deposit,
        .fund_deposit => .fund_deposit,
        .fund_withdraw => .fund_withdraw,
        .funding_status => .funding_status,
        .place => .place,
        .move => .move,
        .reduce => .reduce,
        .cancel => .cancel,
        .balance => .balance,
        .book => .book,
        .shutdown => .shutdown,
        .clearing_status => .clearing_status,
        .clearing_operations => .clearing_operations,
    };
}

pub fn authorize(principal: Principal, request: protocol.Request) AuthorizationError!void {
    switch (principal) {
        .operator => switch (request) {
            .ping,
            .fund_deposit,
            .fund_withdraw,
            .funding_status,
            .book,
            .shutdown,
            .clearing_status,
            .clearing_operations,
            => {},
            .add_user, .deposit, .place, .move, .reduce, .cancel, .balance => return error.Forbidden,
        },
        .trader => |uid| switch (request) {
            .ping, .book => {},
            .place => |value| if (value.uid != uid) return error.CrossUid,
            .move => |value| if (value.uid != uid) return error.CrossUid,
            .reduce => |value| if (value.uid != uid) return error.CrossUid,
            .cancel => |value| if (value.uid != uid) return error.CrossUid,
            .balance => |value| if (value.uid != uid) return error.CrossUid,
            .add_user,
            .deposit,
            .fund_deposit,
            .fund_withdraw,
            .funding_status,
            .shutdown,
            .clearing_status,
            .clearing_operations,
            => return error.Forbidden,
        },
    }
}

const CredentialSpec = struct {
    identity: Identity,
    secret_file_env: Identifier,
};

const ProfileDocument = struct {
    instance_id: Identifier,
    credentials: [max_credentials]CredentialSpec,
    credential_count: usize,
};

fn parseProfile(bytes: []const u8, allocator: std.mem.Allocator) ProfileParseError!ProfileDocument {
    if (bytes.len == 0) return error.EmptyProfile;
    if (bytes.len > max_profile_bytes) return error.ProfileTooLarge;
    if (!jsonNestingWithinLimit(bytes, 8)) return error.JsonNestingTooDeep;

    var parsed = std.json.parseFromSlice(std.json.Value, allocator, bytes, .{
        .duplicate_field_behavior = .@"error",
        .parse_numbers = false,
        .max_value_len = 256,
    }) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return error.InvalidJson;
    };
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidRoot,
    };
    try exactObjectFields(root, &.{ "version", "instance_id", "credentials" });
    if (try requiredInt(u16, root, "version") != protocol_version) {
        return error.UnsupportedVersion;
    }
    const instance_id = validateInstanceId(try requiredString(root, "instance_id")) catch
        return error.InvalidIdentifier;
    const values = try requiredArray(root, "credentials");
    if (values.items.len == 0) return error.InvalidCredentialCount;
    if (values.items.len > max_credentials) return error.TooManyCredentials;

    var document: ProfileDocument = .{
        .instance_id = instance_id,
        .credentials = undefined,
        .credential_count = values.items.len,
    };
    var has_trader = false;
    var has_operator = false;
    for (values.items, 0..) |value, index| {
        const object = try valueObject(value);
        const role = try requiredString(object, "role");
        const principal: Principal = if (std.mem.eql(u8, role, "trader")) blk: {
            try exactObjectFields(object, &.{
                "credential_id", "principal_id", "role", "uid", "secret_file_env",
            });
            has_trader = true;
            break :blk .{ .trader = try requiredPositiveInt(i64, object, "uid") };
        } else if (std.mem.eql(u8, role, "operator")) blk: {
            try exactObjectFields(object, &.{
                "credential_id", "principal_id", "role", "secret_file_env",
            });
            has_operator = true;
            break :blk .operator;
        } else return error.InvalidRole;

        const credential_id = Identifier.init(try requiredString(object, "credential_id")) catch
            return error.InvalidIdentifier;
        const principal_id = Identifier.init(try requiredString(object, "principal_id")) catch
            return error.InvalidIdentifier;
        const env_text = try requiredString(object, "secret_file_env");
        if (!isEnvironmentName(env_text)) return error.InvalidEnvironmentName;
        const secret_file_env = Identifier.init(env_text) catch return error.InvalidEnvironmentName;

        for (document.credentials[0..index]) |prior| {
            if (std.mem.eql(
                u8,
                prior.identity.credential_id.slice(),
                credential_id.slice(),
            )) return error.DuplicateCredentialId;
            if (std.mem.eql(u8, prior.secret_file_env.slice(), secret_file_env.slice())) {
                return error.DuplicateSecretFileEnvironment;
            }
            if (std.mem.eql(u8, prior.identity.principal_id.slice(), principal_id.slice())) {
                if (!std.meta.eql(prior.identity.principal, principal)) {
                    return error.InconsistentPrincipal;
                }
            }
        }
        document.credentials[index] = .{
            .identity = .{
                .credential_id = credential_id,
                .principal_id = principal_id,
                .principal = principal,
            },
            .secret_file_env = secret_file_env,
        };
    }
    if (!has_trader) return error.MissingTrader;
    if (!has_operator) return error.MissingOperator;
    return document;
}

fn isSafeToken(text: []const u8) bool {
    if (text.len == 0 or text.len > max_identifier_bytes) return false;
    for (text) |byte| {
        if (!std.ascii.isAlphanumeric(byte) and byte != '.' and byte != '_' and byte != '-') {
            return false;
        }
    }
    return true;
}

fn isEnvironmentName(text: []const u8) bool {
    if (text.len == 0 or text.len > max_environment_name_bytes) return false;
    if (text[0] != '_' and !std.ascii.isUpper(text[0])) return false;
    for (text[1..]) |byte| {
        if (byte != '_' and !std.ascii.isUpper(byte) and !std.ascii.isDigit(byte)) {
            return false;
        }
    }
    return true;
}

fn exactObjectFields(
    object: std.json.ObjectMap,
    expected: []const []const u8,
) ProfileParseError!void {
    if (object.count() != expected.len) return error.InvalidObjectShape;
    for (expected) |name| {
        if (!object.contains(name)) return error.InvalidObjectShape;
    }
}

fn valueObject(value: std.json.Value) ProfileParseError!std.json.ObjectMap {
    return switch (value) {
        .object => |object| object,
        else => error.InvalidFieldType,
    };
}

fn requiredArray(object: std.json.ObjectMap, key: []const u8) ProfileParseError!std.json.Array {
    const value = object.get(key) orelse return error.MissingField;
    return switch (value) {
        .array => |array| array,
        else => error.InvalidFieldType,
    };
}

fn requiredString(object: std.json.ObjectMap, key: []const u8) ProfileParseError![]const u8 {
    const value = object.get(key) orelse return error.MissingField;
    return switch (value) {
        .string => |text| text,
        else => error.InvalidFieldType,
    };
}

fn requiredPositiveInt(
    comptime T: type,
    object: std.json.ObjectMap,
    key: []const u8,
) ProfileParseError!T {
    const integer = try requiredInt(T, object, key);
    if (integer <= 0) return error.InvalidUid;
    return integer;
}

fn requiredInt(
    comptime T: type,
    object: std.json.ObjectMap,
    key: []const u8,
) ProfileParseError!T {
    const value = object.get(key) orelse return error.MissingField;
    const text = switch (value) {
        .number_string => |number| number,
        else => return error.InvalidFieldType,
    };
    if (text.len == 0) return error.InvalidInteger;
    for (text) |byte| if (!std.ascii.isDigit(byte)) return error.InvalidInteger;
    return std.fmt.parseInt(T, text, 10) catch return error.InvalidInteger;
}

fn jsonNestingWithinLimit(bytes: []const u8, maximum: usize) bool {
    var depth: usize = 0;
    var in_string = false;
    var escaped = false;
    for (bytes) |byte| {
        if (in_string) {
            if (escaped) {
                escaped = false;
            } else if (byte == '\\') {
                escaped = true;
            } else if (byte == '"') {
                in_string = false;
            }
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

fn exactFields(
    comptime count: usize,
    line: []const u8,
    maximum: usize,
) WireError![count][]const u8 {
    try validateWireLine(line, maximum);
    var result: [count][]const u8 = undefined;
    var iterator = std.mem.splitScalar(u8, line, ' ');
    for (&result) |*field| {
        field.* = iterator.next() orelse return error.InvalidFrame;
        if (field.len == 0) return error.InvalidFrame;
    }
    if (iterator.next() != null) return error.InvalidFrame;
    return result;
}

fn requireVersion(field: []const u8) WireError!void {
    if (!std.mem.eql(u8, field, "1")) return error.UnsupportedVersion;
}

const ParsedOuter = struct {
    sequence: u64,
    mac: Mac,
    raw: []const u8,
};

fn parseOuter(
    comptime label: []const u8,
    line: []const u8,
    max_raw: usize,
    max_line: usize,
) WireError!ParsedOuter {
    try validateWireLine(line, max_line);
    const prefix = label ++ " ";
    if (!std.mem.startsWith(u8, line, prefix)) return error.InvalidFrame;
    const after_label = line[prefix.len..];
    const sequence_end = std.mem.findScalar(u8, after_label, ' ') orelse
        return error.InvalidFrame;
    const sequence_text = after_label[0..sequence_end];
    if (sequence_text.len == 0) return error.InvalidFrame;
    const sequence = std.fmt.parseInt(u64, sequence_text, 10) catch
        return error.InvalidSequence;
    if (sequence != sequence_one or !std.mem.eql(u8, sequence_text, "1")) {
        return error.InvalidSequence;
    }
    const after_sequence = after_label[sequence_end + 1 ..];
    if (after_sequence.len < hex_32_bytes + 1 or after_sequence[hex_32_bytes] != ' ') {
        return error.InvalidFrame;
    }
    const mac = try parseLowerHex32(after_sequence[0..hex_32_bytes]);
    const raw = after_sequence[hex_32_bytes + 1 ..];
    try validateRaw(raw, max_raw);
    return .{ .sequence = sequence, .mac = mac, .raw = raw };
}

fn validateWireLine(line: []const u8, maximum: usize) WireError!void {
    if (line.len == 0) return error.EmptyLine;
    if (line.len > maximum) return error.LineTooLong;
    for (line) |byte| {
        if (byte < 0x20 or byte == 0x7f) return error.ControlCharacter;
    }
}

fn validateRaw(raw: []const u8, maximum: usize) WireError!void {
    if (raw.len == 0 or raw.len > maximum) return error.InvalidPayloadLength;
    for (raw) |byte| {
        if (byte < 0x20 or byte == 0x7f) return error.ControlCharacter;
    }
}

fn validatePayload(sequence: u64, raw: []const u8, maximum: usize) WireError!void {
    if (sequence != sequence_one) return error.InvalidSequence;
    try validateRaw(raw, maximum);
}

fn parseLowerHex32(text: []const u8) WireError!Mac {
    var result: Mac = undefined;
    try decodeLowerHex32(text, &result);
    return result;
}

fn decodeLowerHex32(text: []const u8, out: *[32]u8) WireError!void {
    if (text.len != hex_32_bytes) return error.InvalidHex;
    for (text) |byte| {
        if (!std.ascii.isDigit(byte) and (byte < 'a' or byte > 'f')) {
            return error.InvalidHex;
        }
    }
    _ = std.fmt.hexToBytes(out, text) catch return error.InvalidHex;
}

fn transcriptHmac(key: []const u8, domain: []const u8, transcript: Transcript) HmacSha256 {
    var hmac = HmacSha256.init(key);
    updateField(&hmac, domain);
    updateVersion(&hmac);
    updateField(&hmac, transcript.instance_id.slice());
    updateField(&hmac, transcript.credential_id.slice());
    updateField(&hmac, &transcript.client_nonce);
    updateField(&hmac, &transcript.server_nonce);
    return hmac;
}

fn encodeTranscript(buffer: []u8, domain: []const u8, transcript: Transcript) usize {
    var offset: usize = 0;
    appendField(buffer, &offset, domain);
    buffer[offset] = protocol_version;
    offset += 1;
    appendField(buffer, &offset, transcript.instance_id.slice());
    appendField(buffer, &offset, transcript.credential_id.slice());
    appendField(buffer, &offset, &transcript.client_nonce);
    appendField(buffer, &offset, &transcript.server_nonce);
    return offset;
}

fn computePayloadMac(
    key: []const u8,
    domain: []const u8,
    instance_id: []const u8,
    credential_id: []const u8,
    sequence: u64,
    raw: []const u8,
    maximum: usize,
) IntegrityError!Mac {
    if (sequence != sequence_one) return error.InvalidSequence;
    _ = validateInstanceId(instance_id) catch return error.InvalidPayload;
    if (!isSafeToken(credential_id) or raw.len == 0 or raw.len > maximum) {
        return error.InvalidPayload;
    }
    for (raw) |byte| if (byte < 0x20 or byte == 0x7f) return error.InvalidPayload;

    var hmac = HmacSha256.init(key);
    updateField(&hmac, domain);
    updateVersion(&hmac);
    updateField(&hmac, instance_id);
    updateField(&hmac, credential_id);
    var sequence_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &sequence_bytes, sequence, .big);
    updateField(&hmac, &sequence_bytes);
    updateField(&hmac, raw);
    var result: Mac = undefined;
    hmac.final(&result);
    return result;
}

fn updateVersion(hmac: *HmacSha256) void {
    const version = [_]u8{protocol_version};
    hmac.update(&version);
}

fn updateField(hmac: *HmacSha256, bytes: []const u8) void {
    var length: [4]u8 = undefined;
    std.mem.writeInt(u32, &length, @intCast(bytes.len), .big);
    hmac.update(&length);
    hmac.update(bytes);
}

fn appendField(buffer: []u8, offset: *usize, bytes: []const u8) void {
    std.debug.assert(offset.* + 4 + bytes.len <= buffer.len);
    var length: [4]u8 = undefined;
    std.mem.writeInt(u32, &length, @intCast(bytes.len), .big);
    @memcpy(buffer[offset.*..][0..4], &length);
    offset.* += 4;
    @memcpy(buffer[offset.*..][0..bytes.len], bytes);
    offset.* += bytes.len;
}

const test_instance_id = "0e46f90e-c38b-487a-b9f9-630f7839cc3f";

fn testAuth() !Auth {
    return .{
        .instance_id = try validateInstanceId(test_instance_id),
        .credential_id = try Identifier.init("trader-1001-primary"),
        .client_nonce = [_]u8{0x11} ** nonce_bytes,
    };
}

test "wire v1 frames round trip canonically" {
    const auth = try testAuth();
    const challenge = Challenge{ .server_nonce = [_]u8{0x22} ** nonce_bytes };
    const proof = Proof{ .mac = [_]u8{0x33} ** mac_bytes };
    const ack = AuthAck{ .server_mac = [_]u8{0x44} ** mac_bytes };

    var auth_buffer: [256]u8 = undefined;
    var auth_writer = std.Io.Writer.fixed(&auth_buffer);
    try writeAuth(&auth_writer, auth);
    try std.testing.expectEqualDeep(
        auth,
        try parseAuth(std.mem.trimEnd(u8, auth_writer.buffered(), "\n")),
    );

    var challenge_buffer: [128]u8 = undefined;
    var challenge_writer = std.Io.Writer.fixed(&challenge_buffer);
    try writeChallenge(&challenge_writer, challenge);
    try std.testing.expectEqualDeep(
        challenge,
        try parseChallenge(std.mem.trimEnd(u8, challenge_writer.buffered(), "\n")),
    );

    var proof_buffer: [128]u8 = undefined;
    var proof_writer = std.Io.Writer.fixed(&proof_buffer);
    try writeProof(&proof_writer, proof);
    try std.testing.expectEqualDeep(
        proof,
        try parseProof(std.mem.trimEnd(u8, proof_writer.buffered(), "\n")),
    );

    var ack_buffer: [128]u8 = undefined;
    var ack_writer = std.Io.Writer.fixed(&ack_buffer);
    try writeAuthAck(&ack_writer, ack);
    try std.testing.expectEqualDeep(
        ack,
        try parseAuthAck(std.mem.trimEnd(u8, ack_writer.buffered(), "\n")),
    );
    try std.testing.expect(std.mem.endsWith(u8, ack_writer.buffered(), "\n"));
}

test "wire parser rejects noncanonical fixed frames" {
    const zero_hex = "00" ** 32;
    try std.testing.expectError(
        error.InvalidFrame,
        parseAuth("AUTH  1 0e46f90e-c38b-487a-b9f9-630f7839cc3f cred " ++ zero_hex),
    );
    try std.testing.expectError(
        error.UnsupportedVersion,
        parseChallenge("CHALLENGE 2 " ++ zero_hex),
    );
    try std.testing.expectError(
        error.InvalidHex,
        parseProof("PROOF 1 " ++ ("AA" ** 32)),
    );
    try std.testing.expectError(
        error.ControlCharacter,
        parseAuthAck("OK AUTH 1 " ++ zero_hex ++ "\r"),
    );
    try std.testing.expectError(
        error.InvalidIdentifier,
        parseAuth("AUTH 1 exchange-core-local cred " ++ zero_hex),
    );
}

test "generic authentication failure is exact and contains no credential detail" {
    try std.testing.expectEqualStrings(
        "ERR authentication AuthenticationFailed\n",
        authentication_failure_line,
    );
    var buffer: [64]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    try writeAuthenticationFailure(&writer);
    try std.testing.expectEqualStrings(authentication_failure_line, writer.buffered());
}

test "request and response envelopes preserve exact raw suffix" {
    const request_mac = [_]u8{0x55} ** mac_bytes;
    const raw_command = " place 7 1001 241 10 2 bid ";
    var request_buffer: [max_request_envelope_bytes + 1]u8 = undefined;
    var request_writer = std.Io.Writer.fixed(&request_buffer);
    try writeRequestEnvelope(&request_writer, .{
        .sequence = sequence_one,
        .request_mac = request_mac,
        .raw_command = raw_command,
    });
    const request_line = std.mem.trimEnd(u8, request_writer.buffered(), "\n");
    const parsed_request = try parseRequestEnvelope(request_line);
    try std.testing.expectEqual(sequence_one, parsed_request.sequence);
    try std.testing.expectEqual(request_mac, parsed_request.request_mac);
    try std.testing.expectEqualStrings(raw_command, parsed_request.raw_command);

    const response_mac = [_]u8{0x66} ** mac_bytes;
    const raw_response = "OK balance uid=1001 currency=15 value=42";
    var response_buffer: [256]u8 = undefined;
    var response_writer = std.Io.Writer.fixed(&response_buffer);
    try writeResponseEnvelope(&response_writer, .{
        .sequence = sequence_one,
        .response_mac = response_mac,
        .raw_response = raw_response,
    });
    const response_line = std.mem.trimEnd(u8, response_writer.buffered(), "\n");
    const parsed_response = try parseResponseEnvelope(response_line);
    try std.testing.expectEqual(sequence_one, parsed_response.sequence);
    try std.testing.expectEqual(response_mac, parsed_response.response_mac);
    try std.testing.expectEqualStrings(raw_response, parsed_response.raw_response);
}

test "outer envelope parser enforces sequence controls and bounds" {
    const zero_hex = "00" ** 32;
    try std.testing.expectError(
        error.InvalidSequence,
        parseRequestEnvelope("REQ 2 " ++ zero_hex ++ " ping"),
    );
    try std.testing.expectError(
        error.InvalidPayloadLength,
        parseRequestEnvelope("REQ 1 " ++ zero_hex ++ " "),
    );
    try std.testing.expectError(
        error.ControlCharacter,
        parseRequestEnvelope("REQ 1 " ++ zero_hex ++ " ping\t"),
    );
    try std.testing.expectError(
        error.InvalidFrame,
        parseRequestEnvelope("REQ 1 " ++ zero_hex),
    );

    var oversized_raw: [protocol.max_line_bytes + 1]u8 = undefined;
    @memset(&oversized_raw, 'x');
    var line: [max_request_envelope_bytes + 1]u8 = undefined;
    const prefix = "REQ 1 " ++ zero_hex ++ " ";
    @memcpy(line[0..prefix.len], prefix);
    @memcpy(line[prefix.len..], &oversized_raw);
    try std.testing.expectError(error.LineTooLong, parseRequestEnvelope(&line));
}

test "proof session server ack and payload MACs bind all context" {
    var secret = SecretKey{ .bytes = [_]u8{0x71} ** secret_key_bytes };
    defer secret.deinit();
    const auth = try testAuth();
    const challenge = Challenge{ .server_nonce = [_]u8{0x22} ** nonce_bytes };
    const transcript = Transcript.init(auth, challenge);

    var proof = computeProof(&secret, transcript);
    defer std.crypto.secureZero(u8, &proof);
    var changed_auth = auth;
    changed_auth.client_nonce[0] ^= 1;
    var changed_proof = computeProof(&secret, Transcript.init(changed_auth, challenge));
    defer std.crypto.secureZero(u8, &changed_proof);
    try std.testing.expect(!std.crypto.timing_safe.eql(Mac, proof, changed_proof));

    var replay_challenge = challenge;
    replay_challenge.server_nonce[0] ^= 1;
    var replay_proof = computeProof(&secret, Transcript.init(auth, replay_challenge));
    defer std.crypto.secureZero(u8, &replay_proof);
    try std.testing.expect(!std.crypto.timing_safe.eql(Mac, proof, replay_proof));

    var changed_instance_auth = auth;
    changed_instance_auth.instance_id = try validateInstanceId(
        "1e46f90e-c38b-487a-b9f9-630f7839cc3f",
    );
    var changed_instance_proof = computeProof(
        &secret,
        Transcript.init(changed_instance_auth, challenge),
    );
    defer std.crypto.secureZero(u8, &changed_instance_proof);
    try std.testing.expect(!std.crypto.timing_safe.eql(Mac, proof, changed_instance_proof));

    var changed_credential_auth = auth;
    changed_credential_auth.credential_id = try Identifier.init("trader-1001-rotated");
    var changed_credential_proof = computeProof(
        &secret,
        Transcript.init(changed_credential_auth, challenge),
    );
    defer std.crypto.secureZero(u8, &changed_credential_proof);
    try std.testing.expect(!std.crypto.timing_safe.eql(Mac, proof, changed_credential_proof));

    var session = deriveSessionKey(&secret, transcript);
    defer session.deinit();
    var server_mac = computeServerMac(&session, transcript);
    defer std.crypto.secureZero(u8, &server_mac);
    try verifyServerMac(&session, transcript, server_mac);
    var tampered_server_mac = server_mac;
    tampered_server_mac[0] ^= 1;
    try std.testing.expectError(
        error.AuthenticationFailed,
        verifyServerMac(&session, transcript, tampered_server_mac),
    );

    const raw_request = "place 7 1001 241 10 2 bid";
    var request_mac = try computeRequestMac(
        &session,
        auth.instance_id.slice(),
        auth.credential_id.slice(),
        sequence_one,
        raw_request,
    );
    defer std.crypto.secureZero(u8, &request_mac);
    try verifyRequestMac(
        &session,
        auth.instance_id.slice(),
        auth.credential_id.slice(),
        sequence_one,
        raw_request,
        request_mac,
    );
    try std.testing.expectError(
        error.IntegrityFailed,
        verifyRequestMac(
            &session,
            auth.instance_id.slice(),
            auth.credential_id.slice(),
            sequence_one,
            "place 7 1001 241 10 3 bid",
            request_mac,
        ),
    );
    try std.testing.expectError(
        error.InvalidSequence,
        verifyRequestMac(
            &session,
            auth.instance_id.slice(),
            auth.credential_id.slice(),
            2,
            raw_request,
            request_mac,
        ),
    );

    const raw_clearing_request = "clearing-status 910000000000000000000";
    var clearing_request_mac = try computeRequestMac(
        &session,
        auth.instance_id.slice(),
        auth.credential_id.slice(),
        sequence_one,
        raw_clearing_request,
    );
    defer std.crypto.secureZero(u8, &clearing_request_mac);
    try verifyRequestMac(
        &session,
        auth.instance_id.slice(),
        auth.credential_id.slice(),
        sequence_one,
        raw_clearing_request,
        clearing_request_mac,
    );
    try std.testing.expectError(
        error.IntegrityFailed,
        verifyRequestMac(
            &session,
            auth.instance_id.slice(),
            auth.credential_id.slice(),
            sequence_one,
            "clearing-status 910000000000000000001",
            clearing_request_mac,
        ),
    );
    try std.testing.expect(!std.crypto.timing_safe.eql(
        Mac,
        request_mac,
        clearing_request_mac,
    ));

    var other_secret = SecretKey{ .bytes = [_]u8{0x72} ** secret_key_bytes };
    defer other_secret.deinit();
    var other_session = deriveSessionKey(&other_secret, transcript);
    defer other_session.deinit();
    try std.testing.expectError(
        error.IntegrityFailed,
        verifyRequestMac(
            &other_session,
            auth.instance_id.slice(),
            auth.credential_id.slice(),
            sequence_one,
            raw_request,
            request_mac,
        ),
    );

    const raw_response = "OK tag=response code=0 sequence=1 events=0";
    var response_mac = try computeResponseMac(
        &session,
        auth.instance_id.slice(),
        auth.credential_id.slice(),
        sequence_one,
        raw_response,
    );
    defer std.crypto.secureZero(u8, &response_mac);
    try verifyResponseMac(
        &session,
        auth.instance_id.slice(),
        auth.credential_id.slice(),
        sequence_one,
        raw_response,
        response_mac,
    );
    try std.testing.expectError(
        error.IntegrityFailed,
        verifyResponseMac(
            &session,
            auth.instance_id.slice(),
            auth.credential_id.slice(),
            sequence_one,
            "OK tag=response code=1 sequence=1 events=0",
            response_mac,
        ),
    );
    try std.testing.expect(!std.crypto.timing_safe.eql(Mac, request_mac, response_mac));

    try std.testing.expectEqualStrings(
        "f1ed2d3743103245b331c670f47c114ad1881ff124366cbae37aefaa219fd828",
        &std.fmt.bytesToHex(proof, .lower),
    );
    try std.testing.expectEqualStrings(
        "5cd549b4ed72e639ae87a0241d3cbfc1ba4ebe3750ba3a4b397cdd62632b1c13",
        &std.fmt.bytesToHex(session.bytes, .lower),
    );
    try std.testing.expectEqualStrings(
        "f2d8bd17ea44d63694380080d0f309bcb9732ae6e8acccfdd683a83288e40fca",
        &std.fmt.bytesToHex(server_mac, .lower),
    );
    try std.testing.expectEqualStrings(
        "b9d677590465e0bfbe0a22df4d67846f08fb6116cb20f11b7682b9e39fef9967",
        &std.fmt.bytesToHex(request_mac, .lower),
    );
    try std.testing.expectEqualStrings(
        "0aa7ba3af461885d02f768b01b695f52048e0a0df68be1fa86fada6fe814fe18",
        &std.fmt.bytesToHex(response_mac, .lower),
    );
}

test "authorization is exhaustive across request kinds and roles" {
    const operation_id = try protocol.FundingOperationId.parse(
        "123e4567-e89b-12d3-a456-426614174000",
    );
    const payment_id: u128 = 910000000000000000000;
    const requests = [_]protocol.Request{
        .ping,
        .{ .add_user = .{ .uid = 1001 } },
        .{ .deposit = .{
            .uid = 1001,
            .currency = 15,
            .amount = 10,
            .transaction_id = 1,
        } },
        .{ .fund_deposit = .{
            .uid = 1001,
            .currency = 15,
            .amount = 10,
            .operation_id = operation_id,
        } },
        .{ .fund_withdraw = .{
            .uid = 1001,
            .currency = 15,
            .amount = 10,
            .operation_id = operation_id,
        } },
        .{ .funding_status = operation_id },
        .{ .place = .{
            .order_id = 7,
            .uid = 1001,
            .symbol = 241,
            .price = 10,
            .size = 2,
            .side = .bid,
            .time_in_force = .gtc,
        } },
        .{ .move = .{
            .order_id = 7,
            .uid = 1001,
            .symbol = 241,
            .new_price = 11,
        } },
        .{ .reduce = .{
            .order_id = 7,
            .uid = 1001,
            .symbol = 241,
            .size = 1,
        } },
        .{ .cancel = .{
            .order_id = 7,
            .uid = 1001,
            .symbol = 241,
        } },
        .{ .balance = .{ .uid = 1001, .currency = 15 } },
        .{ .book = .{ .symbol = 241, .depth = 20 } },
        .shutdown,
        .{ .clearing_status = payment_id },
        .clearing_operations,
    };
    const expected_kinds = [_]RequestKind{
        .ping,
        .add_user,
        .deposit,
        .fund_deposit,
        .fund_withdraw,
        .funding_status,
        .place,
        .move,
        .reduce,
        .cancel,
        .balance,
        .book,
        .shutdown,
        .clearing_status,
        .clearing_operations,
    };
    const trader_allowed = [_]bool{
        true,  false, false, false, false, false, true, true, true, true, true, true, false, false,
        false,
    };
    const operator_allowed = [_]bool{
        true, false, false, true, true, true, false, false, false, false, false, true, true, true,
        true,
    };

    for (requests, 0..) |request, index| {
        try std.testing.expectEqual(expected_kinds[index], requestKind(request));
        if (trader_allowed[index]) {
            try authorize(.{ .trader = 1001 }, request);
        } else {
            try std.testing.expectError(
                error.Forbidden,
                authorize(.{ .trader = 1001 }, request),
            );
        }
        if (operator_allowed[index]) {
            try authorize(.operator, request);
        } else {
            try std.testing.expectError(error.Forbidden, authorize(.operator, request));
        }
    }
}

test "request kind operation values remain stable" {
    const kinds = [_]RequestKind{
        .ping,
        .add_user,
        .deposit,
        .fund_deposit,
        .fund_withdraw,
        .funding_status,
        .place,
        .move,
        .reduce,
        .cancel,
        .balance,
        .book,
        .shutdown,
        .clearing_status,
        .clearing_operations,
    };
    for (kinds, 0..) |kind, value| {
        try std.testing.expectEqual(@as(u8, @intCast(value)), @intFromEnum(kind));
    }
}

test "trader authorization reports every cross-UID request" {
    const cross_uid_requests = [_]protocol.Request{
        .{ .place = .{
            .order_id = 7,
            .uid = 2002,
            .symbol = 241,
            .price = 10,
            .size = 2,
            .side = .bid,
            .time_in_force = .gtc,
        } },
        .{ .move = .{
            .order_id = 7,
            .uid = 2002,
            .symbol = 241,
            .new_price = 11,
        } },
        .{ .reduce = .{
            .order_id = 7,
            .uid = 2002,
            .symbol = 241,
            .size = 1,
        } },
        .{ .cancel = .{
            .order_id = 7,
            .uid = 2002,
            .symbol = 241,
        } },
        .{ .balance = .{ .uid = 2002, .currency = 15 } },
    };
    for (cross_uid_requests) |request| {
        try std.testing.expectError(
            error.CrossUid,
            authorize(.{ .trader = 1001 }, request),
        );
    }
}

test "trader funding authorization fails closed before UID inspection" {
    const operation_id = try protocol.FundingOperationId.parse(
        "123e4567-e89b-12d3-a456-426614174000",
    );
    const requests = [_]protocol.Request{
        .{ .fund_deposit = .{
            .uid = 2002,
            .currency = 15,
            .amount = 10,
            .operation_id = operation_id,
        } },
        .{ .fund_withdraw = .{
            .uid = 2002,
            .currency = 15,
            .amount = 10,
            .operation_id = operation_id,
        } },
        .{ .funding_status = operation_id },
    };
    for (requests) |request| {
        try std.testing.expectError(
            error.Forbidden,
            authorize(.{ .trader = 1001 }, request),
        );
    }
}

test "secret wrappers securely zero observable storage" {
    var secret = SecretKey{ .bytes = [_]u8{0xaa} ** secret_key_bytes };
    secret.deinit();
    try std.testing.expectEqual([_]u8{0} ** secret_key_bytes, secret.bytes);

    var session = SessionKey{ .bytes = [_]u8{0xbb} ** secret_key_bytes };
    session.deinit();
    try std.testing.expectEqual([_]u8{0} ** secret_key_bytes, session.bytes);
}

const valid_profile_json =
    \\{
    \\  "version": 1,
    \\  "instance_id": "0e46f90e-c38b-487a-b9f9-630f7839cc3f",
    \\  "credentials": [
    \\    {"credential_id":"trader-primary","principal_id":"trader-1001","role":"trader","uid":1001,"secret_file_env":"TRADER_PRIMARY_KEY_FILE"},
    \\    {"credential_id":"trader-rotated","principal_id":"trader-1001","role":"trader","uid":1001,"secret_file_env":"TRADER_ROTATED_KEY_FILE"},
    \\    {"credential_id":"operator-primary","principal_id":"operator-local","role":"operator","secret_file_env":"OPERATOR_KEY_FILE"}
    \\  ]
    \\}
;

test "profile parser accepts consistent credential rotation" {
    const document = try parseProfile(valid_profile_json, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), document.credential_count);
    try std.testing.expectEqualStrings(test_instance_id, document.instance_id.slice());
    try std.testing.expectEqualStrings(
        "trader-primary",
        document.credentials[0].identity.credential_id.slice(),
    );
    try std.testing.expectEqual(
        Principal{ .trader = 1001 },
        document.credentials[1].identity.principal,
    );
    try std.testing.expectEqual(
        Principal.operator,
        document.credentials[2].identity.principal,
    );
}

test "profile parser rejects duplicate unknown and role-shape fields" {
    const cases = [_]struct {
        expected: anyerror,
        json: []const u8,
    }{
        .{
            .expected = error.InvalidJson,
            .json =
            \\{"version":1,"version":1,"instance_id":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","credentials":[]}
            ,
        },
        .{
            .expected = error.InvalidObjectShape,
            .json =
            \\{"version":1,"instance_id":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","credentials":[],"extra":true}
            ,
        },
        .{
            .expected = error.InvalidObjectShape,
            .json =
            \\{"version":1,"instance_id":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","credentials":[{"credential_id":"operator","principal_id":"operator","role":"operator","uid":1,"secret_file_env":"OPERATOR_KEY_FILE"}]}
            ,
        },
        .{
            .expected = error.InvalidObjectShape,
            .json =
            \\{"version":1,"instance_id":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","credentials":[{"credential_id":"trader","principal_id":"trader","role":"trader","secret_file_env":"TRADER_KEY_FILE"}]}
            ,
        },
    };
    for (cases) |case| {
        try std.testing.expectError(case.expected, parseProfile(case.json, std.testing.allocator));
    }
}

test "profile parser enforces identifiers roles and rotation invariants" {
    try std.testing.expectError(
        error.InvalidIdentifier,
        parseProfile(
            \\{"version":1,"instance_id":"exchange-core-local","credentials":[]}
        , std.testing.allocator),
    );
    try std.testing.expectError(
        error.InvalidIdentifier,
        parseProfile(
            \\{"version":1,"instance_id":"00000000-0000-0000-0000-000000000000","credentials":[]}
        , std.testing.allocator),
    );
    try std.testing.expectError(
        error.DuplicateCredentialId,
        parseProfile(
            \\{"version":1,"instance_id":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","credentials":[{"credential_id":"same","principal_id":"trader","role":"trader","uid":1,"secret_file_env":"TRADER_A_KEY_FILE"},{"credential_id":"same","principal_id":"trader","role":"trader","uid":1,"secret_file_env":"TRADER_B_KEY_FILE"},{"credential_id":"operator","principal_id":"operator","role":"operator","secret_file_env":"OPERATOR_KEY_FILE"}]}
        , std.testing.allocator),
    );
    try std.testing.expectError(
        error.DuplicateSecretFileEnvironment,
        parseProfile(
            \\{"version":1,"instance_id":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","credentials":[{"credential_id":"trader-a","principal_id":"trader","role":"trader","uid":1,"secret_file_env":"SHARED_KEY_FILE"},{"credential_id":"operator","principal_id":"operator","role":"operator","secret_file_env":"SHARED_KEY_FILE"}]}
        , std.testing.allocator),
    );
    try std.testing.expectError(
        error.InconsistentPrincipal,
        parseProfile(
            \\{"version":1,"instance_id":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","credentials":[{"credential_id":"trader-a","principal_id":"trader","role":"trader","uid":1,"secret_file_env":"TRADER_A_KEY_FILE"},{"credential_id":"trader-b","principal_id":"trader","role":"trader","uid":2,"secret_file_env":"TRADER_B_KEY_FILE"},{"credential_id":"operator","principal_id":"operator","role":"operator","secret_file_env":"OPERATOR_KEY_FILE"}]}
        , std.testing.allocator),
    );
    try std.testing.expectError(
        error.InvalidEnvironmentName,
        parseProfile(
            \\{"version":1,"instance_id":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","credentials":[{"credential_id":"trader","principal_id":"trader","role":"trader","uid":1,"secret_file_env":"lowercase"},{"credential_id":"operator","principal_id":"operator","role":"operator","secret_file_env":"OPERATOR_KEY_FILE"}]}
        , std.testing.allocator),
    );
    try std.testing.expectError(
        error.MissingOperator,
        parseProfile(
            \\{"version":1,"instance_id":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","credentials":[{"credential_id":"trader","principal_id":"trader","role":"trader","uid":1,"secret_file_env":"TRADER_KEY_FILE"}]}
        , std.testing.allocator),
    );
    try std.testing.expectError(
        error.MissingTrader,
        parseProfile(
            \\{"version":1,"instance_id":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","credentials":[{"credential_id":"operator","principal_id":"operator","role":"operator","secret_file_env":"OPERATOR_KEY_FILE"}]}
        , std.testing.allocator),
    );
    try std.testing.expectError(
        error.JsonNestingTooDeep,
        parseProfile("[[[[[[[[[]]]]]]]]]", std.testing.allocator),
    );
    try std.testing.expectError(
        error.InvalidUid,
        parseProfile(
            \\{"version":1,"instance_id":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","credentials":[{"credential_id":"trader","principal_id":"trader","role":"trader","uid":0,"secret_file_env":"TRADER_KEY_FILE"},{"credential_id":"operator","principal_id":"operator","role":"operator","secret_file_env":"OPERATOR_KEY_FILE"}]}
        , std.testing.allocator),
    );
    try std.testing.expectError(
        error.InvalidInteger,
        parseProfile(
            \\{"version":1,"instance_id":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","credentials":[{"credential_id":"trader","principal_id":"trader","role":"trader","uid":-1,"secret_file_env":"TRADER_KEY_FILE"},{"credential_id":"operator","principal_id":"operator","role":"operator","secret_file_env":"OPERATOR_KEY_FILE"}]}
        , std.testing.allocator),
    );
    try std.testing.expectError(
        error.InvalidRole,
        parseProfile(
            \\{"version":1,"instance_id":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","credentials":[{"credential_id":"trader","principal_id":"trader","role":"admin","uid":1,"secret_file_env":"TRADER_KEY_FILE"},{"credential_id":"operator","principal_id":"operator","role":"operator","secret_file_env":"OPERATOR_KEY_FILE"}]}
        , std.testing.allocator),
    );
}

test "profile parser enforces document and credential bounds" {
    try std.testing.expectError(error.EmptyProfile, parseProfile("", std.testing.allocator));
    try std.testing.expectError(
        error.InvalidCredentialCount,
        parseProfile(
            \\{"version":1,"instance_id":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","credentials":[]}
        , std.testing.allocator),
    );
    try std.testing.expectError(
        error.UnsupportedVersion,
        parseProfile(
            \\{"version":2,"instance_id":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","credentials":[]}
        , std.testing.allocator),
    );
    try std.testing.expectError(
        error.UnsupportedVersion,
        parseProfile(
            \\{"version":0,"instance_id":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","credentials":[]}
        , std.testing.allocator),
    );

    var oversized: [max_profile_bytes + 1]u8 = undefined;
    @memset(&oversized, ' ');
    try std.testing.expectError(
        error.ProfileTooLarge,
        parseProfile(&oversized, std.testing.allocator),
    );

    var credentials_buffer: [16 * 1024]u8 = undefined;
    var credentials_writer = std.Io.Writer.fixed(&credentials_buffer);
    try credentials_writer.writeAll(
        "{\"version\":1,\"instance_id\":\"" ++ test_instance_id ++ "\",\"credentials\":[",
    );
    for (0..max_credentials + 1) |index| {
        if (index != 0) try credentials_writer.writeByte(',');
        try credentials_writer.print(
            "{{\"credential_id\":\"cred-{d}\",\"principal_id\":\"principal-{d}\",\"role\":\"trader\",\"uid\":{d},\"secret_file_env\":\"KEY_{d}\"}}",
            .{ index, index, index + 1, index },
        );
    }
    try credentials_writer.writeAll("]}");
    try std.testing.expectError(
        error.TooManyCredentials,
        parseProfile(credentials_writer.buffered(), std.testing.allocator),
    );
}

fn testFilePermissions(mode: u16) std.Io.Dir.Permissions {
    return if (std.Io.Dir.Permissions.has_executable_bit)
        @enumFromInt(mode)
    else
        .default_file;
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
    var buffer: [2048]u8 = undefined;
    var writer = file.writer(std.testing.io, &buffer);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
}

test "secret file loader enforces encoding permissions and zero rejection" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const private_permissions = testFilePermissions(0o600);

    try writeTestFile(tmp.dir, "valid.key", "01" ** 32, private_permissions);
    var valid = try loadSecretKey(std.testing.io, tmp.dir, "valid.key");
    defer valid.deinit();
    try std.testing.expectEqual([_]u8{0x01} ** secret_key_bytes, valid.bytes);

    try writeTestFile(tmp.dir, "newline.key", ("02" ** 32) ++ "\n", private_permissions);
    var newline = try loadSecretKey(std.testing.io, tmp.dir, "newline.key");
    defer newline.deinit();
    try std.testing.expectEqual([_]u8{0x02} ** secret_key_bytes, newline.bytes);

    try writeTestFile(tmp.dir, "uppercase.key", "AA" ** 32, private_permissions);
    try std.testing.expectError(
        error.InvalidSecretEncoding,
        loadSecretKey(std.testing.io, tmp.dir, "uppercase.key"),
    );
    try writeTestFile(tmp.dir, "zero.key", "00" ** 32, private_permissions);
    try std.testing.expectError(
        error.AllZeroSecret,
        loadSecretKey(std.testing.io, tmp.dir, "zero.key"),
    );
    try writeTestFile(tmp.dir, "cr.key", ("03" ** 32) ++ "\r", private_permissions);
    try std.testing.expectError(
        error.InvalidSecretEncoding,
        loadSecretKey(std.testing.io, tmp.dir, "cr.key"),
    );
    try writeTestFile(tmp.dir, "long.key", ("04" ** 32) ++ "\nX", private_permissions);
    try std.testing.expectError(
        error.SecretFileLength,
        loadSecretKey(std.testing.io, tmp.dir, "long.key"),
    );
    try writeTestFile(tmp.dir, "short.key", "05" ** 31, private_permissions);
    try std.testing.expectError(
        error.SecretFileLength,
        loadSecretKey(std.testing.io, tmp.dir, "short.key"),
    );
    try std.testing.expectError(
        error.SecretFileOpenFailed,
        loadSecretKey(std.testing.io, tmp.dir, "missing.key"),
    );
    try tmp.dir.createDir(std.testing.io, "directory.key", .default_dir);
    try std.testing.expectError(
        error.SecretFileOpenFailed,
        loadSecretKey(std.testing.io, tmp.dir, "directory.key"),
    );
    if (comptime builtin.os.tag != .windows and builtin.os.tag != .wasi) {
        try tmp.dir.symLink(std.testing.io, "valid.key", "symlink.key", .{});
        try std.testing.expectError(
            error.SecretFileOpenFailed,
            loadSecretKey(std.testing.io, tmp.dir, "symlink.key"),
        );
    }

    if (std.Io.Dir.Permissions.has_executable_bit) {
        try writeTestFile(tmp.dir, "public.key", "05" ** 32, testFilePermissions(0o644));
        try std.testing.expectError(
            error.SecretFilePermissions,
            loadSecretKey(std.testing.io, tmp.dir, "public.key"),
        );
    }
}

test "profile loads environment file paths and authenticates known credentials" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const private_permissions = testFilePermissions(0o600);
    try writeTestFile(tmp.dir, "profile.json", valid_profile_json, .default_file);
    try writeTestFile(tmp.dir, "trader-primary.key", "01" ** 32, private_permissions);
    try writeTestFile(tmp.dir, "trader-rotated.key", "02" ** 32, private_permissions);
    try writeTestFile(tmp.dir, "operator.key", "03" ** 32, private_permissions);

    var environment = std.process.Environ.Map.init(allocator);
    defer environment.deinit();
    try environment.put("TRADER_PRIMARY_KEY_FILE", "trader-primary.key");
    try environment.put("TRADER_ROTATED_KEY_FILE", "trader-rotated.key");
    try environment.put("OPERATOR_KEY_FILE", "operator.key");

    const profile = try Profile.loadFromDir(
        std.testing.io,
        allocator,
        tmp.dir,
        &environment,
        "profile.json",
    );
    defer profile.deinit();
    try std.testing.expectEqualStrings(test_instance_id, profile.instanceId());
    try std.testing.expectEqual(@as(usize, 3), profile.credentialCount());
    try std.testing.expect(profile.identityAt(3) == null);
    try std.testing.expect(profile.containsCredential("trader-rotated"));
    try std.testing.expect(!profile.containsCredential("unknown"));
    const rotated = profile.findIdentity("trader-rotated") orelse
        return error.TestExpectedEqual;
    try std.testing.expectEqual(Principal{ .trader = 1001 }, rotated.principal);

    var primary_key = SecretKey{ .bytes = [_]u8{0x01} ** secret_key_bytes };
    defer primary_key.deinit();
    const auth = Auth{
        .instance_id = try validateInstanceId(test_instance_id),
        .credential_id = try Identifier.init("trader-primary"),
        .client_nonce = [_]u8{0x41} ** nonce_bytes,
    };
    const challenge = Challenge{ .server_nonce = [_]u8{0x42} ** nonce_bytes };
    const transcript = Transcript.init(auth, challenge);
    var proof = computeProof(&primary_key, transcript);
    defer std.crypto.secureZero(u8, &proof);
    var authenticated = try profile.verifyProof(transcript, proof);
    defer authenticated.deinit();
    try std.testing.expectEqual(Principal{ .trader = 1001 }, authenticated.identity.principal);

    var wrong_proof = proof;
    wrong_proof[0] ^= 1;
    try std.testing.expectError(
        error.AuthenticationFailed,
        profile.verifyProof(transcript, wrong_proof),
    );

    var unknown_auth = auth;
    unknown_auth.credential_id = try Identifier.init("unknown");
    const unknown_transcript = Transcript.init(unknown_auth, challenge);
    var unknown_proof = computeProof(&primary_key, unknown_transcript);
    defer std.crypto.secureZero(u8, &unknown_proof);
    try std.testing.expectError(
        error.AuthenticationFailed,
        profile.verifyProof(unknown_transcript, unknown_proof),
    );

    var other_instance_auth = auth;
    other_instance_auth.instance_id = try validateInstanceId(
        "1e46f90e-c38b-487a-b9f9-630f7839cc3f",
    );
    const other_instance_transcript = Transcript.init(other_instance_auth, challenge);
    var other_instance_proof = computeProof(&primary_key, other_instance_transcript);
    defer std.crypto.secureZero(u8, &other_instance_proof);
    try std.testing.expectError(
        error.AuthenticationFailed,
        profile.verifyProof(other_instance_transcript, other_instance_proof),
    );
}

test "profile load fails closed for missing paths and duplicate decoded keys" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeTestFile(tmp.dir, "profile.json", valid_profile_json, .default_file);
    const private_permissions = testFilePermissions(0o600);
    try writeTestFile(tmp.dir, "same-a.key", "0a" ** 32, private_permissions);
    try writeTestFile(tmp.dir, "same-b.key", "0a" ** 32, private_permissions);
    try writeTestFile(tmp.dir, "operator.key", "0b" ** 32, private_permissions);

    var missing_environment = std.process.Environ.Map.init(allocator);
    defer missing_environment.deinit();
    try std.testing.expectError(
        error.MissingSecretFileEnvironment,
        Profile.loadFromDir(
            std.testing.io,
            allocator,
            tmp.dir,
            &missing_environment,
            "profile.json",
        ),
    );

    var empty_environment = std.process.Environ.Map.init(allocator);
    defer empty_environment.deinit();
    try empty_environment.put("TRADER_PRIMARY_KEY_FILE", "");
    try empty_environment.put("TRADER_ROTATED_KEY_FILE", "same-b.key");
    try empty_environment.put("OPERATOR_KEY_FILE", "operator.key");
    try std.testing.expectError(
        error.EmptySecretFilePath,
        Profile.loadFromDir(
            std.testing.io,
            allocator,
            tmp.dir,
            &empty_environment,
            "profile.json",
        ),
    );

    var duplicate_environment = std.process.Environ.Map.init(allocator);
    defer duplicate_environment.deinit();
    try duplicate_environment.put("TRADER_PRIMARY_KEY_FILE", "same-a.key");
    try duplicate_environment.put("TRADER_ROTATED_KEY_FILE", "same-b.key");
    try duplicate_environment.put("OPERATOR_KEY_FILE", "operator.key");
    try std.testing.expectError(
        error.DuplicateCredentialSecret,
        Profile.loadFromDir(
            std.testing.io,
            allocator,
            tmp.dir,
            &duplicate_environment,
            "profile.json",
        ),
    );
}

test "checked-in example profile satisfies the strict schema" {
    const example = try std.Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        "src/zig/fixtures/ingress-auth-profile.example.json",
        std.testing.allocator,
        .limited(max_profile_bytes + 1),
    );
    defer std.testing.allocator.free(example);
    const document = try parseProfile(example, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), document.credential_count);
    try std.testing.expectEqualStrings("trader-1001-a", document.credentials[0].identity.credential_id.slice());
    try std.testing.expectEqualStrings("trader-1002-a", document.credentials[1].identity.credential_id.slice());
    try std.testing.expectEqualStrings("operator-a", document.credentials[2].identity.credential_id.slice());
}
