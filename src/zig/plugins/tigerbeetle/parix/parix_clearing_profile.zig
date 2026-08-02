//! Parix host integration for the opt-in TigerBeetle plugin.
const std = @import("std");
const config = @import("parix_clearing_profile_config.zig");
const clearing = @import("parix_clearing_saga.zig");
const settlement_profile_module = @import("parix_settlement_profile.zig");

const Sha256 = std.crypto.hash.sha2.Sha256;
const profile_digest_domain = "exchange-core/parix-clearing-profile/v1";

pub const digest_len: usize = Sha256.digest_length;
pub const max_users: usize = settlement_profile_module.max_users;
pub const RouteSpec = config.Route;
pub const TimeoutBounds = config.TimeoutBounds;
pub const SettlementProfile = settlement_profile_module.Profile;

pub const ValidationError = settlement_profile_module.UuidParseError || error{
    DatabaseMismatch,
    InvalidProviderId,
    InvalidTransferCode,
    InvalidTimeoutBounds,
    InvalidCurrency,
    DuplicateCurrency,
    InvalidLedger,
    InvalidAccountId,
    DuplicateProviderControlAccount,
    SettlementAccountCollision,
    SettlementCurrencyMismatch,
    SettlementLedgerMismatch,
    SettlementTreasuryMismatch,
    InvalidUserCount,
};

pub const InitError = std.mem.Allocator.Error || ValidationError;
pub const LoadError = std.Io.Dir.ReadFileAllocError || config.ParseError || InitError;

pub const AdmissionError = clearing.EventError || clearing.RouteError || error{
    ProviderMismatch,
    UnknownUser,
    UnknownCurrency,
    TimeoutOutOfBounds,
};

pub const ResolutionValidationError = AdmissionError || error{
    IdempotencyConflict,
    ImmutableFieldMismatch,
    StaleRevision,
    OutOfOrderRevision,
    AmountExceedsAuthorization,
    VoidAmountMismatch,
};

/// Heap-owned immutable clearing policy, paired with a validated settlement
/// profile at construction. No accessor retains the JSON buffer or settlement
/// profile pointer.
pub const Profile = opaque {
    pub fn loadFile(
        allocator: std.mem.Allocator,
        io: std.Io,
        path: []const u8,
        settlement_profile: *const SettlementProfile,
    ) LoadError!*Profile {
        const bytes = try std.Io.Dir.cwd().readFileAlloc(
            io,
            path,
            allocator,
            .limited(config.max_profile_bytes),
        );
        defer allocator.free(bytes);
        return parseJson(allocator, bytes, settlement_profile);
    }

    pub fn parseJson(
        allocator: std.mem.Allocator,
        bytes: []const u8,
        settlement_profile: *const SettlementProfile,
    ) (config.ParseError || InitError)!*Profile {
        const document = try config.parse(allocator, bytes);
        return initFromDocument(allocator, document, settlement_profile);
    }

    pub fn developmentExample(
        allocator: std.mem.Allocator,
        settlement_profile: *const SettlementProfile,
    ) (config.ParseError || InitError)!*Profile {
        return parseJson(allocator, development_example_json, settlement_profile);
    }

    pub fn deinit(self: *Profile) void {
        const impl = profileImplMut(self);
        const allocator = impl.allocator;
        impl.* = undefined;
        allocator.destroy(impl);
    }

    pub fn databaseUuid(self: *const Profile) []const u8 {
        return &profileImpl(self).database_uuid;
    }

    pub fn destinationId(self: *const Profile) clearing.DestinationId {
        return profileImpl(self).destination_id;
    }

    pub fn providerId(self: *const Profile) u64 {
        return profileImpl(self).provider_id;
    }

    pub fn timeoutBounds(self: *const Profile) TimeoutBounds {
        return profileImpl(self).timeout_bounds;
    }

    pub fn routes(self: *const Profile) []const RouteSpec {
        return &profileImpl(self).routes;
    }

    pub fn digest(self: *const Profile) [digest_len]u8 {
        return profileImpl(self).profile_digest;
    }

    pub fn settlementProfileDigest(self: *const Profile) [digest_len]u8 {
        return profileImpl(self).settlement_profile_digest;
    }

    pub fn binding(self: *const Profile) clearing.Binding {
        const impl = profileImpl(self);
        return .{
            .destination_id = impl.destination_id,
            .settlement_profile_digest = impl.settlement_profile_digest,
            .clearing_profile_digest = impl.profile_digest,
            .provider_id = impl.provider_id,
        };
    }

    pub fn authorizationRoute(
        self: *const Profile,
        authorization: clearing.Authorization,
    ) AdmissionError!clearing.Route {
        try authorization.validate();
        const impl = profileImpl(self);
        if (authorization.provider_id != impl.provider_id) return error.ProviderMismatch;
        if (authorization.timeout_seconds < impl.timeout_bounds.minimum_seconds or
            authorization.timeout_seconds > impl.timeout_bounds.maximum_seconds)
        {
            return error.TimeoutOutOfBounds;
        }
        if (!configuredUser(impl.users_storage[0..impl.user_count], authorization.uid)) {
            return error.UnknownUser;
        }
        const configured_route = findRoute(impl.routes, authorization.currency) orelse
            return error.UnknownCurrency;
        const route: clearing.Route = .{
            .destination_id = impl.destination_id,
            .ledger = configured_route.ledger,
            .treasury_account_id = configured_route.treasury_account_id,
            .provider_control_account_id = configured_route.provider_control_account_id,
            .transfer_code = impl.transfer_code,
        };
        try route.validate();
        return route;
    }

    pub fn validateResolution(
        self: *const Profile,
        authorization: clearing.Authorization,
        resolution: clearing.Resolution,
    ) ResolutionValidationError!void {
        _ = try self.authorizationRoute(authorization);
        try resolution.validate();
        if (resolution.event_id == authorization.event_id) {
            return error.IdempotencyConflict;
        }
        if (resolution.payment_id != authorization.payment_id or
            resolution.provider_id != authorization.provider_id or
            resolution.direction != authorization.direction or
            resolution.uid != authorization.uid or
            resolution.currency != authorization.currency)
        {
            return error.ImmutableFieldMismatch;
        }
        if (resolution.revision <= authorization.revision) return error.StaleRevision;
        const next_revision = std.math.add(u64, authorization.revision, 1) catch
            return error.OutOfOrderRevision;
        if (resolution.revision != next_revision) return error.OutOfOrderRevision;
        switch (resolution.kind) {
            .post => if (resolution.amount > authorization.amount) {
                return error.AmountExceedsAuthorization;
            },
            .void => if (resolution.amount != authorization.amount) {
                return error.VoidAmountMismatch;
            },
        }
    }
};

const ProfileImpl = struct {
    allocator: std.mem.Allocator,
    database_uuid: [config.max_database_uuid_bytes]u8,
    destination_id: clearing.DestinationId,
    provider_id: u64,
    transfer_code: u16,
    timeout_bounds: TimeoutBounds,
    routes: [config.route_count]RouteSpec,
    users_storage: [max_users]i64 = undefined,
    user_count: usize,
    settlement_profile_digest: [digest_len]u8,
    profile_digest: [digest_len]u8,
};

fn initFromDocument(
    allocator: std.mem.Allocator,
    document: config.Document,
    settlement_profile: *const SettlementProfile,
) InitError!*Profile {
    const destination_id = try settlement_profile_module.parseUuidDestination(&document.database_uuid);
    if (destination_id != settlement_profile.destinationId()) return error.DatabaseMismatch;
    if (document.provider_id == 0) return error.InvalidProviderId;
    if (document.transfer_code == 0) return error.InvalidTransferCode;
    if (document.timeout_bounds.minimum_seconds == 0 or
        document.timeout_bounds.maximum_seconds == 0 or
        document.timeout_bounds.minimum_seconds > document.timeout_bounds.maximum_seconds)
    {
        return error.InvalidTimeoutBounds;
    }

    var routes = document.routes;
    std.sort.heap(RouteSpec, &routes, {}, routeLessThan);
    try validateRoutes(routes, settlement_profile);

    const settlement_users = settlement_profile.users();
    if (settlement_users.len == 0 or settlement_users.len > max_users) {
        return error.InvalidUserCount;
    }
    var users_storage: [max_users]i64 = undefined;
    for (settlement_users, 0..) |user, index| users_storage[index] = user.uid;
    std.sort.heap(i64, users_storage[0..settlement_users.len], {}, intLessThan);

    const impl = try allocator.create(ProfileImpl);
    errdefer allocator.destroy(impl);
    impl.* = .{
        .allocator = allocator,
        .database_uuid = undefined,
        .destination_id = destination_id,
        .provider_id = document.provider_id,
        .transfer_code = document.transfer_code,
        .timeout_bounds = document.timeout_bounds,
        .routes = routes,
        .users_storage = users_storage,
        .user_count = settlement_users.len,
        .settlement_profile_digest = settlement_profile.digest(),
        .profile_digest = undefined,
    };
    @memcpy(&impl.database_uuid, settlement_profile.databaseUuid());
    impl.profile_digest = calculateProfileDigest(impl);
    return profileToken(impl);
}

fn validateRoutes(
    routes: [config.route_count]RouteSpec,
    settlement_profile: *const SettlementProfile,
) ValidationError!void {
    if (routes[0].currency == routes[1].currency) return error.DuplicateCurrency;
    if (routes[0].provider_control_account_id == routes[1].provider_control_account_id) {
        return error.DuplicateProviderControlAccount;
    }
    for (routes) |route| {
        if (route.currency <= 0) return error.InvalidCurrency;
        if (route.ledger == 0) return error.InvalidLedger;
        if (!validAccountId(route.treasury_account_id) or
            !validAccountId(route.provider_control_account_id))
        {
            return error.InvalidAccountId;
        }
        for (settlement_profile.accounts()) |account| {
            if (route.provider_control_account_id == account.id) {
                return error.SettlementAccountCollision;
            }
        }
    }

    const settlement_routes = settlement_profile.currencies();
    if (settlement_routes.len != config.route_count) return error.SettlementCurrencyMismatch;
    for (routes, settlement_routes) |route, expected| {
        if (route.currency != expected.currency) return error.SettlementCurrencyMismatch;
        if (route.ledger != expected.ledger) return error.SettlementLedgerMismatch;
        if (route.treasury_account_id != expected.treasury_account_id) {
            return error.SettlementTreasuryMismatch;
        }
    }
}

fn calculateProfileDigest(impl: *const ProfileImpl) [digest_len]u8 {
    var hasher = Sha256.init(.{});
    hasher.update(profile_digest_domain);
    hashInt(u16, &hasher, config.format_version);
    hashInt(u32, &hasher, clearing.workflow_revision);
    hashInt(u128, &hasher, @intFromEnum(impl.destination_id));
    hashInt(u64, &hasher, impl.provider_id);
    hashInt(u16, &hasher, impl.transfer_code);
    hashInt(u32, &hasher, impl.timeout_bounds.minimum_seconds);
    hashInt(u32, &hasher, impl.timeout_bounds.maximum_seconds);
    hashInt(u8, &hasher, @intCast(impl.routes.len));
    for (impl.routes) |route| {
        hashInt(i32, &hasher, route.currency);
        hashInt(u32, &hasher, route.ledger);
        hashInt(u128, &hasher, route.treasury_account_id);
        hashInt(u128, &hasher, route.provider_control_account_id);
    }
    var result: [digest_len]u8 = undefined;
    hasher.final(&result);
    return result;
}

fn hashInt(comptime T: type, hasher: *Sha256, value: T) void {
    const bits = @typeInfo(T).int.bits;
    const U = std.meta.Int(.unsigned, bits);
    const unsigned: U = @bitCast(value);
    const little = std.mem.nativeToLittle(U, unsigned);
    hasher.update(std.mem.asBytes(&little));
}

fn findRoute(routes: [config.route_count]RouteSpec, currency: i32) ?RouteSpec {
    for (routes) |route| if (route.currency == currency) return route;
    return null;
}

fn configuredUser(users: []const i64, uid: i64) bool {
    return std.sort.binarySearch(i64, users, uid, compareInt) != null;
}

fn routeLessThan(_: void, lhs: RouteSpec, rhs: RouteSpec) bool {
    return lhs.currency < rhs.currency;
}

fn intLessThan(_: void, lhs: i64, rhs: i64) bool {
    return lhs < rhs;
}

fn compareInt(key: i64, item: i64) std.math.Order {
    return std.math.order(key, item);
}

fn validAccountId(id: u128) bool {
    return id != 0 and id != std.math.maxInt(u128);
}

fn profileToken(impl: *ProfileImpl) *Profile {
    return @ptrCast(impl);
}

fn profileImpl(profile: *const Profile) *const ProfileImpl {
    return @ptrCast(@alignCast(profile));
}

fn profileImplMut(profile: *Profile) *ProfileImpl {
    return @ptrCast(@alignCast(profile));
}

const development_example_json =
    \\{
    \\  "version": 1,
    \\  "database_uuid": "0e46f90e-c38b-487a-b9f9-630f7839cc3f",
    \\  "provider_id": "7001",
    \\  "transfer_code": 20,
    \\  "timeout_bounds": {"minimum_seconds": 30, "maximum_seconds": 3600},
    \\  "routes": [
    \\    {"currency": 11, "ledger": 421, "treasury_account_id": "420011", "provider_control_account_id": "430011"},
    \\    {"currency": 15, "ledger": 425, "treasury_account_id": "420015", "provider_control_account_id": "430015"}
    \\  ]
    \\}
;

test "development profile binds routes users and independent digests" {
    const settlement_profile = try SettlementProfile.developmentExample(std.testing.allocator);
    const profile = try Profile.developmentExample(std.testing.allocator, settlement_profile);
    settlement_profile.deinit();
    defer profile.deinit();

    try std.testing.expectEqualStrings(
        "0e46f90e-c38b-487a-b9f9-630f7839cc3f",
        profile.databaseUuid(),
    );
    try std.testing.expectEqual(@as(u64, 7_001), profile.providerId());
    try std.testing.expectEqual(@as(u32, 30), profile.timeoutBounds().minimum_seconds);
    try std.testing.expectEqual(@as(i32, 11), profile.routes()[0].currency);
    try std.testing.expectEqual(@as(i32, 15), profile.routes()[1].currency);

    const authorization = testAuthorization();
    const route = try profile.authorizationRoute(authorization);
    try std.testing.expectEqual(profile.destinationId(), route.destination_id);
    try std.testing.expectEqual(@as(u32, 421), route.ledger);
    try std.testing.expectEqual(@as(u128, 420_011), route.treasury_account_id);
    try std.testing.expectEqual(@as(u128, 430_011), route.provider_control_account_id);
    try std.testing.expectEqual(@as(u16, 20), route.transfer_code);

    const binding = profile.binding();
    try binding.validate();
    try std.testing.expectEqual(profile.destinationId(), binding.destination_id);
    try std.testing.expectEqual(profile.providerId(), binding.provider_id);
    try std.testing.expectEqualSlices(u8, &profile.digest(), &binding.clearing_profile_digest);
    try std.testing.expectEqualSlices(
        u8,
        &profile.settlementProfileDigest(),
        &binding.settlement_profile_digest,
    );
}

test "route ordering and UUID spelling do not change canonical identity" {
    const settlement_profile = try SettlementProfile.developmentExample(std.testing.allocator);
    defer settlement_profile.deinit();
    const ordered = try Profile.developmentExample(std.testing.allocator, settlement_profile);
    defer ordered.deinit();

    const reordered =
        \\{"version":1,"database_uuid":"0E46F90E-C38B-487A-B9F9-630F7839CC3F","provider_id":"7001","transfer_code":20,"timeout_bounds":{"minimum_seconds":30,"maximum_seconds":3600},"routes":[{"currency":15,"ledger":425,"treasury_account_id":"420015","provider_control_account_id":"430015"},{"currency":11,"ledger":421,"treasury_account_id":"420011","provider_control_account_id":"430011"}]}
    ;
    const canonical = try Profile.parseJson(
        std.testing.allocator,
        reordered,
        settlement_profile,
    );
    defer canonical.deinit();

    try std.testing.expectEqualStrings(ordered.databaseUuid(), canonical.databaseUuid());
    try std.testing.expectEqual(@as(i32, 11), canonical.routes()[0].currency);
    try std.testing.expectEqualSlices(u8, &ordered.digest(), &canonical.digest());
}

test "profile digest changes for each clearing policy field" {
    const settlement_profile = try SettlementProfile.developmentExample(std.testing.allocator);
    defer settlement_profile.deinit();
    const baseline = try Profile.developmentExample(std.testing.allocator, settlement_profile);
    defer baseline.deinit();

    const cases = [_]struct { needle: []const u8, replacement: []const u8 }{
        .{ .needle = "\"provider_id\": \"7001\"", .replacement = "\"provider_id\": \"7002\"" },
        .{ .needle = "\"transfer_code\": 20", .replacement = "\"transfer_code\": 21" },
        .{ .needle = "\"minimum_seconds\": 30", .replacement = "\"minimum_seconds\": 31" },
        .{ .needle = "\"maximum_seconds\": 3600", .replacement = "\"maximum_seconds\": 3601" },
        .{ .needle = "\"provider_control_account_id\": \"430011\"", .replacement = "\"provider_control_account_id\": \"430012\"" },
    };
    for (cases) |case| {
        const changed_json = try replaceOnceAlloc(
            std.testing.allocator,
            development_example_json,
            case.needle,
            case.replacement,
        );
        defer std.testing.allocator.free(changed_json);
        const changed = try Profile.parseJson(
            std.testing.allocator,
            changed_json,
            settlement_profile,
        );
        defer changed.deinit();
        try std.testing.expect(!std.mem.eql(u8, &baseline.digest(), &changed.digest()));
        try std.testing.expectEqualSlices(
            u8,
            &baseline.settlementProfileDigest(),
            &changed.settlementProfileDigest(),
        );
    }
}

test "settlement digest remains separate from clearing digest" {
    const baseline_settlement = try SettlementProfile.parseJson(
        std.testing.allocator,
        settlement_example_json,
    );
    defer baseline_settlement.deinit();
    const changed_settlement_json = try replaceOnceAlloc(
        std.testing.allocator,
        settlement_example_json,
        "\"taker_fee\": 2",
        "\"taker_fee\": 3",
    );
    defer std.testing.allocator.free(changed_settlement_json);
    const changed_settlement = try SettlementProfile.parseJson(
        std.testing.allocator,
        changed_settlement_json,
    );
    defer changed_settlement.deinit();

    const baseline = try Profile.developmentExample(std.testing.allocator, baseline_settlement);
    defer baseline.deinit();
    const changed = try Profile.developmentExample(std.testing.allocator, changed_settlement);
    defer changed.deinit();
    try std.testing.expectEqualSlices(u8, &baseline.digest(), &changed.digest());
    try std.testing.expect(!std.mem.eql(
        u8,
        &baseline.settlementProfileDigest(),
        &changed.settlementProfileDigest(),
    ));

    const routed_settlement_json = try replaceOnceAlloc(
        std.testing.allocator,
        settlement_example_json,
        "\"treasury_account_id\": \"420011\"",
        "\"treasury_account_id\": \"420012\"",
    );
    defer std.testing.allocator.free(routed_settlement_json);
    const routed_clearing_json = try replaceOnceAlloc(
        std.testing.allocator,
        development_example_json,
        "\"treasury_account_id\": \"420011\"",
        "\"treasury_account_id\": \"420012\"",
    );
    defer std.testing.allocator.free(routed_clearing_json);
    const routed_settlement = try SettlementProfile.parseJson(
        std.testing.allocator,
        routed_settlement_json,
    );
    defer routed_settlement.deinit();
    const routed = try Profile.parseJson(
        std.testing.allocator,
        routed_clearing_json,
        routed_settlement,
    );
    defer routed.deinit();
    try std.testing.expect(!std.mem.eql(u8, &baseline.digest(), &routed.digest()));
}

test "profile rejects settlement mismatches and account collisions" {
    const settlement_profile = try SettlementProfile.developmentExample(std.testing.allocator);
    defer settlement_profile.deinit();
    const cases = [_]struct {
        expected: anyerror,
        needle: []const u8,
        replacement: []const u8,
    }{
        .{
            .expected = error.DatabaseMismatch,
            .needle = "0e46f90e-c38b-487a-b9f9-630f7839cc3f",
            .replacement = "1e46f90e-c38b-487a-b9f9-630f7839cc3f",
        },
        .{
            .expected = error.SettlementCurrencyMismatch,
            .needle = "\"currency\": 11",
            .replacement = "\"currency\": 12",
        },
        .{
            .expected = error.SettlementLedgerMismatch,
            .needle = "\"ledger\": 421",
            .replacement = "\"ledger\": 422",
        },
        .{
            .expected = error.SettlementTreasuryMismatch,
            .needle = "\"treasury_account_id\": \"420011\"",
            .replacement = "\"treasury_account_id\": \"420012\"",
        },
        .{
            .expected = error.DuplicateProviderControlAccount,
            .needle = "\"provider_control_account_id\": \"430015\"",
            .replacement = "\"provider_control_account_id\": \"430011\"",
        },
        .{
            .expected = error.SettlementAccountCollision,
            .needle = "\"provider_control_account_id\": \"430011\"",
            .replacement = "\"provider_control_account_id\": \"42011\"",
        },
        .{
            .expected = error.InvalidProviderId,
            .needle = "\"provider_id\": \"7001\"",
            .replacement = "\"provider_id\": \"0\"",
        },
        .{
            .expected = error.InvalidTransferCode,
            .needle = "\"transfer_code\": 20",
            .replacement = "\"transfer_code\": 0",
        },
        .{
            .expected = error.InvalidTimeoutBounds,
            .needle = "\"minimum_seconds\": 30",
            .replacement = "\"minimum_seconds\": 3601",
        },
    };
    for (cases) |case| {
        const changed_json = try replaceOnceAlloc(
            std.testing.allocator,
            development_example_json,
            case.needle,
            case.replacement,
        );
        defer std.testing.allocator.free(changed_json);
        try expectProfileError(case.expected, changed_json, settlement_profile);
    }
}

test "authorization admission checks provider user currency and timeout" {
    const settlement_profile = try SettlementProfile.developmentExample(std.testing.allocator);
    defer settlement_profile.deinit();
    const profile = try Profile.developmentExample(std.testing.allocator, settlement_profile);
    defer profile.deinit();

    _ = try profile.authorizationRoute(testAuthorization());
    const cases = [_]struct { expected: anyerror, authorization: clearing.Authorization }{
        .{ .expected = error.ProviderMismatch, .authorization = changedAuthorization(.provider_id, 7_002) },
        .{ .expected = error.UnknownUser, .authorization = changedAuthorization(.uid, 9_999) },
        .{ .expected = error.UnknownCurrency, .authorization = changedAuthorization(.currency, 99) },
        .{ .expected = error.TimeoutOutOfBounds, .authorization = changedAuthorization(.timeout_seconds, 29) },
        .{ .expected = error.TimeoutOutOfBounds, .authorization = changedAuthorization(.timeout_seconds, 3_601) },
        .{ .expected = error.InvalidAmount, .authorization = changedAuthorization(.amount, 0) },
    };
    for (cases) |case| {
        try std.testing.expectError(
            case.expected,
            profile.authorizationRoute(case.authorization),
        );
    }
}

test "resolution validation mirrors clearing revision and amount invariants" {
    const settlement_profile = try SettlementProfile.developmentExample(std.testing.allocator);
    defer settlement_profile.deinit();
    const profile = try Profile.developmentExample(std.testing.allocator, settlement_profile);
    defer profile.deinit();
    const authorization = testAuthorization();

    var resolution = testResolution();
    try profile.validateResolution(authorization, resolution);
    resolution.kind = .void;
    resolution.amount = authorization.amount;
    try profile.validateResolution(authorization, resolution);

    const cases = [_]struct { expected: anyerror, resolution: clearing.Resolution }{
        .{ .expected = error.IdempotencyConflict, .resolution = changedResolution(.event_id, authorization.event_id) },
        .{ .expected = error.ImmutableFieldMismatch, .resolution = changedResolution(.payment_id, authorization.payment_id + 1) },
        .{ .expected = error.StaleRevision, .resolution = changedResolution(.revision, authorization.revision) },
        .{ .expected = error.OutOfOrderRevision, .resolution = changedResolution(.revision, authorization.revision + 2) },
        .{ .expected = error.AmountExceedsAuthorization, .resolution = changedResolution(.amount, @intCast(authorization.amount + 1)) },
        .{ .expected = error.InvalidAmount, .resolution = changedResolution(.amount, 0) },
        .{ .expected = error.VoidAmountMismatch, .resolution = changedVoidResolution(authorization.amount - 1) },
    };
    for (cases) |case| {
        try std.testing.expectError(
            case.expected,
            profile.validateResolution(authorization, case.resolution),
        );
    }
}

test "checked-in clearing profile matches the development fixture" {
    const settlement_profile = try SettlementProfile.developmentExample(std.testing.allocator);
    defer settlement_profile.deinit();
    const from_file = try Profile.loadFile(
        std.testing.allocator,
        std.testing.io,
        "src/zig/fixtures/parix-clearing-profile.example.json",
        settlement_profile,
    );
    defer from_file.deinit();
    const built_in = try Profile.developmentExample(std.testing.allocator, settlement_profile);
    defer built_in.deinit();

    try std.testing.expectEqualSlices(u8, &built_in.digest(), &from_file.digest());
    try std.testing.expectEqualDeep(built_in.timeoutBounds(), from_file.timeoutBounds());
    try std.testing.expectEqualSlices(RouteSpec, built_in.routes(), from_file.routes());
}

const AuthorizationField = enum {
    provider_id,
    uid,
    currency,
    timeout_seconds,
    amount,
};

fn changedAuthorization(comptime field: AuthorizationField, value: u128) clearing.Authorization {
    var authorization = testAuthorization();
    @field(authorization, @tagName(field)) = @intCast(value);
    return authorization;
}

const ResolutionField = enum {
    event_id,
    payment_id,
    revision,
    amount,
};

fn changedResolution(comptime field: ResolutionField, value: u128) clearing.Resolution {
    var resolution = testResolution();
    @field(resolution, @tagName(field)) = @intCast(value);
    return resolution;
}

fn changedVoidResolution(amount: i64) clearing.Resolution {
    var resolution = testResolution();
    resolution.kind = .void;
    resolution.amount = amount;
    return resolution;
}

fn testAuthorization() clearing.Authorization {
    return .{
        .event_id = 101,
        .payment_id = 201,
        .provider_id = 7_001,
        .revision = 1,
        .direction = .deposit,
        .uid = 1_001,
        .currency = 11,
        .amount = 100,
        .timeout_seconds = 300,
    };
}

fn testResolution() clearing.Resolution {
    const authorization = testAuthorization();
    return .{
        .event_id = 102,
        .payment_id = authorization.payment_id,
        .provider_id = authorization.provider_id,
        .revision = authorization.revision + 1,
        .kind = .post,
        .direction = authorization.direction,
        .uid = authorization.uid,
        .currency = authorization.currency,
        .amount = 80,
    };
}

fn expectProfileError(
    expected: anyerror,
    json: []const u8,
    settlement_profile: *const SettlementProfile,
) !void {
    const result = Profile.parseJson(std.testing.allocator, json, settlement_profile);
    if (result) |profile| {
        profile.deinit();
        return error.TestUnexpectedResult;
    } else |actual| {
        try std.testing.expectEqual(expected, actual);
    }
}

fn replaceOnceAlloc(
    allocator: std.mem.Allocator,
    input: []const u8,
    needle: []const u8,
    replacement: []const u8,
) std.mem.Allocator.Error![]u8 {
    const index = std.mem.indexOf(u8, input, needle) orelse unreachable;
    const output = try allocator.alloc(u8, input.len - needle.len + replacement.len);
    @memcpy(output[0..index], input[0..index]);
    @memcpy(output[index .. index + replacement.len], replacement);
    @memcpy(output[index + replacement.len ..], input[index + needle.len ..]);
    return output;
}

const settlement_example_json =
    \\{
    \\  "version": 1,
    \\  "database_uuid": "0e46f90e-c38b-487a-b9f9-630f7839cc3f",
    \\  "publication": {"namespace": 1, "journal_epoch": 0},
    \\  "symbol": {"symbol_id": 241, "symbol_type": "currency_exchange_pair", "base_currency": 11, "quote_currency": 15, "base_scale_k": 100, "quote_scale_k": 10, "taker_fee": 2, "maker_fee": 1, "margin_buy": 0, "margin_sell": 0},
    \\  "currencies": [{"currency": 11, "ledger": 421, "treasury_account_id": "420011", "fee_account_id": "42011"}, {"currency": 15, "ledger": 425, "treasury_account_id": "420015", "fee_account_id": "42015"}],
    \\  "users": [{"uid": 1001, "base_account_id": "421011", "quote_account_id": "421015"}, {"uid": 1002, "base_account_id": "422011", "quote_account_id": "422015"}],
    \\  "initial_funding": [{"id": "8421011", "uid": 1001, "currency": 11, "amount": "1000"}, {"id": "8422015", "uid": 1002, "currency": 15, "amount": "2000"}]
    \\}
;
