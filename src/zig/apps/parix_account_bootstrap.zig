const std = @import("std");
const exchange_core = @import("exchange_core");

const transport = @import("parix_http_transport.zig");
const profile = @import("parix_settlement_profile.zig");
const core = exchange_core.core;
const adapter = core.tigerbeetle_parix_adapter;
const dispatcher = core.tigerbeetle_dispatcher;

pub const Summary = struct {
    accounts_created: usize = 0,
    accounts_reused: usize = 0,
    accounts_reconciled: usize = 0,
    funding_transfers_created: usize = 0,
    funding_transfers_reused: usize = 0,
    funding_transfers_reconciled: usize = 0,
};

pub const BootstrapError = error{
    OutOfMemory,
    ProfileDestinationMismatch,
    RequestEncodingFailed,
    AccountLookupUnavailable,
    AccountLookupMalformed,
    AccountLookupRejected,
    AccountPartialPresence,
    AccountMismatch,
    AccountCreateNotSent,
    AccountCreateRejected,
    AccountCreateUnconfirmed,
    FundingLookupUnavailable,
    FundingLookupMalformed,
    FundingLookupRejected,
    FundingPartialPresence,
    FundingMismatch,
    FundingCreateNotSent,
    FundingCreateRejected,
    FundingCreateUnconfirmed,
};

pub fn isRetryable(err: BootstrapError) bool {
    return switch (err) {
        error.OutOfMemory,
        error.AccountLookupUnavailable,
        error.AccountLookupMalformed,
        error.AccountCreateNotSent,
        error.AccountCreateUnconfirmed,
        error.FundingLookupUnavailable,
        error.FundingLookupMalformed,
        error.FundingCreateNotSent,
        error.FundingCreateUnconfirmed,
        => true,
        else => false,
    };
}

const Presence = enum {
    all_absent,
    all_present_exact,
    partial_application,
    mismatch,
};

const CreateEvidence = enum {
    accepted,
    conflict,
    ambiguous,
    rejected,
};

const DecodeError = error{
    OutOfMemory,
    MalformedResponse,
    UnavailableResponse,
    RejectedResponse,
};

const Requester = struct {
    context: *anyopaque,
    allocator: std.mem.Allocator,
    request_fn: *const fn (
        context: *anyopaque,
        operation: transport.GatewayOperation,
        payload: []const u8,
    ) transport.RequestOutcome,

    fn request(
        self: Requester,
        operation: transport.GatewayOperation,
        payload: []const u8,
    ) transport.RequestOutcome {
        return self.request_fn(self.context, operation, payload);
    }
};

/// Ensures the fixed exchange profile exists without ever replaying a write
/// whose commit status is unknown. Every attempted create is followed by an
/// authoritative stable-ID lookup before success is reported.
pub fn ensureProfile(client: *transport.GatewayClient) BootstrapError!Summary {
    if (!std.mem.eql(u8, client.database_id, profile.database_uuid) or
        client.destinationId() != profile.defaultDestination())
    {
        return error.ProfileDestinationMismatch;
    }
    return ensureProfileWith(.{
        .context = client,
        .allocator = client.allocator,
        .request_fn = gatewayRequest,
    });
}

fn gatewayRequest(
    context: *anyopaque,
    operation: transport.GatewayOperation,
    payload: []const u8,
) transport.RequestOutcome {
    const client: *transport.GatewayClient = @ptrCast(@alignCast(context));
    return client.request(operation, payload);
}

fn ensureProfileWith(requester: Requester) BootstrapError!Summary {
    var summary: Summary = .{};

    switch (try lookupAccounts(requester)) {
        .all_present_exact => summary.accounts_reused = profile.accounts.len,
        .all_absent => {
            const evidence = try createAccounts(requester);
            switch (try lookupAccounts(requester)) {
                .all_present_exact => switch (evidence) {
                    .accepted => summary.accounts_created = profile.accounts.len,
                    .conflict, .ambiguous, .rejected => summary.accounts_reconciled = profile.accounts.len,
                },
                .all_absent => return switch (evidence) {
                    .rejected => error.AccountCreateRejected,
                    else => error.AccountCreateUnconfirmed,
                },
                .partial_application => return error.AccountPartialPresence,
                .mismatch => return error.AccountMismatch,
            }
        },
        .partial_application => return error.AccountPartialPresence,
        .mismatch => return error.AccountMismatch,
    }

    switch (try lookupFundingTransfers(requester)) {
        .all_present_exact => summary.funding_transfers_reused = profile.funding_transfers.len,
        .all_absent => {
            const evidence = try createFundingTransfers(requester);
            switch (try lookupFundingTransfers(requester)) {
                .all_present_exact => switch (evidence) {
                    .accepted => summary.funding_transfers_created = profile.funding_transfers.len,
                    .conflict, .ambiguous, .rejected => summary.funding_transfers_reconciled = profile.funding_transfers.len,
                },
                .all_absent => return switch (evidence) {
                    .rejected => error.FundingCreateRejected,
                    else => error.FundingCreateUnconfirmed,
                },
                .partial_application => return error.FundingPartialPresence,
                .mismatch => return error.FundingMismatch,
            }
        },
        .partial_application => return error.FundingPartialPresence,
        .mismatch => return error.FundingMismatch,
    }

    return summary;
}

fn lookupAccounts(requester: Requester) BootstrapError!Presence {
    var storage: [transport.max_request_bytes]u8 = undefined;
    var writer = std.Io.Writer.fixed(&storage);
    profile.writeLookupAccounts(&writer) catch return error.RequestEncodingFailed;
    return switch (requester.request(.lookup_accounts, writer.buffered())) {
        .failure => error.AccountLookupUnavailable,
        .response => |response| decodeAccountLookupResponse(
            requester.allocator,
            response.status,
            response.body,
        ) catch |err| switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            error.MalformedResponse => error.AccountLookupMalformed,
            error.UnavailableResponse => error.AccountLookupUnavailable,
            error.RejectedResponse => error.AccountLookupRejected,
        },
    };
}

fn createAccounts(requester: Requester) BootstrapError!CreateEvidence {
    var storage: [transport.max_request_bytes]u8 = undefined;
    var writer = std.Io.Writer.fixed(&storage);
    profile.writeCreateAccounts(&writer) catch return error.RequestEncodingFailed;
    return switch (requester.request(.create_accounts, writer.buffered())) {
        .failure => |certainty| switch (certainty) {
            .definitely_not_sent => error.AccountCreateNotSent,
            .indeterminate => .ambiguous,
        },
        .response => |response| decodeAccountCreateResponse(
            requester.allocator,
            response.status,
            response.body,
        ) catch |err| switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            error.MalformedResponse => .ambiguous,
            error.UnavailableResponse, error.RejectedResponse => .ambiguous,
        },
    };
}

fn lookupFundingTransfers(requester: Requester) BootstrapError!Presence {
    const chain = profile.fundingChain();
    var storage: [transport.max_request_bytes]u8 = undefined;
    var writer = std.Io.Writer.fixed(&storage);
    profile.writeLookupFundingTransfers(&writer) catch return error.RequestEncodingFailed;
    const expected: adapter.RequestContext = .{
        .destination_id = profile.defaultDestination(),
        .operation = .lookup_transfers,
    };
    return switch (requester.request(.lookup_transfers, writer.buffered())) {
        .failure => error.FundingLookupUnavailable,
        .response => |response| response_result: {
            const decoded = transport.decodeLookupResponse(
                requester.allocator,
                profile.database_uuid,
                expected,
                &chain,
                response.status,
                response.body,
            );
            break :response_result switch (adapter.classifyLookup(expected, decoded)) {
                .transfers => |receipt| switch (dispatcher.classifyLookup(chain, receipt.batch)) {
                    .all_absent => .all_absent,
                    .all_present_exact => .all_present_exact,
                    .partial_application => .partial_application,
                    .transfer_mismatch => .mismatch,
                },
                .retryable => error.FundingLookupMalformed,
                .permanent => error.FundingLookupRejected,
                .destination_mismatch => error.FundingMismatch,
            };
        },
    };
}

fn createFundingTransfers(requester: Requester) BootstrapError!CreateEvidence {
    const chain = profile.fundingChain();
    var storage: [transport.max_request_bytes]u8 = undefined;
    var writer = std.Io.Writer.fixed(&storage);
    profile.writeCreateFundingTransfers(&writer) catch return error.RequestEncodingFailed;
    const expected: adapter.RequestContext = .{
        .destination_id = profile.defaultDestination(),
        .operation = .create_transfers,
    };
    return switch (requester.request(.create_transfers, writer.buffered())) {
        .failure => |certainty| switch (certainty) {
            .definitely_not_sent => error.FundingCreateNotSent,
            .indeterminate => .ambiguous,
        },
        .response => |response| response_result: {
            const decoded = transport.decodeSubmitResponse(
                requester.allocator,
                profile.database_uuid,
                expected,
                &chain,
                response.status,
                response.body,
            );
            break :response_result switch (adapter.classifySubmit(expected, decoded)) {
                .success => .accepted,
                .conflict => .conflict,
                .ambiguous => .ambiguous,
                .known_not_sent, .permanent, .destination_mismatch => .rejected,
            };
        },
    };
}

fn decodeAccountLookupResponse(
    allocator: std.mem.Allocator,
    status: u16,
    body: []const u8,
) DecodeError!Presence {
    if (status == 429 or (status >= 500 and status < 600)) {
        return error.UnavailableResponse;
    }
    if (status >= 400 and status < 500) return error.RejectedResponse;
    if (status != 200) return error.UnavailableResponse;
    var parsed = try parseJson(allocator, body);
    defer parsed.deinit();
    const object = try validateAccountEnvelope(parsed.value, .lookup_accounts, false);
    const payload = try requiredArray(object, "responsePayload");
    if (payload.items.len > profile.accounts.len) return .mismatch;

    var seen = [_]bool{false} ** profile.accounts.len;
    for (payload.items) |value| {
        const actual = try decodeAccount(value);
        var expected_index: ?usize = null;
        for (profile.accounts, 0..) |expected, index| {
            if (actual.id == expected.id) {
                expected_index = index;
                if (!profile.accountMatches(expected, actual)) return .mismatch;
                break;
            }
        }
        const index = expected_index orelse return .mismatch;
        if (seen[index]) return .mismatch;
        seen[index] = true;
    }
    if (payload.items.len == 0) return .all_absent;
    return if (payload.items.len == profile.accounts.len)
        .all_present_exact
    else
        .partial_application;
}

fn decodeAccountCreateResponse(
    allocator: std.mem.Allocator,
    status: u16,
    body: []const u8,
) DecodeError!CreateEvidence {
    if (status == 409) {
        try validateAccountConflict(allocator, body);
        return .conflict;
    }
    if (status == 429 or (status >= 500 and status < 600)) return .ambiguous;
    if (status >= 400 and status < 500) return .rejected;
    if (status != 200) return .ambiguous;
    var parsed = try parseJson(allocator, body);
    defer parsed.deinit();
    const object = try validateAccountEnvelope(parsed.value, .create_accounts, true);
    const payload = try requiredArray(object, "responsePayload");
    if (payload.items.len != 0) return error.MalformedResponse;
    return .accepted;
}

fn parseJson(
    allocator: std.mem.Allocator,
    body: []const u8,
) DecodeError!std.json.Parsed(std.json.Value) {
    if (body.len == 0 or body.len > transport.max_allowed_response_bytes or
        !jsonNestingWithinLimit(body, transport.max_json_nesting))
    {
        return error.MalformedResponse;
    }
    return std.json.parseFromSlice(std.json.Value, allocator, body, .{
        .duplicate_field_behavior = .@"error",
        .parse_numbers = false,
        .max_value_len = transport.max_json_scalar_bytes,
    }) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return error.MalformedResponse;
    };
}

fn validateAccountEnvelope(
    value: std.json.Value,
    operation: transport.GatewayOperation,
    persisted: bool,
) DecodeError!std.json.ObjectMap {
    const object = switch (value) {
        .object => |result| result,
        else => return error.MalformedResponse,
    };
    if (object.count() != 14) return error.MalformedResponse;
    _ = try requiredString(object, "clusterId");
    const database_id = try requiredString(object, "databaseId");
    _ = try requiredString(object, "databaseName");
    _ = try requiredString(object, "docsUrl");
    _ = try requiredString(object, "message");
    const mode = try requiredString(object, "mode");
    const response_operation = try requiredString(object, "operation");
    _ = try requiredString(object, "operationLabel");
    const response_persisted = try requiredBool(object, "persisted");
    _ = try requiredString(object, "provider");
    _ = try requiredJsonUnsigned(u32, object, "replicaCount");
    _ = try requiredString(object, "targetHost");
    _ = try requiredArray(object, "responsePayload");

    if (!std.mem.eql(u8, database_id, profile.database_uuid) or
        !std.mem.eql(u8, mode, "live") or
        !std.mem.eql(u8, response_operation, operation.pathSegment()) or
        response_persisted != persisted)
    {
        return error.RejectedResponse;
    }
    const request_payload = object.get("requestPayload") orelse return error.MalformedResponse;
    const matches = switch (operation) {
        .lookup_accounts => lookupAccountPayloadMatches(request_payload),
        .create_accounts => createAccountPayloadMatches(request_payload),
        else => false,
    };
    if (!matches) return error.RejectedResponse;
    return object;
}

fn lookupAccountPayloadMatches(value: std.json.Value) bool {
    const array = switch (value) {
        .array => |result| result,
        else => return false,
    };
    if (array.items.len != profile.accounts.len) return false;
    for (array.items, profile.accounts) |item, expected| {
        if ((parseUnsigned(u128, item) catch return false) != expected.id) return false;
    }
    return true;
}

fn createAccountPayloadMatches(value: std.json.Value) bool {
    const array = switch (value) {
        .array => |result| result,
        else => return false,
    };
    if (array.items.len != profile.accounts.len) return false;
    for (array.items, profile.accounts) |item, expected| {
        const actual = decodeAccountSpec(item) catch return false;
        if (!profile.accountMatches(expected, actual)) return false;
    }
    return true;
}

fn decodeAccountSpec(value: std.json.Value) DecodeError!profile.AccountRecord {
    const object = switch (value) {
        .object => |result| result,
        else => return error.MalformedResponse,
    };
    if (object.count() != 7) return error.MalformedResponse;
    return .{
        .id = try requiredUnsigned(u128, object, "id"),
        .user_data_128 = try requiredUnsigned(u128, object, "user_data_128"),
        .user_data_64 = try requiredUnsigned(u64, object, "user_data_64"),
        .user_data_32 = try requiredUnsigned(u32, object, "user_data_32"),
        .ledger = try requiredUnsigned(u32, object, "ledger"),
        .code = try requiredUnsigned(u16, object, "code"),
        .flags = try requiredUnsigned(u16, object, "flags"),
    };
}

fn decodeAccount(value: std.json.Value) DecodeError!profile.AccountRecord {
    const object = switch (value) {
        .object => |result| result,
        else => return error.MalformedResponse,
    };
    if (object.count() != 13) return error.MalformedResponse;
    try validateHexScalar(object, "debits_pending");
    try validateHexScalar(object, "debits_posted");
    try validateHexScalar(object, "credits_pending");
    try validateHexScalar(object, "credits_posted");
    _ = try requiredUnsigned(u32, object, "reserved");
    _ = try requiredUnsigned(u64, object, "timestamp");
    return .{
        .id = try requiredUnsigned(u128, object, "id"),
        .user_data_128 = try requiredUnsigned(u128, object, "user_data_128"),
        .user_data_64 = try requiredUnsigned(u64, object, "user_data_64"),
        .user_data_32 = try requiredUnsigned(u32, object, "user_data_32"),
        .ledger = try requiredUnsigned(u32, object, "ledger"),
        .code = try requiredUnsigned(u16, object, "code"),
        .flags = try requiredUnsigned(u16, object, "flags"),
    };
}

fn validateAccountConflict(allocator: std.mem.Allocator, body: []const u8) DecodeError!void {
    var parsed = try parseJson(allocator, body);
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |result| result,
        else => return error.MalformedResponse,
    };
    if ((try requiredString(object, "type")).len == 0) return error.MalformedResponse;
    if (!std.mem.eql(u8, try requiredString(object, "title"), "Conflict")) {
        return error.MalformedResponse;
    }
    if (try requiredJsonUnsigned(u16, object, "status") != 409) return error.MalformedResponse;
    if (object.get("detail")) |detail| switch (detail) {
        .string => {},
        else => return error.MalformedResponse,
    };
    if (object.get("instance")) |instance| switch (instance) {
        .string => {},
        else => return error.MalformedResponse,
    };
    const results = try requiredArray(object, "tbResults");
    if (results.items.len == 0 or results.items.len > profile.accounts.len) {
        return error.MalformedResponse;
    }
    var seen = [_]bool{false} ** profile.accounts.len;
    for (results.items) |value| {
        const entry = switch (value) {
            .object => |result| result,
            else => return error.MalformedResponse,
        };
        if (entry.count() != 2) return error.MalformedResponse;
        const index = try requiredJsonUnsigned(usize, entry, "index");
        _ = try requiredJsonUnsigned(u32, entry, "result");
        if (index >= profile.accounts.len or seen[index]) return error.MalformedResponse;
        seen[index] = true;
    }
}

fn requiredString(object: std.json.ObjectMap, key: []const u8) DecodeError![]const u8 {
    const value = object.get(key) orelse return error.MalformedResponse;
    return switch (value) {
        .string => |result| result,
        else => error.MalformedResponse,
    };
}

fn requiredBool(object: std.json.ObjectMap, key: []const u8) DecodeError!bool {
    const value = object.get(key) orelse return error.MalformedResponse;
    return switch (value) {
        .bool => |result| result,
        else => error.MalformedResponse,
    };
}

fn requiredArray(object: std.json.ObjectMap, key: []const u8) DecodeError!std.json.Array {
    const value = object.get(key) orelse return error.MalformedResponse;
    return switch (value) {
        .array => |result| result,
        else => error.MalformedResponse,
    };
}

fn requiredUnsigned(
    comptime T: type,
    object: std.json.ObjectMap,
    key: []const u8,
) DecodeError!T {
    const value = object.get(key) orelse return error.MalformedResponse;
    return parseUnsigned(T, value);
}

fn requiredJsonUnsigned(
    comptime T: type,
    object: std.json.ObjectMap,
    key: []const u8,
) DecodeError!T {
    const value = object.get(key) orelse return error.MalformedResponse;
    return switch (value) {
        .number_string => |text| parseDigits(T, text, 10),
        else => error.MalformedResponse,
    };
}

fn parseUnsigned(comptime T: type, value: std.json.Value) DecodeError!T {
    const text = switch (value) {
        .number_string => |result| result,
        .string => |result| result,
        else => return error.MalformedResponse,
    };
    return parseDigits(T, text, 10);
}

fn validateHexScalar(object: std.json.ObjectMap, key: []const u8) DecodeError!void {
    const value = object.get(key) orelse return error.MalformedResponse;
    _ = switch (value) {
        .number_string => |text| try parseDigits(u128, text, 10),
        .string => |text| try parseDigits(u128, text, 16),
        else => return error.MalformedResponse,
    };
}

fn parseDigits(comptime T: type, text: []const u8, base: u8) DecodeError!T {
    if (text.len == 0) return error.MalformedResponse;
    for (text) |byte| {
        const valid = if (base == 10)
            byte >= '0' and byte <= '9'
        else
            (byte >= '0' and byte <= '9') or
                (byte >= 'a' and byte <= 'f') or
                (byte >= 'A' and byte <= 'F');
        if (!valid) return error.MalformedResponse;
    }
    return std.fmt.parseInt(T, text, base) catch error.MalformedResponse;
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

const AccountFixture = enum {
    absent,
    exact,
    mismatch,
};

const FundingFixture = enum {
    absent,
    partial,
    exact,
};

const ScriptStep = struct {
    operation: transport.GatewayOperation,
    outcome: transport.RequestOutcome,
};

const ScriptedGateway = struct {
    steps: []const ScriptStep,
    next: usize = 0,
    invalid_request: bool = false,

    fn requester(self: *ScriptedGateway) Requester {
        return .{
            .context = self,
            .allocator = std.testing.allocator,
            .request_fn = request,
        };
    }

    fn request(
        context: *anyopaque,
        operation: transport.GatewayOperation,
        payload: []const u8,
    ) transport.RequestOutcome {
        const self: *ScriptedGateway = @ptrCast(@alignCast(context));
        if (self.next >= self.steps.len) {
            self.invalid_request = true;
            return .{ .failure = .definitely_not_sent };
        }
        const step = self.steps[self.next];
        self.next += 1;
        if (step.operation != operation or !requestPayloadMatches(operation, payload)) {
            self.invalid_request = true;
            return .{ .failure = .definitely_not_sent };
        }
        return step.outcome;
    }

    fn expectComplete(self: *const ScriptedGateway) !void {
        try std.testing.expect(!self.invalid_request);
        try std.testing.expectEqual(self.steps.len, self.next);
    }
};

fn requestPayloadMatches(
    operation: transport.GatewayOperation,
    actual: []const u8,
) bool {
    var storage: [transport.max_request_bytes]u8 = undefined;
    var writer = std.Io.Writer.fixed(&storage);
    switch (operation) {
        .lookup_accounts => profile.writeLookupAccounts(&writer) catch return false,
        .create_accounts => profile.writeCreateAccounts(&writer) catch return false,
        .lookup_transfers => profile.writeLookupFundingTransfers(&writer) catch return false,
        .create_transfers => profile.writeCreateFundingTransfers(&writer) catch return false,
    }
    return std.mem.eql(u8, writer.buffered(), actual);
}

fn writeEnvelopePrefix(
    writer: *std.Io.Writer,
    operation: transport.GatewayOperation,
    persisted: bool,
) std.Io.Writer.Error!void {
    try writer.print(
        "{{\"clusterId\":\"0\",\"databaseId\":\"{s}\",\"databaseName\":\"exchange-core\",\"docsUrl\":\"https://example.test/docs\",\"message\":\"ok\",\"mode\":\"live\",\"operation\":\"{s}\",\"operationLabel\":\"test\",\"persisted\":{},\"provider\":\"gcp\",\"replicaCount\":1,\"requestPayload\":",
        .{ profile.database_uuid, operation.pathSegment(), persisted },
    );
}

fn finishEnvelope(writer: *std.Io.Writer) std.Io.Writer.Error!void {
    try writer.writeAll(",\"targetHost\":\"tb.internal:3000\"}");
}

fn accountLookupFixture(
    storage: *[transport.max_request_bytes]u8,
    fixture: AccountFixture,
) ![]const u8 {
    var writer = std.Io.Writer.fixed(storage);
    try writeEnvelopePrefix(&writer, .lookup_accounts, false);
    try profile.writeLookupAccounts(&writer);
    try writer.writeAll(",\"responsePayload\":[");
    const count: usize = switch (fixture) {
        .absent => 0,
        .exact, .mismatch => profile.accounts.len,
    };
    for (profile.accounts[0..count], 0..) |account, index| {
        if (index != 0) try writer.writeByte(',');
        const credits_posted = if (index == 4)
            "3e8"
        else if (index == 7)
            "7d0"
        else
            "0";
        const flags = if (fixture == .mismatch and index == 0)
            account.flags + 1
        else
            account.flags;
        try writer.print(
            "{{\"id\":\"{d}\",\"debits_pending\":\"0\",\"debits_posted\":\"0\",\"credits_pending\":\"0\",\"credits_posted\":\"{s}\",\"user_data_128\":\"{d}\",\"user_data_64\":{d},\"user_data_32\":{d},\"reserved\":0,\"ledger\":{d},\"code\":{d},\"flags\":{d},\"timestamp\":{d}}}",
            .{
                account.id,
                credits_posted,
                account.user_data_128,
                account.user_data_64,
                account.user_data_32,
                account.ledger,
                account.code,
                flags,
                index + 1,
            },
        );
    }
    try writer.writeByte(']');
    try finishEnvelope(&writer);
    return writer.buffered();
}

fn accountCreateFixture(storage: *[transport.max_request_bytes]u8) ![]const u8 {
    var writer = std.Io.Writer.fixed(storage);
    try writeEnvelopePrefix(&writer, .create_accounts, true);
    try profile.writeCreateAccounts(&writer);
    try writer.writeAll(",\"responsePayload\":[]");
    try finishEnvelope(&writer);
    return writer.buffered();
}

fn fundingLookupFixture(
    storage: *[transport.max_request_bytes]u8,
    fixture: FundingFixture,
) ![]const u8 {
    var writer = std.Io.Writer.fixed(storage);
    try writeEnvelopePrefix(&writer, .lookup_transfers, false);
    try profile.writeLookupFundingTransfers(&writer);
    try writer.writeAll(",\"responsePayload\":[");
    const count: usize = switch (fixture) {
        .absent => 0,
        .partial => 1,
        .exact => profile.funding_transfers.len,
    };
    for (profile.funding_transfers[0..count], 0..) |transfer, index| {
        if (index != 0) try writer.writeByte(',');
        const amount = if (transfer.amount == 1_000) "3e8" else "7d0";
        try writer.print(
            "{{\"id\":\"{d}\",\"debit_account_id\":\"{d}\",\"credit_account_id\":\"{d}\",\"amount\":\"{s}\",\"pending_id\":\"{d}\",\"user_data_128\":\"{d}\",\"user_data_64\":{d},\"user_data_32\":{d},\"timeout\":{d},\"ledger\":{d},\"code\":{d},\"flags\":{d},\"timestamp\":{d}}}",
            .{
                transfer.id,
                transfer.debit_account_id,
                transfer.credit_account_id,
                amount,
                transfer.pending_id,
                transfer.user_data_128,
                transfer.user_data_64,
                transfer.user_data_32,
                transfer.timeout,
                transfer.ledger,
                transfer.code,
                transfer.flags,
                index + 100,
            },
        );
    }
    try writer.writeByte(']');
    try finishEnvelope(&writer);
    return writer.buffered();
}

fn fundingCreateFixture(storage: *[transport.max_request_bytes]u8) ![]const u8 {
    var writer = std.Io.Writer.fixed(storage);
    try writeEnvelopePrefix(&writer, .create_transfers, true);
    try profile.writeCreateFundingTransfers(&writer);
    try writer.writeAll(",\"responsePayload\":[]");
    try finishEnvelope(&writer);
    return writer.buffered();
}

fn fundingConflictFixture(storage: *[transport.max_request_bytes]u8) ![]const u8 {
    var writer = std.Io.Writer.fixed(storage);
    try writer.writeAll(
        "{\"type\":\"about:blank\",\"title\":\"Conflict\",\"status\":409,\"detail\":\"duplicate transfer IDs\",\"instance\":\"/tb/create_transfers\",\"tbResults\":[{\"index\":0,\"result\":1},{\"index\":1,\"result\":1}]}",
    );
    return writer.buffered();
}

test "empty profile creates once and reconciles exact accounts and funding" {
    var account_absent_storage: [transport.max_request_bytes]u8 = undefined;
    const account_absent = try accountLookupFixture(&account_absent_storage, .absent);
    var account_create_storage: [transport.max_request_bytes]u8 = undefined;
    const account_create = try accountCreateFixture(&account_create_storage);
    var account_exact_storage: [transport.max_request_bytes]u8 = undefined;
    const account_exact = try accountLookupFixture(&account_exact_storage, .exact);
    var funding_absent_storage: [transport.max_request_bytes]u8 = undefined;
    const funding_absent = try fundingLookupFixture(&funding_absent_storage, .absent);
    var funding_create_storage: [transport.max_request_bytes]u8 = undefined;
    const funding_create = try fundingCreateFixture(&funding_create_storage);
    var funding_exact_storage: [transport.max_request_bytes]u8 = undefined;
    const funding_exact = try fundingLookupFixture(&funding_exact_storage, .exact);

    const steps = [_]ScriptStep{
        .{ .operation = .lookup_accounts, .outcome = .{ .response = .{ .status = 200, .body = account_absent } } },
        .{ .operation = .create_accounts, .outcome = .{ .response = .{ .status = 200, .body = account_create } } },
        .{ .operation = .lookup_accounts, .outcome = .{ .response = .{ .status = 200, .body = account_exact } } },
        .{ .operation = .lookup_transfers, .outcome = .{ .response = .{ .status = 200, .body = funding_absent } } },
        .{ .operation = .create_transfers, .outcome = .{ .response = .{ .status = 200, .body = funding_create } } },
        .{ .operation = .lookup_transfers, .outcome = .{ .response = .{ .status = 200, .body = funding_exact } } },
    };
    var gateway: ScriptedGateway = .{ .steps = &steps };
    const summary = try ensureProfileWith(gateway.requester());
    try gateway.expectComplete();
    try std.testing.expectEqual(profile.accounts.len, summary.accounts_created);
    try std.testing.expectEqual(@as(usize, 0), summary.accounts_reused);
    try std.testing.expectEqual(@as(usize, 0), summary.accounts_reconciled);
    try std.testing.expectEqual(profile.funding_transfers.len, summary.funding_transfers_created);
    try std.testing.expectEqual(@as(usize, 0), summary.funding_transfers_reused);
    try std.testing.expectEqual(@as(usize, 0), summary.funding_transfers_reconciled);
}

test "existing exact profile is reused without create" {
    var account_storage: [transport.max_request_bytes]u8 = undefined;
    const accounts = try accountLookupFixture(&account_storage, .exact);
    var funding_storage: [transport.max_request_bytes]u8 = undefined;
    const funding = try fundingLookupFixture(&funding_storage, .exact);
    const steps = [_]ScriptStep{
        .{ .operation = .lookup_accounts, .outcome = .{ .response = .{ .status = 200, .body = accounts } } },
        .{ .operation = .lookup_transfers, .outcome = .{ .response = .{ .status = 200, .body = funding } } },
    };
    var gateway: ScriptedGateway = .{ .steps = &steps };
    const summary = try ensureProfileWith(gateway.requester());
    try gateway.expectComplete();
    try std.testing.expectEqual(profile.accounts.len, summary.accounts_reused);
    try std.testing.expectEqual(profile.funding_transfers.len, summary.funding_transfers_reused);
    try std.testing.expectEqual(@as(usize, 0), summary.accounts_created);
    try std.testing.expectEqual(@as(usize, 0), summary.accounts_reconciled);
    try std.testing.expectEqual(@as(usize, 0), summary.funding_transfers_created);
    try std.testing.expectEqual(@as(usize, 0), summary.funding_transfers_reconciled);
}

test "immutable account mismatch fails before any create" {
    var account_storage: [transport.max_request_bytes]u8 = undefined;
    const accounts = try accountLookupFixture(&account_storage, .mismatch);
    const steps = [_]ScriptStep{
        .{ .operation = .lookup_accounts, .outcome = .{ .response = .{ .status = 200, .body = accounts } } },
    };
    var gateway: ScriptedGateway = .{ .steps = &steps };
    try std.testing.expectError(error.AccountMismatch, ensureProfileWith(gateway.requester()));
    try gateway.expectComplete();
}

test "ambiguous account create reconciles a matching lookup without replay" {
    var absent_storage: [transport.max_request_bytes]u8 = undefined;
    const absent = try accountLookupFixture(&absent_storage, .absent);
    var exact_storage: [transport.max_request_bytes]u8 = undefined;
    const exact = try accountLookupFixture(&exact_storage, .exact);
    var funding_storage: [transport.max_request_bytes]u8 = undefined;
    const funding = try fundingLookupFixture(&funding_storage, .exact);
    const steps = [_]ScriptStep{
        .{ .operation = .lookup_accounts, .outcome = .{ .response = .{ .status = 200, .body = absent } } },
        .{ .operation = .create_accounts, .outcome = .{ .failure = .indeterminate } },
        .{ .operation = .lookup_accounts, .outcome = .{ .response = .{ .status = 200, .body = exact } } },
        .{ .operation = .lookup_transfers, .outcome = .{ .response = .{ .status = 200, .body = funding } } },
    };
    var gateway: ScriptedGateway = .{ .steps = &steps };
    const summary = try ensureProfileWith(gateway.requester());
    try gateway.expectComplete();
    try std.testing.expectEqual(@as(usize, 0), summary.accounts_created);
    try std.testing.expectEqual(profile.accounts.len, summary.accounts_reconciled);
    try std.testing.expectEqual(profile.funding_transfers.len, summary.funding_transfers_reused);
}

test "missing accounts after retryable create response fail without replay" {
    var first_storage: [transport.max_request_bytes]u8 = undefined;
    const first = try accountLookupFixture(&first_storage, .absent);
    var second_storage: [transport.max_request_bytes]u8 = undefined;
    const second = try accountLookupFixture(&second_storage, .absent);
    const steps = [_]ScriptStep{
        .{ .operation = .lookup_accounts, .outcome = .{ .response = .{ .status = 200, .body = first } } },
        .{ .operation = .create_accounts, .outcome = .{ .response = .{ .status = 503, .body = "" } } },
        .{ .operation = .lookup_accounts, .outcome = .{ .response = .{ .status = 200, .body = second } } },
    };
    var gateway: ScriptedGateway = .{ .steps = &steps };
    try std.testing.expectError(
        error.AccountCreateUnconfirmed,
        ensureProfileWith(gateway.requester()),
    );
    try gateway.expectComplete();
}

test "account HTTP status evidence separates retryable and rejected responses" {
    try std.testing.expectError(
        error.UnavailableResponse,
        decodeAccountLookupResponse(std.testing.allocator, 429, ""),
    );
    try std.testing.expectError(
        error.UnavailableResponse,
        decodeAccountLookupResponse(std.testing.allocator, 503, ""),
    );
    try std.testing.expectError(
        error.RejectedResponse,
        decodeAccountLookupResponse(std.testing.allocator, 400, ""),
    );
    try std.testing.expectEqual(
        CreateEvidence.ambiguous,
        try decodeAccountCreateResponse(std.testing.allocator, 429, ""),
    );
    try std.testing.expectEqual(
        CreateEvidence.ambiguous,
        try decodeAccountCreateResponse(std.testing.allocator, 503, ""),
    );
    try std.testing.expectEqual(
        CreateEvidence.rejected,
        try decodeAccountCreateResponse(std.testing.allocator, 400, ""),
    );
    try std.testing.expect(isRetryable(error.AccountLookupUnavailable));
    try std.testing.expect(isRetryable(error.AccountCreateUnconfirmed));
    try std.testing.expect(!isRetryable(error.AccountLookupRejected));
    try std.testing.expect(!isRetryable(error.AccountCreateRejected));
}

test "account lookup status maps unavailable and rejected evidence" {
    const unavailable_steps = [_]ScriptStep{
        .{ .operation = .lookup_accounts, .outcome = .{ .response = .{ .status = 429, .body = "" } } },
    };
    var unavailable: ScriptedGateway = .{ .steps = &unavailable_steps };
    try std.testing.expectError(
        error.AccountLookupUnavailable,
        ensureProfileWith(unavailable.requester()),
    );
    try unavailable.expectComplete();

    const rejected_steps = [_]ScriptStep{
        .{ .operation = .lookup_accounts, .outcome = .{ .response = .{ .status = 400, .body = "" } } },
    };
    var rejected: ScriptedGateway = .{ .steps = &rejected_steps };
    try std.testing.expectError(
        error.AccountLookupRejected,
        ensureProfileWith(rejected.requester()),
    );
    try rejected.expectComplete();
}

test "rejected account create with exact lookup is reconciled not created" {
    var absent_storage: [transport.max_request_bytes]u8 = undefined;
    const absent = try accountLookupFixture(&absent_storage, .absent);
    var exact_storage: [transport.max_request_bytes]u8 = undefined;
    const exact = try accountLookupFixture(&exact_storage, .exact);
    var funding_storage: [transport.max_request_bytes]u8 = undefined;
    const funding = try fundingLookupFixture(&funding_storage, .exact);
    const steps = [_]ScriptStep{
        .{ .operation = .lookup_accounts, .outcome = .{ .response = .{ .status = 200, .body = absent } } },
        .{ .operation = .create_accounts, .outcome = .{ .response = .{ .status = 400, .body = "" } } },
        .{ .operation = .lookup_accounts, .outcome = .{ .response = .{ .status = 200, .body = exact } } },
        .{ .operation = .lookup_transfers, .outcome = .{ .response = .{ .status = 200, .body = funding } } },
    };
    var gateway: ScriptedGateway = .{ .steps = &steps };
    const summary = try ensureProfileWith(gateway.requester());
    try gateway.expectComplete();
    try std.testing.expectEqual(@as(usize, 0), summary.accounts_created);
    try std.testing.expectEqual(profile.accounts.len, summary.accounts_reconciled);
}

test "ambiguous funding create reconciles exact lookup without replay" {
    var account_storage: [transport.max_request_bytes]u8 = undefined;
    const accounts = try accountLookupFixture(&account_storage, .exact);
    var funding_absent_storage: [transport.max_request_bytes]u8 = undefined;
    const funding_absent = try fundingLookupFixture(&funding_absent_storage, .absent);
    var funding_exact_storage: [transport.max_request_bytes]u8 = undefined;
    const funding_exact = try fundingLookupFixture(&funding_exact_storage, .exact);
    const steps = [_]ScriptStep{
        .{ .operation = .lookup_accounts, .outcome = .{ .response = .{ .status = 200, .body = accounts } } },
        .{ .operation = .lookup_transfers, .outcome = .{ .response = .{ .status = 200, .body = funding_absent } } },
        .{ .operation = .create_transfers, .outcome = .{ .failure = .indeterminate } },
        .{ .operation = .lookup_transfers, .outcome = .{ .response = .{ .status = 200, .body = funding_exact } } },
    };
    var gateway: ScriptedGateway = .{ .steps = &steps };
    const summary = try ensureProfileWith(gateway.requester());
    try gateway.expectComplete();
    try std.testing.expectEqual(profile.accounts.len, summary.accounts_reused);
    try std.testing.expectEqual(@as(usize, 0), summary.funding_transfers_created);
    try std.testing.expectEqual(
        profile.funding_transfers.len,
        summary.funding_transfers_reconciled,
    );
}

test "funding create conflict reconciles exact lookup without replay" {
    var account_storage: [transport.max_request_bytes]u8 = undefined;
    const accounts = try accountLookupFixture(&account_storage, .exact);
    var funding_absent_storage: [transport.max_request_bytes]u8 = undefined;
    const funding_absent = try fundingLookupFixture(&funding_absent_storage, .absent);
    var conflict_storage: [transport.max_request_bytes]u8 = undefined;
    const conflict = try fundingConflictFixture(&conflict_storage);
    var funding_exact_storage: [transport.max_request_bytes]u8 = undefined;
    const funding_exact = try fundingLookupFixture(&funding_exact_storage, .exact);
    const steps = [_]ScriptStep{
        .{ .operation = .lookup_accounts, .outcome = .{ .response = .{ .status = 200, .body = accounts } } },
        .{ .operation = .lookup_transfers, .outcome = .{ .response = .{ .status = 200, .body = funding_absent } } },
        .{ .operation = .create_transfers, .outcome = .{ .response = .{ .status = 409, .body = conflict } } },
        .{ .operation = .lookup_transfers, .outcome = .{ .response = .{ .status = 200, .body = funding_exact } } },
    };
    var gateway: ScriptedGateway = .{ .steps = &steps };
    const summary = try ensureProfileWith(gateway.requester());
    try gateway.expectComplete();
    try std.testing.expectEqual(@as(usize, 0), summary.funding_transfers_created);
    try std.testing.expectEqual(
        profile.funding_transfers.len,
        summary.funding_transfers_reconciled,
    );
}

test "missing funding after ambiguous create fails without replay" {
    var account_storage: [transport.max_request_bytes]u8 = undefined;
    const accounts = try accountLookupFixture(&account_storage, .exact);
    var first_absent_storage: [transport.max_request_bytes]u8 = undefined;
    const first_absent = try fundingLookupFixture(&first_absent_storage, .absent);
    var second_absent_storage: [transport.max_request_bytes]u8 = undefined;
    const second_absent = try fundingLookupFixture(&second_absent_storage, .absent);
    const steps = [_]ScriptStep{
        .{ .operation = .lookup_accounts, .outcome = .{ .response = .{ .status = 200, .body = accounts } } },
        .{ .operation = .lookup_transfers, .outcome = .{ .response = .{ .status = 200, .body = first_absent } } },
        .{ .operation = .create_transfers, .outcome = .{ .failure = .indeterminate } },
        .{ .operation = .lookup_transfers, .outcome = .{ .response = .{ .status = 200, .body = second_absent } } },
    };
    var gateway: ScriptedGateway = .{ .steps = &steps };
    try std.testing.expectError(
        error.FundingCreateUnconfirmed,
        ensureProfileWith(gateway.requester()),
    );
    try gateway.expectComplete();
}

test "partial funding after ambiguous create fails without replay" {
    var account_storage: [transport.max_request_bytes]u8 = undefined;
    const accounts = try accountLookupFixture(&account_storage, .exact);
    var absent_storage: [transport.max_request_bytes]u8 = undefined;
    const absent = try fundingLookupFixture(&absent_storage, .absent);
    var partial_storage: [transport.max_request_bytes]u8 = undefined;
    const partial = try fundingLookupFixture(&partial_storage, .partial);
    const steps = [_]ScriptStep{
        .{ .operation = .lookup_accounts, .outcome = .{ .response = .{ .status = 200, .body = accounts } } },
        .{ .operation = .lookup_transfers, .outcome = .{ .response = .{ .status = 200, .body = absent } } },
        .{ .operation = .create_transfers, .outcome = .{ .failure = .indeterminate } },
        .{ .operation = .lookup_transfers, .outcome = .{ .response = .{ .status = 200, .body = partial } } },
    };
    var gateway: ScriptedGateway = .{ .steps = &steps };
    try std.testing.expectError(
        error.FundingPartialPresence,
        ensureProfileWith(gateway.requester()),
    );
    try gateway.expectComplete();
}
