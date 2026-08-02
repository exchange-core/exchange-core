//! Pure Parix protocol codec for the opt-in TigerBeetle plugin.
const std = @import("std");
const dispatcher = @import("tigerbeetle_dispatcher.zig");
const outbox = @import("tigerbeetle_outbox.zig");
const settlement = @import("tigerbeetle_settlement.zig");

/// Allocation-free protocol boundary for the Parix HTTPS gateway.
///
/// This module neither performs I/O nor parses untrusted JSON. A server-side
/// transport owns sockets, credentials, endpoint selection, response-size
/// limits, and JSON decoding. It must reduce that work to the typed evidence
/// below before calling the total classifiers.
pub const Operation = dispatcher.ResponseOperation;

pub const RequestContext = struct {
    destination_id: outbox.DestinationId,
    operation: Operation,
};

pub const HttpStatus = enum(u16) {
    ok = 200,
    conflict = 409,
    too_many_requests = 429,
    _,

    pub fn fromInt(value: u16) HttpStatus {
        return @enumFromInt(value);
    }

    pub fn toInt(self: HttpStatus) u16 {
        return @intFromEnum(self);
    }
};

/// Evidence extracted from a Parix response envelope and the request that
/// produced it. `unavailable` is expected for problem-details responses, which
/// do not repeat the database, operation, or request payload.
pub const MatchEvidence = enum {
    unavailable,
    matches,
    mismatch,
};

pub const GatewayModeEvidence = enum {
    unavailable,
    live,
    non_live,
};

pub const ResponseMetadata = struct {
    request: RequestContext,
    database_id: MatchEvidence = .unavailable,
    mode: GatewayModeEvidence = .unavailable,
    response_operation: ?Operation = null,
    request_payload: MatchEvidence = .unavailable,
};

pub const SubmitSuccessEnvelope = struct {
    persisted: bool,
    response_item_count: u32,
};

pub const SubmitConflictProblem = struct {
    /// Number of syntactically valid entries actually present in `tbResults`.
    item_error_count: u32,
    /// Parix can cap public conflict details. A nonempty truncated report still
    /// requires lookup of every stable ID and is therefore valid evidence.
    truncated: bool = false,
};

pub const SubmitResponseBody = union(enum) {
    success: SubmitSuccessEnvelope,
    conflict: SubmitConflictProblem,
    malformed,
};

pub const SubmitHttpResponse = struct {
    status: HttpStatus,
    metadata: ResponseMetadata,
    body: SubmitResponseBody,
};

pub const SubmitTransportOutcome = union(enum) {
    /// The transport proves no request bytes could have reached Parix.
    definitely_not_sent,
    /// A write was attempted, but whether Parix/TigerBeetle committed it is
    /// unknown. Stable-ID lookup is mandatory before any replay.
    ambiguous_write,
    response: SubmitHttpResponse,
};

pub const RateLimitedSubmitPolicy = enum {
    /// Default: quarantine for operator-controlled retry instead of creating a
    /// self-sustaining quota loop inside the settlement dispatcher.
    permanent,
    /// Opt in only when the transport proves the 429 preceded TigerBeetle I/O.
    known_not_sent,
};

pub const SubmitClassificationPolicy = struct {
    rate_limited: RateLimitedSubmitPolicy = .permanent,
};

pub const LookupSuccessEnvelope = struct {
    persisted: bool,
    batch: dispatcher.LookupBatch,
};

pub const LookupResponseBody = union(enum) {
    success: LookupSuccessEnvelope,
    malformed,
};

pub const LookupHttpResponse = struct {
    status: HttpStatus,
    metadata: ResponseMetadata,
    body: LookupResponseBody,
};

pub const LookupTransportOutcome = union(enum) {
    transport_failure,
    response: LookupHttpResponse,
};

/// Classifies every submit transport outcome into dispatcher-owned semantics.
/// No branch retries a possibly committed write without first reconciling it.
pub fn classifySubmit(
    expected: RequestContext,
    outcome: SubmitTransportOutcome,
) dispatcher.SubmitDisposition {
    return classifySubmitWithPolicy(expected, outcome, .{});
}

pub fn classifySubmitWithPolicy(
    expected: RequestContext,
    outcome: SubmitTransportOutcome,
    policy: SubmitClassificationPolicy,
) dispatcher.SubmitDisposition {
    return switch (outcome) {
        .definitely_not_sent => .known_not_sent,
        .ambiguous_write => .ambiguous,
        .response => |response| classifySubmitResponse(expected, response, policy),
    };
}

fn classifySubmitResponse(
    expected: RequestContext,
    response: SubmitHttpResponse,
    policy: SubmitClassificationPolicy,
) dispatcher.SubmitDisposition {
    // Destination evidence dominates status and body interpretation. A 4xx or
    // 5xx from the wrong request/database must never mutate this destination.
    if (destinationMismatch(expected, response.metadata)) return .destination_mismatch;
    if (expected.operation != .create_transfers or
        response.metadata.request.operation != .create_transfers)
    {
        return .permanent;
    }

    const status = response.status.toInt();
    if (status == @intFromEnum(HttpStatus.ok)) {
        const success = switch (response.body) {
            .success => |value| value,
            else => return .ambiguous,
        };
        // Stub and other explicitly non-live modes must never advance a
        // production settlement. Quarantine them instead of retrying against
        // the same misconfigured endpoint. Missing mode remains ambiguous.
        if (response.metadata.mode == .non_live) return .permanent;
        if (response.metadata.database_id != .matches or
            response.metadata.mode != .live or
            response.metadata.response_operation != .create_transfers or
            response.metadata.request_payload != .matches)
        {
            return .ambiguous;
        }
        // `persisted: false` is a deterministic non-production/stub response,
        // not a committed write and not something to spin on automatically.
        if (!success.persisted) return .permanent;
        if (success.response_item_count != 0) return .ambiguous;
        return .{ .success = .{
            .destination_id = expected.destination_id,
            .operation = .create_transfers,
            .persisted = true,
        } };
    }

    if (status == @intFromEnum(HttpStatus.conflict)) {
        const conflict = switch (response.body) {
            .conflict => |value| value,
            else => return .ambiguous,
        };
        if (conflict.item_error_count == 0) return .ambiguous;
        return .{ .conflict = .{
            .request_destination_id = expected.destination_id,
            .request_operation = .create_transfers,
            .item_error_count = conflict.item_error_count,
        } };
    }

    if (status == @intFromEnum(HttpStatus.too_many_requests)) {
        return switch (policy.rate_limited) {
            .permanent => .permanent,
            .known_not_sent => .known_not_sent,
        };
    }
    if (status >= 400 and status < 500) return .permanent;
    if (status >= 500 and status < 600) return .ambiguous;
    return .ambiguous;
}

/// Classifies every lookup transport outcome. Lookups are read-only, so
/// transport failures, 429s, 5xx responses, and malformed successes are safe
/// to retry through the dispatcher's bounded reconciliation backoff.
pub fn classifyLookup(
    expected: RequestContext,
    outcome: LookupTransportOutcome,
) dispatcher.LookupDisposition {
    return switch (outcome) {
        .transport_failure => .retryable,
        .response => |response| classifyLookupResponse(expected, response),
    };
}

fn classifyLookupResponse(
    expected: RequestContext,
    response: LookupHttpResponse,
) dispatcher.LookupDisposition {
    if (destinationMismatch(expected, response.metadata)) return .destination_mismatch;
    if (expected.operation != .lookup_transfers or
        response.metadata.request.operation != .lookup_transfers)
    {
        return .permanent;
    }

    const status = response.status.toInt();
    if (status == @intFromEnum(HttpStatus.ok)) {
        const success = switch (response.body) {
            .success => |value| value,
            .malformed => return .retryable,
        };
        // An explicit stub/non-live result is configuration failure, not a
        // transient read. Missing or malformed live metadata stays retryable.
        if (response.metadata.mode == .non_live) return .permanent;
        if (response.metadata.database_id != .matches or
            response.metadata.mode != .live or
            response.metadata.response_operation != .lookup_transfers or
            response.metadata.request_payload != .matches or
            success.persisted)
        {
            return .retryable;
        }
        return .{ .transfers = .{
            .destination_id = expected.destination_id,
            .operation = .lookup_transfers,
            .persisted = false,
            .batch = success.batch,
        } };
    }

    if (status == @intFromEnum(HttpStatus.too_many_requests)) return .retryable;
    if (status >= 400 and status < 500) return .permanent;
    if (status >= 500 and status < 600) return .retryable;
    return .retryable;
}

fn destinationMismatch(expected: RequestContext, metadata: ResponseMetadata) bool {
    return metadata.request.destination_id != expected.destination_id or
        metadata.database_id == .mismatch;
}

pub const EncodeError = std.Io.Writer.Error || error{
    EmptyTransferChain,
    TooManyTransfers,
};

/// Writes the bare JSON array accepted by Parix `create_transfers`.
/// Wide integers remain quoted decimal strings so JavaScript never rounds
/// them; u32/u16 fields remain JSON integers as required by the public schema.
pub fn writeCreateTransfers(
    writer: *std.Io.Writer,
    chain: *const settlement.TransferChain,
) EncodeError!void {
    const count = try validatedCount(chain);
    try writer.writeByte('[');
    for (chain.storage[0..count], 0..) |transfer, index| {
        if (index != 0) try writer.writeByte(',');
        try writer.writeAll("{\"id\":");
        try writeQuotedU128(writer, transfer.id);
        try writer.writeAll(",\"debit_account_id\":");
        try writeQuotedU128(writer, transfer.debit_account_id);
        try writer.writeAll(",\"credit_account_id\":");
        try writeQuotedU128(writer, transfer.credit_account_id);
        try writer.writeAll(",\"amount\":");
        try writeQuotedU128(writer, transfer.amount);
        try writer.writeAll(",\"pending_id\":");
        try writeQuotedU128(writer, transfer.pending_id);
        try writer.writeAll(",\"user_data_128\":");
        try writeQuotedU128(writer, transfer.user_data_128);
        try writer.writeAll(",\"user_data_64\":");
        try writeQuotedU64(writer, transfer.user_data_64);
        try writer.print(
            ",\"user_data_32\":{d},\"timeout\":{d},\"ledger\":{d},\"code\":{d},\"flags\":{d},\"timestamp\":",
            .{
                transfer.user_data_32,
                transfer.timeout,
                transfer.ledger,
                transfer.code,
                transfer.flags,
            },
        );
        try writeQuotedU64(writer, transfer.timestamp);
        try writer.writeByte('}');
    }
    try writer.writeByte(']');
}

/// Writes the bare stable-ID array accepted by Parix `lookup_transfers`.
pub fn writeLookupTransfers(
    writer: *std.Io.Writer,
    chain: *const settlement.TransferChain,
) EncodeError!void {
    const count = try validatedCount(chain);
    try writer.writeByte('[');
    for (chain.storage[0..count], 0..) |transfer, index| {
        if (index != 0) try writer.writeByte(',');
        try writeQuotedU128(writer, transfer.id);
    }
    try writer.writeByte(']');
}

fn validatedCount(chain: *const settlement.TransferChain) EncodeError!usize {
    if (chain.count == 0) return error.EmptyTransferChain;
    if (chain.count > settlement.max_transfers_per_fill) return error.TooManyTransfers;
    return @as(usize, chain.count);
}

fn writeQuotedU128(writer: *std.Io.Writer, value: u128) std.Io.Writer.Error!void {
    try writer.print("\"{d}\"", .{value});
}

fn writeQuotedU64(writer: *std.Io.Writer, value: u64) std.Io.Writer.Error!void {
    try writer.print("\"{d}\"", .{value});
}

fn testDestination(value: u128) outbox.DestinationId {
    return @enumFromInt(value);
}

fn testRequest(operation: Operation) RequestContext {
    return .{ .destination_id = testDestination(77), .operation = operation };
}

fn testMetadata(operation: Operation) ResponseMetadata {
    return .{
        .request = testRequest(operation),
        .database_id = .matches,
        .mode = .live,
        .response_operation = operation,
        .request_payload = .matches,
    };
}

fn testChain() settlement.TransferChain {
    var chain: settlement.TransferChain = .{
        .group_id = 900,
        .storage = [_]settlement.Transfer{std.mem.zeroes(settlement.Transfer)} ** settlement.max_transfers_per_fill,
        .count = 2,
    };
    chain.storage[0] = .{
        .id = 11,
        .debit_account_id = 21,
        .credit_account_id = 31,
        .amount = 41,
        .pending_id = 0,
        .user_data_128 = 900,
        .user_data_64 = 915_000,
        .user_data_32 = 1,
        .timeout = 0,
        .ledger = 840,
        .code = 300,
        .flags = settlement.linked_flag,
        .timestamp = 0,
    };
    chain.storage[1] = .{
        .id = 12,
        .debit_account_id = 32,
        .credit_account_id = 22,
        .amount = 42,
        .pending_id = 0,
        .user_data_128 = 900,
        .user_data_64 = 915_000,
        .user_data_32 = 1,
        .timeout = 0,
        .ledger = 978,
        .code = 302,
        .flags = 0,
        .timestamp = 0,
    };
    return chain;
}

fn submitResponse(
    status: u16,
    body: SubmitResponseBody,
) SubmitTransportOutcome {
    return .{ .response = .{
        .status = .fromInt(status),
        .metadata = testMetadata(.create_transfers),
        .body = body,
    } };
}

fn lookupResponse(
    status: u16,
    body: LookupResponseBody,
) LookupTransportOutcome {
    return .{ .response = .{
        .status = .fromInt(status),
        .metadata = testMetadata(.lookup_transfers),
        .body = body,
    } };
}

test "Parix writers emit exact bare arrays with canonical integer encoding" {
    const chain = testChain();
    var create_storage: [2048]u8 = undefined;
    var create_writer = std.Io.Writer.fixed(&create_storage);
    try writeCreateTransfers(&create_writer, &chain);
    try std.testing.expectEqualStrings(
        "[{\"id\":\"11\",\"debit_account_id\":\"21\",\"credit_account_id\":\"31\",\"amount\":\"41\",\"pending_id\":\"0\",\"user_data_128\":\"900\",\"user_data_64\":\"915000\",\"user_data_32\":1,\"timeout\":0,\"ledger\":840,\"code\":300,\"flags\":1,\"timestamp\":\"0\"},{\"id\":\"12\",\"debit_account_id\":\"32\",\"credit_account_id\":\"22\",\"amount\":\"42\",\"pending_id\":\"0\",\"user_data_128\":\"900\",\"user_data_64\":\"915000\",\"user_data_32\":1,\"timeout\":0,\"ledger\":978,\"code\":302,\"flags\":0,\"timestamp\":\"0\"}]",
        create_writer.buffered(),
    );

    var lookup_storage: [128]u8 = undefined;
    var lookup_writer = std.Io.Writer.fixed(&lookup_storage);
    try writeLookupTransfers(&lookup_writer, &chain);
    try std.testing.expectEqualStrings("[\"11\",\"12\"]", lookup_writer.buffered());
}

test "Parix writer preserves maximum wide and narrow integer values" {
    var chain = testChain();
    chain.count = 1;
    chain.storage[0] = .{
        .id = std.math.maxInt(u128),
        .debit_account_id = std.math.maxInt(u128),
        .credit_account_id = std.math.maxInt(u128),
        .amount = std.math.maxInt(u128),
        .pending_id = std.math.maxInt(u128),
        .user_data_128 = std.math.maxInt(u128),
        .user_data_64 = std.math.maxInt(u64),
        .user_data_32 = std.math.maxInt(u32),
        .timeout = std.math.maxInt(u32),
        .ledger = std.math.maxInt(u32),
        .code = std.math.maxInt(u16),
        .flags = std.math.maxInt(u16),
        .timestamp = std.math.maxInt(u64),
    };
    var storage: [2048]u8 = undefined;
    var writer = std.Io.Writer.fixed(&storage);
    try writeCreateTransfers(&writer, &chain);
    try std.testing.expectEqualStrings(
        "[{\"id\":\"340282366920938463463374607431768211455\",\"debit_account_id\":\"340282366920938463463374607431768211455\",\"credit_account_id\":\"340282366920938463463374607431768211455\",\"amount\":\"340282366920938463463374607431768211455\",\"pending_id\":\"340282366920938463463374607431768211455\",\"user_data_128\":\"340282366920938463463374607431768211455\",\"user_data_64\":\"18446744073709551615\",\"user_data_32\":4294967295,\"timeout\":4294967295,\"ledger\":4294967295,\"code\":65535,\"flags\":65535,\"timestamp\":\"18446744073709551615\"}]",
        writer.buffered(),
    );
}

test "Parix writers reject invalid chain counts without indexing storage" {
    var chain = testChain();
    chain.count = 0;
    var storage: [16]u8 = undefined;
    var writer = std.Io.Writer.fixed(&storage);
    try std.testing.expectError(error.EmptyTransferChain, writeCreateTransfers(&writer, &chain));
    chain.count = settlement.max_transfers_per_fill + 1;
    try std.testing.expectError(error.TooManyTransfers, writeLookupTransfers(&writer, &chain));
}

test "submit classifier covers transport success and failure certainty" {
    const request = testRequest(.create_transfers);
    try std.testing.expectEqualDeep(
        dispatcher.SubmitDisposition.known_not_sent,
        classifySubmit(request, .definitely_not_sent),
    );
    try std.testing.expectEqualDeep(
        dispatcher.SubmitDisposition.ambiguous,
        classifySubmit(request, .ambiguous_write),
    );

    const success = classifySubmit(request, submitResponse(200, .{ .success = .{
        .persisted = true,
        .response_item_count = 0,
    } }));
    switch (success) {
        .success => |receipt| {
            try std.testing.expectEqual(request.destination_id, receipt.destination_id);
            try std.testing.expectEqual(Operation.create_transfers, receipt.operation);
            try std.testing.expect(receipt.persisted);
        },
        else => return error.ExpectedSubmitSuccess,
    }

    try std.testing.expectEqualDeep(
        dispatcher.SubmitDisposition.permanent,
        classifySubmit(request, submitResponse(200, .{ .success = .{
            .persisted = false,
            .response_item_count = 0,
        } })),
    );
    try std.testing.expectEqualDeep(
        dispatcher.SubmitDisposition.ambiguous,
        classifySubmit(request, submitResponse(200, .{ .success = .{
            .persisted = true,
            .response_item_count = 1,
        } })),
    );
    try std.testing.expectEqualDeep(
        dispatcher.SubmitDisposition.ambiguous,
        classifySubmit(request, submitResponse(200, .malformed)),
    );
    var non_live = submitResponse(200, .{ .success = .{
        .persisted = true,
        .response_item_count = 0,
    } });
    non_live.response.metadata.mode = .non_live;
    try std.testing.expectEqualDeep(
        dispatcher.SubmitDisposition.permanent,
        classifySubmit(request, non_live),
    );
}

test "submit classifier requires lookup for nonempty and truncated conflicts" {
    const request = testRequest(.create_transfers);
    const complete = classifySubmit(request, submitResponse(409, .{ .conflict = .{
        .item_error_count = 2,
    } }));
    switch (complete) {
        .conflict => |receipt| try std.testing.expectEqual(@as(u32, 2), receipt.item_error_count),
        else => return error.ExpectedSubmitConflict,
    }

    const truncated = classifySubmit(request, submitResponse(409, .{ .conflict = .{
        .item_error_count = 10,
        .truncated = true,
    } }));
    switch (truncated) {
        .conflict => |receipt| try std.testing.expectEqual(@as(u32, 10), receipt.item_error_count),
        else => return error.ExpectedTruncatedSubmitConflict,
    }

    try std.testing.expectEqualDeep(
        dispatcher.SubmitDisposition.ambiguous,
        classifySubmit(request, submitResponse(409, .{ .conflict = .{ .item_error_count = 0 } })),
    );
    try std.testing.expectEqualDeep(
        dispatcher.SubmitDisposition.ambiguous,
        classifySubmit(request, submitResponse(409, .malformed)),
    );
}

test "submit classifier has explicit status and destination precedence" {
    const request = testRequest(.create_transfers);
    for ([_]u16{ 400, 401, 402, 403, 404, 422 }) |status| {
        try std.testing.expectEqualDeep(
            dispatcher.SubmitDisposition.permanent,
            classifySubmit(request, submitResponse(status, .malformed)),
        );
    }
    for ([_]u16{ 500, 502, 503, 599 }) |status| {
        try std.testing.expectEqualDeep(
            dispatcher.SubmitDisposition.ambiguous,
            classifySubmit(request, submitResponse(status, .malformed)),
        );
    }
    try std.testing.expectEqualDeep(
        dispatcher.SubmitDisposition.permanent,
        classifySubmit(request, submitResponse(429, .malformed)),
    );
    try std.testing.expectEqualDeep(
        dispatcher.SubmitDisposition.known_not_sent,
        classifySubmitWithPolicy(
            request,
            submitResponse(429, .malformed),
            .{ .rate_limited = .known_not_sent },
        ),
    );

    var wrong_destination = submitResponse(400, .malformed);
    wrong_destination.response.metadata.request.destination_id = testDestination(88);
    try std.testing.expectEqualDeep(
        dispatcher.SubmitDisposition.destination_mismatch,
        classifySubmit(request, wrong_destination),
    );
    var wrong_database = submitResponse(503, .malformed);
    wrong_database.response.metadata.database_id = .mismatch;
    try std.testing.expectEqualDeep(
        dispatcher.SubmitDisposition.destination_mismatch,
        classifySubmit(request, wrong_database),
    );
}

test "lookup classifier covers valid batches retryable failures and permanent statuses" {
    const request = testRequest(.lookup_transfers);
    const chain = testChain();
    const batch = try dispatcher.LookupBatch.init(chain.items());

    const success = classifyLookup(request, lookupResponse(200, .{ .success = .{
        .persisted = false,
        .batch = batch,
    } }));
    switch (success) {
        .transfers => |receipt| {
            try std.testing.expectEqual(request.destination_id, receipt.destination_id);
            try std.testing.expectEqual(Operation.lookup_transfers, receipt.operation);
            try std.testing.expectEqualDeep(batch, receipt.batch);
        },
        else => return error.ExpectedLookupTransfers,
    }

    try std.testing.expectEqualDeep(
        dispatcher.LookupDisposition.retryable,
        classifyLookup(request, .transport_failure),
    );
    try std.testing.expectEqualDeep(
        dispatcher.LookupDisposition.retryable,
        classifyLookup(request, lookupResponse(200, .malformed)),
    );
    try std.testing.expectEqualDeep(
        dispatcher.LookupDisposition.retryable,
        classifyLookup(request, lookupResponse(200, .{ .success = .{
            .persisted = true,
            .batch = batch,
        } })),
    );
    var non_live = lookupResponse(200, .{ .success = .{
        .persisted = false,
        .batch = batch,
    } });
    non_live.response.metadata.mode = .non_live;
    try std.testing.expectEqualDeep(
        dispatcher.LookupDisposition.permanent,
        classifyLookup(request, non_live),
    );
    for ([_]u16{ 400, 401, 402, 403, 404, 409, 422 }) |status| {
        try std.testing.expectEqualDeep(
            dispatcher.LookupDisposition.permanent,
            classifyLookup(request, lookupResponse(status, .malformed)),
        );
    }
    for ([_]u16{ 429, 500, 502, 503, 599 }) |status| {
        try std.testing.expectEqualDeep(
            dispatcher.LookupDisposition.retryable,
            classifyLookup(request, lookupResponse(status, .malformed)),
        );
    }

    var wrong_destination = lookupResponse(500, .malformed);
    wrong_destination.response.metadata.request.destination_id = testDestination(88);
    try std.testing.expectEqualDeep(
        dispatcher.LookupDisposition.destination_mismatch,
        classifyLookup(request, wrong_destination),
    );
    var wrong_database = lookupResponse(404, .malformed);
    wrong_database.response.metadata.database_id = .mismatch;
    try std.testing.expectEqualDeep(
        dispatcher.LookupDisposition.destination_mismatch,
        classifyLookup(request, wrong_database),
    );
}
