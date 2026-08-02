//! Parix host integration for the opt-in TigerBeetle plugin.
const std = @import("std");
const builtin = @import("builtin");
const tigerbeetle = @import("tigerbeetle");

const dispatcher = tigerbeetle.dispatcher;
const outbox = tigerbeetle.outbox;
const parix = tigerbeetle.parix_adapter;
const plugin = tigerbeetle.plugin;
const settlement = tigerbeetle.settlement;

comptime {
    if (builtin.target.cpu.arch.isWasm()) {
        @compileError("ParixHttpTransport is native-only and must not enter a WebAssembly build");
    }
}

pub const max_api_origin_bytes: usize = 2 * 1024;
pub const max_database_id_bytes: usize = 36;
pub const max_auth_bytes: usize = 4 * 1024;
pub const max_request_bytes: usize = 16 * 1024;
pub const max_allowed_response_bytes: usize = 8 * 1024 * 1024;
pub const max_json_scalar_bytes: usize = 4 * 1024;
pub const max_json_nesting: usize = 32;
pub const max_deadline: std.Io.Duration = .fromSeconds(5 * 60);

const max_endpoint_bytes = max_api_origin_bytes +
    "/api/v1/databases/".len +
    max_database_id_bytes +
    "/tb/lookup_transfers".len;

pub const Auth = union(enum) {
    api_key: []const u8,
    bearer: []const u8,
};

pub const Config = struct {
    api_origin: []const u8,
    database_id: []const u8,
    expected_destination_id: outbox.DestinationId,
    auth: Auth,
    deadline: std.Io.Duration = .fromSeconds(10),
    max_response_bytes: usize = 64 * 1024,

    /// Plain HTTP is never accepted in a production build. Tests must also
    /// opt in explicitly so an HTTP fixture cannot silently become production
    /// configuration.
    allow_http_in_tests: bool = false,
};

pub const ConfigError = error{
    ApiOriginEmpty,
    ApiOriginTooLong,
    InvalidApiOrigin,
    ApiOriginMissingHost,
    InsecureApiOrigin,
    ApiOriginHasUserinfo,
    ApiOriginHasPath,
    ApiOriginHasQuery,
    ApiOriginHasFragment,
    InvalidDatabaseId,
    DestinationIdMismatch,
    AuthEmpty,
    AuthTooLong,
    InvalidAuth,
    InvalidDeadline,
    InvalidResponseLimit,
};

pub const InitError = ConfigError || std.mem.Allocator.Error;

/// Operations deliberately form a closed set so callers cannot smuggle path
/// separators into the reusable request primitive. Account bootstrap can use
/// the same bounded TLS/auth path without duplicating transport code.
pub const GatewayOperation = enum {
    create_accounts,
    create_transfers,
    lookup_accounts,
    lookup_transfers,

    pub fn pathSegment(self: GatewayOperation) []const u8 {
        return @tagName(self);
    }

    pub fn isWrite(self: GatewayOperation) bool {
        return switch (self) {
            .create_accounts, .create_transfers => true,
            .lookup_accounts, .lookup_transfers => false,
        };
    }
};

pub const FailureCertainty = enum {
    /// No call capable of writing request bytes was reached.
    definitely_not_sent,
    /// A body write started, so a create may have committed remotely.
    indeterminate,
};

pub const GatewayResponse = struct {
    status: u16,
    /// Borrowed until the next request or `deinit` on this client.
    body: []const u8,
};

pub const RequestOutcome = union(enum) {
    failure: FailureCertainty,
    response: GatewayResponse,
};

const AuthKind = enum {
    api_key,
    bearer,
};

const AttemptStage = enum(u8) {
    preparing,
    write_started,
    response_complete,
};

const HttpTaskResult = union(enum) {
    failed,
    response: struct {
        status: u16,
        body_len: usize,
    },
};

const DeadlineTaskResult = enum {
    expired,
    canceled,
};

const RaceResult = union(enum) {
    request: HttpTaskResult,
    deadline: DeadlineTaskResult,
};

const RequestRace = std.Io.Select(RaceResult);

/// Bounded, synchronous Parix gateway client.
///
/// The value must remain at a stable address while a `plugin.Transport`
/// returned by `asTransport` is in use. Calls are serialized because the
/// response body is stored in client-owned bounded scratch memory.
pub const GatewayClient = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    http_client: std.http.Client,
    api_origin: []u8,
    database_id: []u8,
    expected_destination_id: outbox.DestinationId,
    auth_kind: AuthKind,
    auth_header: []u8,
    deadline: std.Io.Duration,
    max_response_bytes: usize,
    response_storage: []u8,
    busy: std.atomic.Value(bool) = .init(false),

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        config: Config,
    ) InitError!GatewayClient {
        const validated = try validateConfig(config);

        const origin = try allocator.dupe(u8, validated.normalized_origin);
        errdefer allocator.free(origin);

        const database_id = try allocator.dupe(u8, config.database_id);
        errdefer allocator.free(database_id);

        const auth_kind: AuthKind, const auth_header: []u8 = switch (config.auth) {
            .api_key => |secret| .{ .api_key, try allocator.dupe(u8, secret) },
            .bearer => |secret| bearer: {
                const header = try allocator.alloc(u8, "Bearer ".len + secret.len);
                @memcpy(header[0.."Bearer ".len], "Bearer ");
                @memcpy(header["Bearer ".len..], secret);
                break :bearer .{ .bearer, header };
            },
        };
        errdefer {
            std.crypto.secureZero(u8, auth_header);
            allocator.free(auth_header);
        }

        const response_storage = try allocator.alloc(u8, config.max_response_bytes + 1);
        errdefer allocator.free(response_storage);

        return .{
            .allocator = allocator,
            .io = io,
            .http_client = .{
                .allocator = allocator,
                .io = io,
            },
            .api_origin = origin,
            .database_id = database_id,
            .expected_destination_id = config.expected_destination_id,
            .auth_kind = auth_kind,
            .auth_header = auth_header,
            .deadline = config.deadline,
            .max_response_bytes = config.max_response_bytes,
            .response_storage = response_storage,
        };
    }

    pub fn deinit(self: *GatewayClient) void {
        std.debug.assert(!self.busy.load(.acquire));
        self.http_client.deinit();
        std.crypto.secureZero(u8, self.auth_header);
        self.allocator.free(self.auth_header);
        self.allocator.free(self.response_storage);
        self.allocator.free(self.database_id);
        self.allocator.free(self.api_origin);
        self.* = undefined;
    }

    pub fn destinationId(self: *const GatewayClient) outbox.DestinationId {
        return self.expected_destination_id;
    }

    /// Performs one bounded authenticated POST. A response body is borrowed
    /// from this client and is invalidated by the next request.
    pub fn request(
        self: *GatewayClient,
        operation: GatewayOperation,
        payload: []const u8,
    ) RequestOutcome {
        if (payload.len == 0 or payload.len > max_request_bytes) {
            return .{ .failure = .definitely_not_sent };
        }
        if (!self.tryAcquire()) {
            return .{ .failure = .definitely_not_sent };
        }
        defer self.release();

        return self.requestExclusive(operation, payload);
    }

    fn requestExclusive(
        self: *GatewayClient,
        operation: GatewayOperation,
        payload: []const u8,
    ) RequestOutcome {
        std.debug.assert(self.busy.load(.acquire));
        if (payload.len == 0 or payload.len > max_request_bytes) {
            return .{ .failure = .definitely_not_sent };
        }

        var stage: std.atomic.Value(u8) = .init(@intFromEnum(AttemptStage.preparing));
        const deadline = (std.Io.Timeout{ .duration = .{
            .raw = self.deadline,
            .clock = .boot,
        } }).toDeadline(self.io);

        var result_buffer: [2]RaceResult = undefined;
        var race = RequestRace.init(self.io, &result_buffer);
        race.concurrent(
            .request,
            performHttpTask,
            .{ self, operation, payload, &stage },
        ) catch {
            return .{ .failure = failureCertainty(loadStage(&stage)) };
        };
        race.concurrent(
            .deadline,
            waitForDeadline,
            .{ deadline, self.io },
        ) catch {
            if (cancelRaceForRequest(&race)) |completed| {
                return taskOutcome(self, completed, loadStage(&stage));
            }
            return .{ .failure = failureCertainty(loadStage(&stage)) };
        };

        const first = race.await() catch {
            if (cancelRaceForRequest(&race)) |completed| {
                return taskOutcome(self, completed, loadStage(&stage));
            }
            return .{ .failure = failureCertainty(loadStage(&stage)) };
        };

        return switch (first) {
            .request => |completed| result: {
                race.cancelDiscard();
                break :result taskOutcome(self, completed, loadStage(&stage));
            },
            .deadline => result: {
                // Expiry dominates even if the request completes while it is
                // being canceled. A create that reached the body write stays
                // ambiguous; a request canceled before that point is known not sent.
                race.cancelDiscard();
                break :result .{ .failure = failureCertainty(loadStage(&stage)) };
            },
        };
    }

    /// Returns the synchronous settlement transport facade. `self` must stay
    /// alive and stationary until the plugin has stopped using the facade.
    pub fn asTransport(self: *GatewayClient) plugin.Transport {
        return .{
            .context = self,
            .vtable = &transport_vtable,
        };
    }

    fn submit(
        self: *GatewayClient,
        destination_id: outbox.DestinationId,
        chain: *const settlement.TransferChain,
    ) dispatcher.SubmitDisposition {
        if (destination_id != self.expected_destination_id) {
            return .destination_mismatch;
        }

        var request_storage: [max_request_bytes]u8 = undefined;
        var writer = std.Io.Writer.fixed(&request_storage);
        parix.writeCreateTransfers(&writer, chain) catch return .permanent;
        if (!self.tryAcquire()) return .known_not_sent;
        defer self.release();

        const expected: parix.RequestContext = .{
            .destination_id = destination_id,
            .operation = .create_transfers,
        };
        const outcome: parix.SubmitTransportOutcome = switch (self.requestExclusive(
            .create_transfers,
            writer.buffered(),
        )) {
            .failure => |certainty| switch (certainty) {
                .definitely_not_sent => .definitely_not_sent,
                .indeterminate => .ambiguous_write,
            },
            .response => |response| decodeSubmitResponse(
                self.allocator,
                self.database_id,
                expected,
                chain,
                response.status,
                response.body,
            ),
        };
        return parix.classifySubmit(expected, outcome);
    }

    fn lookup(
        self: *GatewayClient,
        destination_id: outbox.DestinationId,
        chain: *const settlement.TransferChain,
        cause: outbox.ReconciliationCause,
    ) dispatcher.LookupDisposition {
        _ = cause;
        if (destination_id != self.expected_destination_id) {
            return .destination_mismatch;
        }

        var request_storage: [max_request_bytes]u8 = undefined;
        var writer = std.Io.Writer.fixed(&request_storage);
        parix.writeLookupTransfers(&writer, chain) catch return .permanent;
        if (!self.tryAcquire()) return .retryable;
        defer self.release();

        const expected: parix.RequestContext = .{
            .destination_id = destination_id,
            .operation = .lookup_transfers,
        };
        const outcome: parix.LookupTransportOutcome = switch (self.requestExclusive(
            .lookup_transfers,
            writer.buffered(),
        )) {
            .failure => .transport_failure,
            .response => |response| decodeLookupResponse(
                self.allocator,
                self.database_id,
                expected,
                chain,
                response.status,
                response.body,
            ),
        };
        return parix.classifyLookup(expected, outcome);
    }

    fn tryAcquire(self: *GatewayClient) bool {
        return self.busy.cmpxchgStrong(false, true, .acquire, .monotonic) == null;
    }

    fn release(self: *GatewayClient) void {
        self.busy.store(false, .release);
    }
};

pub const ParixHttpTransport = GatewayClient;

const transport_vtable: plugin.Transport.VTable = .{
    .submit = transportSubmit,
    .lookup = transportLookup,
};

fn transportSubmit(
    context: *anyopaque,
    destination_id: outbox.DestinationId,
    chain: *const settlement.TransferChain,
) dispatcher.SubmitDisposition {
    const self: *GatewayClient = @ptrCast(@alignCast(context));
    return self.submit(destination_id, chain);
}

fn transportLookup(
    context: *anyopaque,
    destination_id: outbox.DestinationId,
    chain: *const settlement.TransferChain,
    cause: outbox.ReconciliationCause,
) dispatcher.LookupDisposition {
    const self: *GatewayClient = @ptrCast(@alignCast(context));
    return self.lookup(destination_id, chain, cause);
}

const ValidatedConfig = struct {
    normalized_origin: []const u8,
};

fn validateConfig(config: Config) ConfigError!ValidatedConfig {
    if (config.api_origin.len == 0) return error.ApiOriginEmpty;
    if (config.api_origin.len > max_api_origin_bytes) return error.ApiOriginTooLong;
    if (containsCrLf(config.api_origin)) return error.InvalidApiOrigin;

    const uri = std.Uri.parse(config.api_origin) catch return error.InvalidApiOrigin;
    const is_https = std.ascii.eqlIgnoreCase(uri.scheme, "https");
    const is_test_http = std.ascii.eqlIgnoreCase(uri.scheme, "http") and
        builtin.is_test and config.allow_http_in_tests;
    if (!is_https and !is_test_http) return error.InsecureApiOrigin;
    if (uri.host == null) return error.ApiOriginMissingHost;
    if (uri.user != null or uri.password != null) return error.ApiOriginHasUserinfo;
    if (!uri.path.isEmpty() and !componentEquals(uri.path, "/")) {
        return error.ApiOriginHasPath;
    }
    if (uri.query != null) return error.ApiOriginHasQuery;
    if (uri.fragment != null) return error.ApiOriginHasFragment;

    const parsed_destination = try parseDatabaseDestination(config.database_id);
    if (parsed_destination != config.expected_destination_id) {
        return error.DestinationIdMismatch;
    }

    const secret = switch (config.auth) {
        .api_key => |value| value,
        .bearer => |value| value,
    };
    if (secret.len == 0) return error.AuthEmpty;
    if (secret.len > max_auth_bytes) return error.AuthTooLong;
    if (containsInvalidHeaderByte(secret)) return error.InvalidAuth;
    if (config.deadline.nanoseconds <= 0 or
        config.deadline.nanoseconds > max_deadline.nanoseconds)
    {
        return error.InvalidDeadline;
    }
    if (config.max_response_bytes == 0 or
        config.max_response_bytes > max_allowed_response_bytes)
    {
        return error.InvalidResponseLimit;
    }

    const normalized_origin = if (componentEquals(uri.path, "/"))
        config.api_origin[0 .. config.api_origin.len - 1]
    else
        config.api_origin;
    return .{ .normalized_origin = normalized_origin };
}

/// Parses the canonical lower-case UUID spelling used in public Parix paths.
/// Upper-case or otherwise noncanonical spellings are rejected even though
/// they could represent the same numeric destination.
pub fn parseDatabaseDestination(database_id: []const u8) ConfigError!outbox.DestinationId {
    if (database_id.len != max_database_id_bytes) return error.InvalidDatabaseId;

    var compact: [32]u8 = undefined;
    var output_index: usize = 0;
    for (database_id, 0..) |byte, index| {
        const separator = index == 8 or index == 13 or index == 18 or index == 23;
        if (separator) {
            if (byte != '-') return error.InvalidDatabaseId;
            continue;
        }
        if (!isLowerHex(byte)) return error.InvalidDatabaseId;
        compact[output_index] = byte;
        output_index += 1;
    }
    if (output_index != compact.len) return error.InvalidDatabaseId;

    const raw = std.fmt.parseInt(u128, &compact, 16) catch return error.InvalidDatabaseId;
    if (raw == 0) return error.InvalidDatabaseId;
    return @enumFromInt(raw);
}

fn isLowerHex(byte: u8) bool {
    return (byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f');
}

fn componentEquals(component: std.Uri.Component, expected: []const u8) bool {
    return switch (component) {
        .raw, .percent_encoded => |value| std.mem.eql(u8, value, expected),
    };
}

fn containsCrLf(bytes: []const u8) bool {
    for (bytes) |byte| if (byte == '\r' or byte == '\n') return true;
    return false;
}

fn containsInvalidHeaderByte(bytes: []const u8) bool {
    for (bytes) |byte| {
        if (byte < 0x20 or byte == 0x7f) return true;
    }
    return false;
}

fn performHttpTask(
    self: *GatewayClient,
    operation: GatewayOperation,
    payload: []const u8,
    stage: *std.atomic.Value(u8),
) HttpTaskResult {
    var endpoint_storage: [max_endpoint_bytes]u8 = undefined;
    const endpoint = std.fmt.bufPrint(
        &endpoint_storage,
        "{s}/api/v1/databases/{s}/tb/{s}",
        .{ self.api_origin, self.database_id, operation.pathSegment() },
    ) catch return .failed;
    const uri = std.Uri.parse(endpoint) catch return .failed;

    const request_headers = [_]std.http.Header{
        .{ .name = "accept", .value = "application/json" },
        .{
            .name = switch (self.auth_kind) {
                .api_key => "x-api-key",
                .bearer => "authorization",
            },
            .value = self.auth_header,
        },
    };
    var request = self.http_client.request(.POST, uri, .{
        .redirect_behavior = .not_allowed,
        .headers = .{
            .content_type = .{ .override = "application/json" },
            .accept_encoding = .{ .override = "identity" },
        },
        // Zig 0.16 stores but does not serialize privileged_headers. Keep the
        // credential in extra_headers only because redirects are forbidden,
        // so it can never be forwarded to a different origin.
        .extra_headers = &request_headers,
    }) catch return .failed;
    defer request.deinit();

    request.accept_encoding = @splat(false);
    request.accept_encoding[@intFromEnum(std.http.ContentEncoding.identity)] = true;
    stage.store(@intFromEnum(AttemptStage.write_started), .release);
    request.sendBodyComplete(@constCast(payload)) catch {
        markConnectionClosing(&request);
        return .failed;
    };

    var response = request.receiveHead(&.{}) catch {
        markConnectionClosing(&request);
        return .failed;
    };
    if (response.head.content_encoding != .identity) {
        markConnectionClosing(&request);
        return .failed;
    }
    if (response.head.content_length) |content_length| {
        if (content_length > self.max_response_bytes) {
            markConnectionClosing(&request);
            return .failed;
        }
    }

    var transfer_storage: [8 * 1024]u8 = undefined;
    const body_reader = response.reader(&transfer_storage);
    var body_len: usize = 0;
    while (body_len < self.response_storage.len) {
        const remaining = self.response_storage[body_len..];
        const read_len = body_reader.readSliceShort(remaining) catch {
            markConnectionClosing(&request);
            return .failed;
        };
        body_len += read_len;
        // readSliceShort returns less than requested only after reaching EOF.
        if (read_len < remaining.len) break;
    }
    if (body_len > self.max_response_bytes) {
        markConnectionClosing(&request);
        return .failed;
    }

    stage.store(@intFromEnum(AttemptStage.response_complete), .release);
    return .{ .response = .{
        .status = @intFromEnum(response.head.status),
        .body_len = body_len,
    } };
}

fn markConnectionClosing(request: *std.http.Client.Request) void {
    if (request.connection) |connection| connection.closing = true;
}

fn waitForDeadline(timeout: std.Io.Timeout, io: std.Io) DeadlineTaskResult {
    timeout.sleep(io) catch return .canceled;
    return .expired;
}

fn cancelRaceForRequest(race: *RequestRace) ?HttpTaskResult {
    var completed: ?HttpTaskResult = null;
    while (race.cancel()) |remaining| switch (remaining) {
        .request => |request| completed = request,
        .deadline => {},
    };
    return completed;
}

fn loadStage(stage: *const std.atomic.Value(u8)) AttemptStage {
    return @enumFromInt(stage.load(.acquire));
}

fn failureCertainty(stage: AttemptStage) FailureCertainty {
    return switch (stage) {
        .preparing => .definitely_not_sent,
        .write_started, .response_complete => .indeterminate,
    };
}

fn taskOutcome(
    self: *GatewayClient,
    completed: HttpTaskResult,
    stage: AttemptStage,
) RequestOutcome {
    return switch (completed) {
        .failed => .{ .failure = failureCertainty(stage) },
        .response => |response| .{ .response = .{
            .status = response.status,
            .body = self.response_storage[0..response.body_len],
        } },
    };
}

const DecodeError = error{MalformedResponse};

const DecodedEnvelope = struct {
    object: std.json.ObjectMap,
    metadata: parix.ResponseMetadata,
};

/// Converts one bounded raw response into the adapter's submit evidence.
/// Parsing failures after a create write remain ambiguous through the adapter.
pub fn decodeSubmitResponse(
    allocator: std.mem.Allocator,
    expected_database_id: []const u8,
    request: parix.RequestContext,
    chain: *const settlement.TransferChain,
    status: u16,
    body: []const u8,
) parix.SubmitTransportOutcome {
    const http_status = parix.HttpStatus.fromInt(status);
    if (status != @intFromEnum(parix.HttpStatus.ok) and
        status != @intFromEnum(parix.HttpStatus.conflict))
    {
        return .{ .response = .{
            .status = http_status,
            .metadata = .{ .request = request },
            .body = .malformed,
        } };
    }
    if (!jsonNestingWithinLimit(body, max_json_nesting)) {
        return malformedSubmitResponse(http_status, request);
    }

    const parsed = std.json.parseFromSlice(std.json.Value, allocator, body, .{
        .duplicate_field_behavior = .@"error",
        .parse_numbers = false,
        .max_value_len = max_json_scalar_bytes,
    }) catch return malformedSubmitResponse(http_status, request);
    defer parsed.deinit();

    if (status == @intFromEnum(parix.HttpStatus.conflict)) {
        const conflict = decodeConflict(parsed.value, chain) catch {
            return malformedSubmitResponse(http_status, request);
        };
        return .{ .response = .{
            .status = http_status,
            .metadata = .{ .request = request },
            .body = .{ .conflict = conflict },
        } };
    }

    const envelope = decodeEnvelope(
        parsed.value,
        expected_database_id,
        request,
        chain,
    ) catch return malformedSubmitResponse(http_status, request);
    if (!validateEnvelopeContract(envelope.object)) {
        return .{ .response = .{
            .status = http_status,
            .metadata = envelope.metadata,
            .body = .malformed,
        } };
    }
    const persisted = requiredBool(envelope.object, "persisted") catch {
        return malformedSubmitWithMetadata(http_status, envelope.metadata);
    };
    const response_payload = requiredArray(envelope.object, "responsePayload") catch {
        return malformedSubmitWithMetadata(http_status, envelope.metadata);
    };
    const response_count = std.math.cast(u32, response_payload.items.len) orelse {
        return malformedSubmitWithMetadata(http_status, envelope.metadata);
    };
    return .{ .response = .{
        .status = http_status,
        .metadata = envelope.metadata,
        .body = .{ .success = .{
            .persisted = persisted,
            .response_item_count = response_count,
        } },
    } };
}

/// Converts one bounded raw response into the adapter's lookup evidence.
pub fn decodeLookupResponse(
    allocator: std.mem.Allocator,
    expected_database_id: []const u8,
    request: parix.RequestContext,
    chain: *const settlement.TransferChain,
    status: u16,
    body: []const u8,
) parix.LookupTransportOutcome {
    const http_status = parix.HttpStatus.fromInt(status);
    if (status != @intFromEnum(parix.HttpStatus.ok) or
        !jsonNestingWithinLimit(body, max_json_nesting))
    {
        return malformedLookupResponse(http_status, request);
    }

    const parsed = std.json.parseFromSlice(std.json.Value, allocator, body, .{
        .duplicate_field_behavior = .@"error",
        .parse_numbers = false,
        .max_value_len = max_json_scalar_bytes,
    }) catch return malformedLookupResponse(http_status, request);
    defer parsed.deinit();

    const envelope = decodeEnvelope(
        parsed.value,
        expected_database_id,
        request,
        chain,
    ) catch return malformedLookupResponse(http_status, request);
    if (!validateEnvelopeContract(envelope.object)) {
        return .{ .response = .{
            .status = http_status,
            .metadata = envelope.metadata,
            .body = .malformed,
        } };
    }
    const persisted = requiredBool(envelope.object, "persisted") catch {
        return malformedLookupWithMetadata(http_status, envelope.metadata);
    };
    const response_payload = requiredArray(envelope.object, "responsePayload") catch {
        return malformedLookupWithMetadata(http_status, envelope.metadata);
    };
    if (response_payload.items.len > settlement.max_transfers_per_fill) {
        return malformedLookupWithMetadata(http_status, envelope.metadata);
    }

    var decoded: [settlement.max_transfers_per_fill]settlement.Transfer = undefined;
    for (response_payload.items, 0..) |value, index| {
        decoded[index] = decodeLookupTransfer(value, chain) catch {
            return malformedLookupWithMetadata(http_status, envelope.metadata);
        };
    }
    const batch = dispatcher.LookupBatch.init(decoded[0..response_payload.items.len]) catch {
        return malformedLookupWithMetadata(http_status, envelope.metadata);
    };
    return .{ .response = .{
        .status = http_status,
        .metadata = envelope.metadata,
        .body = .{ .success = .{
            .persisted = persisted,
            .batch = batch,
        } },
    } };
}

fn malformedSubmitResponse(
    status: parix.HttpStatus,
    request: parix.RequestContext,
) parix.SubmitTransportOutcome {
    return malformedSubmitWithMetadata(status, .{ .request = request });
}

fn malformedSubmitWithMetadata(
    status: parix.HttpStatus,
    metadata: parix.ResponseMetadata,
) parix.SubmitTransportOutcome {
    return .{ .response = .{
        .status = status,
        .metadata = metadata,
        .body = .malformed,
    } };
}

fn malformedLookupResponse(
    status: parix.HttpStatus,
    request: parix.RequestContext,
) parix.LookupTransportOutcome {
    return malformedLookupWithMetadata(status, .{ .request = request });
}

fn malformedLookupWithMetadata(
    status: parix.HttpStatus,
    metadata: parix.ResponseMetadata,
) parix.LookupTransportOutcome {
    return .{ .response = .{
        .status = status,
        .metadata = metadata,
        .body = .malformed,
    } };
}

fn decodeEnvelope(
    value: std.json.Value,
    expected_database_id: []const u8,
    request: parix.RequestContext,
    chain: *const settlement.TransferChain,
) DecodeError!DecodedEnvelope {
    const object = switch (value) {
        .object => |result| result,
        else => return error.MalformedResponse,
    };
    var metadata: parix.ResponseMetadata = .{ .request = request };
    if (object.get("databaseId")) |database_value| switch (database_value) {
        .string => |database_id| {
            metadata.database_id = if (std.mem.eql(u8, database_id, expected_database_id))
                .matches
            else
                .mismatch;
        },
        else => {},
    };
    if (object.get("mode")) |mode_value| switch (mode_value) {
        .string => |mode| {
            metadata.mode = if (std.mem.eql(u8, mode, "live")) .live else .non_live;
        },
        else => {},
    };
    if (object.get("operation")) |operation_value| switch (operation_value) {
        .string => |operation| {
            metadata.response_operation = parseResponseOperation(operation);
        },
        else => {},
    };
    if (object.get("requestPayload")) |payload| {
        const matches = switch (request.operation) {
            .create_transfers => createPayloadMatches(payload, chain) catch false,
            .lookup_transfers => lookupPayloadMatches(payload, chain) catch false,
        };
        metadata.request_payload = if (matches) .matches else .mismatch;
    }
    return .{ .object = object, .metadata = metadata };
}

fn validateEnvelopeContract(object: std.json.ObjectMap) bool {
    _ = requiredString(object, "clusterId") catch return false;
    _ = requiredString(object, "databaseId") catch return false;
    _ = requiredString(object, "databaseName") catch return false;
    _ = requiredString(object, "docsUrl") catch return false;
    _ = requiredString(object, "message") catch return false;
    _ = requiredString(object, "mode") catch return false;
    _ = requiredString(object, "operation") catch return false;
    _ = requiredString(object, "operationLabel") catch return false;
    _ = requiredBool(object, "persisted") catch return false;
    _ = requiredString(object, "provider") catch return false;
    _ = requiredJsonUnsigned(u32, object, "replicaCount") catch return false;
    _ = object.get("requestPayload") orelse return false;
    _ = object.get("responsePayload") orelse return false;
    _ = requiredString(object, "targetHost") catch return false;
    return true;
}

fn parseResponseOperation(value: []const u8) ?parix.Operation {
    if (std.mem.eql(u8, value, "create_transfers")) return .create_transfers;
    if (std.mem.eql(u8, value, "lookup_transfers")) return .lookup_transfers;
    return null;
}

fn decodeConflict(
    value: std.json.Value,
    chain: *const settlement.TransferChain,
) DecodeError!parix.SubmitConflictProblem {
    const chain_items = try validatedChainItems(chain);
    const object = switch (value) {
        .object => |result| result,
        else => return error.MalformedResponse,
    };
    const problem_type = try requiredString(object, "type");
    if (problem_type.len == 0) return error.MalformedResponse;
    const title = try requiredString(object, "title");
    if (!std.mem.eql(u8, title, "Conflict")) return error.MalformedResponse;
    const problem_status = try requiredJsonUnsigned(u16, object, "status");
    if (problem_status != 409) return error.MalformedResponse;
    if (object.get("detail")) |detail| switch (detail) {
        .string => {},
        else => return error.MalformedResponse,
    };
    if (object.get("instance")) |instance| switch (instance) {
        .string => {},
        else => return error.MalformedResponse,
    };
    const results = try requiredArray(object, "tbResults");
    if (results.items.len == 0 or results.items.len > chain_items.len) {
        return error.MalformedResponse;
    }

    var seen = [_]bool{false} ** settlement.max_transfers_per_fill;
    for (results.items) |entry_value| {
        const entry = switch (entry_value) {
            .object => |result| result,
            else => return error.MalformedResponse,
        };
        if (entry.count() != 2) return error.MalformedResponse;
        const index = try requiredJsonUnsigned(usize, entry, "index");
        _ = try requiredJsonUnsigned(u32, entry, "result");
        if (index >= chain_items.len or seen[index]) return error.MalformedResponse;
        seen[index] = true;
    }
    return .{
        .item_error_count = @intCast(results.items.len),
        .truncated = false,
    };
}

fn createPayloadMatches(
    value: std.json.Value,
    chain: *const settlement.TransferChain,
) DecodeError!bool {
    const expected = try validatedChainItems(chain);
    const array = switch (value) {
        .array => |result| result,
        else => return false,
    };
    if (array.items.len != expected.len) return false;
    for (array.items, expected) |item, transfer| {
        if (!transferValueMatches(item, transfer)) return false;
    }
    return true;
}

fn lookupPayloadMatches(
    value: std.json.Value,
    chain: *const settlement.TransferChain,
) DecodeError!bool {
    const expected = try validatedChainItems(chain);
    const array = switch (value) {
        .array => |result| result,
        else => return false,
    };
    if (array.items.len != expected.len) return false;
    for (array.items, expected) |item, transfer| {
        const id = parseUnsigned(u128, item) catch return false;
        if (id != transfer.id) return false;
    }
    return true;
}

fn transferValueMatches(value: std.json.Value, expected: settlement.Transfer) bool {
    const object = switch (value) {
        .object => |result| result,
        else => return false,
    };
    if (object.count() != 13) return false;
    return unsignedFieldEquals(u128, object, "id", expected.id) and
        unsignedFieldEquals(u128, object, "debit_account_id", expected.debit_account_id) and
        unsignedFieldEquals(u128, object, "credit_account_id", expected.credit_account_id) and
        unsignedFieldEquals(u128, object, "amount", expected.amount) and
        unsignedFieldEquals(u128, object, "pending_id", expected.pending_id) and
        unsignedFieldEquals(u128, object, "user_data_128", expected.user_data_128) and
        unsignedFieldEquals(u64, object, "user_data_64", expected.user_data_64) and
        unsignedFieldEquals(u32, object, "user_data_32", expected.user_data_32) and
        unsignedFieldEquals(u32, object, "timeout", expected.timeout) and
        unsignedFieldEquals(u32, object, "ledger", expected.ledger) and
        unsignedFieldEquals(u16, object, "code", expected.code) and
        unsignedFieldEquals(u16, object, "flags", expected.flags) and
        unsignedFieldEquals(u64, object, "timestamp", expected.timestamp);
}

fn decodeLookupTransfer(
    value: std.json.Value,
    chain: *const settlement.TransferChain,
) DecodeError!settlement.Transfer {
    const object = switch (value) {
        .object => |result| result,
        else => return error.MalformedResponse,
    };
    if (object.count() != 13) return error.MalformedResponse;

    const id = try requiredUnsigned(u128, object, "id");
    const expected_amount = expectedAmountForId(chain, id);
    return .{
        .id = id,
        .debit_account_id = try requiredUnsigned(u128, object, "debit_account_id"),
        .credit_account_id = try requiredUnsigned(u128, object, "credit_account_id"),
        .amount = try requiredResponseAmount(object, "amount", expected_amount),
        .pending_id = try requiredUnsigned(u128, object, "pending_id"),
        .user_data_128 = try requiredUnsigned(u128, object, "user_data_128"),
        .user_data_64 = try requiredUnsigned(u64, object, "user_data_64"),
        .user_data_32 = try requiredUnsigned(u32, object, "user_data_32"),
        .timeout = try requiredUnsigned(u32, object, "timeout"),
        .ledger = try requiredUnsigned(u32, object, "ledger"),
        .code = try requiredUnsigned(u16, object, "code"),
        .flags = try requiredUnsigned(u16, object, "flags"),
        .timestamp = try requiredUnsigned(u64, object, "timestamp"),
    };
}

fn expectedAmountForId(
    chain: *const settlement.TransferChain,
    id: u128,
) ?u128 {
    const items = validatedChainItems(chain) catch return null;
    for (items) |transfer| if (transfer.id == id) return transfer.amount;
    return null;
}

fn requiredResponseAmount(
    object: std.json.ObjectMap,
    key: []const u8,
    expected: ?u128,
) DecodeError!u128 {
    const value = object.get(key) orelse return error.MalformedResponse;
    return switch (value) {
        .number_string => try parseDigits(u128, value.number_string, 10),
        .string => |text| try parseResponseAmountString(text, expected),
        else => error.MalformedResponse,
    };
}

fn parseResponseAmountString(text: []const u8, expected: ?u128) DecodeError!u128 {
    const hexadecimal = parseDigits(u128, text, 16) catch return error.MalformedResponse;
    const decimal = parseDigits(u128, text, 10) catch null;
    if (expected) |amount| {
        const hex_matches = hexadecimal == amount;
        const decimal_matches = if (decimal) |candidate| candidate == amount else false;
        if (hex_matches and !decimal_matches) return hexadecimal;
        if (decimal_matches and !hex_matches) return decimal.?;
        if (hex_matches and decimal_matches) return amount;
    }
    // Deployed Parix currently serializes Transfer.amount as unprefixed hex.
    // Choosing hex when neither representation matches preserves a mismatch
    // for the dispatcher instead of accidentally blessing the record.
    return hexadecimal;
}

fn validatedChainItems(
    chain: *const settlement.TransferChain,
) DecodeError![]const settlement.Transfer {
    if (chain.count == 0 or chain.count > settlement.max_transfers_per_fill) {
        return error.MalformedResponse;
    }
    return chain.storage[0..@as(usize, chain.count)];
}

fn requiredString(
    object: std.json.ObjectMap,
    key: []const u8,
) DecodeError![]const u8 {
    const value = object.get(key) orelse return error.MalformedResponse;
    return switch (value) {
        .string => |result| result,
        else => error.MalformedResponse,
    };
}

fn requiredBool(
    object: std.json.ObjectMap,
    key: []const u8,
) DecodeError!bool {
    const value = object.get(key) orelse return error.MalformedResponse;
    return switch (value) {
        .bool => |result| result,
        else => error.MalformedResponse,
    };
}

fn requiredArray(
    object: std.json.ObjectMap,
    key: []const u8,
) DecodeError!std.json.Array {
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

fn unsignedFieldEquals(
    comptime T: type,
    object: std.json.ObjectMap,
    key: []const u8,
    expected: T,
) bool {
    const actual = requiredUnsigned(T, object, key) catch return false;
    return actual == expected;
}

fn parseUnsigned(comptime T: type, value: std.json.Value) DecodeError!T {
    const text = switch (value) {
        .number_string => |result| result,
        .string => |result| result,
        else => return error.MalformedResponse,
    };
    return parseDigits(T, text, 10);
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

const test_database_id = "0e46f90e-c38b-487a-b9f9-630f7839cc3f";

fn testDestination() outbox.DestinationId {
    return parseDatabaseDestination(test_database_id) catch unreachable;
}

fn testRequest(operation: parix.Operation) parix.RequestContext {
    return .{
        .destination_id = testDestination(),
        .operation = operation,
    };
}

fn testConfig(auth: Auth) Config {
    return .{
        .api_origin = "http://127.0.0.1:8080",
        .database_id = test_database_id,
        .expected_destination_id = testDestination(),
        .auth = auth,
        .deadline = .fromMilliseconds(50),
        .max_response_bytes = 8 * 1024,
        .allow_http_in_tests = true,
    };
}

fn testChain() settlement.TransferChain {
    var chain: settlement.TransferChain = .{
        .group_id = 10,
        .storage = [_]settlement.Transfer{std.mem.zeroes(settlement.Transfer)} **
            settlement.max_transfers_per_fill,
        .count = 2,
    };
    chain.storage[0] = .{
        .id = 11,
        .debit_account_id = 21,
        .credit_account_id = 31,
        .amount = 1000,
        .pending_id = 0,
        .user_data_128 = 900,
        .user_data_64 = 915_000,
        .user_data_32 = 1,
        .timeout = 0,
        .ledger = 840,
        .code = 300,
        .flags = 1,
        .timestamp = 0,
    };
    chain.storage[1] = .{
        .id = 12,
        .debit_account_id = 41,
        .credit_account_id = 51,
        .amount = 2000,
        .pending_id = 0,
        .user_data_128 = 901,
        .user_data_64 = 916_000,
        .user_data_32 = 2,
        .timeout = 0,
        .ledger = 840,
        .code = 301,
        .flags = 0,
        .timestamp = 0,
    };
    return chain;
}

const create_success_json =
    \\{
    \\  "clusterId":"0",
    \\  "databaseId":"0e46f90e-c38b-487a-b9f9-630f7839cc3f",
    \\  "databaseName":"exchange-core",
    \\  "docsUrl":"https://example.test/docs",
    \\  "message":"Create transfers succeeded",
    \\  "mode":"live",
    \\  "operation":"create_transfers",
    \\  "operationLabel":"Create transfers",
    \\  "persisted":true,
    \\  "provider":"gcp",
    \\  "replicaCount":1,
    \\  "requestPayload":[
    \\    {"id":"11","debit_account_id":"21","credit_account_id":"31","amount":"1000","pending_id":"0","user_data_128":"900","user_data_64":"915000","user_data_32":1,"timeout":0,"ledger":840,"code":300,"flags":1,"timestamp":"0"},
    \\    {"id":"12","debit_account_id":"41","credit_account_id":"51","amount":"2000","pending_id":"0","user_data_128":"901","user_data_64":"916000","user_data_32":2,"timeout":0,"ledger":840,"code":301,"flags":0,"timestamp":"0"}
    \\  ],
    \\  "responsePayload":[],
    \\  "targetHost":"tb.internal:3000"
    \\}
;

const lookup_success_json =
    \\{
    \\  "clusterId":"0",
    \\  "databaseId":"0e46f90e-c38b-487a-b9f9-630f7839cc3f",
    \\  "databaseName":"exchange-core",
    \\  "docsUrl":"https://example.test/docs",
    \\  "message":"Lookup transfers succeeded",
    \\  "mode":"live",
    \\  "operation":"lookup_transfers",
    \\  "operationLabel":"Lookup transfers",
    \\  "persisted":false,
    \\  "provider":"gcp",
    \\  "replicaCount":1,
    \\  "requestPayload":["11","12"],
    \\  "responsePayload":[
    \\    {"id":"11","debit_account_id":"21","credit_account_id":"31","amount":"3e8","pending_id":"0","user_data_128":"900","user_data_64":"915000","user_data_32":1,"timeout":0,"ledger":840,"code":300,"flags":1,"timestamp":99},
    \\    {"id":"12","debit_account_id":"41","credit_account_id":"51","amount":"7d0","pending_id":"0","user_data_128":"901","user_data_64":"916000","user_data_32":2,"timeout":0,"ledger":840,"code":301,"flags":0,"timestamp":100}
    \\  ],
    \\  "targetHost":"tb.internal:3000"
    \\}
;

const loopback_request_body = "[\"101\"]";
const loopback_response_body =
    \\{"clusterId":"0","databaseId":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","databaseName":"exchange-core","docsUrl":"https://example.test/docs","message":"Lookup accounts succeeded","mode":"live","operation":"lookup_accounts","operationLabel":"Lookup accounts","persisted":false,"provider":"gcp","replicaCount":1,"requestPayload":["101"],"responsePayload":[],"targetHost":"tb.internal:3000"}
;

const LoopbackExpectation = struct {
    auth_name: []const u8,
    auth_value: []const u8,
};

fn serveLoopbackRequest(
    io: std.Io,
    listener: *std.Io.net.Server,
    expected: LoopbackExpectation,
) !void {
    const stream = try listener.accept(io);
    defer stream.close(io);

    var network_read_buffer: [16 * 1024]u8 = undefined;
    var network_write_buffer: [16 * 1024]u8 = undefined;
    var network_reader = stream.reader(io, &network_read_buffer);
    var network_writer = stream.writer(io, &network_write_buffer);
    var server = std.http.Server.init(&network_reader.interface, &network_writer.interface);
    var request = try server.receiveHead();

    try std.testing.expectEqual(std.http.Method.POST, request.head.method);
    try std.testing.expectEqualStrings(
        "/api/v1/databases/" ++ test_database_id ++ "/tb/lookup_accounts",
        request.head.target,
    );
    try std.testing.expectEqualStrings("application/json", request.head.content_type.?);
    try std.testing.expectEqual(@as(u64, loopback_request_body.len), request.head.content_length.?);

    var accept_count: usize = 0;
    var encoding_count: usize = 0;
    var expected_auth_count: usize = 0;
    var other_auth_count: usize = 0;
    var headers = request.iterateHeaders();
    while (headers.next()) |header| {
        if (std.ascii.eqlIgnoreCase(header.name, "accept")) {
            accept_count += 1;
            try std.testing.expectEqualStrings("application/json", header.value);
        } else if (std.ascii.eqlIgnoreCase(header.name, "accept-encoding")) {
            encoding_count += 1;
            try std.testing.expectEqualStrings("identity", header.value);
        } else if (std.ascii.eqlIgnoreCase(header.name, expected.auth_name)) {
            expected_auth_count += 1;
            try std.testing.expectEqualStrings(expected.auth_value, header.value);
        } else if (std.ascii.eqlIgnoreCase(header.name, "x-api-key") or
            std.ascii.eqlIgnoreCase(header.name, "authorization"))
        {
            other_auth_count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 1), accept_count);
    try std.testing.expectEqual(@as(usize, 1), encoding_count);
    try std.testing.expectEqual(@as(usize, 1), expected_auth_count);
    try std.testing.expectEqual(@as(usize, 0), other_auth_count);

    var transfer_buffer: [1024]u8 = undefined;
    const body_reader = try request.readerExpectContinue(&transfer_buffer);
    var body: [loopback_request_body.len]u8 = undefined;
    try body_reader.readSliceAll(&body);
    try std.testing.expectEqualStrings(loopback_request_body, &body);

    const response_headers = [_]std.http.Header{
        .{ .name = "content-type", .value = "application/json" },
    };
    try request.respond(loopback_response_body, .{
        .keep_alive = false,
        .extra_headers = &response_headers,
    });
}

fn runLoopbackRequestCase(auth: Auth, expected: LoopbackExpectation) !void {
    const io = std.testing.io;
    const listen_address: std.Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var listener = try listen_address.listen(io, .{});
    defer listener.deinit(io);

    var fixture = try io.concurrent(serveLoopbackRequest, .{ io, &listener, expected });
    defer fixture.cancel(io) catch {};

    var origin_storage: [128]u8 = undefined;
    const origin = try std.fmt.bufPrint(
        &origin_storage,
        "http://127.0.0.1:{d}",
        .{listener.socket.address.getPort()},
    );
    var config = testConfig(auth);
    config.api_origin = origin;
    config.deadline = .fromSeconds(2);
    var client = try GatewayClient.init(std.testing.allocator, io, config);
    defer client.deinit();

    const outcome = client.request(.lookup_accounts, loopback_request_body);
    try fixture.await(io);
    switch (outcome) {
        .failure => |certainty| {
            std.debug.print("unexpected loopback failure certainty={t}\n", .{certainty});
            return error.ExpectedLoopbackResponse;
        },
        .response => |response| {
            try std.testing.expectEqual(@as(u16, 200), response.status);
            try std.testing.expectEqualStrings(loopback_response_body, response.body);
        },
    }
}

test "config validates origin database destination deadline and response cap" {
    var client = try GatewayClient.init(
        std.testing.allocator,
        std.testing.io,
        testConfig(.{ .api_key = "px_test" }),
    );
    defer client.deinit();

    try std.testing.expectEqual(testDestination(), client.destinationId());
    try std.testing.expectEqualStrings("http://127.0.0.1:8080", client.api_origin);
    try std.testing.expectEqualStrings(test_database_id, client.database_id);
    try std.testing.expectEqualStrings("px_test", client.auth_header);

    var trailing = testConfig(.{ .api_key = "px_test" });
    trailing.api_origin = "http://127.0.0.1:8080/";
    var trailing_client = try GatewayClient.init(std.testing.allocator, std.testing.io, trailing);
    defer trailing_client.deinit();
    try std.testing.expectEqualStrings("http://127.0.0.1:8080", trailing_client.api_origin);

    var invalid = testConfig(.{ .api_key = "px_test" });
    invalid.api_origin = "ftp://example.test";
    try std.testing.expectError(
        error.InsecureApiOrigin,
        GatewayClient.init(std.testing.allocator, std.testing.io, invalid),
    );
    invalid = testConfig(.{ .api_key = "px_test" });
    invalid.allow_http_in_tests = false;
    try std.testing.expectError(
        error.InsecureApiOrigin,
        GatewayClient.init(std.testing.allocator, std.testing.io, invalid),
    );
    invalid = testConfig(.{ .api_key = "px_test" });
    invalid.api_origin = "https://user@example.test";
    try std.testing.expectError(
        error.ApiOriginHasUserinfo,
        GatewayClient.init(std.testing.allocator, std.testing.io, invalid),
    );
    invalid = testConfig(.{ .api_key = "px_test" });
    invalid.api_origin = "https://example.test/base";
    try std.testing.expectError(
        error.ApiOriginHasPath,
        GatewayClient.init(std.testing.allocator, std.testing.io, invalid),
    );
    invalid = testConfig(.{ .api_key = "px_test" });
    invalid.api_origin = "https://example.test?tenant=x";
    try std.testing.expectError(
        error.ApiOriginHasQuery,
        GatewayClient.init(std.testing.allocator, std.testing.io, invalid),
    );
    invalid = testConfig(.{ .api_key = "px_test" });
    invalid.api_origin = "https://example.test#fragment";
    try std.testing.expectError(
        error.ApiOriginHasFragment,
        GatewayClient.init(std.testing.allocator, std.testing.io, invalid),
    );
    invalid = testConfig(.{ .api_key = "px_test" });
    invalid.database_id = "not-a-uuid";
    try std.testing.expectError(
        error.InvalidDatabaseId,
        GatewayClient.init(std.testing.allocator, std.testing.io, invalid),
    );
    invalid = testConfig(.{ .api_key = "px_test" });
    invalid.database_id = "0E46F90E-C38B-487A-B9F9-630F7839CC3F";
    try std.testing.expectError(
        error.InvalidDatabaseId,
        GatewayClient.init(std.testing.allocator, std.testing.io, invalid),
    );
    invalid = testConfig(.{ .api_key = "px_test" });
    invalid.expected_destination_id = @enumFromInt(1);
    try std.testing.expectError(
        error.DestinationIdMismatch,
        GatewayClient.init(std.testing.allocator, std.testing.io, invalid),
    );
    invalid = testConfig(.{ .api_key = "px_test" });
    invalid.deadline = .zero;
    try std.testing.expectError(
        error.InvalidDeadline,
        GatewayClient.init(std.testing.allocator, std.testing.io, invalid),
    );
    invalid = testConfig(.{ .api_key = "px_test" });
    invalid.deadline = std.Io.Duration.max;
    try std.testing.expectError(
        error.InvalidDeadline,
        GatewayClient.init(std.testing.allocator, std.testing.io, invalid),
    );
    invalid = testConfig(.{ .api_key = "px_test" });
    invalid.max_response_bytes = 0;
    try std.testing.expectError(
        error.InvalidResponseLimit,
        GatewayClient.init(std.testing.allocator, std.testing.io, invalid),
    );
}

test "auth is copied and rejects empty or unsafe header values" {
    var source = [_]u8{ 'p', 'x', '_', 't', 'e', 's', 't' };
    var client = try GatewayClient.init(
        std.testing.allocator,
        std.testing.io,
        testConfig(.{ .api_key = &source }),
    );
    defer client.deinit();
    source[0] = 'X';
    try std.testing.expectEqualStrings("px_test", client.auth_header);

    var bearer = try GatewayClient.init(
        std.testing.allocator,
        std.testing.io,
        testConfig(.{ .bearer = "token" }),
    );
    defer bearer.deinit();
    try std.testing.expectEqualStrings("Bearer token", bearer.auth_header);

    try std.testing.expectError(
        error.AuthEmpty,
        GatewayClient.init(
            std.testing.allocator,
            std.testing.io,
            testConfig(.{ .api_key = "" }),
        ),
    );
    try std.testing.expectError(
        error.InvalidAuth,
        GatewayClient.init(
            std.testing.allocator,
            std.testing.io,
            testConfig(.{ .bearer = "token\r\nforged: value" }),
        ),
    );
}

test "loopback request sends exact Parix path headers auth and body" {
    try runLoopbackRequestCase(.{ .api_key = "fake_loopback_key" }, .{
        .auth_name = "x-api-key",
        .auth_value = "fake_loopback_key",
    });
    try runLoopbackRequestCase(.{ .bearer = "fake_loopback_token" }, .{
        .auth_name = "authorization",
        .auth_value = "Bearer fake_loopback_token",
    });
}

test "submit success and conflict decode through canonical classifier" {
    const chain = testChain();
    const request = testRequest(.create_transfers);
    const success = decodeSubmitResponse(
        std.testing.allocator,
        test_database_id,
        request,
        &chain,
        200,
        create_success_json,
    );
    switch (parix.classifySubmit(request, success)) {
        .success => |receipt| {
            try std.testing.expectEqual(testDestination(), receipt.destination_id);
            try std.testing.expect(receipt.persisted);
        },
        else => return error.ExpectedSubmitSuccess,
    }

    const conflict = decodeSubmitResponse(
        std.testing.allocator,
        test_database_id,
        request,
        &chain,
        409,
        \\{"type":"about:blank","title":"Conflict","status":409,"detail":"duplicate","tbResults":[{"index":0,"result":1},{"index":1,"result":2}]}
        ,
    );
    switch (parix.classifySubmit(request, conflict)) {
        .conflict => |receipt| try std.testing.expectEqual(@as(u32, 2), receipt.item_error_count),
        else => return error.ExpectedConflict,
    }
}

test "submit malformed duplicate keys and invalid conflicts remain ambiguous" {
    const chain = testChain();
    const request = testRequest(.create_transfers);
    const duplicate = decodeSubmitResponse(
        std.testing.allocator,
        test_database_id,
        request,
        &chain,
        200,
        \\{"databaseId":"a","databaseId":"b"}
        ,
    );
    try std.testing.expectEqual(
        dispatcher.SubmitDisposition.ambiguous,
        parix.classifySubmit(request, duplicate),
    );

    const empty_conflict = decodeSubmitResponse(
        std.testing.allocator,
        test_database_id,
        request,
        &chain,
        409,
        \\{"type":"about:blank","title":"Conflict","status":409,"tbResults":[]}
        ,
    );
    try std.testing.expectEqual(
        dispatcher.SubmitDisposition.ambiguous,
        parix.classifySubmit(request, empty_conflict),
    );

    const duplicate_index = decodeSubmitResponse(
        std.testing.allocator,
        test_database_id,
        request,
        &chain,
        409,
        \\{"type":"about:blank","title":"Conflict","status":409,"tbResults":[{"index":0,"result":1},{"index":0,"result":2}]}
        ,
    );
    try std.testing.expectEqual(
        dispatcher.SubmitDisposition.ambiguous,
        parix.classifySubmit(request, duplicate_index),
    );

    const mismatched_status = decodeSubmitResponse(
        std.testing.allocator,
        test_database_id,
        request,
        &chain,
        409,
        \\{"type":"about:blank","title":"Conflict","status":400,"tbResults":[{"index":0,"result":1}]}
        ,
    );
    try std.testing.expectEqual(
        dispatcher.SubmitDisposition.ambiguous,
        parix.classifySubmit(request, mismatched_status),
    );
}

test "lookup decodes every transfer field and deployed hexadecimal amounts" {
    const chain = testChain();
    const request = testRequest(.lookup_transfers);
    const outcome = decodeLookupResponse(
        std.testing.allocator,
        test_database_id,
        request,
        &chain,
        200,
        lookup_success_json,
    );
    switch (parix.classifyLookup(request, outcome)) {
        .transfers => |receipt| {
            const items = try receipt.batch.items();
            try std.testing.expectEqual(@as(usize, 2), items.len);
            try std.testing.expectEqual(@as(u128, 1000), items[0].amount);
            try std.testing.expectEqual(@as(u128, 2000), items[1].amount);
            try std.testing.expectEqual(@as(u128, 21), items[0].debit_account_id);
            try std.testing.expectEqual(@as(u128, 31), items[0].credit_account_id);
            try std.testing.expectEqual(@as(u128, 0), items[0].pending_id);
            try std.testing.expectEqual(@as(u128, 900), items[0].user_data_128);
            try std.testing.expectEqual(@as(u64, 915_000), items[0].user_data_64);
            try std.testing.expectEqual(@as(u32, 1), items[0].user_data_32);
            try std.testing.expectEqual(@as(u32, 0), items[0].timeout);
            try std.testing.expectEqual(@as(u32, 840), items[0].ledger);
            try std.testing.expectEqual(@as(u16, 300), items[0].code);
            try std.testing.expectEqual(@as(u16, 1), items[0].flags);
            try std.testing.expectEqual(@as(u64, 99), items[0].timestamp);
        },
        else => return error.ExpectedLookupTransfers,
    }
}

test "lookup amount decoder disambiguates future decimal strings against expected evidence" {
    try std.testing.expectEqual(
        @as(u128, 1000),
        try parseResponseAmountString("3e8", 1000),
    );
    try std.testing.expectEqual(
        @as(u128, 1000),
        try parseResponseAmountString("1000", 1000),
    );
    try std.testing.expectEqual(
        @as(u128, 0x64),
        try parseResponseAmountString("64", null),
    );
}

test "lookup evidence mismatch malformed fields and oversized batches are retryable" {
    const chain = testChain();
    const request = testRequest(.lookup_transfers);
    const mismatched_body = try std.mem.replaceOwned(
        u8,
        std.testing.allocator,
        lookup_success_json,
        "\"requestPayload\":[\"11\",\"12\"]",
        "\"requestPayload\":[\"11\",\"99\"]",
    );
    defer std.testing.allocator.free(mismatched_body);
    const wrong_request = decodeLookupResponse(
        std.testing.allocator,
        test_database_id,
        request,
        &chain,
        200,
        mismatched_body,
    );
    try std.testing.expectEqual(
        dispatcher.LookupDisposition.retryable,
        parix.classifyLookup(request, wrong_request),
    );

    const malformed_field = decodeLookupResponse(
        std.testing.allocator,
        test_database_id,
        request,
        &chain,
        200,
        \\{"clusterId":"0","databaseId":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","databaseName":"x","docsUrl":"x","message":"x","mode":"live","operation":"lookup_transfers","operationLabel":"x","persisted":false,"provider":"gcp","replicaCount":1,"requestPayload":["11","12"],"responsePayload":[{"id":"11"}],"targetHost":"x"}
        ,
    );
    try std.testing.expectEqual(
        dispatcher.LookupDisposition.retryable,
        parix.classifyLookup(request, malformed_field),
    );

    const too_many =
        \\{"clusterId":"0","databaseId":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","databaseName":"x","docsUrl":"x","message":"x","mode":"live","operation":"lookup_transfers","operationLabel":"x","persisted":false,"provider":"gcp","replicaCount":1,"requestPayload":["11","12"],"responsePayload":[{},{},{},{},{}],"targetHost":"x"}
    ;
    const oversized = decodeLookupResponse(
        std.testing.allocator,
        test_database_id,
        request,
        &chain,
        200,
        too_many,
    );
    try std.testing.expectEqual(
        dispatcher.LookupDisposition.retryable,
        parix.classifyLookup(request, oversized),
    );
}

test "JSON bounds reject excessive nesting and scalar size" {
    var nested: [2 * (max_json_nesting + 1) + 1]u8 = undefined;
    @memset(nested[0 .. max_json_nesting + 1], '[');
    nested[max_json_nesting + 1] = '0';
    @memset(nested[max_json_nesting + 2 ..], ']');
    try std.testing.expect(!jsonNestingWithinLimit(&nested, max_json_nesting));

    const chain = testChain();
    const request = testRequest(.create_transfers);
    var long_value: [max_json_scalar_bytes + 1]u8 = undefined;
    @memset(&long_value, 'a');
    var body_storage: [max_json_scalar_bytes + 128]u8 = undefined;
    const body = try std.fmt.bufPrint(
        &body_storage,
        "{{\"type\":\"about:blank\",\"title\":\"{s}\",\"status\":409,\"tbResults\":[{{\"index\":0,\"result\":1}}]}}",
        .{&long_value},
    );
    const outcome = decodeSubmitResponse(
        std.testing.allocator,
        test_database_id,
        request,
        &chain,
        409,
        body,
    );
    try std.testing.expectEqual(
        dispatcher.SubmitDisposition.ambiguous,
        parix.classifySubmit(request, outcome),
    );
}

test "canonical request writers fit the transport bound at worst case" {
    var chain: settlement.TransferChain = .{
        .group_id = std.math.maxInt(u128),
        .storage = [_]settlement.Transfer{std.mem.zeroes(settlement.Transfer)} **
            settlement.max_transfers_per_fill,
        .count = settlement.max_transfers_per_fill,
    };
    for (&chain.storage) |*transfer| transfer.* = .{
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

    var create_storage: [max_request_bytes]u8 = undefined;
    var create_writer = std.Io.Writer.fixed(&create_storage);
    try parix.writeCreateTransfers(&create_writer, &chain);
    try std.testing.expect(create_writer.buffered().len < max_request_bytes);

    var lookup_storage: [max_request_bytes]u8 = undefined;
    var lookup_writer = std.Io.Writer.fixed(&lookup_storage);
    try parix.writeLookupTransfers(&lookup_writer, &chain);
    try std.testing.expect(lookup_writer.buffered().len < max_request_bytes);
}

test "request bound and timeout stage mapping are conservative without network IO" {
    var client = try GatewayClient.init(
        std.testing.allocator,
        std.testing.io,
        testConfig(.{ .api_key = "px_test" }),
    );
    defer client.deinit();

    var too_large: [max_request_bytes + 1]u8 = undefined;
    @memset(&too_large, 'x');
    switch (client.request(.create_accounts, &too_large)) {
        .failure => |certainty| try std.testing.expectEqual(
            FailureCertainty.definitely_not_sent,
            certainty,
        ),
        .response => return error.ExpectedBoundedFailure,
    }
    try std.testing.expectEqual(
        FailureCertainty.definitely_not_sent,
        failureCertainty(.preparing),
    );
    try std.testing.expectEqual(
        FailureCertainty.indeterminate,
        failureCertainty(.write_started),
    );
    try std.testing.expectEqual(
        FailureCertainty.indeterminate,
        failureCertainty(.response_complete),
    );
}

test "plugin vtable rejects the wrong destination before IO" {
    var client = try GatewayClient.init(
        std.testing.allocator,
        std.testing.io,
        testConfig(.{ .api_key = "px_test" }),
    );
    defer client.deinit();

    const transport = client.asTransport();
    const chain = testChain();
    const wrong_destination: outbox.DestinationId = @enumFromInt(1);
    try std.testing.expectEqual(
        dispatcher.SubmitDisposition.destination_mismatch,
        transport.vtable.submit(transport.context, wrong_destination, &chain),
    );
    try std.testing.expectEqual(
        dispatcher.LookupDisposition.destination_mismatch,
        transport.vtable.lookup(
            transport.context,
            wrong_destination,
            &chain,
            .ambiguous_submission,
        ),
    );
}
