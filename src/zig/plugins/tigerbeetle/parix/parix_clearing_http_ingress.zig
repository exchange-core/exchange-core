//! Parix host integration for the opt-in TigerBeetle plugin.
const std = @import("std");
const provider = @import("parix_clearing_provider_adapter.zig");
const saga = @import("parix_clearing_saga.zig");

const log = std.log.scoped(.parix_clearing_http_ingress);

pub const max_sessions: usize = 32;
pub const max_head_bytes: usize = 8 * 1024;
pub const max_body_bytes: usize = 16 * 1024;
pub const max_headers: usize = 32;
pub const max_header_name_bytes: usize = 64;
pub const max_header_value_bytes: usize = 256;
pub const absolute_deadline: std.Io.Duration = .fromSeconds(5);

pub const timestamp_header_name = provider.timestamp_header_name;
pub const signature_header_name = provider.signature_header_name;

const inserted_body = "{\"status\":\"accepted\",\"duplicate\":false}\n";
const existing_body = "{\"status\":\"accepted\",\"duplicate\":true}\n";
const malformed_body = "{\"error\":\"malformed_request\"}\n";
const authentication_body = "{\"error\":\"authentication_failed\"}\n";
const conflict_body = "{\"error\":\"conflict\"}\n";
const unavailable_body = "{\"error\":\"unavailable\"}\n";

pub const Health = enum(u8) {
    cold,
    ready,
    failed,
    stopped,
};

/// Closed admission result. Runtime-specific errors must be classified before
/// crossing this boundary, so the HTTP host cannot accidentally expose them.
pub const AdmissionOutcome = union(enum) {
    durable: saga.AcceptResult,
    invalid,
    conflict,
    unavailable,
};

/// Type-erased durable clearing admission. The implementation must return
/// `.durable` only after the CSG1 commit has completed.
pub const AdmissionPort = struct {
    context: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        admit: *const fn (*anyopaque, provider.VerifiedEvent) AdmissionOutcome,
        is_ready: *const fn (*anyopaque) bool,
    };

    pub fn admit(self: AdmissionPort, event: provider.VerifiedEvent) AdmissionOutcome {
        return self.vtable.admit(self.context, event);
    }

    pub fn isReady(self: AdmissionPort) bool {
        return self.vtable.is_ready(self.context);
    }
};

pub const Response = struct {
    status: std.http.Status,
    body: []const u8,

    pub fn isSuccess(self: Response) bool {
        return self.status.class() == .success;
    }
};

pub const Prepared = union(enum) {
    verified: provider.VerifiedEvent,
    response: Response,
};

/// Allocation-free orchestration boundary. `prepare` authenticates and
/// normalizes before `admit`; only a returned durable result can create 2xx.
pub const Handler = struct {
    adapter: provider.Adapter,
    admission: AdmissionPort,

    pub fn prepare(self: Handler, request: provider.RequestView) Prepared {
        const event = self.adapter.verify(request) catch |err| return .{ .response = switch (err) {
            error.InvalidRequest, error.MalformedEvent => malformedResponse(),
            error.AuthenticationFailed, error.ReplayRejected => authenticationResponse(),
            error.OutOfMemory => unavailableResponse(),
        } };
        return .{ .verified = event };
    }

    pub fn admit(self: Handler, event: provider.VerifiedEvent) Response {
        if (!self.admission.isReady()) return unavailableResponse();
        return responseFromAdmission(self.admission.admit(event));
    }

    pub fn handle(self: Handler, request: provider.RequestView) Response {
        return switch (self.prepare(request)) {
            .verified => |event| self.admit(event),
            .response => |response| response,
        };
    }
};

fn responseFromAdmission(outcome: AdmissionOutcome) Response {
    return switch (outcome) {
        .durable => |result| switch (result.disposition) {
            .inserted => .{ .status = .accepted, .body = inserted_body },
            .existing => .{ .status = .ok, .body = existing_body },
        },
        .invalid => malformedResponse(),
        .conflict => .{ .status = .conflict, .body = conflict_body },
        .unavailable => unavailableResponse(),
    };
}

fn malformedResponse() Response {
    return .{ .status = .bad_request, .body = malformed_body };
}

fn authenticationResponse() Response {
    return .{ .status = .unauthorized, .body = authentication_body };
}

fn unavailableResponse() Response {
    return .{ .status = .service_unavailable, .body = unavailable_body };
}

pub const Clock = union(enum) {
    system,
    fixed: i64,
    callback: Callback,

    pub const Callback = struct {
        context: *const anyopaque,
        now_unix_seconds: *const fn (*const anyopaque) i64,
    };

    pub fn nowUnixSeconds(self: Clock, io: std.Io) i64 {
        return switch (self) {
            .system => std.Io.Clock.real.now(io).toSeconds(),
            .fixed => |seconds| seconds,
            .callback => |callback| callback.now_unix_seconds(callback.context),
        };
    }
};

pub const FailureReason = enum {
    listener_failed,
};

pub const FailureCallback = struct {
    context: *anyopaque,
    notify_fn: *const fn (*anyopaque, FailureReason) void,

    pub fn notify(self: FailureCallback, reason: FailureReason) void {
        self.notify_fn(self.context, reason);
    }
};

pub const Config = struct {
    handler: Handler,
    clock: Clock = .system,
    failure_callback: ?FailureCallback = null,
};

pub const ServeError = error{
    NonLoopbackListener,
    AdmissionNotReady,
    AlreadyStarted,
    ListenerFailed,
    Canceled,
};

/// One-shot loopback HTTP/1.1 host. Keep this value at a stable address from
/// `serve` until it returns because session tasks retain a pointer to it.
pub const Host = struct {
    io: std.Io,
    handler: Handler,
    clock: Clock,
    failure_callback: ?FailureCallback,
    health_state: std.atomic.Value(u8) = .init(@intFromEnum(Health.cold)),
    accepting: std.atomic.Value(bool) = .init(false),
    stop_event: std.Io.Event = .unset,
    gate_mutex: std.Io.Mutex = .init,
    drained: std.Io.Condition = .init,
    gate_open: bool = false,
    active_sessions: usize = 0,
    active_admissions: usize = 0,

    const Self = @This();

    pub fn init(io: std.Io, config: Config) Self {
        return .{
            .io = io,
            .handler = config.handler,
            .clock = config.clock,
            .failure_callback = config.failure_callback,
        };
    }

    pub fn health(self: *const Self) Health {
        return @enumFromInt(self.health_state.load(.acquire));
    }

    pub fn isAccepting(self: *const Self) bool {
        return self.health() == .ready and
            self.accepting.load(.acquire) and
            self.handler.admission.isReady();
    }

    /// Atomically fences future admission before waking the listener. Existing
    /// leases and sessions are not canceled and are drained by `serve`.
    pub fn requestStop(self: *Self) void {
        self.gate_mutex.lockUncancelable(self.io);
        self.gate_open = false;
        self.accepting.store(false, .release);
        switch (self.health()) {
            .cold, .ready => self.health_state.store(@intFromEnum(Health.stopped), .release),
            .failed, .stopped => {},
        }
        self.gate_mutex.unlock(self.io);
        self.stop_event.set(self.io);
    }

    pub fn serve(self: *Self, listener: *std.Io.net.Server) ServeError!void {
        if (!isLoopback(listener.socket.address)) return error.NonLoopbackListener;
        if (!self.handler.admission.isReady()) return error.AdmissionNotReady;
        try self.beginServing();

        var sessions: std.Io.Group = .init;
        var terminal: enum { stopped, failed, canceled } = .stopped;

        while (true) {
            const next = acceptUntilStopped(self, listener) catch {
                terminal = .canceled;
                self.requestStop();
                break;
            };
            switch (next) {
                .stream => |stream| {
                    if (!self.tryBeginSession()) {
                        stream.close(self.io);
                        continue;
                    }
                    sessions.concurrent(self.io, serveSession, .{ self, stream }) catch {
                        stream.close(self.io);
                        self.endSession();
                        log.warn("provider ingress session rejected: concurrency unavailable", .{});
                    };
                },
                .retry => continue,
                .stopped => break,
                .failed => {
                    if (self.stop_event.isSet()) break;
                    terminal = .failed;
                    self.publishFailed();
                    break;
                },
            }
        }

        // A cancelation request must never propagate into a clearing admission.
        const prior_protection = self.io.swapCancelProtection(.blocked);
        defer _ = self.io.swapCancelProtection(prior_protection);
        sessions.await(self.io) catch |err| switch (err) {
            error.Canceled => unreachable,
        };

        return switch (terminal) {
            .stopped => {},
            .failed => error.ListenerFailed,
            .canceled => error.Canceled,
        };
    }

    fn beginServing(self: *Self) ServeError!void {
        self.gate_mutex.lockUncancelable(self.io);
        defer self.gate_mutex.unlock(self.io);
        if (self.health() != .cold) return error.AlreadyStarted;
        self.gate_open = true;
        self.accepting.store(true, .release);
        self.health_state.store(@intFromEnum(Health.ready), .release);
    }

    fn publishFailed(self: *Self) void {
        var notify = false;
        self.gate_mutex.lockUncancelable(self.io);
        if (self.health() != .stopped and self.health() != .failed) {
            self.gate_open = false;
            self.accepting.store(false, .release);
            self.health_state.store(@intFromEnum(Health.failed), .release);
            notify = true;
        }
        self.gate_mutex.unlock(self.io);
        self.stop_event.set(self.io);
        if (notify) if (self.failure_callback) |callback| callback.notify(.listener_failed);
    }

    fn tryBeginSession(self: *Self) bool {
        if (!self.accepting.load(.acquire)) return false;
        self.gate_mutex.lockUncancelable(self.io);
        defer self.gate_mutex.unlock(self.io);
        if (!self.gate_open or self.active_sessions >= max_sessions) return false;
        self.active_sessions += 1;
        return true;
    }

    fn endSession(self: *Self) void {
        self.gate_mutex.lockUncancelable(self.io);
        std.debug.assert(self.active_sessions > 0);
        self.active_sessions -= 1;
        self.drained.broadcast(self.io);
        self.gate_mutex.unlock(self.io);
    }

    fn acquireAdmission(self: *Self) ?AdmissionLease {
        if (!self.accepting.load(.acquire)) return null;
        self.gate_mutex.lockUncancelable(self.io);
        defer self.gate_mutex.unlock(self.io);
        if (!self.gate_open) return null;
        self.active_admissions += 1;
        return .{ .host = self };
    }

    fn releaseAdmission(self: *Self) void {
        self.gate_mutex.lockUncancelable(self.io);
        std.debug.assert(self.active_admissions > 0);
        self.active_admissions -= 1;
        self.drained.broadcast(self.io);
        self.gate_mutex.unlock(self.io);
    }
};

const AdmissionLease = struct {
    host: *Host,

    fn release(self: AdmissionLease) void {
        self.host.releaseAdmission();
    }
};

const AcceptTaskResult = union(enum) {
    stream: std.Io.net.Stream,
    retry,
    failed,
    canceled,
};

const StopTaskResult = enum {
    stopped,
    canceled,
};

const AcceptRaceResult = union(enum) {
    accept: AcceptTaskResult,
    stop: StopTaskResult,
};

const AcceptRace = std.Io.Select(AcceptRaceResult);

const AcceptOutcome = union(enum) {
    stream: std.Io.net.Stream,
    retry,
    stopped,
    failed,
};

fn acceptUntilStopped(host: *Host, listener: *std.Io.net.Server) error{Canceled}!AcceptOutcome {
    var result_storage: [2]AcceptRaceResult = undefined;
    var race = AcceptRace.init(host.io, &result_storage);
    race.concurrent(.accept, performAccept, .{ host.io, listener }) catch return error.Canceled;
    race.concurrent(.stop, waitForStop, .{ host.io, &host.stop_event }) catch {
        cancelAcceptRace(&race, host.io);
        return error.Canceled;
    };
    const first = race.await() catch {
        cancelAcceptRace(&race, host.io);
        return error.Canceled;
    };
    // If stop and accept complete together, the losing result can already own
    // a stream. Drain rather than discard it so shutdown cannot leak or admit
    // that connection. If accept won, a concurrently observed fence still
    // makes `tryBeginSession` close the returned stream.
    cancelAcceptRace(&race, host.io);
    return switch (first) {
        .accept => |result| switch (result) {
            .stream => |stream| .{ .stream = stream },
            .retry => .retry,
            .failed => .failed,
            .canceled => if (host.stop_event.isSet()) .stopped else error.Canceled,
        },
        .stop => |result| switch (result) {
            .stopped => .stopped,
            .canceled => error.Canceled,
        },
    };
}

fn cancelAcceptRace(race: *AcceptRace, io: std.Io) void {
    while (race.cancel()) |remaining| disposeAcceptRaceResult(io, remaining);
}

fn disposeAcceptRaceResult(io: std.Io, result: AcceptRaceResult) void {
    switch (result) {
        .accept => |accept| switch (accept) {
            .stream => |stream| stream.close(io),
            .retry, .failed, .canceled => {},
        },
        .stop => {},
    }
}

fn performAccept(io: std.Io, listener: *std.Io.net.Server) AcceptTaskResult {
    const stream = listener.accept(io) catch |err| return switch (err) {
        error.ConnectionAborted => .retry,
        error.Canceled => .canceled,
        else => .failed,
    };
    return .{ .stream = stream };
}

fn waitForStop(io: std.Io, stop_event: *std.Io.Event) StopTaskResult {
    stop_event.wait(io) catch return .canceled;
    return .stopped;
}

const PreAdmissionTaskResult = union(enum) {
    verified: provider.VerifiedEvent,
    response: Response,
    close,
};

const DeadlineTaskResult = enum {
    expired,
    canceled,
};

const PreAdmissionRaceResult = union(enum) {
    prepare: PreAdmissionTaskResult,
    deadline: DeadlineTaskResult,
};

const PreAdmissionRace = std.Io.Select(PreAdmissionRaceResult);

const WriteTaskResult = enum {
    complete,
    failed,
};

const WriteRaceResult = union(enum) {
    write: WriteTaskResult,
    deadline: DeadlineTaskResult,
};

const WriteRace = std.Io.Select(WriteRaceResult);

fn serveSession(host: *Host, stream: std.Io.net.Stream) std.Io.Cancelable!void {
    defer host.endSession();
    defer stream.close(host.io);

    var read_buffer: [max_head_bytes]u8 = undefined;
    var write_buffer: [1024]u8 = undefined;
    var body_buffer: [max_body_bytes]u8 = undefined;
    var transfer_buffer: [4096]u8 = undefined;
    var stream_reader = stream.reader(host.io, &read_buffer);
    var stream_writer = stream.writer(host.io, &write_buffer);
    const reader = &stream_reader.interface;
    const writer = &stream_writer.interface;

    const prepared = runPreAdmissionWithin(
        host,
        reader,
        writer,
        &body_buffer,
        &transfer_buffer,
    );
    const response = switch (prepared) {
        .verified => |event| response: {
            const lease = host.acquireAdmission() orelse break :response unavailableResponse();
            defer lease.release();
            break :response admitUncancelable(host, event);
        },
        .response => |value| value,
        .close => return,
    };
    writeResponseWithin(host.io, writer, response);
}

fn admitUncancelable(host: *Host, event: provider.VerifiedEvent) Response {
    const prior_protection = host.io.swapCancelProtection(.blocked);
    defer _ = host.io.swapCancelProtection(prior_protection);
    return host.handler.admit(event);
}

fn runPreAdmissionWithin(
    host: *Host,
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    body_buffer: *[max_body_bytes]u8,
    transfer_buffer: *[4096]u8,
) PreAdmissionTaskResult {
    const deadline = (std.Io.Timeout{ .duration = .{
        .raw = absolute_deadline,
        .clock = .boot,
    } }).toDeadline(host.io);
    var result_storage: [2]PreAdmissionRaceResult = undefined;
    var race = PreAdmissionRace.init(host.io, &result_storage);
    race.concurrent(
        .prepare,
        prepareHttpRequest,
        .{ host, reader, writer, body_buffer, transfer_buffer },
    ) catch return .close;
    race.concurrent(.deadline, waitForDeadline, .{ deadline, host.io }) catch {
        race.cancelDiscard();
        return .close;
    };
    const first = race.await() catch {
        race.cancelDiscard();
        return .close;
    };
    race.cancelDiscard();
    return switch (first) {
        .prepare => |result| result,
        .deadline => .close,
    };
}

const HeaderValue = struct {
    bytes: [max_header_value_bytes]u8 = [_]u8{0} ** max_header_value_bytes,
    len: u16 = 0,

    fn set(self: *HeaderValue, value: []const u8) bool {
        if (value.len > self.bytes.len) return false;
        @memcpy(self.bytes[0..value.len], value);
        self.len = @intCast(value.len);
        return true;
    }

    fn slice(self: *const HeaderValue) []const u8 {
        return self.bytes[0..self.len];
    }
};

const RequestParts = struct {
    target: [provider.max_path_bytes]u8 = [_]u8{0} ** provider.max_path_bytes,
    target_len: u16 = 0,
    content_type: HeaderValue = .{},
    timestamp: HeaderValue = .{},
    signature: HeaderValue = .{},
    content_length: usize = 0,
};

fn prepareHttpRequest(
    host: *Host,
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    body_buffer: *[max_body_bytes]u8,
    transfer_buffer: *[4096]u8,
) PreAdmissionTaskResult {
    var server = std.http.Server.init(reader, writer);
    var request = server.receiveHead() catch return .{ .response = malformedResponse() };
    const parts = inspectRequest(&request) orelse return .{ .response = malformedResponse() };
    const body_reader = request.readerExpectNone(transfer_buffer);
    body_reader.readSliceAll(body_buffer[0..parts.content_length]) catch
        return .{ .response = malformedResponse() };

    const view: provider.RequestView = .{
        .method = @tagName(request.head.method),
        .target = parts.target[0..parts.target_len],
        .content_type = parts.content_type.slice(),
        .timestamp_header = parts.timestamp.slice(),
        .signature_header = parts.signature.slice(),
        .body = body_buffer[0..parts.content_length],
        .received_unix_seconds = host.clock.nowUnixSeconds(host.io),
    };
    return switch (host.handler.prepare(view)) {
        .verified => |event| .{ .verified = event },
        .response => |response| .{ .response = response },
    };
}

fn inspectRequest(request: *const std.http.Server.Request) ?RequestParts {
    if (request.head.version != .@"HTTP/1.1" or
        request.head.transfer_encoding != .none or
        request.head.transfer_compression != .identity or
        request.head.expect != null or
        request.head.target.len == 0 or
        request.head.target.len > provider.max_path_bytes)
    {
        return null;
    }
    const content_length_u64 = request.head.content_length orelse return null;
    if (content_length_u64 == 0 or content_length_u64 > max_body_bytes) return null;

    var result: RequestParts = .{};
    @memcpy(result.target[0..request.head.target.len], request.head.target);
    result.target_len = @intCast(request.head.target.len);
    result.content_length = @intCast(content_length_u64);

    var header_count: usize = 0;
    var content_length_count: usize = 0;
    var content_type_count: usize = 0;
    var timestamp_count: usize = 0;
    var signature_count: usize = 0;
    var headers = request.iterateHeaders();
    while (headers.next()) |header| {
        header_count += 1;
        if (header_count > max_headers or
            header.name.len == 0 or
            header.name.len > max_header_name_bytes or
            header.value.len > max_header_value_bytes)
        {
            return null;
        }
        if (std.ascii.eqlIgnoreCase(header.name, "content-length")) {
            content_length_count += 1;
        } else if (std.ascii.eqlIgnoreCase(header.name, "content-type")) {
            content_type_count += 1;
            if (!result.content_type.set(header.value)) return null;
        } else if (std.ascii.eqlIgnoreCase(header.name, timestamp_header_name)) {
            timestamp_count += 1;
            if (!result.timestamp.set(header.value)) return null;
        } else if (std.ascii.eqlIgnoreCase(header.name, signature_header_name)) {
            signature_count += 1;
            if (!result.signature.set(header.value)) return null;
        }
    }

    if (content_length_count != 1 or
        content_type_count != 1 or
        timestamp_count != 1 or
        signature_count != 1)
    {
        return null;
    }
    return result;
}

fn writeResponseWithin(io: std.Io, writer: *std.Io.Writer, response: Response) void {
    const deadline = (std.Io.Timeout{ .duration = .{
        .raw = absolute_deadline,
        .clock = .boot,
    } }).toDeadline(io);
    var result_storage: [2]WriteRaceResult = undefined;
    var race = WriteRace.init(io, &result_storage);
    race.concurrent(.write, performWriteResponse, .{ writer, response }) catch return;
    race.concurrent(.deadline, waitForDeadline, .{ deadline, io }) catch {
        race.cancelDiscard();
        return;
    };
    _ = race.await() catch {
        race.cancelDiscard();
        return;
    };
    race.cancelDiscard();
}

fn performWriteResponse(writer: *std.Io.Writer, response: Response) WriteTaskResult {
    const phrase = response.status.phrase() orelse return .failed;
    writer.print(
        "HTTP/1.1 {d} {s}\r\n" ++
            "content-type: application/json\r\n" ++
            "content-length: {d}\r\n" ++
            "cache-control: no-store\r\n" ++
            "connection: close\r\n\r\n{s}",
        .{ @intFromEnum(response.status), phrase, response.body.len, response.body },
    ) catch return .failed;
    writer.flush() catch return .failed;
    return .complete;
}

fn waitForDeadline(deadline: std.Io.Timeout, io: std.Io) DeadlineTaskResult {
    deadline.sleep(io) catch return .canceled;
    return .expired;
}

fn isLoopback(address: std.Io.net.IpAddress) bool {
    return switch (address) {
        .ip4 => |ip4| std.mem.eql(u8, &ip4.bytes, &std.Io.net.Ip4Address.loopback(ip4.port).bytes),
        .ip6 => |ip6| std.mem.eql(u8, &ip6.bytes, &std.Io.net.Ip6Address.loopback(ip6.port).bytes),
    };
}

const FakeAdapterMode = union(enum) {
    verified: provider.VerifiedEvent,
    invalid_request,
    authentication_failed,
    replay_rejected,
    malformed_event,
    out_of_memory,
};

const FakeAdapter = struct {
    mode: FakeAdapterMode,

    fn asAdapter(self: *const FakeAdapter) provider.Adapter {
        return .{ .context = self, .vtable = &vtable };
    }

    const vtable: provider.Adapter.VTable = .{ .verify = verify };

    fn verify(raw: *const anyopaque, request: provider.RequestView) provider.VerifyError!provider.VerifiedEvent {
        _ = request;
        const self: *const FakeAdapter = @ptrCast(@alignCast(raw));
        return switch (self.mode) {
            .verified => |event| event,
            .invalid_request => error.InvalidRequest,
            .authentication_failed => error.AuthenticationFailed,
            .replay_rejected => error.ReplayRejected,
            .malformed_event => error.MalformedEvent,
            .out_of_memory => error.OutOfMemory,
        };
    }
};

const FakeAdmission = struct {
    ready: bool = true,
    outcome: AdmissionOutcome,
    calls: usize = 0,

    fn asPort(self: *FakeAdmission) AdmissionPort {
        return .{ .context = self, .vtable = &vtable };
    }

    const vtable: AdmissionPort.VTable = .{
        .admit = admit,
        .is_ready = isReady,
    };

    fn admit(raw: *anyopaque, event: provider.VerifiedEvent) AdmissionOutcome {
        _ = event;
        const self: *FakeAdmission = @ptrCast(@alignCast(raw));
        self.calls += 1;
        return self.outcome;
    }

    fn isReady(raw: *anyopaque) bool {
        const self: *FakeAdmission = @ptrCast(@alignCast(raw));
        return self.ready;
    }
};

fn testAuthorization() saga.Authorization {
    return .{
        .event_id = 1,
        .payment_id = 2,
        .provider_id = 3,
        .revision = 1,
        .direction = .deposit,
        .uid = 4,
        .currency = 15,
        .amount = 100,
        .timeout_seconds = 30,
    };
}

fn testAcceptResult(disposition: saga.AcceptDisposition) saga.AcceptResult {
    const authorization = testAuthorization();
    return .{
        .disposition = disposition,
        .status = .{
            .authorization = authorization,
            .resolution = null,
            .state = .accepted,
        },
    };
}

fn testRequest() provider.RequestView {
    return .{
        .method = "POST",
        .target = "/v1/clearing/events",
        .content_type = "application/json",
        .timestamp_header = "1700000000",
        .signature_header = "00" ** 32,
        .body = "{}",
        .received_unix_seconds = 1_700_000_000,
    };
}

test "handler creates success only from durable admission" {
    var fake_adapter: FakeAdapter = .{
        .mode = .{ .verified = .{ .authorization = testAuthorization() } },
    };
    var fake_admission: FakeAdmission = .{
        .outcome = .{ .durable = testAcceptResult(.inserted) },
    };
    const handler: Handler = .{
        .adapter = fake_adapter.asAdapter(),
        .admission = fake_admission.asPort(),
    };

    const inserted = handler.handle(testRequest());
    try std.testing.expectEqual(std.http.Status.accepted, inserted.status);
    try std.testing.expectEqualStrings(inserted_body, inserted.body);
    try std.testing.expect(inserted.isSuccess());
    try std.testing.expectEqual(@as(usize, 1), fake_admission.calls);

    fake_admission.outcome = .{ .durable = testAcceptResult(.existing) };
    const existing = handler.handle(testRequest());
    try std.testing.expectEqual(std.http.Status.ok, existing.status);
    try std.testing.expectEqualStrings(existing_body, existing.body);
    try std.testing.expect(existing.isSuccess());
    try std.testing.expectEqual(@as(usize, 2), fake_admission.calls);

    fake_admission.outcome = .unavailable;
    const unavailable = handler.handle(testRequest());
    try std.testing.expectEqual(std.http.Status.service_unavailable, unavailable.status);
    try std.testing.expect(!unavailable.isSuccess());
    try std.testing.expectEqual(@as(usize, 3), fake_admission.calls);

    fake_admission.ready = false;
    const not_ready = handler.handle(testRequest());
    try std.testing.expectEqual(std.http.Status.service_unavailable, not_ready.status);
    try std.testing.expect(!not_ready.isSuccess());
    try std.testing.expectEqual(@as(usize, 3), fake_admission.calls);
}

test "authentication replay and malformed input never call admission" {
    var fake_adapter: FakeAdapter = .{ .mode = .authentication_failed };
    var fake_admission: FakeAdmission = .{
        .outcome = .{ .durable = testAcceptResult(.inserted) },
    };
    const handler: Handler = .{
        .adapter = fake_adapter.asAdapter(),
        .admission = fake_admission.asPort(),
    };

    const authentication = handler.handle(testRequest());
    try std.testing.expectEqual(std.http.Status.unauthorized, authentication.status);
    try std.testing.expectEqualStrings(authentication_body, authentication.body);
    try std.testing.expectEqual(@as(usize, 0), fake_admission.calls);

    fake_adapter.mode = .replay_rejected;
    const replay = handler.handle(testRequest());
    try std.testing.expectEqual(std.http.Status.unauthorized, replay.status);
    try std.testing.expectEqualStrings(authentication_body, replay.body);
    try std.testing.expectEqual(@as(usize, 0), fake_admission.calls);

    fake_adapter.mode = .invalid_request;
    try std.testing.expectEqual(std.http.Status.bad_request, handler.handle(testRequest()).status);
    fake_adapter.mode = .malformed_event;
    try std.testing.expectEqual(std.http.Status.bad_request, handler.handle(testRequest()).status);
    fake_adapter.mode = .out_of_memory;
    try std.testing.expectEqual(std.http.Status.service_unavailable, handler.handle(testRequest()).status);
    try std.testing.expectEqual(@as(usize, 0), fake_admission.calls);
}

test "closed admission outcomes have static bounded non-success responses" {
    const invalid = responseFromAdmission(.invalid);
    const conflict = responseFromAdmission(.conflict);
    const unavailable = responseFromAdmission(.unavailable);
    try std.testing.expectEqual(std.http.Status.bad_request, invalid.status);
    try std.testing.expectEqual(std.http.Status.conflict, conflict.status);
    try std.testing.expectEqual(std.http.Status.service_unavailable, unavailable.status);
    inline for (.{ invalid, conflict, unavailable }) |response| {
        try std.testing.expect(!response.isSuccess());
        try std.testing.expect(response.body.len <= 64);
    }
}

test "HTTP request inspection enforces exact framing headers and body bound" {
    const valid_head =
        "POST /v1/clearing/events HTTP/1.1\r\n" ++
        "content-type: application/json\r\n" ++
        "content-length: 2\r\n" ++
        timestamp_header_name ++ ": 1700000000\r\n" ++
        signature_header_name ++ ": " ++ "00" ** 32 ++ "\r\n\r\n";
    var reader = std.Io.Reader.fixed(valid_head);
    var output: [1]u8 = undefined;
    var writer = std.Io.Writer.fixed(&output);
    var server = std.http.Server.init(&reader, &writer);
    var request = try server.receiveHead();
    const parts = inspectRequest(&request).?;
    try std.testing.expectEqual(@as(usize, 2), parts.content_length);
    try std.testing.expectEqualStrings("/v1/clearing/events", parts.target[0..parts.target_len]);

    const duplicate_signature_head =
        "POST /v1/clearing/events HTTP/1.1\r\n" ++
        "content-type: application/json\r\n" ++
        "content-length: 2\r\n" ++
        timestamp_header_name ++ ": 1700000000\r\n" ++
        signature_header_name ++ ": " ++ "00" ** 32 ++ "\r\n" ++
        signature_header_name ++ ": " ++ "00" ** 32 ++ "\r\n\r\n";
    var duplicate_reader = std.Io.Reader.fixed(duplicate_signature_head);
    var duplicate_server = std.http.Server.init(&duplicate_reader, &writer);
    var duplicate_request = try duplicate_server.receiveHead();
    try std.testing.expectEqual(null, inspectRequest(&duplicate_request));

    const oversized_body_head =
        "POST /v1/clearing/events HTTP/1.1\r\n" ++
        "content-type: application/json\r\n" ++
        "content-length: 16385\r\n" ++
        timestamp_header_name ++ ": 1700000000\r\n" ++
        signature_header_name ++ ": " ++ "00" ** 32 ++ "\r\n\r\n";
    var oversized_reader = std.Io.Reader.fixed(oversized_body_head);
    var oversized_server = std.http.Server.init(&oversized_reader, &writer);
    var oversized_request = try oversized_server.receiveHead();
    try std.testing.expectEqual(null, inspectRequest(&oversized_request));

    const chunked_head =
        "POST /v1/clearing/events HTTP/1.1\r\n" ++
        "content-type: application/json\r\n" ++
        "transfer-encoding: chunked\r\n" ++
        timestamp_header_name ++ ": 1700000000\r\n" ++
        signature_header_name ++ ": " ++ "00" ** 32 ++ "\r\n\r\n";
    var chunked_reader = std.Io.Reader.fixed(chunked_head);
    var chunked_server = std.http.Server.init(&chunked_reader, &writer);
    var chunked_request = try chunked_server.receiveHead();
    try std.testing.expectEqual(null, inspectRequest(&chunked_request));
}

test "admission gate fences linearly and preserves an existing lease" {
    const io = std.testing.io;
    var fake_adapter: FakeAdapter = .{
        .mode = .{ .verified = .{ .authorization = testAuthorization() } },
    };
    var fake_admission: FakeAdmission = .{
        .outcome = .{ .durable = testAcceptResult(.inserted) },
    };
    var host = Host.init(io, .{ .handler = .{
        .adapter = fake_adapter.asAdapter(),
        .admission = fake_admission.asPort(),
    } });
    try host.beginServing();
    try std.testing.expectEqual(Health.ready, host.health());
    const lease = host.acquireAdmission().?;
    host.requestStop();
    try std.testing.expectEqual(Health.stopped, host.health());
    try std.testing.expect(!host.isAccepting());
    try std.testing.expectEqual(null, host.acquireAdmission());
    lease.release();
    try std.testing.expectEqual(@as(usize, 0), host.active_admissions);
}

test "host admits at most thirty two sessions" {
    const io = std.testing.io;
    var fake_adapter: FakeAdapter = .{
        .mode = .{ .verified = .{ .authorization = testAuthorization() } },
    };
    var fake_admission: FakeAdmission = .{
        .outcome = .{ .durable = testAcceptResult(.inserted) },
    };
    var host = Host.init(io, .{ .handler = .{
        .adapter = fake_adapter.asAdapter(),
        .admission = fake_admission.asPort(),
    } });
    try host.beginServing();
    for (0..max_sessions) |_| try std.testing.expect(host.tryBeginSession());
    try std.testing.expect(!host.tryBeginSession());
    for (0..max_sessions) |_| host.endSession();
    host.requestStop();
    try std.testing.expectEqual(@as(usize, 0), host.active_sessions);
}

const loopback_request =
    "POST /v1/clearing/events HTTP/1.1\r\n" ++
    "content-type: application/json\r\n" ++
    "content-length: 2\r\n" ++
    timestamp_header_name ++ ": 1700000000\r\n" ++
    signature_header_name ++ ": " ++ "00" ** 32 ++ "\r\n\r\n{}";

fn serveHostTask(host: *Host, listener: *std.Io.net.Server) ServeError!void {
    return host.serve(listener);
}

fn sendLoopbackRequest(
    io: std.Io,
    address: std.Io.net.IpAddress,
    raw_request: []const u8,
    response_storage: []u8,
) !usize {
    const stream = try address.connect(io, .{ .mode = .stream, .protocol = .tcp });
    defer stream.close(io);
    var write_buffer: [1024]u8 = undefined;
    var read_buffer: [1024]u8 = undefined;
    var writer = stream.writer(io, &write_buffer);
    var reader = stream.reader(io, &read_buffer);
    try writer.interface.writeAll(raw_request);
    try writer.interface.flush();

    var response_len: usize = 0;
    while (response_len < response_storage.len) {
        const remaining = response_storage[response_len..];
        const read_len = try reader.interface.readSliceShort(remaining);
        response_len += read_len;
        if (read_len < remaining.len) break;
    }
    if (response_len == response_storage.len) return error.ResponseTooLarge;
    return response_len;
}

test "loopback host emits inserted and duplicate ACK only after admission" {
    const io = std.testing.io;
    const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var listener = try address.listen(io, .{});
    defer listener.deinit(io);
    var fake_adapter: FakeAdapter = .{
        .mode = .{ .verified = .{ .authorization = testAuthorization() } },
    };
    var fake_admission: FakeAdmission = .{
        .outcome = .{ .durable = testAcceptResult(.inserted) },
    };
    var host = Host.init(io, .{
        .handler = .{
            .adapter = fake_adapter.asAdapter(),
            .admission = fake_admission.asPort(),
        },
        .clock = .{ .fixed = 1_700_000_000 },
    });
    var host_task = try io.concurrent(serveHostTask, .{ &host, &listener });
    defer host_task.cancel(io) catch {};
    defer host.requestStop();

    var inserted_storage: [1024]u8 = undefined;
    const inserted_len = try sendLoopbackRequest(
        io,
        listener.socket.address,
        loopback_request,
        &inserted_storage,
    );
    const inserted = inserted_storage[0..inserted_len];
    try std.testing.expect(std.mem.startsWith(u8, inserted, "HTTP/1.1 202 Accepted\r\n"));
    try std.testing.expect(std.mem.endsWith(u8, inserted, inserted_body));
    try std.testing.expectEqual(@as(usize, 1), fake_admission.calls);

    fake_admission.outcome = .{ .durable = testAcceptResult(.existing) };
    var existing_storage: [1024]u8 = undefined;
    const existing_len = try sendLoopbackRequest(
        io,
        listener.socket.address,
        loopback_request,
        &existing_storage,
    );
    const existing = existing_storage[0..existing_len];
    try std.testing.expect(std.mem.startsWith(u8, existing, "HTTP/1.1 200 OK\r\n"));
    try std.testing.expect(std.mem.endsWith(u8, existing, existing_body));
    try std.testing.expectEqual(@as(usize, 2), fake_admission.calls);

    fake_admission.outcome = .unavailable;
    var unavailable_storage: [1024]u8 = undefined;
    const unavailable_len = try sendLoopbackRequest(
        io,
        listener.socket.address,
        loopback_request,
        &unavailable_storage,
    );
    const unavailable = unavailable_storage[0..unavailable_len];
    try std.testing.expect(std.mem.startsWith(
        u8,
        unavailable,
        "HTTP/1.1 503 Service Unavailable\r\n",
    ));
    try std.testing.expect(!std.mem.containsAtLeast(u8, unavailable, 1, "HTTP/1.1 2"));

    host.requestStop();
    try host_task.await(io);
    try std.testing.expectEqual(Health.stopped, host.health());
}

test "loopback authentication failure is generic and never reaches admission" {
    const io = std.testing.io;
    const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var listener = try address.listen(io, .{});
    defer listener.deinit(io);
    var fake_adapter: FakeAdapter = .{ .mode = .authentication_failed };
    var fake_admission: FakeAdmission = .{
        .outcome = .{ .durable = testAcceptResult(.inserted) },
    };
    var host = Host.init(io, .{
        .handler = .{
            .adapter = fake_adapter.asAdapter(),
            .admission = fake_admission.asPort(),
        },
        .clock = .{ .fixed = 1_700_000_000 },
    });
    var host_task = try io.concurrent(serveHostTask, .{ &host, &listener });
    defer host_task.cancel(io) catch {};
    defer host.requestStop();

    var response_storage: [1024]u8 = undefined;
    const response_len = try sendLoopbackRequest(
        io,
        listener.socket.address,
        loopback_request,
        &response_storage,
    );
    const response = response_storage[0..response_len];
    try std.testing.expect(std.mem.startsWith(u8, response, "HTTP/1.1 401 Unauthorized\r\n"));
    try std.testing.expect(std.mem.endsWith(u8, response, authentication_body));
    try std.testing.expectEqual(@as(usize, 0), fake_admission.calls);

    host.requestStop();
    try host_task.await(io);
}

const BlockingAdmission = struct {
    io: std.Io,
    entered: std.Io.Event = .unset,
    release: std.Io.Event = .unset,
    calls: usize = 0,

    fn asPort(self: *BlockingAdmission) AdmissionPort {
        return .{ .context = self, .vtable = &vtable };
    }

    const vtable: AdmissionPort.VTable = .{
        .admit = admit,
        .is_ready = isReady,
    };

    fn admit(raw: *anyopaque, event: provider.VerifiedEvent) AdmissionOutcome {
        _ = event;
        const self: *BlockingAdmission = @ptrCast(@alignCast(raw));
        self.calls += 1;
        self.entered.set(self.io);
        self.release.waitUncancelable(self.io);
        return .{ .durable = testAcceptResult(.inserted) };
    }

    fn isReady(raw: *anyopaque) bool {
        _ = raw;
        return true;
    }
};

test "shutdown fence drains an admission lease without canceling it" {
    const io = std.testing.io;
    const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var listener = try address.listen(io, .{});
    defer listener.deinit(io);
    var fake_adapter: FakeAdapter = .{
        .mode = .{ .verified = .{ .authorization = testAuthorization() } },
    };
    var blocking_admission: BlockingAdmission = .{ .io = io };
    var host = Host.init(io, .{
        .handler = .{
            .adapter = fake_adapter.asAdapter(),
            .admission = blocking_admission.asPort(),
        },
        .clock = .{ .fixed = 1_700_000_000 },
    });
    var host_task = try io.concurrent(serveHostTask, .{ &host, &listener });
    defer host_task.cancel(io) catch {};
    defer host.requestStop();

    var response_storage: [1024]u8 = undefined;
    var client_task = try io.concurrent(
        sendLoopbackRequest,
        .{ io, listener.socket.address, loopback_request, &response_storage },
    );
    defer _ = client_task.cancel(io) catch 0;
    try blocking_admission.entered.wait(io);
    host.requestStop();
    try std.testing.expectEqual(Health.stopped, host.health());
    try std.testing.expect(!host.isAccepting());
    try std.testing.expectEqual(@as(usize, 1), host.active_admissions);
    try std.testing.expectEqual(null, host.acquireAdmission());

    blocking_admission.release.set(io);
    const response_len = try client_task.await(io);
    const response = response_storage[0..response_len];
    try std.testing.expect(std.mem.startsWith(u8, response, "HTTP/1.1 202 Accepted\r\n"));
    try std.testing.expect(std.mem.endsWith(u8, response, inserted_body));
    try host_task.await(io);
    try std.testing.expectEqual(@as(usize, 1), blocking_admission.calls);
    try std.testing.expectEqual(@as(usize, 0), host.active_admissions);
    try std.testing.expectEqual(@as(usize, 0), host.active_sessions);
}

const FakeFailureCallback = struct {
    calls: usize = 0,
    reason: ?FailureReason = null,

    fn callback(self: *FakeFailureCallback) FailureCallback {
        return .{ .context = self, .notify_fn = notify };
    }

    fn notify(raw: *anyopaque, reason: FailureReason) void {
        const self: *FakeFailureCallback = @ptrCast(@alignCast(raw));
        self.calls += 1;
        self.reason = reason;
    }
};

test "fatal listener publication fences once and invokes callback once" {
    const io = std.testing.io;
    var fake_adapter: FakeAdapter = .{
        .mode = .{ .verified = .{ .authorization = testAuthorization() } },
    };
    var fake_admission: FakeAdmission = .{
        .outcome = .{ .durable = testAcceptResult(.inserted) },
    };
    var failure: FakeFailureCallback = .{};
    var host = Host.init(io, .{
        .handler = .{
            .adapter = fake_adapter.asAdapter(),
            .admission = fake_admission.asPort(),
        },
        .failure_callback = failure.callback(),
    });
    try host.beginServing();
    host.publishFailed();
    host.publishFailed();
    try std.testing.expectEqual(Health.failed, host.health());
    try std.testing.expect(!host.isAccepting());
    try std.testing.expectEqual(@as(usize, 1), failure.calls);
    try std.testing.expectEqual(FailureReason.listener_failed, failure.reason.?);
}

test "stop winner closes a concurrently completed accept result" {
    const io = std.testing.io;
    const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var listener = try address.listen(io, .{});
    defer listener.deinit(io);
    const client = try listener.socket.address.connect(io, .{ .mode = .stream, .protocol = .tcp });
    defer client.close(io);
    const accepted = try listener.accept(io);

    // This is the exact losing result produced when the stop branch has won
    // the Select while accept completed concurrently.
    disposeAcceptRaceResult(io, .{ .accept = .{ .stream = accepted } });

    var read_buffer: [1]u8 = undefined;
    var reader = client.reader(io, &read_buffer);
    try std.testing.expectEqual(@as(usize, 0), try reader.interface.readSliceShort(&read_buffer));
}
