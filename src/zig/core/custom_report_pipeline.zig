//! Bridge between the type-safe custom-report registry and Java-compatible
//! BINARY_EVENT section carriers.
//!
//! Engine callbacks own `SectionHandle` values. This module converts those
//! immutable section bytes into matcher-event chains and reconstructs the
//! external-section-sorted, zero-padded reducer inputs used by Java's
//! `ExchangeApi.processReport` path.

const std = @import("std");
const binary_payload = @import("binary_payload.zig");
const core_events = @import("events.zig");
const java_binary_wire = @import("compat/java/binary_wire.zig");
const report_registry = @import("report_registry.zig");
const wire = @import("compat/java/report_wire.zig");

pub const PipelineError = wire.WireError || report_registry.ReductionDispatchError;

pub const StageRequestError = report_registry.QueryDispatchError ||
    report_registry.EncodeDispatchError ||
    java_binary_wire.RequestEncodeError ||
    error{
        UnregisteredDescriptor,
        ReservedReportCode,
        RequestTokenOverflow,
        RequestStoreClosed,
    };

pub const RequestTokenError = error{
    WrongRequestStore,
    WrongRequestOwner,
    RequestConsumedOrUnknown,
    RequestNotStaged,
    RequestNotPending,
    RequestStoreClosed,
};

pub const PendingReductionError = PipelineError || RequestTokenError;

/// A non-owning handle for a request that has been validated and encoded but
/// not yet published. RequestStore owns every allocation, so sequential copies
/// cannot double free. Staged handles stay on the single submitting thread;
/// only PendingRequest is intended to cross threads.
pub const StagedRequest = struct {
    store: ?*RequestStore,
    store_generation: u64,
    token_id: u64,
    owner_identity: *const anyopaque,

    pub fn carrierWords(
        self: *const StagedRequest,
    ) RequestTokenError![]const i64 {
        const store = self.store orelse return error.RequestConsumedOrUnknown;
        return store.stagedCarrierWords(self.*);
    }

    /// Commits an already-published request id without allocating. The store
    /// was bound to the execution registry before carrier bytes were exposed,
    /// so publication can never precede registry validation.
    pub fn commit(
        self: *StagedRequest,
        request_id: u64,
    ) RequestTokenError!PendingRequest {
        const store = self.store orelse return error.RequestConsumedOrUnknown;
        const pending = try store.commitStaged(self.*, request_id);
        self.store = null;
        return pending;
    }

    /// Idempotently abandons an unpublished request. A copied staged alias
    /// that runs after another alias committed cannot cancel the pending work.
    pub fn deinit(self: *StagedRequest) void {
        const store = self.store orelse return;
        store.discardStaged(self.*);
        self.store = null;
    }
};

/// A copy-safe capability for one published custom report request. It owns no
/// memory and deliberately has no deinitializer: the RequestStore retains the
/// cloned query until reduction, explicit failure, or store shutdown.
pub const PendingRequest = struct {
    store: *RequestStore,
    store_generation: u64,
    token_id: u64,
    owner_identity: *const anyopaque,
};

const RequestState = union(enum) {
    staged,
    pending: u64,
};

const RequestBox = struct {
    allocator: std.mem.Allocator,
    descriptor: *const report_registry.Descriptor,
    query: *report_registry.QueryHandle,
    carrier_words: ?[]i64,
    state: RequestState,
};

/// Owns all staged and pending request allocations for one API instance. The
/// registry and owner identity are fixed at initialization, before any request
/// carrier can be borrowed. Pending operations are mutex-protected so copied
/// tokens can race without double reduction or double destruction.
///
/// The registry and owner identity must outlive the store. Store deinit requires
/// external quiescence, just like the enclosing ExchangeApiSharded shutdown.
pub const RequestStore = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    registry: *report_registry.Registry,
    owner_identity: *const anyopaque,
    generation: u64,
    next_token_id: u64 = 1,
    requests: std.AutoHashMapUnmanaged(u64, *RequestBox) = .empty,
    mutex: std.Io.Mutex = .init,
    closed: bool = false,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        registry: *const report_registry.Registry,
        owner_identity: *const anyopaque,
    ) report_registry.RegistryInitError!RequestStore {
        const owned_registry = try registry.cloneOwned(allocator);
        return .{
            .allocator = allocator,
            .io = io,
            .registry = owned_registry,
            .owner_identity = owner_identity,
            .generation = nextRequestStoreGeneration(),
        };
    }

    pub fn deinit(self: *RequestStore) void {
        self.mutex.lockUncancelable(self.io);
        if (self.closed) {
            self.mutex.unlock(self.io);
            return;
        }
        self.closed = true;
        var requests = self.requests;
        self.requests = .empty;
        self.mutex.unlock(self.io);

        var values = requests.valueIterator();
        while (values.next()) |box| destroyRequestBox(box.*);
        requests.deinit(self.allocator);
        self.registry.deinit();
        self.* = undefined;
    }

    /// Validates exact descriptor membership, retains an independent query
    /// clone, and serializes it through Java's little-endian code/body plus
    /// raw-LZ4 carrier format. Built-in codes remain on their fixed lane even
    /// if the registry itself was constructed without reserved-code checks.
    pub fn stageRequest(
        self: *RequestStore,
        query: *const report_registry.QueryHandle,
        config: java_binary_wire.Config,
    ) StageRequestError!StagedRequest {
        self.mutex.lockUncancelable(self.io);
        const closed = self.closed;
        self.mutex.unlock(self.io);
        if (closed) return error.RequestStoreClosed;

        const descriptor = query.descriptor();
        if (!self.registry.containsDescriptor(descriptor)) {
            return error.UnregisteredDescriptor;
        }
        if (isReservedBuiltInCode(descriptor.code())) {
            return error.ReservedReportCode;
        }

        const owned_query = try descriptor.cloneQuery(self.allocator, query);
        errdefer owned_query.deinit();

        const query_body = try descriptor.encodeQuery(self.allocator, owned_query);
        defer self.allocator.free(query_body);
        const raw_body = try binary_payload.encodeJavaRawReportQueryToBytes(
            self.allocator,
            descriptor.code(),
            query_body,
        );
        defer self.allocator.free(raw_body);
        const carrier_words = try java_binary_wire.encodeRequestWords(
            self.allocator,
            raw_body,
            config,
        );
        errdefer self.allocator.free(carrier_words);

        const box = try self.allocator.create(RequestBox);
        errdefer self.allocator.destroy(box);
        box.* = .{
            .allocator = self.allocator,
            .descriptor = descriptor,
            .query = owned_query,
            .carrier_words = carrier_words,
            .state = .staged,
        };

        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.closed) return error.RequestStoreClosed;
        const token_id = self.next_token_id;
        if (token_id == 0) return error.RequestTokenOverflow;
        try self.requests.putNoClobber(self.allocator, token_id, box);
        self.next_token_id +%= 1;
        return .{
            .store = self,
            .store_generation = self.generation,
            .token_id = token_id,
            .owner_identity = self.owner_identity,
        };
    }

    pub fn pendingRequestId(
        self: *RequestStore,
        pending: PendingRequest,
        expected_owner: *const anyopaque,
    ) RequestTokenError!u64 {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.closed) return error.RequestStoreClosed;
        try self.validatePendingToken(pending, expected_owner);
        const box = self.requests.get(pending.token_id) orelse
            return error.RequestConsumedOrUnknown;
        return switch (box.state) {
            .pending => |request_id| request_id,
            .staged => error.RequestNotPending,
        };
    }

    /// Removes ownership before invoking the reducer. Reducer failure is
    /// terminal and a copied alias subsequently observes
    /// RequestConsumedOrUnknown.
    pub fn reducePending(
        self: *RequestStore,
        pending: PendingRequest,
        expected_owner: *const anyopaque,
        allocator: std.mem.Allocator,
        head: ?*const core_events.MatcherTradeEvent,
        config: wire.Config,
    ) PendingReductionError!*report_registry.ResultHandle {
        const box = try self.removePending(pending, expected_owner);
        defer destroyRequestBox(box);
        return reduceEventChain(
            allocator,
            box.descriptor,
            box.query,
            head,
            config,
        );
    }

    /// Terminally drops a published request without invoking its reducer, used
    /// when the binary command itself failed.
    pub fn failPending(
        self: *RequestStore,
        pending: PendingRequest,
        expected_owner: *const anyopaque,
    ) RequestTokenError!void {
        const box = try self.removePending(pending, expected_owner);
        destroyRequestBox(box);
    }

    fn stagedCarrierWords(
        self: *RequestStore,
        staged: StagedRequest,
    ) RequestTokenError![]const i64 {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.closed) return error.RequestStoreClosed;
        try self.validateStagedToken(staged);
        const box = self.requests.get(staged.token_id) orelse
            return error.RequestConsumedOrUnknown;
        return switch (box.state) {
            .staged => box.carrier_words.?,
            .pending => error.RequestNotStaged,
        };
    }

    fn commitStaged(
        self: *RequestStore,
        staged: StagedRequest,
        request_id: u64,
    ) RequestTokenError!PendingRequest {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.closed) return error.RequestStoreClosed;
        try self.validateStagedToken(staged);
        const box = self.requests.get(staged.token_id) orelse
            return error.RequestConsumedOrUnknown;
        switch (box.state) {
            .staged => {},
            .pending => return error.RequestNotStaged,
        }
        self.allocator.free(box.carrier_words.?);
        box.carrier_words = null;
        box.state = .{ .pending = request_id };
        return .{
            .store = self,
            .store_generation = self.generation,
            .token_id = staged.token_id,
            .owner_identity = self.owner_identity,
        };
    }

    fn discardStaged(self: *RequestStore, staged: StagedRequest) void {
        self.mutex.lockUncancelable(self.io);
        if (self.closed) {
            self.mutex.unlock(self.io);
            return;
        }
        self.validateStagedToken(staged) catch {
            self.mutex.unlock(self.io);
            return;
        };
        const box = self.requests.get(staged.token_id) orelse {
            self.mutex.unlock(self.io);
            return;
        };
        if (box.state != .staged) {
            self.mutex.unlock(self.io);
            return;
        }
        const removed = self.requests.fetchRemove(staged.token_id) orelse unreachable;
        self.mutex.unlock(self.io);
        destroyRequestBox(removed.value);
    }

    fn removePending(
        self: *RequestStore,
        pending: PendingRequest,
        expected_owner: *const anyopaque,
    ) RequestTokenError!*RequestBox {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.closed) return error.RequestStoreClosed;
        try self.validatePendingToken(pending, expected_owner);
        const box = self.requests.get(pending.token_id) orelse
            return error.RequestConsumedOrUnknown;
        if (box.state != .pending) return error.RequestNotPending;
        const removed = self.requests.fetchRemove(pending.token_id) orelse
            unreachable;
        return removed.value;
    }

    fn validateStagedToken(
        self: *RequestStore,
        staged: StagedRequest,
    ) RequestTokenError!void {
        if (staged.store != self) return error.WrongRequestStore;
        if (staged.store_generation != self.generation) {
            return error.WrongRequestStore;
        }
        if (staged.owner_identity != self.owner_identity) {
            return error.WrongRequestOwner;
        }
    }

    fn validatePendingToken(
        self: *RequestStore,
        pending: PendingRequest,
        expected_owner: *const anyopaque,
    ) RequestTokenError!void {
        if (pending.store != self) return error.WrongRequestStore;
        if (pending.store_generation != self.generation) {
            return error.WrongRequestStore;
        }
        if (expected_owner != self.owner_identity or
            pending.owner_identity != self.owner_identity)
        {
            return error.WrongRequestOwner;
        }
    }
};

var request_store_generation = std.atomic.Value(u64).init(1);

fn nextRequestStoreGeneration() u64 {
    while (true) {
        const generation = request_store_generation.fetchAdd(1, .monotonic);
        if (generation != 0) return generation;
    }
}

fn isReservedBuiltInCode(code: i32) bool {
    _ = binary_payload.ReportQueryTypeCode.fromCode(code) catch return false;
    return true;
}

fn destroyRequestBox(box: *RequestBox) void {
    const allocator = box.allocator;
    box.query.deinit();
    if (box.carrier_words) |carrier_words| allocator.free(carrier_words);
    allocator.destroy(box);
}

/// Appends one owned registry section to a caller-owned matcher-event chain.
/// An empty encoded body produces no carriers, matching Java's helper loop.
/// On allocation failure the caller must discard the enclosing chain; report
/// production builds into a private chain and publishes it only on success.
pub fn appendSection(
    allocator: std.mem.Allocator,
    chain: *core_events.EventChain,
    label: wire.SectionLabel,
    section: *const report_registry.SectionHandle,
    config: wire.Config,
) wire.WireError!void {
    const body = section.bytes();
    if (body.len > config.max_body_bytes) return error.BodyTooLarge;

    const carriers = try wire.packSectionEvents(allocator, label, body);
    defer allocator.free(carriers);

    for (carriers) |carrier| {
        _ = try chain.append(.{
            .event_type = .binary_event,
            .section = carrier.section_id,
            .matched_order_id = carrier.words[0],
            .matched_order_uid = carrier.words[1],
            .price = carrier.words[2],
            .size = carrier.words[3],
            .bidder_hold_price = carrier.words[4],
        });
    }
}

/// Applies a descriptor's reducer to a real matcher-event chain. Section
/// inputs are sorted by Java external section id; carriers belonging to the
/// same section retain encounter order. The slices include the final
/// carrier's zero padding and remain borrowed only for the reducer call.
pub fn reduceEventChain(
    allocator: std.mem.Allocator,
    descriptor: *const report_registry.Descriptor,
    original_query: *const report_registry.QueryHandle,
    head: ?*const core_events.MatcherTradeEvent,
    config: wire.Config,
) PipelineError!*report_registry.ResultHandle {
    var grouped = try wire.groupMatcherEventChain(allocator, head, config);
    defer grouped.deinit(allocator);

    const unpacked = try allocator.alloc(wire.UnpackedSection, grouped.sections.len);
    defer allocator.free(unpacked);
    var unpacked_count: usize = 0;
    defer for (unpacked[0..unpacked_count]) |*section| section.deinit(allocator);

    const sections = try allocator.alloc(report_registry.SectionBytes, grouped.sections.len);
    defer allocator.free(sections);

    for (grouped.sections, 0..) |group, index| {
        unpacked[index] = try wire.unpackSectionEvents(allocator, group.carriers);
        unpacked_count += 1;
        sections[index] = .{
            .section_id = unpacked[index].label.externalId(),
            .padded_bytes = unpacked[index].padded_bytes,
        };
    }

    return descriptor.reduceSections(allocator, original_query, sections);
}

const TestQuery = struct {
    bias: i64,
};

const TestSection = struct {
    value: i64,
};

const TestResult = struct {
    value: i64,
    section_ids: []i32,
};

const TestMatchingView = struct {
    value: i64,
};

const TestRiskView = struct {
    value: i64,
};

const TestSpec = struct {
    pub const code: i32 = 70_101;
    pub const schema_id: [report_registry.schema_id_len]u8 = [_]u8{0x71} ** report_registry.schema_id_len;
    pub const schema_version: u32 = 1;
    pub const Query = TestQuery;
    pub const Section = TestSection;
    pub const Result = TestResult;
    pub const MatchingView = TestMatchingView;
    pub const RiskView = TestRiskView;

    pub fn cloneQuery(
        _: std.mem.Allocator,
        query: *const Query,
    ) report_registry.ResourceError!Query {
        return query.*;
    }

    pub fn deinitQuery(_: std.mem.Allocator, query: *Query) void {
        query.* = undefined;
    }

    pub fn encodeQuery(
        _: std.mem.Allocator,
        writer: *report_registry.EncodingWriter,
        query: *const Query,
    ) report_registry.EncodingError!void {
        try writer.writeInt(i64, query.bias, .little);
    }

    pub fn decodeQuery(
        _: std.mem.Allocator,
        body: []const u8,
    ) report_registry.QueryConstructionError!Query {
        if (body.len != @sizeOf(i64)) return error.InvalidQuery;
        return .{ .bias = std.mem.readInt(i64, body[0..@sizeOf(i64)], .little) };
    }

    pub fn processMatching(
        _: std.mem.Allocator,
        query: *const Query,
        context: report_registry.MatchingContext(MatchingView),
    ) report_registry.HandlerError!?Section {
        return .{ .value = query.bias +% context.view.value };
    }

    pub fn processRisk(
        _: std.mem.Allocator,
        query: *const Query,
        context: report_registry.RiskContext(RiskView),
    ) report_registry.HandlerError!?Section {
        return .{ .value = query.bias +% context.view.value };
    }

    pub fn encodeSection(
        _: std.mem.Allocator,
        writer: *report_registry.EncodingWriter,
        section: *const Section,
    ) report_registry.EncodingError!void {
        try writer.writeInt(i64, section.value, .little);
    }

    pub fn deinitSection(_: std.mem.Allocator, section: *Section) void {
        section.* = undefined;
    }

    pub fn reduce(
        allocator: std.mem.Allocator,
        query: *const Query,
        sections: []const report_registry.SectionBytes,
    ) report_registry.ReductionError!Result {
        const section_ids = try allocator.alloc(i32, sections.len);
        errdefer allocator.free(section_ids);
        var value = query.bias;
        for (sections, 0..) |section, index| {
            if (section.padded_bytes.len < @sizeOf(i64)) return error.InvalidSection;
            section_ids[index] = section.section_id;
            value +%= std.mem.readInt(
                i64,
                section.padded_bytes[0..@sizeOf(i64)],
                .little,
            );
        }
        return .{ .value = value, .section_ids = section_ids };
    }

    pub fn deinitResult(allocator: std.mem.Allocator, result: *Result) void {
        allocator.free(result.section_ids);
        result.* = undefined;
    }
};

test "custom report bridge preserves Java section ordering and padded bodies" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(TestSpec);
    const descriptor = Adapted.descriptorPtr();
    const query_value: TestQuery = .{ .bias = 10 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();

    const matching_view: TestMatchingView = .{ .value = 3 };
    const risk_view: TestRiskView = .{ .value = 7 };
    const matching = (try Adapted.processMatching(
        allocator,
        &query_value,
        .{ .view = &matching_view, .shard_id = 0, .shard_mask = 0 },
    )).?;
    defer matching.deinit();
    const risk = (try Adapted.processRisk(
        allocator,
        &query_value,
        .{ .view = &risk_view, .shard_id = 0, .shard_mask = 0 },
    )).?;
    defer risk.deinit();

    var chain = core_events.EventChain.init(allocator);
    defer chain.deinit();
    // Encounter order is deliberately opposite Java's reducer section order.
    try appendSection(allocator, &chain, try wire.SectionLabel.matching(0), matching, .{});
    try appendSection(allocator, &chain, try wire.SectionLabel.risk(0), risk, .{});

    const erased = try reduceEventChain(allocator, descriptor, query, chain.head, .{});
    defer erased.deinit();
    const result = try Adapted.borrowResult(erased);
    try std.testing.expectEqual(@as(i64, 40), result.value);
    try std.testing.expectEqualSlices(i32, &.{ 0, 1024 }, result.section_ids);
}

test "custom report bridge bounds body bytes while allowing carrier padding" {
    const allocator = std.testing.allocator;
    const query_value: TestQuery = .{ .bias = 1 };
    const risk_view: TestRiskView = .{ .value = 2 };
    const section = (try report_registry.Adapter(TestSpec).processRisk(
        allocator,
        &query_value,
        .{ .view = &risk_view, .shard_id = 0, .shard_mask = 0 },
    )).?;
    defer section.deinit();

    var allowed = core_events.EventChain.init(allocator);
    defer allowed.deinit();
    try appendSection(
        allocator,
        &allowed,
        try wire.SectionLabel.risk(0),
        section,
        .{ .max_body_bytes = @sizeOf(i64) },
    );
    try std.testing.expectEqual(@as(usize, 1), allowed.len);

    var rejected = core_events.EventChain.init(allocator);
    defer rejected.deinit();
    try std.testing.expectError(
        error.BodyTooLarge,
        appendSection(
            allocator,
            &rejected,
            try wire.SectionLabel.risk(0),
            section,
            .{ .max_body_bytes = @sizeOf(i64) - 1 },
        ),
    );
    try std.testing.expectEqual(@as(usize, 0), rejected.len);
}

test "custom report bridge reduces the empty section identity" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(TestSpec);
    const query_value: TestQuery = .{ .bias = 9 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();

    const erased = try reduceEventChain(
        allocator,
        Adapted.descriptorPtr(),
        query,
        null,
        .{},
    );
    defer erased.deinit();
    const result = try Adapted.borrowResult(erased);
    try std.testing.expectEqual(@as(i64, 9), result.value);
    try std.testing.expectEqual(@as(usize, 0), result.section_ids.len);
}

test "custom report bridge rejects non-binary event chains before reduction" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(TestSpec);
    const query_value: TestQuery = .{ .bias = 1 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();

    var event: core_events.MatcherTradeEvent = .{ .event_type = .trade };
    try std.testing.expectError(
        error.NonBinaryEvent,
        reduceEventChain(
            allocator,
            Adapted.descriptorPtr(),
            query,
            &event,
            .{},
        ),
    );
}

test "request store retains exact query through commit and reduction" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(TestSpec);
    const registry = report_registry.Registry.fromSpecs(.{TestSpec});
    var owner: u8 = 0;
    const owner_identity: *const anyopaque = @ptrCast(&owner);
    var store = try RequestStore.init(
        allocator,
        std.testing.io,
        registry,
        owner_identity,
    );
    defer store.deinit();
    const query_value: TestQuery = .{ .bias = 10 };
    const source_query = try Adapted.cloneQuery(allocator, &query_value);

    var staged = try store.stageRequest(source_query, .{});
    defer staged.deinit();
    source_query.deinit();

    const carrier_words = try staged.carrierWords();
    var decoded_request = try java_binary_wire.decodeRequestWords(
        allocator,
        carrier_words,
        .{},
    );
    defer decoded_request.deinit(allocator);
    const raw = try binary_payload.decodeJavaRawReportQueryFromBytes(
        decoded_request.body,
    );
    try std.testing.expectEqual(TestSpec.code, raw.code);
    try std.testing.expectEqual(@as(usize, @sizeOf(i64)), raw.body.len);
    try std.testing.expectEqual(
        query_value.bias,
        std.mem.readInt(i64, raw.body[0..@sizeOf(i64)], .little),
    );

    const pending = try staged.commit(55_001);
    const pending_alias = pending;
    try std.testing.expectEqual(
        @as(u64, 55_001),
        try store.pendingRequestId(pending, owner_identity),
    );

    const matching_view: TestMatchingView = .{ .value = 3 };
    const risk_view: TestRiskView = .{ .value = 7 };
    const matching = (try Adapted.processMatching(
        allocator,
        &query_value,
        .{ .view = &matching_view, .shard_id = 0, .shard_mask = 0 },
    )).?;
    defer matching.deinit();
    const risk = (try Adapted.processRisk(
        allocator,
        &query_value,
        .{ .view = &risk_view, .shard_id = 0, .shard_mask = 0 },
    )).?;
    defer risk.deinit();

    var chain = core_events.EventChain.init(allocator);
    defer chain.deinit();
    try appendSection(
        allocator,
        &chain,
        try wire.SectionLabel.matching(0),
        matching,
        .{},
    );
    try appendSection(
        allocator,
        &chain,
        try wire.SectionLabel.risk(0),
        risk,
        .{},
    );

    const erased = try store.reducePending(
        pending,
        owner_identity,
        allocator,
        chain.head,
        .{},
    );
    defer erased.deinit();
    const result = try Adapted.borrowResult(erased);
    try std.testing.expectEqual(@as(i64, 40), result.value);
    try std.testing.expectEqualSlices(i32, &.{ 0, 1024 }, result.section_ids);
    try std.testing.expectError(
        error.RequestConsumedOrUnknown,
        store.reducePending(
            pending_alias,
            owner_identity,
            allocator,
            chain.head,
            .{},
        ),
    );
}

test "request store rejects descriptors before exposing carriers and bounds requests" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(TestSpec);
    const query_value: TestQuery = .{ .bias = 1 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();
    var owner: u8 = 0;
    const owner_identity: *const anyopaque = @ptrCast(&owner);

    const empty_registry = report_registry.Registry.fromSpecs(.{});
    var empty_store = try RequestStore.init(
        allocator,
        std.testing.io,
        empty_registry,
        owner_identity,
    );
    defer empty_store.deinit();
    try std.testing.expectError(
        error.UnregisteredDescriptor,
        empty_store.stageRequest(query, .{}),
    );

    const registry = report_registry.Registry.fromSpecs(.{TestSpec});
    var store = try RequestStore.init(
        allocator,
        std.testing.io,
        registry,
        owner_identity,
    );
    defer store.deinit();
    try std.testing.expectError(
        error.OriginalSizeTooLarge,
        store.stageRequest(
            query,
            .{ .max_original_size = @sizeOf(i32) + @sizeOf(i64) - 1 },
        ),
    );

    store.next_token_id = 0;
    try std.testing.expectError(
        error.RequestTokenOverflow,
        store.stageRequest(query, .{}),
    );
}

test "copied staged handles cannot double free or cancel committed work" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(TestSpec);
    const registry = report_registry.Registry.fromSpecs(.{TestSpec});
    var owner: u8 = 0;
    const owner_identity: *const anyopaque = @ptrCast(&owner);
    var store = try RequestStore.init(
        allocator,
        std.testing.io,
        registry,
        owner_identity,
    );
    defer store.deinit();
    const query_value: TestQuery = .{ .bias = 1 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();

    var abandoned = try store.stageRequest(query, .{});
    var abandoned_alias = abandoned;
    abandoned.deinit();
    try std.testing.expectError(
        error.RequestConsumedOrUnknown,
        abandoned_alias.carrierWords(),
    );
    abandoned_alias.deinit();

    var staged = try store.stageRequest(query, .{});
    var committing_alias = staged;
    const pending = try committing_alias.commit(91);
    staged.deinit();
    try std.testing.expectEqual(
        @as(u64, 91),
        try store.pendingRequestId(pending, owner_identity),
    );
    try store.failPending(pending, owner_identity);
    try std.testing.expectError(
        error.RequestConsumedOrUnknown,
        store.pendingRequestId(pending, owner_identity),
    );
}

test "pending handles reject cross-store and cross-owner use before lookup" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(TestSpec);
    const registry = report_registry.Registry.fromSpecs(.{TestSpec});
    var owner_a: u8 = 0;
    var owner_b: u8 = 0;
    const owner_a_identity: *const anyopaque = @ptrCast(&owner_a);
    const owner_b_identity: *const anyopaque = @ptrCast(&owner_b);
    var store_a = try RequestStore.init(
        allocator,
        std.testing.io,
        registry,
        owner_a_identity,
    );
    defer store_a.deinit();
    var store_b = try RequestStore.init(
        allocator,
        std.testing.io,
        registry,
        owner_b_identity,
    );
    defer store_b.deinit();
    const query_value: TestQuery = .{ .bias = 1 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();

    var staged = try store_a.stageRequest(query, .{});
    defer staged.deinit();
    const pending = try staged.commit(77);
    try std.testing.expectError(
        error.WrongRequestStore,
        store_b.pendingRequestId(pending, owner_b_identity),
    );
    try std.testing.expectError(
        error.WrongRequestOwner,
        store_a.pendingRequestId(pending, owner_b_identity),
    );
    try std.testing.expectEqual(
        @as(u64, 77),
        try store_a.pendingRequestId(pending, owner_a_identity),
    );
    try store_a.failPending(pending, owner_a_identity);
}

test "store generation rejects stale pending tokens after address reuse" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(TestSpec);
    const registry = report_registry.Registry.fromSpecs(.{TestSpec});
    var owner: u8 = 0;
    const owner_identity: *const anyopaque = @ptrCast(&owner);
    const query_value: TestQuery = .{ .bias = 1 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();

    var store = try RequestStore.init(
        allocator,
        std.testing.io,
        registry,
        owner_identity,
    );
    var old_staged = try store.stageRequest(query, .{});
    const old_pending = try old_staged.commit(1);
    store.deinit();

    store = try RequestStore.init(
        allocator,
        std.testing.io,
        registry,
        owner_identity,
    );
    defer store.deinit();
    var new_staged = try store.stageRequest(query, .{});
    const new_pending = try new_staged.commit(2);

    try std.testing.expectError(
        error.WrongRequestStore,
        store.pendingRequestId(old_pending, owner_identity),
    );
    try std.testing.expectEqual(
        @as(u64, 2),
        try store.pendingRequestId(new_pending, owner_identity),
    );
    try store.failPending(new_pending, owner_identity);
}

test "reduction allocation failure terminally consumes pending request" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(TestSpec);
    const registry = report_registry.Registry.fromSpecs(.{TestSpec});
    var owner: u8 = 0;
    const owner_identity: *const anyopaque = @ptrCast(&owner);
    var store = try RequestStore.init(
        allocator,
        std.testing.io,
        registry,
        owner_identity,
    );
    defer store.deinit();
    const query_value: TestQuery = .{ .bias = 1 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();
    var staged = try store.stageRequest(query, .{});
    const pending = try staged.commit(3);

    const risk_view: TestRiskView = .{ .value = 2 };
    const section = (try Adapted.processRisk(
        allocator,
        &query_value,
        .{ .view = &risk_view, .shard_id = 0, .shard_mask = 0 },
    )).?;
    defer section.deinit();
    var chain = core_events.EventChain.init(allocator);
    defer chain.deinit();
    try appendSection(
        allocator,
        &chain,
        try wire.SectionLabel.risk(0),
        section,
        .{},
    );

    var failing = std.testing.FailingAllocator.init(
        allocator,
        .{ .fail_index = 0 },
    );
    try std.testing.expectError(
        error.OutOfMemory,
        store.reducePending(
            pending,
            owner_identity,
            failing.allocator(),
            chain.head,
            .{},
        ),
    );
    try std.testing.expectError(
        error.RequestConsumedOrUnknown,
        store.pendingRequestId(pending, owner_identity),
    );
}

test "request store deinit cleans staged and pending requests" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(TestSpec);
    const registry = report_registry.Registry.fromSpecs(.{TestSpec});
    var owner: u8 = 0;
    var store = try RequestStore.init(
        allocator,
        std.testing.io,
        registry,
        @ptrCast(&owner),
    );
    const query_value: TestQuery = .{ .bias = 1 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();

    _ = try store.stageRequest(query, .{});
    var staged = try store.stageRequest(query, .{});
    _ = try staged.commit(1);
    store.deinit();
}

test "custom report preparation recognizes all reserved built-in codes" {
    try std.testing.expect(isReservedBuiltInCode(10_001));
    try std.testing.expect(isReservedBuiltInCode(10_002));
    try std.testing.expect(isReservedBuiltInCode(10_003));
    try std.testing.expect(!isReservedBuiltInCode(TestSpec.code));
}

fn stageRequestAllocationCampaign(
    allocator: std.mem.Allocator,
    registry: *const report_registry.Registry,
    query: *const report_registry.QueryHandle,
) !void {
    var owner: u8 = 0;
    var store = try RequestStore.init(
        allocator,
        std.testing.io,
        registry,
        @ptrCast(&owner),
    );
    defer store.deinit();
    var staged = try store.stageRequest(query, .{});
    defer staged.deinit();
    try std.testing.expect((try staged.carrierWords()).len > 0);
}

test "custom report staging cleans every allocation failure" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(TestSpec);
    const registry = report_registry.Registry.fromSpecs(.{TestSpec});
    const query_value: TestQuery = .{ .bias = 1 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();

    try std.testing.checkAllAllocationFailures(
        allocator,
        stageRequestAllocationCampaign,
        .{ registry, query },
    );
}
