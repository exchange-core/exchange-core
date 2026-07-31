//! Misuse-resistant foundation for extensible report queries.
//!
//! Concrete reports remain ordinary Zig types validated by `Adapter(Spec)`.
//! Runtime dispatch and owned values cross the public boundary only as opaque
//! tokens. All erased pointers, allocators, destructors, callback tables, and
//! registry storage stay private to this module.
//!
//! Ownership and compatibility contracts:
//! - `Spec.cloneQuery` must deep-clone every query-owned allocation so the
//!   source and clone can be mutated and deinitialized independently.
//! - Adapter borrows never extend ownership: a borrowed query lives until its
//!   `QueryHandle` is deinitialized, section bytes live until their
//!   `SectionHandle` is deinitialized, and a borrowed result lives until its
//!   `ResultHandle` is taken or deinitialized.
//! - `Registry.cloneOwned` clones and revalidates the sorted descriptor table.
//!   Descriptor tokens themselves are immutable Adapter-owned statics and are
//!   intentionally shared by all registry copies.
//! - A report code is the routing identity and must be unique. The schema id
//!   and version are compatibility metadata covered by the registry digest;
//!   callers must bump the version whenever bytes or reducer semantics change.
//!   Dispatch does not negotiate schema versions at runtime.

const std = @import("std");

const Sha256 = std.crypto.hash.sha2.Sha256;

pub const schema_id_len: usize = 16;
pub const registry_digest_len: usize = Sha256.digest_length;

pub const ResourceError = std.mem.Allocator.Error || error{
    SystemResources,
};

pub const QueryConstructionError = ResourceError || error{
    InvalidQuery,
    ConstructorFailed,
};

pub const EncodingError = ResourceError || error{
    EncodingFailed,
};

pub const HandlerError = ResourceError || error{
    HandlerFailed,
};

pub const ReductionError = ResourceError || error{
    InvalidSection,
    ReducerFailed,
};

pub const ProcessError = ResourceError || error{
    EncodingFailed,
    HandlerFailed,
};

pub const ProcessDispatchError = ProcessError || error{
    ContextTypeMismatch,
};

pub const QueryDispatchError = ResourceError || error{
    QueryTypeMismatch,
};

pub const EncodeDispatchError = EncodingError || error{
    QueryTypeMismatch,
};

pub const ReductionDispatchError = ReductionError || error{
    QueryTypeMismatch,
};

pub const ExtractionError = error{
    ResultAlreadyTaken,
    ResultTypeMismatch,
};

pub const RegistryValidationError = error{
    DuplicateReportCode,
    RegistryTooLarge,
};

pub const RegistryInitError = std.mem.Allocator.Error || RegistryValidationError;

pub const ReservedCodeValidationError = error{
    ReservedReportCode,
};

pub const RegistryContextValidationError = error{
    ContextTypeMismatch,
};

/// Cycle-free typed view supplied to a matching-engine report callback.
pub fn MatchingContext(comptime View: type) type {
    return struct {
        view: *const View,
        shard_id: i32,
        shard_mask: u64,
    };
}

/// Cycle-free typed view supplied to a risk-engine report callback. Keeping a
/// distinct generic type prevents accidentally swapping callback contexts.
pub fn RiskContext(comptime View: type) type {
    return struct {
        view: *const View,
        shard_id: i32,
        shard_mask: u64,
    };
}

/// One reducer input after BINARY_EVENT carriers have been grouped by their
/// external Java section id. `padded_bytes` deliberately includes the final
/// carrier's zero padding; a report's section decoder defines its own boundary.
pub const SectionBytes = struct {
    section_id: i32,
    padded_bytes: []const u8,
};

/// Append-only writer exposed to a report specification. The concrete
/// allocating writer and its ownership operations remain private to Adapter.
pub const EncodingWriter = opaque {
    pub fn writeAll(
        self: *EncodingWriter,
        bytes: []const u8,
    ) ResourceError!void {
        encodingWriterImpl(self).writeAll(bytes) catch return error.OutOfMemory;
    }

    pub fn writeByte(
        self: *EncodingWriter,
        byte: u8,
    ) ResourceError!void {
        encodingWriterImpl(self).writeByte(byte) catch return error.OutOfMemory;
    }

    pub fn writeInt(
        self: *EncodingWriter,
        comptime T: type,
        value: T,
        endian: std.builtin.Endian,
    ) ResourceError!void {
        encodingWriterImpl(self).writeInt(T, value, endian) catch
            return error.OutOfMemory;
    }
};

/// Public type-erased descriptor token. Its fields and callback table are held
/// by the private `DescriptorImpl`; callers can only use checked methods.
pub const Descriptor = opaque {
    pub fn code(self: *const Descriptor) i32 {
        return descriptorImpl(self).code;
    }

    pub fn schemaId(self: *const Descriptor) [schema_id_len]u8 {
        return descriptorImpl(self).schema_id;
    }

    pub fn schemaVersion(self: *const Descriptor) u32 {
        return descriptorImpl(self).schema_version;
    }

    /// Checks the concrete matching view at comptime without exposing the
    /// erased callback representation.
    pub fn acceptsMatchingView(
        self: *const Descriptor,
        comptime View: type,
    ) bool {
        return descriptorImpl(self).matching_view_identity == typeIdentity(View);
    }

    /// Checks the concrete risk view at comptime without exposing the erased
    /// callback representation.
    pub fn acceptsRiskView(
        self: *const Descriptor,
        comptime View: type,
    ) bool {
        return descriptorImpl(self).risk_view_identity == typeIdentity(View);
    }

    pub fn cloneQuery(
        self: *const Descriptor,
        allocator: std.mem.Allocator,
        query: *const QueryHandle,
    ) QueryDispatchError!*QueryHandle {
        const impl = descriptorImpl(self);
        const box = queryBoxConst(query);
        if (box.descriptor != impl) return error.QueryTypeMismatch;

        const cloned = try impl.clone_query(allocator, box.value);
        std.debug.assert(queryBoxConst(cloned).descriptor == impl);
        return cloned;
    }

    /// Returns null only for a validation or constructor rejection. Resource
    /// failures always remain errors.
    pub fn decodeQuery(
        self: *const Descriptor,
        allocator: std.mem.Allocator,
        body: []const u8,
    ) ResourceError!?*QueryHandle {
        const impl = descriptorImpl(self);
        const decoded = try impl.decode_query(allocator, body);
        if (decoded) |query| {
            std.debug.assert(queryBoxConst(query).descriptor == impl);
        }
        return decoded;
    }

    pub fn encodeQuery(
        self: *const Descriptor,
        allocator: std.mem.Allocator,
        query: *const QueryHandle,
    ) EncodeDispatchError![]u8 {
        const impl = descriptorImpl(self);
        const box = queryBoxConst(query);
        if (box.descriptor != impl) return error.QueryTypeMismatch;
        return impl.encode_query(allocator, box.value);
    }

    /// Constructs a fresh query for this matching callback, mirroring Java's
    /// one-constructor-per-BinaryCommandsProcessor behavior.
    pub fn processMatchingBody(
        self: *const Descriptor,
        comptime View: type,
        allocator: std.mem.Allocator,
        body: []const u8,
        context: MatchingContext(View),
    ) ProcessDispatchError!ProcessOutcome {
        const impl = descriptorImpl(self);
        if (!self.acceptsMatchingView(View)) {
            return error.ContextTypeMismatch;
        }
        var query = (try impl.decode_query(allocator, body)) orelse
            return .constructor_rejected;
        defer query.deinit();

        std.debug.assert(queryBoxConst(query).descriptor == impl);
        const section = try impl.process_matching(
            allocator,
            queryBoxConst(query).value,
            context.view,
            context.shard_id,
            context.shard_mask,
        );
        if (section) |owned| {
            std.debug.assert(sectionBoxConst(owned).descriptor == impl);
            return .{ .section = owned };
        }
        return .no_section;
    }

    /// Constructs a fresh query for this risk callback, mirroring Java's
    /// one-constructor-per-BinaryCommandsProcessor behavior.
    pub fn processRiskBody(
        self: *const Descriptor,
        comptime View: type,
        allocator: std.mem.Allocator,
        body: []const u8,
        context: RiskContext(View),
    ) ProcessDispatchError!ProcessOutcome {
        const impl = descriptorImpl(self);
        if (!self.acceptsRiskView(View)) {
            return error.ContextTypeMismatch;
        }
        var query = (try impl.decode_query(allocator, body)) orelse
            return .constructor_rejected;
        defer query.deinit();

        std.debug.assert(queryBoxConst(query).descriptor == impl);
        const section = try impl.process_risk(
            allocator,
            queryBoxConst(query).value,
            context.view,
            context.shard_id,
            context.shard_mask,
        );
        if (section) |owned| {
            std.debug.assert(sectionBoxConst(owned).descriptor == impl);
            return .{ .section = owned };
        }
        return .no_section;
    }

    pub fn reduceSections(
        self: *const Descriptor,
        allocator: std.mem.Allocator,
        original_query: *const QueryHandle,
        sections: []const SectionBytes,
    ) ReductionDispatchError!*ResultHandle {
        const impl = descriptorImpl(self);
        const box = queryBoxConst(original_query);
        if (box.descriptor != impl) return error.QueryTypeMismatch;

        const result = try impl.reduce(allocator, box.value, sections);
        std.debug.assert(resultBoxConst(result).descriptor == impl);
        return result;
    }
};

/// Owned erased query token. Only Adapter can construct one; callers can inspect
/// it through the matching typed Adapter and release it here.
pub const QueryHandle = opaque {
    pub fn descriptor(self: *const QueryHandle) *const Descriptor {
        return descriptorToken(queryBoxConst(self).descriptor);
    }

    pub fn deinit(self: *QueryHandle) void {
        const box = queryBox(self);
        const allocator = box.allocator;
        box.destroy_value(allocator, box.value);
        allocator.destroy(box);
    }
};

/// Owned serialized report section. Its allocator and mutable backing slice are
/// private; `bytes` returns an immutable borrow valid only until `deinit`.
pub const SectionHandle = opaque {
    pub fn descriptor(self: *const SectionHandle) *const Descriptor {
        return descriptorToken(sectionBoxConst(self).descriptor);
    }

    pub fn bytes(self: *const SectionHandle) []const u8 {
        return sectionBoxConst(self).bytes;
    }

    pub fn deinit(self: *SectionHandle) void {
        const box = sectionBox(self);
        const allocator = box.allocator;
        allocator.free(box.bytes);
        allocator.destroy(box);
    }
};

/// Owned erased result token. A typed Adapter can borrow or move out its result.
/// A borrow is invalidated by either `takeResult` or `deinit`; the token remains
/// valid but empty after a successful take until it is deinitialized.
pub const ResultHandle = opaque {
    pub fn descriptor(self: *const ResultHandle) *const Descriptor {
        return descriptorToken(resultBoxConst(self).descriptor);
    }

    pub fn isTaken(self: *const ResultHandle) bool {
        return resultBoxConst(self).value == null;
    }

    pub fn deinit(self: *ResultHandle) void {
        const box = resultBox(self);
        const allocator = box.allocator;
        if (box.value) |value| {
            box.destroy_value(allocator, value);
        }
        allocator.destroy(box);
    }
};

/// Constructor rejection and a successful callback that elects not to emit a
/// section are observably different in Java and stay distinct here.
pub const ProcessOutcome = union(enum) {
    constructor_rejected,
    no_section,
    section: *SectionHandle,

    pub fn deinit(self: *ProcessOutcome) void {
        switch (self.*) {
            .section => |section| section.deinit(),
            .constructor_rejected, .no_section => {},
        }
        self.* = undefined;
    }
};

const DestroyErasedFn = *const fn (std.mem.Allocator, *anyopaque) void;

const QueryBox = struct {
    allocator: std.mem.Allocator,
    descriptor: *const DescriptorImpl,
    value: *anyopaque,
    destroy_value: DestroyErasedFn,
};

const SectionBox = struct {
    allocator: std.mem.Allocator,
    descriptor: *const DescriptorImpl,
    bytes: []u8,
};

const ResultBox = struct {
    allocator: std.mem.Allocator,
    descriptor: *const DescriptorImpl,
    value: ?*anyopaque,
    destroy_value: DestroyErasedFn,
};

const CloneQueryFn = *const fn (
    std.mem.Allocator,
    *const anyopaque,
) ResourceError!*QueryHandle;
const DecodeQueryFn = *const fn (
    std.mem.Allocator,
    []const u8,
) ResourceError!?*QueryHandle;
const EncodeQueryFn = *const fn (
    std.mem.Allocator,
    *const anyopaque,
) EncodingError![]u8;
const ProcessMatchingFn = *const fn (
    std.mem.Allocator,
    *const anyopaque,
    *const anyopaque,
    i32,
    u64,
) ProcessError!?*SectionHandle;
const ProcessRiskFn = *const fn (
    std.mem.Allocator,
    *const anyopaque,
    *const anyopaque,
    i32,
    u64,
) ProcessError!?*SectionHandle;
const ReduceFn = *const fn (
    std.mem.Allocator,
    *const anyopaque,
    []const SectionBytes,
) ReductionError!*ResultHandle;

const DescriptorImpl = struct {
    code: i32,
    schema_id: [schema_id_len]u8,
    schema_version: u32,
    matching_view_identity: *const anyopaque,
    risk_view_identity: *const anyopaque,

    clone_query: CloneQueryFn,
    decode_query: DecodeQueryFn,
    encode_query: EncodeQueryFn,
    process_matching: ProcessMatchingFn,
    process_risk: ProcessRiskFn,
    reduce: ReduceFn,
};

/// Adapts a concrete report specification to an opaque `Descriptor`.
///
/// A `Spec` must declare:
/// - `code: i32`, `schema_id: [16]u8`, and `schema_version: u32`
/// - `Query`, `Section`, `Result`, `MatchingView`, and `RiskView` types
/// - clone/deinit functions for `Query`
/// - writer-based encode and dedicated constructor functions for `Query`
/// - matching/risk processors returning `?Section`
/// - writer-based encode and deinit functions for `Section`
/// - reduce/deinit functions for `Result`
pub fn Adapter(comptime Spec: type) type {
    requireSpecDeclarations(Spec);

    const QueryType = Spec.Query;
    const SectionType = Spec.Section;
    const ResultType = Spec.Result;
    const MatchingViewType = Spec.MatchingView;
    const RiskViewType = Spec.RiskView;
    const report_code: i32 = Spec.code;
    const report_schema_id: [schema_id_len]u8 = Spec.schema_id;
    const report_schema_version: u32 = Spec.schema_version;

    comptime {
        _ = @as(
            *const fn (
                std.mem.Allocator,
                *const QueryType,
            ) ResourceError!QueryType,
            &Spec.cloneQuery,
        );
        _ = @as(
            *const fn (std.mem.Allocator, *QueryType) void,
            &Spec.deinitQuery,
        );
        _ = @as(
            *const fn (
                std.mem.Allocator,
                *EncodingWriter,
                *const QueryType,
            ) EncodingError!void,
            &Spec.encodeQuery,
        );
        _ = @as(
            *const fn (
                std.mem.Allocator,
                []const u8,
            ) QueryConstructionError!QueryType,
            &Spec.decodeQuery,
        );
        _ = @as(
            *const fn (
                std.mem.Allocator,
                *const QueryType,
                MatchingContext(MatchingViewType),
            ) HandlerError!?SectionType,
            &Spec.processMatching,
        );
        _ = @as(
            *const fn (
                std.mem.Allocator,
                *const QueryType,
                RiskContext(RiskViewType),
            ) HandlerError!?SectionType,
            &Spec.processRisk,
        );
        _ = @as(
            *const fn (
                std.mem.Allocator,
                *EncodingWriter,
                *const SectionType,
            ) EncodingError!void,
            &Spec.encodeSection,
        );
        _ = @as(
            *const fn (std.mem.Allocator, *SectionType) void,
            &Spec.deinitSection,
        );
        _ = @as(
            *const fn (
                std.mem.Allocator,
                *const QueryType,
                []const SectionBytes,
            ) ReductionError!ResultType,
            &Spec.reduce,
        );
        _ = @as(
            *const fn (std.mem.Allocator, *ResultType) void,
            &Spec.deinitResult,
        );
    }

    return struct {
        const Self = @This();

        pub const Query = QueryType;
        pub const Section = SectionType;
        pub const Result = ResultType;

        const descriptor_impl: DescriptorImpl = .{
            .code = report_code,
            .schema_id = report_schema_id,
            .schema_version = report_schema_version,
            .matching_view_identity = typeIdentity(MatchingViewType),
            .risk_view_identity = typeIdentity(RiskViewType),
            .clone_query = cloneQueryErased,
            .decode_query = decodeQueryErased,
            .encode_query = encodeQueryErased,
            .process_matching = processMatchingErased,
            .process_risk = processRiskErased,
            .reduce = reduceErased,
        };

        pub fn descriptorPtr() *const Descriptor {
            return descriptorToken(&descriptor_impl);
        }

        pub fn cloneQuery(
            allocator: std.mem.Allocator,
            query: *const Query,
        ) ResourceError!*QueryHandle {
            return cloneQueryErased(allocator, query);
        }

        /// Returns null only for validation or constructor rejection.
        pub fn decodeQuery(
            allocator: std.mem.Allocator,
            body: []const u8,
        ) ResourceError!?*QueryHandle {
            return decodeQueryErased(allocator, body);
        }

        /// Returns an immutable borrow valid only until the handle is
        /// deinitialized.
        pub fn borrowQuery(
            query: *const QueryHandle,
        ) error{QueryTypeMismatch}!*const Query {
            const box = queryBoxConst(query);
            if (box.descriptor != &descriptor_impl) {
                return error.QueryTypeMismatch;
            }
            return @ptrCast(@alignCast(box.value));
        }

        pub fn encodeQuery(
            allocator: std.mem.Allocator,
            query: *const Query,
        ) EncodingError![]u8 {
            return encodeQueryValue(allocator, query);
        }

        pub fn processMatching(
            allocator: std.mem.Allocator,
            query: *const Query,
            context: MatchingContext(MatchingViewType),
        ) ProcessError!?*SectionHandle {
            return processMatchingErased(
                allocator,
                query,
                context.view,
                context.shard_id,
                context.shard_mask,
            );
        }

        pub fn processRisk(
            allocator: std.mem.Allocator,
            query: *const Query,
            context: RiskContext(RiskViewType),
        ) ProcessError!?*SectionHandle {
            return processRiskErased(
                allocator,
                query,
                context.view,
                context.shard_id,
                context.shard_mask,
            );
        }

        pub fn reduce(
            allocator: std.mem.Allocator,
            query: *const Query,
            sections: []const SectionBytes,
        ) ReductionError!*ResultHandle {
            return reduceErased(allocator, query, sections);
        }

        /// Returns an immutable borrow valid only until the handle is taken or
        /// deinitialized.
        pub fn borrowResult(
            result: *const ResultHandle,
        ) ExtractionError!*const Result {
            const box = resultBoxConst(result);
            if (box.descriptor != &descriptor_impl) {
                return error.ResultTypeMismatch;
            }
            const value = box.value orelse return error.ResultAlreadyTaken;
            return @ptrCast(@alignCast(value));
        }

        /// Moves the typed value out while retaining an empty opaque token that
        /// must still be deinitialized. The returned value is caller-owned and
        /// must be released with `deinitTakenResult` using the allocator passed
        /// to the reducer that produced this handle.
        pub fn takeResult(
            result: *ResultHandle,
        ) ExtractionError!Result {
            const box = resultBox(result);
            if (box.descriptor != &descriptor_impl) {
                return error.ResultTypeMismatch;
            }
            const raw_value = box.value orelse return error.ResultAlreadyTaken;

            const typed: *Result = @ptrCast(@alignCast(raw_value));
            const moved = typed.*;
            box.allocator.destroy(typed);
            box.value = null;
            return moved;
        }

        /// Releases a result previously returned by `takeResult` without
        /// requiring callers to retain or expose the private report Spec.
        pub fn deinitTakenResult(
            allocator: std.mem.Allocator,
            result: *Result,
        ) void {
            Spec.deinitResult(allocator, result);
        }

        fn cloneQueryErased(
            allocator: std.mem.Allocator,
            raw_query: *const anyopaque,
        ) ResourceError!*QueryHandle {
            const source: *const Query = @ptrCast(@alignCast(raw_query));
            var cloned = try Spec.cloneQuery(allocator, source);
            errdefer Spec.deinitQuery(allocator, &cloned);
            return ownQueryValue(allocator, &cloned);
        }

        fn decodeQueryErased(
            allocator: std.mem.Allocator,
            body: []const u8,
        ) ResourceError!?*QueryHandle {
            var decoded = Spec.decodeQuery(allocator, body) catch |err| switch (err) {
                error.InvalidQuery, error.ConstructorFailed => return null,
                error.OutOfMemory => return error.OutOfMemory,
                error.SystemResources => return error.SystemResources,
            };
            errdefer Spec.deinitQuery(allocator, &decoded);
            return try ownQueryValue(allocator, &decoded);
        }

        fn ownQueryValue(
            allocator: std.mem.Allocator,
            value: *Query,
        ) ResourceError!*QueryHandle {
            const stored = try allocator.create(Query);
            errdefer allocator.destroy(stored);
            stored.* = value.*;

            const box = try allocator.create(QueryBox);
            box.* = .{
                .allocator = allocator,
                .descriptor = &descriptor_impl,
                .value = stored,
                .destroy_value = destroyQueryErased,
            };
            value.* = undefined;
            return queryToken(box);
        }

        fn destroyQueryErased(
            allocator: std.mem.Allocator,
            raw_query: *anyopaque,
        ) void {
            const query: *Query = @ptrCast(@alignCast(raw_query));
            Spec.deinitQuery(allocator, query);
            allocator.destroy(query);
        }

        fn encodeQueryValue(
            allocator: std.mem.Allocator,
            query: *const Query,
        ) EncodingError![]u8 {
            var output: std.Io.Writer.Allocating = .init(allocator);
            errdefer output.deinit();
            try Spec.encodeQuery(
                allocator,
                encodingWriterToken(&output.writer),
                query,
            );
            return output.toOwnedSlice();
        }

        fn encodeQueryErased(
            allocator: std.mem.Allocator,
            raw_query: *const anyopaque,
        ) EncodingError![]u8 {
            const query: *const Query = @ptrCast(@alignCast(raw_query));
            return encodeQueryValue(allocator, query);
        }

        fn encodeSectionValue(
            allocator: std.mem.Allocator,
            section: *const Section,
        ) EncodingError![]u8 {
            var output: std.Io.Writer.Allocating = .init(allocator);
            errdefer output.deinit();
            try Spec.encodeSection(
                allocator,
                encodingWriterToken(&output.writer),
                section,
            );
            return output.toOwnedSlice();
        }

        fn processMatchingErased(
            allocator: std.mem.Allocator,
            raw_query: *const anyopaque,
            raw_view: *const anyopaque,
            shard_id: i32,
            shard_mask: u64,
        ) ProcessError!?*SectionHandle {
            const query: *const Query = @ptrCast(@alignCast(raw_query));
            const view: *const MatchingViewType = @ptrCast(@alignCast(raw_view));
            var section = (try Spec.processMatching(allocator, query, .{
                .view = view,
                .shard_id = shard_id,
                .shard_mask = shard_mask,
            })) orelse
                return null;
            defer Spec.deinitSection(allocator, &section);

            const body = try encodeSectionValue(allocator, &section);
            return ownSectionBytes(allocator, &descriptor_impl, body);
        }

        fn processRiskErased(
            allocator: std.mem.Allocator,
            raw_query: *const anyopaque,
            raw_view: *const anyopaque,
            shard_id: i32,
            shard_mask: u64,
        ) ProcessError!?*SectionHandle {
            const query: *const Query = @ptrCast(@alignCast(raw_query));
            const view: *const RiskViewType = @ptrCast(@alignCast(raw_view));
            var section = (try Spec.processRisk(allocator, query, .{
                .view = view,
                .shard_id = shard_id,
                .shard_mask = shard_mask,
            })) orelse
                return null;
            defer Spec.deinitSection(allocator, &section);

            const body = try encodeSectionValue(allocator, &section);
            return ownSectionBytes(allocator, &descriptor_impl, body);
        }

        fn reduceErased(
            allocator: std.mem.Allocator,
            raw_query: *const anyopaque,
            sections: []const SectionBytes,
        ) ReductionError!*ResultHandle {
            const query: *const Query = @ptrCast(@alignCast(raw_query));
            var result = try Spec.reduce(allocator, query, sections);
            errdefer Spec.deinitResult(allocator, &result);
            return ownResultValue(allocator, &result);
        }

        fn ownResultValue(
            allocator: std.mem.Allocator,
            value: *Result,
        ) ResourceError!*ResultHandle {
            const stored = try allocator.create(Result);
            errdefer allocator.destroy(stored);
            stored.* = value.*;

            const box = try allocator.create(ResultBox);
            box.* = .{
                .allocator = allocator,
                .descriptor = &descriptor_impl,
                .value = stored,
                .destroy_value = destroyResultErased,
            };
            value.* = undefined;
            return resultToken(box);
        }

        fn destroyResultErased(
            allocator: std.mem.Allocator,
            raw_result: *anyopaque,
        ) void {
            const result: *Result = @ptrCast(@alignCast(raw_result));
            Spec.deinitResult(allocator, result);
            allocator.destroy(result);
        }
    };
}

/// Opaque immutable registry token. Static registries are compile-time sorted;
/// runtime registries copy, sort, and validate their input before publication.
/// Registries own only their descriptor table; immutable descriptors remain
/// Adapter-owned statics and may safely be shared between registry lifetimes.
pub const Registry = opaque {
    pub fn fromSpecs(comptime specs: anytype) *const Registry {
        const Storage = RegistryStorage(specs);
        return registryToken(&Storage.impl);
    }

    pub fn initValidated(
        allocator: std.mem.Allocator,
        descriptors: []const *const Descriptor,
    ) RegistryInitError!*Registry {
        if (descriptors.len > std.math.maxInt(u32)) {
            return error.RegistryTooLarge;
        }

        const owned = try allocator.dupe(*const Descriptor, descriptors);
        errdefer allocator.free(owned);
        std.sort.heap(*const Descriptor, owned, {}, descriptorLessThan);
        if (adjacentDuplicateCode(owned) != null) {
            return error.DuplicateReportCode;
        }

        const impl = try allocator.create(RegistryImpl);
        impl.* = .{
            .allocator = allocator,
            .owned_descriptors = owned,
            .descriptors = owned,
        };
        return registryTokenMut(impl);
    }

    /// Returns an independently owned, sorted, and validated descriptor table.
    /// The clone remains usable after an owned source registry is deinitialized;
    /// immutable descriptor tokens are shared rather than duplicated.
    pub fn cloneOwned(
        self: *const Registry,
        allocator: std.mem.Allocator,
    ) RegistryInitError!*Registry {
        return initValidated(allocator, registryImpl(self).descriptors);
    }

    /// Only registries returned by `initValidated` or `cloneOwned` have a
    /// mutable token and may be deinitialized.
    pub fn deinit(self: *Registry) void {
        const impl = registryImplMut(self);
        const allocator = impl.allocator orelse unreachable;
        const owned = impl.owned_descriptors orelse unreachable;
        allocator.free(owned);
        allocator.destroy(impl);
    }

    pub fn len(self: *const Registry) usize {
        return registryImpl(self).descriptors.len;
    }

    pub fn descriptorAt(
        self: *const Registry,
        index: usize,
    ) ?*const Descriptor {
        const descriptors = registryImpl(self).descriptors;
        if (index >= descriptors.len) return null;
        return descriptors[index];
    }

    pub fn digest(self: *const Registry) [registry_digest_len]u8 {
        return digestDescriptors(registryImpl(self).descriptors);
    }

    pub fn lookup(
        self: *const Registry,
        code: i32,
    ) ?*const Descriptor {
        const descriptors = registryImpl(self).descriptors;
        var low: usize = 0;
        var high: usize = descriptors.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            const descriptor = descriptors[mid];
            const candidate_code = descriptor.code();
            if (candidate_code < code) {
                low = mid + 1;
            } else if (candidate_code > code) {
                high = mid;
            } else {
                return descriptor;
            }
        }
        return null;
    }

    /// Tests exact descriptor token identity. Equal report codes or equal
    /// schema metadata never make a different Adapter descriptor a member.
    pub fn containsDescriptor(
        self: *const Registry,
        descriptor: *const Descriptor,
    ) bool {
        for (registryImpl(self).descriptors) |candidate| {
            if (candidate == descriptor) return true;
        }
        return false;
    }

    /// Validates all callback view types before any erased runtime dispatch.
    /// Matching views are checked before risk views for stable failure order.
    pub fn validateContextTypes(
        self: *const Registry,
        comptime MatchingView: type,
        comptime RiskView: type,
    ) RegistryContextValidationError!void {
        for (registryImpl(self).descriptors) |descriptor| {
            if (!descriptor.acceptsMatchingView(MatchingView)) {
                return error.ContextTypeMismatch;
            }
        }
        for (registryImpl(self).descriptors) |descriptor| {
            if (!descriptor.acceptsRiskView(RiskView)) {
                return error.ContextTypeMismatch;
            }
        }
    }

    /// Returns the numerically smallest registered code present in the
    /// caller-supplied reserved set, independent of either input order.
    pub fn smallestReservedCode(
        self: *const Registry,
        reserved_codes: []const i32,
    ) ?i32 {
        return smallestReservedCodeIn(
            registryImpl(self).descriptors,
            reserved_codes,
        );
    }

    /// Rejects any collision with caller-owned reserved codes. Built-in report
    /// codes stay outside this module and must be supplied explicitly.
    pub fn validateNoReservedCodes(
        self: *const Registry,
        reserved_codes: []const i32,
    ) ReservedCodeValidationError!void {
        if (self.smallestReservedCode(reserved_codes) != null) {
            return error.ReservedReportCode;
        }
    }

    pub fn validateUniqueCodes(
        descriptors: []const *const Descriptor,
    ) RegistryValidationError!void {
        if (descriptors.len > std.math.maxInt(u32)) {
            return error.RegistryTooLarge;
        }
        if (smallestDuplicateCode(descriptors) != null) {
            return error.DuplicateReportCode;
        }
    }

    /// Returns the numerically smallest duplicate, independent of registration
    /// order, so validation diagnostics remain deterministic.
    pub fn smallestDuplicateCode(
        descriptors: []const *const Descriptor,
    ) ?i32 {
        var duplicate: ?i32 = null;
        for (descriptors, 0..) |left, left_index| {
            for (descriptors[left_index + 1 ..]) |right| {
                if (left.code() != right.code()) continue;
                if (duplicate == null or left.code() < duplicate.?) {
                    duplicate = left.code();
                }
            }
        }
        return duplicate;
    }
};

const RegistryImpl = struct {
    allocator: ?std.mem.Allocator,
    owned_descriptors: ?[]*const Descriptor,
    descriptors: []const *const Descriptor,
};

fn TypeIdentity(comptime T: type) type {
    return struct {
        const represented_type = T;
        var byte: u8 = 0;
    };
}

fn typeIdentity(comptime T: type) *const anyopaque {
    return &TypeIdentity(T).byte;
}

fn encodingWriterToken(writer: *std.Io.Writer) *EncodingWriter {
    return @ptrCast(writer);
}

fn encodingWriterImpl(writer: *EncodingWriter) *std.Io.Writer {
    return @ptrCast(@alignCast(writer));
}

fn descriptorToken(impl: *const DescriptorImpl) *const Descriptor {
    return @ptrCast(impl);
}

fn descriptorImpl(descriptor: *const Descriptor) *const DescriptorImpl {
    return @ptrCast(@alignCast(descriptor));
}

fn queryToken(box: *QueryBox) *QueryHandle {
    return @ptrCast(box);
}

fn queryBox(query: *QueryHandle) *QueryBox {
    return @ptrCast(@alignCast(query));
}

fn queryBoxConst(query: *const QueryHandle) *const QueryBox {
    return @ptrCast(@alignCast(query));
}

fn sectionToken(box: *SectionBox) *SectionHandle {
    return @ptrCast(box);
}

fn sectionBox(section: *SectionHandle) *SectionBox {
    return @ptrCast(@alignCast(section));
}

fn sectionBoxConst(section: *const SectionHandle) *const SectionBox {
    return @ptrCast(@alignCast(section));
}

fn resultToken(box: *ResultBox) *ResultHandle {
    return @ptrCast(box);
}

fn resultBox(result: *ResultHandle) *ResultBox {
    return @ptrCast(@alignCast(result));
}

fn resultBoxConst(result: *const ResultHandle) *const ResultBox {
    return @ptrCast(@alignCast(result));
}

fn registryToken(impl: *const RegistryImpl) *const Registry {
    return @ptrCast(impl);
}

fn registryTokenMut(impl: *RegistryImpl) *Registry {
    return @ptrCast(impl);
}

fn registryImpl(registry: *const Registry) *const RegistryImpl {
    return @ptrCast(@alignCast(registry));
}

fn registryImplMut(registry: *Registry) *RegistryImpl {
    return @ptrCast(@alignCast(registry));
}

fn ownSectionBytes(
    allocator: std.mem.Allocator,
    descriptor: *const DescriptorImpl,
    bytes: []u8,
) ResourceError!*SectionHandle {
    const box = allocator.create(SectionBox) catch |err| {
        allocator.free(bytes);
        return err;
    };
    box.* = .{
        .allocator = allocator,
        .descriptor = descriptor,
        .bytes = bytes,
    };
    return sectionToken(box);
}

fn requireSpecDeclarations(comptime Spec: type) void {
    inline for (.{
        "code",
        "schema_id",
        "schema_version",
        "Query",
        "Section",
        "Result",
        "MatchingView",
        "RiskView",
        "cloneQuery",
        "deinitQuery",
        "encodeQuery",
        "decodeQuery",
        "processMatching",
        "processRisk",
        "encodeSection",
        "deinitSection",
        "reduce",
        "deinitResult",
    }) |name| {
        if (!@hasDecl(Spec, name)) {
            @compileError("report Spec is missing required declaration `" ++ name ++ "`");
        }
    }
}

fn RegistryStorage(comptime specs: anytype) type {
    return struct {
        pub const descriptors = buildDescriptorArray(specs);
        pub const impl: RegistryImpl = .{
            .allocator = null,
            .owned_descriptors = null,
            .descriptors = &descriptors,
        };
    };
}

fn buildDescriptorArray(
    comptime specs: anytype,
) [specs.len]*const Descriptor {
    if (specs.len > std.math.maxInt(u32)) {
        @compileError("report registry exceeds the supported descriptor count");
    }

    var descriptors: [specs.len]*const Descriptor = undefined;
    inline for (specs, 0..) |Spec, index| {
        descriptors[index] = Adapter(Spec).descriptorPtr();
    }

    var index: usize = 1;
    while (index < descriptors.len) : (index += 1) {
        const candidate = descriptors[index];
        var cursor = index;
        while (cursor > 0 and
            descriptors[cursor - 1].code() > candidate.code())
        {
            descriptors[cursor] = descriptors[cursor - 1];
            cursor -= 1;
        }
        descriptors[cursor] = candidate;
    }

    index = 1;
    while (index < descriptors.len) : (index += 1) {
        if (descriptors[index - 1].code() == descriptors[index].code()) {
            @compileError("duplicate report code in Registry.fromSpecs");
        }
    }
    return descriptors;
}

fn descriptorLessThan(
    _: void,
    left: *const Descriptor,
    right: *const Descriptor,
) bool {
    return left.code() < right.code();
}

fn adjacentDuplicateCode(
    descriptors: []const *const Descriptor,
) ?i32 {
    if (descriptors.len < 2) return null;
    for (descriptors[1..], 1..) |descriptor, index| {
        if (descriptors[index - 1].code() == descriptor.code()) {
            return descriptor.code();
        }
    }
    return null;
}

fn smallestReservedCodeIn(
    descriptors: []const *const Descriptor,
    reserved_codes: []const i32,
) ?i32 {
    var collision: ?i32 = null;
    for (descriptors) |descriptor| {
        const code = descriptor.code();
        for (reserved_codes) |reserved_code| {
            if (code != reserved_code) continue;
            if (collision == null or code < collision.?) {
                collision = code;
            }
        }
    }
    return collision;
}

fn digestDescriptors(
    descriptors: []const *const Descriptor,
) [registry_digest_len]u8 {
    var hasher = Sha256.init(.{});
    hasher.update("exchange-core/report-registry/v1");
    hashInt(u32, &hasher, @intCast(descriptors.len));
    for (descriptors) |descriptor| {
        hashInt(i32, &hasher, descriptor.code());
        const schema_id = descriptor.schemaId();
        hasher.update(&schema_id);
        hashInt(u32, &hasher, descriptor.schemaVersion());
    }
    var digest: [registry_digest_len]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn hashInt(
    comptime T: type,
    hasher: *Sha256,
    value: T,
) void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .big);
    hasher.update(&bytes);
}

const TestQuery = struct {
    value: i64,
    reducer_bias: i64,
    marker: ?[]u8 = null,
};

const TestSection = struct {
    value: i64,
    marker: []u8,
};

const TestResult = struct {
    value: i64,
    marker: []u8,
};

const TestMatchingView = struct {
    bias: i64 = 0,
};

const TestRiskView = struct {
    bias: i64 = 0,
};

const WrongMatchingView = struct {};
const WrongRiskView = struct {};

const test_matching_view: TestMatchingView = .{};
const test_risk_view: TestRiskView = .{};
const wrong_matching_view: WrongMatchingView = .{};

const system_resource_body = [_]u8{0xee} ** (2 * @sizeOf(i64));
const constructor_failure_body = [_]u8{0xdd} ** (2 * @sizeOf(i64));

fn TestSpecWith(
    comptime report_code: i32,
    comptime version: u32,
    comptime identity_tag: u8,
) type {
    return struct {
        const test_identity_tag: u8 = identity_tag;

        pub const code: i32 = report_code;
        pub const schema_id: [schema_id_len]u8 = [_]u8{0x52} ** schema_id_len;
        pub const schema_version: u32 = version;
        pub const Query = TestQuery;
        pub const Section = TestSection;
        pub const Result = TestResult;
        pub const MatchingView = TestMatchingView;
        pub const RiskView = TestRiskView;

        pub fn cloneQuery(
            allocator: std.mem.Allocator,
            query: *const Query,
        ) ResourceError!Query {
            const marker = try allocator.alloc(u8, 1);
            marker[0] = 0xc1;
            return .{
                .value = query.value,
                .reducer_bias = query.reducer_bias,
                .marker = marker,
            };
        }

        pub fn deinitQuery(allocator: std.mem.Allocator, query: *Query) void {
            if (query.marker) |marker| allocator.free(marker);
            query.* = undefined;
        }

        pub fn encodeQuery(
            _: std.mem.Allocator,
            writer: *EncodingWriter,
            query: *const Query,
        ) EncodingError!void {
            try writer.writeInt(i64, query.value, .little);
            try writer.writeInt(i64, query.reducer_bias, .little);
        }

        pub fn decodeQuery(
            allocator: std.mem.Allocator,
            body: []const u8,
        ) QueryConstructionError!Query {
            if (body.len != 2 * @sizeOf(i64)) return error.InvalidQuery;
            if (std.mem.eql(u8, body, &system_resource_body)) {
                return error.SystemResources;
            }
            if (std.mem.eql(u8, body, &constructor_failure_body)) {
                return error.ConstructorFailed;
            }

            const marker = try allocator.alloc(u8, 1);
            marker[0] = 0xd1;
            return .{
                .value = std.mem.readInt(i64, body[0..@sizeOf(i64)], .little),
                .reducer_bias = std.mem.readInt(
                    i64,
                    body[@sizeOf(i64)..][0..@sizeOf(i64)],
                    .little,
                ),
                .marker = marker,
            };
        }

        pub fn processMatching(
            allocator: std.mem.Allocator,
            query: *const Query,
            context: MatchingContext(MatchingView),
        ) HandlerError!?Section {
            if (context.shard_id < 0) return null;
            const marker = try allocator.alloc(u8, 1);
            errdefer allocator.free(marker);
            marker[0] = 0xe1;
            if (context.shard_id == 999) return error.HandlerFailed;
            return .{
                .value = query.value +% context.view.bias +%
                    @as(i64, context.shard_id),
                .marker = marker,
            };
        }

        pub fn processRisk(
            allocator: std.mem.Allocator,
            query: *const Query,
            context: RiskContext(RiskView),
        ) HandlerError!?Section {
            if (context.shard_id < 0) return null;
            const marker = try allocator.alloc(u8, 1);
            marker[0] = 0xe2;
            return .{
                .value = query.value +% context.view.bias +% 100 +%
                    @as(i64, context.shard_id),
                .marker = marker,
            };
        }

        pub fn encodeSection(
            _: std.mem.Allocator,
            writer: *EncodingWriter,
            section: *const Section,
        ) EncodingError!void {
            try writer.writeInt(i64, section.value, .little);
        }

        pub fn deinitSection(
            allocator: std.mem.Allocator,
            section: *Section,
        ) void {
            allocator.free(section.marker);
            section.* = undefined;
        }

        pub fn reduce(
            allocator: std.mem.Allocator,
            query: *const Query,
            sections: []const SectionBytes,
        ) ReductionError!Result {
            var total = query.reducer_bias;
            for (sections) |section| {
                if (section.padded_bytes.len < @sizeOf(i64)) {
                    return error.InvalidSection;
                }
                total +%= std.mem.readInt(
                    i64,
                    section.padded_bytes[0..@sizeOf(i64)],
                    .little,
                );
            }
            const marker = try allocator.alloc(u8, 1);
            marker[0] = 0xf1;
            return .{
                .value = total,
                .marker = marker,
            };
        }

        pub fn deinitResult(
            allocator: std.mem.Allocator,
            result: *Result,
        ) void {
            allocator.free(result.marker);
            result.* = undefined;
        }
    };
}

const TestSpec = TestSpecWith(42_001, 1, 1);
const OtherSpec = TestSpecWith(42_002, 1, 2);
const TestSpecV2 = TestSpecWith(42_001, 2, 3);
const SameMetadataSpec = TestSpecWith(42_001, 1, 4);

test "public dispatch and ownership tokens are opaque" {
    comptime {
        for (.{
            Descriptor,
            EncodingWriter,
            QueryHandle,
            SectionHandle,
            ResultHandle,
            Registry,
        }) |Token| {
            switch (@typeInfo(Token)) {
                .@"opaque" => {},
                else => @compileError("public registry token must remain opaque"),
            }
        }
    }
}

test "adapter owns opaque query section and result lifecycles" {
    const allocator = std.testing.allocator;
    const Adapted = Adapter(TestSpec);
    const descriptor = Adapted.descriptorPtr();
    const source: TestQuery = .{
        .value = 40,
        .reducer_bias = 2,
    };

    const body = try Adapted.encodeQuery(allocator, &source);
    defer allocator.free(body);

    var matching = try descriptor.processMatchingBody(
        TestMatchingView,
        allocator,
        body,
        .{
            .view = &test_matching_view,
            .shard_id = 3,
            .shard_mask = 3,
        },
    );
    defer matching.deinit();
    const section = switch (matching) {
        .section => |owned| owned,
        else => return error.ExpectedReportSection,
    };
    try std.testing.expect(section.descriptor() == descriptor);
    const section_body = section.bytes();
    try std.testing.expectEqual(
        @as(i64, 43),
        std.mem.readInt(i64, section_body[0..@sizeOf(i64)], .little),
    );

    const reducer_query = try Adapted.cloneQuery(allocator, &source);
    defer reducer_query.deinit();
    const borrowed_query = try Adapted.borrowQuery(reducer_query);
    try std.testing.expectEqual(source.value, borrowed_query.value);

    const erased_result = try descriptor.reduceSections(
        allocator,
        reducer_query,
        &.{.{ .section_id = 1_024, .padded_bytes = section_body }},
    );
    defer erased_result.deinit();

    const borrowed_result = try Adapted.borrowResult(erased_result);
    try std.testing.expectEqual(@as(i64, 45), borrowed_result.value);
    var result = try Adapted.takeResult(erased_result);
    defer Adapted.deinitTakenResult(allocator, &result);
    try std.testing.expect(erased_result.isTaken());
    try std.testing.expectEqual(@as(i64, 45), result.value);
}

test "constructor rejection is distinct from no section and resource failure" {
    const allocator = std.testing.allocator;
    const descriptor = Adapter(TestSpec).descriptorPtr();

    var invalid = try descriptor.processMatchingBody(
        TestMatchingView,
        allocator,
        &.{ 1, 2, 3 },
        .{
            .view = &test_matching_view,
            .shard_id = 0,
            .shard_mask = 0,
        },
    );
    defer invalid.deinit();
    switch (invalid) {
        .constructor_rejected => {},
        else => return error.ExpectedConstructorRejection,
    }

    var failed_constructor = try descriptor.processMatchingBody(
        TestMatchingView,
        allocator,
        &constructor_failure_body,
        .{
            .view = &test_matching_view,
            .shard_id = 0,
            .shard_mask = 0,
        },
    );
    defer failed_constructor.deinit();
    switch (failed_constructor) {
        .constructor_rejected => {},
        else => return error.ExpectedConstructorRejection,
    }

    try std.testing.expectError(
        error.SystemResources,
        descriptor.processMatchingBody(
            TestMatchingView,
            allocator,
            &system_resource_body,
            .{
                .view = &test_matching_view,
                .shard_id = 0,
                .shard_mask = 0,
            },
        ),
    );

    const source: TestQuery = .{ .value = 7, .reducer_bias = 0 };
    const body = try Adapter(TestSpec).encodeQuery(allocator, &source);
    defer allocator.free(body);

    var failing = std.testing.FailingAllocator.init(
        allocator,
        .{ .fail_index = 0 },
    );
    try std.testing.expectError(
        error.OutOfMemory,
        descriptor.processMatchingBody(
            TestMatchingView,
            failing.allocator(),
            body,
            .{
                .view = &test_matching_view,
                .shard_id = 0,
                .shard_mask = 0,
            },
        ),
    );

    var omitted = try descriptor.processRiskBody(
        TestRiskView,
        allocator,
        body,
        .{
            .view = &test_risk_view,
            .shard_id = -1,
            .shard_mask = 0,
        },
    );
    defer omitted.deinit();
    switch (omitted) {
        .no_section => {},
        else => return error.ExpectedNoSection,
    }
}

test "identity-checked entry points reject cross-descriptor handles" {
    const allocator = std.testing.allocator;
    const source: TestQuery = .{ .value = 10, .reducer_bias = 5 };
    const query = try Adapter(TestSpec).cloneQuery(allocator, &source);
    defer query.deinit();

    try std.testing.expectError(
        error.ContextTypeMismatch,
        Adapter(TestSpec).descriptorPtr().processMatchingBody(
            WrongMatchingView,
            allocator,
            &.{},
            .{
                .view = &wrong_matching_view,
                .shard_id = 0,
                .shard_mask = 0,
            },
        ),
    );
    try std.testing.expectError(
        error.QueryTypeMismatch,
        Adapter(OtherSpec).borrowQuery(query),
    );
    try std.testing.expectError(
        error.QueryTypeMismatch,
        Adapter(OtherSpec).descriptorPtr().cloneQuery(allocator, query),
    );
    try std.testing.expectError(
        error.QueryTypeMismatch,
        Adapter(OtherSpec).descriptorPtr().encodeQuery(allocator, query),
    );
    try std.testing.expectError(
        error.QueryTypeMismatch,
        Adapter(OtherSpec).descriptorPtr().reduceSections(
            allocator,
            query,
            &.{},
        ),
    );

    const result = try Adapter(TestSpec).descriptorPtr().reduceSections(
        allocator,
        query,
        &.{},
    );
    defer result.deinit();
    try std.testing.expectError(
        error.ResultTypeMismatch,
        Adapter(OtherSpec).borrowResult(result),
    );
    try std.testing.expectError(
        error.ResultTypeMismatch,
        Adapter(OtherSpec).takeResult(result),
    );

    var typed = try Adapter(TestSpec).takeResult(result);
    defer Adapter(TestSpec).deinitTakenResult(allocator, &typed);
    try std.testing.expectError(
        error.ResultAlreadyTaken,
        Adapter(TestSpec).borrowResult(result),
    );
    try std.testing.expectError(
        error.ResultAlreadyTaken,
        Adapter(TestSpec).takeResult(result),
    );
    try std.testing.expectError(
        error.ResultTypeMismatch,
        Adapter(OtherSpec).borrowResult(result),
    );
    try std.testing.expectError(
        error.ResultTypeMismatch,
        Adapter(OtherSpec).takeResult(result),
    );
}

test "adapter writer always returns independently owned encoding" {
    const allocator = std.testing.allocator;
    const source: TestQuery = .{ .value = 123, .reducer_bias = 456 };

    const first = try Adapter(TestSpec).encodeQuery(allocator, &source);
    defer allocator.free(first);
    const second = try Adapter(TestSpec).encodeQuery(allocator, &source);
    defer allocator.free(second);

    try std.testing.expect(first.ptr != second.ptr);
    try std.testing.expectEqualSlices(u8, first, second);
    first[0] +%= 1;
    try std.testing.expect(first[0] != second[0]);

    const decoded = (try Adapter(TestSpec).decodeQuery(allocator, second)) orelse
        return error.ExpectedDecodedQuery;
    defer decoded.deinit();
    const typed = try Adapter(TestSpec).borrowQuery(decoded);
    try std.testing.expectEqual(source.value, typed.value);
    try std.testing.expectEqual(source.reducer_bias, typed.reducer_bias);
}

test "adapter releases a taken result without exposing its Spec" {
    const allocator = std.testing.allocator;
    const Adapted = Adapter(TestSpec);
    const source: TestQuery = .{ .value = 11, .reducer_bias = 7 };

    const erased = try Adapted.reduce(allocator, &source, &.{});
    defer erased.deinit();
    var taken = try Adapted.takeResult(erased);
    try std.testing.expect(erased.isTaken());
    try std.testing.expectEqual(@as(i64, 7), taken.value);
    Adapted.deinitTakenResult(allocator, &taken);
}

test "validated runtime registry sorts input and rejects duplicates" {
    const allocator = std.testing.allocator;
    const a = Adapter(TestSpec).descriptorPtr();
    const b = Adapter(OtherSpec).descriptorPtr();

    var runtime = try Registry.initValidated(allocator, &.{ b, a });
    defer runtime.deinit();
    try std.testing.expectEqual(@as(usize, 2), runtime.len());
    try std.testing.expectEqual(TestSpec.code, runtime.descriptorAt(0).?.code());
    try std.testing.expectEqual(OtherSpec.code, runtime.descriptorAt(1).?.code());
    try std.testing.expect(runtime.lookup(TestSpec.code) == a);
    try std.testing.expect(runtime.lookup(99_999) == null);
    const runtime_digest = runtime.digest();
    const static_digest = Registry.fromSpecs(.{ TestSpec, OtherSpec }).digest();
    try std.testing.expectEqualSlices(u8, &static_digest, &runtime_digest);

    try std.testing.expectEqual(
        @as(?i32, TestSpec.code),
        Registry.smallestDuplicateCode(&.{ b, a, b, a }),
    );
    try std.testing.expectError(
        error.DuplicateReportCode,
        Registry.validateUniqueCodes(&.{ b, a, b, a }),
    );
    try std.testing.expectError(
        error.DuplicateReportCode,
        Registry.initValidated(allocator, &.{ b, a, b, a }),
    );
}

test "owned registry clone outlives its source descriptor table" {
    const allocator = std.testing.allocator;
    const a = Adapter(TestSpec).descriptorPtr();
    const b = Adapter(OtherSpec).descriptorPtr();

    var source = try Registry.initValidated(allocator, &.{ b, a });
    const clone = try source.cloneOwned(allocator);
    source.deinit();
    defer clone.deinit();

    try std.testing.expectEqual(@as(usize, 2), clone.len());
    try std.testing.expect(clone.descriptorAt(0).? == a);
    try std.testing.expect(clone.descriptorAt(1).? == b);
    try std.testing.expect(clone.lookup(TestSpec.code) == a);
}

test "registry membership uses exact descriptor pointer identity" {
    const registered = Adapter(TestSpec).descriptorPtr();
    const same_metadata = Adapter(SameMetadataSpec).descriptorPtr();
    try std.testing.expect(registered != same_metadata);
    try std.testing.expectEqual(registered.code(), same_metadata.code());
    try std.testing.expectEqual(
        registered.schemaVersion(),
        same_metadata.schemaVersion(),
    );
    const registered_schema = registered.schemaId();
    const same_schema = same_metadata.schemaId();
    try std.testing.expectEqualSlices(u8, &registered_schema, &same_schema);

    const registry = Registry.fromSpecs(.{TestSpec});
    try std.testing.expect(registry.containsDescriptor(registered));
    try std.testing.expect(!registry.containsDescriptor(same_metadata));
}

test "registry validates matching and risk view types before dispatch" {
    const registry = Registry.fromSpecs(.{ TestSpec, OtherSpec });
    const descriptor = Adapter(TestSpec).descriptorPtr();

    try std.testing.expect(descriptor.acceptsMatchingView(TestMatchingView));
    try std.testing.expect(!descriptor.acceptsMatchingView(WrongMatchingView));
    try std.testing.expect(descriptor.acceptsRiskView(TestRiskView));
    try std.testing.expect(!descriptor.acceptsRiskView(WrongRiskView));
    try registry.validateContextTypes(TestMatchingView, TestRiskView);
    try std.testing.expectError(
        error.ContextTypeMismatch,
        registry.validateContextTypes(WrongMatchingView, TestRiskView),
    );
    try std.testing.expectError(
        error.ContextTypeMismatch,
        registry.validateContextTypes(TestMatchingView, WrongRiskView),
    );
}

test "reserved report code validation is deterministic" {
    const registry = Registry.fromSpecs(.{ OtherSpec, TestSpec });

    try std.testing.expectEqual(
        @as(?i32, null),
        registry.smallestReservedCode(&.{ 10_001, 10_002, 10_003 }),
    );
    try registry.validateNoReservedCodes(&.{ 10_001, 10_002, 10_003 });

    const reserved = [_]i32{
        OtherSpec.code,
        TestSpec.code,
        OtherSpec.code,
    };
    try std.testing.expectEqual(
        @as(?i32, TestSpec.code),
        registry.smallestReservedCode(&reserved),
    );
    try std.testing.expectError(
        error.ReservedReportCode,
        registry.validateNoReservedCodes(&reserved),
    );
}

test "static registry digest is order independent and schema sensitive" {
    const forward = Registry.fromSpecs(.{ TestSpec, OtherSpec });
    const reverse = Registry.fromSpecs(.{ OtherSpec, TestSpec });
    try std.testing.expectEqual(@as(usize, 2), forward.len());
    try std.testing.expectEqual(TestSpec.code, forward.descriptorAt(0).?.code());

    const forward_digest = forward.digest();
    const reverse_digest = reverse.digest();
    try std.testing.expectEqualSlices(u8, &forward_digest, &reverse_digest);

    const version_one = Registry.fromSpecs(.{TestSpec});
    const version_two = Registry.fromSpecs(.{TestSpecV2});
    const version_one_digest = version_one.digest();
    const version_two_digest = version_two.digest();
    try std.testing.expect(!std.mem.eql(
        u8,
        &version_one_digest,
        &version_two_digest,
    ));
}

test "handler failure releases decoded query and partial section state" {
    const allocator = std.testing.allocator;
    const source: TestQuery = .{ .value = 1, .reducer_bias = 0 };
    const body = try Adapter(TestSpec).encodeQuery(allocator, &source);
    defer allocator.free(body);

    try std.testing.expectError(
        error.HandlerFailed,
        Adapter(TestSpec).descriptorPtr().processMatchingBody(
            TestMatchingView,
            allocator,
            body,
            .{
                .view = &test_matching_view,
                .shard_id = 999,
                .shard_mask = 1_023,
            },
        ),
    );
}

fn adapterAllocationCampaign(allocator: std.mem.Allocator) !void {
    const Adapted = Adapter(TestSpec);
    const descriptor = Adapted.descriptorPtr();
    const source: TestQuery = .{ .value = 30, .reducer_bias = 4 };

    const body = try Adapted.encodeQuery(allocator, &source);
    defer allocator.free(body);

    var outcome = try descriptor.processMatchingBody(
        TestMatchingView,
        allocator,
        body,
        .{
            .view = &test_matching_view,
            .shard_id = 2,
            .shard_mask = 3,
        },
    );
    defer outcome.deinit();
    const section_body = switch (outcome) {
        .section => |section| section.bytes(),
        else => return error.ExpectedReportSection,
    };

    const query = try Adapted.cloneQuery(allocator, &source);
    defer query.deinit();
    const runtime_clone = try descriptor.cloneQuery(allocator, query);
    defer runtime_clone.deinit();

    const encoded_clone = try descriptor.encodeQuery(allocator, runtime_clone);
    defer allocator.free(encoded_clone);

    const erased_result = try descriptor.reduceSections(
        allocator,
        query,
        &.{.{
            .section_id = 1_026,
            .padded_bytes = section_body,
        }},
    );
    defer erased_result.deinit();

    const borrowed = try Adapted.borrowResult(erased_result);
    try std.testing.expectEqual(@as(i64, 36), borrowed.value);
    var result = try Adapted.takeResult(erased_result);
    defer Adapted.deinitTakenResult(allocator, &result);

    var registry = try Registry.initValidated(
        allocator,
        &.{
            Adapter(OtherSpec).descriptorPtr(),
            descriptor,
        },
    );
    defer registry.deinit();
    try std.testing.expect(registry.lookup(TestSpec.code) == descriptor);

    const registry_clone = try registry.cloneOwned(allocator);
    defer registry_clone.deinit();
    try std.testing.expect(registry_clone.containsDescriptor(descriptor));
}

test "adapter and validated registry clean every allocation failure" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        adapterAllocationCampaign,
        .{},
    );
}
