//! Owned extension point equivalent to Java's `ISerializationProcessor`.
//!
//! The factory is borrowed only for the duration of core construction. The
//! returned processor owns its opaque context until `deinit`, which calls the
//! shutdown hook exactly once before the destroy hook. Snapshot callbacks
//! receive typed views of the live kernel state instead of an erased
//! `WriteBytesMarshallable`; implementations may encode that state into any
//! storage they own.
//!
//! Lifecycle methods are serialized by `ExchangeCore`/`ExchangeApiSharded`.
//! A processor implementation does not need to make startup, shutdown, or
//! destruction callbacks reentrant. Store and journal callbacks may run on the
//! exchange worker and must not call synchronously back into that same core.

const std = @import("std");
const cmd = @import("cmd.zig");
const kernel_mod = @import("exchange_kernel.zig");

pub const CreateError = std.mem.Allocator.Error || error{
    SerializationProcessorCreateFailed,
};

pub const StartupError = error{
    SerializationProcessorStartupFailed,
};

pub const StoreError = error{
    SerializationProcessorStoreFailed,
    SerializationProcessorStoreRejected,
};

pub const JournalError = error{
    SerializationProcessorJournalWriteFailed,
};

pub const ModuleType = enum {
    matching_engine_router,
    risk_engine,
};

/// Mutable state supplied during startup so a custom processor can restore a
/// snapshot and replay its journal before the live worker is started.
pub const MutableStateView = union(enum) {
    single: *kernel_mod.ExchangeKernel,
    sharded: *kernel_mod.MultiShardKernelSet,
};

/// Stable state view supplied to synchronous snapshot-store callbacks.
pub const StateView = union(enum) {
    single: *const kernel_mod.ExchangeKernel,
    sharded: *const kernel_mod.MultiShardKernelSet,
};

pub const FactoryContext = struct {
    exchange_id: []const u8,
    matching_engine_count: usize,
    risk_engine_count: usize,
    enable_journaling: bool,
    snapshot_id: u64,
    snapshot_base_seq: u64,
    journal_timestamp_ns: u64,
    throw_if_snapshot_not_found: bool,
};

pub const StartupRequest = struct {
    exchange_id: []const u8,
    snapshot_id: u64,
    snapshot_base_seq: u64,
    journal_timestamp_ns: u64,
    throw_if_snapshot_not_found: bool,
    enable_journaling: bool,
    state: MutableStateView,
};

pub const StoreRequest = struct {
    snapshot_id: u64,
    sequence: u64,
    timestamp_ns: u64,
    module_type: ModuleType,
    instance_id: u32,
    state: StateView,
};

/// Position of a journal callback inside the currently available publication
/// range. A consumer snapshots a nonempty available prefix before draining it;
/// publications that arrive behind that prefix form a later batch.
pub const JournalBatchPosition = enum {
    continues,
    terminal,

    pub fn endOfBatch(self: JournalBatchPosition) bool {
        return self == .terminal;
    }
};

pub const JournalRequest = struct {
    sequence: u64,
    command: cmd.OrderCommand,
    /// Mirrors Java Disruptor's `endOfBatch`: exactly one callback in each
    /// nonempty available publication range is terminal.
    end_of_batch: bool = true,
};

pub const VTable = struct {
    startup: *const fn (
        context: ?*anyopaque,
        request: StartupRequest,
    ) StartupError!void,
    store_data: *const fn (
        context: ?*anyopaque,
        request: StoreRequest,
    ) StoreError!bool,
    write_to_journal: *const fn (
        context: ?*anyopaque,
        request: JournalRequest,
    ) JournalError!void,
    shutdown: *const fn (context: ?*anyopaque) void,
    destroy: *const fn (
        context: ?*anyopaque,
        allocator: std.mem.Allocator,
    ) void,
};

pub const Processor = struct {
    context: ?*anyopaque,
    vtable: *const VTable,
    startup_called: bool = false,
    shutdown_called: bool = false,

    pub fn startup(
        self: *Processor,
        request: StartupRequest,
    ) StartupError!void {
        if (self.startup_called) return;
        try self.vtable.startup(self.context, request);
        self.startup_called = true;
    }

    pub fn storeData(
        self: *Processor,
        request: StoreRequest,
    ) StoreError!void {
        if (!try self.vtable.store_data(self.context, request)) {
            return error.SerializationProcessorStoreRejected;
        }
    }

    pub fn writeToJournal(
        self: *Processor,
        request: JournalRequest,
    ) JournalError!void {
        try self.vtable.write_to_journal(self.context, request);
    }

    pub fn shutdown(self: *Processor) void {
        if (self.shutdown_called) return;
        self.shutdown_called = true;
        self.vtable.shutdown(self.context);
    }

    pub fn deinit(
        self: *Processor,
        allocator: std.mem.Allocator,
    ) void {
        self.shutdown();
        self.vtable.destroy(self.context, allocator);
        self.* = undefined;
    }
};

pub const Factory = struct {
    context: ?*anyopaque = null,
    create: *const fn (
        context: ?*anyopaque,
        allocator: std.mem.Allocator,
        config: FactoryContext,
    ) CreateError!Processor,

    pub fn createProcessor(
        self: *const Factory,
        allocator: std.mem.Allocator,
        config: FactoryContext,
    ) CreateError!Processor {
        return self.create(self.context, allocator, config);
    }
};

test "custom processor owns startup shutdown store and journal lifecycle" {
    const Recorder = struct {
        startup_count: usize = 0,
        store_count: usize = 0,
        journal_count: usize = 0,
        shutdown_count: usize = 0,
        destroy_count: usize = 0,

        fn startup(
            context_ptr: ?*anyopaque,
            _: StartupRequest,
        ) StartupError!void {
            const self: *@This() = @ptrCast(@alignCast(context_ptr.?));
            self.startup_count += 1;
        }

        fn store(
            context_ptr: ?*anyopaque,
            _: StoreRequest,
        ) StoreError!bool {
            const self: *@This() = @ptrCast(@alignCast(context_ptr.?));
            self.store_count += 1;
            return true;
        }

        fn journal(
            context_ptr: ?*anyopaque,
            _: JournalRequest,
        ) JournalError!void {
            const self: *@This() = @ptrCast(@alignCast(context_ptr.?));
            self.journal_count += 1;
        }

        fn shutdown(context_ptr: ?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(context_ptr.?));
            self.shutdown_count += 1;
        }

        fn destroy(context_ptr: ?*anyopaque, _: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(context_ptr.?));
            self.destroy_count += 1;
        }

        const vtable: VTable = .{
            .startup = startup,
            .store_data = store,
            .write_to_journal = journal,
            .shutdown = shutdown,
            .destroy = destroy,
        };
    };

    var recorder: Recorder = .{};
    var processor: Processor = .{
        .context = &recorder,
        .vtable = &Recorder.vtable,
    };

    var kernel = try kernel_mod.ExchangeKernel.init(std.testing.allocator, .{});
    defer kernel.deinit();
    const startup_request: StartupRequest = .{
        .exchange_id = "EXT",
        .snapshot_id = 0,
        .snapshot_base_seq = 0,
        .journal_timestamp_ns = 0,
        .throw_if_snapshot_not_found = false,
        .enable_journaling = true,
        .state = .{ .single = &kernel },
    };
    try processor.startup(startup_request);
    try processor.startup(startup_request);
    try processor.storeData(.{
        .snapshot_id = 1,
        .sequence = 2,
        .timestamp_ns = 3,
        .module_type = .matching_engine_router,
        .instance_id = 0,
        .state = .{ .single = &kernel },
    });
    try processor.writeToJournal(.{
        .sequence = 2,
        .command = .{ .command = .nop },
    });
    processor.shutdown();
    processor.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), recorder.startup_count);
    try std.testing.expectEqual(@as(usize, 1), recorder.store_count);
    try std.testing.expectEqual(@as(usize, 1), recorder.journal_count);
    try std.testing.expectEqual(@as(usize, 1), recorder.shutdown_count);
    try std.testing.expectEqual(@as(usize, 1), recorder.destroy_count);
}

test "custom processor propagates store rejection" {
    const Rejecting = struct {
        fn startup(_: ?*anyopaque, _: StartupRequest) StartupError!void {}
        fn store(_: ?*anyopaque, _: StoreRequest) StoreError!bool {
            return false;
        }
        fn journal(_: ?*anyopaque, _: JournalRequest) JournalError!void {}
        fn shutdown(_: ?*anyopaque) void {}
        fn destroy(_: ?*anyopaque, _: std.mem.Allocator) void {}

        const vtable: VTable = .{
            .startup = startup,
            .store_data = store,
            .write_to_journal = journal,
            .shutdown = shutdown,
            .destroy = destroy,
        };
    };

    var processor: Processor = .{
        .context = null,
        .vtable = &Rejecting.vtable,
    };
    defer processor.deinit(std.testing.allocator);
    var kernel = try kernel_mod.ExchangeKernel.init(std.testing.allocator, .{});
    defer kernel.deinit();

    try std.testing.expectError(
        error.SerializationProcessorStoreRejected,
        processor.storeData(.{
            .snapshot_id = 1,
            .sequence = 2,
            .timestamp_ns = 3,
            .module_type = .risk_engine,
            .instance_id = 0,
            .state = .{ .single = &kernel },
        }),
    );
}
