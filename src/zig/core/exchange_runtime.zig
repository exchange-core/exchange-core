const std = @import("std");
const cmd = @import("cmd.zig");
const kernel_mod = @import("exchange_kernel.zig");
const io_context = @import("io_context.zig");

pub const DefaultL2PublishIntervalNs: u64 = 10_000_000;
pub const DefaultQueueCapacity: usize = 16 * 1024;

pub const WaitStrategy = enum {
    busy_spin,
    yielding,
    blocking,
    second_step_no_wait,
};

/// Compatibility name for callers mirroring Java's `CoreWaitStrategy`.
pub const CoreWaitStrategy = WaitStrategy;

pub const LifecycleState = enum {
    cold,
    running,
    closing,
    stopped,
    poisoned,
};

/// Public, backend-neutral shutdown progress. `closing` includes a poisoned
/// runtime whose worker has not yet been safely reaped.
pub const ShutdownState = enum {
    open,
    closing,
    complete,
};

/// A callback or worker may request closure but cannot wait for itself.
pub const ShutdownOutcome = enum {
    /// Accepted work is settled and every owned worker handle is reaped.
    complete,
    /// Admission is fenced, but a callback/worker did not wait for itself.
    /// A later non-callback/non-worker shutdown or deinit may be required.
    deferred,
};

pub const FailureStage = enum {
    startup,
    grouping,
    journal,
    r1,
    matching,
    r2,
    execute,
    result,
    store_result,
    shutdown,
    runtime,
    unknown,
};

pub const FatalStage = FailureStage;

/// The first fatal runtime error. It intentionally contains no clock-derived
/// data so deterministic schedules can compare and replay it exactly.
pub const FatalRecord = struct {
    stage: FailureStage,
    sequence: ?u64 = null,
    request_id: ?u64 = null,
    command: ?cmd.OrderCommandType = null,
    cause: anyerror,
};

pub const LifecycleTransitionError = error{
    InvalidLifecycleTransition,
    RuntimePoisoned,
};

/// A monotonic runtime lifecycle. The poison payload lives in the terminal
/// tagged-union state, so a non-poisoned lifecycle cannot expose fatal data.
pub const RuntimeLifecycle = union(LifecycleState) {
    cold,
    running,
    closing,
    stopped,
    poisoned: FatalRecord,

    pub fn current(self: *const RuntimeLifecycle) LifecycleState {
        return std.meta.activeTag(self.*);
    }

    pub fn fatalRecord(self: *const RuntimeLifecycle) ?*const FatalRecord {
        return switch (self.*) {
            .poisoned => |*record| record,
            else => null,
        };
    }

    /// Returns true only when this call performed the cold -> running change.
    pub fn start(self: *RuntimeLifecycle) LifecycleTransitionError!bool {
        return switch (self.*) {
            .cold => blk: {
                self.* = .running;
                break :blk true;
            },
            .running => false,
            .poisoned => error.RuntimePoisoned,
            .closing, .stopped => error.InvalidLifecycleTransition,
        };
    }

    /// Starts graceful closure. Calling it again after closure began is safe.
    pub fn beginClose(self: *RuntimeLifecycle) LifecycleTransitionError!bool {
        return switch (self.*) {
            .cold, .running => blk: {
                self.* = .closing;
                break :blk true;
            },
            .closing, .stopped => false,
            .poisoned => error.RuntimePoisoned,
        };
    }

    /// Completes graceful closure; running -> stopped must pass through closing.
    pub fn markStopped(self: *RuntimeLifecycle) LifecycleTransitionError!bool {
        return switch (self.*) {
            .closing => blk: {
                self.* = .stopped;
                break :blk true;
            },
            .stopped => false,
            .poisoned => error.RuntimePoisoned,
            .cold, .running => error.InvalidLifecycleTransition,
        };
    }

    /// Records the first fatal frontier. Later poison attempts cannot replace it.
    pub fn poison(self: *RuntimeLifecycle, record: FatalRecord) bool {
        return switch (self.*) {
            .cold, .running, .closing => blk: {
                self.* = .{ .poisoned = record };
                break :blk true;
            },
            .stopped, .poisoned => false,
        };
    }

    pub fn isAccepting(self: *const RuntimeLifecycle) bool {
        return self.current() == .running;
    }

    pub fn isTerminal(self: *const RuntimeLifecycle) bool {
        return switch (self.current()) {
            .stopped, .poisoned => true,
            .cold, .running, .closing => false,
        };
    }
};

pub const ResultEvent = struct {
    sequence: u64,
    command: cmd.OrderCommandType,
    uid: i64,
    symbol: i32,
    result_code: cmd.CommandResultCode,
};

pub const ResultSink = struct {
    context: ?*anyopaque = null,
    on_result: ?*const fn (?*anyopaque, ResultEvent) void = null,
};

threadlocal var callback_execution_depth: usize = 0;

/// Marks the current thread as executing an exchange callback, regardless of
/// which backend owns it. Scopes must be left in strict LIFO order.
pub const CallbackExecutionScope = struct {
    entered_depth: usize,
    active: bool = true,

    pub fn enter() CallbackExecutionScope {
        const entered_depth = std.math.add(
            usize,
            callback_execution_depth,
            1,
        ) catch @panic("callback execution depth overflow");
        callback_execution_depth = entered_depth;
        return .{ .entered_depth = entered_depth };
    }

    pub fn leave(self: *CallbackExecutionScope) void {
        std.debug.assert(self.active);
        std.debug.assert(callback_execution_depth == self.entered_depth);
        std.debug.assert(callback_execution_depth > 0);
        callback_execution_depth -= 1;
        self.active = false;
    }
};

pub fn isCallbackExecutionActive() bool {
    return callback_execution_depth != 0;
}

/// Observes every completed live command response after execution and journal
/// append, including completed business-level rejection results. Thrown,
/// poisoned, replayed, and otherwise unpublished work is not observed. The
/// response pointer and every nested pointer/slice are borrowed only for the
/// duration of `on_result`: callbacks must not retain or deinitialize any part
/// of it. Data needed after return must be deep-copied.
pub const RawResultSink = struct {
    context: ?*anyopaque = null,
    on_result: ?*const fn (?*anyopaque, *const kernel_mod.CommandResponse) void = null,
};

/// Injectable monotonic time source for grouping and timed L2 publication.
/// The production default matches Java's `System.nanoTime()` semantics by
/// reading Zig's awake clock; tests can provide a deterministic fake without
/// sleeping.
pub const MonotonicClock = struct {
    context: ?*anyopaque = null,
    read_ns: *const fn (?*anyopaque) u64 = readAwakeClockNs,

    pub fn awake() MonotonicClock {
        return .{};
    }

    pub fn nowNs(self: MonotonicClock) u64 {
        return self.read_ns(self.context);
    }
};

/// Injectable wall-clock source for command publication timestamps and
/// human-readable main-log prefixes. Production defaults to OS real time;
/// deterministic simulation installs a seed-driven fake so convenience
/// zero-timestamp paths stay reproducible.
pub const WallClock = struct {
    context: ?*anyopaque = null,
    read_ns: *const fn (?*anyopaque) u64 = readRealClockNs,

    pub fn real() WallClock {
        return .{};
    }

    pub fn nowNs(self: WallClock) u64 {
        return self.read_ns(self.context);
    }

    pub fn nowMs(self: WallClock) u64 {
        return self.nowNs() / std.time.ns_per_ms;
    }
};

pub const Config = struct {
    queue_capacity: usize = DefaultQueueCapacity,
    wait_strategy: WaitStrategy = .blocking,
    max_batch_size: usize = 256,
    l2_flags_every_n_commands: usize = 0,
    l2_publish_interval_ns: u64 = DefaultL2PublishIntervalNs,
    max_group_duration_ns: u64 = 0,
    clock: MonotonicClock = MonotonicClock.awake(),
    sink: ResultSink = .{},

    pub const ValidationError = error{
        InvalidBatchSize,
        InvalidQueueCapacity,
        InvalidWaitStrategy,
    };

    pub fn validate(self: Config) ValidationError!void {
        if (self.queue_capacity == 0 or @popCount(self.queue_capacity) != 1) {
            return error.InvalidQueueCapacity;
        }
        if (self.wait_strategy == .second_step_no_wait) {
            return error.InvalidWaitStrategy;
        }
        if (self.max_batch_size == 0 or self.max_batch_size > self.queue_capacity / 4) {
            return error.InvalidBatchSize;
        }
    }
};

pub const InitError = std.mem.Allocator.Error || Config.ValidationError;

pub const EnqueueError = error{
    Full,
};

pub const DrainError = std.mem.Allocator.Error || kernel_mod.ExecuteError || error{
    BatchMissingSequence,
};

pub const DrainOutcome = struct {
    groups_processed: usize = 0,
    commands_processed: usize = 0,
    last_merged_result: cmd.CommandResultCode = .accepted,
};

/// Deterministic grouping and manual-drain compatibility state.
///
/// `enqueue`, `enqueueBatch`, and `drain` intentionally remain usable while the
/// lifecycle is cold so existing embedded/manual stepping keeps working. The
/// production `ExchangeCore` owns lifecycle checks and synchronization around
/// this object. A drain failure can occur after kernel mutation; the owner must
/// poison the lifecycle and must not retry that retained command prefix.
pub const ExchangeRuntime = struct {
    allocator: std.mem.Allocator,
    kernel: *kernel_mod.ExchangeKernel,
    queue_capacity: usize,
    wait_strategy: WaitStrategy,
    lifecycle: RuntimeLifecycle = .cold,
    max_batch_size: usize,
    l2_flags_every_n_commands: usize,
    l2_publish_interval_ns: u64,
    commands_since_l2_flag: usize = 0,
    l2_next_publish_deadline_ns: ?u64 = null,
    max_group_duration_ns: u64,
    group_deadline_ns: ?u64 = null,
    clock: MonotonicClock,
    last_observed_clock_ns: ?u64 = null,
    sink: ResultSink,
    processing_enabled: bool = true,
    grouping_enabled: bool = true,
    group_counter: i64 = 0,
    msgs_in_group: usize = 0,
    pending: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty,
    batch_scratch: []*cmd.OrderCommand,

    pub fn init(
        allocator: std.mem.Allocator,
        kernel: *kernel_mod.ExchangeKernel,
        config: Config,
    ) InitError!ExchangeRuntime {
        try config.validate();

        var pending: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
        errdefer pending.deinit(allocator);
        try pending.ensureTotalCapacityPrecise(allocator, config.queue_capacity);

        const batch_scratch = try allocator.alloc(*cmd.OrderCommand, config.max_batch_size);
        errdefer allocator.free(batch_scratch);

        return .{
            .allocator = allocator,
            .kernel = kernel,
            .queue_capacity = config.queue_capacity,
            .wait_strategy = config.wait_strategy,
            .max_batch_size = config.max_batch_size,
            .l2_flags_every_n_commands = config.l2_flags_every_n_commands,
            .l2_publish_interval_ns = config.l2_publish_interval_ns,
            .max_group_duration_ns = config.max_group_duration_ns,
            .clock = config.clock,
            .sink = config.sink,
            .pending = pending,
            .batch_scratch = batch_scratch,
        };
    }

    pub fn lifecycleState(self: *const ExchangeRuntime) LifecycleState {
        return self.lifecycle.current();
    }

    pub fn fatalRecord(self: *const ExchangeRuntime) ?*const FatalRecord {
        return self.lifecycle.fatalRecord();
    }

    pub fn start(self: *ExchangeRuntime) LifecycleTransitionError!bool {
        return self.lifecycle.start();
    }

    pub fn beginClose(self: *ExchangeRuntime) LifecycleTransitionError!bool {
        return self.lifecycle.beginClose();
    }

    pub fn markStopped(self: *ExchangeRuntime) LifecycleTransitionError!bool {
        return self.lifecycle.markStopped();
    }

    pub fn poison(self: *ExchangeRuntime, record: FatalRecord) bool {
        return self.lifecycle.poison(record);
    }

    pub fn deinit(self: *ExchangeRuntime) void {
        self.pending.deinit(self.allocator);
        self.allocator.free(self.batch_scratch);
        self.* = undefined;
    }

    pub fn enqueue(self: *ExchangeRuntime, command: cmd.OrderCommand) EnqueueError!void {
        if (self.pending.items.len == self.queue_capacity) return error.Full;
        self.pending.appendAssumeCapacity(command);
    }

    /// Enqueues a logical command batch atomically or leaves the queue unchanged.
    pub fn enqueueBatch(
        self: *ExchangeRuntime,
        commands: []const cmd.OrderCommand,
    ) EnqueueError!void {
        if (commands.len > self.remainingCapacity()) return error.Full;
        for (commands) |command| {
            self.pending.appendAssumeCapacity(command);
        }
    }

    pub fn pendingCount(self: *const ExchangeRuntime) usize {
        return self.pending.items.len;
    }

    pub fn remainingCapacity(self: *const ExchangeRuntime) usize {
        return self.queue_capacity - self.pending.items.len;
    }

    pub fn batchLenForPending(self: *const ExchangeRuntime, start_index: usize) usize {
        std.debug.assert(start_index < self.pending.items.len);
        const limit = if (self.grouping_enabled) self.max_batch_size else 1;
        std.debug.assert(limit > 0);

        var len: usize = 0;
        while (start_index + len < self.pending.items.len and len < limit) {
            const command = &self.pending.items[start_index + len];
            const is_boundary = isGroupBoundaryCommand(command);
            if (len > 0 and is_boundary) {
                break;
            }

            len += 1;

            // Apply grouping-control mode changes before batching subsequent commands.
            if (command.command == .grouping_control) {
                break;
            }

            if (!self.grouping_enabled) {
                break;
            }
        }

        std.debug.assert(len > 0);
        return len;
    }

    pub fn stampBatchMetadata(
        self: *ExchangeRuntime,
        commands: []const *cmd.OrderCommand,
    ) void {
        for (commands) |command_ptr| {
            self.stampCommandMetadata(command_ptr);
        }
    }

    pub fn drain(self: *ExchangeRuntime) DrainError!DrainOutcome {
        var outcome: DrainOutcome = .{};
        var start_index: usize = 0;

        while (start_index < self.pending.items.len) {
            const batch_len = self.batchLenForPending(start_index);
            const pointers = self.batch_scratch[0..batch_len];

            for (0..batch_len) |idx| {
                pointers[idx] = &self.pending.items[start_index + idx];
            }

            self.stampBatchMetadata(pointers);
            const batch = try self.kernel.executeBatch(pointers);
            outcome.groups_processed += 1;
            outcome.commands_processed += batch_len;
            outcome.last_merged_result = batch.result_code;

            const seq_from = try requireBatchSequenceFrom(batch);
            for (0..batch_len) |idx| {
                const command_ptr = pointers[idx];
                if (!self.shouldPublishResultForCommand(command_ptr)) {
                    continue;
                }
                publishResult(self.sink, .{
                    .sequence = seq_from + idx,
                    .command = command_ptr.command,
                    .uid = command_ptr.uid,
                    .symbol = command_ptr.symbol,
                    .result_code = command_ptr.result_code,
                });
            }

            start_index += batch_len;
        }

        self.pending.clearRetainingCapacity();
        return outcome;
    }

    pub fn shouldPublishResultForCommand(
        self: *ExchangeRuntime,
        command: *const cmd.OrderCommand,
    ) bool {
        if (command.command == .grouping_control) {
            self.processing_enabled = command.order_id == 1;
        }
        return self.processing_enabled;
    }

    fn stampCommandMetadata(self: *ExchangeRuntime, command: *cmd.OrderCommand) void {
        if (command.command == .grouping_control) {
            self.grouping_enabled = command.order_id == 1;
        }

        if (!self.grouping_enabled) {
            // Java's grouping-control OFF mode is also the replay/fidelity path:
            // downstream stages must observe metadata supplied by the caller.
            // Replay metadata and timestamps must not advance live clock state.
            return;
        }

        const now_ns = self.monotonicNowNs();

        if (self.shouldSplitForElapsedGroup(now_ns)) {
            self.beginNextGroup();
        }

        if (isGroupBoundaryCommand(command)) {
            self.beginNextGroup();
        }

        if (self.msgs_in_group == 0) {
            self.armGroupDeadline(now_ns);
        }
        command.events_group = self.group_counter;
        command.service_flags = self.nextL2ServiceFlags(now_ns);

        self.msgs_in_group += 1;
        if (self.msgs_in_group >= self.max_batch_size and command.command != .persist_state_risk) {
            self.beginNextGroup();
        }
    }

    fn monotonicNowNs(self: *ExchangeRuntime) u64 {
        const observed_ns = self.clock.nowNs();
        const now_ns = if (self.last_observed_clock_ns) |last_ns|
            @max(last_ns, observed_ns)
        else
            observed_ns;
        self.last_observed_clock_ns = now_ns;
        return now_ns;
    }

    fn beginNextGroup(self: *ExchangeRuntime) void {
        self.group_counter += 1;
        self.msgs_in_group = 0;
        self.group_deadline_ns = null;
    }

    fn armGroupDeadline(self: *ExchangeRuntime, now_ns: u64) void {
        self.group_deadline_ns = if (self.max_group_duration_ns == 0)
            null
        else
            safeAddDurationNs(now_ns, self.max_group_duration_ns);
    }

    fn shouldSplitForElapsedGroup(self: *const ExchangeRuntime, now_ns: u64) bool {
        if (self.msgs_in_group == 0) return false;
        const deadline_ns = self.group_deadline_ns orelse return false;
        return now_ns > deadline_ns;
    }

    fn nextL2ServiceFlags(self: *ExchangeRuntime, now_ns: u64) i32 {
        const timed_trigger = self.nextTimedL2ServiceFlag(now_ns);
        const periodic_trigger = self.nextPeriodicL2ServiceFlag();
        return if (timed_trigger or periodic_trigger) 1 else 0;
    }

    fn nextPeriodicL2ServiceFlag(self: *ExchangeRuntime) bool {
        if (self.l2_flags_every_n_commands == 0) {
            return false;
        }

        if (self.commands_since_l2_flag == std.math.maxInt(usize)) {
            self.commands_since_l2_flag = 0;
        }
        self.commands_since_l2_flag += 1;
        if (self.commands_since_l2_flag >= self.l2_flags_every_n_commands) {
            self.commands_since_l2_flag = 0;
            return true;
        }
        return false;
    }

    fn nextTimedL2ServiceFlag(self: *ExchangeRuntime, now_ns: u64) bool {
        if (self.l2_publish_interval_ns == 0) {
            return false;
        }

        const deadline_ns = self.l2_next_publish_deadline_ns orelse {
            self.l2_next_publish_deadline_ns = safeAddDurationNs(now_ns, self.l2_publish_interval_ns);
            return false;
        };

        if (now_ns <= deadline_ns) {
            return false;
        }
        self.l2_next_publish_deadline_ns = safeAddDurationNs(now_ns, self.l2_publish_interval_ns);
        return true;
    }
};

fn readAwakeClockNs(_: ?*anyopaque) u64 {
    const nanoseconds = std.Io.Clock.awake.now(io_context.current()).nanoseconds;
    if (nanoseconds <= 0) return 0;
    return std.math.cast(u64, nanoseconds) orelse std.math.maxInt(u64);
}

fn readRealClockNs(_: ?*anyopaque) u64 {
    return io_context.realTimestampNs();
}

fn safeAddDurationNs(base_ns: u64, delta_ns: u64) u64 {
    return std.math.add(u64, base_ns, delta_ns) catch std.math.maxInt(u64);
}

fn isGroupBoundaryCommand(command: *const cmd.OrderCommand) bool {
    return switch (command.command) {
        .reset,
        .persist_state_matching,
        .grouping_control,
        => true,
        .binary_data_command, .binary_data_query => command.symbol == -1,
        else => false,
    };
}

fn requireBatchSequenceFrom(batch: kernel_mod.BatchResponse) error{BatchMissingSequence}!u64 {
    return batch.sequence_from orelse error.BatchMissingSequence;
}

fn publishResult(sink: ResultSink, event: ResultEvent) void {
    if (sink.on_result) |callback| {
        callback(sink.context, event);
    }
}

fn makeAddUserCommand(uid: i64) cmd.OrderCommand {
    return .{
        .command = .add_user,
        .uid = uid,
        .result_code = .new,
    };
}

fn makeGroupingControlCommand(enabled: bool) cmd.OrderCommand {
    return .{
        .command = .grouping_control,
        .order_id = if (enabled) 1 else 0,
        .result_code = .new,
    };
}

test "callback execution scope is nesting-safe and balanced" {
    try std.testing.expect(!isCallbackExecutionActive());

    {
        var outer = CallbackExecutionScope.enter();
        defer outer.leave();
        try std.testing.expect(isCallbackExecutionActive());

        {
            var inner = CallbackExecutionScope.enter();
            defer inner.leave();
            try std.testing.expect(isCallbackExecutionActive());
        }
        try std.testing.expect(isCallbackExecutionActive());
    }
    try std.testing.expect(!isCallbackExecutionActive());
}

const FakeMonotonicClock = struct {
    now_ns: u64 = 0,
    reads: usize = 0,

    fn source(self: *@This()) MonotonicClock {
        return .{
            .context = self,
            .read_ns = read,
        };
    }

    fn read(context: ?*anyopaque) u64 {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.reads += 1;
        return self.now_ns;
    }
};

test "runtime lifecycle permits only monotonic graceful transitions" {
    var lifecycle: RuntimeLifecycle = .cold;
    try std.testing.expectEqual(LifecycleState.cold, lifecycle.current());
    try std.testing.expect(!lifecycle.isAccepting());
    try std.testing.expect(!lifecycle.isTerminal());

    try std.testing.expect(try lifecycle.start());
    try std.testing.expectEqual(LifecycleState.running, lifecycle.current());
    try std.testing.expect(lifecycle.isAccepting());
    try std.testing.expect(!try lifecycle.start());
    try std.testing.expectError(
        error.InvalidLifecycleTransition,
        lifecycle.markStopped(),
    );

    try std.testing.expect(try lifecycle.beginClose());
    try std.testing.expectEqual(LifecycleState.closing, lifecycle.current());
    try std.testing.expect(!try lifecycle.beginClose());
    try std.testing.expect(try lifecycle.markStopped());
    try std.testing.expectEqual(LifecycleState.stopped, lifecycle.current());
    try std.testing.expect(lifecycle.isTerminal());
    try std.testing.expectError(
        error.InvalidLifecycleTransition,
        lifecycle.start(),
    );
}

test "runtime lifecycle poison frontier is terminal and first-write-wins" {
    var lifecycle: RuntimeLifecycle = .cold;
    _ = try lifecycle.start();

    const first = FatalRecord{
        .stage = .r2,
        .sequence = 41,
        .request_id = 7001,
        .command = .place_order,
        .cause = error.ArithmeticOverflow,
    };
    try std.testing.expect(lifecycle.poison(first));
    try std.testing.expectEqual(LifecycleState.poisoned, lifecycle.current());
    try std.testing.expect(lifecycle.isTerminal());
    try std.testing.expectError(error.RuntimePoisoned, lifecycle.beginClose());

    const later = FatalRecord{
        .stage = .store_result,
        .sequence = 42,
        .request_id = 7002,
        .command = .cancel_order,
        .cause = error.OutOfMemory,
    };
    try std.testing.expect(!lifecycle.poison(later));

    const retained = lifecycle.fatalRecord().?;
    try std.testing.expectEqual(FailureStage.r2, retained.stage);
    try std.testing.expectEqual(@as(?u64, 41), retained.sequence);
    try std.testing.expectEqual(@as(?u64, 7001), retained.request_id);
    try std.testing.expectEqual(@as(?cmd.OrderCommandType, .place_order), retained.command);
    try std.testing.expectEqual(error.ArithmeticOverflow, retained.cause);
}

test "exchange runtime bounded enqueue has atomic batch backpressure" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .queue_capacity = 4,
        .max_batch_size = 1,
        .wait_strategy = .yielding,
    });
    defer runtime.deinit();

    try runtime.enqueueBatch(&.{
        makeAddUserCommand(801),
        makeAddUserCommand(802),
        makeAddUserCommand(803),
    });
    try std.testing.expectEqual(@as(usize, 3), runtime.pendingCount());
    try std.testing.expectEqual(@as(usize, 1), runtime.remainingCapacity());

    try std.testing.expectError(
        error.Full,
        runtime.enqueueBatch(&.{
            makeAddUserCommand(804),
            makeAddUserCommand(805),
        }),
    );
    try std.testing.expectEqual(@as(usize, 3), runtime.pendingCount());
    try runtime.enqueue(makeAddUserCommand(804));
    try std.testing.expectError(error.Full, runtime.enqueue(makeAddUserCommand(805)));

    const outcome = try runtime.drain();
    try std.testing.expectEqual(@as(usize, 4), outcome.commands_processed);
    try std.testing.expectEqual(@as(usize, 0), runtime.pendingCount());
}

test "exchange runtime drains in fixed-size groups with deterministic sequences" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    const Collector = struct {
        allocator: std.mem.Allocator,
        events: std.ArrayListUnmanaged(ResultEvent) = .empty,

        fn callback(ctx: ?*anyopaque, event: ResultEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.events.append(self.allocator, event) catch @panic("collector append failed");
        }

        fn deinit(self: *@This()) void {
            self.events.deinit(self.allocator);
            self.* = undefined;
        }
    };

    var collector: Collector = .{ .allocator = allocator };
    defer collector.deinit();

    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .max_batch_size = 2,
        .sink = .{
            .context = &collector,
            .on_result = Collector.callback,
        },
    });
    defer runtime.deinit();

    try runtime.enqueue(makeAddUserCommand(901));
    try runtime.enqueue(makeAddUserCommand(901));
    try runtime.enqueue(makeAddUserCommand(902));
    try runtime.enqueue(makeAddUserCommand(903));
    try runtime.enqueue(makeAddUserCommand(903));
    try std.testing.expectEqual(@as(usize, 5), runtime.pendingCount());

    const outcome = try runtime.drain();
    try std.testing.expectEqual(@as(usize, 3), outcome.groups_processed);
    try std.testing.expectEqual(@as(usize, 5), outcome.commands_processed);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, outcome.last_merged_result);
    try std.testing.expectEqual(@as(usize, 0), runtime.pendingCount());

    try std.testing.expectEqual(@as(usize, 5), collector.events.items.len);
    try std.testing.expectEqual(@as(u64, 1), collector.events.items[0].sequence);
    try std.testing.expectEqual(@as(u64, 2), collector.events.items[1].sequence);
    try std.testing.expectEqual(@as(u64, 3), collector.events.items[2].sequence);
    try std.testing.expectEqual(@as(u64, 4), collector.events.items[3].sequence);
    try std.testing.expectEqual(@as(u64, 5), collector.events.items[4].sequence);

    try std.testing.expectEqual(cmd.CommandResultCode.success, collector.events.items[0].result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, collector.events.items[1].result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, collector.events.items[2].result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, collector.events.items[3].result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, collector.events.items[4].result_code);
}

test "exchange runtime reject zero batch size config" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    try std.testing.expectError(
        error.InvalidBatchSize,
        ExchangeRuntime.init(allocator, &kernel, .{
            .max_batch_size = 0,
        }),
    );
}

test "exchange runtime grouping control toggles batching deterministically" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .max_batch_size = 2,
    });
    defer runtime.deinit();

    try runtime.enqueue(makeAddUserCommand(9501));
    try runtime.enqueue(makeAddUserCommand(9502));
    try runtime.enqueue(makeGroupingControlCommand(false));
    try runtime.enqueue(makeAddUserCommand(9503));
    try runtime.enqueue(makeAddUserCommand(9504));
    try runtime.enqueue(makeGroupingControlCommand(true));
    try runtime.enqueue(makeAddUserCommand(9505));
    try runtime.enqueue(makeAddUserCommand(9506));

    const outcome = try runtime.drain();
    try std.testing.expectEqual(@as(usize, 6), outcome.groups_processed);
    try std.testing.expectEqual(@as(usize, 8), outcome.commands_processed);
    try std.testing.expectEqual(cmd.CommandResultCode.success, outcome.last_merged_result);

    try std.testing.expect(runtime.grouping_enabled);
    try std.testing.expectEqual(@as(usize, 0), runtime.pendingCount());

    // Grouping mode persists across drains and can be disabled again.
    try runtime.enqueue(makeGroupingControlCommand(false));
    try runtime.enqueue(makeAddUserCommand(9510));
    const second = try runtime.drain();
    try std.testing.expectEqual(@as(usize, 2), second.groups_processed);
    try std.testing.expectEqual(@as(usize, 2), second.commands_processed);
    try std.testing.expect(!runtime.grouping_enabled);
}

test "exchange runtime preserves caller metadata while grouping is disabled" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .max_batch_size = 16,
    });
    defer runtime.deinit();

    var before_disable = makeAddUserCommand(9551);
    before_disable.events_group = 701;
    before_disable.service_flags = 11;

    var disable = makeGroupingControlCommand(false);
    disable.events_group = 702;
    disable.service_flags = 12;

    var while_disabled = makeAddUserCommand(9552);
    while_disabled.events_group = 703;
    while_disabled.service_flags = 13;

    var terminal_binary: cmd.OrderCommand = .{
        .command = .binary_data_command,
        .symbol = -1,
        .events_group = 704,
        .service_flags = 14,
        .result_code = .new,
    };

    var enable = makeGroupingControlCommand(true);
    enable.events_group = 705;
    enable.service_flags = 15;

    var after_enable = makeAddUserCommand(9553);
    after_enable.events_group = 706;
    after_enable.service_flags = 16;

    var pointers = [_]*cmd.OrderCommand{
        &before_disable,
        &disable,
        &while_disabled,
        &terminal_binary,
        &enable,
        &after_enable,
    };
    runtime.stampBatchMetadata(pointers[0..]);

    try std.testing.expectEqual(@as(i64, 0), before_disable.events_group);
    try std.testing.expectEqual(@as(i32, 0), before_disable.service_flags);
    try std.testing.expectEqual(@as(i64, 702), disable.events_group);
    try std.testing.expectEqual(@as(i32, 12), disable.service_flags);
    try std.testing.expectEqual(@as(i64, 703), while_disabled.events_group);
    try std.testing.expectEqual(@as(i32, 13), while_disabled.service_flags);
    try std.testing.expectEqual(@as(i64, 704), terminal_binary.events_group);
    try std.testing.expectEqual(@as(i32, 14), terminal_binary.service_flags);

    // Re-enabling resumes generated grouping metadata instead of preserving
    // the caller-provided values from the disabled interval.
    try std.testing.expectEqual(@as(i64, 1), enable.events_group);
    try std.testing.expectEqual(@as(i32, 0), enable.service_flags);
    try std.testing.expectEqual(@as(i64, 1), after_enable.events_group);
    try std.testing.expectEqual(@as(i32, 0), after_enable.service_flags);
    try std.testing.expect(runtime.grouping_enabled);
}

test "exchange runtime disabled boundaries do not advance generated groups" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .max_batch_size = 16,
    });
    defer runtime.deinit();

    var initial = makeAddUserCommand(9561);
    runtime.stampBatchMetadata(&.{&initial});

    var disable = makeGroupingControlCommand(false);
    disable.events_group = 801;
    disable.service_flags = 21;
    runtime.stampBatchMetadata(&.{&disable});

    var disabled_reset: cmd.OrderCommand = .{
        .command = .reset,
        .events_group = 802,
        .service_flags = 22,
        .result_code = .new,
    };
    var disabled_terminal_binary: cmd.OrderCommand = .{
        .command = .binary_data_query,
        .symbol = -1,
        .events_group = 803,
        .service_flags = 23,
        .result_code = .new,
    };
    runtime.stampBatchMetadata(&.{ &disabled_reset, &disabled_terminal_binary });

    try std.testing.expectEqual(@as(i64, 0), runtime.group_counter);
    try std.testing.expectEqual(@as(i64, 802), disabled_reset.events_group);
    try std.testing.expectEqual(@as(i32, 22), disabled_reset.service_flags);
    try std.testing.expectEqual(@as(i64, 803), disabled_terminal_binary.events_group);
    try std.testing.expectEqual(@as(i32, 23), disabled_terminal_binary.service_flags);

    var enable = makeGroupingControlCommand(true);
    runtime.stampBatchMetadata(&.{&enable});
    try std.testing.expectEqual(@as(i64, 1), enable.events_group);

    var enabled_reset: cmd.OrderCommand = .{
        .command = .reset,
        .result_code = .new,
    };
    runtime.stampBatchMetadata(&.{&enabled_reset});
    try std.testing.expectEqual(@as(i64, 2), enabled_reset.events_group);
    try std.testing.expectEqual(@as(i64, 2), runtime.group_counter);
}

test "exchange runtime disabled replay preserves metadata without perturbing live deadlines" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var fake_clock: FakeMonotonicClock = .{ .now_ns = 100 };
    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .max_batch_size = 16,
        .max_group_duration_ns = 100,
        .l2_publish_interval_ns = 100,
        .clock = fake_clock.source(),
    });
    defer runtime.deinit();

    var initial = makeAddUserCommand(9571);
    initial.timestamp = 0;
    runtime.stampBatchMetadata(&.{&initial});
    try std.testing.expectEqual(@as(?u64, 200), runtime.group_deadline_ns);
    try std.testing.expectEqual(@as(?u64, 200), runtime.l2_next_publish_deadline_ns);
    try std.testing.expectEqual(@as(usize, 1), fake_clock.reads);

    fake_clock.now_ns = 110;
    var disable = makeGroupingControlCommand(false);
    disable.timestamp = std.math.maxInt(i64);
    disable.events_group = 901;
    disable.service_flags = 31;
    runtime.stampBatchMetadata(&.{&disable});

    // Even a huge unobserved clock jump and extreme replay timestamps leave
    // both live deadlines and the monotonic observation frontier untouched.
    fake_clock.now_ns = 1_000;
    var replay_high = makeAddUserCommand(9572);
    replay_high.timestamp = std.math.maxInt(i64);
    replay_high.events_group = 902;
    replay_high.service_flags = 32;
    var replay_low = makeAddUserCommand(9573);
    replay_low.timestamp = std.math.minInt(i64);
    replay_low.events_group = 903;
    replay_low.service_flags = 33;
    runtime.stampBatchMetadata(&.{ &replay_high, &replay_low });

    try std.testing.expectEqual(@as(i64, 901), disable.events_group);
    try std.testing.expectEqual(@as(i32, 31), disable.service_flags);
    try std.testing.expectEqual(@as(i64, 902), replay_high.events_group);
    try std.testing.expectEqual(@as(i32, 32), replay_high.service_flags);
    try std.testing.expectEqual(@as(i64, 903), replay_low.events_group);
    try std.testing.expectEqual(@as(i32, 33), replay_low.service_flags);
    try std.testing.expectEqual(@as(?u64, 200), runtime.group_deadline_ns);
    try std.testing.expectEqual(@as(?u64, 200), runtime.l2_next_publish_deadline_ns);
    try std.testing.expectEqual(@as(usize, 1), fake_clock.reads);

    fake_clock.now_ns = 150;
    var enable = makeGroupingControlCommand(true);
    enable.timestamp = std.math.maxInt(i64);
    enable.events_group = 904;
    enable.service_flags = 34;
    runtime.stampBatchMetadata(&.{&enable});
    try std.testing.expectEqual(@as(i64, 1), enable.events_group);
    try std.testing.expectEqual(@as(i32, 0), enable.service_flags);
    try std.testing.expectEqual(@as(?u64, 250), runtime.group_deadline_ns);
    try std.testing.expectEqual(@as(?u64, 200), runtime.l2_next_publish_deadline_ns);
    try std.testing.expectEqual(@as(usize, 2), fake_clock.reads);

    // Timed L2 remains based on the original live deadline, while the group
    // opened by ON gets its own fresh duration deadline.
    fake_clock.now_ns = 201;
    var after_enable = makeAddUserCommand(9574);
    after_enable.timestamp = std.math.minInt(i64);
    runtime.stampBatchMetadata(&.{&after_enable});
    try std.testing.expectEqual(@as(i64, 1), after_enable.events_group);
    try std.testing.expectEqual(@as(i32, 1), after_enable.service_flags);

    fake_clock.now_ns = 251;
    var after_idle = makeAddUserCommand(9575);
    after_idle.timestamp = std.math.minInt(i64);
    runtime.stampBatchMetadata(&.{&after_idle});
    try std.testing.expectEqual(@as(i64, 2), after_idle.events_group);
    try std.testing.expectEqual(@as(i32, 0), after_idle.service_flags);
}

test "exchange runtime result sink follows grouping-control processing semantics" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    const Collector = struct {
        allocator: std.mem.Allocator,
        events: std.ArrayListUnmanaged(ResultEvent) = .empty,

        fn callback(ctx: ?*anyopaque, event: ResultEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.events.append(self.allocator, event) catch @panic("collector append failed");
        }

        fn deinit(self: *@This()) void {
            self.events.deinit(self.allocator);
            self.* = undefined;
        }
    };

    var collector: Collector = .{ .allocator = allocator };
    defer collector.deinit();

    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .max_batch_size = 4,
        .sink = .{
            .context = &collector,
            .on_result = Collector.callback,
        },
    });
    defer runtime.deinit();

    try runtime.enqueue(makeAddUserCommand(9601));
    try runtime.enqueue(makeGroupingControlCommand(false));
    try runtime.enqueue(makeAddUserCommand(9602));
    try runtime.enqueue(makeGroupingControlCommand(true));
    try runtime.enqueue(makeAddUserCommand(9603));

    const outcome = try runtime.drain();
    try std.testing.expectEqual(@as(usize, 5), outcome.groups_processed);
    try std.testing.expectEqual(@as(usize, 5), outcome.commands_processed);
    try std.testing.expectEqual(cmd.CommandResultCode.success, outcome.last_merged_result);

    try std.testing.expectEqual(@as(usize, 3), collector.events.items.len);
    try std.testing.expectEqual(@as(u64, 1), collector.events.items[0].sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.add_user, collector.events.items[0].command);
    try std.testing.expectEqual(@as(u64, 4), collector.events.items[1].sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.grouping_control, collector.events.items[1].command);
    try std.testing.expectEqual(@as(u64, 5), collector.events.items[2].sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.add_user, collector.events.items[2].command);

    try std.testing.expect(runtime.processing_enabled);
}

test "exchange runtime stamps periodic l2 service flags deterministically" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .max_batch_size = 8,
        .l2_flags_every_n_commands = 2,
        .l2_publish_interval_ns = 0,
    });
    defer runtime.deinit();

    var c1 = makeAddUserCommand(9701);
    var c2 = makeAddUserCommand(9702);
    var c3 = makeAddUserCommand(9703);
    var c4 = makeAddUserCommand(9704);
    var pointers = [_]*cmd.OrderCommand{ &c1, &c2, &c3, &c4 };
    runtime.stampBatchMetadata(pointers[0..]);

    try std.testing.expectEqual(@as(i32, 0), c1.service_flags);
    try std.testing.expectEqual(@as(i32, 1), c2.service_flags);
    try std.testing.expectEqual(@as(i32, 0), c3.service_flags);
    try std.testing.expectEqual(@as(i32, 1), c4.service_flags);

    var disable = makeGroupingControlCommand(false);
    var suppressed = makeAddUserCommand(9705);
    var while_disabled = [_]*cmd.OrderCommand{ &disable, &suppressed };
    runtime.stampBatchMetadata(while_disabled[0..]);
    try std.testing.expectEqual(@as(i32, 0), disable.service_flags);
    try std.testing.expectEqual(@as(i32, 0), suppressed.service_flags);

    var enable = makeGroupingControlCommand(true);
    var resumed = makeAddUserCommand(9706);
    var after_resume = [_]*cmd.OrderCommand{ &enable, &resumed };
    runtime.stampBatchMetadata(after_resume[0..]);
    try std.testing.expectEqual(@as(i32, 0), enable.service_flags);
    try std.testing.expectEqual(@as(i32, 1), resumed.service_flags);
}

test "exchange runtime timed l2 flags use monotonic time not client timestamps" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var fake_clock: FakeMonotonicClock = .{ .now_ns = 10 };
    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .max_batch_size = 8,
        .l2_publish_interval_ns = 100,
        .clock = fake_clock.source(),
    });
    defer runtime.deinit();

    var c1 = makeAddUserCommand(9751);
    c1.timestamp = std.math.maxInt(i64);
    runtime.stampBatchMetadata(&.{&c1});

    // A frozen huge client timestamp cannot trigger the runtime deadline.
    fake_clock.now_ns = 110;
    var c2 = makeAddUserCommand(9752);
    c2.timestamp = std.math.maxInt(i64);
    runtime.stampBatchMetadata(&.{&c2});

    // The deadline is strict, matching Java's `t > deadline` check. A
    // retrograde client timestamp is irrelevant once awake time has elapsed.
    fake_clock.now_ns = 111;
    var c3 = makeAddUserCommand(9753);
    c3.timestamp = std.math.minInt(i64);
    runtime.stampBatchMetadata(&.{&c3});

    // Clamp a faulty retrograde injected reading so runtime deadlines cannot
    // themselves move backward.
    fake_clock.now_ns = 1;
    var c4 = makeAddUserCommand(9754);
    c4.timestamp = 0;
    runtime.stampBatchMetadata(&.{&c4});

    fake_clock.now_ns = 212;
    var c5 = makeAddUserCommand(9755);
    c5.timestamp = 0;
    runtime.stampBatchMetadata(&.{&c5});

    try std.testing.expectEqual(@as(i32, 0), c1.service_flags);
    try std.testing.expectEqual(@as(i32, 0), c2.service_flags);
    try std.testing.expectEqual(@as(i32, 1), c3.service_flags);
    try std.testing.expectEqual(@as(i32, 0), c4.service_flags);
    try std.testing.expectEqual(@as(i32, 1), c5.service_flags);
    try std.testing.expectEqual(@as(?u64, 312), runtime.l2_next_publish_deadline_ns);
}

test "exchange runtime group deadline starts once and expires while idle" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var fake_clock: FakeMonotonicClock = .{ .now_ns = 100 };
    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .max_batch_size = 16,
        .max_group_duration_ns = 50,
        .l2_publish_interval_ns = 0,
        .clock = fake_clock.source(),
    });
    defer runtime.deinit();

    var c1 = makeAddUserCommand(9801);
    c1.timestamp = std.math.maxInt(i64);
    runtime.stampBatchMetadata(&.{&c1});

    // Later commands do not slide the group deadline, and arbitrary client
    // timestamp direction or magnitude does not participate in grouping.
    fake_clock.now_ns = 140;
    var c2 = makeAddUserCommand(9802);
    c2.timestamp = std.math.minInt(i64);
    runtime.stampBatchMetadata(&.{&c2});

    fake_clock.now_ns = 150;
    var c3 = makeAddUserCommand(9803);
    c3.timestamp = std.math.minInt(i64);
    runtime.stampBatchMetadata(&.{&c3});

    // No sleep is needed: the next command observes that idle awake time is
    // strictly beyond the original group's deadline.
    fake_clock.now_ns = 151;
    var c4 = makeAddUserCommand(9804);
    c4.timestamp = std.math.minInt(i64);
    runtime.stampBatchMetadata(&.{&c4});

    try std.testing.expectEqual(@as(i64, 0), c1.events_group);
    try std.testing.expectEqual(@as(i64, 0), c2.events_group);
    try std.testing.expectEqual(@as(i64, 0), c3.events_group);
    try std.testing.expectEqual(@as(i64, 1), c4.events_group);
    try std.testing.expectEqual(@as(?u64, 201), runtime.group_deadline_ns);
}

test "exchange runtime requires batch sequence start metadata" {
    const ok: kernel_mod.BatchResponse = .{ .sequence_from = 77 };
    try std.testing.expectEqual(@as(u64, 77), try requireBatchSequenceFrom(ok));

    const missing: kernel_mod.BatchResponse = .{};
    try std.testing.expectError(
        error.BatchMissingSequence,
        requireBatchSequenceFrom(missing),
    );
}

test "exchange runtime boundary commands split preceding batches deterministically" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .max_batch_size = 32,
    });
    defer runtime.deinit();

    const reset_cmd: cmd.OrderCommand = .{
        .command = .reset,
        .result_code = .new,
    };
    const persist_matching_cmd: cmd.OrderCommand = .{
        .command = .persist_state_matching,
        .result_code = .new,
    };
    const binary_boundary_cmd: cmd.OrderCommand = .{
        .command = .binary_data_command,
        .symbol = -1,
        .result_code = .new,
    };

    try runtime.enqueue(makeAddUserCommand(9901));
    try runtime.enqueue(reset_cmd);
    try runtime.enqueue(makeAddUserCommand(9902));
    try runtime.enqueue(persist_matching_cmd);
    try runtime.enqueue(makeAddUserCommand(9903));
    try runtime.enqueue(binary_boundary_cmd);
    try runtime.enqueue(makeAddUserCommand(9904));

    try std.testing.expectEqual(@as(usize, 1), runtime.batchLenForPending(0));
    try std.testing.expectEqual(@as(usize, 2), runtime.batchLenForPending(1));
    try std.testing.expectEqual(@as(usize, 2), runtime.batchLenForPending(3));
    try std.testing.expectEqual(@as(usize, 2), runtime.batchLenForPending(5));

    const outcome = try runtime.drain();
    try std.testing.expectEqual(@as(usize, 4), outcome.groups_processed);
    try std.testing.expectEqual(@as(usize, 7), outcome.commands_processed);
    try std.testing.expectEqual(@as(usize, 0), runtime.pendingCount());
}

test "exchange runtime combines periodic and timed l2 triggers with overflow-safe scheduling" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var fake_clock: FakeMonotonicClock = .{ .now_ns = std.math.maxInt(u64) };
    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .max_batch_size = 16,
        .l2_flags_every_n_commands = 2,
        .l2_publish_interval_ns = 100,
        .clock = fake_clock.source(),
    });
    defer runtime.deinit();

    runtime.commands_since_l2_flag = std.math.maxInt(usize);
    runtime.l2_next_publish_deadline_ns = std.math.maxInt(u64) - 1;

    var c1 = makeAddUserCommand(9951);
    c1.timestamp = std.math.maxInt(i64);
    var c2 = makeAddUserCommand(9952);
    c2.timestamp = std.math.maxInt(i64);
    var c3 = makeAddUserCommand(9953);
    c3.timestamp = std.math.maxInt(i64);
    var pointers = [_]*cmd.OrderCommand{ &c1, &c2, &c3 };
    runtime.stampBatchMetadata(pointers[0..]);

    try std.testing.expectEqual(@as(i32, 1), c1.service_flags);
    try std.testing.expectEqual(@as(i32, 1), c2.service_flags);
    try std.testing.expectEqual(@as(i32, 0), c3.service_flags);
    try std.testing.expectEqual(@as(u64, std.math.maxInt(u64)), runtime.l2_next_publish_deadline_ns.?);
    try std.testing.expectEqual(@as(usize, 1), runtime.commands_since_l2_flag);
}

test "exchange runtime elapsed deadline and boundary command both increment groups" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var fake_clock: FakeMonotonicClock = .{ .now_ns = 100 };
    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .max_batch_size = 16,
        .max_group_duration_ns = 50,
        .l2_publish_interval_ns = 0,
        .clock = fake_clock.source(),
    });
    defer runtime.deinit();

    var c1 = makeAddUserCommand(9981);
    runtime.stampBatchMetadata(&.{&c1});

    fake_clock.now_ns = 151;
    var c2: cmd.OrderCommand = .{
        .command = .reset,
        .result_code = .new,
    };
    runtime.stampBatchMetadata(&.{&c2});

    fake_clock.now_ns = 152;
    var c3 = makeAddUserCommand(9982);
    runtime.stampBatchMetadata(&.{&c3});

    try std.testing.expectEqual(@as(i64, 0), c1.events_group);
    try std.testing.expectEqual(@as(i64, 2), c2.events_group);
    try std.testing.expectEqual(@as(i64, 2), c3.events_group);
}
