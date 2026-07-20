const std = @import("std");
const cmd = @import("cmd.zig");
const kernel_mod = @import("exchange_kernel.zig");

pub const DefaultL2PublishIntervalNs: u64 = 10_000_000;

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

pub const Config = struct {
    max_batch_size: usize = 256,
    l2_flags_every_n_commands: usize = 0,
    l2_publish_interval_ns: u64 = DefaultL2PublishIntervalNs,
    max_group_duration_ns: u64 = 0,
    sink: ResultSink = .{},
};

pub const InitError = error{
    InvalidBatchSize,
};

pub const DrainError = std.mem.Allocator.Error || kernel_mod.ExecuteError || error{
    BatchMissingSequence,
};

pub const DrainOutcome = struct {
    groups_processed: usize = 0,
    commands_processed: usize = 0,
    last_merged_result: cmd.CommandResultCode = .accepted,
};

pub const ExchangeRuntime = struct {
    allocator: std.mem.Allocator,
    kernel: *kernel_mod.ExchangeKernel,
    max_batch_size: usize,
    l2_flags_every_n_commands: usize,
    l2_publish_interval_ns: u64,
    commands_since_l2_flag: usize = 0,
    l2_next_publish_timestamp_ns: ?i64 = null,
    max_group_duration_ns: u64,
    last_stamped_timestamp_ns: ?i64 = null,
    sink: ResultSink,
    processing_enabled: bool = true,
    grouping_enabled: bool = true,
    group_counter: i64 = 0,
    msgs_in_group: usize = 0,
    pending: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty,

    pub fn init(
        allocator: std.mem.Allocator,
        kernel: *kernel_mod.ExchangeKernel,
        config: Config,
    ) InitError!ExchangeRuntime {
        if (config.max_batch_size == 0) {
            return error.InvalidBatchSize;
        }
        return .{
            .allocator = allocator,
            .kernel = kernel,
            .max_batch_size = config.max_batch_size,
            .l2_flags_every_n_commands = config.l2_flags_every_n_commands,
            .l2_publish_interval_ns = config.l2_publish_interval_ns,
            .max_group_duration_ns = config.max_group_duration_ns,
            .sink = config.sink,
        };
    }

    pub fn deinit(self: *ExchangeRuntime) void {
        self.pending.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn enqueue(self: *ExchangeRuntime, command: cmd.OrderCommand) std.mem.Allocator.Error!void {
        try self.pending.append(self.allocator, command);
    }

    pub fn pendingCount(self: *const ExchangeRuntime) usize {
        return self.pending.items.len;
    }

    pub fn batchLenForPending(self: *const ExchangeRuntime, start: usize) usize {
        std.debug.assert(start < self.pending.items.len);
        const limit = if (self.grouping_enabled) self.max_batch_size else 1;
        std.debug.assert(limit > 0);

        var len: usize = 0;
        while (start + len < self.pending.items.len and len < limit) {
            const command = &self.pending.items[start + len];
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
        var start: usize = 0;

        while (start < self.pending.items.len) {
            const batch_len = self.batchLenForPending(start);
            var pointers = try self.allocator.alloc(*cmd.OrderCommand, batch_len);
            defer self.allocator.free(pointers);

            for (0..batch_len) |idx| {
                pointers[idx] = &self.pending.items[start + idx];
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

            start += batch_len;
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
            command.events_group = 0;
            command.service_flags = 0;
            self.last_stamped_timestamp_ns = command.timestamp;
            return;
        }

        if (self.shouldSplitForTimestampGap(command)) {
            self.group_counter += 1;
            self.msgs_in_group = 0;
        }

        if (isGroupBoundaryCommand(command)) {
            self.group_counter += 1;
            self.msgs_in_group = 0;
        }

        command.events_group = self.group_counter;
        command.service_flags = self.nextL2ServiceFlags(command.timestamp);

        self.msgs_in_group += 1;
        if (self.msgs_in_group >= self.max_batch_size and command.command != .persist_state_risk) {
            self.group_counter += 1;
            self.msgs_in_group = 0;
        }
        self.last_stamped_timestamp_ns = command.timestamp;
    }

    fn nextL2ServiceFlags(self: *ExchangeRuntime, timestamp_ns: i64) i32 {
        const timed_trigger = self.nextTimedL2ServiceFlag(timestamp_ns);
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

    fn nextTimedL2ServiceFlag(self: *ExchangeRuntime, timestamp_ns: i64) bool {
        if (self.l2_publish_interval_ns == 0) {
            return false;
        }

        const interval_ns = std.math.cast(i64, self.l2_publish_interval_ns) orelse std.math.maxInt(i64);
        const deadline = self.l2_next_publish_timestamp_ns orelse {
            self.l2_next_publish_timestamp_ns = safeAddTimestampNs(timestamp_ns, interval_ns);
            return false;
        };

        if (timestamp_ns <= deadline) {
            return false;
        }
        self.l2_next_publish_timestamp_ns = safeAddTimestampNs(timestamp_ns, interval_ns);
        return true;
    }

    fn shouldSplitForTimestampGap(self: *const ExchangeRuntime, command: *const cmd.OrderCommand) bool {
        if (self.max_group_duration_ns == 0) return false;
        if (self.msgs_in_group == 0) return false;

        const previous = self.last_stamped_timestamp_ns orelse return false;
        const current = command.timestamp;
        if (current <= previous) return false;

        const gap = std.math.cast(u64, current - previous) orelse return false;
        return gap > self.max_group_duration_ns;
    }
};

fn safeAddTimestampNs(base_ns: i64, delta_ns: i64) i64 {
    return std.math.add(i64, base_ns, delta_ns) catch std.math.maxInt(i64);
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

test "exchange runtime stamps java-style time-based l2 service flags from timestamps" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .max_batch_size = 8,
        .l2_publish_interval_ns = 100,
    });
    defer runtime.deinit();

    var c1 = makeAddUserCommand(9751);
    c1.timestamp = 10;
    var c2 = makeAddUserCommand(9752);
    c2.timestamp = 50;
    var c3 = makeAddUserCommand(9753);
    c3.timestamp = 111;
    var c4 = makeAddUserCommand(9754);
    c4.timestamp = 211;
    var c5 = makeAddUserCommand(9755);
    c5.timestamp = 212;
    var pointers = [_]*cmd.OrderCommand{ &c1, &c2, &c3, &c4, &c5 };
    runtime.stampBatchMetadata(pointers[0..]);

    try std.testing.expectEqual(@as(i32, 0), c1.service_flags);
    try std.testing.expectEqual(@as(i32, 0), c2.service_flags);
    try std.testing.expectEqual(@as(i32, 1), c3.service_flags);
    try std.testing.expectEqual(@as(i32, 0), c4.service_flags);
    try std.testing.expectEqual(@as(i32, 1), c5.service_flags);
}

test "exchange runtime creates deterministic group boundary from timestamp gaps" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .max_batch_size = 16,
        .max_group_duration_ns = 50,
    });
    defer runtime.deinit();

    var c1 = makeAddUserCommand(9801);
    c1.timestamp = 100;
    var c2 = makeAddUserCommand(9802);
    c2.timestamp = 130;
    var c3 = makeAddUserCommand(9803);
    c3.timestamp = 205;
    var c4 = makeAddUserCommand(9804);
    c4.timestamp = 210;
    var pointers = [_]*cmd.OrderCommand{ &c1, &c2, &c3, &c4 };
    runtime.stampBatchMetadata(pointers[0..]);

    try std.testing.expectEqual(@as(i64, 0), c1.events_group);
    try std.testing.expectEqual(@as(i64, 0), c2.events_group);
    try std.testing.expectEqual(@as(i64, 1), c3.events_group);
    try std.testing.expectEqual(@as(i64, 1), c4.events_group);
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
        .result_code = .@"new",
    };
    const persist_matching_cmd: cmd.OrderCommand = .{
        .command = .persist_state_matching,
        .result_code = .@"new",
    };
    const binary_boundary_cmd: cmd.OrderCommand = .{
        .command = .binary_data_command,
        .symbol = -1,
        .result_code = .@"new",
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

    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .max_batch_size = 16,
        .l2_flags_every_n_commands = 2,
        .l2_publish_interval_ns = 100,
    });
    defer runtime.deinit();

    runtime.commands_since_l2_flag = std.math.maxInt(usize);
    runtime.l2_next_publish_timestamp_ns = std.math.maxInt(i64) - 1;

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
    try std.testing.expectEqual(@as(i64, std.math.maxInt(i64)), runtime.l2_next_publish_timestamp_ns.?);
    try std.testing.expectEqual(@as(usize, 1), runtime.commands_since_l2_flag);
}

test "exchange runtime timestamp gap and boundary command increment groups deterministically" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var runtime = try ExchangeRuntime.init(allocator, &kernel, .{
        .max_batch_size = 16,
        .max_group_duration_ns = 50,
    });
    defer runtime.deinit();

    var c1 = makeAddUserCommand(9981);
    c1.timestamp = 100;
    var c2: cmd.OrderCommand = .{
        .command = .reset,
        .timestamp = 200,
        .result_code = .@"new",
    };
    var c3 = makeAddUserCommand(9982);
    c3.timestamp = 201;
    var pointers = [_]*cmd.OrderCommand{ &c1, &c2, &c3 };
    runtime.stampBatchMetadata(pointers[0..]);

    try std.testing.expectEqual(@as(i64, 0), c1.events_group);
    try std.testing.expectEqual(@as(i64, 2), c2.events_group);
    try std.testing.expectEqual(@as(i64, 2), c3.events_group);
}
