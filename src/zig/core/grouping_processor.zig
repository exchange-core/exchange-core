const std = @import("std");
const cmd = @import("cmd.zig");

pub const default_l2_publish_interval_ns: u64 = 10_000_000;

pub const Config = struct {
    max_messages_in_group: usize,
    max_group_duration_ns: u64,
    l2_publish_interval_ns: u64 = default_l2_publish_interval_ns,
    /// A deterministic Zig extension used by existing simulations. Java leaves
    /// this at zero and requests L2 data only after an idle-time deadline.
    l2_flags_every_n_commands: usize = 0,

    pub const ValidationError = error{InvalidGroupSize};

    pub fn validate(self: Config) ValidationError!void {
        if (self.max_messages_in_group == 0) return error.InvalidGroupSize;
    }
};

pub const StampDisposition = enum {
    stamped,
    /// Java's GROUPING_CONTROL OFF path deliberately leaves caller-supplied
    /// events-group and service-flag metadata untouched.
    passthrough,
};

/// Deterministic state machine for Java's `GroupingProcessor` metadata stage.
///
/// Call `stampLive` for each command in one contiguous available ring-buffer
/// batch, then call `finishAvailableBatch` exactly once. Call `observeIdle`
/// only when no command is available. Keeping idle observation explicit is
/// important: Java does not split groups or arm timed L2 requests while a
/// continuously available batch is being consumed.
pub const State = struct {
    max_messages_in_group: usize,
    max_group_duration_ns: u64,
    l2_publish_interval_ns: u64,
    l2_flags_every_n_commands: usize,

    grouping_enabled: bool = true,
    group_counter: i64 = 0,
    messages_in_group: usize = 0,
    group_last_ns: u64 = 0,
    l2_last_ns: u64 = 0,
    trigger_l2_request: bool = false,
    commands_since_periodic_l2: usize = 0,
    last_observed_ns: u64 = 0,

    pub fn init(config: Config) Config.ValidationError!State {
        try config.validate();
        return .{
            .max_messages_in_group = config.max_messages_in_group,
            .max_group_duration_ns = config.max_group_duration_ns,
            .l2_publish_interval_ns = config.l2_publish_interval_ns,
            .l2_flags_every_n_commands = config.l2_flags_every_n_commands,
        };
    }

    pub fn stampLive(self: *State, command: *cmd.OrderCommand) StampDisposition {
        if (command.command == .grouping_control) {
            self.grouping_enabled = command.order_id == 1;
        }

        if (!self.grouping_enabled) return .passthrough;

        if (isGroupBoundary(command)) {
            self.beginNextGroup();
        }

        command.events_group = self.group_counter;
        const timed_l2 = self.takeL2Request();
        const periodic_l2 = self.takePeriodicL2Request();
        command.service_flags = if (timed_l2 or periodic_l2) 1 else 0;

        self.messages_in_group += 1;
        // The two-command persistence barrier must keep matching S and risk
        // S+1 in the same group even when S reaches the configured group size.
        if (self.messages_in_group >= self.max_messages_in_group and
            command.command != .persist_state_risk)
        {
            self.beginNextGroup();
        }
        return .stamped;
    }

    pub fn stampLiveBatch(
        self: *State,
        commands: []const *cmd.OrderCommand,
        observed_after_batch_ns: u64,
    ) void {
        for (commands) |command| _ = self.stampLive(command);
        self.finishAvailableBatch(observed_after_batch_ns);
    }

    /// Mirrors the assignment performed after Java consumes the currently
    /// available sequence range, including when grouping is disabled.
    pub fn finishAvailableBatch(self: *State, observed_ns: u64) void {
        const now_ns = self.monotonic(observed_ns);
        self.group_last_ns = saturatingAdd(now_ns, self.max_group_duration_ns);
    }

    /// Mirrors Java's no-event branch. Deadline equality does not trigger;
    /// `System.nanoTime()` must advance strictly beyond the stored deadline.
    pub fn observeIdle(self: *State, observed_ns: u64) void {
        const now_ns = self.monotonic(observed_ns);
        if (self.messages_in_group > 0 and now_ns > self.group_last_ns) {
            self.beginNextGroup();
        }

        if (self.l2_publish_interval_ns != 0 and now_ns > self.l2_last_ns) {
            self.l2_last_ns = saturatingAdd(now_ns, self.l2_publish_interval_ns);
            self.trigger_l2_request = true;
        }
    }

    pub fn groupingEnabled(self: *const State) bool {
        return self.grouping_enabled;
    }

    fn beginNextGroup(self: *State) void {
        self.group_counter +%= 1;
        self.messages_in_group = 0;
    }

    fn takeL2Request(self: *State) bool {
        if (!self.trigger_l2_request) return false;
        self.trigger_l2_request = false;
        return true;
    }

    fn takePeriodicL2Request(self: *State) bool {
        if (self.l2_flags_every_n_commands == 0) return false;
        self.commands_since_periodic_l2 +%= 1;
        if (self.commands_since_periodic_l2 < self.l2_flags_every_n_commands) return false;
        self.commands_since_periodic_l2 = 0;
        return true;
    }

    fn monotonic(self: *State, observed_ns: u64) u64 {
        self.last_observed_ns = @max(self.last_observed_ns, observed_ns);
        return self.last_observed_ns;
    }
};

pub fn isGroupBoundary(command: *const cmd.OrderCommand) bool {
    return switch (command.command) {
        .reset,
        .persist_state_matching,
        .grouping_control,
        => true,
        .binary_data_command, .binary_data_query => command.symbol == -1,
        else => false,
    };
}

fn saturatingAdd(base: u64, duration: u64) u64 {
    return std.math.add(u64, base, duration) catch std.math.maxInt(u64);
}

fn makeCommand(command_type: cmd.OrderCommandType) cmd.OrderCommand {
    return .{
        .command = command_type,
        .result_code = .new,
    };
}

test "grouping processor stamps Java boundary and message-limit groups" {
    var state = try State.init(.{
        .max_messages_in_group = 3,
        .max_group_duration_ns = 100,
        .l2_publish_interval_ns = 0,
    });

    var first = makeCommand(.nop);
    var second = makeCommand(.nop);
    var third = makeCommand(.nop);
    var after_limit = makeCommand(.nop);
    var reset = makeCommand(.reset);

    try std.testing.expectEqual(StampDisposition.stamped, state.stampLive(&first));
    try std.testing.expectEqual(StampDisposition.stamped, state.stampLive(&second));
    try std.testing.expectEqual(StampDisposition.stamped, state.stampLive(&third));
    try std.testing.expectEqual(StampDisposition.stamped, state.stampLive(&after_limit));
    try std.testing.expectEqual(StampDisposition.stamped, state.stampLive(&reset));

    try std.testing.expectEqual(@as(i64, 0), first.events_group);
    try std.testing.expectEqual(@as(i64, 0), second.events_group);
    try std.testing.expectEqual(@as(i64, 0), third.events_group);
    try std.testing.expectEqual(@as(i64, 1), after_limit.events_group);
    try std.testing.expectEqual(@as(i64, 2), reset.events_group);
}

test "persist risk defers the next rollover after Java's matching boundary" {
    var state = try State.init(.{
        .max_messages_in_group = 1,
        .max_group_duration_ns = 0,
        .l2_publish_interval_ns = 0,
    });

    var matching = makeCommand(.persist_state_matching);
    var risk = makeCommand(.persist_state_risk);
    _ = state.stampLive(&matching);
    _ = state.stampLive(&risk);

    // Matching first establishes its boundary and can reach the size limit.
    // Risk is stamped in the following group, but does not roll that group a
    // second time; the next ordinary command remains in risk's group.
    try std.testing.expectEqual(@as(i64, 1), matching.events_group);
    try std.testing.expectEqual(@as(i64, 2), risk.events_group);
    try std.testing.expectEqual(@as(usize, 1), state.messages_in_group);

    var next = makeCommand(.nop);
    _ = state.stampLive(&next);
    try std.testing.expectEqual(@as(i64, 2), next.events_group);
}

test "grouping control off preserves metadata and on resumes Java stamping" {
    var state = try State.init(.{
        .max_messages_in_group = 16,
        .max_group_duration_ns = 10,
        .l2_publish_interval_ns = 0,
    });

    var disable = makeCommand(.grouping_control);
    disable.order_id = 0;
    disable.events_group = 77;
    disable.service_flags = 5;
    try std.testing.expectEqual(StampDisposition.passthrough, state.stampLive(&disable));
    try std.testing.expectEqual(@as(i64, 77), disable.events_group);
    try std.testing.expectEqual(@as(i32, 5), disable.service_flags);

    var raw = makeCommand(.place_order);
    raw.events_group = 88;
    raw.service_flags = 7;
    try std.testing.expectEqual(StampDisposition.passthrough, state.stampLive(&raw));
    try std.testing.expectEqual(@as(i64, 88), raw.events_group);
    try std.testing.expectEqual(@as(i32, 7), raw.service_flags);

    var enable = makeCommand(.grouping_control);
    enable.order_id = 1;
    try std.testing.expectEqual(StampDisposition.stamped, state.stampLive(&enable));
    try std.testing.expectEqual(@as(i64, 1), enable.events_group);
    try std.testing.expectEqual(@as(i32, 0), enable.service_flags);
}

test "idle deadlines split groups and arm exactly one next-command L2 request" {
    var state = try State.init(.{
        .max_messages_in_group = 16,
        .max_group_duration_ns = 10,
        .l2_publish_interval_ns = 25,
    });

    var first = makeCommand(.nop);
    _ = state.stampLive(&first);
    state.finishAvailableBatch(100);

    state.observeIdle(100);
    var l2 = makeCommand(.nop);
    _ = state.stampLive(&l2);
    var no_l2 = makeCommand(.nop);
    _ = state.stampLive(&no_l2);
    try std.testing.expectEqual(@as(i32, 1), l2.service_flags);
    try std.testing.expectEqual(@as(i32, 0), no_l2.service_flags);

    state.finishAvailableBatch(100);
    state.observeIdle(110);
    try std.testing.expectEqual(@as(usize, 3), state.messages_in_group);
    state.observeIdle(111);
    try std.testing.expectEqual(@as(usize, 0), state.messages_in_group);

    var next_group = makeCommand(.nop);
    _ = state.stampLive(&next_group);
    try std.testing.expectEqual(@as(i64, 1), next_group.events_group);

    state.observeIdle(125);
    var still_no_l2 = makeCommand(.nop);
    _ = state.stampLive(&still_no_l2);
    try std.testing.expectEqual(@as(i32, 0), still_no_l2.service_flags);
    state.observeIdle(126);
    var next_l2 = makeCommand(.nop);
    _ = state.stampLive(&next_l2);
    try std.testing.expectEqual(@as(i32, 1), next_l2.service_flags);
}

test "continuous availability does not synthesize Java timed L2 flags" {
    var state = try State.init(.{
        .max_messages_in_group = 32,
        .max_group_duration_ns = 1,
        .l2_publish_interval_ns = 10,
    });

    var commands: [8]cmd.OrderCommand = undefined;
    for (&commands) |*command| {
        command.* = makeCommand(.nop);
        _ = state.stampLive(command);
        try std.testing.expectEqual(@as(i32, 0), command.service_flags);
    }
    state.finishAvailableBatch(1_000_000);
}

test "periodic Zig extension is deterministic and monotonic time clamps regressions" {
    var state = try State.init(.{
        .max_messages_in_group = 32,
        .max_group_duration_ns = 5,
        .l2_publish_interval_ns = 0,
        .l2_flags_every_n_commands = 2,
    });

    var first = makeCommand(.nop);
    var second = makeCommand(.nop);
    _ = state.stampLive(&first);
    _ = state.stampLive(&second);
    try std.testing.expectEqual(@as(i32, 0), first.service_flags);
    try std.testing.expectEqual(@as(i32, 1), second.service_flags);

    state.finishAvailableBatch(100);
    state.observeIdle(90);
    try std.testing.expectEqual(@as(usize, 2), state.messages_in_group);
    state.observeIdle(106);
    try std.testing.expectEqual(@as(usize, 0), state.messages_in_group);
}
