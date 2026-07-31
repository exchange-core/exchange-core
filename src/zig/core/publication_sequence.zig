const std = @import("std");

/// A core-wide command publication identity. This is intentionally distinct
/// from request/correlation IDs: detached work has a publication sequence but
/// no request ID, while request IDs are process-local result handles.
///
/// The native Zig persistence format is already 1-based (`0` means no command
/// has been published), so the first reserved sequence is `1`. Java's
/// Disruptor cursor is 0-based, but the observable invariant we preserve is
/// one unique monotonic identity per visible command slot.
pub const PublicationSequence = enum(u64) {
    _,

    pub fn fromInt(value: u64) PublicationSequence {
        return @enumFromInt(value);
    }

    pub fn toInt(self: PublicationSequence) u64 {
        return @intFromEnum(self);
    }
};

pub const RequestId = enum(u64) {
    _,

    pub fn fromInt(value: u64) RequestId {
        return @enumFromInt(value);
    }

    pub fn toInt(self: RequestId) u64 {
        return @intFromEnum(self);
    }
};

pub const PublicationRange = struct {
    first: PublicationSequence,
    last: PublicationSequence,
    count: usize,

    pub fn at(self: PublicationRange, index: usize) PublicationSequence {
        std.debug.assert(index < self.count);
        return .fromInt(self.first.toInt() + @as(u64, @intCast(index)));
    }
};

pub const PrepareError = error{
    EmptyPublicationRange,
    PublicationRangeTooLarge,
    PublicationSequenceOverflow,
};

pub const ObserveError = error{
    NonMonotonicPublicationSequence,
};

/// A failure-atomic high-water allocator. `prepare` is read-only so callers can
/// complete every fallible queue/callback/order-ID preflight first. `commit`
/// is then infallible while the caller still owns the external driver lock.
pub const PublicationSequencer = struct {
    last: u64 = 0,

    pub fn init(last: u64) PublicationSequencer {
        return .{ .last = last };
    }

    pub fn highWater(self: *const PublicationSequencer) u64 {
        return self.last;
    }

    pub fn prepare(
        self: *const PublicationSequencer,
        count: usize,
    ) PrepareError!PublicationRange {
        if (count == 0) return error.EmptyPublicationRange;
        const count_u64 = std.math.cast(u64, count) orelse
            return error.PublicationRangeTooLarge;
        const range_last = std.math.add(u64, self.last, count_u64) catch
            return error.PublicationSequenceOverflow;
        return .{
            .first = .fromInt(self.last + 1),
            .last = .fromInt(range_last),
            .count = count,
        };
    }

    pub fn commit(self: *PublicationSequencer, prepared: PublicationRange) void {
        std.debug.assert(prepared.count != 0);
        std.debug.assert(prepared.first.toInt() == self.last + 1);
        std.debug.assert(
            prepared.last.toInt() - prepared.first.toInt() + 1 ==
                @as(u64, @intCast(prepared.count)),
        );
        self.last = prepared.last.toInt();
    }

    /// Advances an execution/replay observer to an assigned sequence. Gaps are
    /// allowed because non-mutating commands may be absent from a journal, but
    /// duplicates and regressions are never valid.
    pub fn observe(
        self: *PublicationSequencer,
        sequence: PublicationSequence,
    ) ObserveError!void {
        const value = sequence.toInt();
        if (value <= self.last) return error.NonMonotonicPublicationSequence;
        self.last = value;
    }

    /// Cold restore/bootstrap owns the entire core, so replacing the high-water
    /// is explicit rather than pretending it is an ordinary observation.
    pub fn restore(self: *PublicationSequencer, last: u64) void {
        self.last = last;
    }
};

test "publication sequencer prepares ranges without consuming on failure" {
    var sequencer = PublicationSequencer.init(41);

    const pair = try sequencer.prepare(2);
    try std.testing.expectEqual(@as(u64, 42), pair.first.toInt());
    try std.testing.expectEqual(@as(u64, 43), pair.last.toInt());
    try std.testing.expectEqual(@as(u64, 42), pair.at(0).toInt());
    try std.testing.expectEqual(@as(u64, 43), pair.at(1).toInt());
    try std.testing.expectEqual(@as(u64, 41), sequencer.highWater());

    try std.testing.expectError(error.EmptyPublicationRange, sequencer.prepare(0));
    try std.testing.expectEqual(@as(u64, 41), sequencer.highWater());

    sequencer.commit(pair);
    try std.testing.expectEqual(@as(u64, 43), sequencer.highWater());
}

test "publication sequencer overflow and non-monotonic observations are typed" {
    var exhausted = PublicationSequencer.init(std.math.maxInt(u64));
    try std.testing.expectError(
        error.PublicationSequenceOverflow,
        exhausted.prepare(1),
    );
    try std.testing.expectEqual(std.math.maxInt(u64), exhausted.highWater());

    var observer = PublicationSequencer.init(7);
    try observer.observe(.fromInt(10));
    try std.testing.expectEqual(@as(u64, 10), observer.highWater());
    try std.testing.expectError(
        error.NonMonotonicPublicationSequence,
        observer.observe(.fromInt(10)),
    );
    try std.testing.expectError(
        error.NonMonotonicPublicationSequence,
        observer.observe(.fromInt(9)),
    );
}
