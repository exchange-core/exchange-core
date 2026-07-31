const std = @import("std");
const builtin = @import("builtin");
const core = @import("exchange_core").core;

const wire = core.java_binary_wire;

const MaxTrials: usize = 101;
const MaxIterations: usize = 10_000_000;
const CalibrationFloorNs: u64 = 2 * std.time.ns_per_ms;

const BenchError = wire.CarrierPackError || wire.CarrierUnpackError || error{
    InvalidArguments,
    MissingArgumentValue,
    ImplementationMismatch,
    CalibrationOverflow,
    ReleaseFastRequired,
};

const Implementation = enum {
    scalar_source,
    scalar_direct,
    production,

    fn label(self: Implementation) []const u8 {
        return switch (self) {
            .scalar_source => "scalar_source",
            .scalar_direct => "scalar_direct",
            .production => "production",
        };
    }
};

const CounterbalancedOrders = [_][3]Implementation{
    .{ .scalar_source, .scalar_direct, .production },
    .{ .scalar_direct, .production, .scalar_source },
    .{ .production, .scalar_source, .scalar_direct },
    .{ .scalar_source, .production, .scalar_direct },
    .{ .production, .scalar_direct, .scalar_source },
    .{ .scalar_direct, .scalar_source, .production },
};

const Operation = enum {
    pack,
    unpack,

    fn label(self: Operation) []const u8 {
        return @tagName(self);
    }
};

const Options = struct {
    bytes: usize = 1024 * 1024,
    warmup_rounds: usize = 3,
    trials: usize = 12,
    min_trial_ms: u64 = 100,

    fn validate(self: Options) BenchError!void {
        if (self.bytes == 0 or
            self.warmup_rounds > 1000 or
            self.trials == 0 or
            self.trials > MaxTrials or
            self.min_trial_ms == 0 or
            self.min_trial_ms > 10_000)
        {
            return error.InvalidArguments;
        }
    }

    fn targetNs(self: Options) BenchError!u64 {
        return std.math.mul(u64, self.min_trial_ms, std.time.ns_per_ms) catch
            return error.CalibrationOverflow;
    }
};

const ParseOutcome = union(enum) {
    help,
    options: Options,
};

const Measurement = struct {
    iterations: usize,
    elapsed_ns: u64,
    bytes_per_second: f64,
    checksum: u64,
};

const OperationSamples = struct {
    scalar_source: [MaxTrials]f64 = undefined,
    scalar_direct: [MaxTrials]f64 = undefined,
    production: [MaxTrials]f64 = undefined,
    production_vs_source_speedup: [MaxTrials]f64 = undefined,
    production_vs_direct_speedup: [MaxTrials]f64 = undefined,
};

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.smp_allocator;
    const parsed = try parseOptions(init.minimal.args, allocator);

    var stdout_buffer: [16 * 1024]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;

    switch (parsed) {
        .help => {
            try writeUsage(out);
            try out.flush();
            return;
        },
        .options => |options| {
            try runBenchmark(init.io, allocator, out, options);
            try out.flush();
        },
    }
}

fn runBenchmark(
    io: std.Io,
    allocator: std.mem.Allocator,
    out: *std.Io.Writer,
    options: Options,
) !void {
    try requireReleaseFast();
    try options.validate();
    const target_ns = try options.targetNs();

    const input = try allocator.alloc(u8, options.bytes);
    defer allocator.free(input);
    for (input, 0..) |*byte, index| {
        byte.* = @truncate(index *% 131 +% 17);
    }

    const scalar_words = try packScalarSource(allocator, input);
    defer allocator.free(scalar_words);
    const direct_words = try packScalarDirect(allocator, input);
    defer allocator.free(direct_words);
    const production_words = try wire.packSignedWordsBigEndian(allocator, input);
    defer allocator.free(production_words);
    if (!std.mem.eql(i64, scalar_words, direct_words) or
        !std.mem.eql(i64, scalar_words, production_words))
    {
        return error.ImplementationMismatch;
    }

    const scalar_bytes = try unpackScalarSource(allocator, scalar_words);
    defer allocator.free(scalar_bytes);
    const direct_bytes = try unpackScalarDirect(allocator, scalar_words);
    defer allocator.free(direct_bytes);
    const production_bytes = try wire.unpackSignedWordsBigEndian(allocator, scalar_words);
    defer allocator.free(production_bytes);
    if (!std.mem.eql(u8, scalar_bytes, direct_bytes) or
        !std.mem.eql(u8, scalar_bytes, production_bytes) or
        !std.mem.eql(u8, input, production_bytes[0..input.len]))
    {
        return error.ImplementationMismatch;
    }

    try out.print(
        "exchange-core SIMD carrier benchmark payload_bytes={d} carrier_words={d} " ++
            "carrier_bytes={d} warmup_rounds={d} trials={d} min_trial_ms={d} " ++
            "vector_lanes_u64={d} build_mode={s}\n",
        .{
            options.bytes,
            scalar_words.len,
            @as(u128, scalar_words.len) * @sizeOf(i64),
            options.warmup_rounds,
            options.trials,
            options.min_trial_ms,
            wire.carrierWordVectorLength() orelse 1,
            @tagName(builtin.mode),
        },
    );
    try out.writeAll(
        "workload=steady_state_repeated_buffer allocator=smp_allocator " ++
            "checksum_pre_touch=one_untimed_full_transform_before_each_timed_row " ++
            "timed_scope=allocate_transform_free\n" ++
            "TIMING_RESULTS_CSV\n",
    );
    try out.writeAll(
        "operation,trial,run_order,implementation,iterations,elapsed_ns,bytes_per_second,checksum\n",
    );

    var pack_samples: OperationSamples = .{};
    var unpack_samples: OperationSamples = .{};
    try benchmarkOperation(
        io,
        allocator,
        out,
        options,
        target_ns,
        .pack,
        input,
        scalar_words,
        &pack_samples,
    );
    try benchmarkOperation(
        io,
        allocator,
        out,
        options,
        target_ns,
        .unpack,
        input,
        scalar_words,
        &unpack_samples,
    );

    try out.writeAll(
        "PAIRED_RESULTS_CSV\n" ++
            "operation,trial,counterbalance_order,production_vs_source_speedup," ++
            "production_vs_direct_speedup\n",
    );
    try printPairedRows(out, .pack, pack_samples, options.trials);
    try printPairedRows(out, .unpack, unpack_samples, options.trials);
    try printSummary(out, .pack, pack_samples, options.trials);
    try printSummary(out, .unpack, unpack_samples, options.trials);
}

fn benchmarkOperation(
    io: std.Io,
    allocator: std.mem.Allocator,
    out: *std.Io.Writer,
    options: Options,
    target_ns: u64,
    operation: Operation,
    input: []const u8,
    words: []const i64,
    samples: *OperationSamples,
) !void {
    for (0..options.warmup_rounds) |round| {
        for (CounterbalancedOrders[round % CounterbalancedOrders.len]) |implementation| {
            _ = try measure(io, allocator, operation, implementation, input, words, 1);
        }
    }

    const source_iterations = try calibrate(
        io,
        allocator,
        operation,
        .scalar_source,
        input,
        words,
        target_ns,
    );
    const direct_iterations = try calibrate(
        io,
        allocator,
        operation,
        .scalar_direct,
        input,
        words,
        target_ns,
    );
    const production_iterations = try calibrate(
        io,
        allocator,
        operation,
        .production,
        input,
        words,
        target_ns,
    );
    const iterations = @max(source_iterations, @max(direct_iterations, production_iterations));

    for (0..options.trials) |trial| {
        const order = CounterbalancedOrders[trial % CounterbalancedOrders.len];

        var checksums: [3]u64 = undefined;
        for (order, 0..) |implementation, run_order| {
            const result = try measure(
                io,
                allocator,
                operation,
                implementation,
                input,
                words,
                iterations,
            );
            checksums[@intFromEnum(implementation)] = result.checksum;
            switch (implementation) {
                .scalar_source => samples.scalar_source[trial] = result.bytes_per_second,
                .scalar_direct => samples.scalar_direct[trial] = result.bytes_per_second,
                .production => samples.production[trial] = result.bytes_per_second,
            }
            try out.print(
                "{s},{d},{d},{s},{d},{d},{d:.3},{x:0>16}\n",
                .{
                    operation.label(),
                    trial + 1,
                    run_order + 1,
                    implementation.label(),
                    result.iterations,
                    result.elapsed_ns,
                    result.bytes_per_second,
                    result.checksum,
                },
            );
        }
        if (checksums[0] != checksums[1] or checksums[0] != checksums[2]) {
            return error.ImplementationMismatch;
        }
        samples.production_vs_source_speedup[trial] =
            samples.production[trial] / samples.scalar_source[trial];
        samples.production_vs_direct_speedup[trial] =
            samples.production[trial] / samples.scalar_direct[trial];
    }
}

fn requireReleaseFast() BenchError!void {
    if (builtin.mode != .ReleaseFast) return error.ReleaseFastRequired;
}

fn calibrate(
    io: std.Io,
    allocator: std.mem.Allocator,
    operation: Operation,
    implementation: Implementation,
    input: []const u8,
    words: []const i64,
    target_ns: u64,
) !usize {
    var iterations: usize = 1;
    var elapsed_ns: u64 = 0;
    while (true) {
        const result = try measure(
            io,
            allocator,
            operation,
            implementation,
            input,
            words,
            iterations,
        );
        elapsed_ns = result.elapsed_ns;
        if (elapsed_ns >= CalibrationFloorNs or iterations == MaxIterations) break;
        iterations = @min(iterations * 2, MaxIterations);
    }

    if (elapsed_ns == 0) return error.CalibrationOverflow;
    const scaled = (@as(u128, target_ns) * iterations + elapsed_ns - 1) / elapsed_ns;
    return @max(1, @min(MaxIterations, std.math.cast(usize, scaled) orelse MaxIterations));
}

fn measure(
    io: std.Io,
    allocator: std.mem.Allocator,
    operation: Operation,
    implementation: Implementation,
    input: []const u8,
    words: []const i64,
    iterations: usize,
) !Measurement {
    const checksum = switch (operation) {
        .pack => blk: {
            const packed_words = try pack(allocator, implementation, input);
            defer allocator.free(packed_words);
            break :blk checksumWords(packed_words);
        },
        .unpack => blk: {
            const unpacked = try unpack(allocator, implementation, words);
            defer allocator.free(unpacked);
            break :blk checksumBytes(unpacked);
        },
    };

    var timed_checksum: u64 = 0;
    const started = std.Io.Clock.awake.now(io);
    switch (operation) {
        .pack => for (0..iterations) |_| {
            const packed_words = try pack(allocator, implementation, input);
            defer allocator.free(packed_words);
            std.mem.doNotOptimizeAway(packed_words.ptr);
            timed_checksum +%= @bitCast(packed_words[0]);
            timed_checksum +%= @bitCast(packed_words[packed_words.len - 1]);
        },
        .unpack => for (0..iterations) |_| {
            const unpacked = try unpack(allocator, implementation, words);
            defer allocator.free(unpacked);
            std.mem.doNotOptimizeAway(unpacked.ptr);
            timed_checksum +%= unpacked[0];
            timed_checksum +%= unpacked[unpacked.len - 1];
        },
    }
    std.mem.doNotOptimizeAway(timed_checksum);
    const elapsed_raw = started.durationTo(std.Io.Clock.awake.now(io)).nanoseconds;
    const elapsed_ns = if (elapsed_raw <= 0)
        1
    else
        std.math.cast(u64, elapsed_raw) orelse std.math.maxInt(u64);
    const bytes_per_iteration: u128 = switch (operation) {
        .pack => input.len,
        .unpack => @as(u128, words.len) * @sizeOf(i64),
    };
    const total_bytes = bytes_per_iteration * iterations;
    const bytes_per_second = @as(f64, @floatFromInt(total_bytes)) *
        @as(f64, std.time.ns_per_s) / @as(f64, @floatFromInt(elapsed_ns));
    return .{
        .iterations = iterations,
        .elapsed_ns = elapsed_ns,
        .bytes_per_second = bytes_per_second,
        .checksum = checksum,
    };
}

fn printPairedRows(
    out: *std.Io.Writer,
    operation: Operation,
    samples: OperationSamples,
    trials: usize,
) !void {
    for (0..trials) |trial| {
        try out.print(
            "{s},{d},{d},{d:.6},{d:.6}\n",
            .{
                operation.label(),
                trial + 1,
                trial % CounterbalancedOrders.len + 1,
                samples.production_vs_source_speedup[trial],
                samples.production_vs_direct_speedup[trial],
            },
        );
    }
}

const Distribution = struct {
    min: f64,
    p05: f64,
    median: f64,
    p95: f64,
    max: f64,

    fn fromSorted(sorted: []const f64) Distribution {
        std.debug.assert(sorted.len > 0);
        return .{
            .min = sorted[0],
            .p05 = nearestRank(sorted, 5),
            .median = median(sorted),
            .p95 = nearestRank(sorted, 95),
            .max = sorted[sorted.len - 1],
        };
    }
};

fn printSummary(
    out: *std.Io.Writer,
    operation: Operation,
    samples: OperationSamples,
    trials: usize,
) !void {
    var scalar_source = samples.scalar_source;
    var scalar_direct = samples.scalar_direct;
    var production = samples.production;
    var production_vs_source_speedup = samples.production_vs_source_speedup;
    var production_vs_direct_speedup = samples.production_vs_direct_speedup;
    std.sort.heap(f64, scalar_source[0..trials], {}, std.sort.asc(f64));
    std.sort.heap(f64, scalar_direct[0..trials], {}, std.sort.asc(f64));
    std.sort.heap(f64, production[0..trials], {}, std.sort.asc(f64));
    std.sort.heap(
        f64,
        production_vs_source_speedup[0..trials],
        {},
        std.sort.asc(f64),
    );
    std.sort.heap(
        f64,
        production_vs_direct_speedup[0..trials],
        {},
        std.sort.asc(f64),
    );
    const source_speedup = Distribution.fromSorted(production_vs_source_speedup[0..trials]);
    const direct_speedup = Distribution.fromSorted(production_vs_direct_speedup[0..trials]);
    try out.print(
        "SUMMARY operation={s} scalar_source_median_Bps={d:.3} " ++
            "scalar_direct_median_Bps={d:.3} production_median_Bps={d:.3} " ++
            "production_vs_source_paired_speedup_min={d:.4} " ++
            "production_vs_source_paired_speedup_p05={d:.4} " ++
            "production_vs_source_paired_speedup_median={d:.4} " ++
            "production_vs_source_paired_speedup_p95={d:.4} " ++
            "production_vs_source_paired_speedup_max={d:.4} " ++
            "production_vs_direct_paired_speedup_min={d:.4} " ++
            "production_vs_direct_paired_speedup_p05={d:.4} " ++
            "production_vs_direct_paired_speedup_median={d:.4} " ++
            "production_vs_direct_paired_speedup_p95={d:.4} " ++
            "production_vs_direct_paired_speedup_max={d:.4}\n",
        .{
            operation.label(),
            median(scalar_source[0..trials]),
            median(scalar_direct[0..trials]),
            median(production[0..trials]),
            source_speedup.min,
            source_speedup.p05,
            source_speedup.median,
            source_speedup.p95,
            source_speedup.max,
            direct_speedup.min,
            direct_speedup.p05,
            direct_speedup.median,
            direct_speedup.p95,
            direct_speedup.max,
        },
    );
}

fn median(sorted: []const f64) f64 {
    const middle = sorted.len / 2;
    if ((sorted.len & 1) == 1) return sorted[middle];
    return (sorted[middle - 1] + sorted[middle]) / 2.0;
}

fn nearestRank(sorted: []const f64, percentile: usize) f64 {
    std.debug.assert(sorted.len > 0);
    std.debug.assert(percentile >= 1 and percentile <= 100);
    const rank = (sorted.len * percentile + 99) / 100;
    return sorted[rank - 1];
}

noinline fn packScalarSource(
    allocator: std.mem.Allocator,
    bytes: []const u8,
) wire.CarrierPackError![]i64 {
    const rounded_bytes = std.math.add(usize, bytes.len, 7) catch return error.CarrierTooLarge;
    const words_without_frame_padding = rounded_bytes / 8;
    const remainder = words_without_frame_padding % wire.words_per_frame;
    const frame_padding = if (remainder == 0) @as(usize, 0) else wire.words_per_frame - remainder;
    const word_count = std.math.add(
        usize,
        words_without_frame_padding,
        frame_padding,
    ) catch return error.CarrierTooLarge;

    const words = try allocator.alloc(i64, word_count);
    @memset(words, 0);
    for (0..words_without_frame_padding) |word_index| {
        var raw = [_]u8{0} ** 8;
        const start = word_index * 8;
        const end = @min(start + 8, bytes.len);
        @memcpy(raw[0 .. end - start], bytes[start..end]);
        words[word_index] = std.mem.readInt(i64, &raw, .big);
    }
    return words;
}

noinline fn unpackScalarSource(
    allocator: std.mem.Allocator,
    words: []const i64,
) wire.CarrierUnpackError![]u8 {
    if (words.len % wire.words_per_frame != 0) {
        return error.CarrierWordCountNotMultipleOfFive;
    }
    const byte_count = std.math.mul(usize, words.len, @sizeOf(i64)) catch
        return error.CarrierTooLarge;
    const bytes = try allocator.alloc(u8, byte_count);
    for (words, 0..) |word, word_index| {
        std.mem.writeInt(i64, bytes[word_index * 8 ..][0..8], word, .big);
    }
    return bytes;
}

noinline fn packScalarDirect(
    allocator: std.mem.Allocator,
    bytes: []const u8,
) wire.CarrierPackError![]i64 {
    const rounded_bytes = std.math.add(usize, bytes.len, 7) catch return error.CarrierTooLarge;
    const words_without_frame_padding = rounded_bytes / 8;
    const remainder = words_without_frame_padding % wire.words_per_frame;
    const frame_padding = if (remainder == 0) @as(usize, 0) else wire.words_per_frame - remainder;
    const word_count = std.math.add(
        usize,
        words_without_frame_padding,
        frame_padding,
    ) catch return error.CarrierTooLarge;

    const words = try allocator.alloc(i64, word_count);

    const complete_words = bytes.len / @sizeOf(i64);
    var word_index: usize = 0;
    while (word_index < complete_words) : (word_index += 1) {
        const start = word_index * @sizeOf(i64);
        words[word_index] = std.mem.readInt(i64, bytes[start..][0..8], .big);
    }
    if (word_index < words_without_frame_padding) {
        var raw = [_]u8{0} ** 8;
        const start = word_index * 8;
        @memcpy(raw[0 .. bytes.len - start], bytes[start..]);
        words[word_index] = std.mem.readInt(i64, &raw, .big);
    }
    @memset(words[words_without_frame_padding..], 0);
    return words;
}

noinline fn unpackScalarDirect(
    allocator: std.mem.Allocator,
    words: []const i64,
) wire.CarrierUnpackError![]u8 {
    if (words.len % wire.words_per_frame != 0) {
        return error.CarrierWordCountNotMultipleOfFive;
    }
    const byte_count = std.math.mul(usize, words.len, @sizeOf(i64)) catch
        return error.CarrierTooLarge;
    const bytes = try allocator.alloc(u8, byte_count);
    var word_index: usize = 0;
    while (word_index < words.len) : (word_index += 1) {
        const word = words[word_index];
        std.mem.writeInt(i64, bytes[word_index * 8 ..][0..8], word, .big);
    }
    return bytes;
}

// Keep the timed call boundary opaque so the compiler must materialize every
// output word even though the harness samples only the edges inside the timer.
noinline fn pack(
    allocator: std.mem.Allocator,
    implementation: Implementation,
    bytes: []const u8,
) wire.CarrierPackError![]i64 {
    return switch (implementation) {
        .scalar_source => packScalarSource(allocator, bytes),
        .scalar_direct => packScalarDirect(allocator, bytes),
        .production => wire.packSignedWordsBigEndian(allocator, bytes),
    };
}

noinline fn unpack(
    allocator: std.mem.Allocator,
    implementation: Implementation,
    words: []const i64,
) wire.CarrierUnpackError![]u8 {
    return switch (implementation) {
        .scalar_source => unpackScalarSource(allocator, words),
        .scalar_direct => unpackScalarDirect(allocator, words),
        .production => wire.unpackSignedWordsBigEndian(allocator, words),
    };
}

fn checksumWords(words: []const i64) u64 {
    var checksum: u64 = 0xcbf29ce484222325;
    for (words) |word| {
        checksum ^= @bitCast(word);
        checksum *%= 0x100000001b3;
    }
    checksum ^= words.len;
    return checksum;
}

fn checksumBytes(bytes: []const u8) u64 {
    var checksum: u64 = 0xcbf29ce484222325;
    for (bytes) |byte| {
        checksum ^= byte;
        checksum *%= 0x100000001b3;
    }
    checksum ^= bytes.len;
    return checksum;
}

fn parseOptions(
    process_args: std.process.Args,
    allocator: std.mem.Allocator,
) !ParseOutcome {
    var args = try std.process.Args.Iterator.initAllocator(process_args, allocator);
    defer args.deinit();
    _ = args.next();

    var options: Options = .{};
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) return .help;
        if (std.mem.eql(u8, arg, "--bytes")) {
            options.bytes = try parseInt(usize, args.next() orelse return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--warmup")) {
            options.warmup_rounds = try parseInt(usize, args.next() orelse return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--trials")) {
            options.trials = try parseInt(usize, args.next() orelse return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--min-trial-ms")) {
            options.min_trial_ms = try parseInt(u64, args.next() orelse return error.MissingArgumentValue);
        } else {
            return error.InvalidArguments;
        }
    }
    try options.validate();
    return .{ .options = options };
}

fn parseInt(comptime T: type, value: []const u8) BenchError!T {
    return std.fmt.parseInt(T, value, 10) catch return error.InvalidArguments;
}

fn writeUsage(out: *std.Io.Writer) !void {
    try out.writeAll(
        \\Usage: exchange-core-simd-bench [options]
        \\
        \\Benchmarks the original scalar source, direct scalar endian loops, and the
        \\production SIMD binary-carrier pack/unpack implementation using paired,
        \\counterbalanced ReleaseFast trials. The workload repeatedly transforms one
        \\buffer and times allocation, transform, and free after an untimed full-output
        \\checksum pre-touch for each measured row.
        \\
        \\Options:
        \\  --bytes N          Logical payload size in bytes (default: 1048576)
        \\  --warmup N         Untimed three-way warmup rounds (default: 3)
        \\  --trials N         Timed three-way trials, max 101 (default: 12)
        \\  --min-trial-ms N   Calibrated minimum trial duration (default: 100)
        \\  -h, --help         Show this help
        \\
    );
}

test "default trials cover two complete counterbalance cycles" {
    const options: Options = .{};
    try std.testing.expectEqual(@as(usize, 12), options.trials);
    try std.testing.expectEqual(@as(usize, 0), options.trials % CounterbalancedOrders.len);
}

test "counterbalance cycle uses every permutation and position evenly" {
    var position_counts: [3][3]usize = .{
        .{ 0, 0, 0 },
        .{ 0, 0, 0 },
        .{ 0, 0, 0 },
    };

    for (CounterbalancedOrders) |order| {
        var seen: [3]bool = .{ false, false, false };
        for (order, 0..) |implementation, position| {
            const implementation_index = @intFromEnum(implementation);
            try std.testing.expect(!seen[implementation_index]);
            seen[implementation_index] = true;
            position_counts[implementation_index][position] += 1;
        }
        try std.testing.expectEqualSlices(bool, &.{ true, true, true }, &seen);
    }

    for (position_counts) |counts| {
        try std.testing.expectEqualSlices(usize, &.{ 2, 2, 2 }, &counts);
    }
}

test "distribution reports nearest-rank tails and even median" {
    var values = [_]f64{ 4.0, 1.0, 3.0, 2.0 };
    std.sort.heap(f64, &values, {}, std.sort.asc(f64));
    const distribution = Distribution.fromSorted(&values);

    try std.testing.expectApproxEqAbs(@as(f64, 1.0), distribution.min, 0.000_001);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), distribution.p05, 0.000_001);
    try std.testing.expectApproxEqAbs(@as(f64, 2.5), distribution.median, 0.000_001);
    try std.testing.expectApproxEqAbs(@as(f64, 4.0), distribution.p95, 0.000_001);
    try std.testing.expectApproxEqAbs(@as(f64, 4.0), distribution.max, 0.000_001);
}

test "benchmark rejects non-ReleaseFast execution" {
    if (builtin.mode == .ReleaseFast) {
        try requireReleaseFast();
    } else {
        try std.testing.expectError(error.ReleaseFastRequired, requireReleaseFast());
    }
}

test "all benchmark implementations agree across vector chunks and a tail" {
    const allocator = std.testing.allocator;
    var input: [65]u8 = undefined;
    for (&input, 0..) |*byte, index| {
        byte.* = @truncate(index *% 131 +% 17);
    }

    const source_words = try packScalarSource(allocator, &input);
    defer allocator.free(source_words);
    const direct_words = try packScalarDirect(allocator, &input);
    defer allocator.free(direct_words);
    const production_words = try wire.packSignedWordsBigEndian(allocator, &input);
    defer allocator.free(production_words);
    try std.testing.expectEqualSlices(i64, source_words, direct_words);
    try std.testing.expectEqualSlices(i64, source_words, production_words);

    const source_bytes = try unpackScalarSource(allocator, source_words);
    defer allocator.free(source_bytes);
    const direct_bytes = try unpackScalarDirect(allocator, source_words);
    defer allocator.free(direct_bytes);
    const production_bytes = try wire.unpackSignedWordsBigEndian(allocator, source_words);
    defer allocator.free(production_bytes);
    try std.testing.expectEqualSlices(u8, source_bytes, direct_bytes);
    try std.testing.expectEqualSlices(u8, source_bytes, production_bytes);
    try std.testing.expectEqualSlices(u8, &input, production_bytes[0..input.len]);
}
