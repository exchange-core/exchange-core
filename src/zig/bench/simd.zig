const std = @import("std");
const core = @import("exchange_core").core;

const wire = core.java_binary_wire;

const MaxTrials: usize = 101;
const MaxIterations: usize = 10_000_000;
const CalibrationFloorNs: u64 = 2 * std.time.ns_per_ms;

const BenchError = wire.CarrierPackError || wire.CarrierUnpackError || error{
    InvalidArguments,
    MissingArgumentValue,
    ScalarProductionMismatch,
    CalibrationOverflow,
};

const Implementation = enum {
    scalar_source,
    production,

    fn label(self: Implementation) []const u8 {
        return switch (self) {
            .scalar_source => "scalar_source",
            .production => "production",
        };
    }
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
    warmup_pairs: usize = 3,
    trials: usize = 11,
    min_trial_ms: u64 = 100,

    fn validate(self: Options) BenchError!void {
        if (self.bytes == 0 or
            self.warmup_pairs > 1000 or
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
    scalar: [MaxTrials]f64 = undefined,
    production: [MaxTrials]f64 = undefined,
    speedup: [MaxTrials]f64 = undefined,
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
    try options.validate();
    const target_ns = try options.targetNs();

    const input = try allocator.alloc(u8, options.bytes);
    defer allocator.free(input);
    for (input, 0..) |*byte, index| {
        byte.* = @truncate(index *% 131 +% 17);
    }

    const scalar_words = try packScalarSource(allocator, input);
    defer allocator.free(scalar_words);
    const production_words = try wire.packSignedWordsBigEndian(allocator, input);
    defer allocator.free(production_words);
    if (!std.mem.eql(i64, scalar_words, production_words)) {
        return error.ScalarProductionMismatch;
    }

    const scalar_bytes = try unpackScalarSource(allocator, scalar_words);
    defer allocator.free(scalar_bytes);
    const production_bytes = try wire.unpackSignedWordsBigEndian(allocator, scalar_words);
    defer allocator.free(production_bytes);
    if (!std.mem.eql(u8, scalar_bytes, production_bytes) or
        !std.mem.eql(u8, input, production_bytes[0..input.len]))
    {
        return error.ScalarProductionMismatch;
    }

    try out.print(
        "exchange-core SIMD carrier benchmark bytes={d} warmup_pairs={d} trials={d} min_trial_ms={d} vector_lanes_u64={d}\n",
        .{
            options.bytes,
            options.warmup_pairs,
            options.trials,
            options.min_trial_ms,
            std.simd.suggestVectorLength(u64) orelse 1,
        },
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
    for (0..options.warmup_pairs) |_| {
        _ = try measure(io, allocator, operation, .scalar_source, input, words, 1);
        _ = try measure(io, allocator, operation, .production, input, words, 1);
    }

    const scalar_iterations = try calibrate(
        io,
        allocator,
        operation,
        .scalar_source,
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
    const iterations = @max(scalar_iterations, production_iterations);

    for (0..options.trials) |trial| {
        const order = if ((trial & 1) == 0)
            [_]Implementation{ .scalar_source, .production }
        else
            [_]Implementation{ .production, .scalar_source };

        var checksums: [2]u64 = undefined;
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
                .scalar_source => samples.scalar[trial] = result.bytes_per_second,
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
        if (checksums[0] != checksums[1]) return error.ScalarProductionMismatch;
        samples.speedup[trial] = samples.production[trial] / samples.scalar[trial];
    }
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
    var checksum: u64 = 0;
    const started = std.Io.Clock.awake.now(io);
    switch (operation) {
        .pack => for (0..iterations) |_| {
            const packed_words = switch (implementation) {
                .scalar_source => try packScalarSource(allocator, input),
                .production => try wire.packSignedWordsBigEndian(allocator, input),
            };
            std.mem.doNotOptimizeAway(packed_words.ptr);
            checksum +%= @bitCast(packed_words[0]);
            checksum +%= @bitCast(packed_words[packed_words.len - 1]);
            allocator.free(packed_words);
        },
        .unpack => for (0..iterations) |_| {
            const unpacked = switch (implementation) {
                .scalar_source => try unpackScalarSource(allocator, words),
                .production => try wire.unpackSignedWordsBigEndian(allocator, words),
            };
            std.mem.doNotOptimizeAway(unpacked.ptr);
            checksum +%= unpacked[0];
            checksum +%= unpacked[unpacked.len - 1];
            allocator.free(unpacked);
        },
    }
    const elapsed_raw = started.durationTo(std.Io.Clock.awake.now(io)).nanoseconds;
    const elapsed_ns = if (elapsed_raw <= 0)
        1
    else
        std.math.cast(u64, elapsed_raw) orelse std.math.maxInt(u64);
    const total_bytes = @as(u128, input.len) * iterations;
    const bytes_per_second = @as(f64, @floatFromInt(total_bytes)) *
        @as(f64, std.time.ns_per_s) / @as(f64, @floatFromInt(elapsed_ns));
    return .{
        .iterations = iterations,
        .elapsed_ns = elapsed_ns,
        .bytes_per_second = bytes_per_second,
        .checksum = checksum,
    };
}

fn printSummary(
    out: *std.Io.Writer,
    operation: Operation,
    samples: OperationSamples,
    trials: usize,
) !void {
    var scalar = samples.scalar;
    var production = samples.production;
    var speedup = samples.speedup;
    std.sort.heap(f64, scalar[0..trials], {}, std.sort.asc(f64));
    std.sort.heap(f64, production[0..trials], {}, std.sort.asc(f64));
    std.sort.heap(f64, speedup[0..trials], {}, std.sort.asc(f64));
    try out.print(
        "SUMMARY operation={s} scalar_median_Bps={d:.3} production_median_Bps={d:.3} paired_speedup_median={d:.4}\n",
        .{
            operation.label(),
            median(scalar[0..trials]),
            median(production[0..trials]),
            median(speedup[0..trials]),
        },
    );
}

fn median(sorted: []const f64) f64 {
    const middle = sorted.len / 2;
    if ((sorted.len & 1) == 1) return sorted[middle];
    return (sorted[middle - 1] + sorted[middle]) / 2.0;
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
            options.warmup_pairs = try parseInt(usize, args.next() orelse return error.MissingArgumentValue);
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
        \\Benchmarks the original scalar source against the production binary-carrier
        \\pack/unpack implementation using paired, counterbalanced ReleaseFast trials.
        \\
        \\Options:
        \\  --bytes N          Input size in bytes (default: 1048576)
        \\  --warmup N         Untimed warmup pairs (default: 3)
        \\  --trials N         Timed paired trials, max 101 (default: 11)
        \\  --min-trial-ms N   Calibrated minimum trial duration (default: 100)
        \\  -h, --help         Show this help
        \\
    );
}
