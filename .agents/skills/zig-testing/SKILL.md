---
name: zig-testing
description: Zig testing skill for writing and running tests. Use when using zig build test, writing comptime tests, using test filters, working with test allocators to detect leaks, or using Zig's built-in fuzz testing (0.14+). Activates on queries about Zig tests, zig test, zig build test, comptime testing, test allocators, Zig fuzz testing, or detecting memory leaks in Zig tests.
---

# Zig Testing

## Purpose

Guide agents through Zig's testing system: `zig build test` and `zig test`, comptime testing patterns, test filters, the test allocator for leak detection, and Zig's built-in fuzz testing introduced in 0.14.

## Triggers

- "How do I write and run tests in Zig?"
- "How do I filter which Zig tests run?"
- "How do I detect memory leaks in Zig tests?"
- "How do I write comptime tests in Zig?"
- "How do I use Zig's built-in fuzzer?"
- "How do I test a Zig library?"

## Workflow

### 1. Writing and running tests

```zig
// src/math.zig
const std = @import("std");
const testing = std.testing;

pub fn add(a: i32, b: i32) i32 {
    return a + b;
}

pub fn divide(a: f64, b: f64) !f64 {
    if (b == 0.0) return error.DivisionByZero;
    return a / b;
}

// Tests live in the same file or a dedicated test file
test "add: basic addition" {
    try testing.expectEqual(@as(i32, 5), add(2, 3));
    try testing.expectEqual(@as(i32, -1), add(2, -3));
}

test "add: identity" {
    try testing.expectEqual(@as(i32, 42), add(42, 0));
}

test "divide: normal case" {
    const result = try divide(10.0, 2.0);
    try testing.expectApproxEqAbs(result, 5.0, 1e-9);
}

test "divide: by zero returns error" {
    try testing.expectError(error.DivisionByZero, divide(1.0, 0.0));
}
```

```bash
# Run all tests in a single file
zig test src/math.zig

# Run all tests via build system
zig build test

# Verbose output
zig build test -- --verbose

# Run specific test by name (substring match)
zig build test -- --test-filter "add"
```

### 2. build.zig test configuration

```zig
// build.zig
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Unit test step
    const unit_tests = b.addTest(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    const run_unit_tests = b.addRunArtifact(unit_tests);

    // Integration tests (separate executable)
    const integration_tests = b.addTest(.{
        .root_source_file = b.path("tests/integration.zig"),
        .target = target,
        .optimize = optimize,
    });
    const run_integration = b.addRunArtifact(integration_tests);

    // `zig build test` runs both
    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_integration.step);

    // `zig build test-unit` runs only unit tests
    const unit_step = b.step("test-unit", "Run unit tests");
    unit_step.dependOn(&run_unit_tests.step);
}
```

### 3. Test allocator — leak detection

The `std.testing.allocator` wraps a `GeneralPurposeAllocator` in test mode and reports leaks at the end of each test:

```zig
const std = @import("std");
const testing = std.testing;

test "ArrayList: no leaks" {
    // testing.allocator detects leaks and reports them
    var list = std.ArrayList(u32).init(testing.allocator);
    defer list.deinit();   // MUST defer to return memory

    try list.append(1);
    try list.append(2);
    try list.append(3);

    try testing.expectEqual(@as(usize, 3), list.items.len);
    // If you forget defer list.deinit(), test reports a leak
}

test "custom allocation" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const leaked = gpa.deinit();
        // .ok means no leaks; .leak means memory was not freed
        testing.expect(leaked == .ok) catch @panic("memory leaked!");
    }
    const allocator = gpa.allocator();

    const buf = try allocator.alloc(u8, 1024);
    defer allocator.free(buf);  // leak if forgotten
}
```

### 4. Testing assertions

```zig
const testing = std.testing;

// Equality
try testing.expectEqual(expected, actual);
try testing.expectEqualStrings("hello", result_str);
try testing.expectEqualSlices(u8, expected_slice, actual_slice);

// Approximate equality (for floats)
try testing.expectApproxEqAbs(expected, actual, tolerance);
try testing.expectApproxEqRel(expected, actual, tolerance);

// Errors
try testing.expectError(error.MyError, might_fail());
try testing.expect(condition);    // basic boolean assertion

// Comparison
try testing.expect(a < b);
try testing.expectStringStartsWith(str, "prefix");
try testing.expectStringEndsWith(str, "suffix");
```

### 5. Comptime testing

Zig can run tests at comptime — useful for compile-time constants and type-level checks:

```zig
const std = @import("std");
const testing = std.testing;

// Test comptime functions
fn isPowerOfTwo(n: comptime_int) bool {
    return n > 0 and (n & (n - 1)) == 0;
}

// Comptime assert (compile error if false)
comptime {
    std.debug.assert(isPowerOfTwo(16));
    std.debug.assert(!isPowerOfTwo(15));
    std.debug.assert(isPowerOfTwo(1024));
}

// Test with comptime-known values (runs at comptime in test mode)
test "isPowerOfTwo: comptime" {
    comptime {
        try testing.expect(isPowerOfTwo(8));
        try testing.expect(!isPowerOfTwo(7));
    }
}

// Type-level testing
test "type properties" {
    // Verify alignment and size at comptime
    comptime {
        try testing.expectEqual(8, @alignOf(u64));
        try testing.expectEqual(4, @sizeOf(u32));
        try testing.expectEqual(true, @typeInfo(u8).Int.signedness == .unsigned);
    }
}
```

### 6. Fuzz testing (Zig 0.14+)

Zig 0.14 introduced a built-in fuzzer using coverage-guided fuzzing:

```zig
// fuzz_target.zig
const std = @import("std");

// Fuzz entry point: receives arbitrary bytes
export fn fuzz(input: []const u8) void {
    // Call the function under test with fuzz input
    parseInput(input) catch {};
}

fn parseInput(data: []const u8) !void {
    if (data.len < 4) return error.TooShort;
    const magic = std.mem.readInt(u32, data[0..4], .little);
    if (magic != 0xDEADBEEF) return error.BadMagic;
    // ... more parsing
}
```

```bash
# Run the fuzzer
zig build fuzz -Dfuzz=fuzz_target

# With corpus directory
zig build fuzz -Dfuzz=fuzz_target -- corpus/

# The fuzzer generates and saves interesting inputs to corpus/
# Crashes are saved as artifacts

# Reproduce a specific crash
zig build test-fuzz -- corpus/crash-xxxx
```

For build.zig fuzz setup:

```zig
// build.zig addition
const fuzz_exe = b.addExecutable(.{
    .name = "fuzz",
    .root_source_file = b.path("src/fuzz_target.zig"),
    .target = target,
    .optimize = .ReleaseSafe,
});
fuzz_exe.root_module.fuzz = true;   // enable fuzzing instrumentation
const fuzz_step = b.step("fuzz", "Run fuzzer");
fuzz_step.dependOn(&b.addRunArtifact(fuzz_exe).step);
```

## Related skills

- Use `skills/zig/zig-build-system` for build.zig configuration and test step setup
- Use `skills/zig/zig-comptime` for comptime evaluation patterns tested via comptime asserts
- Use `skills/runtimes/fuzzing` for libFuzzer/AFL as alternative fuzz frameworks
- Use `skills/runtimes/sanitizers` for AddressSanitizer with Zig tests
