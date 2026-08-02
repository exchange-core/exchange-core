# Zig Debugging Patterns Reference

## Panic Handler Customization

```zig
// Custom panic handler in root file (src/main.zig)
pub fn panic(msg: []const u8, error_return_trace: ?*std.builtin.StackTrace, ret_addr: ?usize) noreturn {
    _ = ret_addr;
    std.debug.print("\n=== PANIC ===\n{s}\n", .{msg});
    if (error_return_trace) |trace| {
        std.debug.dumpStackTrace(trace.*);
    }
    std.debug.dumpCurrentStackTrace(null);
    std.process.exit(1);
}
```

## Stack Trace Utilities

```zig
// Print current stack trace
std.debug.dumpCurrentStackTrace(null);

// Capture stack trace
var addresses: [32]usize = undefined;
var trace = std.builtin.StackTrace{
    .instruction_addresses = &addresses,
    .index = 0,
};
std.debug.captureStackTrace(null, &trace);
// Later:
std.debug.dumpStackTrace(trace);

// Print source location at comptime
const location = @src();
std.debug.print("{s}:{d}\n", .{ location.file, location.line });
```

## Format Specifiers for std.debug.print

```zig
// {}: default format
// {d}: decimal integer
// {x}: hex (lowercase)
// {X}: hex (uppercase)
// {o}: octal
// {b}: binary
// {e}: float scientific
// {f}: float fixed
// {s}: string/slice
// {c}: character (u8 as char)
// {u}: unicode codepoint
// {any}: any type (uses std format)
// {*}: pointer address

const val: u32 = 255;
std.debug.print("{d} {x} {b} {o}\n", .{val, val, val, val});
// → 255 ff 11111111 377

// Struct formatting
const Point = struct { x: f32, y: f32 };
const p = Point{ .x = 1.5, .y = 2.7 };
std.debug.print("{}\n", .{p});
// → main.Point{ .x = 1.5e+00, .y = 2.7e+00 }

// Pointer
const ptr: *const u32 = &val;
std.debug.print("{*}\n", .{ptr});
// → *const u32@0x7fff...
```

## Compile-Time Debugging

```zig
// Print at compile time with @compileLog
fn fibonacci(comptime n: u32) u32 {
    @compileLog("computing fibonacci for n =", n);
    if (n <= 1) return n;
    return fibonacci(n - 1) + fibonacci(n - 2);
}

// Inspect type at compile time
const T = u32;
@compileLog(@typeName(T));           // → "u32"
@compileLog(@typeInfo(T));           // → TypeInfo union
@compileLog(@sizeOf(T));             // → 4
@compileLog(@alignOf(T));            // → 4
@compileLog(@bitSizeOf(T));          // → 32
```

## GDB Commands for Zig Types

```gdb
# Print Zig slice
(gdb) p my_slice
# Shows: {ptr = 0x..., len = 5}

# Inspect slice elements
(gdb) p *my_slice.ptr@my_slice.len

# Print Zig optional
(gdb) p my_optional

# Print error union
(gdb) p my_error_union

# Zig stack frames show Zig-mangled names
# Demangle with:
(gdb) set print demangle on
(gdb) set demangle-style auto
```

## Memory Debugging

```zig
// Use GeneralPurposeAllocator for leak detection
var gpa = std.heap.GeneralPurposeAllocator(.{}){};
defer {
    const leaked = gpa.deinit();
    if (leaked == .leak) std.debug.print("Memory leaked!\n", .{});
}
const allocator = gpa.allocator();

// Debug allocation failures
var gpa = std.heap.GeneralPurposeAllocator(.{
    .safety = true,           // Check for use-after-free
    .never_unmap = true,      // Keep memory mapped (detect UAF faster)
    .retain_metadata = true,  // Keep freed block metadata
}){};
```

## Testing and Debug Assertions

```zig
const testing = std.testing;

// In tests
try testing.expect(x == 42);
try testing.expectEqual(@as(u32, 42), x);
try testing.expectEqualStrings("hello", result);
try testing.expectError(error.NotFound, fallible_fn());

// Debug assertions (only in Debug/ReleaseSafe)
std.debug.assert(x > 0);  // panics if false

// Custom assertion with message
if (x <= 0) {
    std.debug.panic("Expected positive x, got {d}", .{x});
}
```

## Zig-Specific GDB Breakpoints

```gdb
# Break on any Zig panic
(gdb) break __zig_panic_start

# Set via pattern matching for specific panic source
(gdb) rbreak zig.*panic

# Watch for slice OOB
(gdb) watch -l array_var.len

# Catch allocation failures in GPA
(gdb) break std.heap.GeneralPurposeAllocator.alloc
```
