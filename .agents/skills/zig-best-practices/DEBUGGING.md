# Debugging Memory in Zig

Use GeneralPurposeAllocator (GPA) to detect memory leaks with stack traces showing allocation origins.

## When to Use

- Debugging memory leaks in development
- Validating cleanup logic in complex systems
- Investigating use-after-free or double-free bugs

## GeneralPurposeAllocator Pattern

```zig
const std = @import("std");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    // Use allocator for all allocations
    const data = try allocator.alloc(u8, 1024);
    defer allocator.free(data);

    // Any leaked allocations will be reported at deinit
}
```

## Configuration Options

```zig
var gpa = std.heap.GeneralPurposeAllocator(.{
    .stack_trace_depth = 10,  // Stack frames to capture (default: 8)
    .enable_memory_limit = true,
    .requested_memory_limit = 1024 * 1024,  // 1MB limit
}){};
```

## Leak Report Output

When leaks occur, GPA prints:

```
error: memory leak detected
Leak at 0x7f... (1024 bytes)
    src/main.zig:42:25
    src/main.zig:38:18
    ...
```

## Testing with Leak Detection

`std.testing.allocator` wraps GPA and fails tests on leaks:

```zig
test "no memory leaks" {
    const allocator = std.testing.allocator;
    var list: std.ArrayListUnmanaged(u32) = .empty;
    defer list.deinit(allocator);

    try list.append(allocator, 42);
    // Test fails if list.deinit is missing
}
```

## Production vs Debug

- Use GPA in debug builds for safety
- Switch to `std.heap.page_allocator` or arena in release for performance
- `std.heap.c_allocator` when interfacing heavily with C code
