---
name: zig-debugging
description: Zig debugging skill. Use when debugging Zig programs with GDB or LLDB, interpreting Zig runtime panics, using std.debug.print for tracing, configuring debug builds, or debugging Zig programs in VS Code. Activates on queries about debugging Zig, Zig panics, zig gdb, zig lldb, std.debug.print, Zig stack traces, or Zig error return traces.
---

# Zig Debugging

## Purpose

Guide agents through debugging Zig programs: GDB/LLDB sessions, interpreting Zig panics and error return traces, `std.debug.print` logging, debug build configuration, and IDE integration.

## Triggers

- "How do I debug a Zig program with GDB?"
- "How do I interpret a Zig panic message?"
- "How do I use std.debug.print for debugging?"
- "Zig is showing an error return trace — what does it mean?"
- "How do I set up Zig debugging in VS Code?"
- "How do I get a stack trace from a Zig crash?"

## Workflow

### 1. Build for debugging

```bash
# Debug build (default) — full debug info, safety checks
zig build-exe src/main.zig -O Debug

# With build system
zig build            # uses Debug by default
zig build -Doptimize=Debug

# Run directly with debug output
zig run src/main.zig
```

### 2. GDB with Zig

Zig emits standard DWARF debug information compatible with GDB:

```bash
# Build with debug info
zig build-exe src/main.zig -O Debug -femit-bin=myapp

# Launch GDB
gdb ./myapp

# GDB session
(gdb) break main
(gdb) run arg1 arg2
(gdb) next          # step over
(gdb) step          # step into
(gdb) continue
(gdb) print my_var
(gdb) info locals
(gdb) bt            # backtrace
```

Break on Zig panics:
```gdb
(gdb) break __zig_panic_start
(gdb) break std.builtin.default_panic
```

### 3. LLDB with Zig

```bash
lldb ./myapp

(lldb) b main
(lldb) r arg1 arg2
(lldb) n            # next
(lldb) s            # step into
(lldb) p my_var     # print
(lldb) frame variable
(lldb) bt           # backtrace
(lldb) c            # continue

# Break on panic
(lldb) b __zig_panic
```

### 4. Interpreting Zig panics

Zig panics include the source location and reason:

```
thread 'main' panic: index out of bounds: index 5, len 3
/home/user/src/main.zig:15:14
/home/user/src/main.zig:42:9
???:?:?: (name not available)
```

Common panic messages:
| Panic | Cause |
|-------|-------|
| `index out of bounds: index N, len M` | Slice/array OOB access |
| `integer overflow` | Arithmetic overflow in Debug/ReleaseSafe |
| `attempt to unwrap null` | Optional access `.?` on null |
| `reached unreachable code` | `unreachable` executed |
| `casting...` | Invalid enum tag or union access |
| `integer cast truncated bits` | `@intCast` with value out of range |
| `out of memory` | Allocator failed |

### 5. Error return traces

Zig tracks where errors propagate with error return traces:

```
error: FileNotFound
/home/user/src/main.zig:30:20: 0x10a3b in openConfig (main)
    const f = try std.fs.openFileAbsolute(path, .{});
                   ^
/home/user/src/main.zig:15:25: 0x10b12 in run (main)
    const cfg = try openConfig("/etc/myapp.conf");
                    ^
/home/user/src/main.zig:8:20: 0x10c44 in main (main)
    try run();
               ^
```

The trace shows the exact `try` chain where the error propagated. Read bottom-up: `main → run → openConfig`.

Enable in release builds:
```zig
// build.zig
const exe = b.addExecutable(.{
    .name = "myapp",
    .root_source_file = b.path("src/main.zig"),
    .target = target,
    .optimize = optimize,
    .error_tracing = true,  // enable even in ReleaseFast
});
```

### 6. std.debug.print for tracing

```zig
const std = @import("std");

pub fn main() !void {
    const x: u32 = 42;
    const name = "world";

    // Basic print (always to stderr)
    std.debug.print("x = {d}, name = {s}\n", .{ x, name });

    // Print any value (useful for structs)
    const point = Point{ .x = 1, .y = 2 };
    std.debug.print("point = {any}\n", .{point});

    // Formatted output
    std.debug.print("hex: {x}, binary: {b}\n", .{ x, x });

    // Log levels (respects compile-time log level)
    const log = std.log.scoped(.my_module);
    log.debug("debug info: {d}", .{x});
    log.info("started processing", .{});
    log.warn("unusual condition", .{});
    log.err("failed: {s}", .{"reason"});
}
```

### 7. std.log configuration

```zig
// Override default log level at root
pub const std_options = std.Options{
    .log_level = .debug,  // .debug | .info | .warn | .err
};

// Custom log handler
pub fn logFn(
    comptime level: std.log.Level,
    comptime scope: @TypeOf(.enum_literal),
    comptime format: []const u8,
    args: anytype,
) void {
    const prefix = "[" ++ @tagName(level) ++ "] (" ++ @tagName(scope) ++ "): ";
    std.debug.print(prefix ++ format ++ "\n", args);
}

pub const std_options = std.Options{
    .logFn = logFn,
};
```

### 8. VS Code / IDE integration

Install the `zig.vscode-zig` extension and CodeLLDB.

`.vscode/launch.json`:
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "type": "lldb",
            "request": "launch",
            "name": "Debug Zig",
            "program": "${workspaceFolder}/zig-out/bin/myapp",
            "args": [],
            "cwd": "${workspaceFolder}",
            "preLaunchTask": "zig build"
        }
    ]
}
```

`.vscode/tasks.json`:
```json
{
    "version": "2.0.0",
    "tasks": [
        {
            "label": "zig build",
            "type": "shell",
            "command": "zig build",
            "group": { "kind": "build", "isDefault": true },
            "problemMatcher": ["$zig"]
        }
    ]
}
```

## Related skills

- Use `skills/zig/zig-compiler` for build modes and debug info flags
- Use `skills/debuggers/gdb` for GDB fundamentals
- Use `skills/debuggers/lldb` for LLDB fundamentals
- Use `skills/zig/zig-cinterop` when debugging mixed Zig/C code
