---
name: zig-best-practices
description: Provides Zig patterns for type-first development with tagged unions, explicit error sets, comptime validation, and memory management. Must use when reading or writing Zig files.
---

# Zig Best Practices

## Type-First Development

Types define the contract before implementation. Follow this workflow:

1. **Define data structures** - structs, unions, and error sets first
2. **Define function signatures** - parameters, return types, and error unions
3. **Implement to satisfy types** - let the compiler guide completeness
4. **Validate at comptime** - catch invalid configurations during compilation

### Make Illegal States Unrepresentable

Use Zig's type system to prevent invalid states at compile time.

**Tagged unions for mutually exclusive states:**
```zig
// Good: only valid combinations possible
const RequestState = union(enum) {
    idle,
    loading,
    success: []const u8,
    failure: anyerror,
};

fn handleState(state: RequestState) void {
    switch (state) {
        .idle => {},
        .loading => showSpinner(),
        .success => |data| render(data),
        .failure => |err| showError(err),
    }
}

// Bad: allows invalid combinations
const RequestState = struct {
    loading: bool,
    data: ?[]const u8,
    err: ?anyerror,
};
```

**Explicit error sets for failure modes:**
```zig
// Good: documents exactly what can fail
const ParseError = error{
    InvalidSyntax,
    UnexpectedToken,
    EndOfInput,
};

fn parse(input: []const u8) ParseError!Ast {
    // implementation
}

// Bad: anyerror hides failure modes
fn parse(input: []const u8) anyerror!Ast {
    // implementation
}
```

**Distinct types for domain concepts:**
```zig
// Prevent mixing up IDs of different types
const UserId = enum(u64) { _ };
const OrderId = enum(u64) { _ };

fn getUser(id: UserId) !User {
    // Compiler prevents passing OrderId here
}

fn createUserId(raw: u64) UserId {
    return @enumFromInt(raw);
}
```

**Comptime validation for invariants:**
```zig
fn Buffer(comptime size: usize) type {
    if (size == 0) {
        @compileError("buffer size must be greater than 0");
    }
    if (size > 1024 * 1024) {
        @compileError("buffer size exceeds 1MB limit");
    }
    return struct {
        data: [size]u8 = undefined,
        len: usize = 0,
    };
}
```

**Non-exhaustive enums for extensibility:**
```zig
// External enum that may gain variants
const Status = enum(u8) {
    active = 1,
    inactive = 2,
    pending = 3,
    _,
};

fn processStatus(status: Status) !void {
    switch (status) {
        .active => {},
        .inactive => {},
        .pending => {},
        _ => return error.UnknownStatus,
    }
}
```

## Module Structure

Larger cohesive files are idiomatic in Zig. Keep related code together: tests alongside implementation, comptime generics at file scope, public/private controlled by `pub`. Split only when a file handles genuinely separate concerns. The standard library demonstrates this pattern with files like `std/mem.zig` containing 2000+ lines of cohesive memory operations.

## Instructions

- Return errors with context using error unions (`!T`); every function returns a value or an error. Explicit error sets document failure modes.
- Use `errdefer` for cleanup on error paths; use `defer` for unconditional cleanup. This prevents resource leaks without try-finally boilerplate.
- Handle all branches in `switch` statements; include an `else` clause that returns an error or uses `unreachable` for truly impossible cases.
- Pass allocators explicitly to functions requiring dynamic memory; prefer `std.testing.allocator` in tests for leak detection.
- Prefer `const` over `var`; prefer slices over raw pointers for bounds safety. Immutability signals intent and enables optimizations.
- Avoid `anytype`; prefer explicit `comptime T: type` parameters. Explicit types document intent and produce clearer error messages.
- Use `std.log.scoped` for namespaced logging; define a module-level `log` constant for consistent scope across the file.
- Add or update tests for new logic; use `std.testing.allocator` to catch memory leaks automatically.

## Examples

Explicit failure for unimplemented logic:
```zig
fn buildWidget(widget_type: []const u8) !Widget {
    return error.NotImplemented;
}
```

Propagate errors with try:
```zig
fn readConfig(path: []const u8) !Config {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    const contents = try file.readToEndAlloc(allocator, max_size);
    return parseConfig(contents);
}
```

Resource cleanup with errdefer:
```zig
fn createResource(allocator: std.mem.Allocator) !*Resource {
    const resource = try allocator.create(Resource);
    errdefer allocator.destroy(resource);

    resource.* = try initializeResource();
    return resource;
}
```

Exhaustive switch with explicit default:
```zig
fn processStatus(status: Status) ![]const u8 {
    return switch (status) {
        .active => "processing",
        .inactive => "skipped",
        _ => error.UnhandledStatus,
    };
}
```

Testing with memory leak detection:
```zig
const std = @import("std");

test "widget creation" {
    const allocator = std.testing.allocator;
    var list: std.ArrayListUnmanaged(u32) = .empty;
    defer list.deinit(allocator);

    try list.append(allocator, 42);
    try std.testing.expectEqual(1, list.items.len);
}
```

## Memory Management

- Pass allocators explicitly; never use global state for allocation. Functions declare their allocation needs in parameters.
- Use `defer` immediately after acquiring a resource. Place cleanup logic next to acquisition for clarity.
- Prefer arena allocators for temporary allocations; they free everything at once when the arena is destroyed.
- Use `std.testing.allocator` in tests; it reports leaks with stack traces showing allocation origins.

### Examples

Allocator as explicit parameter:
```zig
fn processData(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    const result = try allocator.alloc(u8, input.len * 2);
    errdefer allocator.free(result);

    // process input into result
    return result;
}
```

Arena allocator for batch operations:
```zig
fn processBatch(items: []const Item) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    for (items) |item| {
        const processed = try processItem(allocator, item);
        try outputResult(processed);
    }
    // All allocations freed when arena deinits
}
```

## Logging

- Use `std.log.scoped` to create namespaced loggers; each module should define its own scoped logger for filtering.
- Define a module-level `const log` at the top of the file; use it consistently throughout the module.
- Use appropriate log levels: `err` for failures, `warn` for suspicious conditions, `info` for state changes, `debug` for tracing.

### Examples

Scoped logger for a module:
```zig
const std = @import("std");
const log = std.log.scoped(.widgets);

pub fn createWidget(name: []const u8) !Widget {
    log.debug("creating widget: {s}", .{name});
    const widget = try allocateWidget(name);
    log.debug("created widget id={d}", .{widget.id});
    return widget;
}

pub fn deleteWidget(id: u32) void {
    log.info("deleting widget id={d}", .{id});
    // cleanup
}
```

Multiple scopes in a codebase:
```zig
// In src/db.zig
const log = std.log.scoped(.db);

// In src/http.zig
const log = std.log.scoped(.http);

// In src/auth.zig
const log = std.log.scoped(.auth);
```

## Comptime Patterns

- Use `comptime` parameters for generic functions; type information is available at compile time with zero runtime cost.
- Prefer compile-time validation over runtime checks when possible. Catch errors during compilation rather than in production.
- Use `@compileError` for invalid configurations that should fail the build.

### Examples

Generic function with comptime type:
```zig
fn max(comptime T: type, a: T, b: T) T {
    return if (a > b) a else b;
}
```

Compile-time validation:
```zig
fn createBuffer(comptime size: usize) [size]u8 {
    if (size == 0) {
        @compileError("buffer size must be greater than 0");
    }
    return [_]u8{0} ** size;
}
```

## Avoiding anytype

- Prefer `comptime T: type` over `anytype`; explicit type parameters document expected constraints and produce clearer errors.
- Use `anytype` only when the function genuinely accepts any type (like `std.debug.print`) or for callbacks/closures.
- When using `anytype`, add a doc comment describing the expected interface or constraints.

### Examples

Prefer explicit comptime type (good):
```zig
fn sum(comptime T: type, items: []const T) T {
    var total: T = 0;
    for (items) |item| {
        total += item;
    }
    return total;
}
```

Avoid anytype when type is known (bad):
```zig
// Unclear what types are valid; error messages will be confusing
fn sum(items: anytype) @TypeOf(items[0]) {
    // ...
}
```

Acceptable anytype for callbacks:
```zig
/// Calls `callback` for each item. Callback must accept (T) and return void.
fn forEach(comptime T: type, items: []const T, callback: anytype) void {
    for (items) |item| {
        callback(item);
    }
}
```

Using @TypeOf when anytype is necessary:
```zig
fn debugPrint(value: anytype) void {
    const T = @TypeOf(value);
    if (@typeInfo(T) == .Pointer) {
        std.debug.print("ptr: {*}\n", .{value});
    } else {
        std.debug.print("val: {}\n", .{value});
    }
}
```

## Error Handling Patterns

- Define specific error sets for functions; avoid `anyerror` when possible. Specific errors document failure modes.
- Use `catch` with a block for error recovery or logging; use `catch unreachable` only when errors are truly impossible.
- Merge error sets with `||` when combining operations that can fail in different ways.

### Examples

Specific error set:
```zig
const ConfigError = error{
    FileNotFound,
    ParseError,
    InvalidFormat,
};

fn loadConfig(path: []const u8) ConfigError!Config {
    // implementation
}
```

Error handling with catch block:
```zig
const value = operation() catch |err| {
    std.log.err("operation failed: {}", .{err});
    return error.OperationFailed;
};
```

## Configuration

- Load config from environment variables at startup; validate required values before use. Missing config should cause a clean exit with a descriptive message.
- Define a Config struct as single source of truth; avoid `std.posix.getenv` scattered throughout code.
- Use sensible defaults for development; require explicit values for production secrets.

### Examples

Typed config struct:
```zig
const std = @import("std");

pub const Config = struct {
    port: u16,
    database_url: []const u8,
    api_key: []const u8,
    env: []const u8,
};

pub fn loadConfig() !Config {
    const db_url = std.posix.getenv("DATABASE_URL") orelse
        return error.MissingDatabaseUrl;
    const api_key = std.posix.getenv("API_KEY") orelse
        return error.MissingApiKey;
    const port_str = std.posix.getenv("PORT") orelse "3000";
    const port = std.fmt.parseInt(u16, port_str, 10) catch
        return error.InvalidPort;

    return .{
        .port = port,
        .database_url = db_url,
        .api_key = api_key,
        .env = std.posix.getenv("ENV") orelse "development",
    };
}
```

## Optionals

- Use `orelse` to provide default values for optionals; use `.?` only when null is a program error.
- Prefer `if (optional) |value|` pattern for safe unwrapping with access to the value.

### Examples

Safe optional handling:
```zig
fn findWidget(id: u32) ?*Widget {
    // lookup implementation
}

fn processWidget(id: u32) !void {
    const widget = findWidget(id) orelse return error.WidgetNotFound;
    try widget.process();
}
```

Optional with if unwrapping:
```zig
if (maybeValue) |value| {
    try processValue(value);
} else {
    std.log.warn("no value present", .{});
}
```

## Advanced Topics

Reference these guides for specialized patterns:

- **Building custom containers** (queues, stacks, trees): See [GENERICS.md](GENERICS.md)
- **Interfacing with C libraries** (raylib, SDL, curl, system APIs): See [C-INTEROP.md](C-INTEROP.md)
- **Debugging memory leaks** (GPA, stack traces): See [DEBUGGING.md](DEBUGGING.md)

## Tooling

### zigdoc - Documentation Lookup

CLI tool for browsing Zig std library and project dependency docs.

**Install:**
```bash
git clone https://github.com/rockorager/zigdoc
cd zigdoc
zig build install -Doptimize=ReleaseFast --prefix $HOME/.local
```

**Usage:**
```bash
zigdoc std.ArrayList       # std lib symbol
zigdoc std.mem.Allocator   # nested symbol
zigdoc vaxis.Window        # project dependency (if in build.zig)
zigdoc @init               # create AGENTS.md with API patterns
```

### ziglint - Static Analysis

Linter for Zig source code enforcing coding standards.

**Install:**
```bash
git clone https://github.com/rockorager/ziglint
cd ziglint
zig build install -Doptimize=ReleaseFast --prefix $HOME/.local
```

**Usage:**
```bash
ziglint                    # lint current directory (uses .ziglint.zon if present)
ziglint src build.zig      # lint specific paths
ziglint --ignore Z001      # suppress specific rule
```

**Configuration (`.ziglint.zon`):**
```zig
.{
    .paths = .{ "src", "build.zig" },
    .rules = .{
        .Z001 = .{ .enabled = false },
        .Z024 = .{ .max_length = 80 },
    },
}
```

**Inline suppression:**
```zig
fn MyBadName() void {} // ziglint-ignore: Z001
```

## References

- Language Reference: https://ziglang.org/documentation/0.15.2/
- Standard Library: https://ziglang.org/documentation/0.15.2/std/
- Code Samples: https://ziglang.org/learn/samples/
- Zig Guide: https://zig.guide/
