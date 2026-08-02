---
name: zig-comptime
description: Zig comptime skill for compile-time evaluation and metaprogramming. Use when using comptime parameters, comptime types, generics via anytype, comptime reflection with @typeInfo, or metaprogramming patterns that replace C++ templates. Activates on queries about Zig comptime, compile-time evaluation, Zig generics, anytype, @typeInfo, comptime types, or Zig metaprogramming.
---

# Zig comptime

## Purpose

Guide agents through Zig's `comptime` system: compile-time function evaluation, comptime type parameters, generics via `anytype`, type reflection with `@typeInfo`, and metaprogramming patterns that replace C++ templates and macros.

## Triggers

- "How does comptime work in Zig?"
- "How do I write a generic function in Zig?"
- "How do I use @typeInfo for reflection?"
- "How do I generate code at compile time in Zig?"
- "How does anytype work in Zig?"
- "How do Zig generics compare to C++ templates?"

## Workflow

### 1. comptime basics

```zig
// comptime keyword forces compile-time evaluation
const x: comptime_int = 42;         // comptime integer (arbitrary precision)
const y: comptime_float = 3.14159;  // comptime float (arbitrary precision)

// comptime block — runs at compile time
comptime {
    const val = fibonacci(20);       // computed at compile time
    std.debug.assert(val == 6765);   // compile-time assertion
}

// comptime parameter — caller must provide a comptime-known value
fn makeArray(comptime T: type, comptime n: usize) [n]T {
    return [_]T{0} ** n;             // array of n zeros of type T
}

const arr = makeArray(f32, 8);      // [8]f32 computed at compile time
```

### 2. Generic functions with comptime type parameters

```zig
const std = @import("std");

// Generic max function — T must be comptime-known
fn max(comptime T: type, a: T, b: T) T {
    return if (a > b) a else b;
}

// Usage: T is inferred from arguments or specified explicitly
const r1 = max(i32, 3, 7);          // 7
const r2 = max(f64, 2.5, 1.8);     // 2.5

// Generic Stack data structure
fn Stack(comptime T: type) type {
    return struct {
        items: []T,
        top: usize,
        allocator: std.mem.Allocator,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator) !Self {
            return Self{
                .items = try allocator.alloc(T, 64),
                .top = 0,
                .allocator = allocator,
            };
        }

        pub fn push(self: *Self, value: T) void {
            self.items[self.top] = value;
            self.top += 1;
        }

        pub fn pop(self: *Self) ?T {
            if (self.top == 0) return null;
            self.top -= 1;
            return self.items[self.top];
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.items);
        }
    };
}

// Usage: Stack(i32) and Stack(f64) are distinct types
var int_stack = try Stack(i32).init(allocator);
defer int_stack.deinit();
int_stack.push(42);
```

### 3. anytype — duck-typed comptime parameters

`anytype` accepts any type and the compiler infers it at the call site:

```zig
// anytype: function works for any type with .len field
fn printLength(thing: anytype) void {
    std.debug.print("Length: {}\n", .{thing.len});
}

printLength("hello");                // string literal — works
printLength([_]u8{1, 2, 3});        // array — works
printLength(std.ArrayList(u32){});   // ArrayList — works

// anytype with comptime checks for better errors
fn serialize(writer: anytype, value: anytype) !void {
    // Verify writer has write method at comptime
    if (!@hasDecl(@TypeOf(writer), "write")) {
        @compileError("writer must have a write method");
    }
    try writer.write(std.mem.asBytes(&value));
}

// anytype in struct methods (used throughout std library)
pub fn format(
    self: MyType,
    comptime fmt: []const u8,
    options: std.fmt.FormatOptions,
    writer: anytype,    // any writer: file, buffer, etc.
) !void {
    try writer.print("{} {}", .{self.x, self.y});
}
```

### 4. Type reflection with @typeInfo

`@typeInfo` returns a tagged union describing a type's structure at comptime:

```zig
const std = @import("std");
const TypeInfo = std.builtin.Type;

fn printTypeInfo(comptime T: type) void {
    const info = @typeInfo(T);
    switch (info) {
        .Int => |i| std.debug.print("Int: {} bits, {s}\n",
            .{i.bits, @tagName(i.signedness)}),
        .Float => |f| std.debug.print("Float: {} bits\n", .{f.bits}),
        .Struct => |s| {
            std.debug.print("Struct with {} fields:\n", .{s.fields.len});
            inline for (s.fields) |field| {
                std.debug.print("  {s}: {}\n", .{field.name, field.type});
            }
        },
        .Enum => |e| {
            std.debug.print("Enum with {} values:\n", .{e.fields.len});
            inline for (e.fields) |field| {
                std.debug.print("  {s} = {}\n", .{field.name, field.value});
            }
        },
        .Optional => |o| std.debug.print("Optional({s})\n", .{@typeName(o.child)}),
        .Array => |a| std.debug.print("[{}]{s}\n", .{a.len, @typeName(a.child)}),
        else => std.debug.print("Other type: {s}\n", .{@typeName(T)}),
    }
}

// Usage at comptime
comptime { printTypeInfo(u32); }   // Int: 32 bits, unsigned
comptime { printTypeInfo(f64); }   // Float: 64 bits
```

### 5. Comptime-generated code patterns

```zig
// Generate a lookup table at comptime
const sin_table = blk: {
    const N = 256;
    var table: [N]f32 = undefined;
    @setEvalBranchQuota(10000);     // increase for expensive comptime eval
    for (0..N) |i| {
        const angle = @as(f32, @floatFromInt(i)) * (2.0 * std.math.pi / N);
        table[i] = @sin(angle);
    }
    break :blk table;
};

// Comptime string processing
fn upperCase(comptime s: []const u8) [s.len]u8 {
    var result: [s.len]u8 = undefined;
    for (s, 0..) |c, i| {
        result[i] = std.ascii.toUpper(c);
    }
    return result;
}
const HELLO = upperCase("hello");  // computed at compile time

// Structural typing: accept any struct with specific fields
fn area(shape: anytype) f64 {
    const T = @TypeOf(shape);
    if (@hasField(T, "width") and @hasField(T, "height")) {
        return @as(f64, shape.width) * @as(f64, shape.height);
    } else if (@hasField(T, "radius")) {
        return std.math.pi * @as(f64, shape.radius) * @as(f64, shape.radius);
    } else {
        @compileError("shape must have width+height or radius fields");
    }
}
```

### 6. comptime vs C++ templates comparison

| Feature | C++ templates | Zig comptime |
|---------|--------------|--------------|
| Syntax | `template<typename T>` | `fn foo(comptime T: type)` |
| Error messages | Cryptic instantiation stacks | Clear, at definition point |
| Specialization | `template<> class Foo<int>` | `if (T == i32) { ... }` with `inline if` |
| SFINAE | Complex enable_if | `@hasDecl`, `@hasField`, `@compileError` |
| Variadic | `template<typename... Ts>` | `anytype`, tuples, `inline for` |
| Compile time | Can be very slow | Explicit, bounded by `@setEvalBranchQuota` |
| Values | Requires `constexpr` | Any expression can be `comptime` |
| Macros | Separate `#define` system | Comptime functions replace most macros |

### 7. Common comptime patterns

```zig
// Pattern: compile error for unsupported types
fn serializeInt(comptime T: type, value: T) []const u8 {
    if (@typeInfo(T) != .Int) {
        @compileError("serializeInt requires an integer type, got: " ++ @typeName(T));
    }
    // ...
}

// Pattern: conditional compilation
const is_debug = @import("builtin").mode == .Debug;
if (comptime is_debug) {
    // included only in debug builds
    validateInvariant(self);
}

// Pattern: inline for over comptime-known slice
const fields = std.meta.fields(MyStruct);
inline for (fields) |field| {
    // field.name, field.type available at comptime
    std.debug.print("{s}\n", .{field.name});
}
```

## Related skills

- Use `skills/zig/zig-testing` for comptime assertions and testing comptime code
- Use `skills/zig/zig-build-system` for comptime-based build.zig configuration
- Use `skills/compilers/cpp-templates` for C++ template equivalent patterns
