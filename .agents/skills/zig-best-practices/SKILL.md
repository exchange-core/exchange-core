---
name: zig-best-practices
description: Use when reading or writing Zig files (.zig, build.zig, build.zig.zon).
---

# Zig Best Practices

Follows type-first, functional, and error handling patterns from CLAUDE.md. This skill covers Zig-specific idioms only.

## Type System Patterns

**Tagged unions for mutually exclusive states** — prevents invalid combinations that a struct with multiple nullable fields would allow:
```zig
const RequestState = union(enum) {
    idle,
    loading,
    success: []const u8,
    failure: anyerror,
};
```

**Explicit error sets** — documents exactly what can fail; `anyerror` hides failure modes:
```zig
const ParseError = error{ InvalidSyntax, UnexpectedToken, EndOfInput };
fn parse(input: []const u8) ParseError!Ast { ... }
```

**Distinct types for domain IDs** — compiler prevents mixing up different ID types:
```zig
const UserId = enum(u64) { _ };
const OrderId = enum(u64) { _ };
```

**Comptime validation** — catch invalid configurations at compile time, not runtime:
```zig
fn Buffer(comptime size: usize) type {
    if (size == 0) @compileError("buffer size must be greater than 0");
    return struct { data: [size]u8 = undefined, len: usize = 0 };
}
```

## Memory Management

- Pass allocators explicitly to every function that allocates; no global allocator state.
- Place `defer resource.deinit()` immediately after acquisition — keeps cleanup co-located with creation.
- Use `errdefer` for cleanup on error paths; `defer` for unconditional cleanup.
- Use arena allocators for batch/temporary work; they free everything at once.
- Use `std.testing.allocator` in tests — reports leaks with stack traces.

```zig
fn createResource(allocator: std.mem.Allocator) !*Resource {
    const resource = try allocator.create(Resource);
    errdefer allocator.destroy(resource);  // runs only on error
    resource.* = try initializeResource();
    return resource;
}
```

## Key Conventions

- Prefer `const` over `var`; prefer slices over raw pointers.
- Prefer `comptime T: type` over `anytype`; explicit types produce clearer errors. Use `anytype` only for genuinely polymorphic cases (callbacks, `std.debug.print`-style).
- Exhaustive `switch`: include an `else` returning an error or `unreachable` for truly impossible cases.
- Use `std.log.scoped(.module_name)` for namespaced logging; define a module-level `const log` constant.
- Larger cohesive files are idiomatic — tests alongside implementation, comptime generics at file scope.

## Advanced Topics

- **Generic containers** (queues, stacks, trees): See [GENERICS.md](GENERICS.md)
- **C library interop** (raylib, SDL, curl): See [C-INTEROP.md](C-INTEROP.md)
- **Debugging memory leaks** (GPA, stack traces): See [DEBUGGING.md](DEBUGGING.md)

## Tooling

**zigdoc** — browse std library and dependency docs:
```bash
zigdoc std.mem.Allocator   # std lib symbol
zigdoc vaxis.Window        # project dependency
zigdoc @init               # create AGENTS.md with API patterns
```

**ziglint** — static analysis with `.ziglint.zon` config:
```bash
ziglint                    # lint current directory
ziglint --ignore Z001      # suppress specific rule
```

## References

- Language Reference: https://ziglang.org/documentation/0.15.2/
- Standard Library: https://ziglang.org/documentation/0.15.2/std/
- Zig Guide: https://zig.guide/
