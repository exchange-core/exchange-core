---
name: zig-0.16
description: Zig 0.16.0 API guidance and porting notes. Use this when writing or upgrading Zig code to the 0.16.0 stable release (std.Io era, @Type removal, @cImport deprecation).
license: MIT
compatibility:
  - opencode
  - claude-code
metadata:
  version: "0.16.0"
  language: "zig"
  category: "programming-language"
---

# Zig 0.16.0 Programming Guide

> **Version Scope**: This skill is pinned to **Zig 0.16.0** (stable).
> **Release Notes**: https://ziglang.org/download/0.16.0/release-notes.html

Zig 0.16.0 is a major release introducing `std.Io` as the unified I/O interface, removing `@Type`, deprecating `@cImport`, and significantly reworking the build system package management.

---

## Local Documentation First

Run `zig env` to discover installation paths — never hardcode them. Key fields: `.lib_dir`, `.std_dir`, `.version`.

- **Language Reference**: `<lib_dir>/../doc/langref.html`
- **Std Library Source**: read files under `.std_dir` for API verification.
- **Std Library Docs**: run `zig std` to start a local HTTP server.

Always check local docs before web search.

---

## Quick Reference: Top Breaking Changes

| Old (0.15.x) | New (0.16.0) |
|--------------|--------------|
| `@Type(.Int(...))` | `@Int(.signed, bits)` |
| `@Type(.Struct(...))` | `@Struct(layout, BackingInt, field_names, field_types, field_defaults, field_is_comptime, field_alignments)` |
| `@Type(.Pointer(...))` | `@Pointer(size, attrs, Element, sentinel)` |
| `@Type(.Fn(...))` | `@Fn(param_types, param_attrs, ReturnType, attrs)` |
| `@Type(.Tuple(...))` | `@Tuple(field_types)` |
| `@cImport({...})` | `b.addTranslateC(...)` + `@import("c")` (deprecated) |
| `std.net` | `std.Io.net` |
| `std.ArrayList.init(allocator)` | `std.ArrayList.initCapacity(allocator, n)` |
| `std.crypto.random` | `std.Io.randomSecure(io, buf)` |
| `std.meta.intToEnum` | `std.enums.fromInt` |
| `std.fmt.FormatOptions` | `std.fmt.Options` |
| `std.fmt.bufPrintZ` | `std.fmt.bufPrintSentinel` |
| `std.io.fixedBufferStream` | `std.Io.Writer.fixed(buf)` |
| `error.RenameAcrossMountPoints` | `error.CrossDevice` |
| `error.NotSameFileSystem` | `error.CrossDevice` |
| `error.SharingViolation` | `error.FileBusy` |
| `error.EnvironmentVariableNotFound` | `error.EnvironmentVariableMissing` |
| `std.Build.Step.ConfigHeader` cmake handling | fixed leading whitespace |

---

## Language Changes

### @Type Removed — Use Individual Builtins

`@Type` is gone. Replace with these builtins:

```zig
// Integer type
const MyInt = @Int(.signed, 32);

// Struct type
const MyStruct = @Struct(
    .auto,           // layout
    null,            // BackingInt (for packed)
    &.{"x", "y"},    // field_names
    &.{ i32, i32 },  // field_types
    &.{ null, null },// field_defaults
    &.{ false, false }, // field_is_comptime
    &.{ null, null },// field_alignments
);

// Pointer type
const MyPtr = @Pointer(.one, .{
    .alignment = 8,
    .address_space = .generic,
    .is_const = false,
    .is_volatile = false,
}, u8, null);

// Function type
const MyFn = @Fn(&.{i32, i32}, &.{.{}, .{}}, i32, .{});

// Tuple type
const MyTuple = @Tuple(&.{ i32, bool });

// Enum literal type
const EnumLitType = @EnumLiteral();
```

### @cImport Deprecated — Use Build System Translation

`@cImport` is deprecated. Use `addTranslateC` in `build.zig`:

```zig
// build.zig
const translate_c = b.addTranslateC(.{
    .root_source_file = b.path("src/c.h"),
    .target = target,
    .optimize = optimize,
});
translate_c.linkSystemLibrary("glfw", .{});

const exe = b.addExecutable(.{
    .name = "myapp",
    .root_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .optimize = optimize,
        .target = target,
        .imports = &.{
            .{ .name = "c", .module = translate_c.createModule() },
        },
    }),
});
```

Then in Zig: `const c = @import("c");`

**Note**: The C translator is now backed by arocc instead of libclang.

### Switch Improvements

- `packed struct` and `packed union` may now be used as switch prong items (compared by backing integer).
- `decl literals` and result-type-dependent expressions (e.g. `@enumFromInt`) may now be used as switch prong items.
- Union tag captures are now allowed for all prongs, not just inline ones.
- Switch prong captures may no longer all be discarded.
- `.awake` atomic order removed; use `.monotonic` / `.seq_cst`.

### Other Language-Level Gotchas

- **Small integer types coerce to floats** — e.g. `u8` → `f32` is now allowed.
- **Runtime vector indexes forbidden** — vector indexing must be comptime-known.
- **Vectors and arrays no longer support in-memory coercion** — must explicitly cast element-wise.
- **Trivial local addresses cannot be returned from functions** — e.g. `return &local;` is rejected more consistently.
- **Packed unions must not have unused bits** — all bit patterns must be valid.
- **Pointers forbidden in packed structs/unions**.
- **Explicitly-aligned pointer types are now distinct** from naturally-aligned ones in type system.
- **Simplified dependency loop rules** — most code still works, but self-referential alignment queries now error.

---

## Standard Library: std.Io

### Networking

- `std.net` is gone. Use `std.Io.net` (types: `IpAddress`, `Stream`, `Server`, `UnixAddress`).
- High-level helpers like `tcpConnectToHost`, `Address.parseIp`, `Stream.connect` removed. Use `Io` vtables: `io.vtable.netConnectIp`, `netRead`, `netWrite`, `netListenIp`, `netAccept`.
- Raw fd operations: use `std.os.linux.socket`, `accept4`, `connect`, `bind`, `listen`, `write`, `read`, `close`.
- `posix.socket`, `posix.bind`, `posix.accept`, `posix.shutdown`, `posix.writev`, `posix.listen`, `posix.pipe2`, `posix.getrandom`, `std.net.has_unix_sockets` removed.
- Epoll: `std.os.linux.epoll_create1/epoll_ctl/epoll_wait` directly.
- New: `std.Io.net.Socket.createPair` for socketpair usage in tests.

### I/O Readers/Writers

- Writer/reader interfaces live under `std.Io`. TLS expects `*std.Io.Reader` / `*std.Io.Writer` (struct fields `.interface`). Pass pointers to interfaces, not call `interface()` as a function.
- `std.time.sleep` removed; use `std.Io.sleep(io, Duration, Clock)`.
- `std.Io.GenericWriter`, `std.Io.AnyWriter`, `std.Io.null_writer`, `std.Io.CountingReader` removed.

### Time / Random / Env / Exit

- `std.time.milliTimestamp` removed. Use `std.time.Timer` or `std.Io.Clock.now(clock, io)` and compare `Timestamp.nanoseconds`.
- Random secure bytes: `std.Io.randomSecure(io, buf)`; no `std.crypto.random` or `std.posix.getrandom` convenience.
- `std.process.getEnvVarOwned` removed; use `std.c.getenv` and copy.
- `std.posix.exit` removed; use `std.process.exit`.

### TLS Client Options

- `std.crypto.tls.Client.Options` requires `entropy: *const [entropy_len]u8` and `realtime_now_seconds: i64`. Fill entropy with `std.Io.randomSecure`; compute seconds with `Io.Clock.now(.real, io)` / `ns_per_s`.

### MemoryPool API Changes

- `std.heap.MemoryPool(T).initCapacity(allocator, n)` returns the pool.
- `create`/`destroy` now require allocator. No bare `init()` or zero-arg `deinit()`.

### Format Options

- `std.fmt.Options` replaces `FormatOptions`.
- `std.fmt.format` helper removed; call `writer.print` directly (writers live in `std.Io.Writer`).
- `std.fmt.Formatter` renamed to `Alt`.
- `std.fmt.bufPrintZ` renamed to `std.fmt.bufPrintSentinel`.

### Randomness / Crypto

- `std.crypto.random` removed. Use an `std.Io` instance: `const io = std.Io.Threaded.global_single_threaded.ioBasic(); io.random(&buf);`.
- `Ed25519.KeyPair.generate` now requires an `io: std.Io` argument.

### Enum Conversion

- `std.meta.intToEnum` removed. Use `std.enums.fromInt(EnumType, value)` (returns `?EnumType`).
- `meta.declList` removed.

### Fixed-Buffer Writers in Tests

- `std.io.fixedBufferStream` removed. For in-memory writes use `var w = std.Io.Writer.fixed(buf);` and read bytes with `std.Io.Writer.buffered(&w)`.
- `std.ArrayList` no longer has `.init(allocator)` shorthand; use `.initCapacity(allocator, n)`.

### Collections

- `SegmentedList` removed.
- `BitSet`, `EnumSet`: replace `initEmpty`, `initFull` with decl literals (e.g. `.{}`, `.{ .a = true }`).
- New: `Io.Dir.renamePreserve` — rename without replacing destination.
- `std.Io.Dir.rename` returns `error.DirNotEmpty` rather than `error.PathAlreadyExists`.

### Error Set Renames

```zig
// Old -> New
error.RenameAcrossMountPoints -> error.CrossDevice
error.NotSameFileSystem       -> error.CrossDevice
error.SharingViolation        -> error.FileBusy
error.EnvironmentVariableNotFound -> error.EnvironmentVariableMissing
```

### Math

- `math.sign` now returns the smallest integer type that fits possible values.

### Threading

- `Thread.Mutex.Recursive` removed.

### Compression

- `lzma`, `lzma2`, `xz` updated to `Io.Reader` / `Io.Writer` interfaces.

### Dynamic Libraries

- `DynLib` removed Windows support. Use `LoadLibraryExW` and `GetProcAddress` directly.

---

## New Execution Model: Juicy Main

Zig 0.16.0 introduces `std.process.Init` as the preferred `main` parameter. This is the biggest entrypoint change in this release.

### `main` signatures

```zig
// 1. Bare (no args/env available)
pub fn main() !void { }

// 2. Minimal (raw argv + environ only)
pub fn main(init: std.process.Init.Minimal) !void { }

// 3. Juicy Main (recommended)
pub fn main(init: std.process.Init) !void { }
```

### `std.process.Init` contents

```zig
pub const Init = struct {
    minimal: Minimal,
    arena: *std.heap.ArenaAllocator,  // process-lifetime arena, threadsafe
    gpa: Allocator,                     // general-purpose allocator
    io: Io,                             // default Io implementation
    environ_map: *Environ.Map,          // env vars (not threadsafe)
    preopens: Preopens,                 // WASI-style preopened files

    pub const Minimal = struct {
        environ: Environ,
        args: Args,
    };
};
```

### Example

```zig
const std = @import("std");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    try std.Io.File.stdout().writeStreamingAll(io, "Hello, world!\n");

    const args = try init.minimal.args.toSlice(init.arena.allocator());
    for (args, 0..) |arg, i| {
        std.log.info("arg[{d}] = {s}", .{ i, arg });
    }

    std.log.info("{d} env vars", .{init.environ_map.count()});
}
```

**Rule of thumb**: if your program needs I/O, allocations, or environment variables, use `std.process.Init`.

---

## Environment Variables & Process Args Are Non-Global

`std.os.environ` and `std.process.args` as global state are gone. They are now only available through `main`'s `init` parameter.

### Environment variables

```zig
// Juicy Main
for (init.environ_map.keys(), init.environ_map.values()) |key, value| {
    std.log.info("env: {s}={s}", .{ key, value });
}

// Minimal main
std.log.info("HOME: {?s}", .{init.environ.getPosix("HOME")});
std.log.info("EDITOR: {s}", .{ try init.environ.getAlloc(arena, "EDITOR") });
const map = try init.environ.createMap(arena);
```

### CLI arguments

```zig
// Iterate lazily
var args = init.args.iterate();
while (args.next()) |arg| {
    std.log.info("arg: {s}", .{arg});
}

// To slice (needs allocator)
const args = try init.args.toSlice(arena);
```

Any function that needs environment variables should accept `*const process.Environ.Map` or `process.Environ` as a parameter.

---

## Thread.Pool Removed → Use std.Io

`std.Thread.Pool` is removed. Replace with `std.Io.Group`, `std.Io.async`, or `std.Io.concurrent`.

### Migration example

```zig
// OLD (0.15)
fn doAllTheWork(pool: *std.Thread.Pool) void {
    var wg: std.Thread.WaitGroup = .{};
    pool.spawnWg(wg, doSomeWork, .{ pool, &wg, first_work_item });
    wg.wait();
}

// NEW (0.16)
fn doAllTheWork(io: std.Io) void {
    var g: std.Io.Group = .init;
    errdefer g.cancel(io);
    g.async(io, doSomeWork, .{ io, &g, first_work_item });
    g.wait(io);
}

fn doSomeWork(io: std.Io, g: *std.Io.Group, foo: Foo) void {
    foo.doTheThing();
    for (foo.new_work_items) |new| {
        g.async(io, doSomeWork, .{ io, g, new });
    }
}
```

**Important**: when migrating from `Thread.Pool` to `Io`, convert `Thread.Mutex`, `Thread.Condition`, `Thread.ResetEvent` to `Io.Mutex`, `Io.Condition`, `Io.Event`.

---

## File I/O & fs API Changes

### readFileAlloc / readToEndAlloc

```zig
// OLD
const contents = try std.fs.cwd().readFileAlloc(allocator, file_name, 1234);

// NEW
const contents = try std.Io.Dir.cwd().readFileAlloc(io, file_name, allocator, .limited(1234));
// Note: FileTooBig -> StreamTooLong when limit reached.
```

```zig
// OLD
const contents = try file.readToEndAlloc(allocator, 1234);

// NEW
var file_reader = file.reader(&.{});
const contents = try file_reader.interface.allocRemaining(allocator, .limited(1234));
```

### setTimestamps

```zig
// OLD
try atomic_file.file_writer.file.setTimestamps(io, src_stat.atime, src_stat.mtime);

// NEW
try atomic_file.file_writer.file.setTimestamps(io, .{
    .access_timestamp = .init(src_stat.atime),
    .modify_timestamp = .init(src_stat.mtime),
});
```

`Io.File.Stat.atime` is now `?Timestamp` (can be `null` on e.g. ZFS).

### Directory walking

- `std.Io.Dir.walk` still exists.
- New: `std.Io.Dir.walkSelectively` — opt-in recursion per directory, avoiding redundant open/close syscalls.
- Both `Walker` and `SelectiveWalker` gained `leave()` and `depth()`.

### fs.path.relative is now pure

```zig
// OLD
const relative = try std.fs.path.relative(gpa, from, to);

// NEW
const cwd_path = try std.process.currentPathAlloc(io, gpa);
const relative = try std.fs.path.relative(gpa, cwd_path, environ_map, from, to);
```

### fs.path Windows changes

- `windowsParsePath`/`diskDesignator`/`diskDesignatorWindows` → `parsePath`, `parsePathWindows`, `parsePathPosix`
- Added `getWin32PathType`
- `componentIterator`/`ComponentIterator.init` can no longer fail

### Current Directory API renamed

```zig
// OLD
std.process.getCwd(buffer)
std.process.getCwdAlloc(allocator)

// NEW
std.process.currentPath(io, buffer)
std.process.currentPathAlloc(io, allocator)
```

---

## Preopens & Atomic / Temporary Files

### Preopens

```zig
// OLD (WASI)
const wasi_preopens: std.fs.wasi.Preopens = try .preopensAlloc(arena);

// NEW
const preopens: std.process.Preopens = try .init(arena);
// Or simply use init.preopens from Juicy Main.
```

`Preopens` is `void` on non-WASI targets — zero-cost abstraction.

### Atomic / Temporary Files

`std.Io.File.Atomic` is the new API for atomic file writes and temporary files.

- Linux: integrates with `O_TMPFILE` when possible.
- New: `std.Io.File.hardLink`
- Temporary/random filenames are generated via the Io vtable instead of `std.crypto.random`.

---

## Memory & Allocator Changes

### ArenaAllocator is now lock-free and thread-safe

`std.heap.ArenaAllocator` no longer needs `ThreadSafeAllocator` wrapping. Performance is comparable single-threaded and faster under contention (~7 threads).

### ThreadSafe Allocator removed

`std.heap.ThreadSafe` is removed; use `ArenaAllocator` directly, or synchronize access manually.

### Memory Locking / Protection moved to `std.process`

```zig
// OLD
try std.posix.mlock();
try std.posix.mlock2(slice, std.posix.MLOCK_ONFAULT);
try std.posix.mlockall(slice, std.posix.MCL_CURRENT | std.posix.MCL_FUTURE);

// NEW
try std.process.lockMemory(slice, .{});
try std.process.lockMemory(slice, .{ .on_fault = true });
try std.process.lockMemoryAll(.{ .current = true, .future = true });
```

mmap/mprotect flags are now type-safe structs instead of bitwise OR:

```zig
// OLD
std.posix.PROT.READ | std.posix.PROT.WRITE

// NEW
.{ .READ = true, .WRITE = true }
```

### MemoryPool

- `std.heap.MemoryPool(T).initCapacity(allocator, n)` returns the pool.
- `create`/`destroy` now require allocator parameter.
- New unmanaged variants: `MemoryPoolUnmanaged`, `MemoryPoolAlignedUnmanaged`, `MemoryPoolExtraUnmanaged`.

### Io.Writer.Allocating alignment field

```zig
alignment: std.mem.Alignment,  // new field
```

---

## Container & Collection Changes

### Migration to "Unmanaged"

Managed variants with embedded `Allocator` fields are being phased out.

- `ArrayHashMap`, `AutoArrayHashMap`, `StringArrayHashMap` **removed**.
- `AutoArrayHashMapUnmanaged` → `array_hash_map.Auto`
- `StringArrayHashMapUnmanaged` → `array_hash_map.String`
- `ArrayHashMapUnmanaged` → `array_hash_map.Custom`

### PriorityQueue

```zig
var queue = std.PriorityQueue(u32, void, lessThan).empty;
```

- `init` → `initContext`
- `add` → `push`
- `addSlice` → `pushSlice`
- `remove` / `removeOrNull` → `pop`
- `removeIndex` → `popIndex`

### PriorityDequeue

```zig
var dq = std.PriorityDequeue(u32, void, lessThan).empty;
```

- `init` → `.empty`
- `add` → `push`
- `removeMinOrNull` / `removeMin` → `popMin`
- `removeMaxOrNull` / `removeMax` → `popMax`
- `removeIndex` → `popIndex`

### BitSet / EnumSet

Use decl literals instead of `initEmpty` / `initFull`:

```zig
var set = std.EnumSet(MyEnum).empty;
var full = std.EnumSet(MyEnum).full;
```

### SegmentedList

- `std.SegmentedList` removed.

---

## Removed APIs (Quick List)

| Removed API | Replacement |
|-------------|-------------|
| `std.Thread.Pool` | `std.Io.Group`, `Io.async`, `Io.concurrent` |
| `std.heap.ThreadSafe` | `std.heap.ArenaAllocator` (now thread-safe) |
| `std.io.fixedBufferStream` | `std.Io.Reader.fixed(data)` / `std.Io.Writer.fixed(buf)` |
| `std.Io.GenericReader` | `std.Io.Reader` |
| `std.Io.AnyReader` | `std.Io.Reader` |
| `std.Io.GenericWriter` | `std.Io.Writer` |
| `std.Io.AnyWriter` | `std.Io.Writer` |
| `std.Io.null_writer` | Use `std.Io.Writer` directly |
| `std.Io.CountingReader` | No direct replacement; track bytes manually |
| `std.SegmentedList` | None |
| `std.meta.declList` | None |
| `std.fs.getAppDataDir` | None |
| `std.posix.getCwd*` | `std.process.currentPath*` |
| `std.posix.mlock*` | `std.process.lockMemory*` |
| `std.posix.PROT_*` bitwise flags | Struct literal flags |
| `std.ArrayHashMap` | `array_hash_map.Auto` / `.String` / `.Custom` |

---

## Common Idioms in Zig 0.16.0

### ArrayList

```zig
const std = @import("std");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;

    var list = try std.ArrayList(u8).initCapacity(gpa, 16);
    defer list.deinit(gpa);

    try list.append(gpa, 'a');
    try list.appendSlice(gpa, "hello");
    try list.ensureTotalCapacity(gpa, 100);

    const owned = try list.toOwnedSlice(gpa);
    defer gpa.free(owned);
}
```

### HashMap (Default / Unmanaged Style)

```zig
var map = std.StringHashMap(u32).empty;
defer map.deinit(gpa);

try map.put(gpa, "key", 42);
const val = map.get("key") orelse 0;
```

### stdout / stderr with Juicy Main

```zig
pub fn main(init: std.process.Init) !void {
    const io = init.io;

    // Direct streaming write
    try std.Io.File.stdout().writeStreamingAll(io, "Hello, world!\n");

    // Or via writer interface
    var stdout_writer = std.Io.File.stdout().writer(&.{});
    try stdout_writer.interface.print("value: {d}\n", .{42});
}
```

### Fixed-Buffer Reader / Writer

```zig
// Reader from byte slice
var data = "line1\nline2\n";
var reader: std.Io.Reader = .fixed(data);

// Writer into a stack buffer
var buf: [256]u8 = undefined;
var writer: std.Io.Writer = .fixed(&buf);
try writer.interface.print("count: {d}", .{7});
```

### File I/O

```zig
// Read entire file
const contents = try std.Io.Dir.cwd().readFileAlloc(io, "input.txt", gpa, .limited(1024 * 1024));
defer gpa.free(contents);

// Write file atomically
var atomic = try std.Io.File.Atomic.init(io, gpa, "output.txt");
try atomic.file_writer.interface.print("data: {s}\n", .{"hello"});
try atomic.commit(io);
```

### JSON

```zig
const MyStruct = struct {
    name: []const u8,
    value: u32,
};

// Parsing
const json_str =
    \\{"name": "test", "value": 42}
;
const parsed = try std.json.parseFromSlice(MyStruct, gpa, json_str, .{});
defer parsed.deinit();

// Serialization to string (via Io.Writer.Allocating)
var out: std.Io.Writer.Allocating = .init(gpa);
defer out.deinit();
var stringify: std.json.Stringify = .{
    .writer = &out.writer,
    .options = .{},
};
try stringify.write(parsed.value);
const json_output = out.written();
```

### Type Reflection (Comptime)

```zig
const info = @typeInfo(T);
if (info == .pointer) { ... }
if (info == .slice) { ... }
if (info == .@"struct") { ... }
if (info == .@"enum") { ... }
if (info == .@"union") { ... }
if (info == .optional) { ... }
```

### Testing

```zig
test "basic arithmetic" {
    const std = @import("std");
    try std.testing.expectEqual(4, 2 + 2);
    try std.testing.expectError(error.OutOfMemory, mightFail());
}

test "no leaks" {
    const std = @import("std");
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    const ptr = try allocator.create(u32);
    defer allocator.destroy(ptr);
}
```

### Leb128 (Binary Format)

```zig
// OLD: std.leb.readUleb128(reader)
// NEW:
const value = try reader.takeLeb128(u64);
```

---

## Common Error Messages and Fixes (0.16)

| Error | Cause | Fix |
|-------|-------|-----|
| `no field named 'getStdOut'` | Using old `std.io.getStdOut()` | Use `std.Io.File.stdout()` |
| `expected 2 argument(s), found 1` | `ArrayList` method missing allocator | Add allocator as first argument |
| `no member named 'Pool' in 'Thread'` | `std.Thread.Pool` removed | Use `std.Io.Group` + `Io.async` |
| `expected type '*const process.Environ.Map'` | Function needs env map param | Pass it from `main(init)` |
| `no member named 'fixedBufferStream'` | Old stream API removed | Use `std.Io.Reader.fixed(...)` or `std.Io.Writer.fixed(...)` |
| `no field named 'getCwd' in 'process'` | Renamed | Use `std.process.currentPath*` |
| `fingerprint field missing` | Old `build.zig.zon` | Add `.fingerprint = 0x...` and change `.name = "x"` to `.name = .x` |
| `type depends on itself for alignment` | Self-referential alignment query | Remove `@alignOf(@This())` or restructure |

---

## Build System Changes

### Fingerprint Required in build.zig.zon

`build.zig.zon` now requires a `fingerprint` field, and `name` must be an enum literal, not a string:

```zig
.{
    .name = .myproject,  // enum literal, not "myproject"
    .fingerprint = 0x123456789abcdef0,
    .version = "0.1.0",
    .dependencies = .{},
}
```

`zig build` will fail if these are missing.

### Local Package Overrides: --fork

New CLI flag for temporary local overrides:

```bash
zig build --fork=/path/to/local/fork
```

Any dependency with matching `name` + `fingerprint` will resolve to the local path instead of being fetched. This is ephemeral — remove the flag to revert.

### Packages Fetched to zig-pkg Directory

Dependencies are now fetched into a local `zig-pkg/` directory (next to `build.zig`) instead of the global cache. After filtering, they are recompressed into the global cache for future reuse.

### Unit Test Timeouts

Specify per-test timeouts:

```bash
zig build test --test-timeout 500ms
```

After the timeout, the test process is killed and restarted for the next test.

### ConfigHeader

- `std.Build.Step.ConfigHeader` now handles leading whitespace for cmake-style config headers correctly.

---

## Compiler / Toolchain

### C Translation

- `@cImport` still exists but is deprecated and now backed by arocc instead of libclang.
- For new code, always use `addTranslateC` in `build.zig`.

### Type Resolution

- Reworked internal type resolution. Most previously-working code still works, and some "dependency loop" errors are now resolved.
- Self-referential alignment queries (e.g. `align(@alignOf(@This()))`) now correctly error.

### LLVM Backend

- Experimental incremental compilation support.
- Error set types now lowered as enums in debug info, so error names are visible at runtime.

---

## Target Support

Notable additions:
- `aarch64-freebsd`, `aarch64-netbsd`, `loongarch64-linux`, `powerpc64le-linux`, `s390x-linux`, `x86_64-freebsd`, `x86_64-netbsd`, `x86_64-openbsd` now tested in CI.
- `aarch64-maccatalyst`, `x86_64-maccatalyst` cross-compilation added.
- Initial `loongarch32-linux` support (no libc yet).
- Basic support added for Alpha, KVX, MicroBlaze, OpenRISC, PA-RISC, SuperH.
- Solaris, AIX, z/OS support removed.
- Stack tracing improved across almost all major targets.

---

## Testing Adjustments

- For in-process client/server tests, use `std.Io.net.Socket.createPair` or raw `socketpair(AF.UNIX, SOCK.STREAM|CLOEXEC, 0, &fds)` to avoid relying on Io vtable network (Threaded nets return `NetworkDown` if not wired).
- Error sets tightened in many std.Io functions — remove unreachable branches accordingly.

---

## Porting Strategy (0.15 → 0.16)

1. **Replace `@Type` calls** with the specific new builtin functions.
2. **Replace `@cImport`** with `addTranslateC` in `build.zig`.
3. **Add `fingerprint` and fix `name`** in `build.zig.zon`.
4. **Thread `std.Io` through your app** — any function doing I/O, sleep, random, or time needs an `io` parameter.
5. **Update `std.net` usages** to `std.Io.net` or raw syscalls.
6. **Update `ArrayList` calls** to pass allocator explicitly and use `initCapacity`.
7. **Fix error set names** (CrossDevice, FileBusy, EnvironmentVariableMissing, DirNotEmpty).
8. **Run `zig build test --test-timeout 500ms`** to catch hanging tests early.

Use this skill when writing new Zig 0.16.0 code or upgrading from 0.15.x.
