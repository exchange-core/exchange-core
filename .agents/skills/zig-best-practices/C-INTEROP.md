# C Interoperability in Zig

Zig can directly import C headers, call C functions, and expose Zig functions to C. Use these patterns when integrating with existing C libraries or system APIs.

## When to Use

- Wrapping C libraries (raylib, SDL, curl)
- Calling platform-specific system APIs
- Passing callbacks to C code
- Writing Zig libraries callable from C

## Importing C Headers

Use `@cImport` to import C headers directly:

```zig
const ray = @cImport({
    @cInclude("raylib.h");
});

pub fn main() void {
    ray.InitWindow(800, 450, "window title");
    defer ray.CloseWindow();

    ray.SetTargetFPS(60);
    while (!ray.WindowShouldClose()) {
        ray.BeginDrawing();
        defer ray.EndDrawing();
        ray.ClearBackground(ray.RAYWHITE);
    }
}
```

Configure include paths in `build.zig`:

```zig
exe.addIncludePath(.{ .cwd_relative = "/usr/local/include" });
exe.linkSystemLibrary("raylib");
```

## Extern Functions (System APIs)

Call platform APIs without bindings using `extern`:

```zig
const win = @import("std").os.windows;

extern "user32" fn MessageBoxA(
    ?win.HWND,
    [*:0]const u8,
    [*:0]const u8,
    u32,
) callconv(.winapi) i32;
```

## C Callbacks

Pass Zig functions to C libraries using `callconv(.C)`:

```zig
fn writeCallback(
    data: *anyopaque,
    size: c_uint,
    nmemb: c_uint,
    user_data: *anyopaque,
) callconv(.C) c_uint {
    const buffer: *std.ArrayList(u8) = @alignCast(@ptrCast(user_data));
    const typed_data: [*]u8 = @ptrCast(data);
    buffer.appendSlice(typed_data[0 .. nmemb * size]) catch return 0;
    return nmemb * size;
}
```

Key points:
- `callconv(.C)` makes the function callable from C
- `*anyopaque` is Zig's equivalent of `void*`
- Use `@alignCast` and `@ptrCast` to recover typed pointers
- Return 0 on error (C convention) since Zig errors can't cross FFI boundary

## C Types Mapping

| C Type | Zig Type |
|--------|----------|
| `void*` | `*anyopaque` |
| `char*` | `[*:0]const u8` (null-terminated) |
| `size_t` | `usize` |
| `int` | `c_int` |
| `unsigned int` | `c_uint` |
| `NULL` | `null` |
