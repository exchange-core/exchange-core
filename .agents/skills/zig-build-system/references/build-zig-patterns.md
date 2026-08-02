# build.zig Advanced Patterns Reference

## Multi-target Builds

```zig
pub fn build(b: *std.Build) void {
    const targets = [_]std.Target.Query{
        .{ .cpu_arch = .x86_64, .os_tag = .linux },
        .{ .cpu_arch = .aarch64, .os_tag = .linux },
        .{ .cpu_arch = .x86_64, .os_tag = .windows },
        .{ .cpu_arch = .aarch64, .os_tag = .macos },
    };

    for (targets) |t| {
        const target = b.resolveTargetQuery(t);
        const exe = b.addExecutable(.{
            .name = b.fmt("myapp-{s}-{s}", .{
                @tagName(t.cpu_arch.?),
                @tagName(t.os_tag.?),
            }),
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        });
        b.installArtifact(exe);
    }
}
```

## Conditional Compilation

```zig
// Based on target
const builtin = @import("builtin");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "myapp",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Platform-specific sources
    const resolved = target.result;
    switch (resolved.os.tag) {
        .linux => exe.addCSourceFile(.{
            .file = b.path("src/platform/linux.c"),
            .flags = &.{"-std=c11"},
        }),
        .windows => exe.addCSourceFile(.{
            .file = b.path("src/platform/windows.c"),
            .flags = &.{"-std=c11"},
        }),
        .macos => exe.addCSourceFile(.{
            .file = b.path("src/platform/macos.c"),
            .flags = &.{"-std=c11"},
        }),
        else => {},
    }

    // Windows-specific linking
    if (resolved.os.tag == .windows) {
        exe.linkSystemLibrary("ws2_32");
        exe.linkSystemLibrary("advapi32");
    }
}
```

## External C Library Integration

### With pkg-config

```zig
// Link library found via pkg-config
exe.linkSystemLibrary2("libcurl", .{ .use_pkg_config = .force });
exe.linkSystemLibrary2("openssl", .{ .use_pkg_config = .try_first });
exe.linkLibC();
```

### With explicit paths (no pkg-config)

```zig
const lib_path = b.path("vendor/mylib/lib");
const include_path = b.path("vendor/mylib/include");

exe.addLibraryPath(lib_path);
exe.addIncludePath(include_path);
exe.linkSystemLibrary("mylib");
exe.linkLibC();
```

### Build C library from source

```zig
const mylib = b.addStaticLibrary(.{
    .name = "mylib",
    .target = target,
    .optimize = optimize,
});
mylib.addCSourceFiles(.{
    .files = &.{
        "vendor/mylib/src/a.c",
        "vendor/mylib/src/b.c",
    },
    .flags = &.{ "-std=c99", "-fPIC" },
});
mylib.addIncludePath(b.path("vendor/mylib/include"));
mylib.linkLibC();

exe.linkLibrary(mylib);
exe.addIncludePath(b.path("vendor/mylib/include"));
```

## Testing Patterns

```zig
// Separate test binary per module
const lib_tests = b.addTest(.{
    .root_source_file = b.path("src/lib.zig"),
    .target = target,
    .optimize = optimize,
    .test_runner = b.path("test_runner.zig"),  // Custom test runner
});

// Filter tests by name
const run_lib_tests = b.addRunArtifact(lib_tests);
run_lib_tests.addArg("--test-filter=my_test_prefix");

// Integration tests
const integration_tests = b.addTest(.{
    .root_source_file = b.path("tests/integration.zig"),
    .target = target,
    .optimize = optimize,
});
integration_tests.root_module.addImport("mylib", mymodule);

const test_step = b.step("test", "Run all tests");
test_step.dependOn(&b.addRunArtifact(lib_tests).step);
test_step.dependOn(&b.addRunArtifact(integration_tests).step);
```

## Install Layout Control

```zig
// Default: zig-out/bin/, zig-out/lib/
b.installArtifact(exe);

// Custom install directory
b.installFile(b.path("config/default.conf"), "etc/myapp/default.conf");
b.installDirectory(.{
    .source_dir = b.path("assets"),
    .install_dir = .prefix,
    .install_subdir = "share/myapp/assets",
});

// Install headers
b.installFile(b.path("src/mylib.h"), "include/mylib.h");

// Change install prefix at build time
// zig build --prefix /usr/local
```

## Custom Build Steps and Runners

```zig
// Generate version file
const version_step = b.addWriteFile("src/version.zig",
    b.fmt(
        \\pub const version = "{}";
        \\pub const git_hash = "{}";
    , .{ b.version, "abc1234" }),
);
exe.step.dependOn(&version_step.step);

// Run code generator
const gen = b.addSystemCommand(&.{
    "python3",
    b.pathFromRoot("scripts/gen_bindings.py"),
    "--input", b.pathFromRoot("schema.json"),
    "--output", "src/bindings.zig",
});
exe.step.dependOn(&gen.step);

// Custom step alias
const check_step = b.step("check", "Check for compile errors");
check_step.dependOn(&exe.step);
```

## Dependency Tree

```zig
// Print dependency tree
// zig build --verbose
// Shows all steps and their dependencies

// Force rebuild
// zig build --force
```

## Build Variables Available

```zig
pub fn build(b: *std.Build) void {
    _ = b.graph.zig_exe;         // Path to zig compiler
    _ = b.graph.env_map;         // Environment variables
    _ = b.install_prefix;        // Install prefix (--prefix)
    _ = b.cache_root;            // Cache directory
    _ = b.global_cache_root;     // Global cache
    _ = b.build_root;            // Project root
}
```
