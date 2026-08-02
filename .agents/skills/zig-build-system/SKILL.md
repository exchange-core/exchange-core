---
name: zig-build-system
description: Zig build system skill for multi-file Zig projects. Use when writing or debugging build.zig files, adding executables or libraries, integrating C source files, configuring build-time options, running tests with zig build test, or managing build.zig.zon package manifests. Activates on queries about build.zig, b.addExecutable, addCSourceFiles, b.option, zig build, build.zig.zon, or Zig package management.
---

# Zig Build System

## Purpose

Guide agents through writing `build.zig` files: executables, libraries, C source integration, build options, test configuration, and `build.zig.zon` package manifests.

## Triggers

- "How do I set up a build.zig file?"
- "How do I add a C library to a Zig project?"
- "How do I define build-time options in Zig?"
- "How do I run Zig tests with zig build test?"
- "What is build.zig.zon and how do I use it?"
- "How do I add a Zig package dependency?"

## Workflow

### 1. Project initialization

```bash
# Initialize a new project
mkdir myproject && cd myproject
zig init          # creates src/main.zig and build.zig

# Build
zig build

# Run
zig build run

# Test
zig build test
```

### 2. build.zig structure

```zig
const std = @import("std");

pub fn build(b: *std.Build) void {
    // Standard options (--optimize, --target)
    const optimize = b.standardOptimizeOption(.{});
    const target = b.standardTargetOptions(.{});

    // Executable
    const exe = b.addExecutable(.{
        .name = "myapp",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Install step (zig build → copies to zig-out/bin/)
    b.installArtifact(exe);

    // Run step (zig build run)
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    // Test step (zig build test)
    const unit_tests = b.addTest(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
}
```

### 3. Libraries

```zig
// Static library
const lib = b.addStaticLibrary(.{
    .name = "mylib",
    .root_source_file = b.path("src/mylib.zig"),
    .target = target,
    .optimize = optimize,
});
b.installArtifact(lib);

// Shared library
const shared_lib = b.addSharedLibrary(.{
    .name = "mylib",
    .root_source_file = b.path("src/mylib.zig"),
    .target = target,
    .optimize = optimize,
    .version = .{ .major = 1, .minor = 0, .patch = 0 },
});
b.installArtifact(shared_lib);

// Link library into executable
exe.linkLibrary(lib);
```

### 4. Adding C source files

```zig
// Single C file
exe.addCSourceFile(.{
    .file = b.path("src/legacy.c"),
    .flags = &.{ "-std=c11", "-Wall", "-Wextra" },
});

// Multiple C files
exe.addCSourceFiles(.{
    .files = &.{
        "src/a.c",
        "src/b.c",
        "src/c.c",
    },
    .flags = &.{ "-std=c11", "-O2" },
});

// Include directories
exe.addIncludePath(b.path("include/"));
exe.addIncludePath(.{ .cwd_relative = "/usr/local/include" });

// System libraries
exe.linkSystemLibrary("curl");
exe.linkSystemLibrary("ssl");
exe.linkLibC();  // link libc (required if calling C stdlib)
```

### 5. Build-time options

```zig
pub fn build(b: *std.Build) void {
    // Boolean option
    const enable_logging = b.option(
        bool,
        "logging",
        "Enable debug logging",
    ) orelse false;

    // Enum option
    const Backend = enum { opengl, vulkan, software };
    const backend = b.option(
        Backend,
        "backend",
        "Rendering backend",
    ) orelse .opengl;

    // Integer option
    const max_connections = b.option(
        u32,
        "max-connections",
        "Maximum concurrent connections",
    ) orelse 64;

    // Pass to Zig code as compile-time constant
    const options = b.addOptions();
    options.addOption(bool, "enable_logging", enable_logging);
    options.addOption(Backend, "backend", backend);
    options.addOption(u32, "max_connections", max_connections);

    exe.root_module.addOptions("build_options", options);
}
```

In Zig source:
```zig
const build_options = @import("build_options");

pub fn main() void {
    if (build_options.enable_logging) {
        std.debug.print("Logging enabled\n", .{});
    }
}
```

```bash
# Pass options on command line
zig build -Dlogging=true -Dbackend=vulkan -Dmax-connections=256
```

### 6. Module system

```zig
// Create a module (reusable across targets)
const mymodule = b.addModule("mymodule", .{
    .root_source_file = b.path("src/mymodule.zig"),
});

// Use module in executable
exe.root_module.addImport("mymodule", mymodule);

// Share module between exe and tests
const utils = b.addModule("utils", .{
    .root_source_file = b.path("src/utils.zig"),
});
exe.root_module.addImport("utils", utils);
unit_tests.root_module.addImport("utils", utils);
```

In Zig source:
```zig
const utils = @import("utils");
const mymodule = @import("mymodule");
```

### 7. Package management with build.zig.zon

```zig
// build.zig.zon
.{
    .name = "myapp",
    .version = "0.1.0",
    .minimum_zig_version = "0.13.0",

    .dependencies = .{
        .zig_clap = .{
            .url = "https://github.com/Hejsil/zig-clap/archive/refs/tags/0.9.1.tar.gz",
            .hash = "1220...",  // Run zig build to get the hash
        },
        .known_folders = .{
            .url = "https://github.com/ziglibs/known-folders/archive/refs/heads/master.tar.gz",
            .hash = "1220...",
        },
    },

    .paths = .{
        "build.zig",
        "build.zig.zon",
        "src",
        "LICENSE",
        "README.md",
    },
}
```

```zig
// build.zig — use the dependency
const clap_dep = b.dependency("zig_clap", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("clap", clap_dep.module("clap"));
```

```bash
# Fetch dependencies (creates zig-cache/packages/)
zig build    # auto-fetches on first run

# Zig will print the hash if missing — copy it into build.zig.zon
```

### 8. Custom build steps

```zig
// Code generation step
const gen_step = b.addSystemCommand(&.{
    "python3", "scripts/gen.py", "--output", "src/generated.zig",
});
exe.step.dependOn(&gen_step.step);

// Custom install step
const install_config = b.addInstallFile(
    b.path("config/default.toml"),
    "share/myapp/config.toml",
);
b.getInstallStep().dependOn(&install_config.step);
```

For advanced build.zig patterns, see [references/build-zig-patterns.md](references/build-zig-patterns.md).

## Related skills

- Use `skills/zig/zig-compiler` for single-file builds and compiler flags
- Use `skills/zig/zig-cinterop` for C library integration in build.zig
- Use `skills/zig/zig-cross` for cross-compilation in build.zig
- Use `skills/build-systems/cmake` when embedding Zig into a CMake project
