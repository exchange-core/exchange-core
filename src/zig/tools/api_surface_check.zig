const std = @import("std");

const SurfaceCheckError = error{
    MissingFunctionName,
    MissingContainer,
    MalformedContainer,
    MalformedSource,
    ApiSurfaceMismatch,
};

const ContainerBounds = struct {
    open_brace_start: usize,
    close_brace_start: usize,
};

const ContainerDeclarationState = enum {
    idle,
    pub_keyword,
    const_keyword,
    target_name,
    equal,
    struct_keyword,
};

const FunctionDeclarationState = enum {
    idle,
    pub_keyword,
    modifier,
    fn_keyword,
};

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const core_source = try std.Io.Dir.cwd().readFileAlloc(
        init.io,
        "src/zig/core/exchange_core.zig",
        allocator,
        .unlimited,
    );
    defer allocator.free(core_source);

    const api_source = try std.Io.Dir.cwd().readFileAlloc(
        init.io,
        "src/zig/core/exchange_api.zig",
        allocator,
        .unlimited,
    );
    defer allocator.free(api_source);

    var core_names: std.StringHashMapUnmanaged(void) = .empty;
    defer core_names.deinit(allocator);

    var api_names: std.StringHashMapUnmanaged(void) = .empty;
    defer api_names.deinit(allocator);

    try collectPublicFunctionNames(allocator, core_source, "ExchangeCore", &core_names);
    try collectPublicFunctionNames(allocator, api_source, "ExchangeApi", &api_names);

    var missing: std.ArrayListUnmanaged([]const u8) = .empty;
    defer missing.deinit(allocator);

    try collectMissingFunctionNames(allocator, &core_names, &api_names, &missing);

    if (missing.items.len == 0) {
        return;
    }

    std.sort.heap([]const u8, missing.items, {}, lessThanName);
    const stderr = std.Io.File.stderr();
    var err_buffer: [2048]u8 = undefined;
    var err_writer = stderr.writer(init.io, &err_buffer);
    const err = &err_writer.interface;
    try err.print(
        "ExchangeApi is missing {d} public ExchangeCore methods:\n",
        .{missing.items.len},
    );
    for (missing.items) |name| {
        try err.print("  - {s}\n", .{name});
    }
    try err.flush();
    return SurfaceCheckError.ApiSurfaceMismatch;
}

fn collectPublicFunctionNames(
    allocator: std.mem.Allocator,
    source: []const u8,
    container_name: []const u8,
    out: *std.StringHashMapUnmanaged(void),
) (std.mem.Allocator.Error || SurfaceCheckError)!void {
    const source_z = try allocator.dupeZ(u8, source);
    defer allocator.free(source_z);

    const bounds = try findContainerBounds(source_z, container_name);
    var tokenizer = std.zig.Tokenizer.init(source_z);
    var brace_depth: usize = 0;
    var function_state: FunctionDeclarationState = .idle;

    while (true) {
        const token = tokenizer.next();
        switch (token.tag) {
            .invalid, .invalid_periodasterisks => return SurfaceCheckError.MalformedSource,
            .eof => break,
            else => {},
        }

        if (token.loc.start < bounds.open_brace_start) continue;
        if (token.loc.start == bounds.open_brace_start) {
            brace_depth = 1;
            continue;
        }
        if (token.loc.start >= bounds.close_brace_start) break;

        if (brace_depth == 1 and !isComment(token.tag)) {
            switch (function_state) {
                .idle => {
                    if (token.tag == .keyword_pub) function_state = .pub_keyword;
                },
                .pub_keyword, .modifier => switch (token.tag) {
                    .keyword_fn => function_state = .fn_keyword,
                    .keyword_extern,
                    .keyword_export,
                    .keyword_inline,
                    .keyword_noinline,
                    .string_literal,
                    => function_state = .modifier,
                    .keyword_pub => function_state = .pub_keyword,
                    else => function_state = .idle,
                },
                .fn_keyword => {
                    if (token.tag != .identifier) {
                        return SurfaceCheckError.MissingFunctionName;
                    }
                    try out.put(allocator, source[token.loc.start..token.loc.end], {});
                    function_state = .idle;
                },
            }
        } else if (brace_depth != 1) {
            function_state = .idle;
        }

        switch (token.tag) {
            .l_brace => brace_depth += 1,
            .r_brace => {
                if (brace_depth <= 1) return SurfaceCheckError.MalformedContainer;
                brace_depth -= 1;
            },
            else => {},
        }
    }
}

fn findContainerBounds(
    source: [:0]const u8,
    container_name: []const u8,
) SurfaceCheckError!ContainerBounds {
    var tokenizer = std.zig.Tokenizer.init(source);
    var brace_depth: usize = 0;
    var state: ContainerDeclarationState = .idle;
    var target_declaration_seen = false;
    var container_open: ?usize = null;
    var container_depth: usize = 0;
    var container_close: ?usize = null;

    while (true) {
        const token = tokenizer.next();
        switch (token.tag) {
            .invalid, .invalid_periodasterisks => return SurfaceCheckError.MalformedSource,
            .eof => {
                if (container_open != null or target_declaration_seen) {
                    return SurfaceCheckError.MalformedContainer;
                }
                return SurfaceCheckError.MissingContainer;
            },
            else => {},
        }

        if (container_close != null) {
            if (isComment(token.tag)) continue;
            if (token.tag != .semicolon) return SurfaceCheckError.MalformedContainer;
            return .{
                .open_brace_start = container_open.?,
                .close_brace_start = container_close.?,
            };
        }

        if (container_open) |open| {
            _ = open;
            switch (token.tag) {
                .l_brace => container_depth += 1,
                .r_brace => {
                    if (container_depth == 0) return SurfaceCheckError.MalformedContainer;
                    container_depth -= 1;
                    if (container_depth == 0) container_close = token.loc.start;
                },
                else => {},
            }
            continue;
        }

        if (isComment(token.tag)) continue;

        if (brace_depth == 0) {
            switch (state) {
                .idle => {
                    if (token.tag == .keyword_pub) state = .pub_keyword;
                },
                .pub_keyword => {
                    state = if (token.tag == .keyword_const) .const_keyword else .idle;
                },
                .const_keyword => {
                    if (token.tag == .identifier and
                        std.mem.eql(u8, source[token.loc.start..token.loc.end], container_name))
                    {
                        target_declaration_seen = true;
                        state = .target_name;
                    } else {
                        state = .idle;
                    }
                },
                .target_name => {
                    if (token.tag != .equal) return SurfaceCheckError.MalformedContainer;
                    state = .equal;
                },
                .equal => {
                    if (token.tag != .keyword_struct) return SurfaceCheckError.MalformedContainer;
                    state = .struct_keyword;
                },
                .struct_keyword => {
                    if (token.tag != .l_brace) return SurfaceCheckError.MalformedContainer;
                    container_open = token.loc.start;
                    container_depth = 1;
                    state = .idle;
                    continue;
                },
            }
        }

        switch (token.tag) {
            .l_brace => brace_depth += 1,
            .r_brace => {
                if (brace_depth == 0) return SurfaceCheckError.MalformedSource;
                brace_depth -= 1;
            },
            else => {},
        }
    }
}

fn isComment(tag: std.zig.Token.Tag) bool {
    return switch (tag) {
        .doc_comment, .container_doc_comment => true,
        else => false,
    };
}

fn collectMissingFunctionNames(
    allocator: std.mem.Allocator,
    expected: *const std.StringHashMapUnmanaged(void),
    actual: *const std.StringHashMapUnmanaged(void),
    missing: *std.ArrayListUnmanaged([]const u8),
) std.mem.Allocator.Error!void {
    var it = expected.keyIterator();
    while (it.next()) |name_ptr| {
        if (!actual.contains(name_ptr.*)) {
            try missing.append(allocator, name_ptr.*);
        }
    }
}

fn lessThanName(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs) == .lt;
}

test "collector includes only direct public container methods" {
    const source =
        \\pub const ExchangeCore = struct {
        \\    pub fn direct(self: *@This()) void {
        \\        _ = self;
        \\    }
        \\
        \\    const Nested = struct {
        \\        pub fn nested() void {}
        \\    };
        \\
        \\    pub inline fn directInline() void {}
        \\};
        \\
        \\pub fn afterContainer() void {}
    ;

    var names: std.StringHashMapUnmanaged(void) = .empty;
    defer names.deinit(std.testing.allocator);
    try collectPublicFunctionNames(
        std.testing.allocator,
        source,
        "ExchangeCore",
        &names,
    );

    try std.testing.expect(names.contains("direct"));
    try std.testing.expect(names.contains("directInline"));
    try std.testing.expect(!names.contains("nested"));
    try std.testing.expect(!names.contains("afterContainer"));
    try std.testing.expectEqual(@as(u32, 2), names.count());
}

test "surface comparison reports a true missing direct method only" {
    const core_source =
        \\pub const ExchangeCore = struct {
        \\    pub fn shared() void {}
        \\    pub fn missingDirect() void {}
        \\    const Nested = struct {
        \\        pub fn nestedOnly() void {}
        \\    };
        \\};
    ;
    const api_source =
        \\pub const ExchangeApi = struct {
        \\    pub fn shared() void {}
        \\};
        \\pub fn missingDirect() void {}
    ;

    var core_names: std.StringHashMapUnmanaged(void) = .empty;
    defer core_names.deinit(std.testing.allocator);
    var api_names: std.StringHashMapUnmanaged(void) = .empty;
    defer api_names.deinit(std.testing.allocator);
    try collectPublicFunctionNames(
        std.testing.allocator,
        core_source,
        "ExchangeCore",
        &core_names,
    );
    try collectPublicFunctionNames(
        std.testing.allocator,
        api_source,
        "ExchangeApi",
        &api_names,
    );

    var missing: std.ArrayListUnmanaged([]const u8) = .empty;
    defer missing.deinit(std.testing.allocator);
    try collectMissingFunctionNames(
        std.testing.allocator,
        &core_names,
        &api_names,
        &missing,
    );

    try std.testing.expectEqual(@as(usize, 1), missing.items.len);
    try std.testing.expectEqualStrings("missingDirect", missing.items[0]);
}

test "collector rejects missing and malformed target containers explicitly" {
    var names: std.StringHashMapUnmanaged(void) = .empty;
    defer names.deinit(std.testing.allocator);

    try std.testing.expectError(
        error.MissingContainer,
        collectPublicFunctionNames(
            std.testing.allocator,
            "pub const Other = struct {};",
            "ExchangeCore",
            &names,
        ),
    );
    try std.testing.expectError(
        error.MalformedContainer,
        collectPublicFunctionNames(
            std.testing.allocator,
            "pub const ExchangeCore = struct { pub fn direct() void {}",
            "ExchangeCore",
            &names,
        ),
    );
}
