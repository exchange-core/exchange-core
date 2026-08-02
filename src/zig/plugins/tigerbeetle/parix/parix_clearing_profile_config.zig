//! Parix host integration for the opt-in TigerBeetle plugin.
const std = @import("std");

pub const format_version: u16 = 1;
pub const max_profile_bytes: usize = 64 * 1024;
pub const max_database_uuid_bytes: usize = 36;
pub const route_count: usize = 2;

pub const TimeoutBounds = struct {
    minimum_seconds: u32,
    maximum_seconds: u32,
};

pub const Route = struct {
    currency: i32,
    ledger: u32,
    treasury_account_id: u128,
    provider_control_account_id: u128,
};

/// Fully bounded configuration copied out of the temporary JSON parse tree.
pub const Document = struct {
    version: u16,
    database_uuid: [max_database_uuid_bytes]u8,
    provider_id: u64,
    transfer_code: u16,
    timeout_bounds: TimeoutBounds,
    routes: [route_count]Route,
};

pub const ParseError = std.mem.Allocator.Error || error{
    EmptyProfile,
    ProfileTooLarge,
    JsonNestingTooDeep,
    InvalidJson,
    InvalidRoot,
    InvalidObjectShape,
    MissingField,
    InvalidFieldType,
    InvalidInteger,
    InvalidString,
    UnsupportedVersion,
    InvalidRouteCount,
};

pub fn parse(
    allocator: std.mem.Allocator,
    bytes: []const u8,
) ParseError!Document {
    if (bytes.len == 0) return error.EmptyProfile;
    if (bytes.len > max_profile_bytes) return error.ProfileTooLarge;
    if (!jsonNestingWithinLimit(bytes, 8)) return error.JsonNestingTooDeep;

    var parsed = std.json.parseFromSlice(std.json.Value, allocator, bytes, .{
        .duplicate_field_behavior = .@"error",
        .parse_numbers = false,
        .max_value_len = 256,
    }) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return error.InvalidJson;
    };
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidRoot,
    };
    try exactShape(root, 6);

    const version = try requiredInt(u16, root, "version");
    if (version != format_version) return error.UnsupportedVersion;

    const database_uuid_text = try requiredString(root, "database_uuid");
    if (database_uuid_text.len != max_database_uuid_bytes) return error.InvalidString;
    var database_uuid: [max_database_uuid_bytes]u8 = undefined;
    @memcpy(&database_uuid, database_uuid_text);

    const timeout_object = try requiredObject(root, "timeout_bounds");
    try exactShape(timeout_object, 2);
    const timeout_bounds: TimeoutBounds = .{
        .minimum_seconds = try requiredInt(u32, timeout_object, "minimum_seconds"),
        .maximum_seconds = try requiredInt(u32, timeout_object, "maximum_seconds"),
    };

    const routes_array = try requiredArray(root, "routes");
    if (routes_array.items.len != route_count) return error.InvalidRouteCount;
    var routes: [route_count]Route = undefined;
    for (routes_array.items, 0..) |value, index| {
        const object = try valueObject(value);
        try exactShape(object, 4);
        routes[index] = .{
            .currency = try requiredInt(i32, object, "currency"),
            .ledger = try requiredInt(u32, object, "ledger"),
            .treasury_account_id = try requiredDecimalString(
                u128,
                object,
                "treasury_account_id",
            ),
            .provider_control_account_id = try requiredDecimalString(
                u128,
                object,
                "provider_control_account_id",
            ),
        };
    }

    return .{
        .version = version,
        .database_uuid = database_uuid,
        .provider_id = try requiredDecimalString(u64, root, "provider_id"),
        .transfer_code = try requiredInt(u16, root, "transfer_code"),
        .timeout_bounds = timeout_bounds,
        .routes = routes,
    };
}

fn exactShape(object: std.json.ObjectMap, expected_fields: usize) ParseError!void {
    if (object.count() != expected_fields) return error.InvalidObjectShape;
}

fn valueObject(value: std.json.Value) ParseError!std.json.ObjectMap {
    return switch (value) {
        .object => |object| object,
        else => error.InvalidFieldType,
    };
}

fn requiredObject(
    object: std.json.ObjectMap,
    key: []const u8,
) ParseError!std.json.ObjectMap {
    return valueObject(object.get(key) orelse return error.MissingField);
}

fn requiredArray(
    object: std.json.ObjectMap,
    key: []const u8,
) ParseError!std.json.Array {
    const value = object.get(key) orelse return error.MissingField;
    return switch (value) {
        .array => |array| array,
        else => error.InvalidFieldType,
    };
}

fn requiredString(
    object: std.json.ObjectMap,
    key: []const u8,
) ParseError![]const u8 {
    const value = object.get(key) orelse return error.MissingField;
    return switch (value) {
        .string => |text| text,
        else => error.InvalidFieldType,
    };
}

fn requiredInt(
    comptime T: type,
    object: std.json.ObjectMap,
    key: []const u8,
) ParseError!T {
    const value = object.get(key) orelse return error.MissingField;
    const text = switch (value) {
        .number_string => |number| number,
        else => return error.InvalidFieldType,
    };
    return parseDecimal(T, text);
}

fn requiredDecimalString(
    comptime T: type,
    object: std.json.ObjectMap,
    key: []const u8,
) ParseError!T {
    const value = object.get(key) orelse return error.MissingField;
    const text = switch (value) {
        .string => |number| number,
        else => return error.InvalidFieldType,
    };
    return parseDecimal(T, text);
}

fn parseDecimal(comptime T: type, text: []const u8) ParseError!T {
    if (text.len == 0) return error.InvalidInteger;
    var index: usize = 0;
    if (@typeInfo(T).int.signedness == .signed and text[0] == '-') {
        if (text.len == 1) return error.InvalidInteger;
        index = 1;
    }
    for (text[index..]) |byte| {
        if (byte < '0' or byte > '9') return error.InvalidInteger;
    }
    return std.fmt.parseInt(T, text, 10) catch error.InvalidInteger;
}

fn jsonNestingWithinLimit(bytes: []const u8, maximum: usize) bool {
    var depth: usize = 0;
    var in_string = false;
    var escaped = false;
    for (bytes) |byte| {
        if (in_string) {
            if (escaped) {
                escaped = false;
            } else if (byte == '\\') {
                escaped = true;
            } else if (byte == '"') {
                in_string = false;
            }
            continue;
        }
        switch (byte) {
            '"' => in_string = true,
            '{', '[' => {
                depth += 1;
                if (depth > maximum) return false;
            },
            '}', ']' => {
                if (depth == 0) return false;
                depth -= 1;
            },
            else => {},
        }
    }
    return !in_string and depth == 0;
}

const example_json =
    \\{"version":1,"database_uuid":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","provider_id":"7001","transfer_code":20,"timeout_bounds":{"minimum_seconds":30,"maximum_seconds":3600},"routes":[{"currency":11,"ledger":421,"treasury_account_id":"420011","provider_control_account_id":"430011"},{"currency":15,"ledger":425,"treasury_account_id":"420015","provider_control_account_id":"430015"}]}
;

test "parser accepts the bounded v1 schema" {
    const document = try parse(std.testing.allocator, example_json);
    try std.testing.expectEqual(@as(u64, 7_001), document.provider_id);
    try std.testing.expectEqual(@as(u128, 430_015), document.routes[1].provider_control_account_id);
}

test "parser rejects duplicate unknown and non-string decimal fields" {
    const duplicate =
        \\{"version":1,"version":1,"database_uuid":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","provider_id":"7001","transfer_code":20,"timeout_bounds":{"minimum_seconds":30,"maximum_seconds":3600},"routes":[]}
    ;
    try std.testing.expectError(error.InvalidJson, parse(std.testing.allocator, duplicate));

    const unknown =
        \\{"version":1,"database_uuid":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","provider_id":"7001","transfer_code":20,"timeout_bounds":{"minimum_seconds":30,"maximum_seconds":3600},"routes":[],"endpoint":"https://example.invalid"}
    ;
    try std.testing.expectError(error.InvalidObjectShape, parse(std.testing.allocator, unknown));

    const numeric_provider =
        \\{"version":1,"database_uuid":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","provider_id":7001,"transfer_code":20,"timeout_bounds":{"minimum_seconds":30,"maximum_seconds":3600},"routes":[{"currency":11,"ledger":421,"treasury_account_id":"420011","provider_control_account_id":"430011"},{"currency":15,"ledger":425,"treasury_account_id":"420015","provider_control_account_id":"430015"}]}
    ;
    try std.testing.expectError(
        error.InvalidFieldType,
        parse(std.testing.allocator, numeric_provider),
    );
}

test "parser enforces size nesting and route count bounds" {
    var oversized: [max_profile_bytes + 1]u8 = undefined;
    @memset(&oversized, ' ');
    try std.testing.expectError(error.ProfileTooLarge, parse(std.testing.allocator, &oversized));

    const nested = "[[[[[[[[[0]]]]]]]]]";
    try std.testing.expectError(error.JsonNestingTooDeep, parse(std.testing.allocator, nested));

    const no_routes =
        \\{"version":1,"database_uuid":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","provider_id":"7001","transfer_code":20,"timeout_bounds":{"minimum_seconds":30,"maximum_seconds":3600},"routes":[]}
    ;
    try std.testing.expectError(error.InvalidRouteCount, parse(std.testing.allocator, no_routes));
}

test "parser rejects version shape and decimal encoding drift" {
    try std.testing.expectError(error.EmptyProfile, parse(std.testing.allocator, ""));
    try std.testing.expectError(error.InvalidRoot, parse(std.testing.allocator, "[]"));

    const cases = [_]struct {
        expected: anyerror,
        needle: []const u8,
        replacement: []const u8,
    }{
        .{
            .expected = error.UnsupportedVersion,
            .needle = "\"version\":1",
            .replacement = "\"version\":2",
        },
        .{
            .expected = error.InvalidInteger,
            .needle = "\"provider_id\":\"7001\"",
            .replacement = "\"provider_id\":\"+7001\"",
        },
        .{
            .expected = error.InvalidFieldType,
            .needle = "\"treasury_account_id\":\"420011\"",
            .replacement = "\"treasury_account_id\":420011",
        },
        .{
            .expected = error.InvalidObjectShape,
            .needle = "\"maximum_seconds\":3600",
            .replacement = "\"maximum_seconds\":3600,\"retry_seconds\":5",
        },
        .{
            .expected = error.InvalidJson,
            .needle = "\"minimum_seconds\":30",
            .replacement = "\"minimum_seconds\":30,\"minimum_seconds\":30",
        },
    };
    for (cases) |case| {
        const changed = try replaceOnceAlloc(
            std.testing.allocator,
            example_json,
            case.needle,
            case.replacement,
        );
        defer std.testing.allocator.free(changed);
        try std.testing.expectError(case.expected, parse(std.testing.allocator, changed));
    }
}

fn replaceOnceAlloc(
    allocator: std.mem.Allocator,
    input: []const u8,
    needle: []const u8,
    replacement: []const u8,
) std.mem.Allocator.Error![]u8 {
    const index = std.mem.indexOf(u8, input, needle) orelse unreachable;
    const output = try allocator.alloc(u8, input.len - needle.len + replacement.len);
    @memcpy(output[0..index], input[0..index]);
    @memcpy(output[index .. index + replacement.len], replacement);
    @memcpy(output[index + replacement.len ..], input[index + needle.len ..]);
    return output;
}
