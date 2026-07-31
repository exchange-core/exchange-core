const std = @import("std");

pub const max_line_bytes: usize = 1024;
pub const max_tokens: usize = 8;
pub const default_book_depth: usize = 20;
pub const max_book_depth: usize = 1000;

pub const Side = enum {
    bid,
    ask,
};

pub const TimeInForce = enum {
    gtc,
    ioc,
};

pub const Request = union(enum) {
    ping,
    add_user: struct {
        uid: i64,
    },
    deposit: struct {
        uid: i64,
        currency: i32,
        amount: i64,
        transaction_id: i64,
    },
    place: struct {
        order_id: i64,
        uid: i64,
        symbol: i32,
        price: i64,
        size: i64,
        side: Side,
        time_in_force: TimeInForce,
    },
    move: struct {
        order_id: i64,
        uid: i64,
        symbol: i32,
        new_price: i64,
    },
    reduce: struct {
        order_id: i64,
        uid: i64,
        symbol: i32,
        size: i64,
    },
    cancel: struct {
        order_id: i64,
        uid: i64,
        symbol: i32,
    },
    balance: struct {
        uid: i64,
        currency: i32,
    },
    book: struct {
        symbol: i32,
        depth: usize,
    },
    shutdown,
};

pub const ParseError = error{
    EmptyRequest,
    LineTooLong,
    MultipleLines,
    TooManyTokens,
    InvalidToken,
    UnknownCommand,
    InvalidArgumentCount,
    InvalidNumber,
    NonPositiveIdentifier,
    NonPositiveAmount,
    NonPositivePrice,
    NonPositiveSize,
    InvalidSide,
    InvalidTimeInForce,
    InvalidDepth,
};

pub const usage =
    \\Commands:
    \\  ping
    \\  add-user UID
    \\  deposit UID CURRENCY AMOUNT TRANSACTION_ID
    \\  place ORDER_ID UID SYMBOL PRICE SIZE bid|ask [gtc|ioc]
    \\  move ORDER_ID UID SYMBOL NEW_PRICE
    \\  reduce ORDER_ID UID SYMBOL SIZE
    \\  cancel ORDER_ID UID SYMBOL
    \\  balance UID CURRENCY
    \\  book SYMBOL [DEPTH]
    \\  shutdown
    \\
;

pub const usage_text = usage;

pub fn parseLine(line: []const u8) ParseError!Request {
    if (line.len > max_line_bytes) return error.LineTooLong;

    var body = std.mem.trim(u8, line, " \t");
    if (body.len != 0 and body[body.len - 1] == '\r') {
        body = std.mem.trimEnd(u8, body[0 .. body.len - 1], " \t");
    }
    if (body.len == 0) return error.EmptyRequest;
    if (std.mem.findScalar(u8, body, '\n') != null or
        std.mem.findScalar(u8, body, '\r') != null)
    {
        return error.MultipleLines;
    }

    var storage: [max_tokens][]const u8 = undefined;
    var count: usize = 0;
    var iterator = std.mem.tokenizeAny(u8, body, " \t");
    while (iterator.next()) |token| {
        if (count == storage.len) return error.TooManyTokens;
        storage[count] = token;
        count += 1;
    }
    return parseArgs(storage[0..count]);
}

pub fn parseArgs(args: []const []const u8) ParseError!Request {
    if (args.len == 0) return error.EmptyRequest;
    if (args.len > max_tokens) return error.TooManyTokens;
    for (args) |token| try validateToken(token);

    const command = args[0];
    if (std.mem.eql(u8, command, "ping")) {
        try expectArity(args, 1);
        return .ping;
    }
    if (std.mem.eql(u8, command, "add-user")) {
        try expectArity(args, 2);
        return .{ .add_user = .{
            .uid = try parsePositiveIdentifier(i64, args[1]),
        } };
    }
    if (std.mem.eql(u8, command, "deposit")) {
        try expectArity(args, 5);
        return .{ .deposit = .{
            .uid = try parsePositiveIdentifier(i64, args[1]),
            .currency = try parsePositiveIdentifier(i32, args[2]),
            .amount = try parsePositiveAmount(args[3]),
            .transaction_id = try parsePositiveIdentifier(i64, args[4]),
        } };
    }
    if (std.mem.eql(u8, command, "place")) {
        if (args.len != 7 and args.len != 8) return error.InvalidArgumentCount;
        return .{ .place = .{
            .order_id = try parsePositiveIdentifier(i64, args[1]),
            .uid = try parsePositiveIdentifier(i64, args[2]),
            .symbol = try parsePositiveIdentifier(i32, args[3]),
            .price = try parsePositivePrice(args[4]),
            .size = try parsePositiveSize(args[5]),
            .side = try parseSide(args[6]),
            .time_in_force = if (args.len == 8)
                try parseTimeInForce(args[7])
            else
                .gtc,
        } };
    }
    if (std.mem.eql(u8, command, "move")) {
        try expectArity(args, 5);
        return .{ .move = .{
            .order_id = try parsePositiveIdentifier(i64, args[1]),
            .uid = try parsePositiveIdentifier(i64, args[2]),
            .symbol = try parsePositiveIdentifier(i32, args[3]),
            .new_price = try parsePositivePrice(args[4]),
        } };
    }
    if (std.mem.eql(u8, command, "reduce")) {
        try expectArity(args, 5);
        return .{ .reduce = .{
            .order_id = try parsePositiveIdentifier(i64, args[1]),
            .uid = try parsePositiveIdentifier(i64, args[2]),
            .symbol = try parsePositiveIdentifier(i32, args[3]),
            .size = try parsePositiveSize(args[4]),
        } };
    }
    if (std.mem.eql(u8, command, "cancel")) {
        try expectArity(args, 4);
        return .{ .cancel = .{
            .order_id = try parsePositiveIdentifier(i64, args[1]),
            .uid = try parsePositiveIdentifier(i64, args[2]),
            .symbol = try parsePositiveIdentifier(i32, args[3]),
        } };
    }
    if (std.mem.eql(u8, command, "balance")) {
        try expectArity(args, 3);
        return .{ .balance = .{
            .uid = try parsePositiveIdentifier(i64, args[1]),
            .currency = try parsePositiveIdentifier(i32, args[2]),
        } };
    }
    if (std.mem.eql(u8, command, "book")) {
        if (args.len != 2 and args.len != 3) return error.InvalidArgumentCount;
        return .{ .book = .{
            .symbol = try parsePositiveIdentifier(i32, args[1]),
            .depth = if (args.len == 3) try parseDepth(args[2]) else default_book_depth,
        } };
    }
    if (std.mem.eql(u8, command, "shutdown")) {
        try expectArity(args, 1);
        return .shutdown;
    }
    return error.UnknownCommand;
}

pub fn writeRequest(writer: *std.Io.Writer, request: Request) std.Io.Writer.Error!void {
    switch (request) {
        .ping => try writer.writeAll("ping\n"),
        .add_user => |value| try writer.print("add-user {d}\n", .{value.uid}),
        .deposit => |value| try writer.print(
            "deposit {d} {d} {d} {d}\n",
            .{ value.uid, value.currency, value.amount, value.transaction_id },
        ),
        .place => |value| try writer.print(
            "place {d} {d} {d} {d} {d} {s} {s}\n",
            .{
                value.order_id,
                value.uid,
                value.symbol,
                value.price,
                value.size,
                @tagName(value.side),
                @tagName(value.time_in_force),
            },
        ),
        .move => |value| try writer.print(
            "move {d} {d} {d} {d}\n",
            .{ value.order_id, value.uid, value.symbol, value.new_price },
        ),
        .reduce => |value| try writer.print(
            "reduce {d} {d} {d} {d}\n",
            .{ value.order_id, value.uid, value.symbol, value.size },
        ),
        .cancel => |value| try writer.print(
            "cancel {d} {d} {d}\n",
            .{ value.order_id, value.uid, value.symbol },
        ),
        .balance => |value| try writer.print(
            "balance {d} {d}\n",
            .{ value.uid, value.currency },
        ),
        .book => |value| try writer.print(
            "book {d} {d}\n",
            .{ value.symbol, value.depth },
        ),
        .shutdown => try writer.writeAll("shutdown\n"),
    }
}

fn validateToken(token: []const u8) ParseError!void {
    if (token.len == 0) return error.InvalidToken;
    for (token) |byte| {
        if (std.ascii.isWhitespace(byte)) return error.InvalidToken;
    }
}

fn expectArity(args: []const []const u8, expected: usize) ParseError!void {
    if (args.len != expected) return error.InvalidArgumentCount;
}

fn parsePositiveIdentifier(comptime T: type, token: []const u8) ParseError!T {
    const value = std.fmt.parseInt(T, token, 10) catch return error.InvalidNumber;
    if (value <= 0) return error.NonPositiveIdentifier;
    return value;
}

fn parsePositiveAmount(token: []const u8) ParseError!i64 {
    const value = std.fmt.parseInt(i64, token, 10) catch return error.InvalidNumber;
    if (value <= 0) return error.NonPositiveAmount;
    return value;
}

fn parsePositivePrice(token: []const u8) ParseError!i64 {
    const value = std.fmt.parseInt(i64, token, 10) catch return error.InvalidNumber;
    if (value <= 0) return error.NonPositivePrice;
    return value;
}

fn parsePositiveSize(token: []const u8) ParseError!i64 {
    const value = std.fmt.parseInt(i64, token, 10) catch return error.InvalidNumber;
    if (value <= 0) return error.NonPositiveSize;
    return value;
}

fn parseSide(token: []const u8) ParseError!Side {
    if (std.mem.eql(u8, token, "bid")) return .bid;
    if (std.mem.eql(u8, token, "ask")) return .ask;
    return error.InvalidSide;
}

fn parseTimeInForce(token: []const u8) ParseError!TimeInForce {
    if (std.mem.eql(u8, token, "gtc")) return .gtc;
    if (std.mem.eql(u8, token, "ioc")) return .ioc;
    return error.InvalidTimeInForce;
}

fn parseDepth(token: []const u8) ParseError!usize {
    const value = std.fmt.parseInt(usize, token, 10) catch return error.InvalidNumber;
    if (value == 0 or value > max_book_depth) return error.InvalidDepth;
    return value;
}

test "parseLine recognizes every request shape" {
    try std.testing.expectEqualDeep(Request.ping, try parseLine(" ping \t"));
    try std.testing.expectEqualDeep(
        Request{ .add_user = .{ .uid = 7 } },
        try parseLine("add-user 7\r"),
    );
    try std.testing.expectEqualDeep(
        Request{ .deposit = .{ .uid = 7, .currency = 15, .amount = 9000, .transaction_id = 1 } },
        try parseLine("deposit 7 15 9000 1"),
    );
    try std.testing.expectEqualDeep(
        Request{ .place = .{
            .order_id = 100,
            .uid = 7,
            .symbol = 241,
            .price = 105,
            .size = 3,
            .side = .bid,
            .time_in_force = .ioc,
        } },
        try parseLine("place 100 7 241 105 3 bid ioc"),
    );
    try std.testing.expectEqualDeep(
        Request{ .move = .{ .order_id = 100, .uid = 7, .symbol = 241, .new_price = 106 } },
        try parseLine("move 100 7 241 106"),
    );
    try std.testing.expectEqualDeep(
        Request{ .reduce = .{ .order_id = 100, .uid = 7, .symbol = 241, .size = 2 } },
        try parseLine("reduce 100 7 241 2"),
    );
    try std.testing.expectEqualDeep(
        Request{ .cancel = .{ .order_id = 100, .uid = 7, .symbol = 241 } },
        try parseLine("cancel 100 7 241"),
    );
    try std.testing.expectEqualDeep(
        Request{ .balance = .{ .uid = 7, .currency = 15 } },
        try parseLine("balance 7 15"),
    );
    try std.testing.expectEqualDeep(
        Request{ .book = .{ .symbol = 241, .depth = 5 } },
        try parseLine("book 241 5"),
    );
    try std.testing.expectEqualDeep(Request.shutdown, try parseLine("shutdown"));
}

test "optional request arguments have deterministic defaults" {
    const place = try parseLine("place 100 7 241 105 3 ask");
    try std.testing.expectEqual(TimeInForce.gtc, place.place.time_in_force);

    const book = try parseLine("book 241");
    try std.testing.expectEqual(default_book_depth, book.book.depth);
}

test "writeRequest emits a canonical parseable line" {
    const requests = [_]Request{
        .ping,
        .{ .add_user = .{ .uid = 7 } },
        .{ .deposit = .{ .uid = 7, .currency = 15, .amount = 9000, .transaction_id = 1 } },
        .{ .place = .{
            .order_id = 100,
            .uid = 7,
            .symbol = 241,
            .price = 105,
            .size = 3,
            .side = .ask,
            .time_in_force = .ioc,
        } },
        .{ .move = .{ .order_id = 100, .uid = 7, .symbol = 241, .new_price = 106 } },
        .{ .reduce = .{ .order_id = 100, .uid = 7, .symbol = 241, .size = 2 } },
        .{ .cancel = .{ .order_id = 100, .uid = 7, .symbol = 241 } },
        .{ .balance = .{ .uid = 7, .currency = 15 } },
        .{ .book = .{ .symbol = 241, .depth = 5 } },
        .shutdown,
    };

    for (requests) |request| {
        var storage: [max_line_bytes + 1]u8 = undefined;
        var writer = std.Io.Writer.fixed(&storage);
        try writeRequest(&writer, request);
        const encoded = writer.buffered();
        try std.testing.expect(encoded.len > 0);
        try std.testing.expectEqual(@as(u8, '\n'), encoded[encoded.len - 1]);
        try std.testing.expectEqualDeep(request, try parseLine(encoded[0 .. encoded.len - 1]));
    }
}

test "parser rejects malformed and unbounded input" {
    try std.testing.expectError(error.EmptyRequest, parseLine(" \t"));
    try std.testing.expectError(error.MultipleLines, parseLine("ping\nshutdown"));
    try std.testing.expectError(error.UnknownCommand, parseLine("nope"));
    try std.testing.expectError(error.InvalidArgumentCount, parseLine("add-user"));
    try std.testing.expectError(error.InvalidNumber, parseLine("add-user seven"));
    try std.testing.expectError(error.NonPositiveIdentifier, parseLine("add-user 0"));
    try std.testing.expectError(error.NonPositiveAmount, parseLine("deposit 7 15 -1 1"));
    try std.testing.expectError(error.NonPositivePrice, parseLine("place 1 7 241 0 2 bid"));
    try std.testing.expectError(error.NonPositiveSize, parseLine("reduce 1 7 241 0"));
    try std.testing.expectError(error.InvalidSide, parseLine("place 1 7 241 100 2 buy"));
    try std.testing.expectError(error.InvalidTimeInForce, parseLine("place 1 7 241 100 2 bid fok"));
    try std.testing.expectError(error.InvalidDepth, parseLine("book 241 0"));
    try std.testing.expectError(error.InvalidDepth, parseLine("book 241 1001"));
    try std.testing.expectError(
        error.TooManyTokens,
        parseLine("place 1 2 3 4 5 bid gtc extra"),
    );
    try std.testing.expectError(
        error.InvalidToken,
        parseArgs(&.{ "add-user", "7 8" }),
    );

    var oversized: [max_line_bytes + 1]u8 = undefined;
    @memset(&oversized, 'x');
    try std.testing.expectError(error.LineTooLong, parseLine(&oversized));
}
