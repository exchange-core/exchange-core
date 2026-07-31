const std = @import("std");
const builtin = @import("builtin");
const kernel_mod = @import("exchange_kernel");

const ExchangeKernel = kernel_mod.ExchangeKernel;
const ExchangeApi = kernel_mod.ExchangeApi;
const CommandResponse = kernel_mod.CommandResponse;
const ReportResponse = kernel_mod.ReportResponse;
const CommandResultCode = @FieldType(CommandResponse, "result_code");
const SymbolSpecification = @typeInfo(@TypeOf(ExchangeKernel.addSymbol)).@"fn".params[1].type.?;
const OrderAction = @typeInfo(@TypeOf(ExchangeApi.placeOrder)).@"fn".params[7].type.?;
const OrderType = @typeInfo(@TypeOf(ExchangeApi.placeOrder)).@"fn".params[8].type.?;
const BalanceAdjustmentType = @typeInfo(@TypeOf(ExchangeApi.adjustUserBalance)).@"fn".params[5].type.?;
const L2MarketData = @FieldType(ReportResponse, "order_book");

pub const abi_version: u32 = 1;
const max_instances: usize = 8;
const wire_header_size: usize = 16;
const wire_level_size: usize = 24;

const result_success: i32 = @intFromEnum(CommandResultCode.success);
const result_symbol_already_exists: i32 = @intFromEnum(CommandResultCode.symbol_mgmt_symbol_already_exists);
const result_user_already_exists: i32 = @intFromEnum(CommandResultCode.user_mgmt_user_already_exists);

pub const AbiStatus = enum(u32) {
    ok = 0,
    invalid_handle = 1,
    out_of_memory = 2,
    invalid_argument = 3,
    invalid_enum = 4,
    instance_limit = 5,
    handle_exhausted = 6,
    faulted = 7,
    internal = 255,
};

const OrderBookKind = enum(i32) {
    naive = 0,
    direct = 1,
};

const TableError = error{
    InvalidHandle,
    InstanceLimit,
    HandleExhausted,
    Faulted,
};

const EncodeError = std.mem.Allocator.Error || error{
    OrderBookTooLarge,
};

const Instance = struct {
    kernel: ExchangeKernel,
    api: ExchangeApi,
    order_book_wire: std.ArrayListUnmanaged(u8) = .empty,
    last_event_count: u32 = 0,
    faulted: bool = false,

    fn init(
        allocator: std.mem.Allocator,
        orderbook_kind: kernel_mod.OrderBookKind,
    ) kernel_mod.InitError!Instance {
        var kernel = try ExchangeKernel.init(allocator, .{
            .orderbook_kind = orderbook_kind,
        });
        errdefer kernel.deinit();

        return .{
            .kernel = kernel,
            // Rebound after the value reaches its stable heap allocation.
            .api = undefined,
        };
    }

    fn bindApi(self: *Instance) void {
        self.api = ExchangeApi.init(&self.kernel);
    }

    fn deinit(self: *Instance) void {
        const allocator = self.kernel.allocator;
        self.order_book_wire.deinit(allocator);
        self.api.deinit();
        self.kernel.deinit();
        self.* = undefined;
    }

    fn markFaulted(self: *Instance) void {
        self.faulted = true;
        self.last_event_count = 0;
        self.order_book_wire.clearRetainingCapacity();
    }

    fn finishCommand(self: *Instance, response: *CommandResponse) u64 {
        defer response.deinit(self.kernel.allocator);

        var count: u32 = 0;
        var node = response.matcher_events_head;
        while (node) |event| : (node = event.next_event) {
            count = std.math.add(u32, count, 1) catch {
                self.markFaulted();
                return pack(.internal, 0);
            };
        }
        self.last_event_count = count;
        return packResult(response.result_code.code());
    }

    fn executionFailed(self: *Instance, err: kernel_mod.ExecuteError) u64 {
        self.markFaulted();
        return switch (err) {
            error.OutOfMemory => pack(.out_of_memory, 0),
            else => pack(.internal, 0),
        };
    }

    fn encodeOrderBook(self: *Instance, snapshot: *const L2MarketData) EncodeError!void {
        const ask_count = std.math.cast(u32, snapshot.asks.items.len) orelse
            return error.OrderBookTooLarge;
        const bid_count = std.math.cast(u32, snapshot.bids.items.len) orelse
            return error.OrderBookTooLarge;
        const level_count = std.math.add(
            usize,
            snapshot.asks.items.len,
            snapshot.bids.items.len,
        ) catch return error.OrderBookTooLarge;
        const levels_size = std.math.mul(usize, level_count, wire_level_size) catch
            return error.OrderBookTooLarge;
        const total_size = std.math.add(usize, wire_header_size, levels_size) catch
            return error.OrderBookTooLarge;
        _ = std.math.cast(u32, total_size) orelse return error.OrderBookTooLarge;

        var next: std.ArrayListUnmanaged(u8) = .empty;
        errdefer next.deinit(self.kernel.allocator);
        try next.resize(self.kernel.allocator, total_size);

        @memcpy(next.items[0..4], "XCL2");
        std.mem.writeInt(u16, next.items[4..6], abi_version, .little);
        std.mem.writeInt(u16, next.items[6..8], wire_level_size, .little);
        std.mem.writeInt(u32, next.items[8..12], ask_count, .little);
        std.mem.writeInt(u32, next.items[12..16], bid_count, .little);

        var offset: usize = wire_header_size;
        for (snapshot.asks.items) |level| {
            std.mem.writeInt(i64, next.items[offset..][0..8], level.price, .little);
            std.mem.writeInt(i64, next.items[offset + 8 ..][0..8], level.volume, .little);
            std.mem.writeInt(u32, next.items[offset + 16 ..][0..4], level.orders, .little);
            std.mem.writeInt(u32, next.items[offset + 20 ..][0..4], 0, .little);
            offset = std.math.add(usize, offset, wire_level_size) catch
                return error.OrderBookTooLarge;
        }
        for (snapshot.bids.items) |level| {
            std.mem.writeInt(i64, next.items[offset..][0..8], level.price, .little);
            std.mem.writeInt(i64, next.items[offset + 8 ..][0..8], level.volume, .little);
            std.mem.writeInt(u32, next.items[offset + 16 ..][0..4], level.orders, .little);
            std.mem.writeInt(u32, next.items[offset + 20 ..][0..4], 0, .little);
            offset = std.math.add(usize, offset, wire_level_size) catch
                return error.OrderBookTooLarge;
        }
        std.debug.assert(offset == total_size);

        self.order_book_wire.deinit(self.kernel.allocator);
        self.order_book_wire = next;
    }
};

const Slot = struct {
    handle: u32 = 0,
    instance: ?*Instance = null,
};

const InstanceTable = struct {
    allocator: std.mem.Allocator,
    slots: [max_instances]Slot = [_]Slot{.{}} ** max_instances,
    next_handle: u32 = 1,

    fn init(allocator: std.mem.Allocator) InstanceTable {
        return .{ .allocator = allocator };
    }

    fn deinit(self: *InstanceTable) void {
        for (&self.slots) |*slot| {
            if (slot.instance) |instance| {
                instance.deinit();
                self.allocator.destroy(instance);
                slot.* = .{};
            }
        }
    }

    fn create(self: *InstanceTable, raw_kind: i32) u64 {
        const parsed_kind: OrderBookKind = switch (raw_kind) {
            0 => .naive,
            1 => .direct,
            else => return pack(.invalid_enum, 0),
        };
        if (self.next_handle == 0) return pack(.handle_exhausted, 0);

        const slot = self.firstFreeSlot() orelse return pack(.instance_limit, 0);
        const instance = self.allocator.create(Instance) catch
            return pack(.out_of_memory, 0);

        instance.* = Instance.init(
            self.allocator,
            switch (parsed_kind) {
                .naive => .naive,
                .direct => .direct,
            },
        ) catch |err| {
            self.allocator.destroy(instance);
            return switch (err) {
                error.OutOfMemory => pack(.out_of_memory, 0),
                else => pack(.internal, 0),
            };
        };
        instance.bindApi();

        const handle = self.next_handle;
        self.next_handle = if (handle == std.math.maxInt(u32)) 0 else handle + 1;
        slot.* = .{
            .handle = handle,
            .instance = instance,
        };
        return pack(.ok, handle);
    }

    fn destroy(self: *InstanceTable, handle: u32) AbiStatus {
        const slot = self.findSlot(handle) orelse return .invalid_handle;
        const instance = slot.instance orelse return .invalid_handle;
        instance.deinit();
        self.allocator.destroy(instance);
        slot.* = .{};
        return .ok;
    }

    fn reset(self: *InstanceTable, handle: u32) u64 {
        const instance = self.operational(handle) catch |err| return packTableError(err);
        instance.order_book_wire.clearRetainingCapacity();
        var response = instance.api.reset() catch |err| return instance.executionFailed(err);
        return instance.finishCommand(&response);
    }

    fn addSymbol(
        self: *InstanceTable,
        handle: u32,
        symbol_id: i32,
        raw_symbol_type: i32,
        base_currency: i32,
        quote_currency: i32,
        base_scale_k: i64,
        quote_scale_k: i64,
        taker_fee: i64,
        maker_fee: i64,
        margin_buy: i64,
        margin_sell: i64,
    ) u64 {
        const instance = self.operational(handle) catch |err| return packTableError(err);
        instance.last_event_count = 0;

        const spec: SymbolSpecification = .{
            .symbol_id = symbol_id,
            .symbol_type = switch (raw_symbol_type) {
                0 => .currency_exchange_pair,
                1 => .futures_contract,
                2 => .option,
                else => return pack(.invalid_enum, 0),
            },
            .base_currency = base_currency,
            .quote_currency = quote_currency,
            .base_scale_k = base_scale_k,
            .quote_scale_k = quote_scale_k,
            .taker_fee = taker_fee,
            .maker_fee = maker_fee,
            .margin_buy = margin_buy,
            .margin_sell = margin_sell,
        };
        spec.validate() catch return pack(.invalid_argument, 0);

        const added = instance.kernel.addSymbol(spec) catch |err| {
            switch (err) {
                error.NonPositiveBaseScale,
                error.NonPositiveQuoteScale,
                error.NegativeTakerFee,
                error.NegativeMakerFee,
                error.NegativeMarginBuy,
                error.NegativeMarginSell,
                => return pack(.invalid_argument, 0),
                error.OutOfMemory => {
                    instance.markFaulted();
                    return pack(.out_of_memory, 0);
                },
                else => {
                    instance.markFaulted();
                    return pack(.internal, 0);
                },
            }
        };
        return packResult(if (added) result_success else result_symbol_already_exists);
    }

    fn addUser(self: *InstanceTable, handle: u32, uid: i64) u64 {
        const instance = self.operational(handle) catch |err| return packTableError(err);
        instance.last_event_count = 0;
        var response = instance.api.addUser(uid) catch |err| return instance.executionFailed(err);
        return instance.finishCommand(&response);
    }

    fn adjustBalance(
        self: *InstanceTable,
        handle: u32,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        raw_adjustment_type: i32,
    ) u64 {
        const instance = self.operational(handle) catch |err| return packTableError(err);
        instance.last_event_count = 0;
        const adjustment_type: BalanceAdjustmentType = switch (raw_adjustment_type) {
            0 => .adjustment,
            1 => .@"suspend",
            else => return pack(.invalid_enum, 0),
        };
        var response = instance.api.adjustUserBalance(
            uid,
            transaction_id,
            currency,
            amount,
            adjustment_type,
        ) catch |err| return instance.executionFailed(err);
        return instance.finishCommand(&response);
    }

    fn placeOrder(
        self: *InstanceTable,
        handle: u32,
        order_id: i64,
        uid: i64,
        symbol: i32,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        raw_action: i32,
        raw_order_type: i32,
        user_cookie: i32,
    ) u64 {
        const instance = self.operational(handle) catch |err| return packTableError(err);
        instance.last_event_count = 0;
        const action: OrderAction = switch (raw_action) {
            0 => .ask,
            1 => .bid,
            else => return pack(.invalid_enum, 0),
        };
        const order_type: OrderType = switch (raw_order_type) {
            0 => .gtc,
            1 => .ioc,
            2 => .ioc_budget,
            3 => .fok,
            4 => .fok_budget,
            else => return pack(.invalid_enum, 0),
        };
        var response = instance.api.placeOrder(
            order_id,
            uid,
            symbol,
            price,
            reserve_bid_price,
            size,
            action,
            order_type,
            user_cookie,
        ) catch |err| return instance.executionFailed(err);
        return instance.finishCommand(&response);
    }

    fn moveOrder(
        self: *InstanceTable,
        handle: u32,
        order_id: i64,
        uid: i64,
        symbol: i32,
        new_price: i64,
    ) u64 {
        const instance = self.operational(handle) catch |err| return packTableError(err);
        instance.last_event_count = 0;
        var response = instance.api.moveOrder(order_id, uid, symbol, new_price) catch |err|
            return instance.executionFailed(err);
        return instance.finishCommand(&response);
    }

    fn cancelOrder(
        self: *InstanceTable,
        handle: u32,
        order_id: i64,
        uid: i64,
        symbol: i32,
    ) u64 {
        const instance = self.operational(handle) catch |err| return packTableError(err);
        instance.last_event_count = 0;
        var response = instance.api.cancelOrder(order_id, uid, symbol) catch |err|
            return instance.executionFailed(err);
        return instance.finishCommand(&response);
    }

    fn reduceOrder(
        self: *InstanceTable,
        handle: u32,
        order_id: i64,
        uid: i64,
        symbol: i32,
        reduce_size: i64,
    ) u64 {
        const instance = self.operational(handle) catch |err| return packTableError(err);
        instance.last_event_count = 0;
        var response = instance.api.reduceOrder(order_id, uid, symbol, reduce_size) catch |err|
            return instance.executionFailed(err);
        return instance.finishCommand(&response);
    }

    fn queryOrderBook(
        self: *InstanceTable,
        handle: u32,
        symbol: i32,
        raw_depth: u32,
    ) u64 {
        const instance = self.operational(handle) catch |err| return packTableError(err);
        instance.order_book_wire.clearRetainingCapacity();
        const depth = std.math.cast(usize, raw_depth) orelse
            return pack(.invalid_argument, 0);

        var report = instance.api.queryReport(.{
            .order_book = .{ .symbol = symbol, .depth = depth },
        }) catch |err| return switch (err) {
            error.OutOfMemory => pack(.out_of_memory, 0),
            error.UnknownSymbol => pack(.invalid_argument, 0),
            else => pack(.internal, 0),
        };
        defer report.deinit(instance.kernel.allocator);

        switch (report) {
            .order_book => |*snapshot| instance.encodeOrderBook(snapshot) catch |err| return switch (err) {
                error.OutOfMemory => pack(.out_of_memory, 0),
                error.OrderBookTooLarge => pack(.invalid_argument, 0),
            },
            else => return pack(.internal, 0),
        }
        return pack(.ok, 0);
    }

    fn orderBookPointer(self: *InstanceTable, handle: u32) u64 {
        const instance = self.operational(handle) catch |err| return packTableError(err);
        if (instance.order_book_wire.items.len == 0) return pack(.ok, 0);
        const pointer = std.math.cast(
            u32,
            @intFromPtr(instance.order_book_wire.items.ptr),
        ) orelse return pack(.internal, 0);
        return pack(.ok, pointer);
    }

    fn orderBookLength(self: *InstanceTable, handle: u32) u64 {
        const instance = self.operational(handle) catch |err| return packTableError(err);
        const length = std.math.cast(u32, instance.order_book_wire.items.len) orelse
            return pack(.internal, 0);
        return pack(.ok, length);
    }

    fn lastEventCount(self: *InstanceTable, handle: u32) u64 {
        const instance = self.operational(handle) catch |err| return packTableError(err);
        return pack(.ok, instance.last_event_count);
    }

    fn firstFreeSlot(self: *InstanceTable) ?*Slot {
        for (&self.slots) |*slot| {
            if (slot.instance == null) return slot;
        }
        return null;
    }

    fn findSlot(self: *InstanceTable, handle: u32) ?*Slot {
        if (handle == 0) return null;
        for (&self.slots) |*slot| {
            if (slot.handle == handle and slot.instance != null) return slot;
        }
        return null;
    }

    fn operational(self: *InstanceTable, handle: u32) TableError!*Instance {
        const slot = self.findSlot(handle) orelse return error.InvalidHandle;
        const instance = slot.instance orelse return error.InvalidHandle;
        if (instance.faulted) return error.Faulted;
        return instance;
    }
};

fn runtimeAllocator() std.mem.Allocator {
    if (comptime builtin.target.cpu.arch == .wasm32) {
        return std.heap.wasm_allocator;
    }
    return std.heap.page_allocator;
}

var global_instances = InstanceTable.init(runtimeAllocator());

fn pack(status: AbiStatus, payload: u32) u64 {
    return (@as(u64, @intFromEnum(status)) << 32) | @as(u64, payload);
}

fn packResult(result_code: i32) u64 {
    return pack(.ok, @bitCast(result_code));
}

fn packTableError(err: TableError) u64 {
    return switch (err) {
        error.InvalidHandle => pack(.invalid_handle, 0),
        error.Faulted => pack(.faulted, 0),
        error.InstanceLimit => pack(.instance_limit, 0),
        error.HandleExhausted => pack(.handle_exhausted, 0),
    };
}

export fn exchange_abi_version() u32 {
    return abi_version;
}

export fn exchange_create(orderbook_kind: i32) u64 {
    return global_instances.create(orderbook_kind);
}

export fn exchange_destroy(handle: u32) u32 {
    return @intFromEnum(global_instances.destroy(handle));
}

export fn exchange_reset(handle: u32) u64 {
    return global_instances.reset(handle);
}

export fn exchange_add_symbol(
    handle: u32,
    symbol_id: i32,
    symbol_type: i32,
    base_currency: i32,
    quote_currency: i32,
    base_scale_k: i64,
    quote_scale_k: i64,
    taker_fee: i64,
    maker_fee: i64,
    margin_buy: i64,
    margin_sell: i64,
) u64 {
    return global_instances.addSymbol(
        handle,
        symbol_id,
        symbol_type,
        base_currency,
        quote_currency,
        base_scale_k,
        quote_scale_k,
        taker_fee,
        maker_fee,
        margin_buy,
        margin_sell,
    );
}

export fn exchange_add_user(handle: u32, uid: i64) u64 {
    return global_instances.addUser(handle, uid);
}

export fn exchange_adjust_balance(
    handle: u32,
    uid: i64,
    transaction_id: i64,
    currency: i32,
    amount: i64,
    adjustment_type: i32,
) u64 {
    return global_instances.adjustBalance(
        handle,
        uid,
        transaction_id,
        currency,
        amount,
        adjustment_type,
    );
}

export fn exchange_place_order(
    handle: u32,
    order_id: i64,
    uid: i64,
    symbol: i32,
    price: i64,
    reserve_bid_price: i64,
    size: i64,
    action: i32,
    order_type: i32,
    user_cookie: i32,
) u64 {
    return global_instances.placeOrder(
        handle,
        order_id,
        uid,
        symbol,
        price,
        reserve_bid_price,
        size,
        action,
        order_type,
        user_cookie,
    );
}

export fn exchange_move_order(
    handle: u32,
    order_id: i64,
    uid: i64,
    symbol: i32,
    new_price: i64,
) u64 {
    return global_instances.moveOrder(handle, order_id, uid, symbol, new_price);
}

export fn exchange_cancel_order(
    handle: u32,
    order_id: i64,
    uid: i64,
    symbol: i32,
) u64 {
    return global_instances.cancelOrder(handle, order_id, uid, symbol);
}

export fn exchange_reduce_order(
    handle: u32,
    order_id: i64,
    uid: i64,
    symbol: i32,
    reduce_size: i64,
) u64 {
    return global_instances.reduceOrder(handle, order_id, uid, symbol, reduce_size);
}

export fn exchange_query_order_book(handle: u32, symbol: i32, depth: u32) u64 {
    return global_instances.queryOrderBook(handle, symbol, depth);
}

export fn exchange_last_event_count(handle: u32) u64 {
    return global_instances.lastEventCount(handle);
}

export fn exchange_order_book_ptr(handle: u32) u64 {
    return global_instances.orderBookPointer(handle);
}

export fn exchange_order_book_len(handle: u32) u64 {
    return global_instances.orderBookLength(handle);
}

fn unpackStatus(value: u64) AbiStatus {
    return @enumFromInt(@as(u32, @truncate(value >> 32)));
}

fn unpackPayload(value: u64) u32 {
    return @truncate(value);
}

fn unpackResult(value: u64) i32 {
    return @bitCast(unpackPayload(value));
}

fn expectOkResult(value: u64, expected: i32) !void {
    try std.testing.expectEqual(AbiStatus.ok, unpackStatus(value));
    try std.testing.expectEqual(expected, unpackResult(value));
}

test "browser wasm handle lifecycle rejects stale handles and enforces limits" {
    var table = InstanceTable.init(std.testing.allocator);
    defer table.deinit();

    try std.testing.expectEqual(AbiStatus.invalid_enum, unpackStatus(table.create(99)));

    const first_create = table.create(0);
    try std.testing.expectEqual(AbiStatus.ok, unpackStatus(first_create));
    const first = unpackPayload(first_create);
    try std.testing.expect(first != 0);
    try std.testing.expectEqual(AbiStatus.ok, table.destroy(first));
    try std.testing.expectEqual(AbiStatus.invalid_handle, unpackStatus(table.addUser(first, 1)));

    const replacement_create = table.create(1);
    try std.testing.expectEqual(AbiStatus.ok, unpackStatus(replacement_create));
    const replacement = unpackPayload(replacement_create);
    try std.testing.expect(replacement != first);

    for (1..max_instances) |_| {
        try std.testing.expectEqual(AbiStatus.ok, unpackStatus(table.create(0)));
    }
    try std.testing.expectEqual(AbiStatus.instance_limit, unpackStatus(table.create(0)));

    try std.testing.expectEqual(AbiStatus.invalid_enum, unpackStatus(table.addSymbol(
        replacement,
        241,
        99,
        11,
        15,
        1,
        1,
        0,
        0,
        0,
        0,
    )));
    try std.testing.expectEqual(AbiStatus.invalid_enum, unpackStatus(table.placeOrder(
        replacement,
        1,
        1,
        241,
        100,
        100,
        1,
        99,
        0,
        0,
    )));
}

test "browser wasm handle exhaustion never aliases a stale id" {
    var table = InstanceTable.init(std.testing.allocator);
    defer table.deinit();

    table.next_handle = std.math.maxInt(u32);
    const final_create = table.create(0);
    try std.testing.expectEqual(AbiStatus.ok, unpackStatus(final_create));
    try std.testing.expectEqual(std.math.maxInt(u32), unpackPayload(final_create));
    try std.testing.expectEqual(AbiStatus.ok, table.destroy(std.math.maxInt(u32)));
    try std.testing.expectEqual(AbiStatus.handle_exhausted, unpackStatus(table.create(0)));
}

test "browser wasm create reports allocation failures without leaking its stable slot" {
    var failing = std.testing.FailingAllocator.init(
        std.testing.allocator,
        .{ .fail_index = 1 },
    );
    var table = InstanceTable.init(failing.allocator());
    defer table.deinit();

    try std.testing.expectEqual(AbiStatus.out_of_memory, unpackStatus(table.create(0)));
    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    try std.testing.expectEqual(failing.allocations, failing.deallocations);
}

test "browser wasm order mutations preserve native command result codes" {
    var table = InstanceTable.init(std.testing.allocator);
    defer table.deinit();

    const create_result = table.create(0);
    try std.testing.expectEqual(AbiStatus.ok, unpackStatus(create_result));
    const handle = unpackPayload(create_result);

    try expectOkResult(table.addSymbol(
        handle,
        241,
        0,
        11,
        15,
        1,
        1,
        0,
        0,
        0,
        0,
    ), result_success);
    try expectOkResult(table.addUser(handle, 8_201), result_success);
    try expectOkResult(table.addUser(handle, 8_201), result_user_already_exists);
    try expectOkResult(table.adjustBalance(handle, 8_201, 1, 15, 100_000, 0), result_success);
    try expectOkResult(table.placeOrder(
        handle,
        99_001,
        8_201,
        241,
        100,
        110,
        5,
        1,
        0,
        0,
    ), result_success);
    try expectOkResult(table.moveOrder(handle, 99_001, 8_201, 241, 105), result_success);
    try expectOkResult(table.reduceOrder(handle, 99_001, 8_201, 241, 2), result_success);
    try expectOkResult(table.cancelOrder(handle, 99_001, 8_201, 241), result_success);
}

test "browser wasm deterministic exchange flow emits L2 wire and resets state" {
    var table = InstanceTable.init(std.testing.allocator);
    defer table.deinit();

    const create_result = table.create(1);
    try std.testing.expectEqual(AbiStatus.ok, unpackStatus(create_result));
    const handle = unpackPayload(create_result);

    try expectOkResult(table.addSymbol(
        handle,
        241,
        0,
        11,
        15,
        1,
        1,
        0,
        0,
        0,
        0,
    ), result_success);
    try expectOkResult(table.addUser(handle, 8101), result_success);
    try expectOkResult(table.addUser(handle, 8102), result_success);
    try expectOkResult(table.adjustBalance(handle, 8101, 1, 15, 100_000, 0), result_success);
    try expectOkResult(table.adjustBalance(handle, 8102, 2, 11, 100_000, 0), result_success);

    try expectOkResult(table.placeOrder(
        handle,
        98_101,
        8101,
        241,
        100,
        100,
        5,
        1,
        0,
        17,
    ), result_success);
    try expectOkResult(table.placeOrder(
        handle,
        98_102,
        8102,
        241,
        105,
        0,
        4,
        0,
        0,
        18,
    ), result_success);
    try expectOkResult(table.placeOrder(
        handle,
        98_103,
        8102,
        241,
        99,
        0,
        2,
        0,
        1,
        19,
    ), result_success);
    try std.testing.expectEqual(@as(u32, 1), unpackPayload(table.lastEventCount(handle)));

    const query_result = table.queryOrderBook(handle, 241, 8);
    try std.testing.expectEqual(AbiStatus.ok, unpackStatus(query_result));
    const instance = table.operational(handle) catch return error.TestExpectedLiveInstance;
    const wire = instance.order_book_wire.items;
    try std.testing.expectEqual(@as(usize, wire_header_size + 2 * wire_level_size), wire.len);
    try std.testing.expectEqualSlices(u8, "XCL2", wire[0..4]);
    try std.testing.expectEqual(abi_version, std.mem.readInt(u16, wire[4..6], .little));
    try std.testing.expectEqual(@as(u16, wire_level_size), std.mem.readInt(u16, wire[6..8], .little));
    try std.testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, wire[8..12], .little));
    try std.testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, wire[12..16], .little));

    try std.testing.expectEqual(@as(i64, 105), std.mem.readInt(i64, wire[16..24], .little));
    try std.testing.expectEqual(@as(i64, 4), std.mem.readInt(i64, wire[24..32], .little));
    try std.testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, wire[32..36], .little));
    try std.testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, wire[36..40], .little));

    try std.testing.expectEqual(@as(i64, 100), std.mem.readInt(i64, wire[40..48], .little));
    try std.testing.expectEqual(@as(i64, 3), std.mem.readInt(i64, wire[48..56], .little));
    try std.testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, wire[56..60], .little));
    try std.testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, wire[60..64], .little));

    try expectOkResult(table.reset(handle), result_success);
    try std.testing.expectEqual(@as(u32, 0), unpackPayload(table.lastEventCount(handle)));
    try std.testing.expectEqual(@as(u32, 0), unpackPayload(table.orderBookLength(handle)));
    try std.testing.expectEqual(
        AbiStatus.invalid_argument,
        unpackStatus(table.queryOrderBook(handle, 241, 8)),
    );
    try std.testing.expectEqual(@as(u32, 0), unpackPayload(table.orderBookLength(handle)));
}

test "browser wasm faulted instances fail closed until destroy" {
    var table = InstanceTable.init(std.testing.allocator);
    defer table.deinit();

    const create_result = table.create(0);
    const handle = unpackPayload(create_result);
    const instance = table.operational(handle) catch return error.TestExpectedLiveInstance;
    instance.markFaulted();

    try std.testing.expectEqual(AbiStatus.faulted, unpackStatus(table.addUser(handle, 1)));
    try std.testing.expectEqual(AbiStatus.faulted, unpackStatus(table.orderBookLength(handle)));
    try std.testing.expectEqual(AbiStatus.ok, table.destroy(handle));
}
