const std = @import("std");
const common = @import("common.zig");
const cmd = @import("cmd.zig");
const kernel_mod = @import("exchange_kernel.zig");
const risk = @import("risk.zig");

const log = std.log.scoped(.binary_commands);

pub const CurrencyBalance = struct {
    currency: i32,
    balance: i64,
};

pub const UserAccountsBatch = struct {
    uid: i64,
    accounts: []const CurrencyBalance,
};

pub const AccountsApplyOutcome = struct {
    users_added: usize = 0,
    balances_applied: usize = 0,
};

pub const ApplyError = std.mem.Allocator.Error ||
    kernel_mod.ExecuteError ||
    risk.SymbolSpecificationProvider.AddSymbolError;

pub fn applyBatchAddSymbols(
    kernel: *kernel_mod.ExchangeKernel,
    symbols: []const common.CoreSymbolSpecification,
) ApplyError!usize {
    var added: usize = 0;
    for (symbols) |spec| {
        if (try kernel.addSymbol(spec)) {
            added += 1;
        }
    }
    return added;
}

pub fn applyBatchAddAccounts(
    kernel: *kernel_mod.ExchangeKernel,
    users: []const UserAccountsBatch,
) ApplyError!AccountsApplyOutcome {
    var outcome: AccountsApplyOutcome = .{};
    for (users) |batch| {
        var add_user = makeAddUserCommand(batch.uid);
        var add_user_resp = try kernel.execute(&add_user);
        defer add_user_resp.deinit(kernel.allocator);

        if (add_user_resp.result_code != .success) {
            if (add_user_resp.result_code != .user_mgmt_user_already_exists) {
                log.debug(
                    "skipping balances for uid={d} due add_user result={s}",
                    .{ batch.uid, @tagName(add_user_resp.result_code) },
                );
            }
            continue;
        }

        outcome.users_added += 1;
        for (batch.accounts) |entry| {
            var adjust = makeBalanceAdjustmentCommand(
                batch.uid,
                entry.currency,
                entry.balance,
                1_000_000_000 + @as(i64, entry.currency),
            );
            var adjust_resp = try kernel.execute(&adjust);
            defer adjust_resp.deinit(kernel.allocator);
            if (adjust_resp.result_code == .success) {
                outcome.balances_applied += 1;
            } else {
                log.debug(
                    "balance adjust skipped uid={d} ccy={d} result={s}",
                    .{ batch.uid, entry.currency, @tagName(adjust_resp.result_code) },
                );
            }
        }
    }
    return outcome;
}

fn makeAddUserCommand(uid: i64) cmd.OrderCommand {
    return .{
        .command = .add_user,
        .uid = uid,
        .result_code = .new,
    };
}

fn makeBalanceAdjustmentCommand(
    uid: i64,
    currency: i32,
    amount: i64,
    tx_id: i64,
) cmd.OrderCommand {
    return .{
        .command = .balance_adjustment,
        .uid = uid,
        .symbol = currency,
        .price = amount,
        .order_id = tx_id,
        .order_type = .gtc,
        .result_code = .new,
    };
}

fn makeExchangeSpec(symbol_id: i32) !common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol_id,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    });
}

test "batch add symbols adds only new symbols" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer kernel.deinit();

    const spec_241 = try makeExchangeSpec(241);
    const spec_242 = try makeExchangeSpec(242);
    const symbols = [_]common.CoreSymbolSpecification{
        spec_241,
        spec_241,
        spec_242,
    };

    const added = try applyBatchAddSymbols(&kernel, symbols[0..]);
    try std.testing.expectEqual(@as(usize, 2), added);

    var report_241 = try kernel.queryReport(.{
        .order_book = .{
            .symbol = 241,
            .depth = 8,
        },
    });
    defer report_241.deinit(allocator);
    var report_242 = try kernel.queryReport(.{
        .order_book = .{
            .symbol = 242,
            .depth = 8,
        },
    });
    defer report_242.deinit(allocator);
}

test "batch add accounts creates users and applies balances" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .naive,
    });
    defer kernel.deinit();

    const u1_accounts = [_]CurrencyBalance{
        .{ .currency = 11, .balance = 500 },
        .{ .currency = 15, .balance = 800 },
    };
    const u2_accounts = [_]CurrencyBalance{
        .{ .currency = 15, .balance = 1200 },
    };
    const users = [_]UserAccountsBatch{
        .{ .uid = 111, .accounts = u1_accounts[0..] },
        .{ .uid = 222, .accounts = u2_accounts[0..] },
    };

    const outcome = try applyBatchAddAccounts(&kernel, users[0..]);
    try std.testing.expectEqual(@as(usize, 2), outcome.users_added);
    try std.testing.expectEqual(@as(usize, 3), outcome.balances_applied);

    var u1_11 = try kernel.queryReport(.{
        .user_balance = .{
            .uid = 111,
            .currency = 11,
        },
    });
    defer u1_11.deinit(allocator);
    var u1_15 = try kernel.queryReport(.{
        .user_balance = .{
            .uid = 111,
            .currency = 15,
        },
    });
    defer u1_15.deinit(allocator);
    var u2_15 = try kernel.queryReport(.{
        .user_balance = .{
            .uid = 222,
            .currency = 15,
        },
    });
    defer u2_15.deinit(allocator);

    try std.testing.expectEqual(@as(?i64, 500), u1_11.user_balance);
    try std.testing.expectEqual(@as(?i64, 800), u1_15.user_balance);
    try std.testing.expectEqual(@as(?i64, 1200), u2_15.user_balance);
}

test "batch add accounts skips existing users" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    const existing = [_]UserAccountsBatch{
        .{
            .uid = 333,
            .accounts = &[_]CurrencyBalance{
                .{ .currency = 15, .balance = 1000 },
            },
        },
    };
    _ = try applyBatchAddAccounts(&kernel, existing[0..]);

    const duplicate = [_]UserAccountsBatch{
        .{
            .uid = 333,
            .accounts = &[_]CurrencyBalance{
                .{ .currency = 15, .balance = 2000 },
            },
        },
    };
    const outcome = try applyBatchAddAccounts(&kernel, duplicate[0..]);
    try std.testing.expectEqual(@as(usize, 0), outcome.users_added);
    try std.testing.expectEqual(@as(usize, 0), outcome.balances_applied);

    var balance = try kernel.queryReport(.{
        .user_balance = .{
            .uid = 333,
            .currency = 15,
        },
    });
    defer balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 1000), balance.user_balance);
}
