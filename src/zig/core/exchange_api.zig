const std = @import("std");
const cmd = @import("cmd.zig");
const common = @import("common.zig");
const binary_payload = @import("binary_payload.zig");
const orderbook_naive = @import("orderbook_naive.zig");
const api_command_mod = @import("api_command.zig");
const exchange_core_mod = @import("exchange_core.zig");
const kernel_mod = @import("exchange_kernel.zig");
const runtime_mod = @import("exchange_runtime.zig");
const simple_events_mod = @import("simple_events_processor.zig");
const persistence_paths = @import("persistence_paths.zig");

pub const ApiCommand = api_command_mod.ApiCommand;
pub const ApiCommandResult = api_command_mod.ApiCommandResult;
pub const ApiCommandAsyncHandle = api_command_mod.ApiCommandAsyncHandle;
pub const BinaryReportQuery = api_command_mod.BinaryReportQuery;
pub const BinaryCurrencyBalance = api_command_mod.BinaryCurrencyBalance;
pub const BinaryUserAccountsBatch = api_command_mod.BinaryUserAccountsBatch;
pub const Config = exchange_core_mod.Config;
pub const PersistenceModuleType = exchange_core_mod.PersistenceModuleType;
pub const SnapshotRef = exchange_core_mod.SnapshotRef;
pub const JournalRef = exchange_core_mod.JournalRef;
pub const SnapshotDescriptor = exchange_core_mod.SnapshotDescriptor;
pub const JournalDescriptor = exchange_core_mod.JournalDescriptor;
pub const PersistenceFileNameError = persistence_paths.FileNameError;
pub const PersistencePathError = exchange_core_mod.PersistencePathError;
pub const ResultSink = runtime_mod.ResultSink;
pub const SimpleEventsHandler = simple_events_mod.EventsHandler;
pub const BatchResponse = kernel_mod.BatchResponse;

pub const SubmitCommandError = exchange_core_mod.SubmitApiCommandError;
pub const SubmitCommandAsyncError = exchange_core_mod.SubmitApiCommandAsyncError;
pub const HelperError = exchange_core_mod.HelperError;
pub const BinaryPayloadSubmitError = exchange_core_mod.BinaryPayloadSubmitError;
pub const BinaryPayloadAsyncSubmitError = exchange_core_mod.BinaryPayloadAsyncSubmitError;
pub const BinaryReportQuerySubmitError = exchange_core_mod.BinaryReportQuerySubmitError;
pub const SubmitBinaryPayloadAsyncError = exchange_core_mod.BinaryPayloadAsyncSubmitError;
pub const DrainPendingError = exchange_core_mod.ExecuteError;
pub const ExecuteError = exchange_core_mod.ExecuteError;
pub const AsyncSubmitError = exchange_core_mod.AsyncSubmitError;
pub const PersistStateRequestPair = exchange_core_mod.PersistStateRequestPair;
pub const SubmitCommandsSyncError = exchange_core_mod.SubmitApiCommandBatchError;
pub const SubmitCommandAsyncFullResponseError = SubmitCommandAsyncError || error{
    UnsupportedAsyncFullResponseCommand,
};
pub const ProcessCommandResultCodeError = SubmitCommandAsyncError || DrainPendingError || error{
    MissingAsyncResult,
};
pub const ProcessCommandFullResponseError = SubmitCommandAsyncFullResponseError || DrainPendingError || TakeFullResponseError || error{
    MissingAsyncResult,
};
pub const TakeFullResponseError = error{
    ExpectedResponseResult,
};
pub const TakeOrderBookSnapshotError = TakeFullResponseError || error{
    MissingOrderBookSnapshot,
};
pub const TakeReportResponseError = TakeFullResponseError || error{
    MissingReportResponse,
};
pub const TakeTypedReportError = TakeReportResponseError || error{
    UnexpectedReportResponseType,
};
pub const ProcessReportError = SubmitCommandAsyncFullResponseError || DrainPendingError || TakeReportResponseError || error{
    MissingAsyncResult,
};
pub const ProcessTypedReportError = SubmitCommandAsyncFullResponseError || DrainPendingError || TakeTypedReportError || error{
    MissingAsyncResult,
};
pub const ProcessOrderBookError = SubmitCommandAsyncFullResponseError || DrainPendingError || TakeOrderBookSnapshotError || error{
    MissingAsyncResult,
};
pub const QueryReportError = exchange_core_mod.ReportError;
pub const QueryTypedReportError = QueryReportError || error{
    UnexpectedReportResponseType,
};
pub const LifecycleError = exchange_core_mod.LifecycleError;
pub const InitFromJavaConfigError = exchange_core_mod.InitFromJavaConfigError;
pub const BootstrapError = exchange_core_mod.BootstrapError;
pub const StartupError = exchange_core_mod.StartupError;
pub const StartupFromConfiguredPersistenceError = exchange_core_mod.StartupFromConfiguredPersistenceError;
pub const PersistStateWithMainLogError = exchange_core_mod.PersistStateWithMainLogError;
pub const JournalApiError = exchange_core_mod.JournalApiError;
pub const JournalReplayError = exchange_core_mod.JournalReplayError;
pub const SnapshotWriteError = exchange_core_mod.SnapshotWriteError;
pub const SnapshotValidateError = exchange_core_mod.SnapshotValidateError;
pub const EnqueueRuntimeError = exchange_core_mod.LifecycleError || std.mem.Allocator.Error;
pub const RuntimeDrainError = exchange_core_mod.RuntimeDrainError;
pub const RuntimeDrainOutcome = runtime_mod.DrainOutcome;
pub const DefaultJournalApiError = exchange_core_mod.DefaultJournalApiError;
pub const DefaultJournalReplayError = exchange_core_mod.DefaultJournalReplayError;
pub const DefaultJournalReplayRangeError = exchange_core_mod.DefaultJournalReplayRangeError;
pub const DefaultSnapshotWriteError = exchange_core_mod.DefaultSnapshotWriteError;
pub const DefaultSnapshotValidateError = exchange_core_mod.DefaultSnapshotValidateError;
pub const SnapshotExistsError = exchange_core_mod.SnapshotExistsError;
pub const DefaultSnapshotExistsError = exchange_core_mod.DefaultSnapshotExistsError;
pub const BootstrapOutcome = exchange_core_mod.BootstrapOutcome;
pub const StartupFromDiscoveredConfiguredPersistenceError = exchange_core_mod.StartupFromDiscoveredConfiguredPersistenceError;
pub const StartupFromDiscoveredConfiguredPersistenceStrictError = exchange_core_mod.StartupFromDiscoveredConfiguredPersistenceStrictError;
pub const ProcessResultError = error{
    UnexpectedPersistStateResult,
};
pub const CallbackRegistrationError = error{
    UnsupportedPersistStateHandle,
    CallbackRegistryFull,
};
pub const SubmitCommandWithCallbackError = SubmitCommandAsyncFullResponseError || CallbackRegistrationError;
pub const ProcessRegisteredCallbackError = ProcessResultError || CallbackRegistrationError;

pub const CommandResponseCallback = struct {
    context: ?*anyopaque = null,
    callback: *const fn (?*anyopaque, *const kernel_mod.CommandResponse) void,
};

const MaxCallbackRegistrations = 2048;
const CallbackRegistration = struct {
    request_id: u64,
    callback: CommandResponseCallback,
};

pub const ExchangeApi = struct {
    allocator: std.mem.Allocator,
    core: *exchange_core_mod.ExchangeCore,
    callback_registrations_len: usize = 0,
    callback_registrations: [MaxCallbackRegistrations]CallbackRegistration = undefined,

    pub fn init(
        allocator: std.mem.Allocator,
        core: *exchange_core_mod.ExchangeCore,
    ) ExchangeApi {
        return .{
            .allocator = allocator,
            .core = core,
        };
    }

    pub fn initFromJavaConfig(
        allocator: std.mem.Allocator,
        java_config: exchange_core_mod.JavaConfig,
    ) InitFromJavaConfigError!exchange_core_mod.ExchangeCore {
        return exchange_core_mod.ExchangeCore.initFromJavaConfig(allocator, java_config);
    }

    pub fn deinit(self: *ExchangeApi) void {
        // ExchangeApi is a non-owning facade over ExchangeCore.
        self.callback_registrations_len = 0;
        self.* = undefined;
    }

    pub fn kernelRef(self: *ExchangeApi) *kernel_mod.ExchangeKernel {
        return self.core.kernelRef();
    }

    pub fn configRef(self: *const ExchangeApi) *const Config {
        return self.core.configRef();
    }

    pub fn isStarted(self: *const ExchangeApi) bool {
        return self.core.isStarted();
    }

    pub fn isStopped(self: *const ExchangeApi) bool {
        return self.core.isStopped();
    }

    pub fn journalingEnabled(self: *const ExchangeApi) bool {
        return self.core.journalingEnabled();
    }

    pub fn journalLen(self: *const ExchangeApi) usize {
        return self.core.journalLen();
    }

    pub fn asyncPendingCount(self: *const ExchangeApi) usize {
        return self.core.asyncPendingCount();
    }

    pub fn asyncReadyCount(self: *const ExchangeApi) usize {
        return self.core.asyncReadyCount();
    }

    pub fn runtimePendingCount(self: *const ExchangeApi) usize {
        return self.core.runtimePendingCount();
    }

    pub fn enqueueRuntime(
        self: *ExchangeApi,
        command: cmd.OrderCommand,
    ) EnqueueRuntimeError!void {
        return self.core.enqueueRuntime(command);
    }

    pub fn drainRuntime(self: *ExchangeApi) RuntimeDrainError!RuntimeDrainOutcome {
        return self.core.drainRuntime();
    }

    pub fn defaultPersistenceSnapshotFileName(
        self: *const ExchangeApi,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) PersistenceFileNameError![]u8 {
        return self.core.defaultPersistenceSnapshotFileName(
            allocator,
            snapshot_id,
            module_type,
            instance_id,
        );
    }

    pub fn defaultPersistenceJournalFileName(
        self: *const ExchangeApi,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
        partition_id: u16,
    ) PersistenceFileNameError![]u8 {
        return self.core.defaultPersistenceJournalFileName(
            allocator,
            snapshot_id,
            partition_id,
        );
    }

    pub fn defaultPersistenceMainLogFileName(
        self: *const ExchangeApi,
        allocator: std.mem.Allocator,
    ) PersistenceFileNameError![]u8 {
        return self.core.defaultPersistenceMainLogFileName(allocator);
    }

    pub fn defaultPersistenceSnapshotPath(
        self: *const ExchangeApi,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) PersistencePathError![]u8 {
        return self.core.defaultPersistenceSnapshotPath(
            allocator,
            snapshot_id,
            module_type,
            instance_id,
        );
    }

    pub fn defaultPersistenceJournalPath(
        self: *const ExchangeApi,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
        partition_id: u16,
    ) PersistencePathError![]u8 {
        return self.core.defaultPersistenceJournalPath(
            allocator,
            snapshot_id,
            partition_id,
        );
    }

    pub fn defaultPersistenceMainLogPath(
        self: *const ExchangeApi,
        allocator: std.mem.Allocator,
    ) PersistencePathError![]u8 {
        return self.core.defaultPersistenceMainLogPath(allocator);
    }

    pub fn submitCommand(
        self: *ExchangeApi,
        command: ApiCommand,
    ) SubmitCommandError!void {
        var result = try self.core.submitApiCommand(command);
        defer result.deinit(self.allocator);
    }

    pub fn addUser(
        self: *ExchangeApi,
        uid: i64,
    ) ExecuteError!kernel_mod.CommandResponse {
        return self.core.addUser(uid);
    }

    pub fn adjustUserBalance(
        self: *ExchangeApi,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
    ) ExecuteError!kernel_mod.CommandResponse {
        return self.core.adjustUserBalance(
            uid,
            transaction_id,
            currency,
            amount,
            adjustment_type,
        );
    }

    pub fn placeOrder(
        self: *ExchangeApi,
        order_id: i64,
        uid: i64,
        symbol: i32,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: common.OrderAction,
        order_type: common.OrderType,
        user_cookie: i32,
    ) ExecuteError!kernel_mod.CommandResponse {
        return self.core.placeOrder(
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

    pub fn submitApiCommand(
        self: *ExchangeApi,
        command: ApiCommand,
    ) SubmitCommandError!void {
        return self.submitCommand(command);
    }

    pub fn submitCommandAsync(
        self: *ExchangeApi,
        command: ApiCommand,
    ) SubmitCommandAsyncError!ApiCommandAsyncHandle {
        return self.core.submitApiCommandAsync(command);
    }

    pub fn addUserAsync(
        self: *ExchangeApi,
        uid: i64,
    ) AsyncSubmitError!u64 {
        return self.core.addUserAsync(uid);
    }

    pub fn adjustUserBalanceAsync(
        self: *ExchangeApi,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
    ) AsyncSubmitError!u64 {
        return self.core.adjustUserBalanceAsync(
            uid,
            transaction_id,
            currency,
            amount,
            adjustment_type,
        );
    }

    pub fn suspendUserAsync(
        self: *ExchangeApi,
        uid: i64,
    ) AsyncSubmitError!u64 {
        return self.core.suspendUserAsync(uid);
    }

    pub fn resumeUserAsync(
        self: *ExchangeApi,
        uid: i64,
    ) AsyncSubmitError!u64 {
        return self.core.resumeUserAsync(uid);
    }

    pub fn placeOrderAsync(
        self: *ExchangeApi,
        order_id: i64,
        uid: i64,
        symbol: i32,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: common.OrderAction,
        order_type: common.OrderType,
        user_cookie: i32,
    ) AsyncSubmitError!u64 {
        return self.core.placeOrderAsync(
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

    pub fn moveOrderAsync(
        self: *ExchangeApi,
        order_id: i64,
        uid: i64,
        symbol: i32,
        new_price: i64,
    ) AsyncSubmitError!u64 {
        return self.core.moveOrderAsync(
            order_id,
            uid,
            symbol,
            new_price,
        );
    }

    pub fn cancelOrderAsync(
        self: *ExchangeApi,
        order_id: i64,
        uid: i64,
        symbol: i32,
    ) AsyncSubmitError!u64 {
        return self.core.cancelOrderAsync(order_id, uid, symbol);
    }

    pub fn reduceOrderAsync(
        self: *ExchangeApi,
        order_id: i64,
        uid: i64,
        symbol: i32,
        reduce_size: i64,
    ) AsyncSubmitError!u64 {
        return self.core.reduceOrderAsync(order_id, uid, symbol, reduce_size);
    }

    pub fn resetAsync(self: *ExchangeApi) AsyncSubmitError!u64 {
        return self.core.resetAsync();
    }

    pub fn groupingControlAsync(
        self: *ExchangeApi,
        timestamp_ns: i64,
        mode: i64,
    ) AsyncSubmitError!u64 {
        return self.core.groupingControlAsync(timestamp_ns, mode);
    }

    pub fn nopAsync(self: *ExchangeApi) AsyncSubmitError!u64 {
        return self.core.nopAsync();
    }

    pub fn submitApiCommandAsync(
        self: *ExchangeApi,
        command: ApiCommand,
    ) SubmitCommandAsyncError!ApiCommandAsyncHandle {
        return self.submitCommandAsync(command);
    }

    pub fn submitCommandAsyncResultCode(
        self: *ExchangeApi,
        command: ApiCommand,
    ) SubmitCommandAsyncError!ApiCommandAsyncHandle {
        return self.submitCommandAsync(command);
    }

    pub fn submitCommandAsyncFullResponse(
        self: *ExchangeApi,
        command: ApiCommand,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        if (!supportsAsyncFullResponse(command)) {
            return error.UnsupportedAsyncFullResponseCommand;
        }
        return self.submitCommandAsync(command);
    }

    pub fn processCommandResultCode(
        self: *ExchangeApi,
        command: ApiCommand,
    ) ProcessCommandResultCodeError!cmd.CommandResultCode {
        const handle = try self.submitCommandAsyncResultCode(command);
        _ = try self.drainPending();
        return self.takeResultCode(handle) orelse error.MissingAsyncResult;
    }

    pub fn processCommandFullResponse(
        self: *ExchangeApi,
        command: ApiCommand,
    ) ProcessCommandFullResponseError!kernel_mod.CommandResponse {
        const handle = try self.submitCommandAsyncFullResponse(command);
        _ = try self.drainPending();
        return (try self.takeFullResponse(handle)) orelse error.MissingAsyncResult;
    }

    pub fn requestOrderBookAsync(
        self: *ExchangeApi,
        symbol: i32,
        depth: usize,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(.{
            .request_order_book = .{
                .symbol = symbol,
                .depth = depth,
            },
        });
    }

    pub fn orderBookRequestAsync(
        self: *ExchangeApi,
        symbol: i32,
        depth: usize,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.requestOrderBookAsync(symbol, depth);
    }

    pub fn requestOrderBook(
        self: *ExchangeApi,
        symbol: i32,
        depth: usize,
    ) (ExecuteError || HelperError)!kernel_mod.CommandResponse {
        return self.core.requestOrderBook(symbol, depth);
    }

    pub fn submitCreateUserAsync(
        self: *ExchangeApi,
        user_id: i64,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(.{ .add_user = user_id });
    }

    pub fn submitSuspendUserAsync(
        self: *ExchangeApi,
        user_id: i64,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(.{ .suspend_user = user_id });
    }

    pub fn submitResumeUserAsync(
        self: *ExchangeApi,
        user_id: i64,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(.{ .resume_user = user_id });
    }

    pub fn submitBalanceAdjustmentAsync(
        self: *ExchangeApi,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = transaction_id,
                .currency = currency,
                .amount = amount,
                .adjustment_type = adjustment_type,
            },
        });
    }

    pub fn submitPlaceNewOrderAsync(
        self: *ExchangeApi,
        order_id: i64,
        user_cookie: i32,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: common.OrderAction,
        order_type: common.OrderType,
        symbol: i32,
        uid: i64,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(.{
            .place_order = .{
                .order_id = order_id,
                .uid = uid,
                .symbol = symbol,
                .price = price,
                .reserve_bid_price = reserve_bid_price,
                .size = size,
                .action = action,
                .order_type = order_type,
                .user_cookie = user_cookie,
            },
        });
    }

    pub fn submitMoveOrderAsync(
        self: *ExchangeApi,
        price: i64,
        order_id: i64,
        symbol: i32,
        uid: i64,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(.{
            .move_order = .{
                .order_id = order_id,
                .uid = uid,
                .symbol = symbol,
                .new_price = price,
            },
        });
    }

    pub fn submitCancelOrderAsync(
        self: *ExchangeApi,
        order_id: i64,
        symbol: i32,
        uid: i64,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(.{
            .cancel_order = .{
                .order_id = order_id,
                .uid = uid,
                .symbol = symbol,
            },
        });
    }

    pub fn submitReduceOrderAsync(
        self: *ExchangeApi,
        reduce_size: i64,
        order_id: i64,
        symbol: i32,
        uid: i64,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(.{
            .reduce_order = .{
                .order_id = order_id,
                .uid = uid,
                .symbol = symbol,
                .reduce_size = reduce_size,
            },
        });
    }

    pub fn submitPersistStateAsync(
        self: *ExchangeApi,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) SubmitCommandAsyncError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncResultCode(.{
            .persist_state = .{
                .snapshot_id = snapshot_id,
                .timestamp_ns = timestamp_ns,
            },
        });
    }

    pub fn processPersistStateResultCode(
        self: *ExchangeApi,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) ProcessCommandResultCodeError!cmd.CommandResultCode {
        return self.processCommandResultCode(.{
            .persist_state = .{
                .snapshot_id = snapshot_id,
                .timestamp_ns = timestamp_ns,
            },
        });
    }

    pub fn createUserWithCallback(
        self: *ExchangeApi,
        user_id: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(.{ .add_user = user_id }, callback);
    }

    pub fn suspendUserWithCallback(
        self: *ExchangeApi,
        user_id: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(.{ .suspend_user = user_id }, callback);
    }

    pub fn resumeUserWithCallback(
        self: *ExchangeApi,
        user_id: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(.{ .resume_user = user_id }, callback);
    }

    pub fn balanceAdjustmentWithCallback(
        self: *ExchangeApi,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = transaction_id,
                .currency = currency,
                .amount = amount,
                .adjustment_type = adjustment_type,
            },
        }, callback);
    }

    pub fn orderBookRequest(
        self: *ExchangeApi,
        symbol_id: i32,
        depth: usize,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(.{
            .request_order_book = .{
                .symbol = symbol_id,
                .depth = depth,
            },
        }, callback);
    }

    pub fn placeNewOrderWithCallback(
        self: *ExchangeApi,
        order_id: i64,
        user_cookie: i32,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: common.OrderAction,
        order_type: common.OrderType,
        symbol: i32,
        uid: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(.{
            .place_order = .{
                .order_id = order_id,
                .uid = uid,
                .symbol = symbol,
                .price = price,
                .reserve_bid_price = reserve_bid_price,
                .size = size,
                .action = action,
                .order_type = order_type,
                .user_cookie = user_cookie,
            },
        }, callback);
    }

    pub fn moveOrderWithCallback(
        self: *ExchangeApi,
        price: i64,
        order_id: i64,
        symbol: i32,
        uid: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(.{
            .move_order = .{
                .order_id = order_id,
                .uid = uid,
                .symbol = symbol,
                .new_price = price,
            },
        }, callback);
    }

    pub fn cancelOrderWithCallback(
        self: *ExchangeApi,
        order_id: i64,
        symbol: i32,
        uid: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(.{
            .cancel_order = .{
                .order_id = order_id,
                .uid = uid,
                .symbol = symbol,
            },
        }, callback);
    }

    pub fn reduceOrderWithCallback(
        self: *ExchangeApi,
        reduce_size: i64,
        order_id: i64,
        symbol: i32,
        uid: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(.{
            .reduce_order = .{
                .order_id = order_id,
                .uid = uid,
                .symbol = symbol,
                .reduce_size = reduce_size,
            },
        }, callback);
    }

    pub fn submitBinaryReportQueryAsync(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(.{
            .binary_report_query = .{
                .transfer_id = transfer_id,
                .timestamp = timestamp,
                .query = query,
            },
        });
    }

    pub fn submitBinaryDataPayloadAsync(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) SubmitBinaryPayloadAsyncError!ApiCommandAsyncHandle {
        const request_id = try self.core.submitBinaryPayloadAsync(
            .binary_data_command,
            transfer_id,
            timestamp,
            payload,
        );
        return .{ .request_id = request_id };
    }

    pub fn submitBinaryDataAsync(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) SubmitBinaryPayloadAsyncError!ApiCommandAsyncHandle {
        return self.submitBinaryDataPayloadAsync(transfer_id, timestamp, payload);
    }

    pub fn submitBinaryCommandAsync(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) SubmitBinaryPayloadAsyncError!ApiCommandAsyncHandle {
        return self.submitBinaryDataPayloadAsync(transfer_id, timestamp, payload);
    }

    pub fn submitBinaryQueryPayloadAsync(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) SubmitBinaryPayloadAsyncError!ApiCommandAsyncHandle {
        const request_id = try self.core.submitBinaryPayloadAsync(
            .binary_data_query,
            transfer_id,
            timestamp,
            payload,
        );
        return .{ .request_id = request_id };
    }

    pub fn publishBinaryData(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) SubmitBinaryPayloadAsyncError!ApiCommandAsyncHandle {
        return self.submitBinaryDataPayloadAsync(transfer_id, timestamp, payload);
    }

    pub fn publishQuery(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitBinaryReportQueryAsync(transfer_id, timestamp, query);
    }

    pub fn submitQueryAsync(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitBinaryReportQueryAsync(transfer_id, timestamp, query);
    }

    pub fn requestStateHashAsync(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitBinaryReportQueryAsync(transfer_id, timestamp, .state_hash);
    }

    pub fn requestSingleUserReportAsync(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
        uid: i64,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitBinaryReportQueryAsync(transfer_id, timestamp, .{ .single_user = uid });
    }

    pub fn requestTotalCurrencyBalanceAsync(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitBinaryReportQueryAsync(transfer_id, timestamp, .total_currency_balance);
    }

    pub fn isResultReady(
        self: *const ExchangeApi,
        handle: ApiCommandAsyncHandle,
    ) bool {
        return self.core.isApiCommandAsyncResultReady(handle);
    }

    pub fn isAsyncResultReady(
        self: *const ExchangeApi,
        request_id: u64,
    ) bool {
        return self.core.isAsyncResultReady(request_id);
    }

    pub fn isApiCommandAsyncResultReady(
        self: *const ExchangeApi,
        handle: ApiCommandAsyncHandle,
    ) bool {
        return self.isResultReady(handle);
    }

    pub fn takeResult(
        self: *ExchangeApi,
        handle: ApiCommandAsyncHandle,
    ) ?ApiCommandResult {
        return self.core.takeApiCommandAsyncResult(handle);
    }

    pub fn takeAsyncResult(
        self: *ExchangeApi,
        request_id: u64,
    ) ?kernel_mod.CommandResponse {
        return self.core.takeAsyncResult(request_id);
    }

    pub fn takeApiCommandAsyncResult(
        self: *ExchangeApi,
        handle: ApiCommandAsyncHandle,
    ) ?ApiCommandResult {
        return self.takeResult(handle);
    }

    pub fn registerResultCallback(
        self: *ExchangeApi,
        handle: ApiCommandAsyncHandle,
        callback: CommandResponseCallback,
    ) CallbackRegistrationError!void {
        try self.registerCallbackForHandle(handle, callback);
    }

    pub fn pendingCallbackCount(self: *const ExchangeApi) usize {
        return self.callback_registrations_len;
    }

    pub fn processResult(
        self: *ExchangeApi,
        handle: ApiCommandAsyncHandle,
        callback: CommandResponseCallback,
    ) ProcessResultError!bool {
        var result = self.takeResult(handle) orelse return false;
        defer result.deinit(self.allocator);
        switch (result) {
            .response => |*response| callback.callback(callback.context, response),
            .persist_state => return error.UnexpectedPersistStateResult,
        }
        return true;
    }

    pub fn processRegisteredCallback(
        self: *ExchangeApi,
        handle: ApiCommandAsyncHandle,
    ) ProcessRegisteredCallbackError!bool {
        const request_id = try requestIdFromHandle(handle);
        const idx = self.callbackRegistrationIndex(request_id) orelse return false;
        const callback = self.callback_registrations[idx].callback;
        const consumed = try self.processResult(handle, callback);
        if (consumed) {
            self.removeCallbackRegistrationAt(idx);
        }
        return consumed;
    }

    pub fn processPendingCallbacks(self: *ExchangeApi) ProcessRegisteredCallbackError!usize {
        var processed: usize = 0;
        var idx: usize = 0;
        while (idx < self.callback_registrations_len) {
            const registration = self.callback_registrations[idx];
            const consumed = try self.processResult(.{ .request_id = registration.request_id }, registration.callback);
            if (consumed) {
                self.removeCallbackRegistrationAt(idx);
                processed += 1;
            } else {
                idx += 1;
            }
        }
        return processed;
    }

    pub fn takeResultCode(
        self: *ExchangeApi,
        handle: ApiCommandAsyncHandle,
    ) ?cmd.CommandResultCode {
        var result = self.takeResult(handle) orelse return null;
        defer result.deinit(self.allocator);
        return switch (result) {
            .response => |response| response.result_code,
            .persist_state => |result_code| result_code,
        };
    }

    pub fn takeFullResponse(
        self: *ExchangeApi,
        handle: ApiCommandAsyncHandle,
    ) TakeFullResponseError!?kernel_mod.CommandResponse {
        const result = self.takeResult(handle) orelse return null;
        return switch (result) {
            .response => |response| response,
            .persist_state => error.ExpectedResponseResult,
        };
    }

    pub fn takeOrderBookSnapshot(
        self: *ExchangeApi,
        handle: ApiCommandAsyncHandle,
    ) TakeOrderBookSnapshotError!?orderbook_naive.L2MarketData {
        var response = (try self.takeFullResponse(handle)) orelse return null;
        errdefer response.deinit(self.allocator);

        const snapshot = response.l2_snapshot orelse return error.MissingOrderBookSnapshot;
        response.l2_snapshot = null;
        response.deinit(self.allocator);
        return snapshot;
    }

    pub fn takeReportResponse(
        self: *ExchangeApi,
        handle: ApiCommandAsyncHandle,
    ) TakeReportResponseError!?kernel_mod.ReportResponse {
        var response = (try self.takeFullResponse(handle)) orelse return null;
        errdefer response.deinit(self.allocator);

        const report = response.report_response orelse return error.MissingReportResponse;
        response.report_response = null;
        response.deinit(self.allocator);
        return report;
    }

    pub fn takeStateHashDetailed(
        self: *ExchangeApi,
        handle: ApiCommandAsyncHandle,
    ) TakeTypedReportError!?kernel_mod.DetailedStateHashReport {
        var report = (try self.takeReportResponse(handle)) orelse return null;
        errdefer report.deinit(self.allocator);
        return switch (report) {
            .state_hash_detailed => |detailed| detailed,
            else => error.UnexpectedReportResponseType,
        };
    }

    pub fn takeSingleUserReport(
        self: *ExchangeApi,
        handle: ApiCommandAsyncHandle,
    ) TakeTypedReportError!?kernel_mod.SingleUserReport {
        var report = (try self.takeReportResponse(handle)) orelse return null;
        errdefer report.deinit(self.allocator);
        return switch (report) {
            .single_user => |single_user| single_user,
            else => error.UnexpectedReportResponseType,
        };
    }

    pub fn takeTotalCurrencyBalanceReport(
        self: *ExchangeApi,
        handle: ApiCommandAsyncHandle,
    ) TakeTypedReportError!?kernel_mod.TotalCurrencyBalanceReport {
        var report = (try self.takeReportResponse(handle)) orelse return null;
        errdefer report.deinit(self.allocator);
        return switch (report) {
            .total_currency_balance => |balance| balance,
            else => error.UnexpectedReportResponseType,
        };
    }

    pub fn processReport(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) ProcessReportError!kernel_mod.ReportResponse {
        const handle = try self.submitBinaryReportQueryAsync(transfer_id, timestamp, query);
        _ = try self.drainPending();
        return (try self.takeReportResponse(handle)) orelse error.MissingAsyncResult;
    }

    pub fn processStateHashReport(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
    ) ProcessTypedReportError!kernel_mod.DetailedStateHashReport {
        const handle = try self.requestStateHashAsync(transfer_id, timestamp);
        _ = try self.drainPending();
        return (try self.takeStateHashDetailed(handle)) orelse error.MissingAsyncResult;
    }

    pub fn processSingleUserReport(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
        uid: i64,
    ) ProcessTypedReportError!kernel_mod.SingleUserReport {
        const handle = try self.requestSingleUserReportAsync(transfer_id, timestamp, uid);
        _ = try self.drainPending();
        return (try self.takeSingleUserReport(handle)) orelse error.MissingAsyncResult;
    }

    pub fn processTotalCurrencyBalanceReport(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
    ) ProcessTypedReportError!kernel_mod.TotalCurrencyBalanceReport {
        const handle = try self.requestTotalCurrencyBalanceAsync(transfer_id, timestamp);
        _ = try self.drainPending();
        return (try self.takeTotalCurrencyBalanceReport(handle)) orelse error.MissingAsyncResult;
    }

    pub fn processOrderBookRequest(
        self: *ExchangeApi,
        symbol: i32,
        depth: usize,
    ) ProcessOrderBookError!orderbook_naive.L2MarketData {
        const handle = try self.requestOrderBookAsync(symbol, depth);
        _ = try self.drainPending();
        return (try self.takeOrderBookSnapshot(handle)) orelse error.MissingAsyncResult;
    }

    pub fn queryReport(
        self: *ExchangeApi,
        query: kernel_mod.ReportQuery,
    ) QueryReportError!kernel_mod.ReportResponse {
        return self.core.queryReport(query);
    }

    pub fn queryRiskStateHash(self: *ExchangeApi) QueryTypedReportError!u64 {
        var report = try self.queryReport(.risk_state_hash);
        defer report.deinit(self.allocator);
        return switch (report) {
            .risk_state_hash => |value| value,
            else => error.UnexpectedReportResponseType,
        };
    }

    pub fn queryFeeCollected(
        self: *ExchangeApi,
        currency: i32,
    ) QueryTypedReportError!i64 {
        var report = try self.queryReport(.{ .fee_collected = currency });
        defer report.deinit(self.allocator);
        return switch (report) {
            .fee_collected => |value| value,
            else => error.UnexpectedReportResponseType,
        };
    }

    pub fn queryUserBalance(
        self: *ExchangeApi,
        uid: i64,
        currency: i32,
    ) QueryTypedReportError!?i64 {
        var report = try self.queryReport(.{
            .user_balance = .{
                .uid = uid,
                .currency = currency,
            },
        });
        defer report.deinit(self.allocator);
        return switch (report) {
            .user_balance => |value| value,
            else => error.UnexpectedReportResponseType,
        };
    }

    pub fn queryOrderBook(
        self: *ExchangeApi,
        symbol: i32,
        depth: usize,
    ) QueryTypedReportError!orderbook_naive.L2MarketData {
        var report = try self.queryReport(.{
            .order_book = .{
                .symbol = symbol,
                .depth = depth,
            },
        });
        errdefer report.deinit(self.allocator);
        return switch (report) {
            .order_book => |snapshot| snapshot,
            else => error.UnexpectedReportResponseType,
        };
    }

    pub fn drainPending(self: *ExchangeApi) DrainPendingError!usize {
        return self.core.drainAsyncPending();
    }

    pub fn drainAsyncPending(self: *ExchangeApi) DrainPendingError!usize {
        return self.drainPending();
    }

    pub fn submitCommandsSync(
        self: *ExchangeApi,
        commands: []const ApiCommand,
    ) SubmitCommandsSyncError!void {
        if (commands.len == 0) return;
        var result = try self.core.submitApiCommandsSync(commands);
        defer result.deinit(self.allocator);
    }

    pub fn submitApiCommandsSync(
        self: *ExchangeApi,
        commands: []const ApiCommand,
    ) SubmitCommandsSyncError!void {
        return self.submitCommandsSync(commands);
    }

    pub fn submitCommandsSyncWithNop(
        self: *ExchangeApi,
        commands: []const ApiCommand,
    ) SubmitCommandsSyncError!kernel_mod.CommandResponse {
        return self.core.submitApiCommandsSyncWithNop(commands);
    }

    pub fn submitApiCommandsSyncWithNop(
        self: *ExchangeApi,
        commands: []const ApiCommand,
    ) SubmitCommandsSyncError!kernel_mod.CommandResponse {
        return self.submitCommandsSyncWithNop(commands);
    }

    pub fn persistState(
        self: *ExchangeApi,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) ExecuteError!cmd.CommandResultCode {
        return self.core.persistState(snapshot_id, timestamp_ns);
    }

    pub fn submitCommandBatch(
        self: *ExchangeApi,
        commands: []*cmd.OrderCommand,
    ) ExecuteError!BatchResponse {
        return self.core.submitCommandBatch(commands);
    }

    pub fn submitBinaryPayload(
        self: *ExchangeApi,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadSubmitError!kernel_mod.CommandResponse {
        return self.core.submitBinaryPayload(
            command_type,
            transfer_id,
            timestamp,
            payload,
        );
    }

    pub fn submitBinaryPayloadAsync(
        self: *ExchangeApi,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadAsyncSubmitError!u64 {
        return self.core.submitBinaryPayloadAsync(
            command_type,
            transfer_id,
            timestamp,
            payload,
        );
    }

    pub fn submitBinaryReportQuery(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) BinaryReportQuerySubmitError!kernel_mod.CommandResponse {
        return self.core.submitBinaryReportQuery(transfer_id, timestamp, query);
    }

    pub fn persistStateAsync(
        self: *ExchangeApi,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) AsyncSubmitError!PersistStateRequestPair {
        return self.core.persistStateAsync(snapshot_id, timestamp_ns);
    }

    pub fn isPersistStateResultReady(
        self: *const ExchangeApi,
        requests: PersistStateRequestPair,
    ) bool {
        return self.core.isPersistStateResultReady(requests);
    }

    pub fn takePersistStateResult(
        self: *ExchangeApi,
        requests: PersistStateRequestPair,
    ) ?cmd.CommandResultCode {
        return self.core.takePersistStateResult(requests);
    }

    pub fn setResultSink(
        self: *ExchangeApi,
        sink: ResultSink,
    ) void {
        self.core.setResultSink(sink);
    }

    pub fn setSimpleEventsHandler(
        self: *ExchangeApi,
        handler: SimpleEventsHandler,
    ) void {
        self.core.setSimpleEventsHandler(handler);
    }

    pub fn clearSimpleEventsHandler(self: *ExchangeApi) void {
        self.core.clearSimpleEventsHandler();
    }

    pub fn submitAddSymbolsBinary(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
        symbols: []const common.CoreSymbolSpecification,
    ) SubmitCommandError!void {
        try self.submitCommand(.{
            .binary_add_symbols = .{
                .transfer_id = transfer_id,
                .timestamp = timestamp,
                .symbols = symbols,
            },
        });
    }

    pub fn submitAddSymbolsBinaryAsync(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
        symbols: []const common.CoreSymbolSpecification,
    ) SubmitCommandAsyncError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncResultCode(.{
            .binary_add_symbols = .{
                .transfer_id = transfer_id,
                .timestamp = timestamp,
                .symbols = symbols,
            },
        });
    }

    pub fn submitAddAccountsBinary(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
        users: []const BinaryUserAccountsBatch,
    ) SubmitCommandError!void {
        try self.submitCommand(.{
            .binary_add_accounts = .{
                .transfer_id = transfer_id,
                .timestamp = timestamp,
                .users = users,
            },
        });
    }

    pub fn submitAddAccountsBinaryAsync(
        self: *ExchangeApi,
        transfer_id: i32,
        timestamp: i64,
        users: []const BinaryUserAccountsBatch,
    ) SubmitCommandAsyncError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncResultCode(.{
            .binary_add_accounts = .{
                .transfer_id = transfer_id,
                .timestamp = timestamp,
                .users = users,
            },
        });
    }

    pub fn startup(self: *ExchangeApi) LifecycleError!void {
        return self.core.startup();
    }

    pub fn shutdown(self: *ExchangeApi) LifecycleError!void {
        return self.core.shutdown();
    }

    pub fn bootstrapFromPersistence(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        snapshot_path: ?[]const u8,
        journal_path: ?[]const u8,
    ) BootstrapError!BootstrapOutcome {
        return self.core.bootstrapFromPersistence(dir, snapshot_path, journal_path);
    }

    pub fn startupFromPersistence(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        snapshot_path: ?[]const u8,
        journal_path: ?[]const u8,
    ) StartupError!BootstrapOutcome {
        return self.core.startupFromPersistence(dir, snapshot_path, journal_path);
    }

    pub fn startupFromConfiguredPersistence(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromConfiguredPersistenceError!BootstrapOutcome {
        return self.core.startupFromConfiguredPersistence(
            dir,
            snapshot_module_type,
            snapshot_instance_id,
            journal_partition_id,
        );
    }

    pub fn writeJournalToFile(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        path: []const u8,
    ) JournalApiError!void {
        return self.core.writeJournalToFile(dir, path);
    }

    pub fn loadJournalFromFile(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        path: []const u8,
    ) JournalApiError!void {
        return self.core.loadJournalFromFile(dir, path);
    }

    pub fn replayJournalFromFile(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        path: []const u8,
    ) JournalReplayError!usize {
        return self.core.replayJournalFromFile(dir, path);
    }

    pub fn captureSnapshotToFile(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        path: []const u8,
        symbols: []const i32,
        depth: usize,
    ) SnapshotWriteError!void {
        return self.core.captureSnapshotToFile(
            dir,
            path,
            symbols,
            depth,
        );
    }

    pub fn validateSnapshotFile(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        path: []const u8,
    ) SnapshotValidateError!void {
        return self.core.validateSnapshotFile(dir, path);
    }

    pub fn writeJournalToDefaultPath(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        snapshot_id: u64,
        partition_id: u16,
    ) DefaultJournalApiError!void {
        return self.core.writeJournalToDefaultPath(
            dir,
            snapshot_id,
            partition_id,
        );
    }

    pub fn loadJournalFromDefaultPath(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        snapshot_id: u64,
        partition_id: u16,
    ) DefaultJournalApiError!void {
        return self.core.loadJournalFromDefaultPath(
            dir,
            snapshot_id,
            partition_id,
        );
    }

    pub fn replayJournalFromDefaultPath(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        snapshot_id: u64,
        partition_id: u16,
    ) DefaultJournalReplayError!usize {
        return self.core.replayJournalFromDefaultPath(
            dir,
            snapshot_id,
            partition_id,
        );
    }

    pub fn replayJournalFromDefaultPathRange(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        snapshot_id: u64,
        partition_id: u16,
    ) DefaultJournalReplayRangeError!usize {
        return self.core.replayJournalFromDefaultPathRange(
            dir,
            snapshot_id,
            partition_id,
        );
    }

    pub fn captureSnapshotToDefaultPath(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
        symbols: []const i32,
        depth: usize,
    ) DefaultSnapshotWriteError!void {
        return self.core.captureSnapshotToDefaultPath(
            dir,
            snapshot_id,
            module_type,
            instance_id,
            symbols,
            depth,
        );
    }

    pub fn validateSnapshotDefaultPath(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) DefaultSnapshotValidateError!void {
        return self.core.validateSnapshotDefaultPath(
            dir,
            snapshot_id,
            module_type,
            instance_id,
        );
    }

    pub fn persistStateWithDefaultMainLog(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        snapshot_id: i64,
        timestamp_ns: i64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) PersistStateWithMainLogError!cmd.CommandResultCode {
        return self.core.persistStateWithDefaultMainLog(
            dir,
            snapshot_id,
            timestamp_ns,
            module_type,
            instance_id,
        );
    }

    pub fn startupFromDiscoveredConfiguredPersistence(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromDiscoveredConfiguredPersistenceError!BootstrapOutcome {
        return self.core.startupFromDiscoveredConfiguredPersistence(
            dir,
            snapshot_module_type,
            snapshot_instance_id,
            journal_partition_id,
        );
    }

    pub fn startupFromDiscoveredConfiguredPersistenceStrictMainLog(
        self: *ExchangeApi,
        dir: std.fs.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromDiscoveredConfiguredPersistenceStrictError!BootstrapOutcome {
        return self.core.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
            dir,
            snapshot_module_type,
            snapshot_instance_id,
            journal_partition_id,
        );
    }

    pub fn persistenceSnapshotCount(self: *const ExchangeApi) usize {
        return self.core.persistenceSnapshotCount();
    }

    pub fn persistenceJournalCount(self: *const ExchangeApi) usize {
        return self.core.persistenceJournalCount();
    }

    pub fn persistenceCurrentSnapshot(self: *const ExchangeApi) *const SnapshotDescriptor {
        return self.core.persistenceCurrentSnapshot();
    }

    pub fn persistenceCurrentSnapshotRef(self: *const ExchangeApi) SnapshotRef {
        return self.core.persistenceCurrentSnapshotRef();
    }

    pub fn persistenceJournalByRef(self: *const ExchangeApi, journal_ref: JournalRef) *const JournalDescriptor {
        return self.core.persistenceJournalByRef(journal_ref);
    }

    pub fn persistenceFindJournalForSequence(self: *const ExchangeApi, sequence: u64) ?JournalRef {
        return self.core.persistenceFindJournalForSequence(sequence);
    }

    pub fn persistenceFindSnapshotById(self: *const ExchangeApi, snapshot_id: u64) ?SnapshotRef {
        return self.core.persistenceFindSnapshotById(snapshot_id);
    }

    pub fn persistenceSnapshotById(self: *const ExchangeApi, snapshot_id: u64) ?*const SnapshotDescriptor {
        return self.core.persistenceSnapshotById(snapshot_id);
    }

    pub fn snapshotExistsDefaultPath(
        self: *const ExchangeApi,
        dir: std.fs.Dir,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) DefaultSnapshotExistsError!bool {
        return self.core.snapshotExistsDefaultPath(
            dir,
            snapshot_id,
            module_type,
            instance_id,
        );
    }

    pub fn snapshotExistsFile(
        self: *const ExchangeApi,
        dir: std.fs.Dir,
        path: []const u8,
    ) SnapshotExistsError!bool {
        return self.core.snapshotExistsFile(dir, path);
    }

    pub fn createUser(
        self: *ExchangeApi,
        service_flags: i32,
        events_group: i64,
        timestamp_ns: i64,
        user_id: i64,
    ) SubmitCommandError!void {
        try self.submitRawCommand(.{
            .command = .add_user,
            .order_id = -1,
            .symbol = -1,
            .uid = user_id,
            .timestamp = timestamp_ns,
            .events_group = events_group,
            .service_flags = service_flags,
            .result_code = .new,
        });
    }

    pub fn suspendUser(
        self: *ExchangeApi,
        service_flags: i32,
        events_group: i64,
        timestamp_ns: i64,
        user_id: i64,
    ) SubmitCommandError!void {
        try self.submitRawCommand(.{
            .command = .suspend_user,
            .order_id = -1,
            .symbol = -1,
            .uid = user_id,
            .timestamp = timestamp_ns,
            .events_group = events_group,
            .service_flags = service_flags,
            .result_code = .new,
        });
    }

    pub fn resumeUser(
        self: *ExchangeApi,
        service_flags: i32,
        events_group: i64,
        timestamp_ns: i64,
        user_id: i64,
    ) SubmitCommandError!void {
        try self.submitRawCommand(.{
            .command = .resume_user,
            .order_id = -1,
            .symbol = -1,
            .uid = user_id,
            .timestamp = timestamp_ns,
            .events_group = events_group,
            .service_flags = service_flags,
            .result_code = .new,
        });
    }

    pub fn balanceAdjustment(
        self: *ExchangeApi,
        service_flags: i32,
        events_group: i64,
        timestamp_ns: i64,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
    ) SubmitCommandError!void {
        try self.submitRawCommand(.{
            .command = .balance_adjustment,
            .order_id = transaction_id,
            .symbol = currency,
            .uid = uid,
            .price = amount,
            .size = 0,
            .order_type = switch (adjustment_type) {
                .adjustment => .gtc,
                .@"suspend" => .ioc,
            },
            .timestamp = timestamp_ns,
            .events_group = events_group,
            .service_flags = service_flags,
            .result_code = .new,
        });
    }

    pub fn placeNewOrder(
        self: *ExchangeApi,
        service_flags: i32,
        events_group: i64,
        timestamp_ns: i64,
        order_id: i64,
        user_cookie: i32,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: common.OrderAction,
        order_type: common.OrderType,
        symbol: i32,
        uid: i64,
    ) SubmitCommandError!void {
        try self.submitRawCommand(.{
            .command = .place_order,
            .order_id = order_id,
            .user_cookie = user_cookie,
            .price = price,
            .reserve_bid_price = reserve_bid_price,
            .size = size,
            .action = action,
            .order_type = order_type,
            .symbol = symbol,
            .uid = uid,
            .timestamp = timestamp_ns,
            .events_group = events_group,
            .service_flags = service_flags,
            .result_code = .new,
        });
    }

    pub fn moveOrder(
        self: *ExchangeApi,
        service_flags: i32,
        events_group: i64,
        timestamp_ns: i64,
        price: i64,
        order_id: i64,
        symbol: i32,
        uid: i64,
    ) SubmitCommandError!void {
        try self.submitRawCommand(.{
            .command = .move_order,
            .price = price,
            .order_id = order_id,
            .symbol = symbol,
            .uid = uid,
            .timestamp = timestamp_ns,
            .events_group = events_group,
            .service_flags = service_flags,
            .result_code = .new,
        });
    }

    pub fn cancelOrder(
        self: *ExchangeApi,
        service_flags: i32,
        events_group: i64,
        timestamp_ns: i64,
        order_id: i64,
        symbol: i32,
        uid: i64,
    ) SubmitCommandError!void {
        try self.submitRawCommand(.{
            .command = .cancel_order,
            .order_id = order_id,
            .symbol = symbol,
            .uid = uid,
            .timestamp = timestamp_ns,
            .events_group = events_group,
            .service_flags = service_flags,
            .result_code = .new,
        });
    }

    pub fn reduceOrder(
        self: *ExchangeApi,
        service_flags: i32,
        events_group: i64,
        timestamp_ns: i64,
        reduce_size: i64,
        order_id: i64,
        symbol: i32,
        uid: i64,
    ) SubmitCommandError!void {
        try self.submitRawCommand(.{
            .command = .reduce_order,
            .size = reduce_size,
            .order_id = order_id,
            .symbol = symbol,
            .uid = uid,
            .timestamp = timestamp_ns,
            .events_group = events_group,
            .service_flags = service_flags,
            .result_code = .new,
        });
    }

    pub fn binaryData(
        self: *ExchangeApi,
        service_flags: i32,
        events_group: i64,
        timestamp_ns: i64,
        last_flag: i32,
        word0: i64,
        word1: i64,
        word2: i64,
        word3: i64,
        word4: i64,
    ) SubmitCommandError!void {
        try self.submitRawCommand(.{
            .command = .binary_data_command,
            .symbol = last_flag,
            .order_id = word0,
            .price = word1,
            .reserve_bid_price = word2,
            .size = word3,
            .uid = word4,
            .timestamp = timestamp_ns,
            .events_group = events_group,
            .service_flags = service_flags,
            .result_code = .new,
        });
    }

    pub fn groupingControl(
        self: *ExchangeApi,
        timestamp_ns: i64,
        mode: i64,
    ) SubmitCommandError!void {
        try self.submitRawCommand(.{
            .command = .grouping_control,
            .order_id = mode,
            .timestamp = timestamp_ns,
            .result_code = .new,
        });
    }

    pub fn reset(
        self: *ExchangeApi,
        timestamp_ns: i64,
    ) SubmitCommandError!void {
        try self.submitRawCommand(.{
            .command = .reset,
            .timestamp = timestamp_ns,
            .result_code = .new,
        });
    }

    pub fn nop(
        self: *ExchangeApi,
        timestamp_ns: i64,
    ) SubmitCommandError!void {
        try self.submitRawCommand(.{
            .command = .nop,
            .timestamp = timestamp_ns,
            .result_code = .new,
        });
    }

    fn submitRawCommand(
        self: *ExchangeApi,
        command: cmd.OrderCommand,
    ) SubmitCommandError!void {
        try self.submitCommand(.{
            .raw_command = command,
        });
    }

    fn registerCallbackForHandle(
        self: *ExchangeApi,
        handle: ApiCommandAsyncHandle,
        callback: CommandResponseCallback,
    ) CallbackRegistrationError!void {
        const request_id = try requestIdFromHandle(handle);
        if (self.callbackRegistrationIndex(request_id)) |idx| {
            self.callback_registrations[idx].callback = callback;
            return;
        }
        if (self.callback_registrations_len >= MaxCallbackRegistrations) {
            return error.CallbackRegistryFull;
        }
        self.callback_registrations[self.callback_registrations_len] = .{
            .request_id = request_id,
            .callback = callback,
        };
        self.callback_registrations_len += 1;
    }

    fn callbackRegistrationIndex(
        self: *const ExchangeApi,
        request_id: u64,
    ) ?usize {
        for (self.callback_registrations[0..self.callback_registrations_len], 0..) |registration, idx| {
            if (registration.request_id == request_id) {
                return idx;
            }
        }
        return null;
    }

    fn removeCallbackRegistrationAt(self: *ExchangeApi, idx: usize) void {
        std.debug.assert(idx < self.callback_registrations_len);
        const len = self.callback_registrations_len;
        if (idx + 1 < len) {
            std.mem.copyForwards(
                CallbackRegistration,
                self.callback_registrations[idx .. len - 1],
                self.callback_registrations[idx + 1 .. len],
            );
        }
        self.callback_registrations_len = len - 1;
    }

    fn submitCommandAsyncWithCallback(
        self: *ExchangeApi,
        command: ApiCommand,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        const handle = try self.submitCommandAsyncFullResponse(command);
        try self.registerResultCallback(handle, callback);
    }
};

fn requestIdFromHandle(handle: ApiCommandAsyncHandle) error{UnsupportedPersistStateHandle}!u64 {
    return switch (handle) {
        .request_id => |request_id| request_id,
        .persist_state => error.UnsupportedPersistStateHandle,
    };
}

fn supportsAsyncFullResponse(command: ApiCommand) bool {
    return switch (command) {
        .persist_state,
        .binary_add_symbols,
        .binary_add_accounts,
        => false,
        else => true,
    };
}

fn makeExchangeSpecForSymbol(symbol_id: i32) !common.CoreSymbolSpecification {
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

const CallbackCollector = struct {
    count: usize = 0,
    last_result: ?cmd.CommandResultCode = null,
    last_sequence: u64 = 0,
    last_snapshot_present: bool = false,
    last_ask_levels: usize = 0,
    last_bid_levels: usize = 0,

    fn onResponse(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
        const self: *CallbackCollector = @ptrCast(@alignCast(context.?));
        self.count += 1;
        self.last_result = response.result_code;
        self.last_sequence = response.sequence;
        if (response.l2_snapshot) |snapshot| {
            self.last_snapshot_present = true;
            self.last_ask_levels = snapshot.asks.items.len;
            self.last_bid_levels = snapshot.bids.items.len;
        } else {
            self.last_snapshot_present = false;
            self.last_ask_levels = 0;
            self.last_bid_levels = 0;
        }
    }
};

const AsyncResponseTrace = struct {
    result: cmd.CommandResultCode,
    snapshot_present: bool,
    ask_levels: usize,
    bid_levels: usize,
};

fn expectTraceEqual(expected: AsyncResponseTrace, actual: AsyncResponseTrace) !void {
    try std.testing.expectEqual(expected.result, actual.result);
    try std.testing.expectEqual(expected.snapshot_present, actual.snapshot_present);
    try std.testing.expectEqual(expected.ask_levels, actual.ask_levels);
    try std.testing.expectEqual(expected.bid_levels, actual.bid_levels);
}

const ResponseDigest = struct {
    const ReportKind = enum {
        none,
        state_hash_detailed,
        single_user,
        total_currency_balance,
        other,
    };

    result: cmd.CommandResultCode,
    snapshot_present: bool,
    ask_levels: usize,
    bid_levels: usize,
    report_kind: ReportKind = .none,
    state_hash_aggregate: u64 = 0,
    state_hash_submodule_count: usize = 0,
    single_user_uid: i64 = 0,
    single_user_status: kernel_mod.SingleUserQueryExecutionStatus = .ok,
    single_user_accounts_count: usize = 0,
    single_user_positions_count: usize = 0,
    single_user_orders_count: usize = 0,
    total_accounts_11: i64 = 0,
    total_accounts_15: i64 = 0,
    total_orders_11: i64 = 0,
    total_orders_15: i64 = 0,
    total_fees_11: i64 = 0,
    total_fees_15: i64 = 0,
};

fn mapGetOrZero(map: *const std.AutoHashMapUnmanaged(i32, i64), key: i32) i64 {
    return map.get(key) orelse 0;
}

fn digestFromResponse(response: *const kernel_mod.CommandResponse) ResponseDigest {
    var digest: ResponseDigest = .{
        .result = response.result_code,
        .snapshot_present = response.l2_snapshot != null,
        .ask_levels = 0,
        .bid_levels = 0,
    };
    if (response.l2_snapshot) |snapshot| {
        digest.ask_levels = snapshot.asks.items.len;
        digest.bid_levels = snapshot.bids.items.len;
    }

    if (response.report_response) |report| {
        switch (report) {
            .state_hash_detailed => |state| {
                digest.report_kind = .state_hash_detailed;
                digest.state_hash_aggregate = state.aggregate_hash;
                digest.state_hash_submodule_count = state.submodules.items.len;
            },
            .single_user => |single| {
                digest.report_kind = .single_user;
                digest.single_user_uid = single.uid;
                digest.single_user_status = single.query_execution_status;
                digest.single_user_accounts_count = single.accounts.count();
                digest.single_user_positions_count = single.positions.count();
                digest.single_user_orders_count = single.orders.items.len;
            },
            .total_currency_balance => |totals| {
                digest.report_kind = .total_currency_balance;
                digest.total_accounts_11 = mapGetOrZero(&totals.account_balances, 11);
                digest.total_accounts_15 = mapGetOrZero(&totals.account_balances, 15);
                digest.total_orders_11 = mapGetOrZero(&totals.orders_balances, 11);
                digest.total_orders_15 = mapGetOrZero(&totals.orders_balances, 15);
                digest.total_fees_11 = mapGetOrZero(&totals.fees, 11);
                digest.total_fees_15 = mapGetOrZero(&totals.fees, 15);
            },
            else => digest.report_kind = .other,
        }
    }
    return digest;
}

fn expectDigestEqual(expected: ResponseDigest, actual: ResponseDigest) !void {
    try std.testing.expectEqual(expected.result, actual.result);
    try std.testing.expectEqual(expected.snapshot_present, actual.snapshot_present);
    try std.testing.expectEqual(expected.ask_levels, actual.ask_levels);
    try std.testing.expectEqual(expected.bid_levels, actual.bid_levels);
    try std.testing.expectEqual(expected.report_kind, actual.report_kind);
    try std.testing.expectEqual(expected.state_hash_aggregate, actual.state_hash_aggregate);
    try std.testing.expectEqual(expected.state_hash_submodule_count, actual.state_hash_submodule_count);
    try std.testing.expectEqual(expected.single_user_uid, actual.single_user_uid);
    try std.testing.expectEqual(expected.single_user_status, actual.single_user_status);
    try std.testing.expectEqual(expected.single_user_accounts_count, actual.single_user_accounts_count);
    try std.testing.expectEqual(expected.single_user_positions_count, actual.single_user_positions_count);
    try std.testing.expectEqual(expected.single_user_orders_count, actual.single_user_orders_count);
    try std.testing.expectEqual(expected.total_accounts_11, actual.total_accounts_11);
    try std.testing.expectEqual(expected.total_accounts_15, actual.total_accounts_15);
    try std.testing.expectEqual(expected.total_orders_11, actual.total_orders_11);
    try std.testing.expectEqual(expected.total_orders_15, actual.total_orders_15);
    try std.testing.expectEqual(expected.total_fees_11, actual.total_fees_11);
    try std.testing.expectEqual(expected.total_fees_15, actual.total_fees_15);
}

fn traceFromResponse(response: *const kernel_mod.CommandResponse) AsyncResponseTrace {
    if (response.l2_snapshot) |snapshot| {
        return .{
            .result = response.result_code,
            .snapshot_present = true,
            .ask_levels = snapshot.asks.items.len,
            .bid_levels = snapshot.bids.items.len,
        };
    }
    return .{
        .result = response.result_code,
        .snapshot_present = false,
        .ask_levels = 0,
        .bid_levels = 0,
    };
}

const TraceCollector = struct {
    count: usize = 0,
    traces: [64]AsyncResponseTrace = undefined,

    fn onResponse(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
        const self: *TraceCollector = @ptrCast(@alignCast(context.?));
        std.debug.assert(self.count < self.traces.len);
        self.traces[self.count] = traceFromResponse(response);
        self.count += 1;
    }
};

const CountCollector = struct {
    count: usize = 0,

    fn onResponse(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *CountCollector = @ptrCast(@alignCast(context.?));
        self.count += 1;
    }
};

const DigestCollector = struct {
    count: usize = 0,
    digests: [64]ResponseDigest = undefined,

    fn onResponse(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
        const self: *DigestCollector = @ptrCast(@alignCast(context.?));
        std.debug.assert(self.count < self.digests.len);
        self.digests[self.count] = digestFromResponse(response);
        self.count += 1;
    }
};

const DeterministicRng = struct {
    state: u64,

    fn init(seed: u64) DeterministicRng {
        return .{ .state = seed };
    }

    fn next(self: *DeterministicRng) u64 {
        var x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        return x *% 0x2545F4914F6CDD1D;
    }

    fn bounded(self: *DeterministicRng, max_exclusive: u64) u64 {
        std.debug.assert(max_exclusive > 0);
        return self.next() % max_exclusive;
    }
};

test "exchange api sync path supports submitCommand and list sync semantics" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);

    try api.submitCommand(.{ .add_user = 95_001 });

    const list_commands = [_]ApiCommand{
        .{
            .adjust_user_balance = .{
                .uid = 95_001,
                .transaction_id = 1,
                .currency = 15,
                .amount = 42_000,
                .adjustment_type = .adjustment,
            },
        },
    };
    try api.submitCommandsSync(list_commands[0..]);

    const empty: [0]ApiCommand = .{};
    try api.submitCommandsSync(empty[0..]);

    var balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 95_001,
            .currency = 15,
        },
    });
    defer balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 42_000), balance.user_balance);
}

test "exchange api core-style alias wrappers mirror submit/async helper behavior" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    try api.submitApiCommand(.{ .add_user = 95_210 });

    const list_commands = [_]ApiCommand{
        .{
            .adjust_user_balance = .{
                .uid = 95_210,
                .transaction_id = 1,
                .currency = 15,
                .amount = 7_000,
                .adjustment_type = .adjustment,
            },
        },
    };
    try api.submitApiCommandsSync(list_commands[0..]);

    var flush = try api.submitApiCommandsSyncWithNop(&.{.{ .add_user = 95_211 }});
    defer flush.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, flush.result_code);

    const handle = try api.submitApiCommandAsync(.{ .add_user = 95_212 });
    try std.testing.expect(!api.isApiCommandAsyncResultReady(handle));
    _ = try api.drainPending();
    try std.testing.expect(api.isApiCommandAsyncResultReady(handle));

    var async_result = api.takeApiCommandAsyncResult(handle) orelse return error.MissingAsyncResult;
    defer async_result.deinit(allocator);
    switch (async_result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }
}

test "exchange api persist-state pair wrappers expose readiness and result consumption" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try api.persistState(200_101, 9_200_101_111),
    );

    const requests = try api.persistStateAsync(200_102, 9_200_101_222);
    try std.testing.expect(!api.isPersistStateResultReady(requests));
    _ = try api.drainPending();
    try std.testing.expect(api.isPersistStateResultReady(requests));
    try std.testing.expectEqual(
        @as(?cmd.CommandResultCode, .success),
        api.takePersistStateResult(requests),
    );
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, null), api.takePersistStateResult(requests));
}

test "exchange api low-level core wrappers forward request-id async and batch flows" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    try std.testing.expectEqual(core.kernelRef(), api.kernelRef());
    try std.testing.expectEqualStrings(core.configRef().initial_state.exchange_id, api.configRef().initial_state.exchange_id);

    var add_sync = try api.addUser(95_301);
    defer add_sync.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_sync.result_code);

    var adjust_sync = try api.adjustUserBalance(95_301, 1, 15, 10_000, .adjustment);
    defer adjust_sync.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, adjust_sync.result_code);

    const request_id = try api.addUserAsync(95_302);
    try std.testing.expect(!api.isAsyncResultReady(request_id));
    try std.testing.expectEqual(@as(usize, 1), try api.drainAsyncPending());
    try std.testing.expect(api.isAsyncResultReady(request_id));

    var async_response = api.takeAsyncResult(request_id) orelse return error.MissingAsyncResult;
    defer async_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, async_response.result_code);

    var c1: cmd.OrderCommand = .{
        .command = .add_user,
        .order_id = -1,
        .symbol = -1,
        .uid = 95_303,
        .result_code = .new,
    };
    var c2: cmd.OrderCommand = .{
        .command = .add_user,
        .order_id = -1,
        .symbol = -1,
        .uid = 95_303,
        .result_code = .new,
    };
    var batch_commands = [_]*cmd.OrderCommand{ &c1, &c2 };
    const batch = try api.submitCommandBatch(batch_commands[0..]);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, batch.result_code);
    try std.testing.expect(batch.sequence_from != null);
    try std.testing.expect(batch.sequence_to != null);

    var order_book = try api.requestOrderBook(9_999_999, 8);
    defer order_book.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.matching_invalid_order_book_id, order_book.result_code);
}

test "exchange api low-level binary payload wrappers and result sink forwarding" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const Collector = struct {
        count: usize = 0,
        last_command: ?cmd.OrderCommandType = null,
        last_result: ?cmd.CommandResultCode = null,

        fn onResult(context: ?*anyopaque, event: runtime_mod.ResultEvent) void {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            self.count += 1;
            self.last_command = event.command;
            self.last_result = event.result_code;
        }
    };

    var api = ExchangeApi.init(allocator, &core);
    var collector: Collector = .{};
    api.setResultSink(.{
        .context = &collector,
        .on_result = Collector.onResult,
    });

    var user_response = try api.addUser(95_311);
    defer user_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, user_response.result_code);
    try std.testing.expectEqual(@as(usize, 1), collector.count);
    try std.testing.expectEqual(@as(?cmd.OrderCommandType, .add_user), collector.last_command);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), collector.last_result);

    const payload = try binary_payload.encodeReportQueryToBytes(allocator, .state_hash);
    defer allocator.free(payload);

    var sync_response = try api.submitBinaryPayload(.binary_data_query, 4_801, 9_301_100_001, payload);
    defer sync_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, sync_response.result_code);
    try std.testing.expect(sync_response.report_response != null);

    const async_request_id = try api.submitBinaryPayloadAsync(.binary_data_query, 4_802, 9_301_100_002, payload);
    _ = try api.drainAsyncPending();
    var async_payload_response = api.takeAsyncResult(async_request_id) orelse return error.MissingAsyncResult;
    defer async_payload_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, async_payload_response.result_code);
    try std.testing.expect(async_payload_response.report_response != null);

    var query_response = try api.submitBinaryReportQuery(4_803, 9_301_100_003, .state_hash);
    defer query_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, query_response.result_code);
    try std.testing.expect(query_response.report_response != null);
}

test "exchange api async handle flow drains and returns result payload" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const handle = try api.submitCommandAsync(.{ .add_user = 95_101 });
    try std.testing.expect(!api.isResultReady(handle));

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 1), processed);
    try std.testing.expect(api.isResultReady(handle));

    var result = api.takeResult(handle) orelse return error.MissingAsyncResult;
    defer result.deinit(allocator);
    switch (result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }
}

test "exchange api one-shot command result-code helper returns consumed result" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try api.processCommandResultCode(.{ .add_user = 95_102 }),
    );
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_already_exists,
        try api.processCommandResultCode(.{ .add_user = 95_102 }),
    );
}

test "exchange api one-shot full-response helper returns consumed response and preserves persist-state guard" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    var response = try api.processCommandFullResponse(.{ .add_user = 95_103 });
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    try std.testing.expectError(
        error.UnsupportedAsyncFullResponseCommand,
        api.processCommandFullResponse(.{
            .persist_state = .{
                .snapshot_id = 123_456,
                .timestamp_ns = 9_500_100_001,
            },
        }),
    );
}

test "exchange api seeded simulation keeps one-shot command helpers equivalent to explicit async flow" {
    const allocator = std.testing.allocator;
    const rounds: usize = 16;
    const ops_per_round: usize = 32;
    const user_ccy_base: i32 = 11;
    const user_ccy_quote: i32 = 15;

    var rng = DeterministicRng.init(0x91B00B5EED5EED55);

    for (0..rounds) |round_idx| {
        const symbol = 11_100 + @as(i32, @intCast(round_idx));
        const uid_maker = 120_000 + @as(i64, @intCast(round_idx)) * 16;
        const uid_taker = uid_maker + 1;
        const uid_probe = uid_maker + 2;
        const users = [_]i64{ uid_maker, uid_taker, uid_probe };
        var next_tx_id = [_]i64{ 3, 3, 1 };
        var next_order_id: i64 = 1_500_000 + @as(i64, @intCast(round_idx)) * 1_000;
        const timestamp_base: i64 = 21_000_000_000 + @as(i64, @intCast(round_idx)) * 500;

        const spec = try makeExchangeSpecForSymbol(symbol);

        var baseline_core = try exchange_core_mod.ExchangeCore.init(allocator, .{
            .matching = .{
                .orderbook_kind = .direct,
            },
        });
        defer baseline_core.deinit();
        var baseline_api = ExchangeApi.init(allocator, &baseline_core);

        var helper_core = try exchange_core_mod.ExchangeCore.init(allocator, .{
            .matching = .{
                .orderbook_kind = .direct,
            },
        });
        defer helper_core.deinit();
        var helper_api = ExchangeApi.init(allocator, &helper_core);

        try baseline_api.submitAddSymbolsBinary(30_000 + @as(i32, @intCast(round_idx)), timestamp_base + 1, &.{spec});
        try helper_api.submitAddSymbolsBinary(31_000 + @as(i32, @intCast(round_idx)), timestamp_base + 2, &.{spec});

        try baseline_api.submitCommand(.{ .add_user = uid_maker });
        try helper_api.submitCommand(.{ .add_user = uid_maker });
        try baseline_api.submitCommand(.{ .add_user = uid_taker });
        try helper_api.submitCommand(.{ .add_user = uid_taker });

        try baseline_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid_maker,
                .transaction_id = 1,
                .currency = user_ccy_quote,
                .amount = 2_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try helper_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid_maker,
                .transaction_id = 1,
                .currency = user_ccy_quote,
                .amount = 2_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try baseline_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid_maker,
                .transaction_id = 2,
                .currency = user_ccy_base,
                .amount = 2_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try helper_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid_maker,
                .transaction_id = 2,
                .currency = user_ccy_base,
                .amount = 2_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try baseline_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid_taker,
                .transaction_id = 1,
                .currency = user_ccy_quote,
                .amount = 2_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try helper_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid_taker,
                .transaction_id = 1,
                .currency = user_ccy_quote,
                .amount = 2_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try baseline_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid_taker,
                .transaction_id = 2,
                .currency = user_ccy_base,
                .amount = 2_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try helper_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid_taker,
                .transaction_id = 2,
                .currency = user_ccy_base,
                .amount = 2_000_000,
                .adjustment_type = .adjustment,
            },
        });

        for (0..ops_per_round) |op_idx| {
            var command: ApiCommand = undefined;
            var supports_full_response = true;

            switch (rng.bounded(10)) {
                0 => {
                    const user_idx: usize = @intCast(rng.bounded(users.len));
                    command = .{ .add_user = users[user_idx] };
                },
                1 => {
                    const user_idx: usize = @intCast(rng.bounded(users.len));
                    const user_uid = users[user_idx];
                    const tx_id = next_tx_id[user_idx];
                    next_tx_id[user_idx] += 1;
                    const currency = if ((rng.next() & 1) == 0) user_ccy_quote else user_ccy_base;
                    const amount = 1 + @as(i64, @intCast(rng.bounded(2_000)));
                    command = .{
                        .adjust_user_balance = .{
                            .uid = user_uid,
                            .transaction_id = tx_id,
                            .currency = currency,
                            .amount = amount,
                            .adjustment_type = .adjustment,
                        },
                    };
                },
                2 => {
                    next_order_id += 1;
                    const order_id = next_order_id;
                    const uid = if ((rng.next() & 1) == 0) uid_maker else uid_taker;
                    const price = 90 + @as(i64, @intCast(rng.bounded(31)));
                    const reserve_bid_price = price + 8;
                    const size = 1 + @as(i64, @intCast(rng.bounded(12)));
                    const action: common.OrderAction = if ((rng.next() & 1) == 0) .bid else .ask;
                    const user_cookie: i32 = @intCast(rng.bounded(512));
                    command = .{
                        .place_order = .{
                            .order_id = order_id,
                            .uid = uid,
                            .symbol = symbol,
                            .price = price,
                            .reserve_bid_price = reserve_bid_price,
                            .size = size,
                            .action = action,
                            .order_type = .gtc,
                            .user_cookie = user_cookie,
                        },
                    };
                },
                3 => {
                    const uid = if ((rng.next() & 1) == 0) uid_maker else uid_taker;
                    const offset = @as(i64, @intCast(rng.bounded(6)));
                    const order_id = if ((rng.next() & 3) == 0) next_order_id + 100 + offset else next_order_id - offset;
                    const price = 90 + @as(i64, @intCast(rng.bounded(31)));
                    command = .{
                        .move_order = .{
                            .order_id = order_id,
                            .uid = uid,
                            .symbol = symbol,
                            .new_price = price,
                        },
                    };
                },
                4 => {
                    const uid = if ((rng.next() & 1) == 0) uid_maker else uid_taker;
                    const offset = @as(i64, @intCast(rng.bounded(6)));
                    const order_id = if ((rng.next() & 3) == 0) next_order_id + 100 + offset else next_order_id - offset;
                    const reduce_size = 1 + @as(i64, @intCast(rng.bounded(8)));
                    command = .{
                        .reduce_order = .{
                            .order_id = order_id,
                            .uid = uid,
                            .symbol = symbol,
                            .reduce_size = reduce_size,
                        },
                    };
                },
                5 => {
                    const uid = if ((rng.next() & 1) == 0) uid_maker else uid_taker;
                    const offset = @as(i64, @intCast(rng.bounded(6)));
                    const order_id = if ((rng.next() & 3) == 0) next_order_id + 100 + offset else next_order_id - offset;
                    command = .{
                        .cancel_order = .{
                            .order_id = order_id,
                            .uid = uid,
                            .symbol = symbol,
                        },
                    };
                },
                6 => {
                    const depth = 1 + @as(usize, @intCast(rng.bounded(16)));
                    command = .{
                        .request_order_book = .{
                            .symbol = symbol,
                            .depth = depth,
                        },
                    };
                },
                7 => {
                    const user_idx: usize = @intCast(rng.bounded(users.len));
                    command = .{ .suspend_user = users[user_idx] };
                },
                8 => {
                    const user_idx: usize = @intCast(rng.bounded(users.len));
                    command = .{ .resume_user = users[user_idx] };
                },
                else => {
                    supports_full_response = false;
                    command = .{
                        .persist_state = .{
                            .snapshot_id = 900_000 + @as(i64, @intCast(round_idx * ops_per_round + op_idx)),
                            .timestamp_ns = timestamp_base + 10_000 + @as(i64, @intCast(op_idx)),
                        },
                    };
                },
            }

            const use_full_response = supports_full_response and ((rng.next() & 1) == 0);
            if (use_full_response) {
                var helper_response = try helper_api.processCommandFullResponse(command);
                defer helper_response.deinit(allocator);
                const helper_digest = digestFromResponse(&helper_response);

                const baseline_handle = try baseline_api.submitCommandAsyncFullResponse(command);
                try std.testing.expectEqual(@as(usize, 1), try baseline_api.drainPending());
                var baseline_response = (try baseline_api.takeFullResponse(baseline_handle)) orelse return error.MissingAsyncResult;
                defer baseline_response.deinit(allocator);

                try expectDigestEqual(digestFromResponse(&baseline_response), helper_digest);
            } else {
                const helper_code = try helper_api.processCommandResultCode(command);

                const baseline_handle = try baseline_api.submitCommandAsyncResultCode(command);
                const expected_processed: usize = switch (command) {
                    .persist_state => 2,
                    else => 1,
                };
                try std.testing.expectEqual(expected_processed, try baseline_api.drainPending());
                const baseline_code = baseline_api.takeResultCode(baseline_handle) orelse return error.MissingAsyncResult;

                try std.testing.expectEqual(baseline_code, helper_code);
            }

            try std.testing.expectEqual(@as(usize, 0), try baseline_api.drainPending());
            try std.testing.expectEqual(@as(usize, 0), try helper_api.drainPending());

            var baseline_hash = try baseline_api.queryReport(.state_hash_detailed);
            defer baseline_hash.deinit(allocator);
            var helper_hash = try helper_api.queryReport(.state_hash_detailed);
            defer helper_hash.deinit(allocator);

            switch (baseline_hash) {
                .state_hash_detailed => |baseline_detailed| switch (helper_hash) {
                    .state_hash_detailed => |helper_detailed| {
                        try std.testing.expectEqual(baseline_detailed.aggregate_hash, helper_detailed.aggregate_hash);
                        try std.testing.expectEqual(
                            baseline_detailed.submodules.items.len,
                            helper_detailed.submodules.items.len,
                        );
                    },
                    else => return error.UnexpectedReportResponseType,
                },
                else => return error.UnexpectedReportResponseType,
            }
        }
    }
}

test "exchange api callback-style async convenience wrappers support user lifecycle" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const create = try api.submitCreateUserAsync(95_111);
    const suspend_handle = try api.submitSuspendUserAsync(95_111);
    const resume_handle = try api.submitResumeUserAsync(95_111);

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 3), processed);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(create));
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(suspend_handle));
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(resume_handle));
}

test "exchange api callback-style async convenience wrappers support order lifecycle" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const spec = try makeExchangeSpecForSymbol(9_351);
    try api.submitAddSymbolsBinary(5_201, 9_500_100_001, &.{spec});
    try api.submitCommand(.{ .add_user = 95_121 });
    try api.submitCommand(.{
        .adjust_user_balance = .{
            .uid = 95_121,
            .transaction_id = 101,
            .currency = 15,
            .amount = 200_000,
            .adjustment_type = .adjustment,
        },
    });

    const place = try api.submitPlaceNewOrderAsync(77_001, 7, 100, 120, 5, .bid, .gtc, 9_351, 95_121);
    const move = try api.submitMoveOrderAsync(101, 77_001, 9_351, 95_121);
    const reduce = try api.submitReduceOrderAsync(2, 77_001, 9_351, 95_121);
    const cancel = try api.submitCancelOrderAsync(77_001, 9_351, 95_121);
    const book = try api.orderBookRequestAsync(9_351, 16);

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 5), processed);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(place));
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(move));
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(reduce));
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(cancel));

    var snapshot = (try api.takeOrderBookSnapshot(book)) orelse return error.MissingAsyncResult;
    defer snapshot.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), snapshot.bids.items.len);
    try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len);
}

test "exchange api callback wrappers invoke callbacks for user lifecycle commands" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    var collector: CallbackCollector = .{};
    const callback: CommandResponseCallback = .{
        .context = &collector,
        .callback = CallbackCollector.onResponse,
    };

    try api.createUserWithCallback(96_001, callback);
    try api.suspendUserWithCallback(96_001, callback);
    try api.resumeUserWithCallback(96_001, callback);

    try std.testing.expectEqual(@as(usize, 3), api.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 0), collector.count);
    try std.testing.expectEqual(@as(usize, 0), try api.processPendingCallbacks());

    const drained = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 3), drained);
    try std.testing.expectEqual(@as(usize, 3), try api.processPendingCallbacks());

    try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 3), collector.count);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), collector.last_result);
}

test "exchange api processResult consumes callback result after drain" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const handle = try api.submitCreateUserAsync(96_101);

    var collector: CallbackCollector = .{};
    const callback: CommandResponseCallback = .{
        .context = &collector,
        .callback = CallbackCollector.onResponse,
    };

    try std.testing.expect(!(try api.processResult(handle, callback)));
    try std.testing.expectEqual(@as(usize, 0), collector.count);

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 1), processed);

    try std.testing.expect(try api.processResult(handle, callback));
    try std.testing.expectEqual(@as(usize, 1), collector.count);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), collector.last_result);
    try std.testing.expect(!(try api.processResult(handle, callback)));
}

test "exchange api processResult callback exposes order-book snapshot response" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const spec = try makeExchangeSpecForSymbol(9_362);
    try api.submitAddSymbolsBinary(5_302, 9_600_100_002, &.{spec});

    const handle = try api.orderBookRequestAsync(9_362, 16);
    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 1), processed);

    var collector: CallbackCollector = .{};
    try std.testing.expect(try api.processResult(handle, .{
        .context = &collector,
        .callback = CallbackCollector.onResponse,
    }));
    try std.testing.expectEqual(@as(usize, 1), collector.count);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), collector.last_result);
    try std.testing.expect(collector.last_snapshot_present);
    try std.testing.expectEqual(@as(usize, 0), collector.last_ask_levels);
    try std.testing.expectEqual(@as(usize, 0), collector.last_bid_levels);
}

test "exchange api orderBookRequest callback wrapper exposes snapshot response" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const spec = try makeExchangeSpecForSymbol(9_361);
    try api.submitAddSymbolsBinary(5_301, 9_600_100_001, &.{spec});

    var collector: CallbackCollector = .{};
    try api.orderBookRequest(9_361, 32, .{
        .context = &collector,
        .callback = CallbackCollector.onResponse,
    });

    try std.testing.expectEqual(@as(usize, 1), api.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 0), collector.count);
    try std.testing.expectEqual(@as(usize, 0), try api.processPendingCallbacks());

    const drained = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 1), drained);
    try std.testing.expectEqual(@as(usize, 1), try api.processPendingCallbacks());

    try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 1), collector.count);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), collector.last_result);
    try std.testing.expect(collector.last_snapshot_present);
    try std.testing.expectEqual(@as(usize, 0), collector.last_ask_levels);
    try std.testing.expectEqual(@as(usize, 0), collector.last_bid_levels);
}

test "exchange api registered callback can be processed per handle after drain" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const handle = try api.submitCreateUserAsync(96_201);

    var collector: CallbackCollector = .{};
    try api.registerResultCallback(handle, .{
        .context = &collector,
        .callback = CallbackCollector.onResponse,
    });

    try std.testing.expectEqual(@as(usize, 1), api.pendingCallbackCount());
    try std.testing.expect(!(try api.processRegisteredCallback(handle)));
    try std.testing.expectEqual(@as(usize, 0), collector.count);

    const drained = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 1), drained);

    try std.testing.expect(try api.processRegisteredCallback(handle));
    try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 1), collector.count);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), collector.last_result);

    try std.testing.expect(!(try api.processRegisteredCallback(handle)));
}

test "exchange api processResult callback path converges with polled full-response path" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 9_371;
    const uid: i64 = 97_001;

    var poll_core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer poll_core.deinit();
    var poll_api = ExchangeApi.init(allocator, &poll_core);
    const spec = try makeExchangeSpecForSymbol(symbol);
    try poll_api.submitAddSymbolsBinary(5_401, 9_700_100_001, &.{spec});
    try poll_api.submitCommand(.{ .add_user = uid });
    try poll_api.submitCommand(.{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 401,
            .currency = 15,
            .amount = 200_000,
            .adjustment_type = .adjustment,
        },
    });

    const poll_handles = [_]ApiCommandAsyncHandle{
        try poll_api.submitPlaceNewOrderAsync(88_001, 5, 100, 120, 5, .bid, .gtc, symbol, uid),
        try poll_api.submitMoveOrderAsync(101, 88_001, symbol, uid),
        try poll_api.submitReduceOrderAsync(2, 88_001, symbol, uid),
        try poll_api.submitCancelOrderAsync(88_001, symbol, uid),
        try poll_api.orderBookRequestAsync(symbol, 32),
    };

    const poll_processed = try poll_api.drainPending();
    try std.testing.expectEqual(@as(usize, poll_handles.len), poll_processed);

    var expected_traces: [poll_handles.len]AsyncResponseTrace = undefined;
    for (poll_handles, 0..) |handle, idx| {
        var response = (try poll_api.takeFullResponse(handle)) orelse return error.MissingAsyncResult;
        defer response.deinit(allocator);
        expected_traces[idx] = traceFromResponse(&response);
    }

    var callback_core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer callback_core.deinit();
    var callback_api = ExchangeApi.init(allocator, &callback_core);
    try callback_api.submitAddSymbolsBinary(5_402, 9_700_100_002, &.{spec});
    try callback_api.submitCommand(.{ .add_user = uid });
    try callback_api.submitCommand(.{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 401,
            .currency = 15,
            .amount = 200_000,
            .adjustment_type = .adjustment,
        },
    });

    const callback_handles = [_]ApiCommandAsyncHandle{
        try callback_api.submitPlaceNewOrderAsync(88_001, 5, 100, 120, 5, .bid, .gtc, symbol, uid),
        try callback_api.submitMoveOrderAsync(101, 88_001, symbol, uid),
        try callback_api.submitReduceOrderAsync(2, 88_001, symbol, uid),
        try callback_api.submitCancelOrderAsync(88_001, symbol, uid),
        try callback_api.orderBookRequestAsync(symbol, 32),
    };

    const callback_processed = try callback_api.drainPending();
    try std.testing.expectEqual(@as(usize, callback_handles.len), callback_processed);

    var collector: TraceCollector = .{};
    const callback: CommandResponseCallback = .{
        .context = &collector,
        .callback = TraceCollector.onResponse,
    };
    for (callback_handles) |handle| {
        try std.testing.expect(try callback_api.processResult(handle, callback));
    }

    try std.testing.expectEqual(@as(usize, expected_traces.len), collector.count);
    for (expected_traces, 0..) |expected, idx| {
        const actual = collector.traces[idx];
        try std.testing.expectEqual(expected.result, actual.result);
        try std.testing.expectEqual(expected.snapshot_present, actual.snapshot_present);
        try std.testing.expectEqual(expected.ask_levels, actual.ask_levels);
        try std.testing.expectEqual(expected.bid_levels, actual.bid_levels);
    }
}

test "exchange api seeded deterministic simulation keeps callback and poll paths equivalent" {
    const allocator = std.testing.allocator;
    const rounds: usize = 32;
    const max_ops_per_round: usize = 20;

    var rng = DeterministicRng.init(0xC0FFEE1234ABCD99);

    for (0..rounds) |round_idx| {
        const symbol = 9_500 + @as(i32, @intCast(round_idx));
        const uid = 98_000 + @as(i64, @intCast(round_idx));
        const spec = try makeExchangeSpecForSymbol(symbol);

        var poll_core = try exchange_core_mod.ExchangeCore.init(allocator, .{
            .matching = .{
                .orderbook_kind = .direct,
            },
        });
        defer poll_core.deinit();
        var poll_api = ExchangeApi.init(allocator, &poll_core);

        var callback_core = try exchange_core_mod.ExchangeCore.init(allocator, .{
            .matching = .{
                .orderbook_kind = .direct,
            },
        });
        defer callback_core.deinit();
        var callback_api = ExchangeApi.init(allocator, &callback_core);

        try poll_api.submitAddSymbolsBinary(5_500 + @as(i32, @intCast(round_idx)), 9_800_100_001, &.{spec});
        try callback_api.submitAddSymbolsBinary(6_500 + @as(i32, @intCast(round_idx)), 9_800_100_002, &.{spec});

        try poll_api.submitCommand(.{ .add_user = uid });
        try callback_api.submitCommand(.{ .add_user = uid });

        try poll_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 1,
                .currency = 15,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try callback_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 1,
                .currency = 15,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });

        try poll_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 2,
                .currency = 11,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try callback_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 2,
                .currency = 11,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });

        var poll_handles: [max_ops_per_round]ApiCommandAsyncHandle = undefined;
        var callback_handles: [max_ops_per_round]ApiCommandAsyncHandle = undefined;
        const op_count = 10 + @as(usize, @intCast(rng.bounded(@as(u64, max_ops_per_round - 9))));
        var next_order_id: i64 = 300_000 + @as(i64, @intCast(round_idx)) * 1_000;

        for (0..op_count) |op_idx| {
            const op_kind: u8 = @intCast(rng.bounded(5));
            switch (op_kind) {
                0 => {
                    next_order_id += 1;
                    const order_id = next_order_id;
                    const user_cookie: i32 = @intCast(rng.bounded(64));
                    const price = 90 + @as(i64, @intCast(rng.bounded(31)));
                    const reserve_bid_price = price + 5;
                    const size = 1 + @as(i64, @intCast(rng.bounded(8)));
                    const action: common.OrderAction = if (rng.next() & 1 == 0) .bid else .ask;

                    poll_handles[op_idx] = try poll_api.submitPlaceNewOrderAsync(
                        order_id,
                        user_cookie,
                        price,
                        reserve_bid_price,
                        size,
                        action,
                        .gtc,
                        symbol,
                        uid,
                    );
                    callback_handles[op_idx] = try callback_api.submitPlaceNewOrderAsync(
                        order_id,
                        user_cookie,
                        price,
                        reserve_bid_price,
                        size,
                        action,
                        .gtc,
                        symbol,
                        uid,
                    );
                },
                1 => {
                    const offset = @as(i64, @intCast(rng.bounded(6)));
                    const target_order_id = if (rng.next() & 3 == 0) next_order_id + 100 + offset else next_order_id - offset;
                    const new_price = 90 + @as(i64, @intCast(rng.bounded(31)));

                    poll_handles[op_idx] = try poll_api.submitMoveOrderAsync(
                        new_price,
                        target_order_id,
                        symbol,
                        uid,
                    );
                    callback_handles[op_idx] = try callback_api.submitMoveOrderAsync(
                        new_price,
                        target_order_id,
                        symbol,
                        uid,
                    );
                },
                2 => {
                    const offset = @as(i64, @intCast(rng.bounded(6)));
                    const target_order_id = if (rng.next() & 3 == 0) next_order_id + 100 + offset else next_order_id - offset;
                    const reduce_size = 1 + @as(i64, @intCast(rng.bounded(4)));

                    poll_handles[op_idx] = try poll_api.submitReduceOrderAsync(
                        reduce_size,
                        target_order_id,
                        symbol,
                        uid,
                    );
                    callback_handles[op_idx] = try callback_api.submitReduceOrderAsync(
                        reduce_size,
                        target_order_id,
                        symbol,
                        uid,
                    );
                },
                3 => {
                    const offset = @as(i64, @intCast(rng.bounded(6)));
                    const target_order_id = if (rng.next() & 3 == 0) next_order_id + 100 + offset else next_order_id - offset;

                    poll_handles[op_idx] = try poll_api.submitCancelOrderAsync(
                        target_order_id,
                        symbol,
                        uid,
                    );
                    callback_handles[op_idx] = try callback_api.submitCancelOrderAsync(
                        target_order_id,
                        symbol,
                        uid,
                    );
                },
                else => {
                    const depth = 4 + @as(usize, @intCast(rng.bounded(13)));
                    poll_handles[op_idx] = try poll_api.orderBookRequestAsync(symbol, depth);
                    callback_handles[op_idx] = try callback_api.orderBookRequestAsync(symbol, depth);
                },
            }
        }

        const poll_processed = try poll_api.drainPending();
        const callback_processed = try callback_api.drainPending();
        try std.testing.expectEqual(op_count, poll_processed);
        try std.testing.expectEqual(op_count, callback_processed);

        var expected_traces: [max_ops_per_round]AsyncResponseTrace = undefined;
        for (poll_handles[0..op_count], 0..) |handle, idx| {
            var response = (try poll_api.takeFullResponse(handle)) orelse return error.MissingAsyncResult;
            defer response.deinit(allocator);
            expected_traces[idx] = traceFromResponse(&response);
        }

        var collector: TraceCollector = .{};
        const callback: CommandResponseCallback = .{
            .context = &collector,
            .callback = TraceCollector.onResponse,
        };
        for (callback_handles[0..op_count]) |handle| {
            try std.testing.expect(try callback_api.processResult(handle, callback));
        }

        try std.testing.expectEqual(op_count, collector.count);
        for (expected_traces[0..op_count], 0..) |expected, idx| {
            const actual = collector.traces[idx];
            try expectTraceEqual(expected, actual);
        }
    }
}

test "exchange api seeded perturbation simulation keeps registered callbacks equivalent to polling" {
    const allocator = std.testing.allocator;
    const rounds: usize = 16;
    const max_ops_per_round: usize = 20;

    var rng = DeterministicRng.init(0xD00DFEED12345678);

    for (0..rounds) |round_idx| {
        const symbol = 9_700 + @as(i32, @intCast(round_idx));
        const uid = 99_000 + @as(i64, @intCast(round_idx));
        const spec = try makeExchangeSpecForSymbol(symbol);

        var poll_core = try exchange_core_mod.ExchangeCore.init(allocator, .{
            .matching = .{
                .orderbook_kind = .direct,
            },
        });
        defer poll_core.deinit();
        var poll_api = ExchangeApi.init(allocator, &poll_core);

        var callback_core = try exchange_core_mod.ExchangeCore.init(allocator, .{
            .matching = .{
                .orderbook_kind = .direct,
            },
        });
        defer callback_core.deinit();
        var callback_api = ExchangeApi.init(allocator, &callback_core);

        try poll_api.submitAddSymbolsBinary(8_500 + @as(i32, @intCast(round_idx)), 9_900_100_001, &.{spec});
        try callback_api.submitAddSymbolsBinary(9_500 + @as(i32, @intCast(round_idx)), 9_900_100_002, &.{spec});

        try poll_api.submitCommand(.{ .add_user = uid });
        try callback_api.submitCommand(.{ .add_user = uid });

        try poll_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 1,
                .currency = 15,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try callback_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 1,
                .currency = 15,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });

        try poll_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 2,
                .currency = 11,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try callback_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 2,
                .currency = 11,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });

        var poll_handles: [max_ops_per_round]ApiCommandAsyncHandle = undefined;
        var callback_handles: [max_ops_per_round]ApiCommandAsyncHandle = undefined;
        const op_count = 10 + @as(usize, @intCast(rng.bounded(@as(u64, max_ops_per_round - 9))));
        var next_order_id: i64 = 500_000 + @as(i64, @intCast(round_idx)) * 1_000;

        for (0..op_count) |op_idx| {
            const op_kind: u8 = @intCast(rng.bounded(5));
            switch (op_kind) {
                0 => {
                    next_order_id += 1;
                    const order_id = next_order_id;
                    const user_cookie: i32 = @intCast(rng.bounded(64));
                    const price = 90 + @as(i64, @intCast(rng.bounded(31)));
                    const reserve_bid_price = price + 5;
                    const size = 1 + @as(i64, @intCast(rng.bounded(8)));
                    const action: common.OrderAction = if (rng.next() & 1 == 0) .bid else .ask;

                    poll_handles[op_idx] = try poll_api.submitPlaceNewOrderAsync(
                        order_id,
                        user_cookie,
                        price,
                        reserve_bid_price,
                        size,
                        action,
                        .gtc,
                        symbol,
                        uid,
                    );
                    callback_handles[op_idx] = try callback_api.submitPlaceNewOrderAsync(
                        order_id,
                        user_cookie,
                        price,
                        reserve_bid_price,
                        size,
                        action,
                        .gtc,
                        symbol,
                        uid,
                    );
                },
                1 => {
                    const offset = @as(i64, @intCast(rng.bounded(6)));
                    const target_order_id = if (rng.next() & 3 == 0) next_order_id + 100 + offset else next_order_id - offset;
                    const new_price = 90 + @as(i64, @intCast(rng.bounded(31)));

                    poll_handles[op_idx] = try poll_api.submitMoveOrderAsync(
                        new_price,
                        target_order_id,
                        symbol,
                        uid,
                    );
                    callback_handles[op_idx] = try callback_api.submitMoveOrderAsync(
                        new_price,
                        target_order_id,
                        symbol,
                        uid,
                    );
                },
                2 => {
                    const offset = @as(i64, @intCast(rng.bounded(6)));
                    const target_order_id = if (rng.next() & 3 == 0) next_order_id + 100 + offset else next_order_id - offset;
                    const reduce_size = 1 + @as(i64, @intCast(rng.bounded(4)));

                    poll_handles[op_idx] = try poll_api.submitReduceOrderAsync(
                        reduce_size,
                        target_order_id,
                        symbol,
                        uid,
                    );
                    callback_handles[op_idx] = try callback_api.submitReduceOrderAsync(
                        reduce_size,
                        target_order_id,
                        symbol,
                        uid,
                    );
                },
                3 => {
                    const offset = @as(i64, @intCast(rng.bounded(6)));
                    const target_order_id = if (rng.next() & 3 == 0) next_order_id + 100 + offset else next_order_id - offset;

                    poll_handles[op_idx] = try poll_api.submitCancelOrderAsync(
                        target_order_id,
                        symbol,
                        uid,
                    );
                    callback_handles[op_idx] = try callback_api.submitCancelOrderAsync(
                        target_order_id,
                        symbol,
                        uid,
                    );
                },
                else => {
                    const depth = 4 + @as(usize, @intCast(rng.bounded(13)));
                    poll_handles[op_idx] = try poll_api.orderBookRequestAsync(symbol, depth);
                    callback_handles[op_idx] = try callback_api.orderBookRequestAsync(symbol, depth);
                },
            }
        }

        var collector: TraceCollector = .{};
        const callback: CommandResponseCallback = .{
            .context = &collector,
            .callback = TraceCollector.onResponse,
        };
        for (callback_handles[0..op_count]) |handle| {
            try callback_api.registerResultCallback(handle, callback);
        }
        try std.testing.expectEqual(op_count, callback_api.pendingCallbackCount());

        const early_attempts = 1 + @as(usize, @intCast(rng.bounded(@as(u64, op_count))));
        for (0..early_attempts) |_| {
            const idx = @as(usize, @intCast(rng.bounded(@as(u64, op_count))));
            try std.testing.expect(!(try callback_api.processRegisteredCallback(callback_handles[idx])));
        }
        try std.testing.expectEqual(@as(usize, 0), collector.count);
        try std.testing.expectEqual(op_count, callback_api.pendingCallbackCount());
        try std.testing.expectEqual(@as(usize, 0), try callback_api.processPendingCallbacks());

        const poll_processed = try poll_api.drainPending();
        const callback_processed = try callback_api.drainPending();
        try std.testing.expectEqual(op_count, poll_processed);
        try std.testing.expectEqual(op_count, callback_processed);

        var expected_traces: [max_ops_per_round]AsyncResponseTrace = undefined;
        for (poll_handles[0..op_count], 0..) |handle, idx| {
            var response = (try poll_api.takeFullResponse(handle)) orelse return error.MissingAsyncResult;
            defer response.deinit(allocator);
            expected_traces[idx] = traceFromResponse(&response);
        }

        var permutation: [max_ops_per_round]usize = undefined;
        for (0..op_count) |idx| {
            permutation[idx] = idx;
        }
        if (op_count > 1) {
            var i = op_count - 1;
            while (i > 0) : (i -= 1) {
                const j = @as(usize, @intCast(rng.bounded(@as(u64, i + 1))));
                std.mem.swap(usize, &permutation[i], &permutation[j]);
            }
        }

        var processed_immediate: [max_ops_per_round]bool = [_]bool{false} ** max_ops_per_round;
        var immediate_count: usize = 0;
        for (permutation[0..op_count]) |idx| {
            const action: u8 = @intCast(rng.bounded(3));
            if (action == 2) {
                continue;
            }

            const before = collector.count;
            try std.testing.expect(try callback_api.processRegisteredCallback(callback_handles[idx]));
            try std.testing.expectEqual(before + 1, collector.count);
            try expectTraceEqual(expected_traces[idx], collector.traces[before]);
            processed_immediate[idx] = true;
            immediate_count += 1;

            if (action == 0) {
                try std.testing.expect(!(try callback_api.processRegisteredCallback(callback_handles[idx])));
                try std.testing.expectEqual(before + 1, collector.count);
            }
        }

        const deferred_count = op_count - immediate_count;
        try std.testing.expectEqual(deferred_count, callback_api.pendingCallbackCount());

        const batch_start = collector.count;
        try std.testing.expectEqual(deferred_count, try callback_api.processPendingCallbacks());
        try std.testing.expectEqual(op_count, collector.count);
        try std.testing.expectEqual(@as(usize, 0), callback_api.pendingCallbackCount());
        try std.testing.expectEqual(@as(usize, 0), try callback_api.processPendingCallbacks());

        var deferred_cursor = batch_start;
        for (0..op_count) |idx| {
            if (processed_immediate[idx]) continue;
            try expectTraceEqual(expected_traces[idx], collector.traces[deferred_cursor]);
            deferred_cursor += 1;
        }
        try std.testing.expectEqual(op_count, deferred_cursor);
    }
}

test "exchange api mixed callback and poll consumption stays parity and single-consume" {
    const allocator = std.testing.allocator;
    const rounds: usize = 16;
    const max_ops_per_round: usize = 20;

    var rng = DeterministicRng.init(0xA11CEBEEFF00D123);

    for (0..rounds) |round_idx| {
        const symbol = 10_100 + @as(i32, @intCast(round_idx));
        const uid = 101_000 + @as(i64, @intCast(round_idx));
        const spec = try makeExchangeSpecForSymbol(symbol);

        var poll_core = try exchange_core_mod.ExchangeCore.init(allocator, .{
            .matching = .{
                .orderbook_kind = .direct,
            },
        });
        defer poll_core.deinit();
        var poll_api = ExchangeApi.init(allocator, &poll_core);

        var mixed_core = try exchange_core_mod.ExchangeCore.init(allocator, .{
            .matching = .{
                .orderbook_kind = .direct,
            },
        });
        defer mixed_core.deinit();
        var mixed_api = ExchangeApi.init(allocator, &mixed_core);

        try poll_api.submitAddSymbolsBinary(12_500 + @as(i32, @intCast(round_idx)), 10_100_100_001, &.{spec});
        try mixed_api.submitAddSymbolsBinary(13_500 + @as(i32, @intCast(round_idx)), 10_100_100_002, &.{spec});

        try poll_api.submitCommand(.{ .add_user = uid });
        try mixed_api.submitCommand(.{ .add_user = uid });

        try poll_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 1,
                .currency = 15,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try mixed_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 1,
                .currency = 15,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });

        try poll_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 2,
                .currency = 11,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try mixed_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 2,
                .currency = 11,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });

        var poll_handles: [max_ops_per_round]ApiCommandAsyncHandle = undefined;
        var mixed_handles: [max_ops_per_round]ApiCommandAsyncHandle = undefined;
        var use_callback: [max_ops_per_round]bool = [_]bool{false} ** max_ops_per_round;
        const op_count = 10 + @as(usize, @intCast(rng.bounded(@as(u64, max_ops_per_round - 9))));
        var next_order_id: i64 = 700_000 + @as(i64, @intCast(round_idx)) * 1_000;

        for (0..op_count) |op_idx| {
            const op_kind: u8 = @intCast(rng.bounded(5));
            switch (op_kind) {
                0 => {
                    next_order_id += 1;
                    const order_id = next_order_id;
                    const user_cookie: i32 = @intCast(rng.bounded(64));
                    const price = 90 + @as(i64, @intCast(rng.bounded(31)));
                    const reserve_bid_price = price + 5;
                    const size = 1 + @as(i64, @intCast(rng.bounded(8)));
                    const action: common.OrderAction = if (rng.next() & 1 == 0) .bid else .ask;

                    poll_handles[op_idx] = try poll_api.submitPlaceNewOrderAsync(
                        order_id,
                        user_cookie,
                        price,
                        reserve_bid_price,
                        size,
                        action,
                        .gtc,
                        symbol,
                        uid,
                    );
                    mixed_handles[op_idx] = try mixed_api.submitPlaceNewOrderAsync(
                        order_id,
                        user_cookie,
                        price,
                        reserve_bid_price,
                        size,
                        action,
                        .gtc,
                        symbol,
                        uid,
                    );
                },
                1 => {
                    const offset = @as(i64, @intCast(rng.bounded(6)));
                    const target_order_id = if (rng.next() & 3 == 0) next_order_id + 100 + offset else next_order_id - offset;
                    const new_price = 90 + @as(i64, @intCast(rng.bounded(31)));

                    poll_handles[op_idx] = try poll_api.submitMoveOrderAsync(
                        new_price,
                        target_order_id,
                        symbol,
                        uid,
                    );
                    mixed_handles[op_idx] = try mixed_api.submitMoveOrderAsync(
                        new_price,
                        target_order_id,
                        symbol,
                        uid,
                    );
                },
                2 => {
                    const offset = @as(i64, @intCast(rng.bounded(6)));
                    const target_order_id = if (rng.next() & 3 == 0) next_order_id + 100 + offset else next_order_id - offset;
                    const reduce_size = 1 + @as(i64, @intCast(rng.bounded(4)));

                    poll_handles[op_idx] = try poll_api.submitReduceOrderAsync(
                        reduce_size,
                        target_order_id,
                        symbol,
                        uid,
                    );
                    mixed_handles[op_idx] = try mixed_api.submitReduceOrderAsync(
                        reduce_size,
                        target_order_id,
                        symbol,
                        uid,
                    );
                },
                3 => {
                    const offset = @as(i64, @intCast(rng.bounded(6)));
                    const target_order_id = if (rng.next() & 3 == 0) next_order_id + 100 + offset else next_order_id - offset;

                    poll_handles[op_idx] = try poll_api.submitCancelOrderAsync(
                        target_order_id,
                        symbol,
                        uid,
                    );
                    mixed_handles[op_idx] = try mixed_api.submitCancelOrderAsync(
                        target_order_id,
                        symbol,
                        uid,
                    );
                },
                else => {
                    const depth = 4 + @as(usize, @intCast(rng.bounded(13)));
                    poll_handles[op_idx] = try poll_api.orderBookRequestAsync(symbol, depth);
                    mixed_handles[op_idx] = try mixed_api.orderBookRequestAsync(symbol, depth);
                },
            }
            use_callback[op_idx] = (rng.next() & 1) == 0;
        }
        if (op_count >= 2) {
            use_callback[0] = true;
            use_callback[1] = false;
        }

        const poll_processed = try poll_api.drainPending();
        try std.testing.expectEqual(op_count, poll_processed);
        var expected_traces: [max_ops_per_round]AsyncResponseTrace = undefined;
        for (poll_handles[0..op_count], 0..) |handle, idx| {
            var response = (try poll_api.takeFullResponse(handle)) orelse return error.MissingAsyncResult;
            defer response.deinit(allocator);
            expected_traces[idx] = traceFromResponse(&response);
        }

        var collector: TraceCollector = .{};
        const callback: CommandResponseCallback = .{
            .context = &collector,
            .callback = TraceCollector.onResponse,
        };
        var callback_count: usize = 0;
        for (0..op_count) |idx| {
            if (!use_callback[idx]) continue;
            callback_count += 1;
            try mixed_api.registerResultCallback(mixed_handles[idx], callback);

            // Registered callbacks are non-blocking and should not consume before drain.
            try std.testing.expect(!(try mixed_api.processRegisteredCallback(mixed_handles[idx])));
        }
        try std.testing.expectEqual(callback_count, mixed_api.pendingCallbackCount());

        const mixed_processed = try mixed_api.drainPending();
        try std.testing.expectEqual(op_count, mixed_processed);

        var callback_cursor: usize = 0;
        for (0..op_count) |idx| {
            if (use_callback[idx]) {
                try std.testing.expect(try mixed_api.processRegisteredCallback(mixed_handles[idx]));
                try expectTraceEqual(expected_traces[idx], collector.traces[callback_cursor]);
                callback_cursor += 1;

                // Callback consumed it; polling should now be empty.
                try std.testing.expect(mixed_api.takeResult(mixed_handles[idx]) == null);
                try std.testing.expect(!(try mixed_api.processRegisteredCallback(mixed_handles[idx])));
            } else {
                // No registration for this handle, so registered processing should not consume.
                try std.testing.expect(!(try mixed_api.processRegisteredCallback(mixed_handles[idx])));

                var response = (try mixed_api.takeFullResponse(mixed_handles[idx])) orelse return error.MissingAsyncResult;
                defer response.deinit(allocator);
                try expectTraceEqual(expected_traces[idx], traceFromResponse(&response));
                try std.testing.expect(mixed_api.takeResult(mixed_handles[idx]) == null);
            }
        }

        try std.testing.expectEqual(callback_count, callback_cursor);
        try std.testing.expectEqual(callback_count, collector.count);
        try std.testing.expectEqual(@as(usize, 0), mixed_api.pendingCallbackCount());
        try std.testing.expectEqual(@as(usize, 0), try mixed_api.processPendingCallbacks());
    }
}

test "exchange api mixed callback and poll report-query consumption keeps parity" {
    const allocator = std.testing.allocator;
    const rounds: usize = 16;

    var rng = DeterministicRng.init(0xFACE1234BEEF7890);

    for (0..rounds) |round_idx| {
        const symbol = 10_400 + @as(i32, @intCast(round_idx));
        const uid = 103_000 + @as(i64, @intCast(round_idx));
        const missing_uid = uid + 1_000_000;
        const spec = try makeExchangeSpecForSymbol(symbol);
        const timestamp_base: i64 = 11_000_000_000 + @as(i64, @intCast(round_idx)) * 100;

        var poll_core = try exchange_core_mod.ExchangeCore.init(allocator, .{
            .matching = .{
                .orderbook_kind = .direct,
            },
        });
        defer poll_core.deinit();
        var poll_api = ExchangeApi.init(allocator, &poll_core);

        var mixed_core = try exchange_core_mod.ExchangeCore.init(allocator, .{
            .matching = .{
                .orderbook_kind = .direct,
            },
        });
        defer mixed_core.deinit();
        var mixed_api = ExchangeApi.init(allocator, &mixed_core);

        try poll_api.submitAddSymbolsBinary(14_500 + @as(i32, @intCast(round_idx)), timestamp_base + 1, &.{spec});
        try mixed_api.submitAddSymbolsBinary(15_500 + @as(i32, @intCast(round_idx)), timestamp_base + 2, &.{spec});

        try poll_api.submitCommand(.{ .add_user = uid });
        try mixed_api.submitCommand(.{ .add_user = uid });

        try poll_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 1,
                .currency = 15,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try mixed_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 1,
                .currency = 15,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });

        try poll_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 2,
                .currency = 11,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try mixed_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 2,
                .currency = 11,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });

        try poll_api.submitCommand(.{
            .place_order = .{
                .order_id = 800_000 + @as(i64, @intCast(round_idx)),
                .uid = uid,
                .symbol = symbol,
                .price = 101,
                .reserve_bid_price = 110,
                .size = 3,
                .action = .bid,
                .order_type = .gtc,
                .user_cookie = 9,
            },
        });
        try mixed_api.submitCommand(.{
            .place_order = .{
                .order_id = 800_000 + @as(i64, @intCast(round_idx)),
                .uid = uid,
                .symbol = symbol,
                .price = 101,
                .reserve_bid_price = 110,
                .size = 3,
                .action = .bid,
                .order_type = .gtc,
                .user_cookie = 9,
            },
        });

        const poll_handles = [_]ApiCommandAsyncHandle{
            try poll_api.requestStateHashAsync(100 + @as(i32, @intCast(round_idx)), timestamp_base + 10),
            try poll_api.requestSingleUserReportAsync(200 + @as(i32, @intCast(round_idx)), timestamp_base + 11, uid),
            try poll_api.requestSingleUserReportAsync(300 + @as(i32, @intCast(round_idx)), timestamp_base + 12, missing_uid),
            try poll_api.requestTotalCurrencyBalanceAsync(400 + @as(i32, @intCast(round_idx)), timestamp_base + 13),
        };
        const mixed_handles = [_]ApiCommandAsyncHandle{
            try mixed_api.requestStateHashAsync(100 + @as(i32, @intCast(round_idx)), timestamp_base + 10),
            try mixed_api.requestSingleUserReportAsync(200 + @as(i32, @intCast(round_idx)), timestamp_base + 11, uid),
            try mixed_api.requestSingleUserReportAsync(300 + @as(i32, @intCast(round_idx)), timestamp_base + 12, missing_uid),
            try mixed_api.requestTotalCurrencyBalanceAsync(400 + @as(i32, @intCast(round_idx)), timestamp_base + 13),
        };

        var use_callback: [mixed_handles.len]bool = [_]bool{false} ** mixed_handles.len;
        var callback_count: usize = 0;
        for (0..mixed_handles.len) |idx| {
            use_callback[idx] = (rng.next() & 1) == 0;
            if (use_callback[idx]) callback_count += 1;
        }
        if (callback_count == 0) {
            use_callback[0] = true;
            callback_count = 1;
        } else if (callback_count == mixed_handles.len) {
            use_callback[1] = false;
            callback_count -= 1;
        }

        const poll_processed = try poll_api.drainPending();
        const mixed_processed = try mixed_api.drainPending();
        try std.testing.expectEqual(@as(usize, poll_handles.len), poll_processed);
        try std.testing.expectEqual(@as(usize, mixed_handles.len), mixed_processed);

        var expected: [poll_handles.len]ResponseDigest = undefined;
        for (poll_handles, 0..) |handle, idx| {
            var response = (try poll_api.takeFullResponse(handle)) orelse return error.MissingAsyncResult;
            defer response.deinit(allocator);
            expected[idx] = digestFromResponse(&response);
        }

        var collector: DigestCollector = .{};
        const callback: CommandResponseCallback = .{
            .context = &collector,
            .callback = DigestCollector.onResponse,
        };
        for (mixed_handles, 0..) |handle, idx| {
            if (!use_callback[idx]) continue;
            try mixed_api.registerResultCallback(handle, callback);
        }
        try std.testing.expectEqual(callback_count, mixed_api.pendingCallbackCount());

        var callback_cursor: usize = 0;
        for (mixed_handles, 0..) |handle, idx| {
            if (use_callback[idx]) {
                try std.testing.expect(try mixed_api.processRegisteredCallback(handle));
                try expectDigestEqual(expected[idx], collector.digests[callback_cursor]);
                callback_cursor += 1;
                try std.testing.expect(mixed_api.takeResult(handle) == null);
            } else {
                try std.testing.expect(!(try mixed_api.processRegisteredCallback(handle)));
                var response = (try mixed_api.takeFullResponse(handle)) orelse return error.MissingAsyncResult;
                defer response.deinit(allocator);
                try expectDigestEqual(expected[idx], digestFromResponse(&response));
            }
        }

        try std.testing.expectEqual(callback_count, callback_cursor);
        try std.testing.expectEqual(callback_count, collector.count);
        try std.testing.expectEqual(@as(usize, 0), mixed_api.pendingCallbackCount());
        try std.testing.expectEqual(@as(usize, 0), try mixed_api.processPendingCallbacks());
    }
}

test "exchange api seeded perturbation simulation keeps report-query registered callbacks equivalent to polling" {
    const allocator = std.testing.allocator;
    const rounds: usize = 16;
    const max_ops_per_round: usize = 24;

    var rng = DeterministicRng.init(0xBADC0FFEE0DDF00D);

    for (0..rounds) |round_idx| {
        const symbol = 10_700 + @as(i32, @intCast(round_idx));
        const uid = 104_000 + @as(i64, @intCast(round_idx));
        const missing_uid = uid + 1_000_000;
        const spec = try makeExchangeSpecForSymbol(symbol);
        const timestamp_base: i64 = 12_000_000_000 + @as(i64, @intCast(round_idx)) * 200;

        var poll_core = try exchange_core_mod.ExchangeCore.init(allocator, .{
            .matching = .{
                .orderbook_kind = .direct,
            },
        });
        defer poll_core.deinit();
        var poll_api = ExchangeApi.init(allocator, &poll_core);

        var callback_core = try exchange_core_mod.ExchangeCore.init(allocator, .{
            .matching = .{
                .orderbook_kind = .direct,
            },
        });
        defer callback_core.deinit();
        var callback_api = ExchangeApi.init(allocator, &callback_core);

        try poll_api.submitAddSymbolsBinary(16_500 + @as(i32, @intCast(round_idx)), timestamp_base + 1, &.{spec});
        try callback_api.submitAddSymbolsBinary(17_500 + @as(i32, @intCast(round_idx)), timestamp_base + 2, &.{spec});

        try poll_api.submitCommand(.{ .add_user = uid });
        try callback_api.submitCommand(.{ .add_user = uid });

        try poll_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 1,
                .currency = 15,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try callback_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 1,
                .currency = 15,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });

        try poll_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 2,
                .currency = 11,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });
        try callback_api.submitCommand(.{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 2,
                .currency = 11,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        });

        try poll_api.submitCommand(.{
            .place_order = .{
                .order_id = 900_000 + @as(i64, @intCast(round_idx)),
                .uid = uid,
                .symbol = symbol,
                .price = 102,
                .reserve_bid_price = 112,
                .size = 4,
                .action = .bid,
                .order_type = .gtc,
                .user_cookie = 17,
            },
        });
        try callback_api.submitCommand(.{
            .place_order = .{
                .order_id = 900_000 + @as(i64, @intCast(round_idx)),
                .uid = uid,
                .symbol = symbol,
                .price = 102,
                .reserve_bid_price = 112,
                .size = 4,
                .action = .bid,
                .order_type = .gtc,
                .user_cookie = 17,
            },
        });

        var poll_handles: [max_ops_per_round]ApiCommandAsyncHandle = undefined;
        var callback_handles: [max_ops_per_round]ApiCommandAsyncHandle = undefined;
        const op_count = 8 + @as(usize, @intCast(rng.bounded(@as(u64, max_ops_per_round - 7))));

        for (0..op_count) |op_idx| {
            const service_flags = 700 + @as(i32, @intCast(round_idx * max_ops_per_round + op_idx));
            const timestamp = timestamp_base + 10 + @as(i64, @intCast(op_idx));
            const query_kind: u8 = @intCast(rng.bounded(4));
            switch (query_kind) {
                0 => {
                    poll_handles[op_idx] = try poll_api.requestStateHashAsync(service_flags, timestamp);
                    callback_handles[op_idx] = try callback_api.requestStateHashAsync(service_flags, timestamp);
                },
                1 => {
                    poll_handles[op_idx] = try poll_api.requestSingleUserReportAsync(service_flags, timestamp, uid);
                    callback_handles[op_idx] = try callback_api.requestSingleUserReportAsync(service_flags, timestamp, uid);
                },
                2 => {
                    poll_handles[op_idx] = try poll_api.requestSingleUserReportAsync(
                        service_flags,
                        timestamp,
                        missing_uid + @as(i64, @intCast(op_idx)),
                    );
                    callback_handles[op_idx] = try callback_api.requestSingleUserReportAsync(
                        service_flags,
                        timestamp,
                        missing_uid + @as(i64, @intCast(op_idx)),
                    );
                },
                else => {
                    poll_handles[op_idx] = try poll_api.requestTotalCurrencyBalanceAsync(service_flags, timestamp);
                    callback_handles[op_idx] = try callback_api.requestTotalCurrencyBalanceAsync(service_flags, timestamp);
                },
            }
        }

        var collector: DigestCollector = .{};
        const callback: CommandResponseCallback = .{
            .context = &collector,
            .callback = DigestCollector.onResponse,
        };
        for (callback_handles[0..op_count]) |handle| {
            try callback_api.registerResultCallback(handle, callback);
        }
        try std.testing.expectEqual(op_count, callback_api.pendingCallbackCount());

        const early_attempts = 1 + @as(usize, @intCast(rng.bounded(@as(u64, op_count))));
        for (0..early_attempts) |_| {
            const idx = @as(usize, @intCast(rng.bounded(@as(u64, op_count))));
            try std.testing.expect(!(try callback_api.processRegisteredCallback(callback_handles[idx])));
        }
        try std.testing.expectEqual(@as(usize, 0), collector.count);
        try std.testing.expectEqual(op_count, callback_api.pendingCallbackCount());
        try std.testing.expectEqual(@as(usize, 0), try callback_api.processPendingCallbacks());

        const poll_processed = try poll_api.drainPending();
        const callback_processed = try callback_api.drainPending();
        try std.testing.expectEqual(op_count, poll_processed);
        try std.testing.expectEqual(op_count, callback_processed);

        var expected_digests: [max_ops_per_round]ResponseDigest = undefined;
        for (poll_handles[0..op_count], 0..) |handle, idx| {
            var response = (try poll_api.takeFullResponse(handle)) orelse return error.MissingAsyncResult;
            defer response.deinit(allocator);
            expected_digests[idx] = digestFromResponse(&response);
        }

        var permutation: [max_ops_per_round]usize = undefined;
        for (0..op_count) |idx| {
            permutation[idx] = idx;
        }
        if (op_count > 1) {
            var i = op_count - 1;
            while (i > 0) : (i -= 1) {
                const j = @as(usize, @intCast(rng.bounded(@as(u64, i + 1))));
                std.mem.swap(usize, &permutation[i], &permutation[j]);
            }
        }

        var processed_immediate: [max_ops_per_round]bool = [_]bool{false} ** max_ops_per_round;
        var immediate_count: usize = 0;
        for (permutation[0..op_count]) |idx| {
            const action: u8 = @intCast(rng.bounded(3));
            if (action == 2) {
                continue;
            }

            const before = collector.count;
            try std.testing.expect(try callback_api.processRegisteredCallback(callback_handles[idx]));
            try std.testing.expectEqual(before + 1, collector.count);
            try expectDigestEqual(expected_digests[idx], collector.digests[before]);
            processed_immediate[idx] = true;
            immediate_count += 1;

            if (action == 0) {
                try std.testing.expect(!(try callback_api.processRegisteredCallback(callback_handles[idx])));
                try std.testing.expectEqual(before + 1, collector.count);
            }
        }

        const deferred_count = op_count - immediate_count;
        try std.testing.expectEqual(deferred_count, callback_api.pendingCallbackCount());

        const batch_start = collector.count;
        try std.testing.expectEqual(deferred_count, try callback_api.processPendingCallbacks());
        try std.testing.expectEqual(op_count, collector.count);
        try std.testing.expectEqual(@as(usize, 0), callback_api.pendingCallbackCount());
        try std.testing.expectEqual(@as(usize, 0), try callback_api.processPendingCallbacks());

        var deferred_cursor = batch_start;
        for (0..op_count) |idx| {
            if (processed_immediate[idx]) continue;
            try expectDigestEqual(expected_digests[idx], collector.digests[deferred_cursor]);
            deferred_cursor += 1;
        }
        try std.testing.expectEqual(op_count, deferred_cursor);

        for (callback_handles[0..op_count]) |handle| {
            try std.testing.expect(callback_api.takeResult(handle) == null);
        }
    }
}

test "exchange api callback registry enforces capacity and overwrites existing handle callback" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);

    var handles: [MaxCallbackRegistrations]ApiCommandAsyncHandle = undefined;
    for (0..MaxCallbackRegistrations) |idx| {
        handles[idx] = try api.submitCreateUserAsync(400_000 + @as(i64, @intCast(idx)));
    }

    var base_collector: CountCollector = .{};
    const base_callback: CommandResponseCallback = .{
        .context = &base_collector,
        .callback = CountCollector.onResponse,
    };
    for (handles) |handle| {
        try api.registerResultCallback(handle, base_callback);
    }
    try std.testing.expectEqual(@as(usize, MaxCallbackRegistrations), api.pendingCallbackCount());

    const overflow_handle = try api.submitCreateUserAsync(999_999);
    try std.testing.expectError(
        error.CallbackRegistryFull,
        api.registerResultCallback(overflow_handle, base_callback),
    );
    try std.testing.expectEqual(@as(usize, MaxCallbackRegistrations), api.pendingCallbackCount());

    var overwrite_collector: CountCollector = .{};
    const overwrite_callback: CommandResponseCallback = .{
        .context = &overwrite_collector,
        .callback = CountCollector.onResponse,
    };
    try api.registerResultCallback(handles[0], overwrite_callback);
    try std.testing.expectEqual(@as(usize, MaxCallbackRegistrations), api.pendingCallbackCount());

    const drained = try api.drainPending();
    try std.testing.expectEqual(@as(usize, MaxCallbackRegistrations + 1), drained);

    try std.testing.expectEqual(@as(usize, MaxCallbackRegistrations), try api.processPendingCallbacks());
    try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, MaxCallbackRegistrations - 1), base_collector.count);
    try std.testing.expectEqual(@as(usize, 1), overwrite_collector.count);

    const overflow_result = api.takeResultCode(overflow_handle) orelse return error.MissingAsyncResult;
    try std.testing.expectEqual(cmd.CommandResultCode.success, overflow_result);
}

test "exchange api callback registry slots are reusable across repeated full-capacity rounds" {
    const allocator = std.testing.allocator;
    const rounds: usize = 4;

    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    var handles: [MaxCallbackRegistrations]ApiCommandAsyncHandle = undefined;

    for (0..rounds) |round_idx| {
        for (0..MaxCallbackRegistrations) |idx| {
            const uid = 500_000 + @as(i64, @intCast(round_idx * MaxCallbackRegistrations + idx));
            handles[idx] = try api.submitCreateUserAsync(uid);
        }

        var collector: CountCollector = .{};
        const callback: CommandResponseCallback = .{
            .context = &collector,
            .callback = CountCollector.onResponse,
        };

        for (handles) |handle| {
            try api.registerResultCallback(handle, callback);
        }
        try std.testing.expectEqual(@as(usize, MaxCallbackRegistrations), api.pendingCallbackCount());

        const drained = try api.drainPending();
        try std.testing.expectEqual(@as(usize, MaxCallbackRegistrations), drained);
        try std.testing.expectEqual(@as(usize, MaxCallbackRegistrations), try api.processPendingCallbacks());
        try std.testing.expectEqual(@as(usize, MaxCallbackRegistrations), collector.count);
        try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());
        try std.testing.expectEqual(@as(usize, 0), try api.processPendingCallbacks());
        for (handles) |handle| {
            try std.testing.expect(api.takeResult(handle) == null);
        }

        // Probe immediate slot reuse after a full-capacity drain/process cycle.
        const probe_handle = try api.submitCreateUserAsync(900_000 + @as(i64, @intCast(round_idx)));
        try api.registerResultCallback(probe_handle, callback);
        try std.testing.expectEqual(@as(usize, 1), api.pendingCallbackCount());

        const probe_drained = try api.drainPending();
        try std.testing.expectEqual(@as(usize, 1), probe_drained);
        try std.testing.expect(try api.processRegisteredCallback(probe_handle));
        try std.testing.expectEqual(@as(usize, MaxCallbackRegistrations + 1), collector.count);
        try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());
        try std.testing.expect(!(try api.processRegisteredCallback(probe_handle)));
    }
}

test "exchange api stream sync helper returns nop barrier response" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const commands = [_]ApiCommand{
        .{ .add_user = 95_201 },
        .{
            .adjust_user_balance = .{
                .uid = 95_201,
                .transaction_id = 1,
                .currency = 15,
                .amount = 99_000,
                .adjustment_type = .adjustment,
            },
        },
    };
    var response = try api.submitCommandsSyncWithNop(commands[0..]);
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
}

test "exchange api typed binary add-symbols sync helper makes symbol queryable" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const spec = try makeExchangeSpecForSymbol(9_241);
    try api.submitAddSymbolsBinary(5_001, 9_300_100_001, &.{spec});

    var report = try core.queryReport(.{
        .order_book = .{
            .symbol = 9_241,
            .depth = 8,
        },
    });
    defer report.deinit(allocator);
    switch (report) {
        .order_book => |snapshot| {
            try std.testing.expectEqual(@as(usize, 0), snapshot.bids.items.len);
            try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len);
        },
        else => return error.UnexpectedReportResponseType,
    }
}

test "exchange api typed binary add-accounts async helper applies balances and returns success" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const accounts = [_]BinaryCurrencyBalance{
        .{ .currency = 11, .balance = 7_000 },
        .{ .currency = 15, .balance = 4_000 },
    };
    const users = [_]BinaryUserAccountsBatch{
        .{
            .uid = 96_901,
            .accounts = accounts[0..],
        },
    };
    const handle = try api.submitAddAccountsBinaryAsync(5_002, 9_300_100_002, users[0..]);
    const processed = try api.drainPending();
    try std.testing.expect(processed >= 1);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(handle));

    var ccy11 = try core.queryReport(.{
        .user_balance = .{
            .uid = 96_901,
            .currency = 11,
        },
    });
    defer ccy11.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 7_000), ccy11.user_balance);

    var ccy15 = try core.queryReport(.{
        .user_balance = .{
            .uid = 96_901,
            .currency = 15,
        },
    });
    defer ccy15.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 4_000), ccy15.user_balance);
}

test "exchange api raw binary command payload async wrapper applies add-symbols payload" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const spec = try makeExchangeSpecForSymbol(9_301);
    const payload = try binary_payload.encodeAddSymbolsToBytes(allocator, &.{spec});
    defer allocator.free(payload);

    const handle = try api.submitBinaryDataPayloadAsync(5_101, 9_400_100_001, payload);
    const processed = try api.drainPending();
    try std.testing.expect(processed >= 1);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(handle));

    var report = try core.queryReport(.{
        .order_book = .{
            .symbol = 9_301,
            .depth = 8,
        },
    });
    defer report.deinit(allocator);
    switch (report) {
        .order_book => |snapshot| {
            try std.testing.expectEqual(@as(usize, 0), snapshot.bids.items.len);
            try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len);
        },
        else => return error.UnexpectedReportResponseType,
    }
}

test "exchange api raw binary query payload async wrapper returns report payload" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const payload = try binary_payload.encodeReportQueryToBytes(allocator, .state_hash);
    defer allocator.free(payload);

    const handle = try api.submitBinaryQueryPayloadAsync(5_102, 9_400_100_002, payload);
    const processed = try api.drainPending();
    try std.testing.expect(processed >= 1);

    var report = (try api.takeReportResponse(handle)) orelse return error.MissingAsyncResult;
    defer report.deinit(allocator);
    switch (report) {
        .state_hash_detailed => |detailed| {
            try std.testing.expect(detailed.submodules.items.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }
}

test "exchange api raw binary async wrappers reject empty payloads" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const empty: [0]u8 = .{};
    try std.testing.expectError(
        error.EmptyBinaryPayload,
        api.submitBinaryDataPayloadAsync(5_103, 9_400_100_003, empty[0..]),
    );
    try std.testing.expectError(
        error.EmptyBinaryPayload,
        api.submitBinaryQueryPayloadAsync(5_104, 9_400_100_004, empty[0..]),
    );
}

test "exchange api generic submitBinaryCommandAsync alias resolves encoded add-symbols payload" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const spec = try makeExchangeSpecForSymbol(9_302);
    const payload = try binary_payload.encodeAddSymbolsToBytes(allocator, &.{spec});
    defer allocator.free(payload);

    const handle = try api.submitBinaryCommandAsync(5_105, 9_400_100_005, payload);
    const processed = try api.drainPending();
    try std.testing.expect(processed >= 1);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(handle));
}

test "exchange api generic submitQueryAsync alias resolves report payload" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const handle = try api.submitQueryAsync(5_106, 9_400_100_006, .state_hash);
    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 1), processed);

    var report = (try api.takeReportResponse(handle)) orelse return error.MissingAsyncResult;
    defer report.deinit(allocator);
    switch (report) {
        .state_hash_detailed => |detailed| {
            try std.testing.expect(detailed.submodules.items.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }
}

test "exchange api publishBinaryData alias resolves encoded add-symbols payload" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const spec = try makeExchangeSpecForSymbol(9_303);
    const payload = try binary_payload.encodeAddSymbolsToBytes(allocator, &.{spec});
    defer allocator.free(payload);

    const handle = try api.publishBinaryData(5_107, 9_400_100_007, payload);
    const processed = try api.drainPending();
    try std.testing.expect(processed >= 1);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(handle));
}

test "exchange api publishQuery alias resolves report payload" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const handle = try api.publishQuery(5_108, 9_400_100_008, .state_hash);
    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 1), processed);

    var report = (try api.takeReportResponse(handle)) orelse return error.MissingAsyncResult;
    defer report.deinit(allocator);
    switch (report) {
        .state_hash_detailed => |detailed| {
            try std.testing.expect(detailed.submodules.items.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }
}

test "exchange api metadata entrypoints map to raw command fields in journaled input" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    try api.createUser(11, 101, 1_001, 96_001);
    try api.suspendUser(12, 102, 1_002, 96_001);
    try api.resumeUser(13, 103, 1_003, 96_001);
    try api.balanceAdjustment(14, 104, 1_004, 96_001, 77, 15, 120_000, .adjustment);
    try api.placeNewOrder(15, 105, 1_005, 70_001, 9, 101, 102, 7, .bid, .gtc, 241, 96_001);
    try api.moveOrder(16, 106, 1_006, 103, 70_001, 241, 96_001);
    try api.cancelOrder(17, 107, 1_007, 70_001, 241, 96_001);
    try api.reduceOrder(18, 108, 1_008, 2, 70_001, 241, 96_001);
    try api.binaryData(19, 109, 1_009, -1, 11, 12, 13, 14, 15);
    try api.groupingControl(1_009_500, 42);
    try api.reset(1_010);
    try api.nop(1_011);

    const journal = core.journal orelse return error.ExpectedJournal;
    try std.testing.expectEqual(@as(usize, 12), journal.entries.items.len);

    try std.testing.expectEqual(cmd.OrderCommandType.add_user, journal.entries.items[0].input.command);
    try std.testing.expectEqual(@as(i64, -1), journal.entries.items[0].input.order_id);
    try std.testing.expectEqual(@as(i32, -1), journal.entries.items[0].input.symbol);
    try std.testing.expectEqual(@as(i64, 96_001), journal.entries.items[0].input.uid);
    try std.testing.expectEqual(@as(i32, 11), journal.entries.items[0].input.service_flags);
    try std.testing.expectEqual(@as(i64, 101), journal.entries.items[0].input.events_group);
    try std.testing.expectEqual(@as(i64, 1_001), journal.entries.items[0].input.timestamp);

    try std.testing.expectEqual(cmd.OrderCommandType.balance_adjustment, journal.entries.items[3].input.command);
    try std.testing.expectEqual(@as(i64, 77), journal.entries.items[3].input.order_id);
    try std.testing.expectEqual(@as(i32, 15), journal.entries.items[3].input.symbol);
    try std.testing.expectEqual(@as(i64, 120_000), journal.entries.items[3].input.price);
    try std.testing.expectEqual(@as(?common.OrderType, .gtc), journal.entries.items[3].input.order_type);
    try std.testing.expectEqual(@as(i32, 14), journal.entries.items[3].input.service_flags);
    try std.testing.expectEqual(@as(i64, 104), journal.entries.items[3].input.events_group);
    try std.testing.expectEqual(@as(i64, 1_004), journal.entries.items[3].input.timestamp);

    try std.testing.expectEqual(cmd.OrderCommandType.place_order, journal.entries.items[4].input.command);
    try std.testing.expectEqual(@as(i64, 70_001), journal.entries.items[4].input.order_id);
    try std.testing.expectEqual(@as(i32, 9), journal.entries.items[4].input.user_cookie);
    try std.testing.expectEqual(@as(i64, 101), journal.entries.items[4].input.price);
    try std.testing.expectEqual(@as(i64, 102), journal.entries.items[4].input.reserve_bid_price);
    try std.testing.expectEqual(@as(i64, 7), journal.entries.items[4].input.size);
    try std.testing.expectEqual(@as(?common.OrderAction, .bid), journal.entries.items[4].input.action);
    try std.testing.expectEqual(@as(?common.OrderType, .gtc), journal.entries.items[4].input.order_type);
    try std.testing.expectEqual(@as(i32, 241), journal.entries.items[4].input.symbol);
    try std.testing.expectEqual(@as(i64, 96_001), journal.entries.items[4].input.uid);
    try std.testing.expectEqual(@as(i32, 15), journal.entries.items[4].input.service_flags);
    try std.testing.expectEqual(@as(i64, 105), journal.entries.items[4].input.events_group);
    try std.testing.expectEqual(@as(i64, 1_005), journal.entries.items[4].input.timestamp);

    try std.testing.expectEqual(cmd.OrderCommandType.binary_data_command, journal.entries.items[8].input.command);
    try std.testing.expectEqual(@as(i32, -1), journal.entries.items[8].input.symbol);
    try std.testing.expectEqual(@as(i64, 11), journal.entries.items[8].input.order_id);
    try std.testing.expectEqual(@as(i64, 12), journal.entries.items[8].input.price);
    try std.testing.expectEqual(@as(i64, 13), journal.entries.items[8].input.reserve_bid_price);
    try std.testing.expectEqual(@as(i64, 14), journal.entries.items[8].input.size);
    try std.testing.expectEqual(@as(i64, 15), journal.entries.items[8].input.uid);
    try std.testing.expectEqual(@as(i32, 19), journal.entries.items[8].input.service_flags);
    try std.testing.expectEqual(@as(i64, 109), journal.entries.items[8].input.events_group);
    try std.testing.expectEqual(@as(i64, 1_009), journal.entries.items[8].input.timestamp);

    try std.testing.expectEqual(cmd.OrderCommandType.grouping_control, journal.entries.items[9].input.command);
    try std.testing.expectEqual(@as(i64, 42), journal.entries.items[9].input.order_id);
    try std.testing.expectEqual(@as(i64, 1_009_500), journal.entries.items[9].input.timestamp);
    try std.testing.expectEqual(@as(i32, 0), journal.entries.items[9].input.service_flags);
    try std.testing.expectEqual(@as(i64, 0), journal.entries.items[9].input.events_group);

    try std.testing.expectEqual(cmd.OrderCommandType.reset, journal.entries.items[10].input.command);
    try std.testing.expectEqual(@as(i64, 1_010), journal.entries.items[10].input.timestamp);
    try std.testing.expectEqual(@as(i32, 0), journal.entries.items[10].input.service_flags);
    try std.testing.expectEqual(@as(i64, 0), journal.entries.items[10].input.events_group);

    try std.testing.expectEqual(cmd.OrderCommandType.nop, journal.entries.items[11].input.command);
    try std.testing.expectEqual(@as(i64, 1_011), journal.entries.items[11].input.timestamp);
    try std.testing.expectEqual(@as(i32, 0), journal.entries.items[11].input.service_flags);
    try std.testing.expectEqual(@as(i64, 0), journal.entries.items[11].input.events_group);
}

test "exchange api async full-response helper returns command response" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const handle = try api.submitCommandAsyncFullResponse(.{ .add_user = 97_001 });
    try std.testing.expect(!api.isResultReady(handle));

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 1), processed);
    try std.testing.expect(api.isResultReady(handle));

    var response = (try api.takeFullResponse(handle)) orelse return error.MissingAsyncResult;
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
}

test "exchange api typed grouping-control async command resolves and preserves journal metadata" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const handle = try api.submitCommandAsyncFullResponse(.{
        .grouping_control = .{
            .timestamp_ns = 1_020_333,
            .mode = 0,
        },
    });
    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 1), processed);

    var response = (try api.takeFullResponse(handle)) orelse return error.MissingAsyncResult;
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    const journal = core.journal orelse return error.ExpectedJournal;
    try std.testing.expectEqual(@as(usize, 1), journal.entries.items.len);
    try std.testing.expectEqual(cmd.OrderCommandType.grouping_control, journal.entries.items[0].input.command);
    try std.testing.expectEqual(@as(i64, 0), journal.entries.items[0].input.order_id);
    try std.testing.expectEqual(@as(i64, 1_020_333), journal.entries.items[0].input.timestamp);
}

test "exchange api async result-code helper handles duplicate add-user deterministically" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const first = try api.submitCommandAsyncResultCode(.{ .add_user = 97_101 });
    const duplicate = try api.submitCommandAsyncResultCode(.{ .add_user = 97_101 });

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 2), processed);

    try std.testing.expectEqual(
        @as(?cmd.CommandResultCode, .success),
        api.takeResultCode(first),
    );
    try std.testing.expectEqual(
        @as(?cmd.CommandResultCode, .user_mgmt_user_already_exists),
        api.takeResultCode(duplicate),
    );
}

test "exchange api async helpers keep persist-state behavior explicit" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const spec = try makeExchangeSpecForSymbol(241);
    const persist_handle = try api.submitCommandAsyncResultCode(.{
        .persist_state = .{
            .snapshot_id = 77_777,
            .timestamp_ns = 9_000_111_222,
        },
    });
    try std.testing.expectError(
        error.UnsupportedAsyncFullResponseCommand,
        api.submitCommandAsyncFullResponse(.{
            .persist_state = .{
                .snapshot_id = 88_888,
                .timestamp_ns = 9_000_333_444,
            },
        }),
    );
    try std.testing.expectError(
        error.UnsupportedAsyncFullResponseCommand,
        api.submitCommandAsyncFullResponse(.{
            .binary_add_symbols = .{
                .transfer_id = 4_401,
                .timestamp = 9_000_333_445,
                .symbols = &.{spec},
            },
        }),
    );

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 2), processed);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(persist_handle));
}

test "exchange api observability wrappers forward lifecycle and queue counters" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    try std.testing.expect(!api.isStarted());
    try std.testing.expect(!api.isStopped());
    try std.testing.expect(api.journalingEnabled());
    try std.testing.expectEqual(@as(usize, 0), api.journalLen());
    try std.testing.expectEqual(@as(usize, 0), api.asyncPendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.asyncReadyCount());
    try std.testing.expectEqual(@as(usize, 0), api.runtimePendingCount());

    const handle = try api.submitCommandAsync(.{ .add_user = 98_801 });
    try std.testing.expectEqual(@as(usize, 1), api.asyncPendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.asyncReadyCount());

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 1), processed);
    try std.testing.expectEqual(@as(usize, 0), api.asyncPendingCount());
    try std.testing.expectEqual(@as(usize, 1), api.asyncReadyCount());
    try std.testing.expect(api.isResultReady(handle));

    _ = api.takeResultCode(handle) orelse return error.MissingAsyncResult;
    try std.testing.expectEqual(@as(usize, 0), api.asyncReadyCount());

    try api.startup();
    try std.testing.expect(api.isStarted());
    try api.shutdown();
    try std.testing.expect(api.isStopped());
}

test "exchange api runtime queue wrappers execute enqueued commands" {
    const allocator = std.testing.allocator;
    const uid: i64 = 98_901;

    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();
    var api = ExchangeApi.init(allocator, &core);

    try api.enqueueRuntime(.{
        .command = .add_user,
        .order_id = -1,
        .symbol = -1,
        .uid = uid,
        .result_code = .new,
    });
    try std.testing.expectEqual(@as(usize, 1), api.runtimePendingCount());

    const outcome = try api.drainRuntime();
    try std.testing.expectEqual(@as(usize, 1), outcome.groups_processed);
    try std.testing.expectEqual(@as(usize, 1), outcome.commands_processed);
    try std.testing.expectEqual(cmd.CommandResultCode.success, outcome.last_merged_result);
    try std.testing.expectEqual(@as(usize, 0), api.runtimePendingCount());

    var user_report = try api.queryReport(.{ .single_user = uid });
    defer user_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user_report.single_user.query_execution_status,
    );
}

test "exchange api default persistence file-name and path wrappers match core builders" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAPIPATHS",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = false,
        },
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "api-path-builders",
        },
    });
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const snapshot_id: u64 = 551;

    const api_snapshot_name = try api.defaultPersistenceSnapshotFileName(allocator, snapshot_id, .risk_engine, 0);
    defer allocator.free(api_snapshot_name);
    const core_snapshot_name = try core.defaultPersistenceSnapshotFileName(allocator, snapshot_id, .risk_engine, 0);
    defer allocator.free(core_snapshot_name);
    try std.testing.expectEqualStrings(core_snapshot_name, api_snapshot_name);

    const api_journal_name = try api.defaultPersistenceJournalFileName(allocator, snapshot_id, 7);
    defer allocator.free(api_journal_name);
    const core_journal_name = try core.defaultPersistenceJournalFileName(allocator, snapshot_id, 7);
    defer allocator.free(core_journal_name);
    try std.testing.expectEqualStrings(core_journal_name, api_journal_name);

    const api_main_log_name = try api.defaultPersistenceMainLogFileName(allocator);
    defer allocator.free(api_main_log_name);
    const core_main_log_name = try core.defaultPersistenceMainLogFileName(allocator);
    defer allocator.free(core_main_log_name);
    try std.testing.expectEqualStrings(core_main_log_name, api_main_log_name);

    const api_snapshot_path = try api.defaultPersistenceSnapshotPath(allocator, snapshot_id, .risk_engine, 0);
    defer allocator.free(api_snapshot_path);
    const core_snapshot_path = try core.defaultPersistenceSnapshotPath(allocator, snapshot_id, .risk_engine, 0);
    defer allocator.free(core_snapshot_path);
    try std.testing.expectEqualStrings(core_snapshot_path, api_snapshot_path);

    const api_journal_path = try api.defaultPersistenceJournalPath(allocator, snapshot_id, 7);
    defer allocator.free(api_journal_path);
    const core_journal_path = try core.defaultPersistenceJournalPath(allocator, snapshot_id, 7);
    defer allocator.free(core_journal_path);
    try std.testing.expectEqualStrings(core_journal_path, api_journal_path);

    const api_main_log_path = try api.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(api_main_log_path);
    const core_main_log_path = try core.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(core_main_log_path);
    try std.testing.expectEqualStrings(core_main_log_path, api_main_log_path);
}

test "exchange api persist-state main-log wrapper appends entries using provided dir" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "persist-main-log",
        },
    });
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const first = try api.persistStateWithDefaultMainLog(
        tmp.dir,
        91_001,
        5_111_222,
        .risk_engine,
        0,
    );
    try std.testing.expectEqual(cmd.CommandResultCode.success, first);

    const second = try api.persistStateWithDefaultMainLog(
        tmp.dir,
        91_002,
        5_111_333,
        .matching_engine_router,
        1,
    );
    try std.testing.expectEqual(cmd.CommandResultCode.success, second);

    const main_log_path = try core.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    var main_log_file = try tmp.dir.openFile(main_log_path, .{});
    defer main_log_file.close();
    const main_log_contents = try main_log_file.readToEndAlloc(allocator, 8192);
    defer allocator.free(main_log_contents);

    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "snapshotId=91001") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "timestampNs=5111222") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "type=RE") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "instance=0") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "snapshotId=91002") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "timestampNs=5111333") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "type=ME") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "instance=1") != null);

    var line_count: usize = 0;
    for (main_log_contents) |ch| {
        if (ch == '\n') line_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), line_count);
}

test "exchange api initFromJavaConfig helper returns usable core instance" {
    const allocator = std.testing.allocator;

    var core = try ExchangeApi.initFromJavaConfig(allocator, .{});
    defer core.deinit();
    var api = ExchangeApi.init(allocator, &core);

    try api.submitCommand(.{ .add_user = 98_601 });
}

test "exchange api deinit remains non-owning over core lifecycle" {
    const allocator = std.testing.allocator;

    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    errdefer core.deinit();
    var api = ExchangeApi.init(allocator, &core);

    api.deinit();

    var response = try core.addUser(98_602);
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    core.deinit();
}

test "exchange api lifecycle startup/shutdown wrappers forward core state transitions" {
    const allocator = std.testing.allocator;

    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();
    var api = ExchangeApi.init(allocator, &core);

    try std.testing.expect(!core.isStarted());
    try std.testing.expect(!core.isStopped());

    try api.startup();
    try std.testing.expect(core.isStarted());
    try std.testing.expect(!core.isStopped());

    try api.shutdown();
    try std.testing.expect(core.isStopped());
    try std.testing.expectError(error.CoreStopped, api.startup());
}

test "exchange api bootstrapFromPersistence wrapper forwards clean-start bootstrap without startup side effects" {
    const allocator = std.testing.allocator;

    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();
    var api = ExchangeApi.init(allocator, &core);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const outcome = try api.bootstrapFromPersistence(tmp.dir, null, null);

    try std.testing.expect(!core.isStarted());
    try std.testing.expect(!outcome.snapshot_requested);
    try std.testing.expect(!outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);
}

test "exchange api startupFromPersistence wrapper forwards clean-start bootstrap" {
    const allocator = std.testing.allocator;

    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();
    var api = ExchangeApi.init(allocator, &core);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const outcome = try api.startupFromPersistence(tmp.dir, null, null);

    try std.testing.expect(core.isStarted());
    try std.testing.expect(!outcome.snapshot_requested);
    try std.testing.expect(!outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);
}

test "exchange api startupFromConfiguredPersistence wrapper forwards configured snapshot bootstrap" {
    const allocator = std.testing.allocator;

    var source = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAPICFG",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = false,
        },
        .serialization = .{
            .backend = .disk,
            .enable_journaling = false,
        },
    });
    defer source.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.captureSnapshotToDefaultPath(tmp.dir, 201, .risk_engine, 0, &.{}, 8);

    var target = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAPICFG",
            .snapshot_id = 201,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = true,
        },
        .serialization = .{
            .backend = .disk,
            .enable_journaling = false,
        },
    });
    defer target.deinit();
    var api = ExchangeApi.init(allocator, &target);

    const outcome = try api.startupFromConfiguredPersistence(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(u64, 201), target.persistenceCurrentSnapshot().snapshot_id);
}

test "exchange api discovered startup helper forwards success path and latest snapshot selection" {
    const allocator = std.testing.allocator;

    var source = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAPIDISC",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = false,
        },
        .serialization = .{
            .backend = .disk,
            .enable_journaling = false,
        },
    });
    defer source.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.captureSnapshotToDefaultPath(tmp.dir, 41, .risk_engine, 0, &.{}, 8);
    try source.captureSnapshotToDefaultPath(tmp.dir, 42, .risk_engine, 0, &.{}, 8);
    try source.captureSnapshotToDefaultPath(tmp.dir, 99, .matching_engine_router, 1, &.{}, 8);

    var target = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAPIDISC",
            .snapshot_id = 1,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = true,
        },
        .serialization = .{
            .backend = .disk,
            .enable_journaling = false,
        },
    });
    defer target.deinit();

    var api = ExchangeApi.init(allocator, &target);
    const outcome = try api.startupFromDiscoveredConfiguredPersistence(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(u64, 42), target.persistenceCurrentSnapshot().snapshot_id);
}

test "exchange api strict discovered startup helper propagates malformed main-log entries" {
    const allocator = std.testing.allocator;

    var source = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAPIBADLOG",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = false,
        },
        .serialization = .{
            .backend = .disk,
            .enable_journaling = false,
        },
    });
    defer source.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.captureSnapshotToDefaultPath(tmp.dir, 101, .risk_engine, 0, &.{}, 8);

    const main_log_path = try source.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    var main_log_file = try tmp.dir.createFile(main_log_path, .{ .truncate = true });
    defer main_log_file.close();
    try main_log_file.writeAll("malformed\n");

    var target = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAPIBADLOG",
            .snapshot_id = 1,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = true,
        },
        .serialization = .{
            .backend = .disk,
            .enable_journaling = false,
        },
    });
    defer target.deinit();

    var api = ExchangeApi.init(allocator, &target);
    try std.testing.expectError(
        error.MalformedMainLogEntry,
        api.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
            tmp.dir,
            .risk_engine,
            0,
            1,
        ),
    );
}

test "exchange api persist-state async handles reject callback registration and preserve polling path" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const persist_poll_handle = try api.submitCommandAsyncResultCode(.{
        .persist_state = .{
            .snapshot_id = 99_001,
            .timestamp_ns = 9_901_111_222,
        },
    });
    const persist_process_handle = try api.submitCommandAsyncResultCode(.{
        .persist_state = .{
            .snapshot_id = 99_002,
            .timestamp_ns = 9_901_111_333,
        },
    });

    var collector: CountCollector = .{};
    const callback: CommandResponseCallback = .{
        .context = &collector,
        .callback = CountCollector.onResponse,
    };

    try std.testing.expectError(
        error.UnsupportedPersistStateHandle,
        api.registerResultCallback(persist_poll_handle, callback),
    );
    try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());
    try std.testing.expectError(
        error.UnsupportedPersistStateHandle,
        api.processRegisteredCallback(persist_poll_handle),
    );
    try std.testing.expect(!(try api.processResult(persist_process_handle, callback)));
    try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());

    const processed = try api.drainPending();
    try std.testing.expect(processed >= 2);
    try std.testing.expectError(
        error.UnexpectedPersistStateResult,
        api.processResult(persist_process_handle, callback),
    );
    try std.testing.expectEqual(@as(usize, 0), try api.processPendingCallbacks());
    try std.testing.expectEqual(@as(usize, 0), collector.count);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(persist_poll_handle));
}

test "exchange api typed persist-state async helper matches manual persist-state handle path" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const typed = try api.submitPersistStateAsync(100_001, 9_100_111_222);
    const manual = try api.submitCommandAsyncResultCode(.{
        .persist_state = .{
            .snapshot_id = 100_002,
            .timestamp_ns = 9_100_111_333,
        },
    });

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 4), processed);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(typed));
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(manual));
}

test "exchange api typed persist-state one-shot helper matches manual process path" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const typed = try api.processPersistStateResultCode(100_101, 9_101_111_222);
    const manual = try api.processCommandResultCode(.{
        .persist_state = .{
            .snapshot_id = 100_102,
            .timestamp_ns = 9_101_111_333,
        },
    });
    try std.testing.expectEqual(cmd.CommandResultCode.success, typed);
    try std.testing.expectEqual(cmd.CommandResultCode.success, manual);
}

test "exchange api forwards persistence snapshot-id lookup helpers" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    try std.testing.expectEqual(@as(usize, 1), api.persistenceSnapshotCount());
    try std.testing.expectEqual(@as(usize, 0), api.persistenceJournalCount());
    const root_ref = api.persistenceFindSnapshotById(0) orelse return error.ExpectedSnapshot;
    const root_snapshot = api.persistenceSnapshotById(0) orelse return error.ExpectedSnapshot;
    const current_snapshot = api.persistenceCurrentSnapshot();
    try std.testing.expectEqual(root_ref, api.persistenceCurrentSnapshotRef());
    try std.testing.expectEqual(root_ref, core.persistenceCurrentSnapshotRef());
    try std.testing.expectEqual(@as(u64, 0), root_snapshot.snapshot_id);
    try std.testing.expectEqual(@as(u64, 0), current_snapshot.snapshot_id);

    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try api.processPersistStateResultCode(120_001, 9_120_111_222),
    );
    try std.testing.expectEqual(@as(usize, 2), api.persistenceSnapshotCount());
    try std.testing.expectEqual(@as(u64, 120_001), api.persistenceCurrentSnapshot().snapshot_id);
    try std.testing.expectEqual(core.persistenceCurrentSnapshotRef(), api.persistenceCurrentSnapshotRef());
    const persisted = api.persistenceSnapshotById(120_001) orelse return error.ExpectedSnapshot;
    try std.testing.expectEqual(@as(u64, 120_001), persisted.snapshot_id);

    try std.testing.expectEqual(@as(?SnapshotRef, null), api.persistenceFindSnapshotById(999_999));
    try std.testing.expectEqual(@as(?*const SnapshotDescriptor, null), api.persistenceSnapshotById(999_999));
}

test "exchange api forwards persistence journal lookup helpers" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    try api.submitCommand(.{ .add_user = 120_101 });
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try core.writeJournalToDefaultPath(tmp.dir, 0, 1);

    const journal_ref = api.persistenceFindJournalForSequence(1) orelse return error.ExpectedJournal;
    const journal = api.persistenceJournalByRef(journal_ref);
    try std.testing.expectEqual(@as(u64, 1), journal.seq_first);
    const seq_last = journal.seq_last orelse return error.ExpectedJournalSequenceEnd;
    try std.testing.expect(seq_last >= journal.seq_first);
    try std.testing.expectEqual(@as(?JournalRef, null), api.persistenceFindJournalForSequence(std.math.maxInt(u64)));
}

test "exchange api forwards snapshot existence helpers" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAPIEXISTS",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = false,
        },
        .serialization = .{
            .backend = .disk,
            .enable_journaling = false,
        },
    });
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try core.captureSnapshotToDefaultPath(tmp.dir, 301, .risk_engine, 0, &.{}, 8);

    try std.testing.expect(try api.snapshotExistsDefaultPath(tmp.dir, 301, .risk_engine, 0));
    try std.testing.expect(!(try api.snapshotExistsDefaultPath(tmp.dir, 999, .risk_engine, 0)));

    const existing_path = try core.defaultPersistenceSnapshotPath(allocator, 301, .risk_engine, 0);
    defer allocator.free(existing_path);
    const missing_path = try core.defaultPersistenceSnapshotPath(allocator, 999, .risk_engine, 0);
    defer allocator.free(missing_path);

    try std.testing.expect(try api.snapshotExistsFile(tmp.dir, existing_path));
    try std.testing.expect(!(try api.snapshotExistsFile(tmp.dir, missing_path)));
}

test "exchange api forwards default journal path wrappers for write/load/replay flows" {
    const allocator = std.testing.allocator;
    const uid: i64 = 130_101;

    var source = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAPIJPATH",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = false,
        },
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer source.deinit();
    var source_api = ExchangeApi.init(allocator, &source);
    try source_api.startup();
    try source_api.submitCommand(.{ .add_user = uid });

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source_api.writeJournalToDefaultPath(tmp.dir, 0, 1);

    var load_only = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAPIJPATH",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = false,
        },
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer load_only.deinit();
    var load_api = ExchangeApi.init(allocator, &load_only);
    try load_api.loadJournalFromDefaultPath(tmp.dir, 0, 1);
    try std.testing.expectEqual(@as(usize, 1), load_only.journalLen());

    var replay_single = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAPIJPATH",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = false,
        },
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer replay_single.deinit();
    var replay_api = ExchangeApi.init(allocator, &replay_single);
    const replayed_single = try replay_api.replayJournalFromDefaultPath(tmp.dir, 0, 1);

    var replay_range = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAPIJPATH",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = false,
        },
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer replay_range.deinit();
    var replay_range_api = ExchangeApi.init(allocator, &replay_range);
    const replayed_range = try replay_range_api.replayJournalFromDefaultPathRange(tmp.dir, 0, 1);
    try std.testing.expectEqual(replayed_single, replayed_range);
}

test "exchange api forwards default snapshot capture/validate wrappers" {
    const allocator = std.testing.allocator;

    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAPISNAP",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = false,
        },
        .serialization = .{
            .backend = .disk,
            .enable_journaling = false,
        },
    });
    defer core.deinit();
    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var api = ExchangeApi.init(allocator, &core);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try api.captureSnapshotToDefaultPath(tmp.dir, 401, .risk_engine, 0, &.{241}, 8);
    try api.validateSnapshotDefaultPath(tmp.dir, 401, .risk_engine, 0);
    try std.testing.expect(try api.snapshotExistsDefaultPath(tmp.dir, 401, .risk_engine, 0));
}

test "exchange api forwards explicit journal file wrappers" {
    const allocator = std.testing.allocator;
    const uid: i64 = 140_101;

    var source = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer source.deinit();
    var source_api = ExchangeApi.init(allocator, &source);
    try source_api.startup();
    try source_api.submitCommand(.{ .add_user = uid });

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const journal_path = "manual-path.journal";
    try source_api.writeJournalToFile(tmp.dir, journal_path);

    var load_only = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer load_only.deinit();
    var load_api = ExchangeApi.init(allocator, &load_only);
    try load_api.loadJournalFromFile(tmp.dir, journal_path);
    try std.testing.expectEqual(@as(usize, 1), load_only.journalLen());

    var replay_core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer replay_core.deinit();
    var replay_api = ExchangeApi.init(allocator, &replay_core);
    const replayed = try replay_api.replayJournalFromFile(tmp.dir, journal_path);
    try std.testing.expect(replayed <= 1);
}

test "exchange api forwards explicit snapshot file wrappers" {
    const allocator = std.testing.allocator;

    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .serialization = .{
            .backend = .disk,
            .enable_journaling = false,
        },
    });
    defer core.deinit();
    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(242)));

    var api = ExchangeApi.init(allocator, &core);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const snapshot_path = "manual-path.snapshot";
    try api.captureSnapshotToFile(tmp.dir, snapshot_path, &.{242}, 8);
    try api.validateSnapshotFile(tmp.dir, snapshot_path);
    try std.testing.expect(try api.snapshotExistsFile(tmp.dir, snapshot_path));
}

test "exchange api async binary report-query helper returns typed report response" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const handle = try api.submitBinaryReportQueryAsync(4_501, 9_200_100_333, .state_hash);
    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 1), processed);

    var report = (try api.takeReportResponse(handle)) orelse return error.MissingAsyncResult;
    defer report.deinit(allocator);
    switch (report) {
        .state_hash_detailed => |detailed| {
            try std.testing.expect(detailed.submodules.items.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }
}

test "exchange api typed state-hash async helper returns detailed report" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const handle = try api.requestStateHashAsync(4_502, 9_200_100_334);
    _ = try api.drainPending();

    var detailed = (try api.takeStateHashDetailed(handle)) orelse return error.MissingAsyncResult;
    defer detailed.deinit(allocator);
    try std.testing.expect(detailed.submodules.items.len > 0);
}

test "exchange api typed single-user report helper returns user report" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    try api.submitCommand(.{ .add_user = 97_701 });
    try api.submitCommand(.{
        .adjust_user_balance = .{
            .uid = 97_701,
            .transaction_id = 71,
            .currency = 15,
            .amount = 55_000,
            .adjustment_type = .adjustment,
        },
    });

    const handle = try api.requestSingleUserReportAsync(4_503, 9_200_100_335, 97_701);
    _ = try api.drainPending();
    var report = (try api.takeSingleUserReport(handle)) orelse return error.MissingAsyncResult;
    defer report.deinit(allocator);

    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        report.query_execution_status,
    );
    try std.testing.expectEqual(@as(i64, 97_701), report.uid);
}

test "exchange api typed total-currency-balance helper returns report payload" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const handle = try api.requestTotalCurrencyBalanceAsync(4_504, 9_200_100_336);
    _ = try api.drainPending();

    var report = (try api.takeTotalCurrencyBalanceReport(handle)) orelse return error.MissingAsyncResult;
    defer report.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), report.account_balances.count());
}

test "exchange api processReport convenience returns report payload in one step" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    var report = try api.processReport(4_506, 9_200_100_338, .state_hash);
    defer report.deinit(allocator);
    switch (report) {
        .state_hash_detailed => |detailed| {
            try std.testing.expect(detailed.submodules.items.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }
}

test "exchange api processSingleUserReport convenience returns typed user report" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    try api.submitCommand(.{ .add_user = 97_801 });
    try api.submitCommand(.{
        .adjust_user_balance = .{
            .uid = 97_801,
            .transaction_id = 81,
            .currency = 15,
            .amount = 11_000,
            .adjustment_type = .adjustment,
        },
    });

    var report = try api.processSingleUserReport(4_507, 9_200_100_339, 97_801);
    defer report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        report.query_execution_status,
    );
    try std.testing.expectEqual(@as(i64, 97_801), report.uid);
}

test "exchange api processTotalCurrencyBalanceReport convenience returns typed balance report" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    try api.submitCommand(.{ .add_user = 97_901 });
    try api.submitCommand(.{
        .adjust_user_balance = .{
            .uid = 97_901,
            .transaction_id = 91,
            .currency = 15,
            .amount = 1_000,
            .adjustment_type = .adjustment,
        },
    });

    var report = try api.processTotalCurrencyBalanceReport(4_508, 9_200_100_340);
    defer report.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1_000), report.account_balances.get(15) orelse 0);
    try std.testing.expectEqual(@as(i64, -1_000), report.adjustments.get(15) orelse 0);
}

test "exchange api report typed take rejects responses without report payload" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const handle = try api.submitCommandAsyncFullResponse(.{ .add_user = 97_601 });
    _ = try api.drainPending();

    try std.testing.expectError(error.MissingReportResponse, api.takeReportResponse(handle));
}

test "exchange api typed report take rejects mismatched report kind" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const handle = try api.requestStateHashAsync(4_505, 9_200_100_337);
    _ = try api.drainPending();

    try std.testing.expectError(error.UnexpectedReportResponseType, api.takeSingleUserReport(handle));
}

test "exchange api order-book async helper returns typed snapshot ownership" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();
    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var api = ExchangeApi.init(allocator, &core);
    const handle = try api.requestOrderBookAsync(241, 16);
    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 1), processed);

    var snapshot = (try api.takeOrderBookSnapshot(handle)) orelse return error.MissingAsyncResult;
    defer snapshot.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len);
    try std.testing.expectEqual(@as(usize, 0), snapshot.bids.items.len);
}

test "exchange api processOrderBookRequest convenience returns typed snapshot" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();
    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(242)));

    var api = ExchangeApi.init(allocator, &core);
    var snapshot = try api.processOrderBookRequest(242, 12);
    defer snapshot.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len);
    try std.testing.expectEqual(@as(usize, 0), snapshot.bids.items.len);
}

test "exchange api processOrderBookRequest propagates missing snapshot" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    try std.testing.expectError(
        error.MissingOrderBookSnapshot,
        api.processOrderBookRequest(999_999, 8),
    );
}

test "exchange api direct typed query helpers align with core report responses" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 3_201;
    const uid: i64 = 93_001;

    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();
    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(symbol)));

    var api = ExchangeApi.init(allocator, &core);
    try api.submitCommand(.{ .add_user = uid });
    try api.submitCommand(.{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 1,
            .currency = 15,
            .amount = 1_200,
            .adjustment_type = .adjustment,
        },
    });

    var expected_hash_report = try core.queryReport(.risk_state_hash);
    defer expected_hash_report.deinit(allocator);
    const expected_hash = switch (expected_hash_report) {
        .risk_state_hash => |value| value,
        else => return error.UnexpectedReportResponseType,
    };
    try std.testing.expectEqual(expected_hash, try api.queryRiskStateHash());

    var expected_fee_report = try core.queryReport(.{ .fee_collected = 15 });
    defer expected_fee_report.deinit(allocator);
    const expected_fee = switch (expected_fee_report) {
        .fee_collected => |value| value,
        else => return error.UnexpectedReportResponseType,
    };
    try std.testing.expectEqual(expected_fee, try api.queryFeeCollected(15));

    var expected_user_balance_report = try core.queryReport(.{
        .user_balance = .{
            .uid = uid,
            .currency = 15,
        },
    });
    defer expected_user_balance_report.deinit(allocator);
    const expected_user_balance = switch (expected_user_balance_report) {
        .user_balance => |value| value,
        else => return error.UnexpectedReportResponseType,
    };
    try std.testing.expectEqual(expected_user_balance, try api.queryUserBalance(uid, 15));
    try std.testing.expectEqual(@as(?i64, null), try api.queryUserBalance(9_999_999, 15));

    var expected_order_book_report = try core.queryReport(.{
        .order_book = .{
            .symbol = symbol,
            .depth = 8,
        },
    });
    errdefer expected_order_book_report.deinit(allocator);
    var expected_order_book = switch (expected_order_book_report) {
        .order_book => |snapshot| snapshot,
        else => return error.UnexpectedReportResponseType,
    };
    defer expected_order_book.deinit(allocator);

    var actual_order_book = try api.queryOrderBook(symbol, 8);
    defer actual_order_book.deinit(allocator);
    try std.testing.expectEqual(expected_order_book.asks.items.len, actual_order_book.asks.items.len);
    try std.testing.expectEqual(expected_order_book.bids.items.len, actual_order_book.bids.items.len);
}

test "exchange api direct query order-book helper propagates unknown symbol" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    try std.testing.expectError(error.UnknownSymbol, api.queryOrderBook(7_777, 8));
}

test "exchange api order-book typed take rejects responses without market data payload" {
    const allocator = std.testing.allocator;
    var core = try exchange_core_mod.ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var api = ExchangeApi.init(allocator, &core);
    const handle = try api.submitCommandAsyncFullResponse(.{ .add_user = 97_501 });
    _ = try api.drainPending();

    try std.testing.expectError(error.MissingOrderBookSnapshot, api.takeOrderBookSnapshot(handle));
}
