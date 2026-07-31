const std = @import("std");
const kernel_mod = @import("exchange_kernel.zig");
const report_registry = @import("report_registry.zig");
const runtime_mod = @import("exchange_runtime.zig");
const serialization_processor = @import("serialization_processor.zig");
const risk = @import("risk.zig");

pub const RiskProcessingMode = enum {
    full_per_currency,
    no_risk_processing,
};

pub const MarginTradingMode = enum {
    margin_trading_disabled,
    margin_trading_enabled,
};

pub const CoreWaitStrategy = runtime_mod.WaitStrategy;

/// Selects the production sharded execution engine independently from the
/// per-command routed/split publication mode persisted in the journal.
pub const ShardedPipelineMode = enum {
    immediate,
    persistent_grouped,
};

pub const PerformanceConfig = struct {
    ring_buffer_size: usize = 16 * 1024,
    matching_engines_num: u64 = 1,
    risk_engines_num: u64 = 1,
    msgs_in_group_limit: usize = 256,
    max_group_duration_ns: u64 = 10_000,
    send_l2_for_every_cmd: bool = false,
    l2_refresh_depth: usize = 8,
    wait_strategy: CoreWaitStrategy = .blocking,
    orderbook_kind: kernel_mod.OrderBookKind = .naive,

    pub const ValidationError = error{
        InvalidRingBufferSize,
        InvalidMatchingEnginesNum,
        InvalidRiskEnginesNum,
        InvalidMsgsInGroupLimit,
        InvalidL2RefreshDepth,
        InvalidWaitStrategy,
    };

    pub const MappingError = ValidationError;

    pub fn javaDefault() PerformanceConfig {
        return basePreset();
    }

    pub fn basePreset() PerformanceConfig {
        return .{
            .ring_buffer_size = 16 * 1024,
            .matching_engines_num = 1,
            .risk_engines_num = 1,
            .msgs_in_group_limit = 256,
            .max_group_duration_ns = 10_000,
            .send_l2_for_every_cmd = false,
            .l2_refresh_depth = 8,
            .wait_strategy = .blocking,
            .orderbook_kind = .naive,
        };
    }

    pub fn latencyPreset() PerformanceConfig {
        return .{
            .ring_buffer_size = 2 * 1024,
            .matching_engines_num = 1,
            .risk_engines_num = 1,
            .msgs_in_group_limit = 256,
            .max_group_duration_ns = 10_000,
            .send_l2_for_every_cmd = false,
            .l2_refresh_depth = 8,
            .wait_strategy = .busy_spin,
            .orderbook_kind = .direct,
        };
    }

    pub fn throughputPreset() PerformanceConfig {
        return .{
            .ring_buffer_size = 64 * 1024,
            .matching_engines_num = 4,
            .risk_engines_num = 2,
            .msgs_in_group_limit = 4_096,
            .max_group_duration_ns = 4_000_000,
            .send_l2_for_every_cmd = false,
            .l2_refresh_depth = 8,
            .wait_strategy = .busy_spin,
            .orderbook_kind = .direct,
        };
    }

    pub fn validate(self: PerformanceConfig) ValidationError!void {
        if (self.ring_buffer_size == 0 or @popCount(self.ring_buffer_size) != 1) {
            return error.InvalidRingBufferSize;
        }
        if (self.matching_engines_num == 0 or @popCount(self.matching_engines_num) != 1) {
            return error.InvalidMatchingEnginesNum;
        }
        if (self.risk_engines_num == 0 or @popCount(self.risk_engines_num) != 1) {
            return error.InvalidRiskEnginesNum;
        }
        if (self.msgs_in_group_limit == 0 or
            self.msgs_in_group_limit > self.ring_buffer_size / 4)
        {
            return error.InvalidMsgsInGroupLimit;
        }
        if (self.l2_refresh_depth == 0) {
            return error.InvalidL2RefreshDepth;
        }
        if (self.wait_strategy == .second_step_no_wait) {
            return error.InvalidWaitStrategy;
        }
    }

    pub fn toMatchingConfig(self: PerformanceConfig) MappingError!MatchingConfig {
        try self.validate();

        // Java presets can request multi-engine topology (e.g. throughput), but
        // this adapter currently maps into a single-kernel ExchangeCore config.
        // Sharded topology wiring uses sharded-core init paths.
        return .{
            .orderbook_kind = self.orderbook_kind,
            .shard_id = 0,
            .num_shards = 1,
            .send_l2_for_every_cmd = self.send_l2_for_every_cmd,
            .l2_refresh_depth = self.l2_refresh_depth,
        };
    }

    pub fn toRuntimeConfig(self: PerformanceConfig) ValidationError!RuntimeConfig {
        return RuntimeConfig.fromPerformanceConfig(self);
    }
};

pub const OrdersProcessingConfig = struct {
    risk_processing_mode: RiskProcessingMode = .full_per_currency,
    margin_trading_mode: MarginTradingMode = .margin_trading_disabled,

    pub fn javaDefault() OrdersProcessingConfig {
        return .{
            .risk_processing_mode = .full_per_currency,
            .margin_trading_mode = .margin_trading_enabled,
        };
    }

    pub fn toRiskConfig(self: OrdersProcessingConfig) risk.Config {
        return .{
            .ignore_risk_processing = self.risk_processing_mode == .no_risk_processing,
            .margin_trading_enabled = self.margin_trading_mode == .margin_trading_enabled,
        };
    }
};

pub const MatchingConfig = struct {
    orderbook_kind: kernel_mod.OrderBookKind = .direct,
    shard_id: u64 = 0,
    num_shards: u64 = 1,
    send_l2_for_every_cmd: bool = false,
    l2_refresh_depth: usize = 8,

    pub const ValidationError = error{
        InvalidNumShards,
        InvalidShardId,
        InvalidL2RefreshDepth,
    };

    pub fn validate(self: MatchingConfig) ValidationError!void {
        if (self.num_shards == 0 or @popCount(self.num_shards) != 1) {
            return error.InvalidNumShards;
        }
        if (self.shard_id >= self.num_shards) {
            return error.InvalidShardId;
        }
        if (self.l2_refresh_depth == 0) {
            return error.InvalidL2RefreshDepth;
        }
    }

    pub fn toKernelConfig(
        self: MatchingConfig,
        orders_processing: OrdersProcessingConfig,
    ) kernel_mod.Config {
        return .{
            .orderbook_kind = self.orderbook_kind,
            .shard_id = self.shard_id,
            .num_shards = self.num_shards,
            .send_l2_for_every_cmd = self.send_l2_for_every_cmd,
            .l2_refresh_depth = self.l2_refresh_depth,
            .risk = orders_processing.toRiskConfig(),
        };
    }
};

pub const RuntimeConfig = struct {
    queue_capacity: usize = runtime_mod.DefaultQueueCapacity,
    wait_strategy: runtime_mod.WaitStrategy = .blocking,
    max_batch_size: usize = 256,
    l2_flags_every_n_commands: usize = 0,
    l2_publish_interval_ns: u64 = runtime_mod.DefaultL2PublishIntervalNs,
    max_group_duration_ns: u64 = 0,

    pub const ValidationError = error{
        InvalidQueueCapacity,
        InvalidBatchSize,
        InvalidWaitStrategy,
    };

    pub fn fromPerformanceConfig(
        performance: PerformanceConfig,
    ) PerformanceConfig.ValidationError!RuntimeConfig {
        try performance.validate();
        return .{
            .queue_capacity = performance.ring_buffer_size,
            .wait_strategy = performance.wait_strategy,
            .max_batch_size = performance.msgs_in_group_limit,
            .l2_flags_every_n_commands = 0,
            .l2_publish_interval_ns = runtime_mod.DefaultL2PublishIntervalNs,
            .max_group_duration_ns = performance.max_group_duration_ns,
        };
    }

    pub fn validate(self: RuntimeConfig) ValidationError!void {
        if (self.queue_capacity == 0 or @popCount(self.queue_capacity) != 1) {
            return error.InvalidQueueCapacity;
        }
        if (self.max_batch_size == 0 or self.max_batch_size > self.queue_capacity / 4) {
            return error.InvalidBatchSize;
        }
        if (self.wait_strategy == .second_step_no_wait) {
            return error.InvalidWaitStrategy;
        }
    }

    pub fn toRuntimeConfig(self: RuntimeConfig) runtime_mod.Config {
        return .{
            .queue_capacity = self.queue_capacity,
            .wait_strategy = self.wait_strategy,
            .max_batch_size = self.max_batch_size,
            .l2_flags_every_n_commands = self.l2_flags_every_n_commands,
            .l2_publish_interval_ns = self.l2_publish_interval_ns,
            .max_group_duration_ns = self.max_group_duration_ns,
        };
    }
};

pub const InitialStateConfig = struct {
    exchange_id: []const u8 = "MY_EXCHANGE",
    snapshot_id: u64 = 0,
    snapshot_base_seq: u64 = 0,
    journal_timestamp_ns: u64 = 0,
    throw_if_snapshot_not_found: bool = false,

    pub const ValidationError = error{
        EmptyExchangeId,
        InvalidExchangeIdCharacter,
        SnapshotBaseSeqWithoutSnapshot,
    };

    pub fn javaDefault() InitialStateConfig {
        return cleanStart("MY_EXCHANGE");
    }

    pub fn cleanTest() InitialStateConfig {
        return cleanStart("EC0");
    }

    pub fn cleanStart(exchange_id: []const u8) InitialStateConfig {
        return .{
            .exchange_id = exchange_id,
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = false,
        };
    }

    pub fn cleanStartJournaling(exchange_id: []const u8) InitialStateConfig {
        return .{
            .exchange_id = exchange_id,
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = true,
        };
    }

    pub fn fromSnapshotOnly(
        exchange_id: []const u8,
        snapshot_id: u64,
        snapshot_base_seq: u64,
    ) InitialStateConfig {
        return .{
            .exchange_id = exchange_id,
            .snapshot_id = snapshot_id,
            .snapshot_base_seq = snapshot_base_seq,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = true,
        };
    }

    pub fn lastKnownStateFromJournal(
        exchange_id: []const u8,
        snapshot_id: u64,
        snapshot_base_seq: u64,
    ) InitialStateConfig {
        return .{
            .exchange_id = exchange_id,
            .snapshot_id = snapshot_id,
            .snapshot_base_seq = snapshot_base_seq,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = true,
        };
    }

    pub fn fromSnapshot(self: InitialStateConfig) bool {
        return self.snapshot_id != 0;
    }

    pub fn validate(self: InitialStateConfig) ValidationError!void {
        if (self.exchange_id.len == 0) return error.EmptyExchangeId;
        for (self.exchange_id) |ch| {
            if (!std.ascii.isAlphanumeric(ch) and ch != '_' and ch != '-') {
                return error.InvalidExchangeIdCharacter;
            }
        }
        if (self.snapshot_id == 0 and self.snapshot_base_seq != 0) {
            return error.SnapshotBaseSeqWithoutSnapshot;
        }
    }
};

pub const ReportTypeCode = enum(i32) {
    state_hash = 10001,
    single_user_report = 10002,
    total_currency_balance = 10003,
};

const built_in_report_codes = [_]i32{
    @intFromEnum(ReportTypeCode.state_hash),
    @intFromEnum(ReportTypeCode.single_user_report),
    @intFromEnum(ReportTypeCode.total_currency_balance),
};

pub const EmptyCustomReportRegistry: *const report_registry.Registry =
    report_registry.Registry.fromSpecs(.{});

pub const BinaryCommandTypeCode = enum(i32) {
    add_accounts = 1002,
    add_symbols = 1003,
};

pub const ReportsQueriesConfig = struct {
    enable_state_hash_report: bool = true,
    enable_single_user_report: bool = true,
    enable_total_currency_balance_report: bool = true,
    enable_add_accounts_binary: bool = true,
    enable_add_symbols_binary: bool = true,
    custom_report_registry: *const report_registry.Registry =
        EmptyCustomReportRegistry,

    pub const ValidationError =
        report_registry.ReservedCodeValidationError ||
        report_registry.RegistryContextValidationError ||
        error{
            NoReportQueriesEnabled,
            NoBinaryCommandsEnabled,
        };

    pub fn javaDefault() ReportsQueriesConfig {
        return .{};
    }

    pub fn validate(self: ReportsQueriesConfig) ValidationError!void {
        try self.custom_report_registry.validateNoReservedCodes(
            &built_in_report_codes,
        );
        try self.custom_report_registry.validateContextTypes(
            kernel_mod.MatchingReportView,
            kernel_mod.RiskReportView,
        );

        if (!self.enable_state_hash_report and
            !self.enable_single_user_report and
            !self.enable_total_currency_balance_report and
            self.custom_report_registry.len() == 0)
        {
            return error.NoReportQueriesEnabled;
        }
        if (!self.enable_add_accounts_binary and !self.enable_add_symbols_binary) {
            return error.NoBinaryCommandsEnabled;
        }
    }

    pub fn supportsReport(self: ReportsQueriesConfig, code: ReportTypeCode) bool {
        return switch (code) {
            .state_hash => self.enable_state_hash_report,
            .single_user_report => self.enable_single_user_report,
            .total_currency_balance => self.enable_total_currency_balance_report,
        };
    }

    pub fn supportsBinary(self: ReportsQueriesConfig, code: BinaryCommandTypeCode) bool {
        return switch (code) {
            .add_accounts => self.enable_add_accounts_binary,
            .add_symbols => self.enable_add_symbols_binary,
        };
    }

    pub fn toKernelConfig(self: ReportsQueriesConfig) kernel_mod.ReportQueriesConfig {
        return .{
            .state_hash = self.enable_state_hash_report,
            .single_user = self.enable_single_user_report,
            .total_currency_balance = self.enable_total_currency_balance_report,
            .add_accounts_binary = self.enable_add_accounts_binary,
            .add_symbols_binary = self.enable_add_symbols_binary,
            .custom_registry = self.custom_report_registry,
        };
    }
};

pub const LoggingLevel = enum {
    warnings,
    risk_debug,
    matching_debug,
};

pub const LoggingConfig = struct {
    warnings: bool = true,
    risk_debug: bool = false,
    matching_debug: bool = false,

    pub fn javaDefault() LoggingConfig {
        return .{
            .warnings = true,
            .risk_debug = false,
            .matching_debug = false,
        };
    }

    pub fn isEnabled(self: LoggingConfig, level: LoggingLevel) bool {
        return switch (level) {
            .warnings => self.warnings,
            .risk_debug => self.risk_debug,
            .matching_debug => self.matching_debug,
        };
    }
};

pub const SerializationBackend = enum {
    dummy,
    disk,
    custom,
};

pub const DefaultDiskStorageFolder = "./dumps";
pub const SerializationProcessorFactory = serialization_processor.Factory;
pub const SerializationProcessor = serialization_processor.Processor;

pub const SerializationConfig = struct {
    enable_journaling: bool = false,
    backend: SerializationBackend = .dummy,
    storage_folder: []const u8 = DefaultDiskStorageFolder,
    /// The descriptor is copied with the configuration and invoked during core
    /// construction. Its context is borrowed through that call only; the
    /// returned processor context is owned until the core destroys it.
    custom_processor_factory: ?SerializationProcessorFactory = null,

    pub const ValidationError = error{
        JournalingRequiresPersistentBackend,
        EmptyStorageFolder,
        CustomProcessorFactoryRequired,
        CustomProcessorFactoryUnexpected,
    };

    pub fn javaDefault() SerializationConfig {
        return .{};
    }

    pub fn diskSnapshotOnly() SerializationConfig {
        return .{
            .enable_journaling = false,
            .backend = .disk,
            .storage_folder = DefaultDiskStorageFolder,
        };
    }

    pub fn diskJournaling() SerializationConfig {
        return .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = DefaultDiskStorageFolder,
        };
    }

    pub fn custom(
        factory: SerializationProcessorFactory,
        enable_journaling: bool,
    ) SerializationConfig {
        return .{
            .enable_journaling = enable_journaling,
            .backend = .custom,
            .custom_processor_factory = factory,
        };
    }

    pub fn validate(self: SerializationConfig) ValidationError!void {
        if (self.enable_journaling and self.backend == .dummy) {
            return error.JournalingRequiresPersistentBackend;
        }
        if (self.backend == .disk and self.storage_folder.len == 0) {
            return error.EmptyStorageFolder;
        }
        if (self.backend == .custom and self.custom_processor_factory == null) {
            return error.CustomProcessorFactoryRequired;
        }
        if (self.backend != .custom and self.custom_processor_factory != null) {
            return error.CustomProcessorFactoryUnexpected;
        }
    }
};

pub const ExchangeCoreConfig = struct {
    orders_processing: OrdersProcessingConfig = .{},
    matching: MatchingConfig = .{},
    runtime: RuntimeConfig = .{},
    sharded_pipeline_mode: ShardedPipelineMode = .persistent_grouped,
    initial_state: InitialStateConfig = InitialStateConfig.javaDefault(),
    reports_queries: ReportsQueriesConfig = ReportsQueriesConfig.javaDefault(),
    /// JVM identity-derived enum hashes required only by the exact Java
    /// StateHash report lane. No portable default exists.
    java_identity_hash_context: ?kernel_mod.JavaIdentityHashContext = null,
    logging: LoggingConfig = LoggingConfig.javaDefault(),
    serialization: SerializationConfig = SerializationConfig.javaDefault(),

    pub const ValidationError = MatchingConfig.ValidationError ||
        RuntimeConfig.ValidationError ||
        InitialStateConfig.ValidationError ||
        ReportsQueriesConfig.ValidationError ||
        SerializationConfig.ValidationError ||
        error{
            SnapshotLoadRequiresPersistentBackend,
            JournalReplayRequiresJournalingEnabled,
        };

    pub fn validate(self: ExchangeCoreConfig) ValidationError!void {
        try self.matching.validate();
        try self.runtime.validate();
        try self.initial_state.validate();
        try self.reports_queries.validate();
        try self.serialization.validate();

        if (self.initial_state.fromSnapshot() and self.serialization.backend == .dummy) {
            return error.SnapshotLoadRequiresPersistentBackend;
        }
        if (self.initial_state.journal_timestamp_ns != 0 and !self.serialization.enable_journaling) {
            return error.JournalReplayRequiresJournalingEnabled;
        }
    }

    pub fn toKernelConfig(self: ExchangeCoreConfig) kernel_mod.Config {
        var kernel_cfg = self.matching.toKernelConfig(self.orders_processing);
        kernel_cfg.report_queries = self.reports_queries.toKernelConfig();
        kernel_cfg.java_identity_hash_context = self.java_identity_hash_context;
        return kernel_cfg;
    }

    pub fn toRuntimeConfig(self: ExchangeCoreConfig) runtime_mod.Config {
        return self.runtime.toRuntimeConfig();
    }

    pub fn javaDefault() ExchangeCoreConfig {
        const performance = PerformanceConfig.javaDefault();
        return .{
            .orders_processing = OrdersProcessingConfig.javaDefault(),
            .matching = performance.toMatchingConfig() catch unreachable,
            .runtime = performance.toRuntimeConfig() catch unreachable,
            .sharded_pipeline_mode = .persistent_grouped,
            .initial_state = InitialStateConfig.javaDefault(),
            .reports_queries = ReportsQueriesConfig.javaDefault(),
            .logging = LoggingConfig.javaDefault(),
            .serialization = SerializationConfig.javaDefault(),
        };
    }
};

pub const JavaExchangeConfig = struct {
    orders_processing_cfg: OrdersProcessingConfig = OrdersProcessingConfig.javaDefault(),
    performance_cfg: PerformanceConfig = PerformanceConfig.javaDefault(),
    /// Zig-only production adapter choice. It does not alter Java command
    /// fields, routed/split journal modes, or persistence wire semantics.
    sharded_pipeline_mode: ShardedPipelineMode = .persistent_grouped,
    init_state_cfg: InitialStateConfig = InitialStateConfig.javaDefault(),
    reports_queries_cfg: ReportsQueriesConfig = ReportsQueriesConfig.javaDefault(),
    logging_cfg: LoggingConfig = LoggingConfig.javaDefault(),
    serialization_cfg: SerializationConfig = SerializationConfig.javaDefault(),

    pub const ValidationError = PerformanceConfig.MappingError || ExchangeCoreConfig.ValidationError;

    pub fn javaDefault() JavaExchangeConfig {
        return .{
            .orders_processing_cfg = OrdersProcessingConfig.javaDefault(),
            .performance_cfg = PerformanceConfig.javaDefault(),
            .sharded_pipeline_mode = .persistent_grouped,
            .init_state_cfg = InitialStateConfig.javaDefault(),
            .reports_queries_cfg = ReportsQueriesConfig.javaDefault(),
            .logging_cfg = LoggingConfig.javaDefault(),
            .serialization_cfg = SerializationConfig.javaDefault(),
        };
    }

    pub fn toExchangeCoreConfig(self: JavaExchangeConfig) PerformanceConfig.MappingError!ExchangeCoreConfig {
        return .{
            .orders_processing = self.orders_processing_cfg,
            .matching = try self.performance_cfg.toMatchingConfig(),
            .runtime = try self.performance_cfg.toRuntimeConfig(),
            .sharded_pipeline_mode = self.sharded_pipeline_mode,
            .initial_state = self.init_state_cfg,
            .reports_queries = self.reports_queries_cfg,
            .logging = self.logging_cfg,
            .serialization = self.serialization_cfg,
        };
    }

    pub fn validate(self: JavaExchangeConfig) ValidationError!void {
        const cfg = try self.toExchangeCoreConfig();
        try cfg.validate();
    }
};

const ConfigTestQuery = struct {
    marker: u8 = 0,
};

const ConfigTestSection = struct {
    marker: u8 = 0,
};

const ConfigTestResult = struct {
    marker: u8 = 0,
};

const ConfigWrongMatchingReportView = struct {};
const ConfigWrongRiskReportView = struct {};

fn ConfigTestReportSpec(
    comptime report_code: i32,
    comptime schema_byte: u8,
    comptime report_schema_version: u32,
    comptime MatchingViewType: type,
    comptime RiskViewType: type,
) type {
    return struct {
        pub const code: i32 = report_code;
        pub const schema_id: [report_registry.schema_id_len]u8 =
            [_]u8{schema_byte} ** report_registry.schema_id_len;
        pub const schema_version: u32 = report_schema_version;
        pub const Query = ConfigTestQuery;
        pub const Section = ConfigTestSection;
        pub const Result = ConfigTestResult;
        pub const MatchingView = MatchingViewType;
        pub const RiskView = RiskViewType;

        pub fn cloneQuery(
            _: std.mem.Allocator,
            query: *const Query,
        ) report_registry.ResourceError!Query {
            return query.*;
        }

        pub fn deinitQuery(_: std.mem.Allocator, _: *Query) void {}

        pub fn encodeQuery(
            _: std.mem.Allocator,
            _: *report_registry.EncodingWriter,
            _: *const Query,
        ) report_registry.EncodingError!void {}

        pub fn decodeQuery(
            _: std.mem.Allocator,
            _: []const u8,
        ) report_registry.QueryConstructionError!Query {
            return .{};
        }

        pub fn processMatching(
            _: std.mem.Allocator,
            _: *const Query,
            _: report_registry.MatchingContext(MatchingView),
        ) report_registry.HandlerError!?Section {
            return null;
        }

        pub fn processRisk(
            _: std.mem.Allocator,
            _: *const Query,
            _: report_registry.RiskContext(RiskView),
        ) report_registry.HandlerError!?Section {
            return null;
        }

        pub fn encodeSection(
            _: std.mem.Allocator,
            _: *report_registry.EncodingWriter,
            _: *const Section,
        ) report_registry.EncodingError!void {}

        pub fn deinitSection(_: std.mem.Allocator, _: *Section) void {}

        pub fn reduce(
            _: std.mem.Allocator,
            _: *const Query,
            _: []const report_registry.SectionBytes,
        ) report_registry.ReductionError!Result {
            return .{};
        }

        pub fn deinitResult(_: std.mem.Allocator, _: *Result) void {}
    };
}

const ConfigValidCustomReport = ConfigTestReportSpec(
    42_001,
    0x41,
    1,
    kernel_mod.MatchingReportView,
    kernel_mod.RiskReportView,
);
const ConfigReservedCustomReport = ConfigTestReportSpec(
    @intFromEnum(ReportTypeCode.state_hash),
    0x42,
    1,
    kernel_mod.MatchingReportView,
    kernel_mod.RiskReportView,
);
const ConfigWrongViewCustomReport = ConfigTestReportSpec(
    42_002,
    0x43,
    1,
    ConfigWrongMatchingReportView,
    kernel_mod.RiskReportView,
);
const ConfigWrongRiskViewCustomReport = ConfigTestReportSpec(
    42_003,
    0x44,
    1,
    kernel_mod.MatchingReportView,
    ConfigWrongRiskReportView,
);

test "orders processing config maps into risk config flags" {
    const base = OrdersProcessingConfig{};
    const base_risk = base.toRiskConfig();
    try std.testing.expect(!base_risk.ignore_risk_processing);
    try std.testing.expect(!base_risk.margin_trading_enabled);

    const java_defaults = OrdersProcessingConfig.javaDefault().toRiskConfig();
    try std.testing.expect(!java_defaults.ignore_risk_processing);
    try std.testing.expect(java_defaults.margin_trading_enabled);

    const no_risk = (OrdersProcessingConfig{
        .risk_processing_mode = .no_risk_processing,
        .margin_trading_mode = .margin_trading_enabled,
    }).toRiskConfig();
    try std.testing.expect(no_risk.ignore_risk_processing);
    try std.testing.expect(no_risk.margin_trading_enabled);
}

test "matching config validation enforces shard invariants" {
    try std.testing.expectError(
        error.InvalidNumShards,
        (MatchingConfig{ .num_shards = 0 }).validate(),
    );
    try std.testing.expectError(
        error.InvalidNumShards,
        (MatchingConfig{ .num_shards = 3 }).validate(),
    );
    try std.testing.expectError(
        error.InvalidShardId,
        (MatchingConfig{
            .num_shards = 2,
            .shard_id = 2,
        }).validate(),
    );
    try std.testing.expectError(
        error.InvalidL2RefreshDepth,
        (MatchingConfig{
            .l2_refresh_depth = 0,
        }).validate(),
    );
    try (MatchingConfig{
        .num_shards = 4,
        .shard_id = 3,
    }).validate();
}

test "runtime config validates bounded queue and top-level wait strategy" {
    try std.testing.expectError(
        error.InvalidQueueCapacity,
        (RuntimeConfig{ .queue_capacity = 3 }).validate(),
    );
    try std.testing.expectError(
        error.InvalidBatchSize,
        (RuntimeConfig{ .max_batch_size = 0 }).validate(),
    );
    try std.testing.expectError(
        error.InvalidBatchSize,
        (RuntimeConfig{
            .queue_capacity = 8,
            .max_batch_size = 3,
        }).validate(),
    );
    try std.testing.expectError(
        error.InvalidWaitStrategy,
        (RuntimeConfig{ .wait_strategy = .second_step_no_wait }).validate(),
    );

    const valid = RuntimeConfig{
        .queue_capacity = 8,
        .wait_strategy = .yielding,
        .max_batch_size = 2,
    };
    try valid.validate();
    const mapped = valid.toRuntimeConfig();
    try std.testing.expectEqual(@as(usize, 8), mapped.queue_capacity);
    try std.testing.expectEqual(runtime_mod.WaitStrategy.yielding, mapped.wait_strategy);
    try std.testing.expectEqual(@as(usize, 2), mapped.max_batch_size);
}

test "initial state config supports profile constructors and validates exchange id" {
    const clean = InitialStateConfig.cleanStart("EC0");
    try clean.validate();
    try std.testing.expect(!clean.fromSnapshot());

    const from_snapshot = InitialStateConfig.fromSnapshotOnly("EX1", 42, 100);
    try from_snapshot.validate();
    try std.testing.expect(from_snapshot.fromSnapshot());
    try std.testing.expect(from_snapshot.throw_if_snapshot_not_found);

    const journal = InitialStateConfig.lastKnownStateFromJournal("EX1", 42, 100);
    try journal.validate();
    try std.testing.expectEqual(std.math.maxInt(u64), journal.journal_timestamp_ns);

    try std.testing.expectError(
        error.InvalidExchangeIdCharacter,
        (InitialStateConfig.cleanStart("BAD/ID")).validate(),
    );
    try std.testing.expectError(
        error.SnapshotBaseSeqWithoutSnapshot,
        (InitialStateConfig{
            .exchange_id = "EX2",
            .snapshot_id = 0,
            .snapshot_base_seq = 7,
        }).validate(),
    );
}

test "reports queries config validates enabled handlers and maps supported codes" {
    const standard = ReportsQueriesConfig.javaDefault();
    try standard.validate();
    try std.testing.expect(standard.custom_report_registry == EmptyCustomReportRegistry);
    try std.testing.expectEqual(@as(usize, 0), standard.custom_report_registry.len());
    try std.testing.expect(standard.supportsReport(.state_hash));
    try std.testing.expect(standard.supportsReport(.single_user_report));
    try std.testing.expect(standard.supportsReport(.total_currency_balance));
    try std.testing.expect(standard.supportsBinary(.add_accounts));
    try std.testing.expect(standard.supportsBinary(.add_symbols));

    try std.testing.expectError(
        error.NoReportQueriesEnabled,
        (ReportsQueriesConfig{
            .enable_state_hash_report = false,
            .enable_single_user_report = false,
            .enable_total_currency_balance_report = false,
        }).validate(),
    );
    try std.testing.expectError(
        error.NoBinaryCommandsEnabled,
        (ReportsQueriesConfig{
            .enable_add_accounts_binary = false,
            .enable_add_symbols_binary = false,
        }).validate(),
    );

    const kernel_cfg = (ReportsQueriesConfig{
        .enable_state_hash_report = false,
        .enable_single_user_report = true,
        .enable_total_currency_balance_report = false,
        .enable_add_accounts_binary = false,
        .enable_add_symbols_binary = true,
    }).toKernelConfig();
    try std.testing.expect(!kernel_cfg.state_hash);
    try std.testing.expect(kernel_cfg.single_user);
    try std.testing.expect(!kernel_cfg.total_currency_balance);
    try std.testing.expect(!kernel_cfg.add_accounts_binary);
    try std.testing.expect(kernel_cfg.add_symbols_binary);
    try std.testing.expect(kernel_cfg.custom_registry == EmptyCustomReportRegistry);
}

test "reports queries config validates custom code and kernel callback contexts" {
    const custom_registry = report_registry.Registry.fromSpecs(.{
        ConfigValidCustomReport,
    });
    const custom_only = ReportsQueriesConfig{
        .enable_state_hash_report = false,
        .enable_single_user_report = false,
        .enable_total_currency_balance_report = false,
        .custom_report_registry = custom_registry,
    };
    try custom_only.validate();
    try std.testing.expect(
        custom_only.toKernelConfig().custom_registry == custom_registry,
    );

    const reserved_registry = report_registry.Registry.fromSpecs(.{
        ConfigReservedCustomReport,
    });
    try std.testing.expectError(
        error.ReservedReportCode,
        (ReportsQueriesConfig{
            .custom_report_registry = reserved_registry,
        }).validate(),
    );

    const wrong_view_registry = report_registry.Registry.fromSpecs(.{
        ConfigWrongViewCustomReport,
    });
    try std.testing.expectError(
        error.ContextTypeMismatch,
        (ReportsQueriesConfig{
            .custom_report_registry = wrong_view_registry,
        }).validate(),
    );

    const wrong_risk_view_registry = report_registry.Registry.fromSpecs(.{
        ConfigWrongRiskViewCustomReport,
    });
    try std.testing.expectError(
        error.ContextTypeMismatch,
        (ReportsQueriesConfig{
            .custom_report_registry = wrong_risk_view_registry,
        }).validate(),
    );
}

test "kernel owns a custom registry clone beyond the config source lifetime" {
    const allocator = std.testing.allocator;
    const descriptor =
        report_registry.Adapter(ConfigValidCustomReport).descriptorPtr();
    var source_registry = try report_registry.Registry.initValidated(
        allocator,
        &.{descriptor},
    );
    var source_live = true;
    defer if (source_live) source_registry.deinit();
    const expected_digest = source_registry.digest();

    const reports = ReportsQueriesConfig{
        .custom_report_registry = source_registry,
    };
    try reports.validate();
    var kernel_config: kernel_mod.Config = .{};
    kernel_config.report_queries = reports.toKernelConfig();
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, kernel_config);
    source_registry.deinit();
    source_live = false;
    defer kernel.deinit();

    const owned_registry = kernel.customReportRegistry();
    try std.testing.expectEqual(@as(usize, 1), owned_registry.len());
    try std.testing.expectEqual(expected_digest, owned_registry.digest());
    try std.testing.expect(owned_registry.lookup(descriptor.code()) == descriptor);
}

test "logging config java default enables warnings only" {
    const logging = LoggingConfig.javaDefault();
    try std.testing.expect(logging.isEnabled(.warnings));
    try std.testing.expect(!logging.isEnabled(.risk_debug));
    try std.testing.expect(!logging.isEnabled(.matching_debug));
}

test "serialization config validates journaling backend requirements" {
    const FactoryStub = struct {
        fn create(
            _: ?*anyopaque,
            _: std.mem.Allocator,
            _: serialization_processor.FactoryContext,
        ) serialization_processor.CreateError!serialization_processor.Processor {
            return error.SerializationProcessorCreateFailed;
        }

        const factory: SerializationProcessorFactory = .{
            .create = create,
        };
    };

    const default_cfg = SerializationConfig.javaDefault();
    try default_cfg.validate();
    try std.testing.expectEqual(SerializationBackend.dummy, default_cfg.backend);
    try std.testing.expect(!default_cfg.enable_journaling);
    try std.testing.expectEqualStrings(DefaultDiskStorageFolder, default_cfg.storage_folder);

    const disk_snapshots = SerializationConfig.diskSnapshotOnly();
    try disk_snapshots.validate();
    try std.testing.expectEqual(SerializationBackend.disk, disk_snapshots.backend);
    try std.testing.expect(!disk_snapshots.enable_journaling);
    try std.testing.expectEqualStrings(DefaultDiskStorageFolder, disk_snapshots.storage_folder);

    const disk_journaling = SerializationConfig.diskJournaling();
    try disk_journaling.validate();
    try std.testing.expectEqual(SerializationBackend.disk, disk_journaling.backend);
    try std.testing.expect(disk_journaling.enable_journaling);
    try std.testing.expectEqualStrings(DefaultDiskStorageFolder, disk_journaling.storage_folder);

    try std.testing.expectError(
        error.JournalingRequiresPersistentBackend,
        (SerializationConfig{
            .enable_journaling = true,
            .backend = .dummy,
        }).validate(),
    );
    try std.testing.expectError(
        error.EmptyStorageFolder,
        (SerializationConfig{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "",
        }).validate(),
    );
    const custom_cfg = SerializationConfig.custom(FactoryStub.factory, true);
    try custom_cfg.validate();
    try std.testing.expectEqual(SerializationBackend.custom, custom_cfg.backend);
    try std.testing.expect(
        custom_cfg.custom_processor_factory.?.create == FactoryStub.factory.create,
    );
    try std.testing.expectError(
        error.CustomProcessorFactoryRequired,
        (SerializationConfig{
            .backend = .custom,
        }).validate(),
    );
    try std.testing.expectError(
        error.CustomProcessorFactoryUnexpected,
        (SerializationConfig{
            .backend = .disk,
            .custom_processor_factory = FactoryStub.factory,
        }).validate(),
    );
}

test "exchange core config conversion matches kernel and runtime targets" {
    const cfg = ExchangeCoreConfig{
        .orders_processing = .{
            .risk_processing_mode = .full_per_currency,
            .margin_trading_mode = .margin_trading_enabled,
        },
        .matching = .{
            .orderbook_kind = .naive,
            .shard_id = 1,
            .num_shards = 2,
            .send_l2_for_every_cmd = true,
            .l2_refresh_depth = 16,
        },
        .runtime = .{
            .queue_capacity = 1_024,
            .wait_strategy = .yielding,
            .max_batch_size = 128,
            .l2_flags_every_n_commands = 5,
            .l2_publish_interval_ns = 2_500,
            .max_group_duration_ns = 1_000,
        },
        .initial_state = InitialStateConfig.fromSnapshotOnly("EX4", 77, 10),
        .reports_queries = .{},
        .logging = LoggingConfig.javaDefault(),
        .serialization = SerializationConfig.diskSnapshotOnly(),
    };
    try cfg.validate();

    const kernel_cfg = cfg.toKernelConfig();
    try std.testing.expectEqual(kernel_mod.OrderBookKind.naive, kernel_cfg.orderbook_kind);
    try std.testing.expectEqual(@as(u64, 1), kernel_cfg.shard_id);
    try std.testing.expectEqual(@as(u64, 2), kernel_cfg.num_shards);
    try std.testing.expect(kernel_cfg.send_l2_for_every_cmd);
    try std.testing.expectEqual(@as(usize, 16), kernel_cfg.l2_refresh_depth);
    try std.testing.expect(kernel_cfg.risk.margin_trading_enabled);
    try std.testing.expect(!kernel_cfg.risk.ignore_risk_processing);
    try std.testing.expect(kernel_cfg.report_queries.state_hash);
    try std.testing.expect(kernel_cfg.report_queries.single_user);
    try std.testing.expect(kernel_cfg.report_queries.total_currency_balance);
    try std.testing.expect(kernel_cfg.report_queries.add_accounts_binary);
    try std.testing.expect(kernel_cfg.report_queries.add_symbols_binary);

    const runtime_cfg = cfg.toRuntimeConfig();
    try std.testing.expectEqual(@as(usize, 1_024), runtime_cfg.queue_capacity);
    try std.testing.expectEqual(runtime_mod.WaitStrategy.yielding, runtime_cfg.wait_strategy);
    try std.testing.expectEqual(@as(usize, 128), runtime_cfg.max_batch_size);
    try std.testing.expectEqual(@as(usize, 5), runtime_cfg.l2_flags_every_n_commands);
    try std.testing.expectEqual(@as(u64, 2_500), runtime_cfg.l2_publish_interval_ns);
    try std.testing.expectEqual(@as(u64, 1_000), runtime_cfg.max_group_duration_ns);
}

test "java default config stays valid and carries java-style families" {
    const cfg = ExchangeCoreConfig.javaDefault();
    try cfg.validate();

    const adapted = try JavaExchangeConfig.javaDefault().toExchangeCoreConfig();
    try std.testing.expectEqualDeep(adapted, cfg);
    try std.testing.expectEqual(ShardedPipelineMode.persistent_grouped, cfg.sharded_pipeline_mode);

    var immediate = JavaExchangeConfig.javaDefault();
    immediate.sharded_pipeline_mode = .immediate;
    const immediate_core = try immediate.toExchangeCoreConfig();
    try std.testing.expectEqual(ShardedPipelineMode.immediate, immediate_core.sharded_pipeline_mode);

    const kernel_cfg = cfg.toKernelConfig();
    try std.testing.expectEqual(kernel_mod.OrderBookKind.naive, kernel_cfg.orderbook_kind);
    try std.testing.expect(!kernel_cfg.send_l2_for_every_cmd);
    try std.testing.expectEqual(@as(usize, 8), kernel_cfg.l2_refresh_depth);
    try std.testing.expect(kernel_cfg.risk.margin_trading_enabled);
    try std.testing.expect(kernel_cfg.report_queries.state_hash);
    try std.testing.expect(kernel_cfg.report_queries.add_accounts_binary);
    try std.testing.expect(kernel_cfg.report_queries.add_symbols_binary);

    const runtime_cfg = cfg.toRuntimeConfig();
    try std.testing.expectEqual(runtime_mod.DefaultQueueCapacity, runtime_cfg.queue_capacity);
    try std.testing.expectEqual(runtime_mod.WaitStrategy.blocking, runtime_cfg.wait_strategy);
    try std.testing.expectEqual(@as(usize, 256), runtime_cfg.max_batch_size);
    try std.testing.expectEqual(@as(usize, 0), runtime_cfg.l2_flags_every_n_commands);
    try std.testing.expectEqual(runtime_mod.DefaultL2PublishIntervalNs, runtime_cfg.l2_publish_interval_ns);
    try std.testing.expectEqual(@as(u64, 10_000), runtime_cfg.max_group_duration_ns);

    try std.testing.expect(std.mem.eql(u8, "MY_EXCHANGE", cfg.initial_state.exchange_id));
    try std.testing.expect(cfg.reports_queries.supportsReport(.state_hash));
    try std.testing.expect(cfg.logging.isEnabled(.warnings));
    try std.testing.expectEqual(SerializationBackend.dummy, cfg.serialization.backend);
}

test "performance config presets mirror java defaults and flavors" {
    const base = PerformanceConfig.basePreset();
    try std.testing.expectEqual(@as(usize, 16 * 1024), base.ring_buffer_size);
    try std.testing.expectEqual(@as(u64, 1), base.matching_engines_num);
    try std.testing.expectEqual(@as(u64, 1), base.risk_engines_num);
    try std.testing.expectEqual(@as(usize, 256), base.msgs_in_group_limit);
    try std.testing.expectEqual(@as(u64, 10_000), base.max_group_duration_ns);
    try std.testing.expectEqual(CoreWaitStrategy.blocking, base.wait_strategy);
    try std.testing.expectEqual(kernel_mod.OrderBookKind.naive, base.orderbook_kind);

    const latency = PerformanceConfig.latencyPreset();
    try std.testing.expectEqual(@as(usize, 2 * 1024), latency.ring_buffer_size);
    try std.testing.expectEqual(@as(usize, 256), latency.msgs_in_group_limit);
    try std.testing.expectEqual(@as(u64, 10_000), latency.max_group_duration_ns);
    try std.testing.expectEqual(CoreWaitStrategy.busy_spin, latency.wait_strategy);
    try std.testing.expectEqual(kernel_mod.OrderBookKind.direct, latency.orderbook_kind);

    const throughput = PerformanceConfig.throughputPreset();
    try std.testing.expectEqual(@as(usize, 64 * 1024), throughput.ring_buffer_size);
    try std.testing.expectEqual(@as(u64, 4), throughput.matching_engines_num);
    try std.testing.expectEqual(@as(u64, 2), throughput.risk_engines_num);
    try std.testing.expectEqual(@as(usize, 4_096), throughput.msgs_in_group_limit);
    try std.testing.expectEqual(@as(u64, 4_000_000), throughput.max_group_duration_ns);
    try std.testing.expectEqual(CoreWaitStrategy.busy_spin, throughput.wait_strategy);
    try std.testing.expectEqual(kernel_mod.OrderBookKind.direct, throughput.orderbook_kind);
}

test "performance config validation rejects invalid numeric constraints" {
    try std.testing.expectError(
        error.InvalidRingBufferSize,
        (PerformanceConfig{ .ring_buffer_size = 3 }).validate(),
    );
    try std.testing.expectError(
        error.InvalidMatchingEnginesNum,
        (PerformanceConfig{ .matching_engines_num = 0 }).validate(),
    );
    try std.testing.expectError(
        error.InvalidMatchingEnginesNum,
        (PerformanceConfig{ .matching_engines_num = 3 }).validate(),
    );
    try std.testing.expectError(
        error.InvalidRiskEnginesNum,
        (PerformanceConfig{ .risk_engines_num = 0 }).validate(),
    );
    try std.testing.expectError(
        error.InvalidRiskEnginesNum,
        (PerformanceConfig{ .risk_engines_num = 3 }).validate(),
    );
    try std.testing.expectError(
        error.InvalidMsgsInGroupLimit,
        (PerformanceConfig{ .msgs_in_group_limit = 0 }).validate(),
    );
    try std.testing.expectError(
        error.InvalidL2RefreshDepth,
        (PerformanceConfig{ .l2_refresh_depth = 0 }).validate(),
    );
    try std.testing.expectError(
        error.InvalidWaitStrategy,
        (PerformanceConfig{ .wait_strategy = .second_step_no_wait }).validate(),
    );

    try (PerformanceConfig{
        .ring_buffer_size = 16,
        .msgs_in_group_limit = 4,
    }).validate();
    try std.testing.expectError(
        error.InvalidMsgsInGroupLimit,
        (PerformanceConfig{
            .ring_buffer_size = 16,
            .msgs_in_group_limit = 5,
        }).validate(),
    );
}

test "java exchange config adapter maps performance fields into core config" {
    const java_cfg = JavaExchangeConfig{
        .performance_cfg = .{
            .ring_buffer_size = 16 * 1024,
            .matching_engines_num = 1,
            .risk_engines_num = 1,
            .msgs_in_group_limit = 512,
            .max_group_duration_ns = 25_000,
            .send_l2_for_every_cmd = true,
            .l2_refresh_depth = 16,
            .wait_strategy = .yielding,
            .orderbook_kind = .direct,
        },
        .sharded_pipeline_mode = .persistent_grouped,
    };

    const core_cfg = try java_cfg.toExchangeCoreConfig();
    try std.testing.expectEqual(kernel_mod.OrderBookKind.direct, core_cfg.matching.orderbook_kind);
    try std.testing.expect(core_cfg.matching.send_l2_for_every_cmd);
    try std.testing.expectEqual(@as(usize, 16), core_cfg.matching.l2_refresh_depth);
    try std.testing.expectEqual(@as(usize, 16 * 1024), core_cfg.runtime.queue_capacity);
    try std.testing.expectEqual(runtime_mod.WaitStrategy.yielding, core_cfg.runtime.wait_strategy);
    try std.testing.expectEqual(@as(usize, 512), core_cfg.runtime.max_batch_size);
    try std.testing.expectEqual(runtime_mod.DefaultL2PublishIntervalNs, core_cfg.runtime.l2_publish_interval_ns);
    try std.testing.expectEqual(@as(u64, 25_000), core_cfg.runtime.max_group_duration_ns);
    try std.testing.expectEqual(
        ShardedPipelineMode.persistent_grouped,
        core_cfg.sharded_pipeline_mode,
    );
}

test "java exchange config adapter maps throughput preset via single-kernel fallback" {
    const java_cfg = JavaExchangeConfig{
        .performance_cfg = PerformanceConfig.throughputPreset(),
    };

    const core_cfg = try java_cfg.toExchangeCoreConfig();
    try std.testing.expectEqual(kernel_mod.OrderBookKind.direct, core_cfg.matching.orderbook_kind);
    try std.testing.expectEqual(@as(u64, 0), core_cfg.matching.shard_id);
    try std.testing.expectEqual(@as(u64, 1), core_cfg.matching.num_shards);
    try std.testing.expectEqual(@as(usize, 4_096), core_cfg.runtime.max_batch_size);
    try std.testing.expectEqual(@as(u64, 4_000_000), core_cfg.runtime.max_group_duration_ns);
}

test "performance config accepts independent matching and risk engine counts" {
    const matching_heavy = PerformanceConfig{
        .matching_engines_num = 4,
        .risk_engines_num = 2,
    };
    try matching_heavy.validate();
    _ = try matching_heavy.toMatchingConfig();

    const risk_heavy = PerformanceConfig{
        .matching_engines_num = 1,
        .risk_engines_num = 4,
    };
    try risk_heavy.validate();
    _ = try risk_heavy.toMatchingConfig();
}

test "exchange core config validates initial-state and serialization cross constraints" {
    try std.testing.expectError(
        error.SnapshotLoadRequiresPersistentBackend,
        (ExchangeCoreConfig{
            .initial_state = InitialStateConfig.fromSnapshotOnly("EXSNAP", 1, 10),
            .serialization = SerializationConfig.javaDefault(),
        }).validate(),
    );

    try std.testing.expectError(
        error.JournalReplayRequiresJournalingEnabled,
        (ExchangeCoreConfig{
            .initial_state = InitialStateConfig.lastKnownStateFromJournal("EXJ", 0, 0),
            .serialization = SerializationConfig.diskSnapshotOnly(),
        }).validate(),
    );

    try (ExchangeCoreConfig{
        .initial_state = InitialStateConfig.lastKnownStateFromJournal("EXJ", 7, 100),
        .serialization = SerializationConfig.diskJournaling(),
    }).validate();
}
