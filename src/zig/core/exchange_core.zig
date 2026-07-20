const std = @import("std");
const common = @import("common.zig");
const cmd = @import("cmd.zig");
const config_mod = @import("config.zig");
const binary_frames = @import("binary_frames.zig");
const binary_payload = @import("binary_payload.zig");
const api_command_mod = @import("api_command.zig");
const kernel_mod = @import("exchange_kernel.zig");
const journal_mod = @import("journal.zig");
const persistence_mod = @import("persistence_descriptors.zig");
const persistence_paths = @import("persistence_paths.zig");
const runtime_mod = @import("exchange_runtime.zig");
const simple_events_mod = @import("simple_events_processor.zig");
const snapshot_mod = @import("snapshot.zig");
const log = std.log.scoped(.exchange_core);

pub const Config = config_mod.ExchangeCoreConfig;
pub const JavaConfig = config_mod.JavaExchangeConfig;
pub const BinaryCurrencyBalance = api_command_mod.BinaryCurrencyBalance;
pub const BinaryUserAccountsBatch = api_command_mod.BinaryUserAccountsBatch;
pub const BinaryReportQuery = api_command_mod.BinaryReportQuery;
pub const ApiCommand = api_command_mod.ApiCommand;
pub const ApiCommandResult = api_command_mod.ApiCommandResult;
pub const ApiCommandAsyncHandle = api_command_mod.ApiCommandAsyncHandle;
pub const SimpleEventsHandler = simple_events_mod.EventsHandler;

pub const SnapshotRef = persistence_mod.SnapshotRef;
pub const JournalRef = persistence_mod.JournalRef;
pub const SnapshotDescriptor = persistence_mod.SnapshotDescriptor;
pub const JournalDescriptor = persistence_mod.JournalDescriptor;
pub const PersistenceModuleType = persistence_paths.ModuleType;
pub const PersistencePathError = persistence_paths.PathError;
const DescriptorInitError = persistence_mod.CreateSnapshotError || error{
    MatchingEngineCountOverflow,
};
const DescriptorTrackingError = persistence_mod.CreateSnapshotError ||
    persistence_mod.CreateJournalError ||
    persistence_mod.FinishJournalError ||
    error{
        InvalidPersistSnapshotId,
        InvalidPersistTimestamp,
        MissingPersistSequence,
        MatchingEngineCountOverflow,
        JournalSequenceNotMonotonic,
    };
pub const InitError = Config.ValidationError || kernel_mod.InitError || runtime_mod.InitError || std.mem.Allocator.Error || DescriptorInitError;
pub const InitFromJavaConfigError = config_mod.PerformanceConfig.MappingError || InitError;
pub const ExecuteError = LifecycleError || kernel_mod.ExecuteError || DescriptorTrackingError;
pub const ReportError = kernel_mod.ReportError;
pub const AsyncSubmitError = LifecycleError || kernel_mod.ExchangeApi.AsyncSubmitError;
pub const HelperError = kernel_mod.ExchangeApi.HelperError;
pub const PersistStateRequestPair = kernel_mod.ExchangeApi.PersistStateRequestPair;
pub const RuntimeDrainError = ExecuteError || runtime_mod.DrainError;
pub const BinaryPayloadSubmitError = ExecuteError || error{
    InvalidBinaryCommandType,
    EmptyBinaryPayload,
    BinaryPayloadSizeOverflow,
};
pub const BinaryPayloadAsyncSubmitError = AsyncSubmitError || error{
    InvalidBinaryCommandType,
    EmptyBinaryPayload,
    BinaryPayloadSizeOverflow,
};
pub const BinaryPayloadTypedSubmitError = BinaryPayloadSubmitError || binary_payload.EncodeError;
pub const BinaryPayloadTypedAsyncSubmitError = BinaryPayloadAsyncSubmitError || binary_payload.EncodeError;
pub const BinaryReportQuerySubmitError = BinaryPayloadSubmitError || std.mem.Allocator.Error;
pub const BinaryReportQueryAsyncSubmitError = BinaryPayloadAsyncSubmitError || std.mem.Allocator.Error;
pub const SubmitApiCommandError = ExecuteError || HelperError || BinaryPayloadTypedSubmitError || BinaryReportQuerySubmitError;
pub const SubmitApiCommandAsyncError = AsyncSubmitError || HelperError || BinaryPayloadTypedAsyncSubmitError || BinaryReportQueryAsyncSubmitError;
pub const SubmitApiCommandBatchError = SubmitApiCommandAsyncError || ExecuteError || std.mem.Allocator.Error || error{
    EmptyApiCommandBatch,
    MissingBatchResult,
    UnexpectedFlushResultVariant,
};
pub const JournalApiError = error{
    JournalingDisabled,
} || journal_mod.JournalIoError || DescriptorTrackingError;
pub const JournalReplayError = JournalApiError || journal_mod.ReplayError;
pub const SnapshotWriteError = snapshot_mod.SnapshotCaptureError || snapshot_mod.SnapshotIoError;
pub const SnapshotValidateError = snapshot_mod.SnapshotValidationError || snapshot_mod.SnapshotIoError;
const StorageFolderEnsureError = std.fs.Dir.MakeError || std.fs.Dir.StatFileError;
const MainLogWriteError = std.fs.File.OpenError || std.fs.File.SeekError || std.fs.File.WriteError || std.fmt.BufPrintError;
pub const DefaultJournalApiError = JournalApiError || PersistencePathError || StorageFolderEnsureError;
pub const DefaultJournalReplayError = JournalReplayError || PersistencePathError;
pub const DefaultJournalReplayRangeError = DefaultJournalReplayError || error{
    JournalReplayCountOverflow,
};
pub const DefaultSnapshotWriteError = SnapshotWriteError || PersistencePathError || StorageFolderEnsureError || MainLogWriteError;
pub const DefaultSnapshotValidateError = SnapshotValidateError || PersistencePathError;
pub const SnapshotExistsError = std.fs.Dir.StatFileError;
pub const DefaultSnapshotExistsError = SnapshotExistsError || PersistencePathError;
pub const PersistStateWithMainLogError = ExecuteError || PersistencePathError || StorageFolderEnsureError || MainLogWriteError || error{
    PersistStateRejected,
    PersistStateSnapshotMismatch,
    PersistStateSnapshotIdOverflow,
    PersistStateTimestampOverflow,
};
pub const LifecycleError = error{
    CoreStopped,
};
pub const BootstrapOutcome = struct {
    snapshot_requested: bool = false,
    snapshot_found: bool = false,
    journal_replayed_commands: usize = 0,
};
pub const BootstrapError = JournalReplayError || snapshot_mod.SnapshotIoError || snapshot_mod.SnapshotRestoreError || error{
    SnapshotPathRequired,
    SnapshotFileNotFound,
    JournalPathRequired,
};
pub const StartupError = LifecycleError || BootstrapError;
pub const StartupFromConfiguredPersistenceError = StartupError || PersistencePathError || error{
    JournalReplayCountOverflow,
};
const MainLogSequenceParseError = error{
    MalformedMainLogEntry,
};
pub const StartupFromDiscoveredConfiguredPersistenceError = StartupFromConfiguredPersistenceError || std.fs.Dir.OpenError || std.fs.Dir.Iterator.Error;
pub const StartupFromDiscoveredConfiguredPersistenceStrictError = StartupFromDiscoveredConfiguredPersistenceError || MainLogSequenceParseError;
const ParityHelperError = error{
    UnexpectedReportVariant,
};
const BinaryFrameLayout = struct {
    words_without_padding: usize,
    frame_count: usize,
};

pub const ExchangeCore = struct {
    allocator: std.mem.Allocator,
    config: Config,
    kernel: *kernel_mod.ExchangeKernel,
    journal: ?journal_mod.CommandJournal,
    descriptors: persistence_mod.DescriptorStore,
    current_snapshot_ref: SnapshotRef,
    last_journal_ref: ?JournalRef,
    api: kernel_mod.ExchangeApi,
    runtime: runtime_mod.ExchangeRuntime,
    simple_events_processor: ?simple_events_mod.SimpleEventsProcessor,
    started: bool,
    stopped: bool,

    pub fn init(allocator: std.mem.Allocator, config: Config) InitError!ExchangeCore {
        try config.validate();

        const kernel_ptr = try allocator.create(kernel_mod.ExchangeKernel);
        errdefer allocator.destroy(kernel_ptr);

        kernel_ptr.* = try kernel_mod.ExchangeKernel.init(allocator, config.toKernelConfig());
        errdefer kernel_ptr.deinit();

        var runtime = try runtime_mod.ExchangeRuntime.init(allocator, kernel_ptr, config.toRuntimeConfig());
        errdefer runtime.deinit();

        var descriptors = persistence_mod.DescriptorStore.init(allocator);
        errdefer descriptors.deinit();
        const matching_engine_count = std.math.cast(u32, config.matching.num_shards) orelse return error.MatchingEngineCountOverflow;
        const root_snapshot_ref = try descriptors.createEmptySnapshot(matching_engine_count, 1);

        const api = kernel_mod.ExchangeApi.init(kernel_ptr);
        const journal = if (config.serialization.enable_journaling)
            journal_mod.CommandJournal.init(allocator)
        else
            null;

        return .{
            .allocator = allocator,
            .config = config,
            .kernel = kernel_ptr,
            .journal = journal,
            .descriptors = descriptors,
            .current_snapshot_ref = root_snapshot_ref,
            .last_journal_ref = null,
            .api = api,
            .runtime = runtime,
            .simple_events_processor = null,
            .started = false,
            .stopped = false,
        };
    }

    pub fn initFromJavaConfig(
        allocator: std.mem.Allocator,
        java_config: JavaConfig,
    ) InitFromJavaConfigError!ExchangeCore {
        return init(allocator, try java_config.toExchangeCoreConfig());
    }

    pub fn deinit(self: *ExchangeCore) void {
        self.assertWiring();
        if (self.simple_events_processor) |*processor| {
            processor.deinit();
            self.simple_events_processor = null;
        }
        self.runtime.deinit();
        self.api.deinit();
        if (self.journal) |*journal| {
            journal.deinit();
            self.journal = null;
        }
        self.descriptors.deinit();
        self.kernel.deinit();
        self.allocator.destroy(self.kernel);
        self.* = undefined;
    }

    pub fn kernelRef(self: *ExchangeCore) *kernel_mod.ExchangeKernel {
        self.assertWiring();
        return self.kernel;
    }

    pub fn configRef(self: *const ExchangeCore) *const Config {
        self.assertWiringConst();
        return &self.config;
    }

    pub fn isStarted(self: *const ExchangeCore) bool {
        self.assertWiringConst();
        return self.started;
    }

    pub fn isStopped(self: *const ExchangeCore) bool {
        self.assertWiringConst();
        return self.stopped;
    }

    pub fn startup(self: *ExchangeCore) LifecycleError!void {
        self.assertWiring();
        if (self.stopped) return error.CoreStopped;
        self.started = true;
    }

    pub fn startupFromPersistence(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_path: ?[]const u8,
        journal_path: ?[]const u8,
    ) StartupError!BootstrapOutcome {
        try self.startup();
        return self.bootstrapFromPersistence(dir, snapshot_path, journal_path);
    }

    pub fn startupFromConfiguredPersistence(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromConfiguredPersistenceError!BootstrapOutcome {
        var snapshot_path: ?[]u8 = null;
        defer if (snapshot_path) |path| self.allocator.free(path);

        var journal_path: ?[]u8 = null;
        defer if (journal_path) |path| self.allocator.free(path);

        const initial_state = self.config.initial_state;
        if (initial_state.fromSnapshot()) {
            snapshot_path = try self.defaultPersistenceSnapshotPath(
                self.allocator,
                initial_state.snapshot_id,
                snapshot_module_type,
                snapshot_instance_id,
            );
        }
        if (initial_state.journal_timestamp_ns != 0) {
            journal_path = try self.defaultPersistenceJournalPath(
                self.allocator,
                initial_state.snapshot_id,
                journal_partition_id,
            );
        }

        try self.startup();
        var outcome: BootstrapOutcome = .{};
        try self.bootstrapSnapshotPhase(dir, snapshot_path, &outcome);

        var first_partition_found = false;
        if (initial_state.journal_timestamp_ns != 0) {
            const path = journal_path orelse return error.JournalPathRequired;
            first_partition_found = true;
            outcome.journal_replayed_commands = self.replayJournalFromFile(dir, path) catch |err| switch (err) {
                error.FileNotFound => blk: {
                    first_partition_found = false;
                    break :blk 0;
                },
                else => return err,
            };
        }

        if (first_partition_found and journal_partition_id < std.math.maxInt(u16)) {
            const replayed_tail = try self.replayJournalFromDefaultPathRange(
                dir,
                initial_state.snapshot_id,
                journal_partition_id + 1,
            );
            outcome.journal_replayed_commands = std.math.add(
                usize,
                outcome.journal_replayed_commands,
                replayed_tail,
            ) catch return error.JournalReplayCountOverflow;
        }

        return outcome;
    }

    pub fn startupFromDiscoveredConfiguredPersistence(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromDiscoveredConfiguredPersistenceError!BootstrapOutcome {
        const discovered_snapshot_id = try self.discoverLatestDefaultSnapshotId(
            dir,
            snapshot_module_type,
            snapshot_instance_id,
        );
        if (discovered_snapshot_id) |snapshot_id| {
            return self.startupFromConfiguredPersistenceWithSnapshotId(
                dir,
                snapshot_module_type,
                snapshot_instance_id,
                journal_partition_id,
                snapshot_id,
            );
        }

        const initial_state = self.config.initial_state;
        try self.startup();
        if (initial_state.fromSnapshot() and initial_state.throw_if_snapshot_not_found) {
            return error.SnapshotFileNotFound;
        }
        return .{
            .snapshot_requested = initial_state.fromSnapshot(),
            .snapshot_found = false,
            .journal_replayed_commands = 0,
        };
    }

    pub fn startupFromDiscoveredConfiguredPersistenceStrictMainLog(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromDiscoveredConfiguredPersistenceStrictError!BootstrapOutcome {
        const discovered_snapshot_id = try self.discoverLatestDefaultSnapshotId(
            dir,
            snapshot_module_type,
            snapshot_instance_id,
        );
        if (discovered_snapshot_id) |snapshot_id| {
            return self.startupFromConfiguredPersistenceWithSnapshotIdStrictMainLog(
                dir,
                snapshot_module_type,
                snapshot_instance_id,
                journal_partition_id,
                snapshot_id,
            );
        }

        const initial_state = self.config.initial_state;
        try self.startup();
        if (initial_state.fromSnapshot() and initial_state.throw_if_snapshot_not_found) {
            return error.SnapshotFileNotFound;
        }
        return .{
            .snapshot_requested = initial_state.fromSnapshot(),
            .snapshot_found = false,
            .journal_replayed_commands = 0,
        };
    }

    fn startupFromConfiguredPersistenceWithSnapshotId(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
        snapshot_id: u64,
    ) StartupFromConfiguredPersistenceError!BootstrapOutcome {
        const snapshot_path = try self.defaultPersistenceSnapshotPath(
            self.allocator,
            snapshot_id,
            snapshot_module_type,
            snapshot_instance_id,
        );
        defer self.allocator.free(snapshot_path);

        var journal_path: ?[]u8 = null;
        defer if (journal_path) |path| self.allocator.free(path);
        if (self.config.initial_state.journal_timestamp_ns != 0) {
            journal_path = try self.defaultPersistenceJournalPath(
                self.allocator,
                snapshot_id,
                journal_partition_id,
            );
        }

        const snapshot_base_seq = self.discoverSnapshotBaseSeqFromMainLog(
            dir,
            snapshot_id,
            snapshot_module_type,
            snapshot_instance_id,
        ) orelse self.config.initial_state.snapshot_base_seq;

        try self.startup();
        var outcome: BootstrapOutcome = .{};
        try self.bootstrapSnapshotPhaseWithId(
            dir,
            snapshot_path,
            snapshot_id,
            snapshot_base_seq,
            &outcome,
        );

        var first_partition_found = false;
        if (self.config.initial_state.journal_timestamp_ns != 0) {
            const path = journal_path orelse return error.JournalPathRequired;
            first_partition_found = true;
            outcome.journal_replayed_commands = self.replayJournalFromFile(dir, path) catch |err| switch (err) {
                error.FileNotFound => blk: {
                    first_partition_found = false;
                    break :blk 0;
                },
                else => return err,
            };
        }

        if (first_partition_found and journal_partition_id < std.math.maxInt(u16)) {
            const replayed_tail = try self.replayJournalFromDefaultPathRange(
                dir,
                snapshot_id,
                journal_partition_id + 1,
            );
            outcome.journal_replayed_commands = std.math.add(
                usize,
                outcome.journal_replayed_commands,
                replayed_tail,
            ) catch return error.JournalReplayCountOverflow;
        }

        return outcome;
    }

    fn startupFromConfiguredPersistenceWithSnapshotIdStrictMainLog(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
        snapshot_id: u64,
    ) (StartupFromConfiguredPersistenceError || MainLogSequenceParseError)!BootstrapOutcome {
        const snapshot_path = try self.defaultPersistenceSnapshotPath(
            self.allocator,
            snapshot_id,
            snapshot_module_type,
            snapshot_instance_id,
        );
        defer self.allocator.free(snapshot_path);

        var journal_path: ?[]u8 = null;
        defer if (journal_path) |path| self.allocator.free(path);
        if (self.config.initial_state.journal_timestamp_ns != 0) {
            journal_path = try self.defaultPersistenceJournalPath(
                self.allocator,
                snapshot_id,
                journal_partition_id,
            );
        }

        const discovered_seq = try self.discoverSnapshotBaseSeqFromMainLogStrict(
            dir,
            snapshot_id,
            snapshot_module_type,
            snapshot_instance_id,
        );
        const snapshot_base_seq = discovered_seq orelse self.config.initial_state.snapshot_base_seq;

        try self.startup();
        var outcome: BootstrapOutcome = .{};
        try self.bootstrapSnapshotPhaseWithId(
            dir,
            snapshot_path,
            snapshot_id,
            snapshot_base_seq,
            &outcome,
        );

        var first_partition_found = false;
        if (self.config.initial_state.journal_timestamp_ns != 0) {
            const path = journal_path orelse return error.JournalPathRequired;
            first_partition_found = true;
            outcome.journal_replayed_commands = self.replayJournalFromFile(dir, path) catch |err| switch (err) {
                error.FileNotFound => blk: {
                    first_partition_found = false;
                    break :blk 0;
                },
                else => return err,
            };
        }

        if (first_partition_found and journal_partition_id < std.math.maxInt(u16)) {
            const replayed_tail = try self.replayJournalFromDefaultPathRange(
                dir,
                snapshot_id,
                journal_partition_id + 1,
            );
            outcome.journal_replayed_commands = std.math.add(
                usize,
                outcome.journal_replayed_commands,
                replayed_tail,
            ) catch return error.JournalReplayCountOverflow;
        }

        return outcome;
    }

    fn bootstrapSnapshotPhaseWithId(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_path: []const u8,
        snapshot_id: u64,
        snapshot_base_seq: u64,
        outcome: *BootstrapOutcome,
    ) BootstrapError!void {
        const init_state = self.config.initial_state;
        outcome.snapshot_requested = true;
        var snapshot = snapshot_mod.KernelSnapshot.readFromFile(self.allocator, dir, snapshot_path) catch |err| switch (err) {
            error.FileNotFound => {
                if (init_state.throw_if_snapshot_not_found) return error.SnapshotFileNotFound;
                return;
            },
            else => return err,
        };
        defer snapshot.deinit();

        outcome.snapshot_found = true;
        try snapshot.restoreIntoKernel(self.kernel);
        self.kernel.next_seq = snapshot_base_seq;
        try self.registerSnapshotDescriptor(
            snapshot_id,
            snapshot_base_seq,
            0,
        );
    }

    fn discoverLatestDefaultSnapshotId(
        self: *const ExchangeCore,
        dir: std.fs.Dir,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) (std.fs.Dir.OpenError || std.fs.Dir.Iterator.Error)!?u64 {
        const storage_folder = self.config.serialization.storage_folder;
        var storage_dir = dir.openDir(storage_folder, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer storage_dir.close();

        var it = storage_dir.iterate();
        var latest_snapshot_id: ?u64 = null;
        while (try it.next()) |entry| {
            const snapshot_id = parseSnapshotIdFromDefaultFileName(
                entry.name,
                self.config.initial_state.exchange_id,
                module_type,
                instance_id,
            ) orelse continue;
            if (latest_snapshot_id == null or snapshot_id > latest_snapshot_id.?) {
                latest_snapshot_id = snapshot_id;
            }
        }
        return latest_snapshot_id;
    }

    fn parseSnapshotIdFromDefaultFileName(
        file_name: []const u8,
        exchange_id: []const u8,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) ?u64 {
        if (!std.mem.startsWith(u8, file_name, exchange_id)) return null;

        const marker = "_snapshot_";
        if (file_name.len <= exchange_id.len + marker.len) return null;
        if (!std.mem.eql(u8, file_name[exchange_id.len .. exchange_id.len + marker.len], marker)) return null;

        const after_marker = file_name[exchange_id.len + marker.len ..];
        const id_sep = std.mem.indexOfScalar(u8, after_marker, '_') orelse return null;
        const snapshot_id_slice = after_marker[0..id_sep];
        if (snapshot_id_slice.len == 0) return null;
        const snapshot_id = std.fmt.parseInt(u64, snapshot_id_slice, 10) catch return null;

        const module_and_suffix = after_marker[id_sep + 1 ..];
        const module_code = persistence_paths.moduleCode(module_type);
        if (!std.mem.startsWith(u8, module_and_suffix, module_code)) return null;

        const ext = ".ecs";
        if (!std.mem.endsWith(u8, module_and_suffix, ext)) return null;

        const instance_slice = module_and_suffix[module_code.len .. module_and_suffix.len - ext.len];
        if (instance_slice.len == 0) return null;
        const parsed_instance = std.fmt.parseInt(u32, instance_slice, 10) catch return null;
        if (parsed_instance != instance_id) return null;

        return snapshot_id;
    }

    fn discoverSnapshotBaseSeqFromMainLog(
        self: *const ExchangeCore,
        dir: std.fs.Dir,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) ?u64 {
        const main_log_path = self.defaultPersistenceMainLogPath(self.allocator) catch return null;
        defer self.allocator.free(main_log_path);

        var main_log_file = dir.openFile(main_log_path, .{}) catch return null;
        defer main_log_file.close();
        const main_log_contents = main_log_file.readToEndAlloc(self.allocator, 8 * 1024 * 1024) catch return null;
        defer self.allocator.free(main_log_contents);

        var best_seq: ?u64 = null;
        var lines = std.mem.splitScalar(u8, main_log_contents, '\n');
        while (lines.next()) |line| {
            const seq = parseMainLogSequenceForSnapshot(
                line,
                snapshot_id,
                module_type,
                instance_id,
            ) orelse continue;
            if (best_seq == null or seq > best_seq.?) {
                best_seq = seq;
            }
        }
        return best_seq;
    }

    fn discoverSnapshotBaseSeqFromMainLogStrict(
        self: *const ExchangeCore,
        dir: std.fs.Dir,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) MainLogSequenceParseError!?u64 {
        const main_log_path = self.defaultPersistenceMainLogPath(self.allocator) catch return null;
        defer self.allocator.free(main_log_path);

        var main_log_file = dir.openFile(main_log_path, .{}) catch return null;
        defer main_log_file.close();
        const main_log_contents = main_log_file.readToEndAlloc(self.allocator, 8 * 1024 * 1024) catch return null;
        defer self.allocator.free(main_log_contents);

        var best_seq: ?u64 = null;
        var lines = std.mem.splitScalar(u8, main_log_contents, '\n');
        while (lines.next()) |line| {
            const parsed = try parseMainLogEntryStrict(line) orelse continue;
            if (parsed.snapshot_id != snapshot_id) continue;
            if (!std.mem.eql(u8, parsed.type_code, persistence_paths.moduleCode(module_type))) continue;
            if (parsed.instance_id != instance_id) continue;
            if (best_seq == null or parsed.seq > best_seq.?) {
                best_seq = parsed.seq;
            }
        }
        return best_seq;
    }

    fn parseMainLogSequenceForSnapshot(
        line: []const u8,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) ?u64 {
        if (line.len == 0) return null;

        var tokens = std.mem.tokenizeScalar(u8, line, ' ');
        _ = tokens.next() orelse return null;
        const seq_token = tokens.next() orelse return null;
        _ = tokens.next() orelse return null;
        const snapshot_token = tokens.next() orelse return null;
        const type_token = tokens.next() orelse return null;
        const instance_token = tokens.next() orelse return null;

        const seq = parsePrefixedU64Token(seq_token, "seq=") orelse return null;
        const parsed_snapshot_id = parsePrefixedU64Token(snapshot_token, "snapshotId=") orelse return null;
        if (parsed_snapshot_id != snapshot_id) return null;

        if (!std.mem.startsWith(u8, type_token, "type=")) return null;
        const type_code = type_token["type=".len..];
        if (!std.mem.eql(u8, type_code, persistence_paths.moduleCode(module_type))) return null;

        const parsed_instance = parsePrefixedU32Token(instance_token, "instance=") orelse return null;
        if (parsed_instance != instance_id) return null;

        return seq;
    }

    const ParsedMainLogEntry = struct {
        seq: u64,
        snapshot_id: u64,
        type_code: []const u8,
        instance_id: u32,
    };

    fn parseMainLogEntryStrict(line: []const u8) MainLogSequenceParseError!?ParsedMainLogEntry {
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0) return null;

        var tokens = std.mem.tokenizeScalar(u8, trimmed, ' ');
        _ = tokens.next() orelse return error.MalformedMainLogEntry;

        var seq: ?u64 = null;
        var snapshot_id: ?u64 = null;
        var type_code: ?[]const u8 = null;
        var instance_id: ?u32 = null;

        while (tokens.next()) |token| {
            if (std.mem.startsWith(u8, token, "seq=")) {
                seq = parsePrefixedU64Token(token, "seq=") orelse return error.MalformedMainLogEntry;
            } else if (std.mem.startsWith(u8, token, "snapshotId=")) {
                snapshot_id = parsePrefixedU64Token(token, "snapshotId=") orelse return error.MalformedMainLogEntry;
            } else if (std.mem.startsWith(u8, token, "type=")) {
                const value = token["type=".len..];
                if (value.len == 0) return error.MalformedMainLogEntry;
                type_code = value;
            } else if (std.mem.startsWith(u8, token, "instance=")) {
                instance_id = parsePrefixedU32Token(token, "instance=") orelse return error.MalformedMainLogEntry;
            }
        }

        return .{
            .seq = seq orelse return error.MalformedMainLogEntry,
            .snapshot_id = snapshot_id orelse return error.MalformedMainLogEntry,
            .type_code = type_code orelse return error.MalformedMainLogEntry,
            .instance_id = instance_id orelse return error.MalformedMainLogEntry,
        };
    }

    fn parsePrefixedU64Token(token: []const u8, prefix: []const u8) ?u64 {
        if (!std.mem.startsWith(u8, token, prefix)) return null;
        const value = token[prefix.len..];
        if (value.len == 0) return null;
        return std.fmt.parseInt(u64, value, 10) catch return null;
    }

    fn parsePrefixedU32Token(token: []const u8, prefix: []const u8) ?u32 {
        if (!std.mem.startsWith(u8, token, prefix)) return null;
        const value = token[prefix.len..];
        if (value.len == 0) return null;
        return std.fmt.parseInt(u32, value, 10) catch return null;
    }

    fn bootstrapSnapshotPhase(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_path: ?[]const u8,
        outcome: *BootstrapOutcome,
    ) BootstrapError!void {
        const init_state = self.config.initial_state;

        snapshot_phase: {
            if (!init_state.fromSnapshot()) break :snapshot_phase;
            outcome.snapshot_requested = true;

            const path = snapshot_path orelse {
                if (init_state.throw_if_snapshot_not_found) return error.SnapshotPathRequired;
                break :snapshot_phase;
            };

            var snapshot = snapshot_mod.KernelSnapshot.readFromFile(self.allocator, dir, path) catch |err| switch (err) {
                error.FileNotFound => {
                    if (init_state.throw_if_snapshot_not_found) return error.SnapshotFileNotFound;
                    break :snapshot_phase;
                },
                else => return err,
            };
            defer snapshot.deinit();

            outcome.snapshot_found = true;
            try snapshot.restoreIntoKernel(self.kernel);
            self.kernel.next_seq = init_state.snapshot_base_seq;
            try self.registerSnapshotDescriptor(
                init_state.snapshot_id,
                init_state.snapshot_base_seq,
                0,
            );
        }
    }

    fn bootstrapFromPersistenceStrict(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_path: ?[]const u8,
        journal_path: ?[]const u8,
    ) BootstrapError!BootstrapOutcome {
        var outcome: BootstrapOutcome = .{};
        try self.bootstrapSnapshotPhase(dir, snapshot_path, &outcome);

        const init_state = self.config.initial_state;
        if (init_state.journal_timestamp_ns != 0) {
            const path = journal_path orelse return error.JournalPathRequired;
            outcome.journal_replayed_commands = try self.replayJournalFromFile(dir, path);
        }

        return outcome;
    }

    pub fn bootstrapFromPersistence(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_path: ?[]const u8,
        journal_path: ?[]const u8,
    ) BootstrapError!BootstrapOutcome {
        self.assertWiring();
        return self.bootstrapFromPersistenceStrict(
            dir,
            snapshot_path,
            journal_path,
        );
    }

    pub fn shutdown(self: *ExchangeCore) void {
        self.assertWiring();
        self.stopped = true;
    }

    pub fn journalingEnabled(self: *const ExchangeCore) bool {
        self.assertWiringConst();
        return self.journal != null;
    }

    pub fn journalLen(self: *const ExchangeCore) usize {
        self.assertWiringConst();
        if (self.journal) |journal| {
            return journal.len();
        }
        return 0;
    }

    pub fn persistenceSnapshotCount(self: *const ExchangeCore) usize {
        self.assertWiringConst();
        return self.descriptors.snapshotCount();
    }

    pub fn persistenceJournalCount(self: *const ExchangeCore) usize {
        self.assertWiringConst();
        return self.descriptors.journalCount();
    }

    pub fn persistenceCurrentSnapshotRef(self: *const ExchangeCore) SnapshotRef {
        self.assertWiringConst();
        return self.current_snapshot_ref;
    }

    pub fn persistenceCurrentSnapshot(self: *const ExchangeCore) *const SnapshotDescriptor {
        self.assertWiringConst();
        return self.descriptors.snapshotConst(self.current_snapshot_ref);
    }

    pub fn persistenceFindSnapshotById(self: *const ExchangeCore, snapshot_id: u64) ?SnapshotRef {
        self.assertWiringConst();
        return self.descriptors.findSnapshotById(snapshot_id);
    }

    pub fn persistenceSnapshotById(self: *const ExchangeCore, snapshot_id: u64) ?*const SnapshotDescriptor {
        self.assertWiringConst();
        const snapshot_ref = self.descriptors.findSnapshotById(snapshot_id) orelse return null;
        return self.descriptors.snapshotConst(snapshot_ref);
    }

    pub fn persistenceJournalByRef(self: *const ExchangeCore, journal_ref: JournalRef) *const JournalDescriptor {
        self.assertWiringConst();
        return self.descriptors.journalConst(journal_ref);
    }

    pub fn persistenceFindJournalForSequence(self: *const ExchangeCore, sequence: u64) ?JournalRef {
        self.assertWiringConst();
        return self.descriptors.findJournalForSequence(self.current_snapshot_ref, sequence);
    }

    pub fn defaultPersistenceSnapshotFileName(
        self: *const ExchangeCore,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) persistence_paths.FileNameError![]u8 {
        self.assertWiringConst();
        return persistence_paths.snapshotFileName(
            allocator,
            self.config.initial_state.exchange_id,
            snapshot_id,
            module_type,
            instance_id,
        );
    }

    pub fn defaultPersistenceJournalFileName(
        self: *const ExchangeCore,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
        partition_id: u16,
    ) persistence_paths.FileNameError![]u8 {
        self.assertWiringConst();
        return persistence_paths.journalFileName(
            allocator,
            self.config.initial_state.exchange_id,
            snapshot_id,
            partition_id,
        );
    }

    pub fn defaultPersistenceMainLogFileName(
        self: *const ExchangeCore,
        allocator: std.mem.Allocator,
    ) persistence_paths.FileNameError![]u8 {
        self.assertWiringConst();
        return persistence_paths.mainLogFileName(
            allocator,
            self.config.initial_state.exchange_id,
        );
    }

    pub fn defaultPersistenceSnapshotPath(
        self: *const ExchangeCore,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) persistence_paths.PathError![]u8 {
        self.assertWiringConst();
        return persistence_paths.snapshotPath(
            allocator,
            self.config.serialization.storage_folder,
            self.config.initial_state.exchange_id,
            snapshot_id,
            module_type,
            instance_id,
        );
    }

    pub fn defaultPersistenceJournalPath(
        self: *const ExchangeCore,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
        partition_id: u16,
    ) persistence_paths.PathError![]u8 {
        self.assertWiringConst();
        return persistence_paths.journalPath(
            allocator,
            self.config.serialization.storage_folder,
            self.config.initial_state.exchange_id,
            snapshot_id,
            partition_id,
        );
    }

    pub fn defaultPersistenceMainLogPath(
        self: *const ExchangeCore,
        allocator: std.mem.Allocator,
    ) persistence_paths.PathError![]u8 {
        self.assertWiringConst();
        return persistence_paths.mainLogPath(
            allocator,
            self.config.serialization.storage_folder,
            self.config.initial_state.exchange_id,
        );
    }

    pub fn writeJournalToDefaultPath(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_id: u64,
        partition_id: u16,
    ) DefaultJournalApiError!void {
        try self.ensureConfiguredStorageFolderExists(dir);
        const path = try self.defaultPersistenceJournalPath(
            self.allocator,
            snapshot_id,
            partition_id,
        );
        defer self.allocator.free(path);
        try self.writeJournalToFile(dir, path);
    }

    pub fn loadJournalFromDefaultPath(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_id: u64,
        partition_id: u16,
    ) DefaultJournalApiError!void {
        const path = try self.defaultPersistenceJournalPath(
            self.allocator,
            snapshot_id,
            partition_id,
        );
        defer self.allocator.free(path);
        try self.loadJournalFromFile(dir, path);
    }

    pub fn replayJournalFromDefaultPath(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_id: u64,
        partition_id: u16,
    ) DefaultJournalReplayError!usize {
        const path = try self.defaultPersistenceJournalPath(
            self.allocator,
            snapshot_id,
            partition_id,
        );
        defer self.allocator.free(path);
        return self.replayJournalFromFile(dir, path);
    }

    pub fn replayJournalFromDefaultPathRange(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_id: u64,
        starting_partition_id: u16,
    ) DefaultJournalReplayRangeError!usize {
        var total_replayed: usize = 0;
        var partition_id = starting_partition_id;
        while (true) {
            const replayed = self.replayJournalFromDefaultPath(
                dir,
                snapshot_id,
                partition_id,
            ) catch |err| switch (err) {
                error.FileNotFound => break,
                else => return err,
            };
            total_replayed = std.math.add(
                usize,
                total_replayed,
                replayed,
            ) catch return error.JournalReplayCountOverflow;
            if (partition_id == std.math.maxInt(u16)) break;
            partition_id += 1;
        }
        return total_replayed;
    }

    pub fn captureSnapshotToDefaultPath(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
        symbols: []const i32,
        depth: usize,
    ) DefaultSnapshotWriteError!void {
        try self.ensureConfiguredStorageFolderExists(dir);
        const path = try self.defaultPersistenceSnapshotPath(
            self.allocator,
            snapshot_id,
            module_type,
            instance_id,
        );
        defer self.allocator.free(path);
        try self.captureSnapshotToFile(
            dir,
            path,
            symbols,
            depth,
        );
        try self.appendDefaultPersistenceMainLogEntry(
            dir,
            self.kernel.next_seq,
            currentWallClockTimestampNs(),
            snapshot_id,
            module_type,
            instance_id,
        );
    }

    pub fn validateSnapshotDefaultPath(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) DefaultSnapshotValidateError!void {
        const path = try self.defaultPersistenceSnapshotPath(
            self.allocator,
            snapshot_id,
            module_type,
            instance_id,
        );
        defer self.allocator.free(path);
        try self.validateSnapshotFile(
            dir,
            path,
        );
    }

    pub fn snapshotExistsDefaultPath(
        self: *const ExchangeCore,
        dir: std.fs.Dir,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) DefaultSnapshotExistsError!bool {
        const path = try self.defaultPersistenceSnapshotPath(
            self.allocator,
            snapshot_id,
            module_type,
            instance_id,
        );
        defer self.allocator.free(path);
        return self.snapshotExistsFile(dir, path);
    }

    fn ensureConfiguredStorageFolderExists(
        self: *const ExchangeCore,
        dir: std.fs.Dir,
    ) StorageFolderEnsureError!void {
        self.assertWiringConst();
        try dir.makePath(self.config.serialization.storage_folder);
    }

    fn appendDefaultPersistenceMainLogEntry(
        self: *const ExchangeCore,
        dir: std.fs.Dir,
        seq: u64,
        timestamp_ns: u64,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) (PersistencePathError || StorageFolderEnsureError || MainLogWriteError)!void {
        self.assertWiringConst();
        try self.ensureConfiguredStorageFolderExists(dir);
        const path = try self.defaultPersistenceMainLogPath(self.allocator);
        defer self.allocator.free(path);
        try appendMainLogEntryToFile(
            dir,
            path,
            seq,
            timestamp_ns,
            snapshot_id,
            module_type,
            instance_id,
        );
    }

    pub fn writeJournalToFile(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        path: []const u8,
    ) JournalApiError!void {
        self.assertWiring();
        if (self.journal) |*journal| {
            try journal.writeToFile(dir, path);
            try self.syncJournalDescriptorFromCurrentSnapshot();
            return;
        }
        return error.JournalingDisabled;
    }

    pub fn captureSnapshotToFile(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        path: []const u8,
        symbols: []const i32,
        depth: usize,
    ) SnapshotWriteError!void {
        self.assertWiring();
        var snapshot = try snapshot_mod.KernelSnapshot.captureFromKernel(
            self.allocator,
            self.kernel,
            symbols,
            depth,
        );
        defer snapshot.deinit();
        try snapshot.writeToFile(dir, path);
    }

    pub fn validateSnapshotFile(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        path: []const u8,
    ) SnapshotValidateError!void {
        self.assertWiring();
        var snapshot = try snapshot_mod.KernelSnapshot.readFromFile(self.allocator, dir, path);
        defer snapshot.deinit();
        try snapshot.validateAgainstKernel(self.kernel);
    }

    pub fn snapshotExistsFile(
        self: *const ExchangeCore,
        dir: std.fs.Dir,
        path: []const u8,
    ) SnapshotExistsError!bool {
        self.assertWiringConst();
        _ = dir.statFile(path) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        return true;
    }

    fn appendMainLogEntryToFile(
        dir: std.fs.Dir,
        path: []const u8,
        seq: u64,
        timestamp_ns: u64,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) MainLogWriteError!void {
        var file = try dir.createFile(path, .{
            .truncate = false,
        });
        defer file.close();
        try file.seekFromEnd(0);

        const wall_clock_ms_i64 = std.time.milliTimestamp();
        const wall_clock_ms: u64 = if (wall_clock_ms_i64 < 0)
            0
        else
            @as(u64, @intCast(wall_clock_ms_i64));

        var line_buf: [256]u8 = undefined;
        const line = try std.fmt.bufPrint(
            &line_buf,
            "{d} seq={d} timestampNs={d} snapshotId={d} type={s} instance={d}\n",
            .{
                wall_clock_ms,
                seq,
                timestamp_ns,
                snapshot_id,
                persistence_paths.moduleCode(module_type),
                instance_id,
            },
        );
        try file.writeAll(line);
    }

    pub fn loadJournalFromFile(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        path: []const u8,
    ) JournalApiError!void {
        self.assertWiring();
        if (self.journal) |*journal| {
            const loaded = try journal_mod.CommandJournal.readFromFile(self.allocator, dir, path);
            journal.deinit();
            journal.* = loaded;
            try self.syncJournalDescriptorFromCurrentSnapshot();
            return;
        }
        return error.JournalingDisabled;
    }

    pub fn replayJournalFromFile(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        path: []const u8,
    ) JournalReplayError!usize {
        self.assertWiring();
        if (self.journal) |*journal| {
            var loaded = try journal_mod.CommandJournal.readFromFile(self.allocator, dir, path);
            errdefer loaded.deinit();

            const replay_window = try loaded.replayIntoKernelFromSequenceUntilTimestamp(
                self.kernel,
                self.config.initial_state.snapshot_base_seq,
                self.config.initial_state.journal_timestamp_ns,
            );
            const replayed = replay_window.replayed;
            if (replayed == 0) {
                loaded.entries.clearRetainingCapacity();
            } else {
                const start = replay_window.start_index;
                std.debug.assert(start + replayed <= loaded.entries.items.len);
                if (start != 0) {
                    const JournalEntry = std.meta.Child(@TypeOf(loaded.entries.items));
                    std.mem.copyForwards(
                        JournalEntry,
                        loaded.entries.items[0..replayed],
                        loaded.entries.items[start .. start + replayed],
                    );
                }
                loaded.entries.items.len = replayed;
            }

            journal.deinit();
            journal.* = loaded;
            try self.syncJournalDescriptorFromCurrentSnapshot();
            return replayed;
        }
        return error.JournalingDisabled;
    }

    pub fn submitCommand(self: *ExchangeCore, command: *cmd.OrderCommand) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        try self.ensureRunning();
        if (self.journal) |*journal| {
            const response = try journal.executeAndRecord(self.kernel, command);
            try self.registerPersistDescriptorIfNeeded(command, response.sequence);
            self.publishCommandResult(response.sequence, command, &response);
            return response;
        }
        const response = try self.api.submitCommand(command);
        try self.registerPersistDescriptorIfNeeded(command, response.sequence);
        self.publishCommandResult(response.sequence, command, &response);
        return response;
    }

    pub fn submitCommandBatch(
        self: *ExchangeCore,
        commands: []*cmd.OrderCommand,
    ) ExecuteError!kernel_mod.BatchResponse {
        self.assertWiring();
        try self.ensureRunning();
        if (self.journal) |*journal| {
            var response: kernel_mod.BatchResponse = .{};
            var has_success = false;
            var first_failure: ?cmd.CommandResultCode = null;

            for (commands) |command| {
                var single = try journal.executeAndRecord(self.kernel, command);
                defer single.deinit(self.allocator);

                if (response.sequence_from == null) {
                    response.sequence_from = single.sequence;
                }
                response.sequence_to = single.sequence;

                switch (single.result_code) {
                    .accepted => {},
                    .success => has_success = true,
                    else => {
                        if (first_failure == null) {
                            first_failure = single.result_code;
                        }
                    },
                }

                try self.registerPersistDescriptorIfNeeded(command, single.sequence);
                self.publishCommandResult(single.sequence, command, &single);
            }

            response.result_code = if (first_failure) |failed|
                failed
            else if (has_success)
                .success
            else
                .accepted;
            return response;
        }

        var response: kernel_mod.BatchResponse = .{};
        var has_success = false;
        var first_failure: ?cmd.CommandResultCode = null;

        for (commands) |command| {
            var single = try self.api.submitCommand(command);
            defer single.deinit(self.allocator);

            if (response.sequence_from == null) {
                response.sequence_from = single.sequence;
            }
            response.sequence_to = single.sequence;

            switch (single.result_code) {
                .accepted => {},
                .success => has_success = true,
                else => {
                    if (first_failure == null) {
                        first_failure = single.result_code;
                    }
                },
            }

            try self.registerPersistDescriptorIfNeeded(command, single.sequence);
            self.publishCommandResult(single.sequence, command, &single);
        }

        response.result_code = if (first_failure) |failed|
            failed
        else if (has_success)
            .success
        else
            .accepted;
        return response;
    }

    pub fn queryReport(self: *ExchangeCore, query: kernel_mod.ReportQuery) ReportError!kernel_mod.ReportResponse {
        self.assertWiring();
        return self.api.queryReport(query);
    }

    pub fn submitCommandAsync(self: *ExchangeCore, command: *const cmd.OrderCommand) AsyncSubmitError!u64 {
        self.assertWiring();
        try self.ensureRunning();
        return self.api.submitCommandAsync(command);
    }

    pub fn drainAsyncPending(self: *ExchangeCore) ExecuteError!usize {
        self.assertWiring();
        try self.ensureRunning();
        var processed: usize = 0;
        errdefer self.dropProcessedAsyncPending(processed);

        while (processed < self.api.pending_queue.items.len) {
            const entry = self.api.pending_queue.items[processed];
            var command = entry.command;
            var response = try self.submitCommand(&command);
            errdefer response.deinit(self.allocator);
            try self.api.ready_results.put(self.allocator, entry.request_id, response);
            processed += 1;
        }

        self.dropProcessedAsyncPending(processed);
        return processed;
    }

    pub fn takeAsyncResult(self: *ExchangeCore, request_id: u64) ?kernel_mod.CommandResponse {
        self.assertWiring();
        return self.api.takeResult(request_id);
    }

    pub fn isAsyncResultReady(self: *const ExchangeCore, request_id: u64) bool {
        self.assertWiringConst();
        return self.api.isResultReady(request_id);
    }

    pub fn asyncPendingCount(self: *const ExchangeCore) usize {
        self.assertWiringConst();
        return self.api.pendingCount();
    }

    pub fn asyncReadyCount(self: *const ExchangeCore) usize {
        self.assertWiringConst();
        return self.api.readyCount();
    }

    pub fn enqueueRuntime(
        self: *ExchangeCore,
        command: cmd.OrderCommand,
    ) (LifecycleError || std.mem.Allocator.Error)!void {
        self.assertWiring();
        try self.ensureRunning();
        try self.runtime.enqueue(command);
    }

    pub fn drainRuntime(self: *ExchangeCore) RuntimeDrainError!runtime_mod.DrainOutcome {
        self.assertWiring();
        try self.ensureRunning();
        var outcome: runtime_mod.DrainOutcome = .{};
        var start: usize = 0;

        while (start < self.runtime.pending.items.len) {
            const batch_len = self.runtime.batchLenForPending(start);
            var pointers = try self.allocator.alloc(*cmd.OrderCommand, batch_len);
            defer self.allocator.free(pointers);

            for (0..batch_len) |idx| {
                pointers[idx] = &self.runtime.pending.items[start + idx];
            }

            self.runtime.stampBatchMetadata(pointers);
            const batch = try self.submitCommandBatch(pointers);
            outcome.groups_processed += 1;
            outcome.commands_processed += batch_len;
            outcome.last_merged_result = batch.result_code;

            start += batch_len;
        }

        self.runtime.pending.clearRetainingCapacity();
        return outcome;
    }

    pub fn runtimePendingCount(self: *const ExchangeCore) usize {
        self.assertWiringConst();
        return self.runtime.pendingCount();
    }

    pub fn setResultSink(self: *ExchangeCore, sink: runtime_mod.ResultSink) void {
        self.assertWiring();
        self.runtime.sink = sink;
    }

    pub fn setSimpleEventsHandler(self: *ExchangeCore, handler: SimpleEventsHandler) void {
        self.assertWiring();
        self.clearSimpleEventsHandler();
        self.simple_events_processor = simple_events_mod.SimpleEventsProcessor.init(
            self.allocator,
            handler,
        );
    }

    pub fn clearSimpleEventsHandler(self: *ExchangeCore) void {
        self.assertWiring();
        if (self.simple_events_processor) |*processor| {
            processor.deinit();
            self.simple_events_processor = null;
        }
    }

    pub fn addUser(self: *ExchangeCore, uid: i64) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeAddUserCommand(uid);
        return self.submitCommand(&command);
    }

    pub fn adjustUserBalance(
        self: *ExchangeCore,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
    ) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeBalanceAdjustmentCommand(
            uid,
            currency,
            amount,
            transaction_id,
            adjustment_type,
        );
        return self.submitCommand(&command);
    }

    pub fn placeOrder(
        self: *ExchangeCore,
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
        self.assertWiring();
        var command = makePlaceOrderCommand(
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
        return self.submitCommand(&command);
    }

    pub fn moveOrder(
        self: *ExchangeCore,
        order_id: i64,
        uid: i64,
        symbol: i32,
        new_price: i64,
    ) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeMoveOrderCommand(order_id, uid, symbol, new_price);
        return self.submitCommand(&command);
    }

    pub fn cancelOrder(
        self: *ExchangeCore,
        order_id: i64,
        uid: i64,
        symbol: i32,
    ) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeCancelOrderCommand(order_id, uid, symbol);
        return self.submitCommand(&command);
    }

    pub fn reduceOrder(
        self: *ExchangeCore,
        order_id: i64,
        uid: i64,
        symbol: i32,
        reduce_size: i64,
    ) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeReduceOrderCommand(order_id, uid, symbol, reduce_size);
        return self.submitCommand(&command);
    }

    pub fn suspendUser(self: *ExchangeCore, uid: i64) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeSuspendUserCommand(uid);
        return self.submitCommand(&command);
    }

    pub fn resumeUser(self: *ExchangeCore, uid: i64) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeResumeUserCommand(uid);
        return self.submitCommand(&command);
    }

    pub fn persistState(
        self: *ExchangeCore,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) ExecuteError!cmd.CommandResultCode {
        self.assertWiring();
        var matching = makePersistStateMatchingCommand(snapshot_id, timestamp_ns);
        var risk_cmd = makePersistStateRiskCommand(snapshot_id, timestamp_ns);
        var commands = [_]*cmd.OrderCommand{
            &matching,
            &risk_cmd,
        };
        const batch = try self.submitCommandBatch(commands[0..]);
        return batch.result_code;
    }

    pub fn persistStateWithDefaultMainLog(
        self: *ExchangeCore,
        dir: std.fs.Dir,
        snapshot_id: i64,
        timestamp_ns: i64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) PersistStateWithMainLogError!cmd.CommandResultCode {
        self.assertWiring();

        const result = try self.persistState(snapshot_id, timestamp_ns);
        if (result != .success) return error.PersistStateRejected;

        const snapshot_id_u64 = std.math.cast(u64, snapshot_id) orelse return error.PersistStateSnapshotIdOverflow;
        const timestamp_ns_u64 = std.math.cast(u64, timestamp_ns) orelse return error.PersistStateTimestampOverflow;
        const persisted_snapshot = self.persistenceCurrentSnapshot();
        if (persisted_snapshot.snapshot_id != snapshot_id_u64 or
            persisted_snapshot.timestamp_ns != timestamp_ns_u64)
        {
            return error.PersistStateSnapshotMismatch;
        }

        try self.appendDefaultPersistenceMainLogEntry(
            dir,
            persisted_snapshot.seq,
            persisted_snapshot.timestamp_ns,
            persisted_snapshot.snapshot_id,
            module_type,
            instance_id,
        );
        return result;
    }

    pub fn groupingControl(
        self: *ExchangeCore,
        timestamp_ns: i64,
        mode: i64,
    ) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeGroupingControlModeCommand(timestamp_ns, mode);
        return self.submitCommand(&command);
    }

    pub fn requestOrderBook(
        self: *ExchangeCore,
        symbol: i32,
        depth: usize,
    ) (ExecuteError || HelperError)!kernel_mod.CommandResponse {
        self.assertWiring();
        const depth_i64 = std.math.cast(i64, depth) orelse return error.OrderBookDepthOverflow;
        var command = makeRequestOrderBookCommandI64(symbol, depth_i64);
        return self.submitCommand(&command);
    }

    pub fn submitBinaryPayload(
        self: *ExchangeCore,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadSubmitError!kernel_mod.CommandResponse {
        self.assertWiring();
        switch (command_type) {
            .binary_data_command, .binary_data_query => {},
            else => return error.InvalidBinaryCommandType,
        }
        const frame_layout = try computeBinaryFrameLayout(payload.len);
        const frame_words = binary_frames.FrameWordsPerCommand;

        var last_response: ?kernel_mod.CommandResponse = null;
        errdefer if (last_response) |*resp| resp.deinit(self.allocator);

        for (0..frame_layout.frame_count) |frame_idx| {
            var frame_payload: [binary_frames.FrameWordsPerCommand]i64 = [_]i64{0} ** binary_frames.FrameWordsPerCommand;
            for (0..frame_words) |word_idx| {
                frame_payload[word_idx] = readBigEndianWordPadded(payload, frame_idx * frame_words + word_idx);
            }

            var frame_command = makeBinaryFrameCommand(
                command_type,
                transfer_id,
                if (frame_idx + 1 == frame_layout.frame_count) -1 else 0,
                timestamp,
                frame_payload,
            );
            const response = try self.submitCommand(&frame_command);

            if (last_response) |*prev| {
                prev.deinit(self.allocator);
            }
            last_response = response;
        }

        return last_response.?;
    }

    pub fn submitBinaryPayloadAsync(
        self: *ExchangeCore,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadAsyncSubmitError!u64 {
        self.assertWiring();
        switch (command_type) {
            .binary_data_command, .binary_data_query => {},
            else => return error.InvalidBinaryCommandType,
        }
        const frame_layout = try computeBinaryFrameLayout(payload.len);
        const frame_words = binary_frames.FrameWordsPerCommand;

        var last_request_id: ?u64 = null;
        for (0..frame_layout.frame_count) |frame_idx| {
            var frame_payload: [binary_frames.FrameWordsPerCommand]i64 = [_]i64{0} ** binary_frames.FrameWordsPerCommand;
            for (0..frame_words) |word_idx| {
                frame_payload[word_idx] = readBigEndianWordPadded(payload, frame_idx * frame_words + word_idx);
            }

            const frame_command = makeBinaryFrameCommand(
                command_type,
                transfer_id,
                if (frame_idx + 1 == frame_layout.frame_count) -1 else 0,
                timestamp,
                frame_payload,
            );
            last_request_id = try self.submitCommandAsync(&frame_command);
        }

        return last_request_id.?;
    }

    pub fn submitAddSymbolsBinary(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        symbols: []const common.CoreSymbolSpecification,
    ) BinaryPayloadTypedSubmitError!kernel_mod.CommandResponse {
        self.assertWiring();
        const payload = try binary_payload.encodeAddSymbolsToBytes(self.allocator, symbols);
        defer self.allocator.free(payload);
        return self.submitBinaryPayload(.binary_data_command, transfer_id, timestamp, payload);
    }

    pub fn submitAddSymbolsBinaryAsync(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        symbols: []const common.CoreSymbolSpecification,
    ) BinaryPayloadTypedAsyncSubmitError!u64 {
        self.assertWiring();
        const payload = try binary_payload.encodeAddSymbolsToBytes(self.allocator, symbols);
        defer self.allocator.free(payload);
        return self.submitBinaryPayloadAsync(.binary_data_command, transfer_id, timestamp, payload);
    }

    pub fn submitAddAccountsBinary(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        users: []const BinaryUserAccountsBatch,
    ) BinaryPayloadTypedSubmitError!kernel_mod.CommandResponse {
        self.assertWiring();
        const payload = try binary_payload.encodeAddAccountsToBytes(self.allocator, users);
        defer self.allocator.free(payload);
        return self.submitBinaryPayload(.binary_data_command, transfer_id, timestamp, payload);
    }

    pub fn submitAddAccountsBinaryAsync(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        users: []const BinaryUserAccountsBatch,
    ) BinaryPayloadTypedAsyncSubmitError!u64 {
        self.assertWiring();
        const payload = try binary_payload.encodeAddAccountsToBytes(self.allocator, users);
        defer self.allocator.free(payload);
        return self.submitBinaryPayloadAsync(.binary_data_command, transfer_id, timestamp, payload);
    }

    pub fn submitBinaryReportQuery(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) BinaryReportQuerySubmitError!kernel_mod.CommandResponse {
        self.assertWiring();
        const payload = try binary_payload.encodeReportQueryToBytes(self.allocator, query);
        defer self.allocator.free(payload);
        return self.submitBinaryPayload(.binary_data_query, transfer_id, timestamp, payload);
    }

    pub fn submitBinaryReportQueryAsync(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) BinaryReportQueryAsyncSubmitError!u64 {
        self.assertWiring();
        const payload = try binary_payload.encodeReportQueryToBytes(self.allocator, query);
        defer self.allocator.free(payload);
        return self.submitBinaryPayloadAsync(.binary_data_query, transfer_id, timestamp, payload);
    }

    pub fn reset(self: *ExchangeCore) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeResetCommand();
        return self.submitCommand(&command);
    }

    pub fn nop(self: *ExchangeCore) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeNopCommand();
        return self.submitCommand(&command);
    }

    pub fn addUserAsync(self: *ExchangeCore, uid: i64) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeAddUserCommand(uid);
        return self.submitCommandAsync(&command);
    }

    pub fn adjustUserBalanceAsync(
        self: *ExchangeCore,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
    ) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeBalanceAdjustmentCommand(
            uid,
            currency,
            amount,
            transaction_id,
            adjustment_type,
        );
        return self.submitCommandAsync(&command);
    }

    pub fn suspendUserAsync(self: *ExchangeCore, uid: i64) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeSuspendUserCommand(uid);
        return self.submitCommandAsync(&command);
    }

    pub fn resumeUserAsync(self: *ExchangeCore, uid: i64) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeResumeUserCommand(uid);
        return self.submitCommandAsync(&command);
    }

    pub fn placeOrderAsync(
        self: *ExchangeCore,
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
        self.assertWiring();
        const command = makePlaceOrderCommand(
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
        return self.submitCommandAsync(&command);
    }

    pub fn moveOrderAsync(
        self: *ExchangeCore,
        order_id: i64,
        uid: i64,
        symbol: i32,
        new_price: i64,
    ) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeMoveOrderCommand(order_id, uid, symbol, new_price);
        return self.submitCommandAsync(&command);
    }

    pub fn cancelOrderAsync(
        self: *ExchangeCore,
        order_id: i64,
        uid: i64,
        symbol: i32,
    ) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeCancelOrderCommand(order_id, uid, symbol);
        return self.submitCommandAsync(&command);
    }

    pub fn reduceOrderAsync(
        self: *ExchangeCore,
        order_id: i64,
        uid: i64,
        symbol: i32,
        reduce_size: i64,
    ) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeReduceOrderCommand(order_id, uid, symbol, reduce_size);
        return self.submitCommandAsync(&command);
    }

    pub fn resetAsync(self: *ExchangeCore) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeResetCommand();
        return self.submitCommandAsync(&command);
    }

    pub fn groupingControlAsync(
        self: *ExchangeCore,
        timestamp_ns: i64,
        mode: i64,
    ) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeGroupingControlModeCommand(timestamp_ns, mode);
        return self.submitCommandAsync(&command);
    }

    pub fn nopAsync(self: *ExchangeCore) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeNopCommand();
        return self.submitCommandAsync(&command);
    }

    pub fn persistStateAsync(
        self: *ExchangeCore,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) AsyncSubmitError!PersistStateRequestPair {
        self.assertWiring();
        try self.ensureRunning();
        return self.api.persistStateAsync(snapshot_id, timestamp_ns);
    }

    pub fn isPersistStateResultReady(
        self: *const ExchangeCore,
        requests: PersistStateRequestPair,
    ) bool {
        self.assertWiringConst();
        return self.api.isPersistStateResultReady(requests);
    }

    pub fn takePersistStateResult(
        self: *ExchangeCore,
        requests: PersistStateRequestPair,
    ) ?cmd.CommandResultCode {
        self.assertWiring();
        return self.api.takePersistStateResult(requests);
    }

    pub fn requestOrderBookAsync(
        self: *ExchangeCore,
        symbol: i32,
        depth: usize,
    ) (AsyncSubmitError || HelperError)!u64 {
        self.assertWiring();
        const depth_i64 = std.math.cast(i64, depth) orelse return error.OrderBookDepthOverflow;
        const command = makeRequestOrderBookCommandI64(symbol, depth_i64);
        return self.submitCommandAsync(&command);
    }

    pub fn submitApiCommand(
        self: *ExchangeCore,
        api_command: ApiCommand,
    ) SubmitApiCommandError!ApiCommandResult {
        self.assertWiring();
        return switch (api_command) {
            .raw_command => |command| blk: {
                var mutable = command;
                break :blk .{ .response = try self.submitCommand(&mutable) };
            },
            .add_user => |uid| .{ .response = try self.addUser(uid) },
            .adjust_user_balance => |payload| .{ .response = try self.adjustUserBalance(
                payload.uid,
                payload.transaction_id,
                payload.currency,
                payload.amount,
                payload.adjustment_type,
            ) },
            .suspend_user => |uid| .{ .response = try self.suspendUser(uid) },
            .resume_user => |uid| .{ .response = try self.resumeUser(uid) },
            .place_order => |payload| .{ .response = try self.placeOrder(
                payload.order_id,
                payload.uid,
                payload.symbol,
                payload.price,
                payload.reserve_bid_price,
                payload.size,
                payload.action,
                payload.order_type,
                payload.user_cookie,
            ) },
            .move_order => |payload| .{ .response = try self.moveOrder(
                payload.order_id,
                payload.uid,
                payload.symbol,
                payload.new_price,
            ) },
            .cancel_order => |payload| .{ .response = try self.cancelOrder(
                payload.order_id,
                payload.uid,
                payload.symbol,
            ) },
            .reduce_order => |payload| .{ .response = try self.reduceOrder(
                payload.order_id,
                payload.uid,
                payload.symbol,
                payload.reduce_size,
            ) },
            .request_order_book => |payload| .{ .response = try self.requestOrderBook(
                payload.symbol,
                payload.depth,
            ) },
            .persist_state => |payload| .{ .persist_state = try self.persistState(
                payload.snapshot_id,
                payload.timestamp_ns,
            ) },
            .grouping_control => |payload| .{ .response = try self.groupingControl(
                payload.timestamp_ns,
                payload.mode,
            ) },
            .reset => .{ .response = try self.reset() },
            .nop => .{ .response = try self.nop() },
            .binary_add_symbols => |payload| .{ .response = try self.submitAddSymbolsBinary(
                payload.transfer_id,
                payload.timestamp,
                payload.symbols,
            ) },
            .binary_add_accounts => |payload| .{ .response = try self.submitAddAccountsBinary(
                payload.transfer_id,
                payload.timestamp,
                payload.users,
            ) },
            .binary_report_query => |payload| .{ .response = try self.submitBinaryReportQuery(
                payload.transfer_id,
                payload.timestamp,
                payload.query,
            ) },
        };
    }

    pub fn submitApiCommandAsync(
        self: *ExchangeCore,
        api_command: ApiCommand,
    ) SubmitApiCommandAsyncError!ApiCommandAsyncHandle {
        self.assertWiring();
        return switch (api_command) {
            .raw_command => |command| .{ .request_id = try self.submitCommandAsync(&command) },
            .add_user => |uid| .{ .request_id = try self.addUserAsync(uid) },
            .adjust_user_balance => |payload| .{ .request_id = try self.adjustUserBalanceAsync(
                payload.uid,
                payload.transaction_id,
                payload.currency,
                payload.amount,
                payload.adjustment_type,
            ) },
            .suspend_user => |uid| .{ .request_id = try self.suspendUserAsync(uid) },
            .resume_user => |uid| .{ .request_id = try self.resumeUserAsync(uid) },
            .place_order => |payload| .{ .request_id = try self.placeOrderAsync(
                payload.order_id,
                payload.uid,
                payload.symbol,
                payload.price,
                payload.reserve_bid_price,
                payload.size,
                payload.action,
                payload.order_type,
                payload.user_cookie,
            ) },
            .move_order => |payload| .{ .request_id = try self.moveOrderAsync(
                payload.order_id,
                payload.uid,
                payload.symbol,
                payload.new_price,
            ) },
            .cancel_order => |payload| .{ .request_id = try self.cancelOrderAsync(
                payload.order_id,
                payload.uid,
                payload.symbol,
            ) },
            .reduce_order => |payload| .{ .request_id = try self.reduceOrderAsync(
                payload.order_id,
                payload.uid,
                payload.symbol,
                payload.reduce_size,
            ) },
            .request_order_book => |payload| .{ .request_id = try self.requestOrderBookAsync(
                payload.symbol,
                payload.depth,
            ) },
            .persist_state => |payload| .{ .persist_state = try self.persistStateAsync(
                payload.snapshot_id,
                payload.timestamp_ns,
            ) },
            .grouping_control => |payload| .{ .request_id = try self.groupingControlAsync(
                payload.timestamp_ns,
                payload.mode,
            ) },
            .reset => .{ .request_id = try self.resetAsync() },
            .nop => .{ .request_id = try self.nopAsync() },
            .binary_add_symbols => |payload| .{ .request_id = try self.submitAddSymbolsBinaryAsync(
                payload.transfer_id,
                payload.timestamp,
                payload.symbols,
            ) },
            .binary_add_accounts => |payload| .{ .request_id = try self.submitAddAccountsBinaryAsync(
                payload.transfer_id,
                payload.timestamp,
                payload.users,
            ) },
            .binary_report_query => |payload| .{ .request_id = try self.submitBinaryReportQueryAsync(
                payload.transfer_id,
                payload.timestamp,
                payload.query,
            ) },
        };
    }

    pub fn isApiCommandAsyncResultReady(
        self: *const ExchangeCore,
        handle: ApiCommandAsyncHandle,
    ) bool {
        self.assertWiringConst();
        return switch (handle) {
            .request_id => |request_id| self.isAsyncResultReady(request_id),
            .persist_state => |requests| self.isPersistStateResultReady(requests),
        };
    }

    pub fn takeApiCommandAsyncResult(
        self: *ExchangeCore,
        handle: ApiCommandAsyncHandle,
    ) ?ApiCommandResult {
        self.assertWiring();
        return switch (handle) {
            .request_id => |request_id| if (self.takeAsyncResult(request_id)) |response|
                .{ .response = response }
            else
                null,
            .persist_state => |requests| if (self.takePersistStateResult(requests)) |result_code|
                .{ .persist_state = result_code }
            else
                null,
        };
    }

    pub fn submitApiCommandsSync(
        self: *ExchangeCore,
        commands: []const ApiCommand,
    ) SubmitApiCommandBatchError!ApiCommandResult {
        self.assertWiring();
        if (commands.len == 0) return error.EmptyApiCommandBatch;

        var handles = try self.allocator.alloc(ApiCommandAsyncHandle, commands.len);
        defer self.allocator.free(handles);

        for (commands, 0..) |api_command, idx| {
            handles[idx] = try self.submitApiCommandAsync(api_command);
        }

        _ = try self.drainAsyncPending();

        for (handles, 0..) |handle, idx| {
            var result = self.takeApiCommandAsyncResult(handle) orelse return error.MissingBatchResult;
            if (idx + 1 == handles.len) {
                return result;
            }
            result.deinit(self.allocator);
        }

        return error.MissingBatchResult;
    }

    pub fn submitApiCommandsSyncWithNop(
        self: *ExchangeCore,
        commands: []const ApiCommand,
    ) SubmitApiCommandBatchError!kernel_mod.CommandResponse {
        self.assertWiring();

        var handles = try self.allocator.alloc(ApiCommandAsyncHandle, commands.len + 1);
        defer self.allocator.free(handles);

        for (commands, 0..) |api_command, idx| {
            handles[idx] = try self.submitApiCommandAsync(api_command);
        }
        handles[commands.len] = try self.submitApiCommandAsync(.nop);

        _ = try self.drainAsyncPending();

        for (handles[0..commands.len]) |handle| {
            var result = self.takeApiCommandAsyncResult(handle) orelse return error.MissingBatchResult;
            result.deinit(self.allocator);
        }

        var flush_result = self.takeApiCommandAsyncResult(handles[commands.len]) orelse return error.MissingBatchResult;
        errdefer flush_result.deinit(self.allocator);
        try expectFlushResultVariant(&flush_result);
        return switch (flush_result) {
            .response => |response| response,
            .persist_state => return error.UnexpectedFlushResultVariant,
        };
    }

    fn registerSnapshotDescriptor(
        self: *ExchangeCore,
        snapshot_id: u64,
        sequence: u64,
        timestamp_ns: u64,
    ) DescriptorTrackingError!void {
        self.current_snapshot_ref = try self.descriptors.createNextSnapshot(
            self.current_snapshot_ref,
            snapshot_id,
            sequence,
            timestamp_ns,
        );
        self.last_journal_ref = null;
    }

    fn registerPersistDescriptorIfNeeded(
        self: *ExchangeCore,
        command: *const cmd.OrderCommand,
        sequence: u64,
    ) DescriptorTrackingError!void {
        if (command.command != .persist_state_risk) return;
        if (command.result_code != .success) return;

        const snapshot_id = std.math.cast(u64, command.order_id) orelse return error.InvalidPersistSnapshotId;
        const timestamp_ns = std.math.cast(u64, command.timestamp) orelse return error.InvalidPersistTimestamp;
        try self.registerSnapshotDescriptor(snapshot_id, sequence, timestamp_ns);
    }

    fn syncJournalDescriptorFromCurrentSnapshot(self: *ExchangeCore) DescriptorTrackingError!void {
        const journal = &(self.journal orelse return);
        if (journal.entries.items.len == 0) return;

        const current_snapshot = self.descriptors.snapshotConst(self.current_snapshot_ref);
        const snapshot_seq = current_snapshot.seq;

        var start_index: usize = 0;
        while (start_index < journal.entries.items.len and
            journal.entries.items[start_index].sequence <= snapshot_seq) : (start_index += 1)
        {}
        if (start_index >= journal.entries.items.len) return;

        const seq_first = journal.entries.items[start_index].sequence;
        const seq_last = journal.entries.items[journal.entries.items.len - 1].sequence;
        const timestamp_ns = std.math.cast(u64, journal.entries.items[start_index].input.timestamp) orelse
            return error.InvalidPersistTimestamp;

        var target_ref: ?JournalRef = null;
        if (self.last_journal_ref) |last_ref| {
            const last_desc = self.descriptors.journalConst(last_ref);
            if (last_desc.base_snapshot == self.current_snapshot_ref and
                last_desc.seq_first == seq_first)
            {
                target_ref = last_ref;
            }
        }

        if (target_ref == null) {
            if (self.descriptors.findJournalForSequence(self.current_snapshot_ref, seq_first)) |existing_ref| {
                const existing_desc = self.descriptors.journalConst(existing_ref);
                if (existing_desc.seq_first == seq_first) {
                    target_ref = existing_ref;
                }
            }
        }

        if (target_ref == null) {
            const created = try self.descriptors.createJournal(
                self.current_snapshot_ref,
                timestamp_ns,
                seq_first,
            );
            target_ref = created;
        }

        const journal_ref = target_ref.?;
        var descriptor = self.descriptors.journal(journal_ref);
        if (timestamp_ns < descriptor.timestamp_ns) {
            descriptor.timestamp_ns = timestamp_ns;
        }
        if (descriptor.seq_last) |known_last| {
            if (seq_last < known_last) return error.JournalSequenceNotMonotonic;
            if (seq_last > known_last) {
                try self.descriptors.finishJournal(journal_ref, seq_last);
            }
        } else {
            try self.descriptors.finishJournal(journal_ref, seq_last);
        }
        self.last_journal_ref = journal_ref;
    }

    fn assertWiring(self: *ExchangeCore) void {
        std.debug.assert(self.api.kernel == self.kernel);
        std.debug.assert(self.runtime.kernel == self.kernel);
        std.debug.assert(@intFromEnum(self.current_snapshot_ref) < self.descriptors.snapshotCount());
        if (self.last_journal_ref) |journal_ref| {
            std.debug.assert(@intFromEnum(journal_ref) < self.descriptors.journalCount());
        }
    }

    fn assertWiringConst(self: *const ExchangeCore) void {
        std.debug.assert(self.api.kernel == self.kernel);
        std.debug.assert(self.runtime.kernel == self.kernel);
        std.debug.assert(@intFromEnum(self.current_snapshot_ref) < self.descriptors.snapshotCount());
        if (self.last_journal_ref) |journal_ref| {
            std.debug.assert(@intFromEnum(journal_ref) < self.descriptors.journalCount());
        }
    }

    fn ensureRunning(self: *const ExchangeCore) LifecycleError!void {
        if (self.stopped) return error.CoreStopped;
    }

    fn expectFlushResultVariant(result: *const ApiCommandResult) error{UnexpectedFlushResultVariant}!void {
        return switch (result.*) {
            .response => {},
            .persist_state => error.UnexpectedFlushResultVariant,
        };
    }

    fn publishCommandResult(
        self: *ExchangeCore,
        sequence: u64,
        command: *const cmd.OrderCommand,
        response: ?*const kernel_mod.CommandResponse,
    ) void {
        if (!self.runtime.shouldPublishResultForCommand(command)) {
            return;
        }
        publishRuntimeResult(self.runtime.sink, .{
            .sequence = sequence,
            .command = command.command,
            .uid = command.uid,
            .symbol = command.symbol,
            .result_code = command.result_code,
        });

        if (self.simple_events_processor) |*processor| {
            var synthetic_response: kernel_mod.CommandResponse = .{
                .sequence = sequence,
                .result_code = command.result_code,
            };
            const response_ref = response orelse &synthetic_response;
            processor.accept(
                command,
                sequence,
                response_ref,
                response_ref.matcher_events_head,
            ) catch |err| {
                log.err("simple-events callback failed cmd={s} seq={d} err={}", .{
                    @tagName(command.command),
                    sequence,
                    err,
                });
            };
        }
    }

    fn dropProcessedAsyncPending(self: *ExchangeCore, processed: usize) void {
        if (processed == 0) return;
        if (processed >= self.api.pending_queue.items.len) {
            self.api.pending_queue.clearRetainingCapacity();
            return;
        }

        const PendingEntry = std.meta.Child(@TypeOf(self.api.pending_queue.items));
        const remaining = self.api.pending_queue.items.len - processed;
        std.mem.copyForwards(
            PendingEntry,
            self.api.pending_queue.items[0..remaining],
            self.api.pending_queue.items[processed..],
        );
        self.api.pending_queue.items.len = remaining;
    }
};

fn makeAddUserCommand(uid: i64) cmd.OrderCommand {
    return .{
        .command = .add_user,
        .uid = uid,
        .result_code = .new,
    };
}

fn makeSuspendUserCommand(uid: i64) cmd.OrderCommand {
    return .{
        .command = .suspend_user,
        .uid = uid,
        .result_code = .new,
    };
}

fn makeResumeUserCommand(uid: i64) cmd.OrderCommand {
    return .{
        .command = .resume_user,
        .uid = uid,
        .result_code = .new,
    };
}

fn makeBalanceAdjustmentCommand(
    uid: i64,
    currency: i32,
    amount: i64,
    transaction_id: i64,
    adjustment_type: common.BalanceAdjustmentType,
) cmd.OrderCommand {
    return .{
        .command = .balance_adjustment,
        .uid = uid,
        .symbol = currency,
        .price = amount,
        .order_id = transaction_id,
        .order_type = switch (adjustment_type) {
            .adjustment => .gtc,
            .@"suspend" => .ioc,
        },
        .result_code = .new,
    };
}

fn makePlaceOrderCommand(
    order_id: i64,
    uid: i64,
    symbol: i32,
    price: i64,
    reserve_bid_price: i64,
    size: i64,
    action: common.OrderAction,
    order_type: common.OrderType,
    user_cookie: i32,
) cmd.OrderCommand {
    return .{
        .command = .place_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .price = price,
        .reserve_bid_price = reserve_bid_price,
        .size = size,
        .action = action,
        .order_type = order_type,
        .user_cookie = user_cookie,
        .result_code = .new,
    };
}

fn makeMoveOrderCommand(order_id: i64, uid: i64, symbol: i32, new_price: i64) cmd.OrderCommand {
    return .{
        .command = .move_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .price = new_price,
        .result_code = .new,
    };
}

fn makeCancelOrderCommand(order_id: i64, uid: i64, symbol: i32) cmd.OrderCommand {
    return .{
        .command = .cancel_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .result_code = .new,
    };
}

fn makeReduceOrderCommand(order_id: i64, uid: i64, symbol: i32, reduce_size: i64) cmd.OrderCommand {
    return .{
        .command = .reduce_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .size = reduce_size,
        .result_code = .new,
    };
}

fn makeRequestOrderBookCommandI64(symbol: i32, depth: i64) cmd.OrderCommand {
    return .{
        .command = .order_book_request,
        .symbol = symbol,
        .size = depth,
        .result_code = .new,
    };
}

fn makeResetCommand() cmd.OrderCommand {
    return .{
        .command = .reset,
        .result_code = .new,
    };
}

fn makeNopCommand() cmd.OrderCommand {
    return .{
        .command = .nop,
        .result_code = .new,
    };
}

fn makeBinaryFrameCommand(
    command_type: cmd.OrderCommandType,
    transfer_id: i32,
    last_flag: i32,
    timestamp: i64,
    words: [binary_frames.FrameWordsPerCommand]i64,
) cmd.OrderCommand {
    return .{
        .command = command_type,
        .user_cookie = transfer_id,
        .symbol = last_flag,
        .timestamp = timestamp,
        .order_id = words[0],
        .price = words[1],
        .reserve_bid_price = words[2],
        .size = words[3],
        .uid = words[4],
        .result_code = .new,
    };
}

fn readBigEndianWordPadded(payload: []const u8, word_index: usize) i64 {
    const start = word_index * @sizeOf(i64);
    if (start >= payload.len) return 0;

    const end = @min(start + @sizeOf(i64), payload.len);
    var raw: [8]u8 = [_]u8{0} ** 8;
    @memcpy(raw[0 .. end - start], payload[start..end]);
    return std.mem.readInt(i64, &raw, .big);
}

fn computeBinaryFrameLayout(payload_len: usize) error{ EmptyBinaryPayload, BinaryPayloadSizeOverflow }!BinaryFrameLayout {
    if (payload_len == 0) return error.EmptyBinaryPayload;

    const words_without_padding = std.math.divCeil(usize, payload_len, @sizeOf(i64)) catch
        return error.BinaryPayloadSizeOverflow;
    const frame_count = std.math.divCeil(usize, words_without_padding, binary_frames.FrameWordsPerCommand) catch
        return error.BinaryPayloadSizeOverflow;
    if (frame_count == 0) return error.BinaryPayloadSizeOverflow;

    return .{
        .words_without_padding = words_without_padding,
        .frame_count = frame_count,
    };
}

fn makeGroupingControlCommand(enabled: bool) cmd.OrderCommand {
    return makeGroupingControlModeCommand(0, if (enabled) 1 else 0);
}

fn makeGroupingControlModeCommand(timestamp_ns: i64, mode: i64) cmd.OrderCommand {
    return .{
        .command = .grouping_control,
        .order_id = mode,
        .timestamp = timestamp_ns,
        .result_code = .new,
    };
}

fn makePersistStateMatchingCommand(snapshot_id: i64, timestamp_ns: i64) cmd.OrderCommand {
    return .{
        .command = .persist_state_matching,
        .order_id = snapshot_id,
        .timestamp = timestamp_ns,
        .result_code = .new,
    };
}

fn makePersistStateRiskCommand(snapshot_id: i64, timestamp_ns: i64) cmd.OrderCommand {
    return .{
        .command = .persist_state_risk,
        .order_id = snapshot_id,
        .timestamp = timestamp_ns,
        .result_code = .new,
    };
}

fn currentWallClockTimestampNs() u64 {
    const wall_clock_ns_i128 = std.time.nanoTimestamp();
    if (wall_clock_ns_i128 < 0) return 0;
    return std.math.cast(u64, wall_clock_ns_i128) orelse std.math.maxInt(u64);
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

fn queryRiskStateHash(core: *ExchangeCore, allocator: std.mem.Allocator) (ReportError || ParityHelperError)!u64 {
    var report = try core.queryReport(.risk_state_hash);
    defer report.deinit(allocator);
    return switch (report) {
        .risk_state_hash => |hash| hash,
        else => error.UnexpectedReportVariant,
    };
}

fn queryDetailedStateHash(core: *ExchangeCore, allocator: std.mem.Allocator) (ReportError || ParityHelperError)!u64 {
    var report = try core.queryReport(.state_hash_detailed);
    defer report.deinit(allocator);
    return switch (report) {
        .state_hash_detailed => |detailed| detailed.aggregate_hash,
        else => error.UnexpectedReportVariant,
    };
}

fn mapValueFromI32I64Map(map: *const std.AutoHashMapUnmanaged(i32, i64), key: i32) i64 {
    return map.get(key) orelse 0;
}

fn expectExchangeCoreStateParity(
    allocator: std.mem.Allocator,
    source: *ExchangeCore,
    restored: *ExchangeCore,
    user_ids: []const i64,
    symbol: i32,
) !void {
    try std.testing.expectEqual(
        try queryRiskStateHash(source, allocator),
        try queryRiskStateHash(restored, allocator),
    );
    try std.testing.expectEqual(
        try queryDetailedStateHash(source, allocator),
        try queryDetailedStateHash(restored, allocator),
    );

    var source_total = try source.queryReport(.total_currency_balance);
    defer source_total.deinit(allocator);
    var restored_total = try restored.queryReport(.total_currency_balance);
    defer restored_total.deinit(allocator);
    switch (source_total) {
        .total_currency_balance => |*lhs| switch (restored_total) {
            .total_currency_balance => |*rhs| {
                try std.testing.expectEqual(
                    mapValueFromI32I64Map(&lhs.account_balances, 11),
                    mapValueFromI32I64Map(&rhs.account_balances, 11),
                );
                try std.testing.expectEqual(
                    mapValueFromI32I64Map(&lhs.account_balances, 15),
                    mapValueFromI32I64Map(&rhs.account_balances, 15),
                );
                try std.testing.expectEqual(
                    mapValueFromI32I64Map(&lhs.orders_balances, 11),
                    mapValueFromI32I64Map(&rhs.orders_balances, 11),
                );
                try std.testing.expectEqual(
                    mapValueFromI32I64Map(&lhs.orders_balances, 15),
                    mapValueFromI32I64Map(&rhs.orders_balances, 15),
                );
                try std.testing.expectEqual(
                    mapValueFromI32I64Map(&lhs.fees, 11),
                    mapValueFromI32I64Map(&rhs.fees, 11),
                );
                try std.testing.expectEqual(
                    mapValueFromI32I64Map(&lhs.fees, 15),
                    mapValueFromI32I64Map(&rhs.fees, 15),
                );
            },
            else => return error.UnexpectedReportVariant,
        },
        else => return error.UnexpectedReportVariant,
    }

    var source_book = try source.queryReport(.{
        .order_book = .{
            .symbol = symbol,
            .depth = 64,
        },
    });
    defer source_book.deinit(allocator);
    var restored_book = try restored.queryReport(.{
        .order_book = .{
            .symbol = symbol,
            .depth = 64,
        },
    });
    defer restored_book.deinit(allocator);
    switch (source_book) {
        .order_book => |*lhs| switch (restored_book) {
            .order_book => |*rhs| {
                try std.testing.expectEqual(lhs.asks.items.len, rhs.asks.items.len);
                try std.testing.expectEqual(lhs.bids.items.len, rhs.bids.items.len);
                const asks_len = lhs.asks.items.len;
                const bids_len = lhs.bids.items.len;
                for (0..asks_len) |idx| {
                    const left_level = lhs.asks.items[idx];
                    const right_level = rhs.asks.items[idx];
                    try std.testing.expectEqual(left_level.price, right_level.price);
                    try std.testing.expectEqual(left_level.volume, right_level.volume);
                    try std.testing.expectEqual(left_level.orders, right_level.orders);
                }
                for (0..bids_len) |idx| {
                    const left_level = lhs.bids.items[idx];
                    const right_level = rhs.bids.items[idx];
                    try std.testing.expectEqual(left_level.price, right_level.price);
                    try std.testing.expectEqual(left_level.volume, right_level.volume);
                    try std.testing.expectEqual(left_level.orders, right_level.orders);
                }
            },
            else => return error.UnexpectedReportVariant,
        },
        else => return error.UnexpectedReportVariant,
    }

    for (user_ids) |uid| {
        var source_user = try source.queryReport(.{ .single_user = uid });
        defer source_user.deinit(allocator);
        var restored_user = try restored.queryReport(.{ .single_user = uid });
        defer restored_user.deinit(allocator);
        switch (source_user) {
            .single_user => |*lhs| switch (restored_user) {
                .single_user => |*rhs| {
                    try std.testing.expectEqual(lhs.query_execution_status, rhs.query_execution_status);
                    if (lhs.query_execution_status == .ok) {
                        try std.testing.expectEqual(
                            mapValueFromI32I64Map(&lhs.accounts, 11),
                            mapValueFromI32I64Map(&rhs.accounts, 11),
                        );
                        try std.testing.expectEqual(
                            mapValueFromI32I64Map(&lhs.accounts, 15),
                            mapValueFromI32I64Map(&rhs.accounts, 15),
                        );
                        try std.testing.expectEqual(lhs.orders.items.len, rhs.orders.items.len);
                    }
                },
                else => return error.UnexpectedReportVariant,
            },
            else => return error.UnexpectedReportVariant,
        }
    }
}

const BinaryAccountSeed = struct {
    uid: i64,
    currency: i32,
    balance: i64,
};

fn encodeAddAccountsBinaryPayload(
    allocator: std.mem.Allocator,
    seeds: []const BinaryAccountSeed,
) binary_payload.EncodeError![]u8 {
    var users = try allocator.alloc(BinaryUserAccountsBatch, seeds.len);
    defer allocator.free(users);
    var accounts = try allocator.alloc(BinaryCurrencyBalance, seeds.len);
    defer allocator.free(accounts);

    for (seeds, 0..) |seed, idx| {
        accounts[idx] = .{
            .currency = seed.currency,
            .balance = seed.balance,
        };
        users[idx] = .{
            .uid = seed.uid,
            .accounts = accounts[idx .. idx + 1],
        };
    }
    return binary_payload.encodeAddAccountsToBytes(allocator, users);
}

fn encodeStateHashReportPayload(allocator: std.mem.Allocator) std.mem.Allocator.Error![]u8 {
    return binary_payload.encodeReportQueryToBytes(allocator, .state_hash);
}

fn publishRuntimeResult(sink: runtime_mod.ResultSink, event: runtime_mod.ResultEvent) void {
    if (sink.on_result) |callback| {
        callback(sink.context, event);
    }
}

test "exchange core wraps sync api flow and report query path" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();

    var add = try core.addUser(8801);
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    var adjust = try core.adjustUserBalance(8801, 1, 15, 5_000, .adjustment);
    defer adjust.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, adjust.result_code);

    var balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 8801,
            .currency = 15,
        },
    });
    defer balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 5_000), balance.user_balance);

    try std.testing.expectError(
        error.OrderBookDepthOverflow,
        core.requestOrderBook(241, std.math.maxInt(usize)),
    );
}

test "exchange core ApiCommand sync dispatch routes core and binary flows" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();

    const symbol_id: i32 = 932;
    const spec = try makeExchangeSpecForSymbol(symbol_id);
    var add_symbol = try core.submitApiCommand(.{
        .binary_add_symbols = .{
            .transfer_id = 701,
            .timestamp = 11,
            .symbols = &.{spec},
        },
    });
    defer add_symbol.deinit(allocator);
    switch (add_symbol) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var add_user = try core.submitApiCommand(.{ .add_user = 8_821 });
    defer add_user.deinit(allocator);
    switch (add_user) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var adjust_balance = try core.submitApiCommand(.{
        .adjust_user_balance = .{
            .uid = 8_821,
            .transaction_id = 1,
            .currency = 15,
            .amount = 12_000,
            .adjustment_type = .adjustment,
        },
    });
    defer adjust_balance.deinit(allocator);
    switch (adjust_balance) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var order_book = try core.submitApiCommand(.{
        .request_order_book = .{
            .symbol = symbol_id,
            .depth = 16,
        },
    });
    defer order_book.deinit(allocator);
    switch (order_book) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var report = try core.submitApiCommand(.{
        .binary_report_query = .{
            .transfer_id = 702,
            .timestamp = 12,
            .query = .state_hash,
        },
    });
    defer report.deinit(allocator);
    switch (report) {
        .response => |response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            const report_response = response.report_response orelse return error.MissingReportResponse;
            switch (report_response) {
                .state_hash_detailed => |detailed| {
                    try std.testing.expect(detailed.submodules.items.len > 0);
                },
                else => return error.UnexpectedReportResponseType,
            }
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var persist = try core.submitApiCommand(.{
        .persist_state = .{
            .snapshot_id = 88001,
            .timestamp_ns = 777_888_999,
        },
    });
    defer persist.deinit(allocator);
    switch (persist) {
        .persist_state => |result_code| try std.testing.expectEqual(cmd.CommandResultCode.success, result_code),
        .response => return error.UnexpectedResponseResult,
    }

    var grouping = try core.submitApiCommand(.{
        .grouping_control = .{
            .timestamp_ns = 14,
            .mode = 1,
        },
    });
    defer grouping.deinit(allocator);
    switch (grouping) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    try std.testing.expectError(
        error.OrderBookDepthOverflow,
        core.submitApiCommand(.{
            .request_order_book = .{
                .symbol = symbol_id,
                .depth = std.math.maxInt(usize),
            },
        }),
    );
}

test "exchange core ApiCommand raw command sync path preserves metadata and journal input" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer core.deinit();

    var result = try core.submitApiCommand(.{
        .raw_command = .{
            .command = .add_user,
            .uid = 8_915,
            .timestamp = 444_555_666,
            .events_group = 77,
            .service_flags = 5,
            .user_cookie = 19,
            .result_code = .new,
        },
    });
    defer result.deinit(allocator);

    switch (result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    const journal = core.journal orelse return error.ExpectedJournal;
    try std.testing.expectEqual(@as(usize, 1), journal.entries.items.len);
    const entry = journal.entries.items[0];
    try std.testing.expectEqual(cmd.OrderCommandType.add_user, entry.input.command);
    try std.testing.expectEqual(@as(i64, 8_915), entry.input.uid);
    try std.testing.expectEqual(@as(i64, 444_555_666), entry.input.timestamp);
    try std.testing.expectEqual(@as(i64, 77), entry.input.events_group);
    try std.testing.expectEqual(@as(i32, 5), entry.input.service_flags);
    try std.testing.expectEqual(@as(i32, 19), entry.input.user_cookie);
}

test "exchange core ApiCommand grouping-control sync path keeps mode and timestamp metadata" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer core.deinit();

    var result = try core.submitApiCommand(.{
        .grouping_control = .{
            .timestamp_ns = 222_333_444,
            .mode = 0,
        },
    });
    defer result.deinit(allocator);

    switch (result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    const journal = core.journal orelse return error.ExpectedJournal;
    try std.testing.expectEqual(@as(usize, 1), journal.entries.items.len);
    const entry = journal.entries.items[0];
    try std.testing.expectEqual(cmd.OrderCommandType.grouping_control, entry.input.command);
    try std.testing.expectEqual(@as(i64, 0), entry.input.order_id);
    try std.testing.expectEqual(@as(i64, 222_333_444), entry.input.timestamp);
}

test "exchange core ApiCommand async dispatch routes requests and resolves results" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const add_req = try core.submitApiCommandAsync(.{ .add_user = 8_911 });
    const adjust_req = try core.submitApiCommandAsync(.{
        .adjust_user_balance = .{
            .uid = 8_911,
            .transaction_id = 11,
            .currency = 15,
            .amount = 19_000,
            .adjustment_type = .adjustment,
        },
    });
    const query_req = try core.submitApiCommandAsync(.{
        .binary_report_query = .{
            .transfer_id = 703,
            .timestamp = 13,
            .query = .state_hash,
        },
    });
    const grouping_req = try core.submitApiCommandAsync(.{
        .grouping_control = .{
            .timestamp_ns = 14,
            .mode = 0,
        },
    });

    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 4), processed);

    var add_result = core.takeApiCommandAsyncResult(add_req) orelse return error.MissingAsyncResult;
    defer add_result.deinit(allocator);
    switch (add_result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var adjust_result = core.takeApiCommandAsyncResult(adjust_req) orelse return error.MissingAsyncResult;
    defer adjust_result.deinit(allocator);
    switch (adjust_result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var query_result = core.takeApiCommandAsyncResult(query_req) orelse return error.MissingAsyncResult;
    defer query_result.deinit(allocator);
    switch (query_result) {
        .response => |response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            const report_response = response.report_response orelse return error.MissingReportResponse;
            switch (report_response) {
                .state_hash_detailed => |detailed| {
                    try std.testing.expect(detailed.submodules.items.len > 0);
                },
                else => return error.UnexpectedReportResponseType,
            }
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var grouping_result = core.takeApiCommandAsyncResult(grouping_req) orelse return error.MissingAsyncResult;
    defer grouping_result.deinit(allocator);
    switch (grouping_result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }
}

test "exchange core ApiCommand async raw command path enqueues and resolves metadata-bearing command" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const handle = try core.submitApiCommandAsync(.{
        .raw_command = .{
            .command = .add_user,
            .uid = 8_916,
            .timestamp = 555_666_777,
            .events_group = 88,
            .service_flags = 7,
            .result_code = .new,
        },
    });

    try std.testing.expect(!core.isApiCommandAsyncResultReady(handle));
    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 1), processed);
    try std.testing.expect(core.isApiCommandAsyncResultReady(handle));

    var result = core.takeApiCommandAsyncResult(handle) orelse return error.MissingAsyncResult;
    defer result.deinit(allocator);
    switch (result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }
}

test "exchange core ApiCommand async dispatch supports persist_state handle readiness and take" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const handle = try core.submitApiCommandAsync(.{
        .persist_state = .{
            .snapshot_id = 99001,
            .timestamp_ns = 1_111_222_333,
        },
    });
    switch (handle) {
        .persist_state => {},
        .request_id => return error.UnexpectedRequestHandle,
    }

    try std.testing.expect(!core.isApiCommandAsyncResultReady(handle));
    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 2), processed);
    try std.testing.expect(core.isApiCommandAsyncResultReady(handle));

    var result = core.takeApiCommandAsyncResult(handle) orelse return error.MissingAsyncResult;
    defer result.deinit(allocator);
    switch (result) {
        .persist_state => |result_code| try std.testing.expectEqual(cmd.CommandResultCode.success, result_code),
        .response => return error.UnexpectedResponseResult,
    }

    try std.testing.expect(!core.isApiCommandAsyncResultReady(handle));
    try std.testing.expect(core.takeApiCommandAsyncResult(handle) == null);
}

test "exchange core ApiCommand batch helper runs mixed command stream and returns final response result" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const commands = [_]ApiCommand{
        .{ .add_user = 9_201 },
        .{
            .adjust_user_balance = .{
                .uid = 9_201,
                .transaction_id = 901,
                .currency = 15,
                .amount = 22_000,
                .adjustment_type = .adjustment,
            },
        },
        .{
            .binary_report_query = .{
                .transfer_id = 811,
                .timestamp = 14,
                .query = .state_hash,
            },
        },
    };

    var result = try core.submitApiCommandsSync(commands[0..]);
    defer result.deinit(allocator);

    switch (result) {
        .response => |response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            const report_response = response.report_response orelse return error.MissingReportResponse;
            switch (report_response) {
                .state_hash_detailed => |detailed| {
                    try std.testing.expect(detailed.submodules.items.len > 0);
                },
                else => return error.UnexpectedReportResponseType,
            }
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 9_201,
            .currency = 15,
        },
    });
    defer balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 22_000), balance.user_balance);
}

test "exchange core ApiCommand batch helper supports persist final result and empty batch rejection" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const commands = [_]ApiCommand{
        .{ .add_user = 9_301 },
        .{
            .persist_state = .{
                .snapshot_id = 77_701,
                .timestamp_ns = 4_444_555_666,
            },
        },
    };

    var result = try core.submitApiCommandsSync(commands[0..]);
    defer result.deinit(allocator);
    switch (result) {
        .persist_state => |result_code| try std.testing.expectEqual(cmd.CommandResultCode.success, result_code),
        .response => return error.UnexpectedResponseResult,
    }

    const empty: [0]ApiCommand = .{};
    try std.testing.expectError(error.EmptyApiCommandBatch, core.submitApiCommandsSync(empty[0..]));
}

test "exchange core ApiCommand flush helper accepts empty stream and returns nop response" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const empty: [0]ApiCommand = .{};
    var response = try core.submitApiCommandsSyncWithNop(empty[0..]);
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
}

test "exchange core ApiCommand flush helper processes mixed stream with persist and returns nop barrier" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const commands = [_]ApiCommand{
        .{ .add_user = 9_401 },
        .{
            .adjust_user_balance = .{
                .uid = 9_401,
                .transaction_id = 1_001,
                .currency = 15,
                .amount = 31_000,
                .adjustment_type = .adjustment,
            },
        },
        .{
            .persist_state = .{
                .snapshot_id = 78_801,
                .timestamp_ns = 5_555_666_777,
            },
        },
    };

    var response = try core.submitApiCommandsSyncWithNop(commands[0..]);
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    var balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 9_401,
            .currency = 15,
        },
    });
    defer balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 31_000), balance.user_balance);
}

test "exchange core suspend resume and persist sync wrappers are wired" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var add = try core.addUser(8851);
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    var suspend_resp = try core.suspendUser(8851);
    defer suspend_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, suspend_resp.result_code);

    var suspend_again = try core.suspendUser(8851);
    defer suspend_again.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_not_found, suspend_again.result_code);

    var resume_resp = try core.resumeUser(8851);
    defer resume_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, resume_resp.result_code);

    const persist_result = try core.persistState(8001, 111_222_333);
    try std.testing.expectEqual(cmd.CommandResultCode.success, persist_result);
}

test "exchange core order sync wrappers run command lifecycle with reset and nop" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var add = try core.addUser(8901);
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    var dep_quote = try core.adjustUserBalance(8901, 1, 15, 100_000, .adjustment);
    defer dep_quote.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, dep_quote.result_code);

    var placed = try core.placeOrder(98101, 8901, 241, 101, 101, 5, .bid, .gtc, 17);
    defer placed.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, placed.result_code);

    var moved = try core.moveOrder(98101, 8901, 241, 100);
    defer moved.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, moved.result_code);

    var reduced = try core.reduceOrder(98101, 8901, 241, 2);
    defer reduced.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, reduced.result_code);

    var canceled = try core.cancelOrder(98101, 8901, 241);
    defer canceled.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, canceled.result_code);

    var nop_resp = try core.nop();
    defer nop_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, nop_resp.result_code);

    var reset_resp = try core.reset();
    defer reset_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, reset_resp.result_code);

    var add_after_reset = try core.addUser(8901);
    defer add_after_reset.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_after_reset.result_code);
}

test "exchange core order async wrappers enqueue and execute deterministically" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    const req_add = try core.addUserAsync(8911);
    const req_dep = try core.adjustUserBalanceAsync(8911, 1, 15, 100_000, .adjustment);
    const req_place = try core.placeOrderAsync(99101, 8911, 241, 101, 101, 5, .bid, .gtc, 19);
    const req_move = try core.moveOrderAsync(99101, 8911, 241, 100);
    const req_reduce = try core.reduceOrderAsync(99101, 8911, 241, 2);
    const req_cancel = try core.cancelOrderAsync(99101, 8911, 241);
    const req_nop = try core.nopAsync();
    const req_reset = try core.resetAsync();

    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 8), processed);

    var add = core.takeAsyncResult(req_add) orelse unreachable;
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    var dep = core.takeAsyncResult(req_dep) orelse unreachable;
    defer dep.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, dep.result_code);

    var place = core.takeAsyncResult(req_place) orelse unreachable;
    defer place.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, place.result_code);

    var move = core.takeAsyncResult(req_move) orelse unreachable;
    defer move.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, move.result_code);

    var reduce = core.takeAsyncResult(req_reduce) orelse unreachable;
    defer reduce.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, reduce.result_code);

    var cancel = core.takeAsyncResult(req_cancel) orelse unreachable;
    defer cancel.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, cancel.result_code);

    var nop_resp = core.takeAsyncResult(req_nop) orelse unreachable;
    defer nop_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, nop_resp.result_code);

    var reset_resp = core.takeAsyncResult(req_reset) orelse unreachable;
    defer reset_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, reset_resp.result_code);
}

test "exchange core binary command payload wrapper applies multi-frame add-accounts batch" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const seeds = [_]BinaryAccountSeed{
        .{ .uid = 8971, .currency = 15, .balance = 7_000 },
        .{ .uid = 8972, .currency = 11, .balance = 9_000 },
    };
    const payload = try encodeAddAccountsBinaryPayload(allocator, seeds[0..]);
    defer allocator.free(payload);
    try std.testing.expect(payload.len > binary_frames.FrameWordsPerCommand * @sizeOf(i64));

    var response = try core.submitBinaryPayload(
        .binary_data_command,
        501,
        1_234,
        payload,
    );
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    var user_1_balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 8971,
            .currency = 15,
        },
    });
    defer user_1_balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 7_000), user_1_balance.user_balance);

    var user_2_balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 8972,
            .currency = 11,
        },
    });
    defer user_2_balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 9_000), user_2_balance.user_balance);
}

test "exchange core binary query payload wrapper returns state-hash detailed report" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const payload = try encodeStateHashReportPayload(allocator);
    defer allocator.free(payload);

    var response = try core.submitBinaryPayload(
        .binary_data_query,
        601,
        2_468,
        payload,
    );
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    const report = response.report_response orelse return error.MissingReportResponse;
    switch (report) {
        .state_hash_detailed => |detailed| {
            try std.testing.expect(detailed.submodules.items.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }
}

test "exchange core typed binary add-symbols wrapper adds symbol and serves order-book report" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const symbol_id: i32 = 777;
    const spec = try makeExchangeSpecForSymbol(symbol_id);

    var response = try core.submitAddSymbolsBinary(
        610,
        2_469,
        &.{spec},
    );
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    var order_book = try core.requestOrderBook(symbol_id, 16);
    defer order_book.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, order_book.result_code);
}

test "exchange core typed binary add-accounts wrapper applies balances" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const accounts_1 = [_]BinaryCurrencyBalance{
        .{ .currency = 15, .balance = 13_000 },
    };
    const accounts_2 = [_]BinaryCurrencyBalance{
        .{ .currency = 11, .balance = 14_000 },
    };
    const users = [_]BinaryUserAccountsBatch{
        .{ .uid = 9171, .accounts = accounts_1[0..] },
        .{ .uid = 9172, .accounts = accounts_2[0..] },
    };

    var response = try core.submitAddAccountsBinary(
        611,
        2_470,
        users[0..],
    );
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    var user_1_balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 9171,
            .currency = 15,
        },
    });
    defer user_1_balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 13_000), user_1_balance.user_balance);

    var user_2_balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 9172,
            .currency = 11,
        },
    });
    defer user_2_balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 14_000), user_2_balance.user_balance);
}

test "exchange core typed binary async wrappers enqueue and resolve requests" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const accounts = [_]BinaryCurrencyBalance{
        .{ .currency = 15, .balance = 15_000 },
    };
    const users = [_]BinaryUserAccountsBatch{
        .{ .uid = 9271, .accounts = accounts[0..] },
    };

    const add_req = try core.submitAddAccountsBinaryAsync(
        612,
        2_471,
        users[0..],
    );
    const query_req = try core.submitBinaryReportQueryAsync(
        613,
        2_472,
        .state_hash,
    );

    const processed = try core.drainAsyncPending();
    try std.testing.expect(processed >= 2);

    var add_response = core.takeAsyncResult(add_req) orelse return error.MissingAsyncResult;
    defer add_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_response.result_code);

    var query_response = core.takeAsyncResult(query_req) orelse return error.MissingAsyncResult;
    defer query_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, query_response.result_code);
    const report = query_response.report_response orelse return error.MissingReportResponse;
    switch (report) {
        .state_hash_detailed => |detailed| {
            try std.testing.expect(detailed.submodules.items.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }
}

test "exchange core deterministic simulation keeps sync and async typed binary account ingestion convergent" {
    const allocator = std.testing.allocator;
    var sync_core = try ExchangeCore.init(allocator, .{});
    defer sync_core.deinit();
    var async_core = try ExchangeCore.init(allocator, .{});
    defer async_core.deinit();

    const seed: u64 = 0xA11C0DECCAFE5511;
    const rounds: usize = 256;
    const uid_range: usize = 64;
    const base_uid: i64 = 20_000;

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var request_ids: std.ArrayListUnmanaged(u64) = .empty;
    defer request_ids.deinit(allocator);
    try request_ids.ensureTotalCapacityPrecise(allocator, rounds);

    for (0..rounds) |idx| {
        const uid = base_uid + @as(i64, @intCast(random.uintLessThan(usize, uid_range)));
        const currency: i32 = if (random.boolean()) 11 else 15;
        const balance = @as(i64, @intCast(random.uintLessThan(u32, 8_000))) +
            1 +
            @as(i64, @intCast(idx % 7));

        const accounts = [_]BinaryCurrencyBalance{
            .{
                .currency = currency,
                .balance = balance,
            },
        };
        const users = [_]BinaryUserAccountsBatch{
            .{
                .uid = uid,
                .accounts = accounts[0..],
            },
        };

        var sync_response = try sync_core.submitAddAccountsBinary(
            @as(i32, @intCast(3_000 + idx)),
            5_000 + @as(i64, @intCast(idx)),
            users[0..],
        );
        try std.testing.expectEqual(cmd.CommandResultCode.success, sync_response.result_code);
        sync_response.deinit(allocator);

        const request_id = try async_core.submitAddAccountsBinaryAsync(
            @as(i32, @intCast(4_000 + idx)),
            6_000 + @as(i64, @intCast(idx)),
            users[0..],
        );
        request_ids.appendAssumeCapacity(request_id);
    }

    const processed = try async_core.drainAsyncPending();
    try std.testing.expectEqual(rounds, processed);

    for (request_ids.items) |request_id| {
        var async_response = async_core.takeAsyncResult(request_id) orelse return error.MissingAsyncResult;
        try std.testing.expectEqual(cmd.CommandResultCode.success, async_response.result_code);
        async_response.deinit(allocator);
    }

    const sync_risk_hash = try queryRiskStateHash(&sync_core, allocator);
    const async_risk_hash = try queryRiskStateHash(&async_core, allocator);
    try std.testing.expectEqual(sync_risk_hash, async_risk_hash);

    const sync_detailed_hash = try queryDetailedStateHash(&sync_core, allocator);
    const async_detailed_hash = try queryDetailedStateHash(&async_core, allocator);
    try std.testing.expectEqual(sync_detailed_hash, async_detailed_hash);

    for (0..uid_range) |offset| {
        const uid = base_uid + @as(i64, @intCast(offset));
        for ([_]i32{ 11, 15 }) |currency| {
            var sync_balance = try sync_core.queryReport(.{
                .user_balance = .{
                    .uid = uid,
                    .currency = currency,
                },
            });
            const sync_value = sync_balance.user_balance;
            sync_balance.deinit(allocator);

            var async_balance = try async_core.queryReport(.{
                .user_balance = .{
                    .uid = uid,
                    .currency = currency,
                },
            });
            const async_value = async_balance.user_balance;
            async_balance.deinit(allocator);

            try std.testing.expectEqual(sync_value, async_value);
        }
    }
}

test "exchange core binary payload wrapper rejects unsupported command type" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const payload = try encodeStateHashReportPayload(allocator);
    defer allocator.free(payload);

    try std.testing.expectError(
        error.InvalidBinaryCommandType,
        core.submitBinaryPayload(.add_user, 701, 99, payload),
    );
}

test "exchange core binary payload wrapper rejects empty payload" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    try std.testing.expectError(
        error.EmptyBinaryPayload,
        core.submitBinaryPayload(.binary_data_query, 702, 100, &.{}),
    );
}

test "exchange core binary frame layout helper validates payload boundaries" {
    const one_byte = try computeBinaryFrameLayout(1);
    try std.testing.expectEqual(@as(usize, 1), one_byte.words_without_padding);
    try std.testing.expectEqual(@as(usize, 1), one_byte.frame_count);

    const bytes_per_frame = binary_frames.FrameWordsPerCommand * @sizeOf(i64);
    const across_frame = try computeBinaryFrameLayout(bytes_per_frame + 1);
    try std.testing.expectEqual(binary_frames.FrameWordsPerCommand + 1, across_frame.words_without_padding);
    try std.testing.expectEqual(@as(usize, 2), across_frame.frame_count);

    try std.testing.expectError(
        error.EmptyBinaryPayload,
        computeBinaryFrameLayout(0),
    );

    const huge = try computeBinaryFrameLayout(std.math.maxInt(usize));
    try std.testing.expect(huge.words_without_padding > 0);
    try std.testing.expect(huge.frame_count > 0);
}

test "exchange core binary command payload async wrapper queues and executes framed add-accounts batch" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const seeds = [_]BinaryAccountSeed{
        .{ .uid = 9071, .currency = 15, .balance = 11_000 },
        .{ .uid = 9072, .currency = 11, .balance = 12_000 },
    };
    const payload = try encodeAddAccountsBinaryPayload(allocator, seeds[0..]);
    defer allocator.free(payload);
    try std.testing.expect(payload.len > binary_frames.FrameWordsPerCommand * @sizeOf(i64));

    const words_without_padding = std.math.divCeil(usize, payload.len, @sizeOf(i64)) catch unreachable;
    const expected_frames = std.math.divCeil(usize, words_without_padding, binary_frames.FrameWordsPerCommand) catch unreachable;

    const request_id = try core.submitBinaryPayloadAsync(
        .binary_data_command,
        801,
        3_579,
        payload,
    );
    try std.testing.expectEqual(expected_frames, core.asyncPendingCount());
    try std.testing.expectEqual(@as(u64, @intCast(expected_frames)), request_id);

    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(expected_frames, processed);

    var response = core.takeAsyncResult(request_id) orelse return error.MissingAsyncResult;
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    var user_1_balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 9071,
            .currency = 15,
        },
    });
    defer user_1_balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 11_000), user_1_balance.user_balance);

    var user_2_balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 9072,
            .currency = 11,
        },
    });
    defer user_2_balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 12_000), user_2_balance.user_balance);
}

test "exchange core binary query payload async wrapper returns state-hash detailed report" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const payload = try encodeStateHashReportPayload(allocator);
    defer allocator.free(payload);

    const request_id = try core.submitBinaryPayloadAsync(
        .binary_data_query,
        901,
        4_680,
        payload,
    );

    const processed = try core.drainAsyncPending();
    try std.testing.expect(processed > 0);

    var response = core.takeAsyncResult(request_id) orelse return error.MissingAsyncResult;
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    const report = response.report_response orelse return error.MissingReportResponse;
    switch (report) {
        .state_hash_detailed => |detailed| {
            try std.testing.expect(detailed.submodules.items.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }
}

test "exchange core binary payload async wrapper rejects unsupported command type" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const payload = try encodeStateHashReportPayload(allocator);
    defer allocator.free(payload);

    try std.testing.expectError(
        error.InvalidBinaryCommandType,
        core.submitBinaryPayloadAsync(.add_user, 902, 55, payload),
    );
}

test "exchange core binary payload async wrapper rejects empty payload" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    try std.testing.expectError(
        error.EmptyBinaryPayload,
        core.submitBinaryPayloadAsync(.binary_data_query, 903, 66, &.{}),
    );
}

test "exchange core runtime drain groups queued commands deterministically" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .runtime = .{
            .max_batch_size = 2,
        },
    });
    defer core.deinit();

    try core.enqueueRuntime(makeAddUserCommand(8901));
    try core.enqueueRuntime(makeAddUserCommand(8902));
    try core.enqueueRuntime(makeAddUserCommand(8903));
    try core.enqueueRuntime(makeAddUserCommand(8903));
    try std.testing.expectEqual(@as(usize, 4), core.runtimePendingCount());

    const outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 2), outcome.groups_processed);
    try std.testing.expectEqual(@as(usize, 4), outcome.commands_processed);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, outcome.last_merged_result);
    try std.testing.expectEqual(@as(usize, 0), core.runtimePendingCount());
}

test "exchange core runtime drain honors grouping control mode transitions" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .runtime = .{
            .max_batch_size = 2,
        },
    });
    defer core.deinit();

    try core.enqueueRuntime(makeAddUserCommand(8911));
    try core.enqueueRuntime(makeAddUserCommand(8912));
    try core.enqueueRuntime(makeGroupingControlCommand(false));
    try core.enqueueRuntime(makeAddUserCommand(8913));
    try core.enqueueRuntime(makeAddUserCommand(8914));
    try core.enqueueRuntime(makeGroupingControlCommand(true));
    try core.enqueueRuntime(makeAddUserCommand(8915));
    try core.enqueueRuntime(makeAddUserCommand(8916));

    const outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 6), outcome.groups_processed);
    try std.testing.expectEqual(@as(usize, 8), outcome.commands_processed);
    try std.testing.expectEqual(cmd.CommandResultCode.success, outcome.last_merged_result);
    try std.testing.expect(core.runtime.grouping_enabled);
    try std.testing.expectEqual(@as(usize, 0), core.runtimePendingCount());
}

test "exchange core publishes result sink events for sync and batch submits" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        allocator: std.mem.Allocator,
        events: std.ArrayListUnmanaged(runtime_mod.ResultEvent) = .empty,

        fn callback(ctx: ?*anyopaque, event: runtime_mod.ResultEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.events.append(self.allocator, event) catch @panic("collector append failed");
        }

        fn deinit(self: *@This()) void {
            self.events.deinit(self.allocator);
            self.* = undefined;
        }
    };

    var collector: Collector = .{ .allocator = allocator };
    defer collector.deinit();

    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    core.setResultSink(.{
        .context = &collector,
        .on_result = Collector.callback,
    });

    var add = try core.addUser(9301);
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    var c1 = makeAddUserCommand(9302);
    var c2 = makeAddUserCommand(9302);
    var commands = [_]*cmd.OrderCommand{ &c1, &c2 };
    const batch = try core.submitCommandBatch(commands[0..]);
    try std.testing.expectEqual(@as(?u64, 2), batch.sequence_from);
    try std.testing.expectEqual(@as(?u64, 3), batch.sequence_to);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, batch.result_code);

    try std.testing.expectEqual(@as(usize, 3), collector.events.items.len);
    try std.testing.expectEqual(@as(u64, 1), collector.events.items[0].sequence);
    try std.testing.expectEqual(@as(u64, 2), collector.events.items[1].sequence);
    try std.testing.expectEqual(@as(u64, 3), collector.events.items[2].sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, collector.events.items[0].result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, collector.events.items[1].result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, collector.events.items[2].result_code);
}

test "exchange core publishes result sink events during async drain" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        allocator: std.mem.Allocator,
        events: std.ArrayListUnmanaged(runtime_mod.ResultEvent) = .empty,

        fn callback(ctx: ?*anyopaque, event: runtime_mod.ResultEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.events.append(self.allocator, event) catch @panic("collector append failed");
        }

        fn deinit(self: *@This()) void {
            self.events.deinit(self.allocator);
            self.* = undefined;
        }
    };

    var collector: Collector = .{ .allocator = allocator };
    defer collector.deinit();

    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    core.setResultSink(.{
        .context = &collector,
        .on_result = Collector.callback,
    });

    const req_ok = try core.addUserAsync(9401);
    const req_dup = try core.addUserAsync(9401);
    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 2), processed);

    var ok = core.takeAsyncResult(req_ok) orelse unreachable;
    defer ok.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, ok.result_code);

    var dup = core.takeAsyncResult(req_dup) orelse unreachable;
    defer dup.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, dup.result_code);

    try std.testing.expectEqual(@as(usize, 2), collector.events.items.len);
    try std.testing.expectEqual(@as(u64, 1), collector.events.items[0].sequence);
    try std.testing.expectEqual(@as(u64, 2), collector.events.items[1].sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, collector.events.items[0].result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, collector.events.items[1].result_code);
}

test "exchange core result sink follows grouping-control processing across sync/runtime paths" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        allocator: std.mem.Allocator,
        events: std.ArrayListUnmanaged(runtime_mod.ResultEvent) = .empty,

        fn callback(ctx: ?*anyopaque, event: runtime_mod.ResultEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.events.append(self.allocator, event) catch @panic("collector append failed");
        }

        fn deinit(self: *@This()) void {
            self.events.deinit(self.allocator);
            self.* = undefined;
        }
    };

    var collector: Collector = .{ .allocator = allocator };
    defer collector.deinit();

    var core = try ExchangeCore.init(allocator, .{
        .runtime = .{
            .max_batch_size = 4,
        },
    });
    defer core.deinit();
    core.setResultSink(.{
        .context = &collector,
        .on_result = Collector.callback,
    });

    var add_visible = try core.addUser(9451);
    defer add_visible.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_visible.result_code);

    var disable = try core.groupingControl(1_000, 0);
    defer disable.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, disable.result_code);

    var add_suppressed = try core.addUser(9452);
    defer add_suppressed.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_suppressed.result_code);

    try core.enqueueRuntime(makeAddUserCommand(9453));
    try core.enqueueRuntime(makeGroupingControlCommand(true));
    try core.enqueueRuntime(makeAddUserCommand(9454));
    const runtime_outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 3), runtime_outcome.commands_processed);

    var add_visible_again = try core.addUser(9455);
    defer add_visible_again.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_visible_again.result_code);

    try std.testing.expectEqual(@as(usize, 4), collector.events.items.len);
    try std.testing.expectEqual(@as(u64, 1), collector.events.items[0].sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.add_user, collector.events.items[0].command);
    try std.testing.expectEqual(@as(u64, 5), collector.events.items[1].sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.grouping_control, collector.events.items[1].command);
    try std.testing.expectEqual(@as(u64, 6), collector.events.items[2].sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.add_user, collector.events.items[2].command);
    try std.testing.expectEqual(@as(u64, 7), collector.events.items[3].sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.add_user, collector.events.items[3].command);

    try std.testing.expect(core.runtime.processing_enabled);
}

test "exchange core simple events handler emits command and orderbook callbacks across sync async and runtime paths" {
    const allocator = std.testing.allocator;

    const SeenCommand = enum {
        add_user,
        order_book_request,
        other,
    };

    const Collector = struct {
        allocator: std.mem.Allocator,
        commands: std.ArrayListUnmanaged(SeenCommand) = .empty,
        order_book_count: usize = 0,
        trade_count: usize = 0,

        fn classify(event: simple_events_mod.ApiCommandResult) SeenCommand {
            return switch (event.command) {
                .add_user => .add_user,
                .order_book_request => .order_book_request,
                else => .other,
            };
        }

        fn onCommandResult(ctx: ?*anyopaque, event: simple_events_mod.ApiCommandResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.commands.append(self.allocator, classify(event)) catch @panic("collector append failed");
        }

        fn onOrderBook(ctx: ?*anyopaque, _: simple_events_mod.OrderBook) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.order_book_count += 1;
        }

        fn onTrade(ctx: ?*anyopaque, _: simple_events_mod.TradeEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.trade_count += 1;
        }

        fn deinit(self: *@This()) void {
            self.commands.deinit(self.allocator);
            self.* = undefined;
        }
    };

    var collector: Collector = .{ .allocator = allocator };
    defer collector.deinit();

    var core = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();
    core.setSimpleEventsHandler(.{
        .context = &collector,
        .on_command_result = Collector.onCommandResult,
        .on_order_book = Collector.onOrderBook,
        .on_trade_event = Collector.onTrade,
    });

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var sync_add = try core.addUser(9601);
    defer sync_add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, sync_add.result_code);

    const async_req = try core.addUserAsync(9602);
    try std.testing.expectEqual(@as(usize, 1), try core.drainAsyncPending());
    var async_response = core.takeAsyncResult(async_req) orelse unreachable;
    defer async_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, async_response.result_code);

    try core.enqueueRuntime(makeAddUserCommand(9603));
    const runtime_outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 1), runtime_outcome.commands_processed);

    var order_book = try core.requestOrderBook(241, 8);
    defer order_book.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, order_book.result_code);

    try std.testing.expectEqual(@as(usize, 4), collector.commands.items.len);
    try std.testing.expectEqual(SeenCommand.add_user, collector.commands.items[0]);
    try std.testing.expectEqual(SeenCommand.add_user, collector.commands.items[1]);
    try std.testing.expectEqual(SeenCommand.add_user, collector.commands.items[2]);
    try std.testing.expectEqual(SeenCommand.order_book_request, collector.commands.items[3]);
    try std.testing.expectEqual(@as(usize, 1), collector.order_book_count);
    // Kernel event chains are not yet exposed through CommandResponse on live paths.
    try std.testing.expectEqual(@as(usize, 0), collector.trade_count);
}

test "exchange core simple events handler honors grouping-control result suppression" {
    const allocator = std.testing.allocator;

    const SeenCommand = enum {
        add_user,
        other,
    };

    const Collector = struct {
        allocator: std.mem.Allocator,
        commands: std.ArrayListUnmanaged(SeenCommand) = .empty,

        fn classify(event: simple_events_mod.ApiCommandResult) SeenCommand {
            return switch (event.command) {
                .add_user => .add_user,
                else => .other,
            };
        }

        fn onCommandResult(ctx: ?*anyopaque, event: simple_events_mod.ApiCommandResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.commands.append(self.allocator, classify(event)) catch @panic("collector append failed");
        }

        fn deinit(self: *@This()) void {
            self.commands.deinit(self.allocator);
            self.* = undefined;
        }
    };

    var collector: Collector = .{ .allocator = allocator };
    defer collector.deinit();

    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    core.setSimpleEventsHandler(.{
        .context = &collector,
        .on_command_result = Collector.onCommandResult,
    });

    var add_before_disable = try core.addUser(9701);
    defer add_before_disable.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_before_disable.result_code);

    var disable = try core.groupingControl(1_000, 0);
    defer disable.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, disable.result_code);

    var add_while_disabled = try core.addUser(9702);
    defer add_while_disabled.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_while_disabled.result_code);

    var enable = try core.groupingControl(1_001, 1);
    defer enable.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, enable.result_code);

    var add_after_enable = try core.addUser(9703);
    defer add_after_enable.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_after_enable.result_code);

    try std.testing.expectEqual(@as(usize, 2), collector.commands.items.len);
    try std.testing.expectEqual(SeenCommand.add_user, collector.commands.items[0]);
    try std.testing.expectEqual(SeenCommand.add_user, collector.commands.items[1]);
    try std.testing.expect(core.runtime.processing_enabled);
}

test "exchange core can clear simple events handler without affecting command execution" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        callback_count: usize = 0,

        fn onCommandResult(ctx: ?*anyopaque, _: simple_events_mod.ApiCommandResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.callback_count += 1;
        }
    };

    var collector: Collector = .{};

    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    core.setSimpleEventsHandler(.{
        .context = &collector,
        .on_command_result = Collector.onCommandResult,
    });

    var first = try core.addUser(9801);
    defer first.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, first.result_code);
    try std.testing.expectEqual(@as(usize, 1), collector.callback_count);

    core.clearSimpleEventsHandler();

    var second = try core.addUser(9802);
    defer second.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, second.result_code);
    try std.testing.expectEqual(@as(usize, 1), collector.callback_count);
}

test "exchange core simple events handler emits trade callbacks from live matcher events" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        trade_count: usize = 0,
        reject_count: usize = 0,
        last_total_volume: i64 = 0,
        last_trades_len: usize = 0,
        last_taker_action: common.OrderAction = .ask,

        fn onTrade(ctx: ?*anyopaque, event: simple_events_mod.TradeEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.trade_count += 1;
            self.last_total_volume = event.total_volume;
            self.last_trades_len = event.trades.len;
            self.last_taker_action = event.taker_action;
        }

        fn onReject(ctx: ?*anyopaque, _: simple_events_mod.RejectEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.reject_count += 1;
        }
    };

    var collector: Collector = .{};

    var core = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();
    core.setSimpleEventsHandler(.{
        .context = &collector,
        .on_trade_event = Collector.onTrade,
        .on_reject_event = Collector.onReject,
    });

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var add_maker = try core.addUser(9901);
    defer add_maker.deinit(allocator);
    var add_taker = try core.addUser(9902);
    defer add_taker.deinit(allocator);

    var fund_maker = try core.adjustUserBalance(9901, 1, 11, 10_000, .adjustment);
    defer fund_maker.deinit(allocator);
    var fund_taker = try core.adjustUserBalance(9902, 2, 15, 100_000, .adjustment);
    defer fund_taker.deinit(allocator);

    var maker_order = try core.placeOrder(
        70_001,
        9901,
        241,
        100,
        100,
        1,
        .ask,
        .gtc,
        0,
    );
    defer maker_order.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, maker_order.result_code);

    var taker_order = try core.placeOrder(
        70_002,
        9902,
        241,
        100,
        100,
        1,
        .bid,
        .gtc,
        0,
    );
    defer taker_order.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, taker_order.result_code);

    try std.testing.expectEqual(@as(usize, 1), collector.trade_count);
    try std.testing.expectEqual(@as(i64, 1), collector.last_total_volume);
    try std.testing.expectEqual(@as(usize, 1), collector.last_trades_len);
    try std.testing.expectEqual(common.OrderAction.bid, collector.last_taker_action);
    try std.testing.expectEqual(@as(usize, 0), collector.reject_count);
}

test "exchange core runtime non-journaling simple events handler emits trade callbacks" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        trade_count: usize = 0,
        last_total_volume: i64 = 0,
        last_trades_len: usize = 0,
        last_taker_action: common.OrderAction = .ask,

        fn onTrade(ctx: ?*anyopaque, event: simple_events_mod.TradeEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.trade_count += 1;
            self.last_total_volume = event.total_volume;
            self.last_trades_len = event.trades.len;
            self.last_taker_action = event.taker_action;
        }
    };

    var collector: Collector = .{};

    var core = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
        .runtime = .{
            .max_batch_size = 4,
        },
    });
    defer core.deinit();
    core.setSimpleEventsHandler(.{
        .context = &collector,
        .on_trade_event = Collector.onTrade,
    });

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var add_maker = try core.addUser(9911);
    defer add_maker.deinit(allocator);
    var add_taker = try core.addUser(9912);
    defer add_taker.deinit(allocator);

    var fund_maker = try core.adjustUserBalance(9911, 11, 11, 10_000, .adjustment);
    defer fund_maker.deinit(allocator);
    var fund_taker = try core.adjustUserBalance(9912, 12, 15, 100_000, .adjustment);
    defer fund_taker.deinit(allocator);

    try core.enqueueRuntime(makePlaceOrderCommand(71_001, 9911, 241, 100, 100, 1, .ask, .gtc, 0));
    try core.enqueueRuntime(makePlaceOrderCommand(71_002, 9912, 241, 100, 100, 1, .bid, .gtc, 0));

    const outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 2), outcome.commands_processed);
    try std.testing.expectEqual(@as(usize, 1), collector.trade_count);
    try std.testing.expectEqual(@as(i64, 1), collector.last_total_volume);
    try std.testing.expectEqual(@as(usize, 1), collector.last_trades_len);
    try std.testing.expectEqual(common.OrderAction.bid, collector.last_taker_action);
}

test "exchange core journaling runtime drain does not duplicate result sink events" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        allocator: std.mem.Allocator,
        events: std.ArrayListUnmanaged(runtime_mod.ResultEvent) = .empty,

        fn callback(ctx: ?*anyopaque, event: runtime_mod.ResultEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.events.append(self.allocator, event) catch @panic("collector append failed");
        }

        fn deinit(self: *@This()) void {
            self.events.deinit(self.allocator);
            self.* = undefined;
        }
    };

    var collector: Collector = .{ .allocator = allocator };
    defer collector.deinit();

    var core = try ExchangeCore.init(allocator, .{
        .runtime = .{
            .max_batch_size = 2,
        },
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer core.deinit();
    core.setResultSink(.{
        .context = &collector,
        .on_result = Collector.callback,
    });

    try core.enqueueRuntime(makeAddUserCommand(9501));
    try core.enqueueRuntime(makeAddUserCommand(9502));
    try core.enqueueRuntime(makeAddUserCommand(9502));

    const outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 3), outcome.commands_processed);
    try std.testing.expectEqual(@as(usize, 3), collector.events.items.len);
    try std.testing.expectEqual(@as(u64, 1), collector.events.items[0].sequence);
    try std.testing.expectEqual(@as(u64, 2), collector.events.items[1].sequence);
    try std.testing.expectEqual(@as(u64, 3), collector.events.items[2].sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, collector.events.items[0].result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, collector.events.items[1].result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, collector.events.items[2].result_code);
}

test "exchange core async wrappers keep request/result routing semantics" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const req_ok = try core.addUserAsync(9001);
    const req_dup = try core.addUserAsync(9001);
    try std.testing.expectEqual(@as(usize, 2), core.asyncPendingCount());
    try std.testing.expect(!core.isAsyncResultReady(req_ok));
    try std.testing.expect(!core.isAsyncResultReady(req_dup));

    try std.testing.expectError(
        error.OrderBookDepthOverflow,
        core.requestOrderBookAsync(241, std.math.maxInt(usize)),
    );

    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 2), processed);
    try std.testing.expectEqual(@as(usize, 0), core.asyncPendingCount());

    var ok = core.takeAsyncResult(req_ok) orelse unreachable;
    defer ok.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, ok.result_code);

    var dup = core.takeAsyncResult(req_dup) orelse unreachable;
    defer dup.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, dup.result_code);
    try std.testing.expectEqual(@as(?kernel_mod.CommandResponse, null), core.takeAsyncResult(req_dup));
}

test "exchange core flush result validator accepts response and rejects persist variant" {
    var response_result: ApiCommandResult = .{
        .response = .{
            .result_code = .success,
        },
    };
    try ExchangeCore.expectFlushResultVariant(&response_result);

    var persist_result: ApiCommandResult = .{
        .persist_state = .success,
    };
    try std.testing.expectError(
        error.UnexpectedFlushResultVariant,
        ExchangeCore.expectFlushResultVariant(&persist_result),
    );
}

test "exchange core persist async wrappers expose merged paired result" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const req_add = try core.addUserAsync(9051);
    const req_suspend = try core.suspendUserAsync(9051);
    const req_resume = try core.resumeUserAsync(9051);
    const persist_requests = try core.persistStateAsync(8002, 222_333_444);
    try std.testing.expectEqual(@as(usize, 5), core.asyncPendingCount());
    try std.testing.expect(!core.isPersistStateResultReady(persist_requests));

    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 5), processed);
    try std.testing.expect(core.isPersistStateResultReady(persist_requests));
    try std.testing.expectEqual(@as(usize, 2), core.persistenceSnapshotCount());
    try std.testing.expectEqual(@as(u64, 8002), core.persistenceCurrentSnapshot().snapshot_id);
    try std.testing.expectEqual(@as(u64, 5), core.persistenceCurrentSnapshot().seq);

    var add = core.takeAsyncResult(req_add) orelse unreachable;
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    var suspend_async_resp = core.takeAsyncResult(req_suspend) orelse unreachable;
    defer suspend_async_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, suspend_async_resp.result_code);

    var resume_async_resp = core.takeAsyncResult(req_resume) orelse unreachable;
    defer resume_async_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, resume_async_resp.result_code);

    const merged = core.takePersistStateResult(persist_requests) orelse unreachable;
    try std.testing.expectEqual(cmd.CommandResultCode.success, merged);
    try std.testing.expect(core.takePersistStateResult(persist_requests) == null);
}

test "exchange core persist wrappers record both commands when journaling is enabled" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer core.deinit();

    try std.testing.expect(core.journalingEnabled());
    try std.testing.expectEqual(@as(usize, 0), core.journalLen());
    try std.testing.expectEqual(@as(usize, 1), core.persistenceSnapshotCount());
    try std.testing.expectEqual(@as(usize, 0), core.persistenceJournalCount());

    const sync_result = try core.persistState(8003, 333_444_555);
    try std.testing.expectEqual(cmd.CommandResultCode.success, sync_result);
    try std.testing.expectEqual(@as(usize, 2), core.journalLen());
    try std.testing.expectEqual(@as(usize, 2), core.persistenceSnapshotCount());

    const persisted_snapshot = core.persistenceCurrentSnapshot();
    try std.testing.expectEqual(@as(u64, 8003), persisted_snapshot.snapshot_id);
    try std.testing.expectEqual(@as(u64, 2), persisted_snapshot.seq);

    const persist_requests = try core.persistStateAsync(8004, 444_555_666);
    try std.testing.expectEqual(@as(usize, 2), core.asyncPendingCount());
    _ = try core.drainAsyncPending();
    const async_result = core.takePersistStateResult(persist_requests) orelse unreachable;
    try std.testing.expectEqual(cmd.CommandResultCode.success, async_result);
    try std.testing.expectEqual(@as(usize, 4), core.journalLen());
    try std.testing.expectEqual(@as(usize, 3), core.persistenceSnapshotCount());
    try std.testing.expectEqual(@as(u64, 8004), core.persistenceCurrentSnapshot().snapshot_id);
    try std.testing.expectEqual(@as(u64, 4), core.persistenceCurrentSnapshot().seq);

    var post_persist = try core.addUser(9052);
    defer post_persist.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, post_persist.result_code);
    try std.testing.expectEqual(@as(usize, 5), core.journalLen());

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try core.writeJournalToFile(tmp.dir, "persist.journal");

    try std.testing.expectEqual(@as(usize, 1), core.persistenceJournalCount());
    try std.testing.expectEqual(@as(?JournalRef, null), core.persistenceFindJournalForSequence(4));
    const replay_ref = core.persistenceFindJournalForSequence(5) orelse return error.ExpectedJournal;
    const replay_desc = core.persistenceJournalByRef(replay_ref);
    try std.testing.expectEqual(@as(u64, 5), replay_desc.seq_first);
    try std.testing.expectEqual(@as(?u64, 5), replay_desc.seq_last);
    try std.testing.expectEqual(core.persistenceCurrentSnapshotRef(), replay_desc.base_snapshot);
}

test "exchange core persistence snapshot lookup resolves known ids" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const root_ref = core.persistenceFindSnapshotById(0) orelse return error.ExpectedSnapshot;
    try std.testing.expectEqual(core.persistenceCurrentSnapshotRef(), root_ref);
    const root_snapshot = core.persistenceSnapshotById(0) orelse return error.ExpectedSnapshot;
    try std.testing.expectEqual(@as(u64, 0), root_snapshot.snapshot_id);

    _ = try core.persistState(82_001, 1_000_111);
    _ = try core.persistState(82_002, 1_000_222);

    const first_ref = core.persistenceFindSnapshotById(82_001) orelse return error.ExpectedSnapshot;
    const second_ref = core.persistenceFindSnapshotById(82_002) orelse return error.ExpectedSnapshot;
    try std.testing.expect(first_ref != second_ref);

    const second_snapshot = core.persistenceSnapshotById(82_002) orelse return error.ExpectedSnapshot;
    try std.testing.expectEqual(@as(u64, 82_002), second_snapshot.snapshot_id);
    try std.testing.expectEqual(@as(?SnapshotRef, null), core.persistenceFindSnapshotById(999_999));
    try std.testing.expectEqual(@as(?*const SnapshotDescriptor, null), core.persistenceSnapshotById(999_999));
}

test "exchange core persist-state main-log wrapper appends entries using provided dir" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var storage_folder_buf: [256]u8 = undefined;
    const storage_folder = try std.fmt.bufPrint(
        &storage_folder_buf,
        ".zig-cache/tmp/{s}/persist-main-log",
        .{tmp.sub_path},
    );

    var core = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXPERSISTLOG"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = storage_folder,
        },
    });
    defer core.deinit();

    const first = try core.persistStateWithDefaultMainLog(
        std.fs.cwd(),
        81_001,
        1_111_222,
        .risk_engine,
        0,
    );
    try std.testing.expectEqual(cmd.CommandResultCode.success, first);

    const second = try core.persistStateWithDefaultMainLog(
        std.fs.cwd(),
        81_002,
        1_111_333,
        .matching_engine_router,
        1,
    );
    try std.testing.expectEqual(cmd.CommandResultCode.success, second);

    const main_log_path = try core.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    var main_log_file = try std.fs.cwd().openFile(main_log_path, .{});
    defer main_log_file.close();
    const main_log_contents = try main_log_file.readToEndAlloc(allocator, 8192);
    defer allocator.free(main_log_contents);

    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "snapshotId=81001") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "timestampNs=1111222") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "type=RE") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "instance=0") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "snapshotId=81002") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "timestampNs=1111333") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "type=ME") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "instance=1") != null);

    var line_count: usize = 0;
    for (main_log_contents) |ch| {
        if (ch == '\n') line_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), line_count);
}

test "exchange core config validation fails before initialization for bad runtime config" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidBatchSize,
        ExchangeCore.init(allocator, .{
            .runtime = .{
                .max_batch_size = 0,
            },
        }),
    );
}

test "exchange core accepts java default configuration profile" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, Config.javaDefault());
    defer core.deinit();

    var add = try core.addUser(9101);
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);
    const cfg = core.configRef();
    try std.testing.expect(cfg.logging.isEnabled(.warnings));
    try std.testing.expect(cfg.reports_queries.supportsReport(.state_hash));
    try std.testing.expectEqual(config_mod.SerializationBackend.dummy, cfg.serialization.backend);
}

test "exchange core persistence path helpers follow Java file naming layout" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXPATH"),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer core.deinit();

    const snapshot_name = try core.defaultPersistenceSnapshotFileName(
        allocator,
        42,
        .risk_engine,
        1,
    );
    defer allocator.free(snapshot_name);
    try std.testing.expectEqualStrings("EXPATH_snapshot_42_RE1.ecs", snapshot_name);

    const snapshot_matching_name = try core.defaultPersistenceSnapshotFileName(
        allocator,
        42,
        .matching_engine_router,
        0,
    );
    defer allocator.free(snapshot_matching_name);
    try std.testing.expectEqualStrings("EXPATH_snapshot_42_ME0.ecs", snapshot_matching_name);

    const journal_name = try core.defaultPersistenceJournalFileName(allocator, 42, 0x000F);
    defer allocator.free(journal_name);
    try std.testing.expectEqualStrings("EXPATH_journal_42_000F.ecj", journal_name);

    const main_log_name = try core.defaultPersistenceMainLogFileName(allocator);
    defer allocator.free(main_log_name);
    try std.testing.expectEqualStrings("EXPATH.eca", main_log_name);

    const snapshot_path = try core.defaultPersistenceSnapshotPath(
        allocator,
        42,
        .risk_engine,
        1,
    );
    defer allocator.free(snapshot_path);
    try std.testing.expect(std.mem.startsWith(u8, snapshot_path, config_mod.DefaultDiskStorageFolder));
    try std.testing.expect(std.mem.endsWith(u8, snapshot_path, snapshot_name));

    const journal_path = try core.defaultPersistenceJournalPath(allocator, 42, 0x000F);
    defer allocator.free(journal_path);
    try std.testing.expect(std.mem.startsWith(u8, journal_path, config_mod.DefaultDiskStorageFolder));
    try std.testing.expect(std.mem.endsWith(u8, journal_path, journal_name));

    const main_log_path = try core.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    try std.testing.expect(std.mem.startsWith(u8, main_log_path, config_mod.DefaultDiskStorageFolder));
    try std.testing.expect(std.mem.endsWith(u8, main_log_path, main_log_name));
}

test "exchange core initializes from java exchange config adapter" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initFromJavaConfig(allocator, .{
        .performance_cfg = config_mod.PerformanceConfig.latencyPreset(),
    });
    defer core.deinit();

    const cfg = core.configRef();
    try std.testing.expectEqual(kernel_mod.OrderBookKind.direct, cfg.matching.orderbook_kind);
    try std.testing.expectEqual(@as(usize, 256), cfg.runtime.max_batch_size);
    try std.testing.expectEqual(runtime_mod.DefaultL2PublishIntervalNs, cfg.runtime.l2_publish_interval_ns);
    try std.testing.expectEqual(@as(u64, 10_000), cfg.runtime.max_group_duration_ns);
}

test "exchange core initializes throughput profile via single-kernel adapter fallback" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initFromJavaConfig(allocator, .{
        .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
    });
    defer core.deinit();

    const cfg = core.configRef();
    try std.testing.expectEqual(kernel_mod.OrderBookKind.direct, cfg.matching.orderbook_kind);
    try std.testing.expectEqual(@as(u64, 0), cfg.matching.shard_id);
    try std.testing.expectEqual(@as(u64, 1), cfg.matching.num_shards);
    try std.testing.expectEqual(@as(usize, 4_096), cfg.runtime.max_batch_size);
    try std.testing.expectEqual(@as(u64, 4_000_000), cfg.runtime.max_group_duration_ns);
}

test "exchange core applies report query capability gating from config" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .reports_queries = .{
            .enable_state_hash_report = false,
            .enable_single_user_report = true,
            .enable_total_currency_balance_report = true,
        },
    });
    defer core.deinit();

    try std.testing.expectError(
        error.UnsupportedReportQueryType,
        core.queryReport(.risk_state_hash),
    );

    var single_user = try core.queryReport(.{ .single_user = 9999 });
    defer single_user.deinit(allocator);
    switch (single_user) {
        .single_user => |report| {
            try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.user_not_found, report.query_execution_status);
        },
        else => unreachable,
    }
}

test "exchange core maps binary command capability flags into kernel config" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .reports_queries = .{
            .enable_add_accounts_binary = false,
            .enable_add_symbols_binary = true,
        },
    });
    defer core.deinit();

    const kernel_cfg = core.kernelRef().report_queries_cfg;
    try std.testing.expect(!kernel_cfg.add_accounts_binary);
    try std.testing.expect(kernel_cfg.add_symbols_binary);
}

test "exchange core sync journaling records commands and supports file roundtrip" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer core.deinit();

    try std.testing.expect(core.journalingEnabled());
    try std.testing.expectEqual(@as(usize, 0), core.journalLen());

    var add_user = try core.addUser(9201);
    defer add_user.deinit(allocator);
    var deposit = try core.adjustUserBalance(9201, 1, 15, 7_000, .adjustment);
    defer deposit.deinit(allocator);

    var c1 = makeAddUserCommand(9202);
    var c2 = makeAddUserCommand(9202);
    var batch_items = [_]*cmd.OrderCommand{ &c1, &c2 };
    const batch = try core.submitCommandBatch(batch_items[0..]);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, batch.result_code);

    try std.testing.expectEqual(@as(usize, 4), core.journalLen());

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try core.writeJournalToFile(tmp.dir, "core.journal");

    var replay_journal = try journal_mod.CommandJournal.readFromFile(allocator, tmp.dir, "core.journal");
    defer replay_journal.deinit();
    try std.testing.expectEqual(@as(usize, 4), replay_journal.len());

    var replay_kernel = try kernel_mod.ExchangeKernel.init(allocator, core.configRef().toKernelConfig());
    defer replay_kernel.deinit();
    try replay_journal.replayIntoKernel(&replay_kernel);

    var replayed_balance = try replay_kernel.queryReport(.{
        .user_balance = .{
            .uid = 9201,
            .currency = 15,
        },
    });
    defer replayed_balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 7_000), replayed_balance.user_balance);

    var reloaded_core = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer reloaded_core.deinit();
    try std.testing.expectEqual(@as(usize, 0), reloaded_core.journalLen());
    try reloaded_core.loadJournalFromFile(tmp.dir, "core.journal");
    try std.testing.expectEqual(@as(usize, 4), reloaded_core.journalLen());
}

test "exchange core default-path journal wrappers support write/load/replay flows" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXJDEF"),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();

    var add_user = makeAddUserCommand(9911);
    add_user.timestamp = 10;
    var add_response = try source.submitCommand(&add_user);
    defer add_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_response.result_code);
    try std.testing.expectEqual(@as(usize, 1), source.journalLen());

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.writeJournalToDefaultPath(tmp.dir, 0, 1);
    var dumps_dir = try tmp.dir.openDir(config_mod.DefaultDiskStorageFolder, .{});
    dumps_dir.close();

    var loaded = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXJDEF"),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer loaded.deinit();
    try loaded.loadJournalFromDefaultPath(tmp.dir, 0, 1);
    try std.testing.expectEqual(@as(usize, 1), loaded.journalLen());

    var replay = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.lastKnownStateFromJournal("EXJDEF", 0, 0),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer replay.deinit();

    const replayed = try replay.replayJournalFromDefaultPath(tmp.dir, 0, 1);
    try std.testing.expectEqual(@as(usize, 1), replayed);

    var user_report = try replay.queryReport(.{ .single_user = 9911 });
    defer user_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user_report.single_user.query_execution_status,
    );
}

test "exchange core default-path snapshot wrappers capture and validate state" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXSDEF"),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer core.deinit();

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var add_user = try core.addUser(9921);
    defer add_user.deinit(allocator);
    var deposit = try core.adjustUserBalance(9921, 1, 15, 4_000, .adjustment);
    defer deposit.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try core.captureSnapshotToDefaultPath(
        tmp.dir,
        55,
        .risk_engine,
        0,
        &.{241},
        16,
    );
    var dumps_dir = try tmp.dir.openDir(config_mod.DefaultDiskStorageFolder, .{});
    dumps_dir.close();
    try core.validateSnapshotDefaultPath(tmp.dir, 55, .risk_engine, 0);

    const main_log_path = try core.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    var main_log_file = try tmp.dir.openFile(main_log_path, .{});
    defer main_log_file.close();
    const main_log_contents = try main_log_file.readToEndAlloc(allocator, 4096);
    defer allocator.free(main_log_contents);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "seq=") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "timestampNs=") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "snapshotId=55") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "type=RE") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "instance=0") != null);
}

test "exchange core default-path snapshot capture appends .eca main log entries" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXSLOG"),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer core.deinit();

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try core.captureSnapshotToDefaultPath(
        tmp.dir,
        61,
        .risk_engine,
        0,
        &.{241},
        16,
    );
    try core.captureSnapshotToDefaultPath(
        tmp.dir,
        62,
        .matching_engine_router,
        1,
        &.{241},
        16,
    );

    const main_log_path = try core.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    var main_log_file = try tmp.dir.openFile(main_log_path, .{});
    defer main_log_file.close();
    const main_log_contents = try main_log_file.readToEndAlloc(allocator, 8192);
    defer allocator.free(main_log_contents);

    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "snapshotId=61") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "type=RE") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "instance=0") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "snapshotId=62") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "type=ME") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "instance=1") != null);

    var line_count: usize = 0;
    for (main_log_contents) |ch| {
        if (ch == '\n') line_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), line_count);
}

test "exchange core snapshot exists helpers detect missing and existing files" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXSEXIST"),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer core.deinit();

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try std.testing.expect(!try core.snapshotExistsFile(tmp.dir, "state.snap"));
    try core.captureSnapshotToFile(tmp.dir, "state.snap", &.{241}, 16);
    try std.testing.expect(try core.snapshotExistsFile(tmp.dir, "state.snap"));

    try std.testing.expect(!try core.snapshotExistsDefaultPath(tmp.dir, 77, .risk_engine, 0));
    try core.captureSnapshotToDefaultPath(
        tmp.dir,
        77,
        .risk_engine,
        0,
        &.{241},
        16,
    );
    try std.testing.expect(try core.snapshotExistsDefaultPath(tmp.dir, 77, .risk_engine, 0));
}

test "exchange core journal api reports disabled when journaling config is off" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    try std.testing.expect(!core.journalingEnabled());
    try std.testing.expectEqual(@as(usize, 0), core.journalLen());

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try std.testing.expectError(
        error.JournalingDisabled,
        core.writeJournalToFile(tmp.dir, "core.journal"),
    );
    try std.testing.expectError(
        error.JournalingDisabled,
        core.loadJournalFromFile(tmp.dir, "core.journal"),
    );
    try std.testing.expectError(
        error.JournalingDisabled,
        core.replayJournalFromFile(tmp.dir, "core.journal"),
    );
}

test "exchange core async drain journaling mode records queued commands" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer core.deinit();

    const req_ok = try core.addUserAsync(9301);
    const req_dup = try core.addUserAsync(9301);
    try std.testing.expectEqual(@as(usize, 2), core.asyncPendingCount());
    try std.testing.expectEqual(@as(usize, 0), core.journalLen());

    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 2), processed);
    try std.testing.expectEqual(@as(usize, 0), core.asyncPendingCount());
    try std.testing.expectEqual(@as(usize, 2), core.journalLen());

    var ok = core.takeAsyncResult(req_ok) orelse unreachable;
    defer ok.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, ok.result_code);

    var dup = core.takeAsyncResult(req_dup) orelse unreachable;
    defer dup.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, dup.result_code);
}

test "exchange core runtime drain journaling mode records queued commands" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
        .runtime = .{
            .max_batch_size = 2,
        },
    });
    defer core.deinit();

    try core.enqueueRuntime(makeAddUserCommand(9401));
    try core.enqueueRuntime(makeAddUserCommand(9402));
    try core.enqueueRuntime(makeAddUserCommand(9402));
    try std.testing.expectEqual(@as(usize, 3), core.runtimePendingCount());
    try std.testing.expectEqual(@as(usize, 0), core.journalLen());

    const outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 2), outcome.groups_processed);
    try std.testing.expectEqual(@as(usize, 3), outcome.commands_processed);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, outcome.last_merged_result);
    try std.testing.expectEqual(@as(usize, 0), core.runtimePendingCount());
    try std.testing.expectEqual(@as(usize, 3), core.journalLen());

    var add_again = try core.addUser(9401);
    defer add_again.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, add_again.result_code);
}

test "exchange core runtime drain journaling mode keeps grouping control semantics" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .runtime = .{
            .max_batch_size = 2,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer core.deinit();

    try core.enqueueRuntime(makeGroupingControlCommand(false));
    try core.enqueueRuntime(makeAddUserCommand(8951));
    try core.enqueueRuntime(makeAddUserCommand(8952));
    try core.enqueueRuntime(makeGroupingControlCommand(true));
    try core.enqueueRuntime(makeAddUserCommand(8953));
    try core.enqueueRuntime(makeAddUserCommand(8954));

    const outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 5), outcome.groups_processed);
    try std.testing.expectEqual(@as(usize, 6), outcome.commands_processed);
    try std.testing.expectEqual(cmd.CommandResultCode.success, outcome.last_merged_result);
    try std.testing.expect(core.runtime.grouping_enabled);
    try std.testing.expectEqual(@as(usize, 6), core.journalLen());
}

test "exchange core runtime drain journaling stamps events_group metadata deterministically" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .runtime = .{
            .max_batch_size = 2,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer core.deinit();

    try core.enqueueRuntime(makeAddUserCommand(8961));
    try core.enqueueRuntime(makeAddUserCommand(8962));
    try core.enqueueRuntime(makeGroupingControlCommand(false));
    try core.enqueueRuntime(makeAddUserCommand(8963));
    try core.enqueueRuntime(makeAddUserCommand(8964));
    try core.enqueueRuntime(makeGroupingControlCommand(true));
    try core.enqueueRuntime(makeAddUserCommand(8965));
    try core.enqueueRuntime(makePersistStateMatchingCommand(70_001, 11));
    try core.enqueueRuntime(makePersistStateRiskCommand(70_001, 11));
    try core.enqueueRuntime(makeAddUserCommand(8966));

    const outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 8), outcome.groups_processed);
    try std.testing.expectEqual(@as(usize, 10), outcome.commands_processed);
    try std.testing.expectEqual(cmd.CommandResultCode.success, outcome.last_merged_result);
    try std.testing.expect(core.runtime.grouping_enabled);
    try std.testing.expectEqual(@as(usize, 10), core.journalLen());

    const journal = core.journal orelse return error.ExpectedJournal;
    const expected_groups = [_]i64{ 0, 0, 0, 0, 0, 2, 2, 4, 4, 4 };
    for (journal.entries.items, 0..) |entry, idx| {
        try std.testing.expectEqual(expected_groups[idx], entry.input.events_group);
        try std.testing.expectEqual(@as(i32, 0), entry.input.service_flags);
    }
}

test "exchange core replay journal honors initial-state timestamp limit" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();

    var c1 = makeAddUserCommand(9501);
    c1.timestamp = 10;
    var c2 = makeAddUserCommand(9502);
    c2.timestamp = 20;
    var c3 = makeAddUserCommand(9503);
    c3.timestamp = 40;
    var r1 = try source.submitCommand(&c1);
    defer r1.deinit(allocator);
    var r2 = try source.submitCommand(&c2);
    defer r2.deinit(allocator);
    var r3 = try source.submitCommand(&c3);
    defer r3.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 3), source.journalLen());

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.writeJournalToFile(tmp.dir, "bootstrap.journal");

    var replay = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXBOOT",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 25,
            .throw_if_snapshot_not_found = false,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer replay.deinit();

    const replayed = try replay.replayJournalFromFile(tmp.dir, "bootstrap.journal");
    try std.testing.expectEqual(@as(usize, 2), replayed);
    try std.testing.expectEqual(@as(usize, 2), replay.journalLen());

    var user1_report = try replay.queryReport(.{ .single_user = 9501 });
    defer user1_report.deinit(allocator);
    var user2_report = try replay.queryReport(.{ .single_user = 9502 });
    defer user2_report.deinit(allocator);
    var user3_report = try replay.queryReport(.{ .single_user = 9503 });
    defer user3_report.deinit(allocator);

    try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, user1_report.single_user.query_execution_status);
    try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, user2_report.single_user.query_execution_status);
    try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.user_not_found, user3_report.single_user.query_execution_status);
}

test "exchange core bootstrap policy replays journal on clean-start journal profile" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();

    var c1 = makeAddUserCommand(9601);
    c1.timestamp = 10;
    var c2 = makeAddUserCommand(9602);
    c2.timestamp = 20;
    var r1 = try source.submitCommand(&c1);
    defer r1.deinit(allocator);
    var r2 = try source.submitCommand(&c2);
    defer r2.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.writeJournalToFile(tmp.dir, "bootstrap.journal");

    var replay = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.lastKnownStateFromJournal("EXBOOT2", 0, 0),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer replay.deinit();

    const outcome = try replay.bootstrapFromPersistence(
        tmp.dir,
        null,
        "bootstrap.journal",
    );
    try std.testing.expect(!outcome.snapshot_requested);
    try std.testing.expect(!outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 2), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(usize, 2), replay.journalLen());

    var user1_report = try replay.queryReport(.{ .single_user = 9601 });
    defer user1_report.deinit(allocator);
    var user2_report = try replay.queryReport(.{ .single_user = 9602 });
    defer user2_report.deinit(allocator);
    try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, user1_report.single_user.query_execution_status);
    try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, user2_report.single_user.query_execution_status);
}

test "exchange core bootstrap from snapshot replays only post-snapshot journal suffix" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();

    var add_u1 = makeAddUserCommand(9651);
    add_u1.timestamp = 10;
    var r1 = try source.submitCommand(&add_u1);
    defer r1.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), r1.sequence);

    var snapshot = try snapshot_mod.KernelSnapshot.captureFromKernel(
        allocator,
        source.kernelRef(),
        &.{},
        8,
    );
    defer snapshot.deinit();

    var add_u2 = makeAddUserCommand(9652);
    add_u2.timestamp = 20;
    var r2 = try source.submitCommand(&add_u2);
    defer r2.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 2), r2.sequence);

    var add_u3 = makeAddUserCommand(9653);
    add_u3.timestamp = 30;
    var r3 = try source.submitCommand(&add_u3);
    defer r3.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 3), r3.sequence);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try snapshot.writeToFile(tmp.dir, "state.snap");
    try source.writeJournalToFile(tmp.dir, "state.journal");

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXSNAPJRN",
            .snapshot_id = 1,
            .snapshot_base_seq = 1,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = true,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();

    const outcome = try target.bootstrapFromPersistence(
        tmp.dir,
        "state.snap",
        "state.journal",
    );
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 2), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(usize, 2), target.journalLen());
    try std.testing.expectEqual(@as(u64, 3), target.kernelRef().next_seq);
    try std.testing.expectEqual(@as(usize, 2), target.persistenceSnapshotCount());
    try std.testing.expectEqual(@as(usize, 1), target.persistenceJournalCount());

    const restored_snapshot_desc = target.persistenceCurrentSnapshot();
    try std.testing.expectEqual(@as(u64, 1), restored_snapshot_desc.snapshot_id);
    try std.testing.expectEqual(@as(u64, 1), restored_snapshot_desc.seq);
    try std.testing.expectEqual(@as(?JournalRef, null), target.persistenceFindJournalForSequence(1));
    const replay_ref = target.persistenceFindJournalForSequence(2) orelse return error.ExpectedJournal;
    const replay_desc = target.persistenceJournalByRef(replay_ref);
    try std.testing.expectEqual(@as(u64, 2), replay_desc.seq_first);
    try std.testing.expectEqual(@as(?u64, 3), replay_desc.seq_last);
    try std.testing.expectEqual(target.persistenceCurrentSnapshotRef(), replay_desc.base_snapshot);

    var user1_report = try target.queryReport(.{ .single_user = 9651 });
    defer user1_report.deinit(allocator);
    var user2_report = try target.queryReport(.{ .single_user = 9652 });
    defer user2_report.deinit(allocator);
    var user3_report = try target.queryReport(.{ .single_user = 9653 });
    defer user3_report.deinit(allocator);

    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user1_report.single_user.query_execution_status,
    );
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user2_report.single_user.query_execution_status,
    );
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user3_report.single_user.query_execution_status,
    );
}

test "exchange core bootstrap policy missing snapshot obeys throw-if-not-found" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var required_snapshot = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.fromSnapshotOnly("EXSNAP", 7, 100),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer required_snapshot.deinit();

    try std.testing.expectError(
        error.SnapshotFileNotFound,
        required_snapshot.bootstrapFromPersistence(tmp.dir, "missing.snap", null),
    );
    try std.testing.expectError(
        error.SnapshotPathRequired,
        required_snapshot.bootstrapFromPersistence(tmp.dir, null, null),
    );

    var optional_snapshot = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXSNAPOPT",
            .snapshot_id = 9,
            .snapshot_base_seq = 100,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = false,
        },
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer optional_snapshot.deinit();

    const outcome = try optional_snapshot.bootstrapFromPersistence(
        tmp.dir,
        "missing.snap",
        null,
    );
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(!outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);
}

test "exchange core bootstrap policy restores snapshot file when present" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer source.deinit();
    try std.testing.expect(try source.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var add_user = try source.addUser(9701);
    defer add_user.deinit(allocator);
    var deposit = try source.adjustUserBalance(9701, 1, 15, 6_000, .adjustment);
    defer deposit.deinit(allocator);

    var snapshot = try snapshot_mod.KernelSnapshot.captureFromKernel(
        allocator,
        source.kernelRef(),
        &.{241},
        8,
    );
    defer snapshot.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try snapshot.writeToFile(tmp.dir, "state.snap");

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.fromSnapshotOnly("EXSNAPLOAD", 11, 777),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer target.deinit();

    const outcome = try target.bootstrapFromPersistence(tmp.dir, "state.snap", null);
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(u64, 777), target.kernelRef().next_seq);

    var user_report = try target.queryReport(.{ .single_user = 9701 });
    defer user_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user_report.single_user.query_execution_status,
    );
}

test "exchange core snapshot file APIs capture and validate kernel state" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var add_user = try core.addUser(9801);
    defer add_user.deinit(allocator);
    var deposit = try core.adjustUserBalance(9801, 1, 15, 5_000, .adjustment);
    defer deposit.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try core.captureSnapshotToFile(tmp.dir, "core.snap", &.{241}, 16);
    try core.validateSnapshotFile(tmp.dir, "core.snap");

    var mutate = try core.addUser(9802);
    defer mutate.deinit(allocator);
    try std.testing.expectError(
        error.RiskHashMismatch,
        core.validateSnapshotFile(tmp.dir, "core.snap"),
    );
}

test "exchange core lifecycle startup and shutdown flags are deterministic" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    try std.testing.expect(!core.isStarted());
    try std.testing.expect(!core.isStopped());

    try core.startup();
    try std.testing.expect(core.isStarted());
    try std.testing.expect(!core.isStopped());

    try core.startup();
    try std.testing.expect(core.isStarted());
    try std.testing.expect(!core.isStopped());

    core.shutdown();
    try std.testing.expect(core.isStopped());
    try std.testing.expectError(error.CoreStopped, core.startup());
}

test "exchange core blocks command submission paths after shutdown" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    core.shutdown();

    var add_cmd = makeAddUserCommand(9951);
    add_cmd.timestamp = 1;
    try std.testing.expectError(error.CoreStopped, core.submitCommand(&add_cmd));
    try std.testing.expectError(error.CoreStopped, core.addUser(9951));

    var batch_cmd = makeAddUserCommand(9952);
    batch_cmd.timestamp = 2;
    var batch = [_]*cmd.OrderCommand{&batch_cmd};
    try std.testing.expectError(error.CoreStopped, core.submitCommandBatch(batch[0..]));

    try std.testing.expectError(error.CoreStopped, core.submitCommandAsync(&batch_cmd));
    try std.testing.expectError(error.CoreStopped, core.addUserAsync(9953));
    try std.testing.expectError(error.CoreStopped, core.persistStateAsync(9001, 10));
    try std.testing.expectError(
        error.CoreStopped,
        core.submitBinaryPayloadAsync(.binary_data_query, 704, 111, &.{0x7F}),
    );

    try std.testing.expectError(error.CoreStopped, core.drainAsyncPending());
    try std.testing.expectError(error.CoreStopped, core.enqueueRuntime(makeAddUserCommand(9954)));
    try std.testing.expectError(error.CoreStopped, core.drainRuntime());
}

test "exchange core startup-from-persistence composes lifecycle and bootstrap replay" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();

    var add_user = makeAddUserCommand(9901);
    add_user.timestamp = 10;
    var add_response = try source.submitCommand(&add_user);
    defer add_response.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.writeJournalToFile(tmp.dir, "startup.journal");

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXSTART",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = false,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();

    const outcome = try target.startupFromPersistence(
        tmp.dir,
        null,
        "startup.journal",
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expectEqual(@as(usize, 1), outcome.journal_replayed_commands);

    var user_report = try target.queryReport(.{ .single_user = 9901 });
    defer user_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user_report.single_user.query_execution_status,
    );
}

test "exchange core startup-from-persistence reports missing explicit journal file" {
    const allocator = std.testing.allocator;

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXSTARTMISS",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = false,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try std.testing.expectError(
        error.FileNotFound,
        target.startupFromPersistence(
            tmp.dir,
            null,
            "missing.journal",
        ),
    );
}

test "exchange core startupFromConfiguredPersistence derives default paths from config" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXSTARTCFG"),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();

    var add_user = makeAddUserCommand(9902);
    add_user.timestamp = 10;
    var add_response = try source.submitCommand(&add_user);
    defer add_response.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.writeJournalToDefaultPath(tmp.dir, 0, 1);

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXSTARTCFG",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = false,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();

    const outcome = try target.startupFromConfiguredPersistence(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expectEqual(@as(usize, 1), outcome.journal_replayed_commands);

    var user_report = try target.queryReport(.{ .single_user = 9902 });
    defer user_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user_report.single_user.query_execution_status,
    );
}

test "exchange core startupFromConfiguredPersistence tolerates missing first journal partition" {
    const allocator = std.testing.allocator;

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXSTARTCFGMISS",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = false,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const outcome = try target.startupFromConfiguredPersistence(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);

    var user_report = try target.queryReport(.{ .single_user = 9905 });
    defer user_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.user_not_found,
        user_report.single_user.query_execution_status,
    );
}

test "exchange core startupFromDiscoveredConfiguredPersistence picks latest snapshot id by module+instance" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXAUTOSTART"),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer source.deinit();

    try std.testing.expect(try source.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try source.captureSnapshotToDefaultPath(tmp.dir, 51, .risk_engine, 0, &.{241}, 12);
    try source.captureSnapshotToDefaultPath(tmp.dir, 52, .risk_engine, 0, &.{241}, 12);
    try source.captureSnapshotToDefaultPath(tmp.dir, 99, .matching_engine_router, 1, &.{241}, 12);

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAUTOSTART",
            .snapshot_id = 1,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = true,
        },
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer target.deinit();

    const outcome = try target.startupFromDiscoveredConfiguredPersistence(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(u64, 52), target.persistenceCurrentSnapshot().snapshot_id);
}

test "exchange core startupFromDiscoveredConfiguredPersistence derives base sequence from main log metadata" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXAUTOSEQ"),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer source.deinit();

    var add_user = makeAddUserCommand(9910);
    add_user.timestamp = 10;
    var add_response = try source.submitCommand(&add_user);
    defer add_response.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), source.kernelRef().next_seq);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.captureSnapshotToDefaultPath(tmp.dir, 71, .risk_engine, 0, &.{}, 8);

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAUTOSEQ",
            .snapshot_id = 1,
            .snapshot_base_seq = 999,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = true,
        },
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer target.deinit();

    const outcome = try target.startupFromDiscoveredConfiguredPersistence(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(u64, 1), target.kernelRef().next_seq);
    try std.testing.expectEqual(@as(u64, 1), target.persistenceCurrentSnapshot().seq);
}

test "exchange core startupFromDiscoveredConfiguredPersistence falls back to configured base sequence without main log metadata" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXAUTOFALLBACK"),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer source.deinit();

    var add_user = makeAddUserCommand(9912);
    add_user.timestamp = 10;
    var add_response = try source.submitCommand(&add_user);
    defer add_response.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.captureSnapshotToDefaultPath(tmp.dir, 81, .risk_engine, 0, &.{}, 8);
    const main_log_path = try source.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    try tmp.dir.deleteFile(main_log_path);

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAUTOFALLBACK",
            .snapshot_id = 1,
            .snapshot_base_seq = 777,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = true,
        },
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer target.deinit();

    const outcome = try target.startupFromDiscoveredConfiguredPersistence(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(u64, 777), target.kernelRef().next_seq);
    try std.testing.expectEqual(@as(u64, 777), target.persistenceCurrentSnapshot().seq);
}

test "exchange core strict discovered startup derives base sequence from valid main log metadata" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXAUTOSTRICT"),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer source.deinit();

    var add_user = makeAddUserCommand(9913);
    add_user.timestamp = 10;
    var add_response = try source.submitCommand(&add_user);
    defer add_response.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), source.kernelRef().next_seq);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.captureSnapshotToDefaultPath(tmp.dir, 91, .risk_engine, 0, &.{}, 8);

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAUTOSTRICT",
            .snapshot_id = 1,
            .snapshot_base_seq = 555,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = true,
        },
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer target.deinit();

    const outcome = try target.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(u64, 1), target.kernelRef().next_seq);
    try std.testing.expectEqual(@as(u64, 1), target.persistenceCurrentSnapshot().seq);
}

test "exchange core strict discovered startup reports malformed main log entry" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXAUTOBADLOG"),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer source.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.captureSnapshotToDefaultPath(tmp.dir, 101, .risk_engine, 0, &.{}, 8);

    const main_log_path = try source.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    var main_log_file = try tmp.dir.createFile(main_log_path, .{ .truncate = true });
    defer main_log_file.close();
    try main_log_file.writeAll("this is malformed\n");

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAUTOBADLOG",
            .snapshot_id = 1,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = true,
        },
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer target.deinit();

    try std.testing.expectError(
        error.MalformedMainLogEntry,
        target.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
            tmp.dir,
            .risk_engine,
            0,
            1,
        ),
    );
}

test "exchange core startupFromDiscoveredConfiguredPersistence clean-starts when snapshot missing" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXAUTONONE"),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();

    var add_user = makeAddUserCommand(9911);
    add_user.timestamp = 10;
    var add_response = try source.submitCommand(&add_user);
    defer add_response.deinit(allocator);
    try source.writeJournalToDefaultPath(tmp.dir, 0, 1);

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAUTONONE",
            .snapshot_id = 10,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = false,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();

    const outcome = try target.startupFromDiscoveredConfiguredPersistence(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(!outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(usize, 0), target.journalLen());

    var user_report = try target.queryReport(.{ .single_user = 9911 });
    defer user_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.user_not_found,
        user_report.single_user.query_execution_status,
    );
}

test "exchange core startupFromDiscoveredConfiguredPersistence errors when snapshot required but missing" {
    const allocator = std.testing.allocator;

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAUTOREQ",
            .snapshot_id = 77,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = true,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try std.testing.expectError(
        error.SnapshotFileNotFound,
        target.startupFromDiscoveredConfiguredPersistence(
            tmp.dir,
            .risk_engine,
            0,
            1,
        ),
    );
}

test "exchange core startupFromConfiguredPersistence replays sequential journal partitions" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXSTARTCFG2"),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();

    var add_user_a = makeAddUserCommand(9903);
    add_user_a.timestamp = 10;
    var add_response_a = try source.submitCommand(&add_user_a);
    defer add_response_a.deinit(allocator);

    var add_user_b = makeAddUserCommand(9904);
    add_user_b.timestamp = 20;
    var add_response_b = try source.submitCommand(&add_user_b);
    defer add_response_b.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try source.writeJournalToDefaultPath(tmp.dir, 0, 1);

    const partition_1_path = try source.defaultPersistenceJournalPath(allocator, 0, 1);
    defer allocator.free(partition_1_path);
    const partition_2_path = try source.defaultPersistenceJournalPath(allocator, 0, 2);
    defer allocator.free(partition_2_path);

    var partition_1_journal = try journal_mod.CommandJournal.readFromFile(
        allocator,
        tmp.dir,
        partition_1_path,
    );
    defer partition_1_journal.deinit();
    try std.testing.expectEqual(@as(usize, 2), partition_1_journal.entries.items.len);
    partition_1_journal.entries.items.len = 1;
    try partition_1_journal.writeToFile(tmp.dir, partition_1_path);

    var partition_2_journal = try journal_mod.CommandJournal.readFromFile(
        allocator,
        tmp.dir,
        partition_1_path,
    );
    defer partition_2_journal.deinit();
    try std.testing.expectEqual(@as(usize, 1), partition_2_journal.entries.items.len);
    try source.writeJournalToDefaultPath(tmp.dir, 0, 1);
    var full_journal = try journal_mod.CommandJournal.readFromFile(
        allocator,
        tmp.dir,
        partition_1_path,
    );
    defer full_journal.deinit();
    try std.testing.expectEqual(@as(usize, 2), full_journal.entries.items.len);
    partition_2_journal.entries.items[0] = full_journal.entries.items[1];
    try partition_2_journal.writeToFile(tmp.dir, partition_2_path);

    try partition_1_journal.writeToFile(tmp.dir, partition_1_path);

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXSTARTCFG2",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = false,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();

    const outcome = try target.startupFromConfiguredPersistence(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expectEqual(@as(usize, 2), outcome.journal_replayed_commands);

    var user_report_a = try target.queryReport(.{ .single_user = 9903 });
    defer user_report_a.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user_report_a.single_user.query_execution_status,
    );

    var user_report_b = try target.queryReport(.{ .single_user = 9904 });
    defer user_report_b.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user_report_b.single_user.query_execution_status,
    );
}

test "exchange core seeded configured startup detects partition corruption deterministically" {
    const allocator = std.testing.allocator;

    const Perturbation = enum {
        baseline,
        sequence_mismatch,
        result_mismatch,
    };

    const Scenario = struct {
        name: []const u8,
        seed: u64,
        perturbation: Perturbation,
    };

    const OutcomeKind = enum {
        success,
        sequence_mismatch,
        result_mismatch,
    };

    const Outcome = struct {
        kind: OutcomeKind = .success,
        replayed_commands: usize = 0,
        user_a_status: kernel_mod.SingleUserQueryExecutionStatus = .user_not_found,
        user_b_status: kernel_mod.SingleUserQueryExecutionStatus = .user_not_found,
    };

    const Harness = struct {
        fn runScenario(scenario: Scenario) !Outcome {
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();

            var exchange_id_buf: [64]u8 = undefined;
            const exchange_id = try std.fmt.bufPrint(&exchange_id_buf, "EXCFGCORR{x}", .{scenario.seed});

            var source = try ExchangeCore.init(allocator, .{
                .initial_state = config_mod.InitialStateConfig.cleanStart(exchange_id),
                .serialization = config_mod.SerializationConfig.diskJournaling(),
            });
            defer source.deinit();

            const uid_a: i64 = 992_000 + @as(i64, @intCast(scenario.seed % 4000)) * 2;
            const uid_b: i64 = uid_a + 1;

            var add_a = makeAddUserCommand(uid_a);
            add_a.timestamp = 10;
            var add_a_response = try source.submitCommand(&add_a);
            defer add_a_response.deinit(allocator);

            var add_b = makeAddUserCommand(uid_b);
            add_b.timestamp = 20;
            var add_b_response = try source.submitCommand(&add_b);
            defer add_b_response.deinit(allocator);

            try source.writeJournalToDefaultPath(tmp.dir, 0, 1);

            const partition_1_path = try source.defaultPersistenceJournalPath(allocator, 0, 1);
            defer allocator.free(partition_1_path);
            const partition_2_path = try source.defaultPersistenceJournalPath(allocator, 0, 2);
            defer allocator.free(partition_2_path);

            var full_journal = try journal_mod.CommandJournal.readFromFile(
                allocator,
                tmp.dir,
                partition_1_path,
            );
            defer full_journal.deinit();
            try std.testing.expectEqual(@as(usize, 2), full_journal.entries.items.len);

            var partition_1_journal = try journal_mod.CommandJournal.readFromFile(
                allocator,
                tmp.dir,
                partition_1_path,
            );
            defer partition_1_journal.deinit();
            partition_1_journal.entries.items.len = 1;
            try partition_1_journal.writeToFile(tmp.dir, partition_1_path);

            var partition_2_journal = try journal_mod.CommandJournal.readFromFile(
                allocator,
                tmp.dir,
                partition_1_path,
            );
            defer partition_2_journal.deinit();
            partition_2_journal.entries.items[0] = full_journal.entries.items[1];

            switch (scenario.perturbation) {
                .baseline => {},
                .sequence_mismatch => {
                    partition_2_journal.entries.items[0].sequence += 1;
                },
                .result_mismatch => {
                    partition_2_journal.entries.items[0].result_code = .drop;
                },
            }
            try partition_2_journal.writeToFile(tmp.dir, partition_2_path);

            var target = try ExchangeCore.init(allocator, .{
                .initial_state = .{
                    .exchange_id = exchange_id,
                    .snapshot_id = 0,
                    .snapshot_base_seq = 0,
                    .journal_timestamp_ns = std.math.maxInt(u64),
                    .throw_if_snapshot_not_found = false,
                },
                .serialization = config_mod.SerializationConfig.diskJournaling(),
            });
            defer target.deinit();

            const startup = target.startupFromConfiguredPersistence(
                tmp.dir,
                .risk_engine,
                0,
                1,
            ) catch |err| switch (err) {
                error.SequenceMismatch => return .{
                    .kind = .sequence_mismatch,
                },
                error.ResultMismatch => return .{
                    .kind = .result_mismatch,
                },
                else => std.debug.panic(
                    "seeded configured startup scenario='{s}' seed=0x{x} perturbation={s} failed with unexpected error {s}; replay with: zig build test --test-filter \"exchange core seeded configured startup detects partition corruption deterministically\"",
                    .{ scenario.name, scenario.seed, @tagName(scenario.perturbation), @errorName(err) },
                ),
            };

            var user_a_report = try target.queryReport(.{ .single_user = uid_a });
            defer user_a_report.deinit(allocator);
            var user_b_report = try target.queryReport(.{ .single_user = uid_b });
            defer user_b_report.deinit(allocator);

            return .{
                .kind = .success,
                .replayed_commands = startup.journal_replayed_commands,
                .user_a_status = user_a_report.single_user.query_execution_status,
                .user_b_status = user_b_report.single_user.query_execution_status,
            };
        }
    };

    const scenarios = [_]Scenario{
        .{
            .name = "baseline",
            .seed = 0xACE9201,
            .perturbation = .baseline,
        },
        .{
            .name = "sequence-mismatch",
            .seed = 0xACE9202,
            .perturbation = .sequence_mismatch,
        },
        .{
            .name = "result-mismatch",
            .seed = 0xACE9203,
            .perturbation = .result_mismatch,
        },
    };

    for (scenarios) |scenario| {
        const first = try Harness.runScenario(scenario);
        const second = try Harness.runScenario(scenario);
        try std.testing.expectEqualDeep(first, second);

        switch (scenario.perturbation) {
            .baseline => {
                try std.testing.expectEqual(OutcomeKind.success, first.kind);
                try std.testing.expectEqual(@as(usize, 2), first.replayed_commands);
                try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, first.user_a_status);
                try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, first.user_b_status);
            },
            .sequence_mismatch => {
                try std.testing.expectEqual(OutcomeKind.sequence_mismatch, first.kind);
            },
            .result_mismatch => {
                try std.testing.expectEqual(OutcomeKind.result_mismatch, first.kind);
            },
        }
    }
}

test "exchange core seeded persistence checkpoints keep snapshot-journal bootstrap deterministic" {
    const allocator = std.testing.allocator;

    const symbol: i32 = 241;
    const user_count: usize = 24;
    const rounds: usize = 600;
    const checkpoint_interval: usize = 120;
    const initial_balance: i64 = 1_000_000_000;

    var user_ids: [user_count]i64 = undefined;
    for (&user_ids, 0..) |*slot, idx| {
        slot.* = 20_000 + @as(i64, @intCast(idx));
    }

    var source = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();
    try std.testing.expect(try source.kernelRef().addSymbol(try makeExchangeSpecForSymbol(symbol)));

    var timestamp: i64 = 1;
    var transaction_id: i64 = 1;
    for (user_ids) |uid| {
        var add_user = makeAddUserCommand(uid);
        add_user.timestamp = timestamp;
        timestamp += 1;
        var add_response = try source.submitCommand(&add_user);
        defer add_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, add_response.result_code);

        var deposit_base = makeBalanceAdjustmentCommand(uid, 11, initial_balance, transaction_id, .adjustment);
        transaction_id += 1;
        deposit_base.timestamp = timestamp;
        timestamp += 1;
        var base_response = try source.submitCommand(&deposit_base);
        defer base_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, base_response.result_code);

        var deposit_quote = makeBalanceAdjustmentCommand(uid, 15, initial_balance, transaction_id, .adjustment);
        transaction_id += 1;
        deposit_quote.timestamp = timestamp;
        timestamp += 1;
        var quote_response = try source.submitCommand(&deposit_quote);
        defer quote_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, quote_response.result_code);
    }

    var prng = std.Random.DefaultPrng.init(0xC0DEC0DEC0FFEE42);
    const random = prng.random();
    var next_order_id: i64 = 70_000;
    var previous_checkpoint_seq: ?u64 = null;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    for (0..rounds) |idx| {
        const roll = random.uintLessThan(u32, 100);
        var command: cmd.OrderCommand = undefined;

        if (roll < 76) {
            const uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            const action: common.OrderAction = if (random.boolean()) .ask else .bid;
            const order_type: common.OrderType = if (random.uintLessThan(u32, 100) < 82) .gtc else .ioc;
            const price = 980 + @as(i64, @intCast(random.uintLessThan(u32, 120)));
            const size = @as(i64, @intCast(random.uintLessThan(u32, 12))) + 1;
            const reserve = if (action == .bid)
                price + @as(i64, @intCast(random.uintLessThan(u32, 24)))
            else
                0;

            command = cmd.OrderCommand.newOrder(
                order_type,
                next_order_id,
                uid,
                price,
                reserve,
                size,
                action,
            );
            command.symbol = symbol;
            next_order_id += 1;
        } else if (roll < 92) {
            const uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            const currency: i32 = if (random.boolean()) 11 else 15;
            const amount_abs = @as(i64, @intCast(random.uintLessThan(u32, 3_000))) + 1;
            const amount = if (random.uintLessThan(u32, 100) < 16) -amount_abs else amount_abs;
            command = makeBalanceAdjustmentCommand(uid, currency, amount, transaction_id, .adjustment);
            transaction_id += 1;
        } else {
            command = makeRequestOrderBookCommandI64(
                symbol,
                @as(i64, @intCast(random.uintLessThan(usize, 48) + 1)),
            );
        }

        command.timestamp = timestamp;
        timestamp += @as(i64, @intCast(random.uintLessThan(u32, 3))) + 1;

        var response = try source.submitCommand(&command);
        defer response.deinit(allocator);

        if ((idx + 1) % checkpoint_interval != 0) continue;

        const checkpoint_seq = source.kernelRef().next_seq;
        var snapshot_name_buf: [64]u8 = undefined;
        const snapshot_name = try std.fmt.bufPrint(
            &snapshot_name_buf,
            "core-sim-seq-{d}.snap",
            .{checkpoint_seq},
        );
        var journal_name_buf: [64]u8 = undefined;
        const journal_name = try std.fmt.bufPrint(
            &journal_name_buf,
            "core-sim-seq-{d}.journal",
            .{checkpoint_seq},
        );

        try source.captureSnapshotToFile(tmp.dir, snapshot_name, &.{symbol}, 64);
        try source.writeJournalToFile(tmp.dir, journal_name);

        var restored_from_current = try ExchangeCore.init(allocator, .{
            .initial_state = .{
                .exchange_id = "EXSIMCUR",
                .snapshot_id = 1,
                .snapshot_base_seq = checkpoint_seq,
                .journal_timestamp_ns = std.math.maxInt(u64),
                .throw_if_snapshot_not_found = true,
            },
            .matching = .{
                .orderbook_kind = .direct,
            },
            .serialization = config_mod.SerializationConfig.diskJournaling(),
        });
        defer restored_from_current.deinit();

        const from_current = try restored_from_current.startupFromPersistence(
            tmp.dir,
            snapshot_name,
            journal_name,
        );
        try std.testing.expect(from_current.snapshot_requested);
        try std.testing.expect(from_current.snapshot_found);
        try std.testing.expectEqual(@as(usize, 0), from_current.journal_replayed_commands);
        try std.testing.expectEqual(checkpoint_seq, restored_from_current.kernelRef().next_seq);
        try std.testing.expectEqual(@as(usize, 0), restored_from_current.journalLen());
        try expectExchangeCoreStateParity(
            allocator,
            &source,
            &restored_from_current,
            user_ids[0..],
            symbol,
        );

        if (previous_checkpoint_seq) |previous_seq| {
            var previous_snapshot_name_buf: [64]u8 = undefined;
            const previous_snapshot_name = try std.fmt.bufPrint(
                &previous_snapshot_name_buf,
                "core-sim-seq-{d}.snap",
                .{previous_seq},
            );

            var restored_from_previous = try ExchangeCore.init(allocator, .{
                .initial_state = .{
                    .exchange_id = "EXSIMPRV",
                    .snapshot_id = 1,
                    .snapshot_base_seq = previous_seq,
                    .journal_timestamp_ns = std.math.maxInt(u64),
                    .throw_if_snapshot_not_found = true,
                },
                .matching = .{
                    .orderbook_kind = .direct,
                },
                .serialization = config_mod.SerializationConfig.diskJournaling(),
            });
            defer restored_from_previous.deinit();

            const from_previous = try restored_from_previous.startupFromPersistence(
                tmp.dir,
                previous_snapshot_name,
                journal_name,
            );
            const expected_replayed_u64 = checkpoint_seq - previous_seq;
            const expected_replayed = std.math.cast(usize, expected_replayed_u64) orelse unreachable;
            try std.testing.expect(from_previous.snapshot_requested);
            try std.testing.expect(from_previous.snapshot_found);
            try std.testing.expectEqual(expected_replayed, from_previous.journal_replayed_commands);
            try std.testing.expectEqual(expected_replayed, restored_from_previous.journalLen());
            try std.testing.expectEqual(checkpoint_seq, restored_from_previous.kernelRef().next_seq);
            try expectExchangeCoreStateParity(
                allocator,
                &source,
                &restored_from_previous,
                user_ids[0..],
                symbol,
            );
        }

        previous_checkpoint_seq = checkpoint_seq;
    }

    try std.testing.expect(previous_checkpoint_seq != null);
}
