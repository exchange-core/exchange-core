const std = @import("std");
const binary_frames = @import("binary_frames.zig");
const cmd = @import("cmd.zig");
const common = @import("common.zig");
const history_codec = @import("compat/java/eclipse_collections/history_codec.zig");
const int_map_order = @import("compat/java/eclipse_collections/int_map_order.zig");
const long_map_order = @import("compat/java/eclipse_collections/long_map_order.zig");
const kernel_mod = @import("exchange_kernel.zig");
const risk_mod = @import("risk.zig");
const snapshot = @import("snapshot.zig");
const user = @import("user_profile.zig");

pub const matching_body_magic = "XMB2";
pub const risk_body_magic = "XRB2";
pub const legacy_body_schema_version: u16 = 2;
pub const transfer_capacity_schema_version: u16 = 3;
pub const body_schema_version: u16 = 4;

/// Fixed byte widths shared with the legacy snapshot field codecs. XCM2 body
/// framing is new, but symbol, order, account, and position records retain one
/// canonical definition across snapshot generations.
pub const symbol_spec_len: usize = 61;
pub const resting_order_len: usize = 61;
pub const i32_i64_entry_len: usize = 12;
pub const position_len: usize = 49;
pub const last_price_len: usize = 20;
pub const legacy_transfer_header_len: usize = 9;
pub const transfer_header_len: usize = 13;
pub const matching_body_header_len: usize = 28;
pub const risk_body_header_len: usize = 28;

pub const Limits = struct {
    max_body_bytes: usize = 256 * 1024 * 1024,
    max_symbols: usize = 1_048_576,
    max_orders_per_book: usize = 4_194_304,
    max_total_orders: usize = 16_777_216,
    max_users: usize = 4_194_304,
    max_accounts_per_user: usize = 65_536,
    max_positions_per_user: usize = 1_048_576,
    max_total_user_entries: usize = 16_777_216,
    max_map_entries: usize = 4_194_304,
    max_transfers: usize = 1_048_576,
    max_words_per_transfer: usize = 16_777_215,
    max_total_transfer_words: usize = 67_108_864,
    /// Per-history raw EC backing-slot bound. The aggregate encoded body is
    /// independently constrained by `max_body_bytes`.
    max_history_slots: usize = 16_777_216,
};

pub const FormatError = error{
    InvalidMagic,
    UnsupportedVersion,
    UnknownFlags,
    InvalidFormat,
    Truncated,
    TrailingBytes,
    LimitExceeded,
    EncodingLengthMismatch,
    NonCanonicalOrder,
    DuplicateSymbol,
    DuplicateOrderId,
    DuplicateUser,
    DuplicateKey,
    InvalidOrder,
    InvalidTransfer,
    InvalidUser,
    PositionSymbolMissing,
    WrongUidOwner,
    TargetNotFresh,
    MatchingStateHashMismatch,
    RiskStateHashMismatch,
    BinaryStateHashMismatch,
    StateCorrupted,
    HistorySemanticKeySetMismatch,
};

pub const MatchingState = struct {
    allocator: std.mem.Allocator,
    books_state_hash: u64,
    binary_state_hash: u64,
    books: std.ArrayListUnmanaged(snapshot.BookSnapshot) = .empty,
    books_history: int_map_order.History = int_map_order.History.initUnavailable(),
    pending: binary_frames.PendingState,

    pub fn capture(
        allocator: std.mem.Allocator,
        kernel: *const kernel_mod.ExchangeKernel,
        limits: Limits,
    ) !MatchingState {
        var result = try MatchingState.initEmpty(allocator);
        errdefer result.deinit();

        if (kernel.books.count() > limits.max_symbols) return error.LimitExceeded;
        var symbols: std.ArrayListUnmanaged(i32) = .empty;
        defer symbols.deinit(allocator);
        try symbols.ensureTotalCapacityPrecise(allocator, kernel.books.count());
        var symbol_it = kernel.books.keyIterator();
        while (symbol_it.next()) |symbol| symbols.appendAssumeCapacity(symbol.*);
        std.sort.heap(i32, symbols.items, {}, lessThanI32);

        try result.books.ensureTotalCapacityPrecise(allocator, symbols.items.len);
        var total_orders: usize = 0;
        for (symbols.items) |symbol| {
            var book: snapshot.BookSnapshot = .{
                .symbol_spec = bookSpecification(kernel, symbol) orelse return error.StateCorrupted,
            };
            errdefer book.deinit(allocator);
            try kernel.appendOrdersForSymbol(allocator, symbol, &book.orders);
            if (book.orders.items.len > limits.max_orders_per_book) return error.LimitExceeded;
            total_orders = std.math.add(usize, total_orders, book.orders.items.len) catch
                return error.LimitExceeded;
            if (total_orders > limits.max_total_orders) return error.LimitExceeded;
            try validateBook(allocator, book);
            result.books.appendAssumeCapacity(book);
        }

        var books_history = try kernel.cloneMatchingBooksHistory(allocator);
        errdefer books_history.deinit(allocator);

        var pending = try kernel.binary_frame_assembler.capturePendingState(allocator);
        errdefer pending.deinit();
        try validatePending(pending.transfers, limits);
        const books_state_hash = try matchingBookSnapshotsStateHash(result.books.items);
        const binary_state_hash = try kernel.binary_frame_assembler.stateHash();

        // Transfer ownership only after every fallible operation. Until this
        // point `pending` is owned solely by its local errdefer; afterwards the
        // function returns without another error edge and `result` owns it.
        result.books_state_hash = books_state_hash;
        result.binary_state_hash = binary_state_hash;
        result.books_history.deinit(allocator);
        result.books_history = books_history;
        books_history = int_map_order.History.initUnavailable();
        result.pending.deinit();
        result.pending = pending;
        return result;
    }

    pub fn decodeAlloc(
        allocator: std.mem.Allocator,
        bytes: []const u8,
        limits: Limits,
    ) (std.mem.Allocator.Error || FormatError)!MatchingState {
        if (bytes.len > limits.max_body_bytes) return error.LimitExceeded;
        if (bytes.len < matching_body_header_len) return error.Truncated;
        var reader = std.Io.Reader.fixed(bytes);
        try expectMagic(&reader, matching_body_magic);
        const schema_version = try readInt(u16, &reader);
        if (!isSupportedBodySchema(schema_version)) return error.UnsupportedVersion;
        if (try readInt(u16, &reader) != 0) return error.UnknownFlags;

        var result = try MatchingState.initEmpty(allocator);
        errdefer result.deinit();
        result.books_state_hash = try readInt(u64, &reader);
        result.binary_state_hash = try readInt(u64, &reader);

        const book_count = try readBoundedCount(&reader, limits.max_symbols, symbol_spec_len + @sizeOf(u32));
        try result.books.ensureTotalCapacityPrecise(allocator, book_count);
        var prior_symbol: ?i32 = null;
        var total_orders: usize = 0;
        for (0..book_count) |_| {
            var book: snapshot.BookSnapshot = .{
                .symbol_spec = try readSymbolSpec(&reader),
            };
            errdefer book.deinit(allocator);
            if (prior_symbol) |prior| {
                if (book.symbol_spec.symbol_id == prior) return error.DuplicateSymbol;
                if (book.symbol_spec.symbol_id < prior) return error.NonCanonicalOrder;
            }
            prior_symbol = book.symbol_spec.symbol_id;

            const order_count = try readBoundedCount(&reader, limits.max_orders_per_book, resting_order_len);
            total_orders = std.math.add(usize, total_orders, order_count) catch return error.LimitExceeded;
            if (total_orders > limits.max_total_orders) return error.LimitExceeded;
            try book.orders.ensureTotalCapacityPrecise(allocator, order_count);
            for (0..order_count) |_| book.orders.appendAssumeCapacity(try readRestingOrder(&reader));
            try validateBook(allocator, book);
            result.books.appendAssumeCapacity(book);
        }

        if (schema_version >= body_schema_version) {
            var decoded_history = try readIntHistory(allocator, &reader, limits);
            errdefer decoded_history.deinit(allocator);
            result.books_history.deinit(allocator);
            result.books_history = decoded_history;
            decoded_history = int_map_order.History.initUnavailable();
        }

        var pending = try readPending(allocator, &reader, limits, schema_version);
        errdefer pending.deinit();
        if (reader.seek != reader.end) return error.TrailingBytes;
        try validateMatchingHistory(&result, allocator);
        try validatePendingStateHash(allocator, pending.transfers, result.binary_state_hash);
        result.pending.deinit();
        result.pending = pending;
        return result;
    }

    pub fn encodeAlloc(
        self: *const MatchingState,
        allocator: std.mem.Allocator,
        limits: Limits,
    ) (std.mem.Allocator.Error || FormatError)![]u8 {
        try validateBooks(allocator, self.books.items, limits);
        try validateMatchingHistory(self, allocator);
        try validatePending(self.pending.transfers, limits);
        if (try matchingBookSnapshotsStateHash(self.books.items) != self.books_state_hash) {
            return error.MatchingStateHashMismatch;
        }
        try validatePendingStateHash(allocator, self.pending.transfers, self.binary_state_hash);
        const encoded_len = try matchingEncodedLength(self, limits);
        const bytes = try allocator.alloc(u8, encoded_len);
        errdefer allocator.free(bytes);
        var writer = std.Io.Writer.fixed(bytes);

        try writeBytes(&writer, matching_body_magic);
        try writeInt(&writer, u16, body_schema_version);
        try writeInt(&writer, u16, 0);
        try writeInt(&writer, u64, self.books_state_hash);
        try writeInt(&writer, u64, self.binary_state_hash);
        try writeCount(&writer, self.books.items.len);
        for (self.books.items) |book| {
            try writeSymbolSpec(&writer, book.symbol_spec);
            try writeCount(&writer, book.orders.items.len);
            for (book.orders.items) |order| try writeRestingOrder(&writer, order);
        }
        try writeIntHistory(&writer, allocator, &self.books_history, limits);
        try writePending(&writer, self.pending.transfers);
        if (writer.end != bytes.len) return error.EncodingLengthMismatch;
        return bytes;
    }

    /// Rebuilds the exact EC IntObjectMap sidecar from Java's serialized book
    /// encounter order. The sized-constructor argument is the serialized key
    /// count, matching Java snapshot restore. Replacement is failure-atomic.
    pub fn initializeBooksHistoryFromEncounterOrder(
        self: *MatchingState,
        encounter_keys: []const i32,
    ) !void {
        var next = try int_map_order.History.initFromEncounterOrder(
            self.allocator,
            encounter_keys.len,
            encounter_keys,
        );
        errdefer next.deinit(self.allocator);
        const keys = try next.orderedKeys(self.allocator);
        defer self.allocator.free(keys);
        if (keys.len != self.books.items.len) {
            return error.HistorySemanticKeySetMismatch;
        }
        for (keys) |key| {
            if (!containsBook(self.books.items, key)) {
                return error.HistorySemanticKeySetMismatch;
            }
        }
        var old = self.books_history;
        self.books_history = next;
        next = int_map_order.History.initUnavailable();
        old.deinit(self.allocator);
    }

    /// Restores only matching-owner state into a fresh carrier. `addSymbol`
    /// temporarily creates carrier risk specifications because the existing
    /// kernel construction API is shared; the final reset is mandatory and
    /// leaves dormant carrier risk state semantically empty. This is a staged,
    /// non-transactional restore: on any error the caller must discard and
    /// deinitialize `kernel`. Integration constructs a fresh target and swaps
    /// it into service only after this function succeeds.
    pub fn restoreIntoFreshKernel(
        self: *const MatchingState,
        kernel: *kernel_mod.ExchangeKernel,
    ) !void {
        if (!matchingTargetIsFresh(kernel)) return error.TargetNotFresh;
        try validateMatchingHistory(self, kernel.allocator);
        for (self.books.items) |book| {
            if (!try kernel.addSymbol(book.symbol_spec)) return error.DuplicateSymbol;
            try kernel.restoreOrdersForSymbol(book.symbol_spec.symbol_id, book.orders.items);
        }
        if (self.books_history.isKnown()) {
            var history = try self.books_history.clone(kernel.allocator);
            defer history.deinit(kernel.allocator);
            try kernel.replaceMatchingBooksHistory(&history);
        } else {
            kernel.markMatchingBooksHistoryUnavailable();
        }
        kernel.risk_engine.reset();
        try kernel.binary_frame_assembler.restorePendingState(self.pending.transfers);
        if (try matchingBooksStateHash(kernel) != self.books_state_hash) {
            return error.MatchingStateHashMismatch;
        }
        if (try kernel.binary_frame_assembler.stateHash() != self.binary_state_hash) {
            return error.BinaryStateHashMismatch;
        }
        if (!riskEngineIsEmpty(&kernel.risk_engine)) return error.StateCorrupted;
    }

    pub fn deinit(self: *MatchingState) void {
        for (self.books.items) |*book| book.deinit(self.allocator);
        self.books.deinit(self.allocator);
        self.books_history.deinit(self.allocator);
        self.pending.deinit();
        self.* = undefined;
    }

    fn initEmpty(allocator: std.mem.Allocator) std.mem.Allocator.Error!MatchingState {
        return .{
            .allocator = allocator,
            .books_state_hash = 0,
            .binary_state_hash = 0,
            .books_history = int_map_order.History.initUnavailable(),
            .pending = .{
                .allocator = allocator,
                .transfers = try allocator.alloc(binary_frames.PendingTransfer, 0),
            },
        };
    }
};

pub const UserHistoryEncounter = struct {
    uid: i64,
    account_keys: []const i32,
    position_keys: []const i32,
};

pub const RiskHistoryEncounters = struct {
    user_uids: []const i64,
    users: []const UserHistoryEncounter,
    fee_keys: []const i32,
    adjustment_keys: []const i32,
    suspend_keys: []const i32,
};

const UserHistorySidecar = struct {
    uid: i64,
    accounts: int_map_order.History,
    positions: int_map_order.History,

    fn deinit(self: *UserHistorySidecar, allocator: std.mem.Allocator) void {
        self.accounts.deinit(allocator);
        self.positions.deinit(allocator);
        self.* = undefined;
    }
};

pub const RiskState = struct {
    allocator: std.mem.Allocator,
    risk_state_hash: u64,
    binary_state_hash: u64,
    symbols: std.ArrayListUnmanaged(common.CoreSymbolSpecification) = .empty,
    users: std.ArrayListUnmanaged(snapshot.UserSnapshot) = .empty,
    user_profiles_history: long_map_order.History = long_map_order.History.initUnavailable(),
    user_histories: std.ArrayListUnmanaged(UserHistorySidecar) = .empty,
    last_prices: std.ArrayListUnmanaged(snapshot.LastPriceCacheEntry) = .empty,
    fees: std.ArrayListUnmanaged(snapshot.I32I64Entry) = .empty,
    fees_history: int_map_order.History = int_map_order.History.initUnavailable(),
    adjustments: std.ArrayListUnmanaged(snapshot.I32I64Entry) = .empty,
    adjustments_history: int_map_order.History = int_map_order.History.initUnavailable(),
    suspends: std.ArrayListUnmanaged(snapshot.I32I64Entry) = .empty,
    suspends_history: int_map_order.History = int_map_order.History.initUnavailable(),
    pending: binary_frames.PendingState,

    pub fn capture(
        allocator: std.mem.Allocator,
        engine: *const risk_mod.RiskEngine,
        assembler: *const binary_frames.BinaryFrameAssembler,
        limits: Limits,
    ) !RiskState {
        var result = try RiskState.initEmpty(allocator);
        errdefer result.deinit();
        result.risk_state_hash = try engine.stateHash();
        result.binary_state_hash = try assembler.stateHash();

        try captureRiskSymbols(&result, engine, limits);
        try captureRiskUsers(&result, engine, limits);
        try captureLastPrices(&result, engine, limits);
        try captureI32I64Map(allocator, &result.fees, &engine.fees, limits.max_map_entries);
        try captureI32I64Map(allocator, &result.adjustments, &engine.adjustments, limits.max_map_entries);
        try captureI32I64Map(allocator, &result.suspends, &engine.suspends, limits.max_map_entries);
        try captureRiskHistories(&result, engine);

        var pending = try assembler.capturePendingState(allocator);
        errdefer pending.deinit();
        try validatePending(pending.transfers, limits);
        result.pending.deinit();
        result.pending = pending;
        return result;
    }

    pub fn decodeAlloc(
        allocator: std.mem.Allocator,
        bytes: []const u8,
        limits: Limits,
    ) (std.mem.Allocator.Error || FormatError)!RiskState {
        if (bytes.len > limits.max_body_bytes) return error.LimitExceeded;
        if (bytes.len < risk_body_header_len) return error.Truncated;
        var reader = std.Io.Reader.fixed(bytes);
        try expectMagic(&reader, risk_body_magic);
        const schema_version = try readInt(u16, &reader);
        if (!isSupportedBodySchema(schema_version)) return error.UnsupportedVersion;
        if (try readInt(u16, &reader) != 0) return error.UnknownFlags;

        var result = try RiskState.initEmpty(allocator);
        errdefer result.deinit();
        result.risk_state_hash = try readInt(u64, &reader);
        result.binary_state_hash = try readInt(u64, &reader);

        const symbol_count = try readBoundedCount(&reader, limits.max_symbols, symbol_spec_len);
        try result.symbols.ensureTotalCapacityPrecise(allocator, symbol_count);
        var prior_symbol: ?i32 = null;
        for (0..symbol_count) |_| {
            const spec = try readSymbolSpec(&reader);
            if (prior_symbol) |prior| {
                if (spec.symbol_id == prior) return error.DuplicateSymbol;
                if (spec.symbol_id < prior) return error.NonCanonicalOrder;
            }
            prior_symbol = spec.symbol_id;
            result.symbols.appendAssumeCapacity(spec);
        }

        const user_count = try readBoundedCount(&reader, limits.max_users, 25);
        try result.users.ensureTotalCapacityPrecise(allocator, user_count);
        var prior_uid: ?i64 = null;
        var total_user_entries: usize = 0;
        for (0..user_count) |_| {
            var snapshot_user: snapshot.UserSnapshot = .{
                .uid = try readInt(i64, &reader),
                .user_status = common.UserStatus.fromCode(try readInt(i8, &reader)) catch
                    return error.InvalidUser,
                .adjustments_counter = try readInt(i64, &reader),
            };
            errdefer snapshot_user.deinit(allocator);
            if (prior_uid) |prior| {
                if (snapshot_user.uid == prior) return error.DuplicateUser;
                if (snapshot_user.uid < prior) return error.NonCanonicalOrder;
            }
            prior_uid = snapshot_user.uid;

            const account_count = try readBoundedCount(&reader, limits.max_accounts_per_user, i32_i64_entry_len);
            total_user_entries = std.math.add(usize, total_user_entries, account_count) catch
                return error.LimitExceeded;
            if (total_user_entries > limits.max_total_user_entries) return error.LimitExceeded;
            try snapshot_user.accounts.ensureTotalCapacityPrecise(allocator, account_count);
            for (0..account_count) |_| snapshot_user.accounts.appendAssumeCapacity(try readI32I64Entry(&reader));
            try validateI32I64Entries(snapshot_user.accounts.items);

            const position_count = try readBoundedCount(&reader, limits.max_positions_per_user, position_len);
            total_user_entries = std.math.add(usize, total_user_entries, position_count) catch
                return error.LimitExceeded;
            if (total_user_entries > limits.max_total_user_entries) return error.LimitExceeded;
            try snapshot_user.positions.ensureTotalCapacityPrecise(allocator, position_count);
            for (0..position_count) |_| snapshot_user.positions.appendAssumeCapacity(try readPosition(&reader));
            try validatePositions(snapshot_user.uid, snapshot_user.positions.items, result.symbols.items);
            result.users.appendAssumeCapacity(snapshot_user);
        }

        try readLastPrices(allocator, &reader, &result.last_prices, limits);
        try readI32I64Entries(allocator, &reader, &result.fees, limits.max_map_entries);
        try readI32I64Entries(allocator, &reader, &result.adjustments, limits.max_map_entries);
        try readI32I64Entries(allocator, &reader, &result.suspends, limits.max_map_entries);
        if (schema_version >= body_schema_version) {
            try readRiskHistories(&result, &reader, limits);
        } else {
            try initializeUnavailableRiskHistories(&result);
        }
        var pending = try readPending(allocator, &reader, limits, schema_version);
        errdefer pending.deinit();
        if (reader.seek != reader.end) return error.TrailingBytes;
        try validatePendingStateHash(allocator, pending.transfers, result.binary_state_hash);
        result.pending.deinit();
        result.pending = pending;
        return result;
    }

    pub fn encodeAlloc(
        self: *const RiskState,
        allocator: std.mem.Allocator,
        limits: Limits,
    ) (std.mem.Allocator.Error || FormatError)![]u8 {
        try validateRiskState(self, allocator, limits);
        try validatePendingStateHash(allocator, self.pending.transfers, self.binary_state_hash);
        const encoded_len = try riskEncodedLength(self, limits);
        const bytes = try allocator.alloc(u8, encoded_len);
        errdefer allocator.free(bytes);
        var writer = std.Io.Writer.fixed(bytes);

        try writeBytes(&writer, risk_body_magic);
        try writeInt(&writer, u16, body_schema_version);
        try writeInt(&writer, u16, 0);
        try writeInt(&writer, u64, self.risk_state_hash);
        try writeInt(&writer, u64, self.binary_state_hash);
        try writeCount(&writer, self.symbols.items.len);
        for (self.symbols.items) |spec| try writeSymbolSpec(&writer, spec);

        try writeCount(&writer, self.users.items.len);
        for (self.users.items) |snapshot_user| {
            try writeInt(&writer, i64, snapshot_user.uid);
            try writeInt(&writer, i8, snapshot_user.user_status.code());
            try writeInt(&writer, i64, snapshot_user.adjustments_counter);
            try writeCount(&writer, snapshot_user.accounts.items.len);
            for (snapshot_user.accounts.items) |entry| try writeI32I64Entry(&writer, entry);
            try writeCount(&writer, snapshot_user.positions.items.len);
            for (snapshot_user.positions.items) |position| try writePosition(&writer, position);
        }

        try writeCount(&writer, self.last_prices.items.len);
        for (self.last_prices.items) |entry| {
            try writeInt(&writer, i32, entry.symbol);
            try writeInt(&writer, i64, entry.ask_price);
            try writeInt(&writer, i64, entry.bid_price);
        }
        try writeI32I64Entries(&writer, self.fees.items);
        try writeI32I64Entries(&writer, self.adjustments.items);
        try writeI32I64Entries(&writer, self.suspends.items);
        try writeRiskHistories(self, &writer, allocator, limits);
        try writePending(&writer, self.pending.transfers);
        if (writer.end != bytes.len) return error.EncodingLengthMismatch;
        return bytes;
    }

    /// Rebuilds every exact EC risk sidecar from Java snapshot encounter
    /// sequences before the semantic entries are sorted. Each map uses its
    /// serialized count as the Java sized-constructor argument. All histories
    /// are staged and validated before any existing sidecar is replaced.
    pub fn initializeHistoriesFromEncounterOrder(
        self: *RiskState,
        encounters: RiskHistoryEncounters,
    ) !void {
        if (encounters.user_uids.len != self.users.items.len or
            encounters.users.len != self.users.items.len)
        {
            return error.HistorySemanticKeySetMismatch;
        }

        var outer = try long_map_order.History.initFromEncounterOrder(
            self.allocator,
            encounters.user_uids.len,
            encounters.user_uids,
        );
        errdefer outer.deinit(self.allocator);
        try validateLongHistoryAgainstUsers(self.allocator, &outer, self.users.items);

        var users: std.ArrayListUnmanaged(UserHistorySidecar) = .empty;
        errdefer {
            for (users.items) |*histories| histories.deinit(self.allocator);
            users.deinit(self.allocator);
        }
        try users.ensureTotalCapacityPrecise(self.allocator, self.users.items.len);
        for (self.users.items) |snapshot_user| {
            const encounter = findUserHistoryEncounter(encounters.users, snapshot_user.uid) orelse
                return error.HistorySemanticKeySetMismatch;
            var accounts = try int_map_order.History.initFromEncounterOrder(
                self.allocator,
                encounter.account_keys.len,
                encounter.account_keys,
            );
            errdefer accounts.deinit(self.allocator);
            try validateIntHistoryAgainstEntries(
                self.allocator,
                &accounts,
                snapshot_user.accounts.items,
            );
            var positions = try int_map_order.History.initFromEncounterOrder(
                self.allocator,
                encounter.position_keys.len,
                encounter.position_keys,
            );
            errdefer positions.deinit(self.allocator);
            try validateIntHistoryAgainstPositions(
                self.allocator,
                &positions,
                snapshot_user.positions.items,
            );
            users.appendAssumeCapacity(.{
                .uid = snapshot_user.uid,
                .accounts = accounts,
                .positions = positions,
            });
            accounts = int_map_order.History.initUnavailable();
            positions = int_map_order.History.initUnavailable();
        }

        var fees = try int_map_order.History.initFromEncounterOrder(
            self.allocator,
            encounters.fee_keys.len,
            encounters.fee_keys,
        );
        errdefer fees.deinit(self.allocator);
        try validateIntHistoryAgainstEntries(self.allocator, &fees, self.fees.items);
        var adjustments = try int_map_order.History.initFromEncounterOrder(
            self.allocator,
            encounters.adjustment_keys.len,
            encounters.adjustment_keys,
        );
        errdefer adjustments.deinit(self.allocator);
        try validateIntHistoryAgainstEntries(
            self.allocator,
            &adjustments,
            self.adjustments.items,
        );
        var suspends = try int_map_order.History.initFromEncounterOrder(
            self.allocator,
            encounters.suspend_keys.len,
            encounters.suspend_keys,
        );
        errdefer suspends.deinit(self.allocator);
        try validateIntHistoryAgainstEntries(self.allocator, &suspends, self.suspends.items);

        self.user_profiles_history.deinit(self.allocator);
        self.user_profiles_history = outer;
        outer = long_map_order.History.initUnavailable();
        for (self.user_histories.items) |*histories| histories.deinit(self.allocator);
        self.user_histories.deinit(self.allocator);
        self.user_histories = users;
        users = .empty;
        self.fees_history.deinit(self.allocator);
        self.fees_history = fees;
        fees = int_map_order.History.initUnavailable();
        self.adjustments_history.deinit(self.allocator);
        self.adjustments_history = adjustments;
        adjustments = int_map_order.History.initUnavailable();
        self.suspends_history.deinit(self.allocator);
        self.suspends_history = suspends;
        suspends = int_map_order.History.initUnavailable();
    }

    /// Restores into fresh staged risk/binary targets. This function does not
    /// roll back partial mutation: on any error the caller must discard and
    /// deinitialize both targets. Integration publishes them only after a
    /// successful restore and fingerprint validation.
    pub fn restoreIntoFreshRisk(
        self: *const RiskState,
        engine: *risk_mod.RiskEngine,
        assembler: *binary_frames.BinaryFrameAssembler,
    ) !void {
        if (!riskEngineIsEmpty(engine) or assembler.pendingTransfers() != 0) return error.TargetNotFresh;
        for (self.symbols.items) |spec| try engine.addSymbol(spec);
        for (self.users.items) |snapshot_user| {
            if (!engine.uidForThisHandler(snapshot_user.uid)) return error.WrongUidOwner;
            var profile = user.UserProfile.init(snapshot_user.uid, snapshot_user.user_status);
            errdefer profile.deinit(engine.allocator);
            profile.adjustments_counter = snapshot_user.adjustments_counter;
            for (snapshot_user.accounts.items) |entry| try profile.accounts.put(engine.allocator, entry.key, entry.value);
            for (snapshot_user.positions.items) |entry| {
                if (engine.symbol_spec_provider.getSymbolSpecification(entry.symbol) == null) {
                    return error.PositionSymbolMissing;
                }
                var position: user.SymbolPositionRecord = .{
                    .uid = snapshot_user.uid,
                    .symbol = entry.symbol,
                    .currency = entry.currency,
                    .direction = entry.direction,
                    .open_volume = entry.open_volume,
                    .open_price_sum = entry.open_price_sum,
                    .profit = entry.profit,
                    .pending_sell_size = entry.pending_sell_size,
                    .pending_buy_size = entry.pending_buy_size,
                };
                try position.validateInternalState();
                try profile.positions.put(engine.allocator, entry.symbol, position);
            }
            try engine.user_profile_service.user_profiles.put(engine.allocator, snapshot_user.uid, profile);
        }
        for (self.last_prices.items) |entry| try engine.last_price_cache.put(engine.allocator, entry.symbol, .{
            .ask_price = entry.ask_price,
            .bid_price = entry.bid_price,
        });
        for (self.fees.items) |entry| try engine.fees.put(engine.allocator, entry.key, entry.value);
        for (self.adjustments.items) |entry| try engine.adjustments.put(engine.allocator, entry.key, entry.value);
        for (self.suspends.items) |entry| try engine.suspends.put(engine.allocator, entry.key, entry.value);
        try restoreRiskHistories(self, engine);
        try assembler.restorePendingState(self.pending.transfers);
        if (try engine.stateHash() != self.risk_state_hash) return error.RiskStateHashMismatch;
        if (try assembler.stateHash() != self.binary_state_hash) return error.BinaryStateHashMismatch;
    }

    pub fn deinit(self: *RiskState) void {
        self.symbols.deinit(self.allocator);
        for (self.users.items) |*snapshot_user| snapshot_user.deinit(self.allocator);
        self.users.deinit(self.allocator);
        self.user_profiles_history.deinit(self.allocator);
        for (self.user_histories.items) |*histories| histories.deinit(self.allocator);
        self.user_histories.deinit(self.allocator);
        self.last_prices.deinit(self.allocator);
        self.fees.deinit(self.allocator);
        self.fees_history.deinit(self.allocator);
        self.adjustments.deinit(self.allocator);
        self.adjustments_history.deinit(self.allocator);
        self.suspends.deinit(self.allocator);
        self.suspends_history.deinit(self.allocator);
        self.pending.deinit();
        self.* = undefined;
    }

    fn initEmpty(allocator: std.mem.Allocator) std.mem.Allocator.Error!RiskState {
        return .{
            .allocator = allocator,
            .risk_state_hash = 0,
            .binary_state_hash = 0,
            .user_profiles_history = long_map_order.History.initUnavailable(),
            .fees_history = int_map_order.History.initUnavailable(),
            .adjustments_history = int_map_order.History.initUnavailable(),
            .suspends_history = int_map_order.History.initUnavailable(),
            .pending = .{
                .allocator = allocator,
                .transfers = try allocator.alloc(binary_frames.PendingTransfer, 0),
            },
        };
    }
};

pub fn matchingBooksStateHash(kernel: *const kernel_mod.ExchangeKernel) !u64 {
    var symbols: std.ArrayListUnmanaged(i32) = .empty;
    defer symbols.deinit(kernel.allocator);
    try symbols.ensureTotalCapacityPrecise(kernel.allocator, kernel.books.count());
    var it = kernel.books.keyIterator();
    while (it.next()) |symbol| symbols.appendAssumeCapacity(symbol.*);
    std.sort.heap(i32, symbols.items, {}, lessThanI32);

    var hasher = std.hash.Wyhash.init(0);
    try hashCanonicalCount(&hasher, symbols.items.len);
    var orders: std.ArrayListUnmanaged(kernel_mod.RestingOrder) = .empty;
    defer orders.deinit(kernel.allocator);
    for (symbols.items) |symbol| {
        const spec = bookSpecification(kernel, symbol) orelse return error.StateCorrupted;
        try hashCanonicalSymbolSpec(&hasher, spec);
        orders.clearRetainingCapacity();
        try kernel.appendOrdersForSymbol(kernel.allocator, symbol, &orders);
        try hashCanonicalCount(&hasher, orders.items.len);
        for (orders.items) |order| try hashCanonicalRestingOrder(&hasher, order);
    }
    return hasher.final();
}

/// Fingerprints the exact canonical bytes represented by the matching-state
/// book section: book count, sorted symbol specifications, per-book order
/// counts, and every resting-order record in serialized/FIFO order.
fn matchingBookSnapshotsStateHash(books: []const snapshot.BookSnapshot) FormatError!u64 {
    var hasher = std.hash.Wyhash.init(0);
    try hashCanonicalCount(&hasher, books.len);
    for (books) |book| {
        try hashCanonicalSymbolSpec(&hasher, book.symbol_spec);
        try hashCanonicalCount(&hasher, book.orders.items.len);
        for (book.orders.items) |order| try hashCanonicalRestingOrder(&hasher, order);
    }
    return hasher.final();
}

fn hashCanonicalCount(hasher: *std.hash.Wyhash, count: usize) FormatError!void {
    const value = std.math.cast(u32, count) orelse return error.LimitExceeded;
    var bytes: [@sizeOf(u32)]u8 = undefined;
    std.mem.writeInt(u32, &bytes, value, .little);
    hasher.update(&bytes);
}

fn hashCanonicalSymbolSpec(
    hasher: *std.hash.Wyhash,
    spec: common.CoreSymbolSpecification,
) FormatError!void {
    var bytes: [symbol_spec_len]u8 = undefined;
    var writer = std.Io.Writer.fixed(&bytes);
    try writeSymbolSpec(&writer, spec);
    if (writer.end != bytes.len) return error.EncodingLengthMismatch;
    hasher.update(&bytes);
}

fn hashCanonicalRestingOrder(
    hasher: *std.hash.Wyhash,
    order: kernel_mod.RestingOrder,
) FormatError!void {
    var bytes: [resting_order_len]u8 = undefined;
    var writer = std.Io.Writer.fixed(&bytes);
    try writeRestingOrder(&writer, order);
    if (writer.end != bytes.len) return error.EncodingLengthMismatch;
    hasher.update(&bytes);
}

fn captureRiskSymbols(result: *RiskState, engine: *const risk_mod.RiskEngine, limits: Limits) !void {
    const symbols = &engine.symbol_spec_provider.symbols;
    if (symbols.count() > limits.max_symbols) return error.LimitExceeded;
    var ids: std.ArrayListUnmanaged(i32) = .empty;
    defer ids.deinit(result.allocator);
    try ids.ensureTotalCapacityPrecise(result.allocator, symbols.count());
    var it = symbols.keyIterator();
    while (it.next()) |id| ids.appendAssumeCapacity(id.*);
    std.sort.heap(i32, ids.items, {}, lessThanI32);
    try result.symbols.ensureTotalCapacityPrecise(result.allocator, ids.items.len);
    for (ids.items) |id| result.symbols.appendAssumeCapacity(
        symbols.get(id) orelse return error.StateCorrupted,
    );
}

fn captureRiskUsers(result: *RiskState, engine: *const risk_mod.RiskEngine, limits: Limits) !void {
    const profiles = &engine.user_profile_service.user_profiles;
    if (profiles.count() > limits.max_users) return error.LimitExceeded;
    var uids: std.ArrayListUnmanaged(i64) = .empty;
    defer uids.deinit(result.allocator);
    try uids.ensureTotalCapacityPrecise(result.allocator, profiles.count());
    var it = profiles.keyIterator();
    while (it.next()) |uid| uids.appendAssumeCapacity(uid.*);
    std.sort.heap(i64, uids.items, {}, lessThanI64);
    try result.users.ensureTotalCapacityPrecise(result.allocator, uids.items.len);

    var total_user_entries: usize = 0;
    for (uids.items) |uid| {
        const profile = profiles.getPtr(uid) orelse return error.StateCorrupted;
        if (profile.accounts.count() > limits.max_accounts_per_user or
            profile.positions.count() > limits.max_positions_per_user)
        {
            return error.LimitExceeded;
        }
        total_user_entries = std.math.add(usize, total_user_entries, profile.accounts.count()) catch
            return error.LimitExceeded;
        total_user_entries = std.math.add(usize, total_user_entries, profile.positions.count()) catch
            return error.LimitExceeded;
        if (total_user_entries > limits.max_total_user_entries) return error.LimitExceeded;

        var captured: snapshot.UserSnapshot = .{
            .uid = uid,
            .user_status = profile.user_status,
            .adjustments_counter = profile.adjustments_counter,
        };
        errdefer captured.deinit(result.allocator);
        try captureI32I64Map(result.allocator, &captured.accounts, &profile.accounts, limits.max_accounts_per_user);

        var symbols: std.ArrayListUnmanaged(i32) = .empty;
        defer symbols.deinit(result.allocator);
        try symbols.ensureTotalCapacityPrecise(result.allocator, profile.positions.count());
        var pos_it = profile.positions.keyIterator();
        while (pos_it.next()) |symbol| symbols.appendAssumeCapacity(symbol.*);
        std.sort.heap(i32, symbols.items, {}, lessThanI32);
        try captured.positions.ensureTotalCapacityPrecise(result.allocator, symbols.items.len);
        for (symbols.items) |symbol| {
            const position = profile.positions.get(symbol) orelse return error.StateCorrupted;
            captured.positions.appendAssumeCapacity(.{
                .symbol = position.symbol,
                .currency = position.currency,
                .direction = position.direction,
                .open_volume = position.open_volume,
                .open_price_sum = position.open_price_sum,
                .profit = position.profit,
                .pending_sell_size = position.pending_sell_size,
                .pending_buy_size = position.pending_buy_size,
            });
        }
        result.users.appendAssumeCapacity(captured);
    }
}

fn captureRiskHistories(result: *RiskState, engine: *const risk_mod.RiskEngine) !void {
    var outer = try engine.user_profile_service.cloneUserProfilesHistory(result.allocator);
    errdefer outer.deinit(result.allocator);

    try result.user_histories.ensureTotalCapacityPrecise(
        result.allocator,
        result.users.items.len,
    );
    for (result.users.items) |snapshot_user| {
        const profile = engine.user_profile_service.user_profiles.getPtr(snapshot_user.uid) orelse
            return error.StateCorrupted;
        var accounts = try profile.cloneAccountsHistory(result.allocator);
        errdefer accounts.deinit(result.allocator);
        var positions = try profile.clonePositionsHistory(result.allocator);
        errdefer positions.deinit(result.allocator);
        result.user_histories.appendAssumeCapacity(.{
            .uid = snapshot_user.uid,
            .accounts = accounts,
            .positions = positions,
        });
        accounts = int_map_order.History.initUnavailable();
        positions = int_map_order.History.initUnavailable();
    }

    var fees = try engine.cloneFeesHistory(result.allocator);
    errdefer fees.deinit(result.allocator);
    var adjustments = try engine.cloneAdjustmentsHistory(result.allocator);
    errdefer adjustments.deinit(result.allocator);
    var suspends = try engine.cloneSuspendsHistory(result.allocator);
    errdefer suspends.deinit(result.allocator);

    result.user_profiles_history.deinit(result.allocator);
    result.user_profiles_history = outer;
    outer = long_map_order.History.initUnavailable();
    result.fees_history.deinit(result.allocator);
    result.fees_history = fees;
    fees = int_map_order.History.initUnavailable();
    result.adjustments_history.deinit(result.allocator);
    result.adjustments_history = adjustments;
    adjustments = int_map_order.History.initUnavailable();
    result.suspends_history.deinit(result.allocator);
    result.suspends_history = suspends;
    suspends = int_map_order.History.initUnavailable();
}

fn initializeUnavailableRiskHistories(state: *RiskState) std.mem.Allocator.Error!void {
    try state.user_histories.ensureTotalCapacityPrecise(
        state.allocator,
        state.users.items.len,
    );
    for (state.users.items) |snapshot_user| {
        state.user_histories.appendAssumeCapacity(.{
            .uid = snapshot_user.uid,
            .accounts = int_map_order.History.initUnavailable(),
            .positions = int_map_order.History.initUnavailable(),
        });
    }
}

fn restoreRiskHistories(state: *const RiskState, engine: *risk_mod.RiskEngine) !void {
    try validateRiskHistories(state, engine.allocator);

    if (state.user_profiles_history.isKnown()) {
        var outer = try state.user_profiles_history.clone(engine.allocator);
        defer outer.deinit(engine.allocator);
        try engine.user_profile_service.replaceUserProfilesHistory(&outer);
    } else {
        engine.user_profile_service.markUserProfilesHistoryUnavailable();
    }

    for (state.user_histories.items) |histories| {
        const profile = engine.user_profile_service.user_profiles.getPtr(histories.uid) orelse
            return error.HistorySemanticKeySetMismatch;
        if (histories.accounts.isKnown()) {
            var accounts = try histories.accounts.clone(engine.allocator);
            defer accounts.deinit(engine.allocator);
            try profile.replaceAccountsHistory(engine.allocator, &accounts);
        } else {
            profile.markAccountsHistoryUnavailable(engine.allocator);
        }
        if (histories.positions.isKnown()) {
            var positions = try histories.positions.clone(engine.allocator);
            defer positions.deinit(engine.allocator);
            try profile.replacePositionsHistory(engine.allocator, &positions);
        } else {
            profile.markPositionsHistoryUnavailable(engine.allocator);
        }
    }

    if (state.fees_history.isKnown()) {
        var fees = try state.fees_history.clone(engine.allocator);
        defer fees.deinit(engine.allocator);
        try engine.replaceFeesHistory(&fees);
    } else {
        markRiskHistoryUnavailable(engine, .fees);
    }
    if (state.adjustments_history.isKnown()) {
        var adjustments = try state.adjustments_history.clone(engine.allocator);
        defer adjustments.deinit(engine.allocator);
        try engine.replaceAdjustmentsHistory(&adjustments);
    } else {
        markRiskHistoryUnavailable(engine, .adjustments);
    }
    if (state.suspends_history.isKnown()) {
        var suspends = try state.suspends_history.clone(engine.allocator);
        defer suspends.deinit(engine.allocator);
        try engine.replaceSuspendsHistory(&suspends);
    } else {
        markRiskHistoryUnavailable(engine, .suspends);
    }
}

const RiskHistoryKind = enum { fees, adjustments, suspends };

fn markRiskHistoryUnavailable(engine: *risk_mod.RiskEngine, kind: RiskHistoryKind) void {
    const history = switch (kind) {
        .fees => &engine.fees_history,
        .adjustments => &engine.adjustments_history,
        .suspends => &engine.suspends_history,
    };
    if (!history.isKnown()) return;
    history.deinit(engine.allocator);
    history.* = int_map_order.History.initUnavailable();
}

fn captureLastPrices(result: *RiskState, engine: *const risk_mod.RiskEngine, limits: Limits) !void {
    if (engine.last_price_cache.count() > limits.max_map_entries) return error.LimitExceeded;
    var symbols: std.ArrayListUnmanaged(i32) = .empty;
    defer symbols.deinit(result.allocator);
    try symbols.ensureTotalCapacityPrecise(result.allocator, engine.last_price_cache.count());
    var it = engine.last_price_cache.keyIterator();
    while (it.next()) |symbol| symbols.appendAssumeCapacity(symbol.*);
    std.sort.heap(i32, symbols.items, {}, lessThanI32);
    try result.last_prices.ensureTotalCapacityPrecise(result.allocator, symbols.items.len);
    for (symbols.items) |symbol| {
        const value = engine.last_price_cache.get(symbol) orelse return error.StateCorrupted;
        result.last_prices.appendAssumeCapacity(.{
            .symbol = symbol,
            .ask_price = value.ask_price,
            .bid_price = value.bid_price,
        });
    }
}

fn captureI32I64Map(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(snapshot.I32I64Entry),
    map: *const std.AutoHashMapUnmanaged(i32, i64),
    max_count: usize,
) !void {
    if (map.count() > max_count) return error.LimitExceeded;
    var keys: std.ArrayListUnmanaged(i32) = .empty;
    defer keys.deinit(allocator);
    try keys.ensureTotalCapacityPrecise(allocator, map.count());
    var it = map.keyIterator();
    while (it.next()) |key| keys.appendAssumeCapacity(key.*);
    std.sort.heap(i32, keys.items, {}, lessThanI32);
    try out.ensureTotalCapacityPrecise(allocator, keys.items.len);
    for (keys.items) |key| out.appendAssumeCapacity(.{
        .key = key,
        .value = map.get(key) orelse return error.StateCorrupted,
    });
}

fn validateRiskState(
    state: *const RiskState,
    allocator: std.mem.Allocator,
    limits: Limits,
) (std.mem.Allocator.Error || FormatError)!void {
    if (state.symbols.items.len > limits.max_symbols or state.users.items.len > limits.max_users or
        state.last_prices.items.len > limits.max_map_entries or state.fees.items.len > limits.max_map_entries or
        state.adjustments.items.len > limits.max_map_entries or state.suspends.items.len > limits.max_map_entries)
    {
        return error.LimitExceeded;
    }
    try validateSymbols(state.symbols.items);
    var prior_uid: ?i64 = null;
    var total_user_entries: usize = 0;
    for (state.users.items) |snapshot_user| {
        if (prior_uid) |prior| {
            if (snapshot_user.uid == prior) return error.DuplicateUser;
            if (snapshot_user.uid < prior) return error.NonCanonicalOrder;
        }
        prior_uid = snapshot_user.uid;
        if (snapshot_user.accounts.items.len > limits.max_accounts_per_user or
            snapshot_user.positions.items.len > limits.max_positions_per_user)
        {
            return error.LimitExceeded;
        }
        total_user_entries = std.math.add(usize, total_user_entries, snapshot_user.accounts.items.len) catch
            return error.LimitExceeded;
        total_user_entries = std.math.add(usize, total_user_entries, snapshot_user.positions.items.len) catch
            return error.LimitExceeded;
        if (total_user_entries > limits.max_total_user_entries) return error.LimitExceeded;
        try validateI32I64Entries(snapshot_user.accounts.items);
        try validatePositions(snapshot_user.uid, snapshot_user.positions.items, state.symbols.items);
    }
    try validateLastPrices(state.last_prices.items);
    try validateI32I64Entries(state.fees.items);
    try validateI32I64Entries(state.adjustments.items);
    try validateI32I64Entries(state.suspends.items);
    try validateRiskHistories(state, allocator);
    try validatePending(state.pending.transfers, limits);
}

fn validateMatchingHistory(
    state: *const MatchingState,
    allocator: std.mem.Allocator,
) (std.mem.Allocator.Error || FormatError)!void {
    if (!state.books_history.isKnown()) return;
    const keys = state.books_history.orderedKeys(allocator) catch |err|
        return mapHistoryValidationError(err);
    defer allocator.free(keys);
    if (keys.len != state.books.items.len) return error.HistorySemanticKeySetMismatch;
    for (keys) |key| {
        if (!containsBook(state.books.items, key)) {
            return error.HistorySemanticKeySetMismatch;
        }
    }
}

fn validateRiskHistories(
    state: *const RiskState,
    allocator: std.mem.Allocator,
) (std.mem.Allocator.Error || FormatError)!void {
    if (state.user_histories.items.len != state.users.items.len) {
        return error.HistorySemanticKeySetMismatch;
    }
    try validateLongHistoryAgainstUsers(
        allocator,
        &state.user_profiles_history,
        state.users.items,
    );
    for (state.users.items, state.user_histories.items) |snapshot_user, histories| {
        if (histories.uid != snapshot_user.uid) {
            return error.HistorySemanticKeySetMismatch;
        }
        try validateIntHistoryAgainstEntries(
            allocator,
            &histories.accounts,
            snapshot_user.accounts.items,
        );
        try validateIntHistoryAgainstPositions(
            allocator,
            &histories.positions,
            snapshot_user.positions.items,
        );
    }
    try validateIntHistoryAgainstEntries(allocator, &state.fees_history, state.fees.items);
    try validateIntHistoryAgainstEntries(
        allocator,
        &state.adjustments_history,
        state.adjustments.items,
    );
    try validateIntHistoryAgainstEntries(
        allocator,
        &state.suspends_history,
        state.suspends.items,
    );
}

fn validateLongHistoryAgainstUsers(
    allocator: std.mem.Allocator,
    history: *const long_map_order.History,
    users: []const snapshot.UserSnapshot,
) (std.mem.Allocator.Error || FormatError)!void {
    if (!history.isKnown()) return;
    const keys = history.orderedKeys(allocator) catch |err|
        return mapHistoryValidationError(err);
    defer allocator.free(keys);
    if (keys.len != users.len) return error.HistorySemanticKeySetMismatch;
    for (keys) |uid| {
        if (!containsUser(users, uid)) return error.HistorySemanticKeySetMismatch;
    }
}

fn validateIntHistoryAgainstEntries(
    allocator: std.mem.Allocator,
    history: *const int_map_order.History,
    entries: []const snapshot.I32I64Entry,
) (std.mem.Allocator.Error || FormatError)!void {
    if (!history.isKnown()) return;
    const keys = history.orderedKeys(allocator) catch |err|
        return mapHistoryValidationError(err);
    defer allocator.free(keys);
    if (keys.len != entries.len) return error.HistorySemanticKeySetMismatch;
    for (keys) |key| {
        if (!containsI32I64Entry(entries, key)) {
            return error.HistorySemanticKeySetMismatch;
        }
    }
}

fn validateIntHistoryAgainstPositions(
    allocator: std.mem.Allocator,
    history: *const int_map_order.History,
    positions: []const snapshot.PositionSnapshot,
) (std.mem.Allocator.Error || FormatError)!void {
    if (!history.isKnown()) return;
    const keys = history.orderedKeys(allocator) catch |err|
        return mapHistoryValidationError(err);
    defer allocator.free(keys);
    if (keys.len != positions.len) return error.HistorySemanticKeySetMismatch;
    for (keys) |key| {
        if (!containsPosition(positions, key)) {
            return error.HistorySemanticKeySetMismatch;
        }
    }
}

fn mapHistoryValidationError(err: int_map_order.HistoryError) error{
    OutOfMemory,
    LimitExceeded,
    InvalidFormat,
}!void {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.CapacityOverflow => error.LimitExceeded,
        error.HistoryUnavailable => error.InvalidFormat,
        error.InvalidTableSize,
        error.InvalidFlag,
        error.InvalidOccupancy,
        error.DuplicateActiveKey,
        error.UnreachableActiveKey,
        => error.InvalidFormat,
    };
}

fn validateSymbols(symbols: []const common.CoreSymbolSpecification) FormatError!void {
    var prior: ?i32 = null;
    for (symbols) |spec| {
        spec.validate() catch return error.InvalidFormat;
        if (prior) |value| {
            if (spec.symbol_id == value) return error.DuplicateSymbol;
            if (spec.symbol_id < value) return error.NonCanonicalOrder;
        }
        prior = spec.symbol_id;
    }
}

fn validateBooks(
    allocator: std.mem.Allocator,
    books: []const snapshot.BookSnapshot,
    limits: Limits,
) (std.mem.Allocator.Error || FormatError)!void {
    if (books.len > limits.max_symbols) return error.LimitExceeded;
    var prior: ?i32 = null;
    var total_orders: usize = 0;
    for (books) |book| {
        if (prior) |value| {
            if (book.symbol_spec.symbol_id == value) return error.DuplicateSymbol;
            if (book.symbol_spec.symbol_id < value) return error.NonCanonicalOrder;
        }
        prior = book.symbol_spec.symbol_id;
        if (book.orders.items.len > limits.max_orders_per_book) return error.LimitExceeded;
        total_orders = std.math.add(usize, total_orders, book.orders.items.len) catch return error.LimitExceeded;
        if (total_orders > limits.max_total_orders) return error.LimitExceeded;
        try validateBook(allocator, book);
    }
}

fn validateBook(
    allocator: std.mem.Allocator,
    book: snapshot.BookSnapshot,
) (std.mem.Allocator.Error || FormatError)!void {
    book.symbol_spec.validate() catch return error.InvalidFormat;
    var seen = std.AutoHashMapUnmanaged(i64, void){};
    defer seen.deinit(allocator);
    var phase: enum { asks, bids } = .asks;
    var prior_price: ?i64 = null;
    var prior_action: ?common.OrderAction = null;
    for (book.orders.items) |order| {
        if (order.symbol != book.symbol_spec.symbol_id or order.size <= 0 or
            order.filled < 0 or order.filled >= order.size)
        {
            return error.InvalidOrder;
        }
        const gop = try seen.getOrPut(allocator, order.order_id);
        if (gop.found_existing) return error.DuplicateOrderId;
        if (prior_action) |action| {
            if (action != order.action) {
                if (action != .ask or order.action != .bid) return error.NonCanonicalOrder;
                phase = .bids;
                prior_price = null;
            }
        } else if (order.action == .bid) {
            phase = .bids;
        }
        if (phase == .asks and order.action != .ask) return error.NonCanonicalOrder;
        if (phase == .bids and order.action != .bid) return error.NonCanonicalOrder;
        if (prior_price) |price| {
            if ((phase == .asks and order.price < price) or (phase == .bids and order.price > price)) {
                return error.NonCanonicalOrder;
            }
        }
        prior_price = order.price;
        prior_action = order.action;
    }
}

fn validatePositions(
    uid: i64,
    positions: []const snapshot.PositionSnapshot,
    symbols: []const common.CoreSymbolSpecification,
) FormatError!void {
    var prior: ?i32 = null;
    for (positions) |entry| {
        if (prior) |value| {
            if (entry.symbol == value) return error.DuplicateKey;
            if (entry.symbol < value) return error.NonCanonicalOrder;
        }
        prior = entry.symbol;
        if (!containsSymbol(symbols, entry.symbol)) return error.PositionSymbolMissing;
        const record: user.SymbolPositionRecord = .{
            .uid = uid,
            .symbol = entry.symbol,
            .currency = entry.currency,
            .direction = entry.direction,
            .open_volume = entry.open_volume,
            .open_price_sum = entry.open_price_sum,
            .profit = entry.profit,
            .pending_sell_size = entry.pending_sell_size,
            .pending_buy_size = entry.pending_buy_size,
        };
        record.validateInternalState() catch return error.InvalidUser;
    }
}

fn validateLastPrices(entries: []const snapshot.LastPriceCacheEntry) FormatError!void {
    var prior: ?i32 = null;
    for (entries) |entry| {
        if (prior) |value| {
            if (entry.symbol == value) return error.DuplicateKey;
            if (entry.symbol < value) return error.NonCanonicalOrder;
        }
        prior = entry.symbol;
    }
}

fn validateI32I64Entries(entries: []const snapshot.I32I64Entry) FormatError!void {
    var prior: ?i32 = null;
    for (entries) |entry| {
        if (prior) |value| {
            if (entry.key == value) return error.DuplicateKey;
            if (entry.key < value) return error.NonCanonicalOrder;
        }
        prior = entry.key;
    }
}

fn validatePending(transfers: []const binary_frames.PendingTransfer, limits: Limits) FormatError!void {
    if (transfers.len > limits.max_transfers) return error.LimitExceeded;
    var prior: ?i32 = null;
    var total_words: usize = 0;
    for (transfers) |transfer| {
        if (prior) |value| {
            if (transfer.transfer_id == value) return error.DuplicateKey;
            if (transfer.transfer_id < value) return error.NonCanonicalOrder;
        }
        prior = transfer.transfer_id;
        if (transfer.command_type) |command_type| {
            switch (command_type) {
                .binary_data_command, .binary_data_query => {},
                else => return error.InvalidTransfer,
            }
        }
        if (transfer.words.len == 0 or transfer.words.len % binary_frames.FrameWordsPerCommand != 0) {
            return error.InvalidTransfer;
        }
        if (transfer.words.len > limits.max_words_per_transfer) return error.LimitExceeded;
        total_words = std.math.add(usize, total_words, transfer.words.len) catch return error.LimitExceeded;
        if (total_words > limits.max_total_transfer_words) return error.LimitExceeded;
    }
}

fn isSupportedBodySchema(schema_version: u16) bool {
    return schema_version >= legacy_body_schema_version and
        schema_version <= body_schema_version;
}

fn matchingEncodedLength(state: *const MatchingState, limits: Limits) FormatError!usize {
    var len = matching_body_header_len;
    for (state.books.items) |book| {
        len = try addLen(len, symbol_spec_len + @sizeOf(u32), limits.max_body_bytes);
        len = try addItemsLen(len, book.orders.items.len, resting_order_len, limits.max_body_bytes);
    }
    len = try addHistoryLength(
        len,
        intHistoryEncodedLength(&state.books_history, limits),
        limits.max_body_bytes,
    );
    len = try addLen(len, @sizeOf(u32), limits.max_body_bytes);
    return pendingItemsLength(len, state.pending.transfers, limits.max_body_bytes);
}

fn riskEncodedLength(state: *const RiskState, limits: Limits) FormatError!usize {
    var len = risk_body_header_len;
    len = try addItemsLen(len, state.symbols.items.len, symbol_spec_len, limits.max_body_bytes);
    len = try addLen(len, @sizeOf(u32), limits.max_body_bytes);
    for (state.users.items) |snapshot_user| {
        len = try addLen(len, 21, limits.max_body_bytes);
        len = try addItemsLen(len, snapshot_user.accounts.items.len, i32_i64_entry_len, limits.max_body_bytes);
        len = try addLen(len, @sizeOf(u32), limits.max_body_bytes);
        len = try addItemsLen(len, snapshot_user.positions.items.len, position_len, limits.max_body_bytes);
    }
    len = try addLen(len, @sizeOf(u32), limits.max_body_bytes);
    len = try addItemsLen(len, state.last_prices.items.len, last_price_len, limits.max_body_bytes);
    len = try addLen(len, @sizeOf(u32), limits.max_body_bytes);
    len = try addItemsLen(len, state.fees.items.len, i32_i64_entry_len, limits.max_body_bytes);
    len = try addLen(len, @sizeOf(u32), limits.max_body_bytes);
    len = try addItemsLen(len, state.adjustments.items.len, i32_i64_entry_len, limits.max_body_bytes);
    len = try addLen(len, @sizeOf(u32), limits.max_body_bytes);
    len = try addItemsLen(len, state.suspends.items.len, i32_i64_entry_len, limits.max_body_bytes);
    len = try addHistoryLength(
        len,
        longHistoryEncodedLength(&state.user_profiles_history, limits),
        limits.max_body_bytes,
    );
    len = try addLen(len, @sizeOf(u32), limits.max_body_bytes);
    for (state.user_histories.items) |histories| {
        len = try addLen(len, @sizeOf(i64), limits.max_body_bytes);
        len = try addHistoryLength(
            len,
            intHistoryEncodedLength(&histories.accounts, limits),
            limits.max_body_bytes,
        );
        len = try addHistoryLength(
            len,
            intHistoryEncodedLength(&histories.positions, limits),
            limits.max_body_bytes,
        );
    }
    len = try addHistoryLength(
        len,
        intHistoryEncodedLength(&state.fees_history, limits),
        limits.max_body_bytes,
    );
    len = try addHistoryLength(
        len,
        intHistoryEncodedLength(&state.adjustments_history, limits),
        limits.max_body_bytes,
    );
    len = try addHistoryLength(
        len,
        intHistoryEncodedLength(&state.suspends_history, limits),
        limits.max_body_bytes,
    );
    len = try addLen(len, @sizeOf(u32), limits.max_body_bytes);
    return pendingItemsLength(len, state.pending.transfers, limits.max_body_bytes);
}

fn pendingItemsLength(start: usize, transfers: []const binary_frames.PendingTransfer, max: usize) FormatError!usize {
    var len = start;
    for (transfers) |transfer| {
        len = try addLen(len, transfer_header_len, max);
        len = try addItemsLen(len, transfer.words.len, @sizeOf(i64), max);
    }
    return len;
}

fn addLen(current: usize, extra: usize, max: usize) FormatError!usize {
    const next = std.math.add(usize, current, extra) catch return error.LimitExceeded;
    if (next > max) return error.LimitExceeded;
    return next;
}

fn addItemsLen(current: usize, count: usize, item_len: usize, max: usize) FormatError!usize {
    const bytes = std.math.mul(usize, count, item_len) catch return error.LimitExceeded;
    return addLen(current, bytes, max);
}

fn addHistoryLength(current: usize, history_len: FormatError!usize, max: usize) FormatError!usize {
    var len = try addLen(current, @sizeOf(u32), max);
    len = try addLen(len, try history_len, max);
    return len;
}

fn historyCodecLimits(limits: Limits) history_codec.Limits {
    return .{ .max_slots = limits.max_history_slots };
}

fn intHistoryEncodedLength(
    history: *const int_map_order.History,
    limits: Limits,
) FormatError!usize {
    const len = history_codec.encodedIntLength(history, historyCodecLimits(limits)) catch |err| switch (err) {
        error.OutOfMemory => unreachable,
        else => return mapHistoryCodecError(err),
    };
    if (len > std.math.maxInt(u32)) return error.LimitExceeded;
    return len;
}

fn longHistoryEncodedLength(
    history: *const long_map_order.History,
    limits: Limits,
) FormatError!usize {
    const len = history_codec.encodedLongLength(history, historyCodecLimits(limits)) catch |err| switch (err) {
        error.OutOfMemory => unreachable,
        else => return mapHistoryCodecError(err),
    };
    if (len > std.math.maxInt(u32)) return error.LimitExceeded;
    return len;
}

const MappedHistoryCodecError = error{
    LimitExceeded,
    InvalidFormat,
    Truncated,
};

fn mapHistoryCodecError(err: history_codec.CodecError) MappedHistoryCodecError {
    return switch (err) {
        error.OutOfMemory => unreachable,
        error.SlotLimitExceeded,
        error.LengthOverflow,
        error.CapacityOverflow,
        => error.LimitExceeded,
        error.Truncated => error.Truncated,
        error.UnknownTag,
        error.TrailingBytes,
        error.InvalidTableSize,
        error.InvalidFlag,
        error.InvalidOccupancy,
        error.DuplicateActiveKey,
        error.UnreachableActiveKey,
        => error.InvalidFormat,
    };
}

fn writeIntHistory(
    writer: *std.Io.Writer,
    allocator: std.mem.Allocator,
    history: *const int_map_order.History,
    limits: Limits,
) (std.mem.Allocator.Error || FormatError)!void {
    const bytes = history_codec.encodeIntAlloc(
        allocator,
        history,
        historyCodecLimits(limits),
    ) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return mapHistoryCodecError(err),
    };
    defer allocator.free(bytes);
    try writeCount(writer, bytes.len);
    try writeBytes(writer, bytes);
}

fn writeLongHistory(
    writer: *std.Io.Writer,
    allocator: std.mem.Allocator,
    history: *const long_map_order.History,
    limits: Limits,
) (std.mem.Allocator.Error || FormatError)!void {
    const bytes = history_codec.encodeLongAlloc(
        allocator,
        history,
        historyCodecLimits(limits),
    ) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return mapHistoryCodecError(err),
    };
    defer allocator.free(bytes);
    try writeCount(writer, bytes.len);
    try writeBytes(writer, bytes);
}

fn readHistoryBlob(reader: *std.Io.Reader, limits: Limits) FormatError![]const u8 {
    const len: usize = try readInt(u32, reader);
    if (len == 0) return error.InvalidFormat;
    if (len > limits.max_body_bytes) return error.LimitExceeded;
    if (len > reader.end - reader.seek) return error.Truncated;
    const start = reader.seek;
    reader.seek += len;
    return reader.buffer[start..reader.seek];
}

fn readIntHistory(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    limits: Limits,
) (std.mem.Allocator.Error || FormatError)!int_map_order.History {
    const bytes = try readHistoryBlob(reader, limits);
    return history_codec.decodeIntExactAlloc(
        allocator,
        bytes,
        historyCodecLimits(limits),
    ) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return mapHistoryCodecError(err),
    };
}

fn readLongHistory(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    limits: Limits,
) (std.mem.Allocator.Error || FormatError)!long_map_order.History {
    const bytes = try readHistoryBlob(reader, limits);
    return history_codec.decodeLongExactAlloc(
        allocator,
        bytes,
        historyCodecLimits(limits),
    ) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return mapHistoryCodecError(err),
    };
}

fn writeRiskHistories(
    state: *const RiskState,
    writer: *std.Io.Writer,
    allocator: std.mem.Allocator,
    limits: Limits,
) (std.mem.Allocator.Error || FormatError)!void {
    try writeLongHistory(writer, allocator, &state.user_profiles_history, limits);
    try writeCount(writer, state.user_histories.items.len);
    for (state.user_histories.items) |histories| {
        try writeInt(writer, i64, histories.uid);
        try writeIntHistory(writer, allocator, &histories.accounts, limits);
        try writeIntHistory(writer, allocator, &histories.positions, limits);
    }
    try writeIntHistory(writer, allocator, &state.fees_history, limits);
    try writeIntHistory(writer, allocator, &state.adjustments_history, limits);
    try writeIntHistory(writer, allocator, &state.suspends_history, limits);
}

fn readRiskHistories(
    state: *RiskState,
    reader: *std.Io.Reader,
    limits: Limits,
) (std.mem.Allocator.Error || FormatError)!void {
    var outer = try readLongHistory(state.allocator, reader, limits);
    errdefer outer.deinit(state.allocator);
    const user_count = try readBoundedCount(reader, limits.max_users, @sizeOf(i64) + 10);
    if (user_count != state.users.items.len) {
        return error.HistorySemanticKeySetMismatch;
    }
    try state.user_histories.ensureTotalCapacityPrecise(state.allocator, user_count);
    for (state.users.items) |snapshot_user| {
        const uid = try readInt(i64, reader);
        if (uid != snapshot_user.uid) return error.HistorySemanticKeySetMismatch;
        var accounts = try readIntHistory(state.allocator, reader, limits);
        errdefer accounts.deinit(state.allocator);
        var positions = try readIntHistory(state.allocator, reader, limits);
        errdefer positions.deinit(state.allocator);
        state.user_histories.appendAssumeCapacity(.{
            .uid = uid,
            .accounts = accounts,
            .positions = positions,
        });
        accounts = int_map_order.History.initUnavailable();
        positions = int_map_order.History.initUnavailable();
    }
    var fees = try readIntHistory(state.allocator, reader, limits);
    errdefer fees.deinit(state.allocator);
    var adjustments = try readIntHistory(state.allocator, reader, limits);
    errdefer adjustments.deinit(state.allocator);
    var suspends = try readIntHistory(state.allocator, reader, limits);
    errdefer suspends.deinit(state.allocator);

    state.user_profiles_history.deinit(state.allocator);
    state.user_profiles_history = outer;
    outer = long_map_order.History.initUnavailable();
    state.fees_history.deinit(state.allocator);
    state.fees_history = fees;
    fees = int_map_order.History.initUnavailable();
    state.adjustments_history.deinit(state.allocator);
    state.adjustments_history = adjustments;
    adjustments = int_map_order.History.initUnavailable();
    state.suspends_history.deinit(state.allocator);
    state.suspends_history = suspends;
    suspends = int_map_order.History.initUnavailable();
    try validateRiskHistories(state, state.allocator);
}

fn readPending(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    limits: Limits,
    schema_version: u16,
) (std.mem.Allocator.Error || FormatError)!binary_frames.PendingState {
    const header_len = if (schema_version >= transfer_capacity_schema_version)
        transfer_header_len
    else
        legacy_transfer_header_len;
    const transfer_count = try readBoundedCount(reader, limits.max_transfers, header_len + binary_frames.FrameWordsPerCommand * @sizeOf(i64));
    const transfers = try allocator.alloc(binary_frames.PendingTransfer, transfer_count);
    var initialized: usize = 0;
    errdefer {
        for (transfers[0..initialized]) |*transfer| transfer.deinit(allocator);
        allocator.free(transfers);
    }
    var prior: ?i32 = null;
    var total_words: usize = 0;
    while (initialized < transfer_count) : (initialized += 1) {
        const transfer_id = try readInt(i32, reader);
        if (prior) |value| {
            if (transfer_id == value) return error.DuplicateKey;
            if (transfer_id < value) return error.NonCanonicalOrder;
        }
        prior = transfer_id;
        const command_type = binary_frames.pendingCommandTypeFromCode(
            try readInt(i8, reader),
        ) catch return error.InvalidTransfer;
        const raw_capacity = if (schema_version >= transfer_capacity_schema_version)
            try readInt(u32, reader)
        else
            0;
        const word_count = try readBoundedCount(reader, limits.max_words_per_transfer, @sizeOf(i64));
        if (word_count == 0 or word_count % binary_frames.FrameWordsPerCommand != 0) {
            return error.InvalidTransfer;
        }
        total_words = std.math.add(usize, total_words, word_count) catch return error.LimitExceeded;
        if (total_words > limits.max_total_transfer_words) return error.LimitExceeded;
        const words = try allocator.alloc(i64, word_count);
        errdefer allocator.free(words);
        for (words) |*word| word.* = try readInt(i64, reader);
        transfers[initialized] = .{
            .transfer_id = transfer_id,
            .command_type = command_type,
            .java_backing_capacity = if (raw_capacity == 0) null else raw_capacity,
            .words = words,
        };
    }
    return .{ .allocator = allocator, .transfers = transfers };
}

fn writePending(writer: *std.Io.Writer, transfers: []const binary_frames.PendingTransfer) FormatError!void {
    try writeCount(writer, transfers.len);
    for (transfers) |transfer| {
        try writeInt(writer, i32, transfer.transfer_id);
        try writeInt(writer, i8, binary_frames.pendingCommandTypeCode(transfer.command_type));
        try writeInt(writer, u32, transfer.java_backing_capacity orelse 0);
        try writeCount(writer, transfer.words.len);
        for (transfer.words) |word| try writeInt(writer, i64, word);
    }
}

fn validatePendingStateHash(
    allocator: std.mem.Allocator,
    transfers: []const binary_frames.PendingTransfer,
    expected: u64,
) (std.mem.Allocator.Error || FormatError)!void {
    var assembler = binary_frames.BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();
    assembler.restorePendingState(transfers) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidTransfer,
    };
    const actual = assembler.stateHash() catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidTransfer,
    };
    if (actual != expected) return error.BinaryStateHashMismatch;
}

fn readLastPrices(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    out: *std.ArrayListUnmanaged(snapshot.LastPriceCacheEntry),
    limits: Limits,
) (std.mem.Allocator.Error || FormatError)!void {
    const count = try readBoundedCount(reader, limits.max_map_entries, last_price_len);
    try out.ensureTotalCapacityPrecise(allocator, count);
    for (0..count) |_| out.appendAssumeCapacity(.{
        .symbol = try readInt(i32, reader),
        .ask_price = try readInt(i64, reader),
        .bid_price = try readInt(i64, reader),
    });
    try validateLastPrices(out.items);
}

fn readI32I64Entries(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    out: *std.ArrayListUnmanaged(snapshot.I32I64Entry),
    max_count: usize,
) (std.mem.Allocator.Error || FormatError)!void {
    const count = try readBoundedCount(reader, max_count, i32_i64_entry_len);
    try out.ensureTotalCapacityPrecise(allocator, count);
    for (0..count) |_| out.appendAssumeCapacity(try readI32I64Entry(reader));
    try validateI32I64Entries(out.items);
}

fn writeI32I64Entries(writer: *std.Io.Writer, entries: []const snapshot.I32I64Entry) FormatError!void {
    try writeCount(writer, entries.len);
    for (entries) |entry| try writeI32I64Entry(writer, entry);
}

fn readBoundedCount(reader: *std.Io.Reader, max_count: usize, min_item_len: usize) FormatError!usize {
    const count: usize = try readInt(u32, reader);
    if (count > max_count) return error.LimitExceeded;
    const remaining = reader.end - reader.seek;
    if (min_item_len != 0 and count > remaining / min_item_len) return error.Truncated;
    return count;
}

fn writeCount(writer: *std.Io.Writer, count: usize) FormatError!void {
    const value = std.math.cast(u32, count) orelse return error.LimitExceeded;
    try writeInt(writer, u32, value);
}

fn expectMagic(reader: *std.Io.Reader, magic: *const [4]u8) FormatError!void {
    const actual = reader.takeArray(4) catch return error.Truncated;
    if (!std.mem.eql(u8, actual, magic)) return error.InvalidMagic;
}

fn readInt(comptime T: type, reader: *std.Io.Reader) FormatError!T {
    return reader.takeInt(T, .little) catch return error.Truncated;
}

fn writeInt(writer: *std.Io.Writer, comptime T: type, value: T) FormatError!void {
    writer.writeInt(T, value, .little) catch return error.EncodingLengthMismatch;
}

fn writeBytes(writer: *std.Io.Writer, bytes: []const u8) FormatError!void {
    writer.writeAll(bytes) catch return error.EncodingLengthMismatch;
}

fn writeSymbolSpec(writer: *std.Io.Writer, spec: common.CoreSymbolSpecification) FormatError!void {
    snapshot.writeSymbolSpec(*std.Io.Writer, writer, spec) catch return error.EncodingLengthMismatch;
}

fn readSymbolSpec(reader: *std.Io.Reader) FormatError!common.CoreSymbolSpecification {
    return snapshot.readSymbolSpec(*std.Io.Reader, reader) catch |err| switch (err) {
        error.ReadFailed => error.Truncated,
        else => error.InvalidFormat,
    };
}

fn writeRestingOrder(writer: *std.Io.Writer, order: kernel_mod.RestingOrder) FormatError!void {
    snapshot.writeRestingOrder(*std.Io.Writer, writer, order) catch return error.EncodingLengthMismatch;
}

fn readRestingOrder(reader: *std.Io.Reader) FormatError!kernel_mod.RestingOrder {
    return snapshot.readRestingOrder(*std.Io.Reader, reader) catch |err| switch (err) {
        error.ReadFailed => error.Truncated,
        else => error.InvalidFormat,
    };
}

fn writeI32I64Entry(writer: *std.Io.Writer, entry: snapshot.I32I64Entry) FormatError!void {
    snapshot.writeI32I64Entry(*std.Io.Writer, writer, entry) catch return error.EncodingLengthMismatch;
}

fn readI32I64Entry(reader: *std.Io.Reader) FormatError!snapshot.I32I64Entry {
    return snapshot.readI32I64Entry(*std.Io.Reader, reader) catch |err| switch (err) {
        error.ReadFailed => error.Truncated,
        else => error.InvalidFormat,
    };
}

fn writePosition(writer: *std.Io.Writer, position: snapshot.PositionSnapshot) FormatError!void {
    snapshot.writePositionSnapshot(*std.Io.Writer, writer, position) catch return error.EncodingLengthMismatch;
}

fn readPosition(reader: *std.Io.Reader) FormatError!snapshot.PositionSnapshot {
    return snapshot.readPositionSnapshot(*std.Io.Reader, reader) catch |err| switch (err) {
        error.ReadFailed => error.Truncated,
        else => error.InvalidFormat,
    };
}

fn bookSpecification(kernel: *const kernel_mod.ExchangeKernel, symbol: i32) ?common.CoreSymbolSpecification {
    const book = kernel.books.get(symbol) orelse return null;
    return switch (book.*) {
        .naive => |value| value.symbol_spec,
        .direct => |value| value.symbol_spec,
    };
}

fn containsSymbol(symbols: []const common.CoreSymbolSpecification, symbol: i32) bool {
    var low: usize = 0;
    var high = symbols.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (symbols[mid].symbol_id < symbol) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low < symbols.len and symbols[low].symbol_id == symbol;
}

fn containsBook(books: []const snapshot.BookSnapshot, symbol: i32) bool {
    var low: usize = 0;
    var high = books.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (books[mid].symbol_spec.symbol_id < symbol) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low < books.len and books[low].symbol_spec.symbol_id == symbol;
}

fn containsUser(users: []const snapshot.UserSnapshot, uid: i64) bool {
    var low: usize = 0;
    var high = users.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (users[mid].uid < uid) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low < users.len and users[low].uid == uid;
}

fn findUserHistoryEncounter(
    encounters: []const UserHistoryEncounter,
    uid: i64,
) ?UserHistoryEncounter {
    var found: ?UserHistoryEncounter = null;
    for (encounters) |encounter| {
        if (encounter.uid != uid) continue;
        if (found != null) return null;
        found = encounter;
    }
    return found;
}

fn containsI32I64Entry(entries: []const snapshot.I32I64Entry, key: i32) bool {
    var low: usize = 0;
    var high = entries.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (entries[mid].key < key) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low < entries.len and entries[low].key == key;
}

fn containsPosition(positions: []const snapshot.PositionSnapshot, symbol: i32) bool {
    var low: usize = 0;
    var high = positions.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (positions[mid].symbol < symbol) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low < positions.len and positions[low].symbol == symbol;
}

fn matchingTargetIsFresh(kernel: *const kernel_mod.ExchangeKernel) bool {
    return kernel.books.count() == 0 and kernel.binary_frame_assembler.pendingTransfers() == 0 and
        riskEngineIsEmpty(&kernel.risk_engine);
}

fn riskEngineIsEmpty(engine: *const risk_mod.RiskEngine) bool {
    return engine.symbol_spec_provider.symbols.count() == 0 and
        engine.user_profile_service.user_profiles.count() == 0 and
        engine.last_price_cache.count() == 0 and engine.fees.count() == 0 and
        engine.adjustments.count() == 0 and engine.suspends.count() == 0;
}

fn lessThanI32(_: void, lhs: i32, rhs: i32) bool {
    return lhs < rhs;
}

fn lessThanI64(_: void, lhs: i64, rhs: i64) bool {
    return lhs < rhs;
}

fn testExchangeSpec(symbol: i32) !common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    });
}

fn testMarginSpec(symbol: i32) !common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .futures_contract,
        .base_currency = 12,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 3,
        .maker_fee = 1,
        .margin_buy = 40,
        .margin_sell = 50,
    });
}

fn expectIntHistoryRawEqual(
    allocator: std.mem.Allocator,
    expected: *const int_map_order.History,
    actual: *const int_map_order.History,
) !void {
    try std.testing.expectEqual(expected.isKnown(), actual.isKnown());
    if (!expected.isKnown()) return;
    var expected_raw = try expected.exportRaw(allocator);
    defer expected_raw.deinit(allocator);
    var actual_raw = try actual.exportRaw(allocator);
    defer actual_raw.deinit(allocator);
    try std.testing.expectEqual(expected_raw.table_length, actual_raw.table_length);
    try std.testing.expectEqual(expected_raw.zero_key_flag, actual_raw.zero_key_flag);
    try std.testing.expectEqual(expected_raw.one_key_flag, actual_raw.one_key_flag);
    try std.testing.expectEqualSlices(i32, expected_raw.slots, actual_raw.slots);
}

fn expectLongHistoryRawEqual(
    allocator: std.mem.Allocator,
    expected: *const long_map_order.History,
    actual: *const long_map_order.History,
) !void {
    try std.testing.expectEqual(expected.isKnown(), actual.isKnown());
    if (!expected.isKnown()) return;
    var expected_raw = try expected.exportRaw(allocator);
    defer expected_raw.deinit(allocator);
    var actual_raw = try actual.exportRaw(allocator);
    defer actual_raw.deinit(allocator);
    try std.testing.expectEqual(expected_raw.table_length, actual_raw.table_length);
    try std.testing.expectEqual(expected_raw.zero_key_flag, actual_raw.zero_key_flag);
    try std.testing.expectEqual(expected_raw.one_key_flag, actual_raw.one_key_flag);
    try std.testing.expectEqualSlices(i64, expected_raw.slots, actual_raw.slots);
}

fn testOrders(symbol: i32) [5]kernel_mod.RestingOrder {
    return .{
        .{
            .symbol = symbol,
            .order_id = 101,
            .price = 110,
            .size = 10,
            .filled = 3,
            .reserve_bid_price = 0,
            .action = .ask,
            .uid = 1001,
            .timestamp = 10,
        },
        .{
            .symbol = symbol,
            .order_id = 102,
            .price = 110,
            .size = 9,
            .filled = 0,
            .reserve_bid_price = 0,
            .action = .ask,
            .uid = 1002,
            .timestamp = 11,
        },
        .{
            .symbol = symbol,
            .order_id = 103,
            .price = 120,
            .size = 8,
            .filled = 1,
            .reserve_bid_price = 0,
            .action = .ask,
            .uid = 1003,
            .timestamp = 12,
        },
        .{
            .symbol = symbol,
            .order_id = 201,
            .price = 100,
            .size = 7,
            .filled = 2,
            .reserve_bid_price = 130,
            .action = .bid,
            .uid = 2001,
            .timestamp = 13,
        },
        .{
            .symbol = symbol,
            .order_id = 202,
            .price = 90,
            .size = 6,
            .filled = 0,
            .reserve_bid_price = 130,
            .action = .bid,
            .uid = 2002,
            .timestamp = 14,
        },
    };
}

fn addPendingTransfers(assembler: *binary_frames.BinaryFrameAssembler) !void {
    const query: cmd.OrderCommand = .{
        .command = .binary_data_query,
        .order_id = 1,
        .price = 2,
        .reserve_bid_price = 3,
        .size = 4,
        .uid = 5,
        .symbol = 0,
        .user_cookie = 91,
    };
    const command: cmd.OrderCommand = .{
        .command = .binary_data_command,
        .order_id = 6,
        .price = 7,
        .reserve_bid_price = 8,
        .size = 9,
        .uid = 10,
        .symbol = 0,
        .user_cookie = 92,
    };
    try std.testing.expectEqual(binary_frames.AcceptResult.accepted, try assembler.acceptFrame(&query));
    try std.testing.expectEqual(binary_frames.AcceptResult.accepted, try assembler.acceptFrame(&command));
}

fn addJavaCapacityPendingTransfer(
    assembler: *binary_frames.BinaryFrameAssembler,
    transfer_id: i32,
) !void {
    const frame: cmd.OrderCommand = .{
        .command = .binary_data_query,
        .order_id = (@as(i64, 41) << 32) | 0x1122_3344,
        .price = -2,
        .reserve_bid_price = std.math.maxInt(i64),
        .size = std.math.minInt(i64),
        .uid = 99,
        .symbol = 0,
        .user_cookie = transfer_id,
    };
    try std.testing.expectEqual(binary_frames.AcceptResult.accepted, try assembler.acceptFrame(&frame));
}

fn downgradeSinglePendingBodyToV2(
    allocator: std.mem.Allocator,
    current: []const u8,
    capacity_offset: usize,
) ![]u8 {
    std.debug.assert(std.mem.readInt(u16, current[4..6], .little) == transfer_capacity_schema_version);
    std.debug.assert(capacity_offset + @sizeOf(u32) <= current.len);
    const legacy = try allocator.alloc(u8, current.len - @sizeOf(u32));
    @memcpy(legacy[0..capacity_offset], current[0..capacity_offset]);
    @memcpy(legacy[capacity_offset..], current[capacity_offset + @sizeOf(u32) ..]);
    std.mem.writeInt(u16, legacy[4..6], legacy_body_schema_version, .little);
    return legacy;
}

fn stripV4HistorySectionForTest(
    allocator: std.mem.Allocator,
    current: []const u8,
    history_start: usize,
    pending_start: usize,
) ![]u8 {
    std.debug.assert(std.mem.readInt(u16, current[4..6], .little) == body_schema_version);
    std.debug.assert(history_start <= pending_start and pending_start <= current.len);
    const removed = pending_start - history_start;
    const legacy = try allocator.alloc(u8, current.len - removed);
    @memcpy(legacy[0..history_start], current[0..history_start]);
    @memcpy(legacy[history_start..], current[pending_start..]);
    std.mem.writeInt(u16, legacy[4..6], transfer_capacity_schema_version, .little);
    return legacy;
}

fn matchingHistoryStartForTest(state: *const MatchingState) usize {
    var offset = matching_body_header_len;
    for (state.books.items) |book| {
        offset += symbol_spec_len + @sizeOf(u32) + book.orders.items.len * resting_order_len;
    }
    return offset;
}

fn matchingPendingStartForTest(state: *const MatchingState, limits: Limits) !usize {
    return matchingHistoryStartForTest(state) +
        @sizeOf(u32) +
        try intHistoryEncodedLength(&state.books_history, limits);
}

fn riskHistoryStartForTest(state: *const RiskState) usize {
    var offset = risk_body_header_len + state.symbols.items.len * symbol_spec_len + @sizeOf(u32);
    for (state.users.items) |snapshot_user| {
        offset += 21 +
            snapshot_user.accounts.items.len * i32_i64_entry_len +
            @sizeOf(u32) +
            snapshot_user.positions.items.len * position_len;
    }
    offset += @sizeOf(u32) + state.last_prices.items.len * last_price_len;
    offset += @sizeOf(u32) + state.fees.items.len * i32_i64_entry_len;
    offset += @sizeOf(u32) + state.adjustments.items.len * i32_i64_entry_len;
    offset += @sizeOf(u32) + state.suspends.items.len * i32_i64_entry_len;
    return offset;
}

fn riskPendingStartForTest(state: *const RiskState, limits: Limits) !usize {
    var offset = riskHistoryStartForTest(state);
    offset += @sizeOf(u32) + try longHistoryEncodedLength(&state.user_profiles_history, limits);
    offset += @sizeOf(u32);
    for (state.user_histories.items) |histories| {
        offset += @sizeOf(i64);
        offset += @sizeOf(u32) + try intHistoryEncodedLength(&histories.accounts, limits);
        offset += @sizeOf(u32) + try intHistoryEncodedLength(&histories.positions, limits);
    }
    offset += @sizeOf(u32) + try intHistoryEncodedLength(&state.fees_history, limits);
    offset += @sizeOf(u32) + try intHistoryEncodedLength(&state.adjustments_history, limits);
    offset += @sizeOf(u32) + try intHistoryEncodedLength(&state.suspends_history, limits);
    return offset;
}

fn makeMatchingKernel(
    allocator: std.mem.Allocator,
    kind: kernel_mod.OrderBookKind,
) !kernel_mod.ExchangeKernel {
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{ .orderbook_kind = kind });
    errdefer kernel.deinit();
    const spec = try testExchangeSpec(241);
    try std.testing.expect(try kernel.addSymbol(spec));
    const orders = testOrders(spec.symbol_id);
    try kernel.restoreOrdersForSymbol(spec.symbol_id, &orders);
    try addPendingTransfers(&kernel.binary_frame_assembler);
    return kernel;
}

fn makeRiskState(
    allocator: std.mem.Allocator,
) !struct { engine: risk_mod.RiskEngine, assembler: binary_frames.BinaryFrameAssembler } {
    var engine = try risk_mod.RiskEngine.init(allocator, 1, 2, .{ .margin_trading_enabled = true });
    errdefer engine.deinit();
    const first = try testExchangeSpec(240);
    const margin = try testMarginSpec(241);
    try engine.addSymbol(margin);
    try engine.addSymbol(first);

    {
        try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(101));
        const profile = engine.user_profile_service.getUserProfile(101).?;
        profile.adjustments_counter = 77;
        try profile.putAccount(allocator, 15, 100_000);
        try profile.putAccount(allocator, 12, 500);
        try profile.putPosition(allocator, margin.symbol_id, .{
            .uid = 101,
            .symbol = margin.symbol_id,
            .currency = margin.quote_currency,
            .direction = .long,
            .open_volume = 3,
            .open_price_sum = 300,
            .profit = 7,
            .pending_sell_size = 2,
            .pending_buy_size = 1,
        });
    }

    _ = try engine.user_profile_service.getUserProfileOrAddSuspended(103);
    try engine.last_price_cache.put(allocator, margin.symbol_id, .{ .ask_price = 111, .bid_price = 109 });
    try engine.fees_history.recordPut(allocator, 15);
    try engine.fees.put(allocator, 15, 31);
    try engine.fees_history.recordPut(allocator, 12);
    try engine.fees.put(allocator, 12, 9);
    try engine.adjustments_history.recordPut(allocator, 15);
    try engine.adjustments.put(allocator, 15, -7);
    try engine.suspends_history.recordPut(allocator, 12);
    try engine.suspends.put(allocator, 12, 4);

    var assembler = binary_frames.BinaryFrameAssembler.init(allocator);
    errdefer assembler.deinit();
    try addPendingTransfers(&assembler);
    return .{ .engine = engine, .assembler = assembler };
}

fn expectMatchingRoundtrip(kind: kernel_mod.OrderBookKind) !void {
    const allocator = std.testing.allocator;
    var source = try makeMatchingKernel(allocator, kind);
    defer source.deinit();
    var captured = try MatchingState.capture(allocator, &source, .{});
    defer captured.deinit();
    const bytes = try captured.encodeAlloc(allocator, .{});
    defer allocator.free(bytes);
    var decoded = try MatchingState.decodeAlloc(allocator, bytes, .{});
    defer decoded.deinit();

    var restored = try kernel_mod.ExchangeKernel.init(allocator, .{ .orderbook_kind = kind });
    defer restored.deinit();
    try decoded.restoreIntoFreshKernel(&restored);
    try std.testing.expect(riskEngineIsEmpty(&restored.risk_engine));

    var recaptured = try MatchingState.capture(allocator, &restored, .{});
    defer recaptured.deinit();
    const restored_bytes = try recaptured.encodeAlloc(allocator, .{});
    defer allocator.free(restored_bytes);
    try std.testing.expectEqualSlices(u8, bytes, restored_bytes);
    try std.testing.expectEqual(captured.books_state_hash, recaptured.books_state_hash);
    try std.testing.expectEqual(captured.binary_state_hash, recaptured.binary_state_hash);
    try std.testing.expectEqual(@as(usize, 1), recaptured.books.items.len);
    try std.testing.expectEqualSlices(
        kernel_mod.RestingOrder,
        captured.books.items[0].orders.items,
        recaptured.books.items[0].orders.items,
    );
}

test "XMB2 capture encode decode and fresh restore preserve naive FIFO partial fills and pending transfers" {
    try expectMatchingRoundtrip(.naive);
}

test "XMB2 capture encode decode and fresh restore preserve direct FIFO partial fills and pending transfers" {
    try expectMatchingRoundtrip(.direct);
}

test "XRB2 capture encode decode and fresh restore preserve margin users maps and both pending transfer kinds" {
    const allocator = std.testing.allocator;
    var source = try makeRiskState(allocator);
    defer source.engine.deinit();
    defer source.assembler.deinit();
    var captured = try RiskState.capture(allocator, &source.engine, &source.assembler, .{});
    defer captured.deinit();
    const bytes = try captured.encodeAlloc(allocator, .{});
    defer allocator.free(bytes);
    var decoded = try RiskState.decodeAlloc(allocator, bytes, .{});
    defer decoded.deinit();

    var engine = try risk_mod.RiskEngine.init(allocator, 1, 2, .{ .margin_trading_enabled = true });
    defer engine.deinit();
    var assembler = binary_frames.BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();
    try decoded.restoreIntoFreshRisk(&engine, &assembler);
    try std.testing.expectEqual(captured.risk_state_hash, try engine.stateHash());
    try std.testing.expectEqual(captured.binary_state_hash, try assembler.stateHash());

    var recaptured = try RiskState.capture(allocator, &engine, &assembler, .{});
    defer recaptured.deinit();
    const restored_bytes = try recaptured.encodeAlloc(allocator, .{});
    defer allocator.free(restored_bytes);
    try std.testing.expectEqualSlices(u8, bytes, restored_bytes);
    try std.testing.expectEqual(@as(usize, 2), recaptured.symbols.items.len);
    try std.testing.expectEqual(@as(usize, 2), recaptured.users.items.len);
    try std.testing.expectEqual(@as(usize, 2), recaptured.pending.transfers.len);
}

test "XRB2 raw staged restore preserves serialized futures under disabled live admission" {
    const allocator = std.testing.allocator;
    var source = try risk_mod.RiskEngine.init(allocator, 0, 1, .{});
    defer source.deinit();
    const futures = try testMarginSpec(241);
    try source.addSymbol(futures);
    var source_assembler = binary_frames.BinaryFrameAssembler.init(allocator);
    defer source_assembler.deinit();

    var captured = try RiskState.capture(allocator, &source, &source_assembler, .{});
    defer captured.deinit();
    const bytes = try captured.encodeAlloc(allocator, .{});
    defer allocator.free(bytes);
    var decoded = try RiskState.decodeAlloc(allocator, bytes, .{});
    defer decoded.deinit();

    var target = try risk_mod.RiskEngine.init(allocator, 0, 1, .{});
    defer target.deinit();
    var target_assembler = binary_frames.BinaryFrameAssembler.init(allocator);
    defer target_assembler.deinit();
    try decoded.restoreIntoFreshRisk(&target, &target_assembler);

    try std.testing.expect(target.symbol_spec_provider.getSymbolSpecification(futures.symbol_id) != null);
    try std.testing.expectEqual(try source.stateHash(), try target.stateHash());
}

test "XMB2 and XRB2 empty bodies roundtrip and restore" {
    const allocator = std.testing.allocator;
    var empty_kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer empty_kernel.deinit();
    var matching = try MatchingState.capture(allocator, &empty_kernel, .{});
    defer matching.deinit();
    const matching_bytes = try matching.encodeAlloc(allocator, .{});
    defer allocator.free(matching_bytes);
    try std.testing.expect(matching_bytes.len > 32);
    var matching_decoded = try MatchingState.decodeAlloc(allocator, matching_bytes, .{});
    defer matching_decoded.deinit();
    var matching_target = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer matching_target.deinit();
    try matching_decoded.restoreIntoFreshKernel(&matching_target);
    try std.testing.expect(matching_target.books_history.isKnown());
    const matching_keys = try matching_target.matchingBookKeysInHistoryOrder(allocator);
    defer allocator.free(matching_keys);
    try std.testing.expectEqual(@as(usize, 0), matching_keys.len);

    var empty_engine = try risk_mod.RiskEngine.init(allocator, 0, 1, .{});
    defer empty_engine.deinit();
    var empty_assembler = binary_frames.BinaryFrameAssembler.init(allocator);
    defer empty_assembler.deinit();
    var risk_state = try RiskState.capture(allocator, &empty_engine, &empty_assembler, .{});
    defer risk_state.deinit();
    const risk_bytes = try risk_state.encodeAlloc(allocator, .{});
    defer allocator.free(risk_bytes);
    try std.testing.expect(risk_bytes.len > 52);
    var risk_decoded = try RiskState.decodeAlloc(allocator, risk_bytes, .{});
    defer risk_decoded.deinit();
    var risk_target = try risk_mod.RiskEngine.init(allocator, 0, 1, .{});
    defer risk_target.deinit();
    var risk_assembler = binary_frames.BinaryFrameAssembler.init(allocator);
    defer risk_assembler.deinit();
    try risk_decoded.restoreIntoFreshRisk(&risk_target, &risk_assembler);
    _ = try risk_target.user_profile_service.validatedUserProfilesHistory();
    _ = try risk_target.validatedFeesHistory();
    _ = try risk_target.validatedAdjustmentsHistory();
    _ = try risk_target.validatedSuspendsHistory();
}

test "XMB2 schema 4 preserves sentinel collision tombstone and clear-retained EC layout" {
    const allocator = std.testing.allocator;
    var source = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer source.deinit();
    for ([_]i32{ 0, 1, 17, 49 }) |symbol| {
        try std.testing.expect(try source.addSymbol(try testExchangeSpec(symbol)));
    }

    var exact = try int_map_order.History.initWithExpectedSize(4);
    defer exact.deinit(allocator);
    for ([_]i32{ 0, 1, 17, 33, 49 }) |key| try exact.recordPut(allocator, key);
    exact.recordRemove(33);
    try source.replaceMatchingBooksHistory(&exact);

    var captured = try MatchingState.capture(allocator, &source, .{});
    defer captured.deinit();
    const bytes = try captured.encodeAlloc(allocator, .{});
    defer allocator.free(bytes);
    var decoded = try MatchingState.decodeAlloc(allocator, bytes, .{});
    defer decoded.deinit();
    try expectIntHistoryRawEqual(allocator, &captured.books_history, &decoded.books_history);

    var restored = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer restored.deinit();
    try decoded.restoreIntoFreshKernel(&restored);
    _ = try restored.validatedMatchingBooksHistory(allocator);
    try expectIntHistoryRawEqual(allocator, &source.books_history, &restored.books_history);

    // Clear keeps the EC table allocation but removes sentinel flags and
    // tombstones. Reinsert the semantic keys and prove the retained capacity
    // remains byte-exact across another v4 cycle.
    var cleared = try restored.books_history.clone(allocator);
    defer cleared.deinit(allocator);
    const retained_table_size = cleared.tableSizeIfKnown().?;
    cleared.clear();
    for ([_]i32{ 0, 1, 17, 49 }) |key| try cleared.recordPut(allocator, key);
    try restored.replaceMatchingBooksHistory(&cleared);
    try std.testing.expectEqual(retained_table_size, restored.books_history.tableSizeIfKnown().?);
    var recaptured = try MatchingState.capture(allocator, &restored, .{});
    defer recaptured.deinit();
    try expectIntHistoryRawEqual(allocator, &restored.books_history, &recaptured.books_history);
}

test "XRB2 schema 4 restores all uid-bound and aggregate histories byte-exact" {
    const allocator = std.testing.allocator;
    var source = try makeRiskState(allocator);
    defer source.engine.deinit();
    defer source.assembler.deinit();

    const profile = source.engine.user_profile_service.getUserProfile(101).?;
    for (2..12) |raw| try profile.putAccount(allocator, @intCast(raw), @intCast(raw * 10));
    profile.clearAccountsRetainingCapacity();
    try profile.putAccount(allocator, 0, 7);
    try profile.putAccount(allocator, 1, 8);
    try profile.putAccount(allocator, 17, 9);
    try profile.putPosition(allocator, 240, .{
        .uid = 101,
        .symbol = 240,
        .currency = 15,
        .direction = .long,
    });
    _ = profile.removePosition(240);
    try source.engine.fees_history.recordPut(allocator, 0);
    try source.engine.fees.put(allocator, 0, 88);
    try source.engine.fees_history.recordPut(allocator, 1);
    try source.engine.fees.put(allocator, 1, 99);

    var captured = try RiskState.capture(
        allocator,
        &source.engine,
        &source.assembler,
        .{},
    );
    defer captured.deinit();
    const bytes = try captured.encodeAlloc(allocator, .{});
    defer allocator.free(bytes);
    var decoded = try RiskState.decodeAlloc(allocator, bytes, .{});
    defer decoded.deinit();

    try expectLongHistoryRawEqual(
        allocator,
        &captured.user_profiles_history,
        &decoded.user_profiles_history,
    );
    for (captured.user_histories.items, decoded.user_histories.items) |expected, actual| {
        try std.testing.expectEqual(expected.uid, actual.uid);
        try expectIntHistoryRawEqual(allocator, &expected.accounts, &actual.accounts);
        try expectIntHistoryRawEqual(allocator, &expected.positions, &actual.positions);
    }
    try expectIntHistoryRawEqual(allocator, &captured.fees_history, &decoded.fees_history);
    try expectIntHistoryRawEqual(
        allocator,
        &captured.adjustments_history,
        &decoded.adjustments_history,
    );
    try expectIntHistoryRawEqual(
        allocator,
        &captured.suspends_history,
        &decoded.suspends_history,
    );

    var target = try risk_mod.RiskEngine.init(
        allocator,
        1,
        2,
        .{ .margin_trading_enabled = true },
    );
    defer target.deinit();
    var target_assembler = binary_frames.BinaryFrameAssembler.init(allocator);
    defer target_assembler.deinit();
    try decoded.restoreIntoFreshRisk(&target, &target_assembler);
    _ = try target.user_profile_service.validatedUserProfilesHistory();
    _ = try target.validatedFeesHistory();
    _ = try target.validatedAdjustmentsHistory();
    _ = try target.validatedSuspendsHistory();
    try expectLongHistoryRawEqual(
        allocator,
        &source.engine.user_profile_service.user_profiles_history,
        &target.user_profile_service.user_profiles_history,
    );
    const restored_profile = target.user_profile_service.getUserProfile(101).?;
    try expectIntHistoryRawEqual(
        allocator,
        &profile.accounts_history,
        &restored_profile.accounts_history,
    );
    try expectIntHistoryRawEqual(
        allocator,
        &profile.positions_history,
        &restored_profile.positions_history,
    );
    try expectIntHistoryRawEqual(
        allocator,
        &source.engine.fees_history,
        &target.fees_history,
    );
}

test "XMB2 and XRB2 schema 4 explicit unavailable histories roundtrip fail closed" {
    const allocator = std.testing.allocator;
    var matching_source = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer matching_source.deinit();
    try std.testing.expect(try matching_source.addSymbol(try testExchangeSpec(31)));
    matching_source.markMatchingBooksHistoryUnavailable();
    var matching = try MatchingState.capture(allocator, &matching_source, .{});
    defer matching.deinit();
    const matching_bytes = try matching.encodeAlloc(allocator, .{});
    defer allocator.free(matching_bytes);
    var matching_decoded = try MatchingState.decodeAlloc(allocator, matching_bytes, .{});
    defer matching_decoded.deinit();
    try std.testing.expect(!matching_decoded.books_history.isKnown());
    var matching_target = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer matching_target.deinit();
    try matching_decoded.restoreIntoFreshKernel(&matching_target);
    try std.testing.expectError(
        error.HistoryUnavailable,
        matching_target.matchingBookKeysInHistoryOrder(allocator),
    );

    var risk_source = try makeRiskState(allocator);
    defer risk_source.engine.deinit();
    defer risk_source.assembler.deinit();
    var profiles = risk_source.engine.user_profile_service.user_profiles.valueIterator();
    while (profiles.next()) |source_profile| {
        source_profile.markAccountsHistoryUnavailable(allocator);
        source_profile.markPositionsHistoryUnavailable(allocator);
    }
    risk_source.engine.user_profile_service.markUserProfilesHistoryUnavailable();
    risk_source.engine.markAggregateHistoriesUnavailable();
    var risk_state = try RiskState.capture(
        allocator,
        &risk_source.engine,
        &risk_source.assembler,
        .{},
    );
    defer risk_state.deinit();
    const risk_bytes = try risk_state.encodeAlloc(allocator, .{});
    defer allocator.free(risk_bytes);
    var risk_decoded = try RiskState.decodeAlloc(allocator, risk_bytes, .{});
    defer risk_decoded.deinit();
    var risk_target = try risk_mod.RiskEngine.init(
        allocator,
        1,
        2,
        .{ .margin_trading_enabled = true },
    );
    defer risk_target.deinit();
    var risk_assembler = binary_frames.BinaryFrameAssembler.init(allocator);
    defer risk_assembler.deinit();
    try risk_decoded.restoreIntoFreshRisk(&risk_target, &risk_assembler);
    try std.testing.expectError(
        error.HistoryUnavailable,
        risk_target.user_profile_service.userProfileKeysInHistoryOrder(),
    );
    try std.testing.expectError(error.HistoryUnavailable, risk_target.feeKeysInHistoryOrder());
    const restored_profile = risk_target.user_profile_service.getUserProfile(101).?;
    try std.testing.expectError(
        error.HistoryUnavailable,
        restored_profile.accountKeysInHistoryOrder(allocator),
    );
}

test "XMB2 preserves legacy Java untyped pending transfer sentinel" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    const backing = [_]i64{ 1, 2, 3, 4, 5, 0, 0, 0, 0, 0 };
    try kernel.binary_frame_assembler.restoreLegacyUntypedPendingState(&.{.{
        .transfer_id = 42,
        .words_transferred = 5,
        .data = &backing,
    }});

    var captured = try MatchingState.capture(allocator, &kernel, .{});
    defer captured.deinit();
    const encoded = try captured.encodeAlloc(allocator, .{});
    defer allocator.free(encoded);
    var decoded = try MatchingState.decodeAlloc(allocator, encoded, .{});
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 1), decoded.pending.transfers.len);
    try std.testing.expectEqual(
        @as(?cmd.OrderCommandType, null),
        decoded.pending.transfers[0].command_type,
    );

    var restored = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer restored.deinit();
    try decoded.restoreIntoFreshKernel(&restored);
    try std.testing.expectEqual(captured.binary_state_hash, try restored.binary_frame_assembler.stateHash());
    var recaptured = try restored.binary_frame_assembler.capturePendingState(allocator);
    defer recaptured.deinit();
    try std.testing.expectEqual(
        @as(?cmd.OrderCommandType, null),
        recaptured.transfers[0].command_type,
    );
}

test "XMB2 schema 4 preserves explicit and unavailable Java backing capacity" {
    const allocator = std.testing.allocator;

    var explicit_kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer explicit_kernel.deinit();
    try addJavaCapacityPendingTransfer(&explicit_kernel.binary_frame_assembler, -11);
    const expected_java_hash = try explicit_kernel.binary_frame_assembler.javaStateHash32();
    const expected_native_hash = try explicit_kernel.binary_frame_assembler.stateHash();

    var explicit_state = try MatchingState.capture(allocator, &explicit_kernel, .{});
    defer explicit_state.deinit();
    const explicit_bytes = try explicit_state.encodeAlloc(allocator, .{});
    defer allocator.free(explicit_bytes);
    const capacity_offset = try matchingPendingStartForTest(&explicit_state, .{}) +
        @sizeOf(u32) + @sizeOf(i32) + @sizeOf(i8);
    try std.testing.expectEqual(body_schema_version, std.mem.readInt(u16, explicit_bytes[4..6], .little));
    try std.testing.expectEqual(@as(u32, 10), std.mem.readInt(u32, explicit_bytes[capacity_offset..][0..4], .little));
    try std.testing.expectEqual(@as(u32, 5), std.mem.readInt(u32, explicit_bytes[capacity_offset + 4 ..][0..4], .little));

    var explicit_decoded = try MatchingState.decodeAlloc(allocator, explicit_bytes, .{});
    defer explicit_decoded.deinit();
    try std.testing.expectEqual(@as(?u32, 10), explicit_decoded.pending.transfers[0].java_backing_capacity);
    var explicit_target = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer explicit_target.deinit();
    try explicit_decoded.restoreIntoFreshKernel(&explicit_target);
    try std.testing.expectEqual(expected_native_hash, try explicit_target.binary_frame_assembler.stateHash());
    try std.testing.expectEqual(expected_java_hash, try explicit_target.binary_frame_assembler.javaStateHash32());

    var unavailable_kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer unavailable_kernel.deinit();
    var derivable_words = [_]i64{
        (@as(i64, 41) << 32) | 0x1122_3344,
        -2,
        std.math.maxInt(i64),
        std.math.minInt(i64),
        99,
    };
    try unavailable_kernel.binary_frame_assembler.restorePendingState(&.{.{
        .transfer_id = -11,
        .command_type = .binary_data_query,
        .java_backing_capacity = null,
        .words = &derivable_words,
    }});
    var unavailable_state = try MatchingState.capture(allocator, &unavailable_kernel, .{});
    defer unavailable_state.deinit();
    const unavailable_bytes = try unavailable_state.encodeAlloc(allocator, .{});
    defer allocator.free(unavailable_bytes);
    try std.testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, unavailable_bytes[capacity_offset..][0..4], .little));
    var unavailable_decoded = try MatchingState.decodeAlloc(allocator, unavailable_bytes, .{});
    defer unavailable_decoded.deinit();
    try std.testing.expectEqual(@as(?u32, null), unavailable_decoded.pending.transfers[0].java_backing_capacity);
    var unavailable_target = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer unavailable_target.deinit();
    try unavailable_decoded.restoreIntoFreshKernel(&unavailable_target);
    try std.testing.expectError(
        error.JavaStateHashUnavailable,
        unavailable_target.binary_frame_assembler.javaStateHash32(),
    );
}

test "XMB2 and XRB2 schema 2 pending records remain readable with unavailable Java capacity" {
    const allocator = std.testing.allocator;

    var matching_kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer matching_kernel.deinit();
    try addJavaCapacityPendingTransfer(&matching_kernel.binary_frame_assembler, -11);
    var matching_state = try MatchingState.capture(allocator, &matching_kernel, .{});
    defer matching_state.deinit();
    const matching_v4 = try matching_state.encodeAlloc(allocator, .{});
    defer allocator.free(matching_v4);
    const matching_history_start = matchingHistoryStartForTest(&matching_state);
    const matching_pending_start = try matchingPendingStartForTest(&matching_state, .{});
    const matching_v3 = try stripV4HistorySectionForTest(
        allocator,
        matching_v4,
        matching_history_start,
        matching_pending_start,
    );
    defer allocator.free(matching_v3);
    var matching_v3_decoded = try MatchingState.decodeAlloc(allocator, matching_v3, .{});
    defer matching_v3_decoded.deinit();
    try std.testing.expect(!matching_v3_decoded.books_history.isKnown());
    const matching_capacity_offset = matching_history_start +
        @sizeOf(u32) + @sizeOf(i32) + @sizeOf(i8);
    const matching_v2 = try downgradeSinglePendingBodyToV2(allocator, matching_v3, matching_capacity_offset);
    defer allocator.free(matching_v2);
    var matching_decoded = try MatchingState.decodeAlloc(allocator, matching_v2, .{});
    defer matching_decoded.deinit();
    try std.testing.expectEqual(@as(?u32, null), matching_decoded.pending.transfers[0].java_backing_capacity);
    try std.testing.checkAllAllocationFailures(allocator, matchingDecodeAllocationTest, .{matching_v2});

    var risk_engine = try risk_mod.RiskEngine.init(allocator, 0, 1, .{});
    defer risk_engine.deinit();
    var risk_assembler = binary_frames.BinaryFrameAssembler.init(allocator);
    defer risk_assembler.deinit();
    try addJavaCapacityPendingTransfer(&risk_assembler, -11);
    var risk_state = try RiskState.capture(allocator, &risk_engine, &risk_assembler, .{});
    defer risk_state.deinit();
    const risk_v4 = try risk_state.encodeAlloc(allocator, .{});
    defer allocator.free(risk_v4);
    const risk_history_start = riskHistoryStartForTest(&risk_state);
    const risk_pending_start = try riskPendingStartForTest(&risk_state, .{});
    const risk_v3 = try stripV4HistorySectionForTest(
        allocator,
        risk_v4,
        risk_history_start,
        risk_pending_start,
    );
    defer allocator.free(risk_v3);
    var risk_v3_decoded = try RiskState.decodeAlloc(allocator, risk_v3, .{});
    defer risk_v3_decoded.deinit();
    try std.testing.expect(!risk_v3_decoded.user_profiles_history.isKnown());
    try std.testing.expect(!risk_v3_decoded.fees_history.isKnown());
    const risk_capacity_offset = risk_history_start +
        @sizeOf(u32) + @sizeOf(i32) + @sizeOf(i8);
    const risk_v2 = try downgradeSinglePendingBodyToV2(allocator, risk_v3, risk_capacity_offset);
    defer allocator.free(risk_v2);
    var risk_decoded = try RiskState.decodeAlloc(allocator, risk_v2, .{});
    defer risk_decoded.deinit();
    try std.testing.expectEqual(@as(?u32, null), risk_decoded.pending.transfers[0].java_backing_capacity);
}

test "XMB2 schema 4 rejects mismatched explicit Java backing capacity" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    try addJavaCapacityPendingTransfer(&kernel.binary_frame_assembler, -11);
    var state = try MatchingState.capture(allocator, &kernel, .{});
    defer state.deinit();
    const encoded = try state.encodeAlloc(allocator, .{});
    defer allocator.free(encoded);
    const corrupted = try allocator.dupe(u8, encoded);
    defer allocator.free(corrupted);
    const capacity_offset = try matchingPendingStartForTest(&state, .{}) +
        @sizeOf(u32) + @sizeOf(i32) + @sizeOf(i8);
    std.mem.writeInt(u32, corrupted[capacity_offset..][0..4], 5, .little);
    try std.testing.expectError(error.InvalidTransfer, MatchingState.decodeAlloc(allocator, corrupted, .{}));
}

test "XMB2 schema 4 rejects corrupted history key sets malformed blobs and slot limits" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try testExchangeSpec(17)));

    var state = try MatchingState.capture(allocator, &kernel, .{});
    defer state.deinit();
    state.books_history.recordRemove(17);
    try std.testing.expectError(
        error.HistorySemanticKeySetMismatch,
        state.encodeAlloc(allocator, .{}),
    );
    var target = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer target.deinit();
    try std.testing.expectError(
        error.HistorySemanticKeySetMismatch,
        state.restoreIntoFreshKernel(&target),
    );
    try std.testing.expectEqual(@as(usize, 0), target.books.count());

    // Recapture a valid body and corrupt only the length-prefixed sidecar.
    state.deinit();
    state = try MatchingState.capture(allocator, &kernel, .{});
    const encoded = try state.encodeAlloc(allocator, .{});
    defer allocator.free(encoded);
    const history_offset = matchingHistoryStartForTest(&state);
    const malformed = try allocator.dupe(u8, encoded);
    defer allocator.free(malformed);
    malformed[history_offset + @sizeOf(u32)] = 0xff;
    try std.testing.expectError(
        error.InvalidFormat,
        MatchingState.decodeAlloc(allocator, malformed, .{}),
    );
    try std.testing.expectError(
        error.LimitExceeded,
        MatchingState.decodeAlloc(allocator, encoded, .{ .max_history_slots = 0 }),
    );
    std.mem.writeInt(
        u32,
        malformed[history_offset..][0..@sizeOf(u32)],
        std.math.maxInt(u32),
        .little,
    );
    try std.testing.expectError(
        error.LimitExceeded,
        MatchingState.decodeAlloc(allocator, malformed, .{}),
    );
}

test "XRB2 schema 4 capture rejects known history semantic corruption" {
    const allocator = std.testing.allocator;
    var source = try makeRiskState(allocator);
    defer source.engine.deinit();
    defer source.assembler.deinit();
    source.engine.fees_history.recordRemove(15);
    try std.testing.expectError(
        error.HistorySemanticKeySetMismatch,
        RiskState.capture(allocator, &source.engine, &source.assembler, .{}),
    );
}

test "Java encounter initializers are uid-bound failure-atomic and use serialized counts" {
    const allocator = std.testing.allocator;
    var matching_kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer matching_kernel.deinit();
    for ([_]i32{ 0, 1, 17, 49 }) |symbol| {
        try std.testing.expect(try matching_kernel.addSymbol(try testExchangeSpec(symbol)));
    }
    var matching = try MatchingState.capture(allocator, &matching_kernel, .{});
    defer matching.deinit();
    try matching.initializeBooksHistoryFromEncounterOrder(&.{ 49, 0, 17, 1 });
    try std.testing.expectEqual(@as(?usize, 8), matching.books_history.tableSizeIfKnown());
    const matching_before = try matching.encodeAlloc(allocator, .{});
    defer allocator.free(matching_before);
    try std.testing.expectError(
        error.HistorySemanticKeySetMismatch,
        matching.initializeBooksHistoryFromEncounterOrder(&.{ 49, 0, 17, 33 }),
    );
    const matching_after = try matching.encodeAlloc(allocator, .{});
    defer allocator.free(matching_after);
    try std.testing.expectEqualSlices(u8, matching_before, matching_after);

    var risk_source = try makeRiskState(allocator);
    defer risk_source.engine.deinit();
    defer risk_source.assembler.deinit();
    var risk_state = try RiskState.capture(
        allocator,
        &risk_source.engine,
        &risk_source.assembler,
        .{},
    );
    defer risk_state.deinit();
    try risk_state.initializeHistoriesFromEncounterOrder(.{
        .user_uids = &.{ 103, 101 },
        .users = &.{
            .{
                .uid = 103,
                .account_keys = &.{},
                .position_keys = &.{},
            },
            .{
                .uid = 101,
                .account_keys = &.{ 15, 12 },
                .position_keys = &.{241},
            },
        },
        .fee_keys = &.{ 15, 12 },
        .adjustment_keys = &.{15},
        .suspend_keys = &.{12},
    });
    try std.testing.expectEqual(
        @as(?usize, 4),
        risk_state.user_profiles_history.tableSizeIfKnown(),
    );
    const risk_before = try risk_state.encodeAlloc(allocator, .{});
    defer allocator.free(risk_before);
    try std.testing.expectError(
        error.HistorySemanticKeySetMismatch,
        risk_state.initializeHistoriesFromEncounterOrder(.{
            .user_uids = &.{ 103, 101 },
            .users = &.{
                .{
                    .uid = 103,
                    .account_keys = &.{},
                    .position_keys = &.{},
                },
                .{
                    .uid = 101,
                    .account_keys = &.{ 15, 99 },
                    .position_keys = &.{241},
                },
            },
            .fee_keys = &.{ 15, 12 },
            .adjustment_keys = &.{15},
            .suspend_keys = &.{12},
        }),
    );
    const risk_after = try risk_state.encodeAlloc(allocator, .{});
    defer allocator.free(risk_after);
    try std.testing.expectEqualSlices(u8, risk_before, risk_after);
}

test "XMB2 and XRB2 exact little-endian body offsets are stable" {
    const allocator = std.testing.allocator;
    var matching_kernel = try makeMatchingKernel(allocator, .naive);
    defer matching_kernel.deinit();
    var matching = try MatchingState.capture(allocator, &matching_kernel, .{});
    defer matching.deinit();
    const matching_bytes = try matching.encodeAlloc(allocator, .{});
    defer allocator.free(matching_bytes);
    try std.testing.expectEqualSlices(u8, matching_body_magic, matching_bytes[0..4]);
    try std.testing.expectEqual(body_schema_version, std.mem.readInt(u16, matching_bytes[4..6], .little));
    try std.testing.expectEqual(@as(u16, 0), std.mem.readInt(u16, matching_bytes[6..8], .little));
    try std.testing.expectEqual(matching.books_state_hash, std.mem.readInt(u64, matching_bytes[8..16], .little));
    try std.testing.expectEqual(matching.binary_state_hash, std.mem.readInt(u64, matching_bytes[16..24], .little));
    try std.testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, matching_bytes[24..28], .little));
    try std.testing.expectEqual(@as(i32, 241), std.mem.readInt(i32, matching_bytes[28..32], .little));
    try std.testing.expectEqual(@as(u32, 5), std.mem.readInt(u32, matching_bytes[89..93], .little));
    try std.testing.expectEqual(@as(i64, 101), std.mem.readInt(i64, matching_bytes[97..105], .little));

    var risk_source = try makeRiskState(allocator);
    defer risk_source.engine.deinit();
    defer risk_source.assembler.deinit();
    var risk_state = try RiskState.capture(allocator, &risk_source.engine, &risk_source.assembler, .{});
    defer risk_state.deinit();
    const risk_bytes = try risk_state.encodeAlloc(allocator, .{});
    defer allocator.free(risk_bytes);
    try std.testing.expectEqualSlices(u8, risk_body_magic, risk_bytes[0..4]);
    try std.testing.expectEqual(body_schema_version, std.mem.readInt(u16, risk_bytes[4..6], .little));
    try std.testing.expectEqual(risk_state.risk_state_hash, std.mem.readInt(u64, risk_bytes[8..16], .little));
    try std.testing.expectEqual(risk_state.binary_state_hash, std.mem.readInt(u64, risk_bytes[16..24], .little));
    try std.testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, risk_bytes[24..28], .little));
    try std.testing.expectEqual(@as(i32, 240), std.mem.readInt(i32, risk_bytes[28..32], .little));
}

fn emptyBodyBytes(allocator: std.mem.Allocator) !struct { matching: []u8, risk: []u8 } {
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    var matching = try MatchingState.capture(allocator, &kernel, .{});
    defer matching.deinit();
    const matching_bytes = try matching.encodeAlloc(allocator, .{});
    errdefer allocator.free(matching_bytes);

    var engine = try risk_mod.RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    var assembler = binary_frames.BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();
    var risk_state = try RiskState.capture(allocator, &engine, &assembler, .{});
    defer risk_state.deinit();
    return .{
        .matching = matching_bytes,
        .risk = try risk_state.encodeAlloc(allocator, .{}),
    };
}

test "XMB2 and XRB2 reject every truncation boundary trailing bytes schema flags and huge counts" {
    const allocator = std.testing.allocator;
    const bodies = try emptyBodyBytes(allocator);
    defer allocator.free(bodies.matching);
    defer allocator.free(bodies.risk);
    for (0..bodies.matching.len) |cut| {
        try std.testing.expectError(error.Truncated, MatchingState.decodeAlloc(allocator, bodies.matching[0..cut], .{}));
    }
    for (0..bodies.risk.len) |cut| {
        try std.testing.expectError(error.Truncated, RiskState.decodeAlloc(allocator, bodies.risk[0..cut], .{}));
    }

    const matching_trailing = try allocator.alloc(u8, bodies.matching.len + 1);
    defer allocator.free(matching_trailing);
    @memcpy(matching_trailing[0..bodies.matching.len], bodies.matching);
    matching_trailing[bodies.matching.len] = 0;
    try std.testing.expectError(error.TrailingBytes, MatchingState.decodeAlloc(allocator, matching_trailing, .{}));
    const risk_trailing = try allocator.alloc(u8, bodies.risk.len + 1);
    defer allocator.free(risk_trailing);
    @memcpy(risk_trailing[0..bodies.risk.len], bodies.risk);
    risk_trailing[bodies.risk.len] = 0;
    try std.testing.expectError(error.TrailingBytes, RiskState.decodeAlloc(allocator, risk_trailing, .{}));

    const mutable = try allocator.dupe(u8, bodies.matching);
    defer allocator.free(mutable);
    mutable[0] = 'Z';
    try std.testing.expectError(error.InvalidMagic, MatchingState.decodeAlloc(allocator, mutable, .{}));
    @memcpy(mutable, bodies.matching);
    std.mem.writeInt(u16, mutable[4..6], body_schema_version + 1, .little);
    try std.testing.expectError(error.UnsupportedVersion, MatchingState.decodeAlloc(allocator, mutable, .{}));
    @memcpy(mutable, bodies.matching);
    std.mem.writeInt(u16, mutable[6..8], 1, .little);
    try std.testing.expectError(error.UnknownFlags, MatchingState.decodeAlloc(allocator, mutable, .{}));
    @memcpy(mutable, bodies.matching);
    std.mem.writeInt(u32, mutable[24..28], std.math.maxInt(u32), .little);
    try std.testing.expectError(error.LimitExceeded, MatchingState.decodeAlloc(allocator, mutable, .{}));

    const risk_mutable = try allocator.dupe(u8, bodies.risk);
    defer allocator.free(risk_mutable);
    risk_mutable[0] = 'Z';
    try std.testing.expectError(error.InvalidMagic, RiskState.decodeAlloc(allocator, risk_mutable, .{}));
    @memcpy(risk_mutable, bodies.risk);
    std.mem.writeInt(u16, risk_mutable[4..6], body_schema_version + 1, .little);
    try std.testing.expectError(error.UnsupportedVersion, RiskState.decodeAlloc(allocator, risk_mutable, .{}));
    @memcpy(risk_mutable, bodies.risk);
    std.mem.writeInt(u16, risk_mutable[6..8], 1, .little);
    try std.testing.expectError(error.UnknownFlags, RiskState.decodeAlloc(allocator, risk_mutable, .{}));
    @memcpy(risk_mutable, bodies.risk);
    std.mem.writeInt(u32, risk_mutable[24..28], std.math.maxInt(u32), .little);
    try std.testing.expectError(error.LimitExceeded, RiskState.decodeAlloc(allocator, risk_mutable, .{}));
}

test "XMB2 decoder rejects duplicate invalid and noncanonical order records" {
    const allocator = std.testing.allocator;
    var kernel = try makeMatchingKernel(allocator, .naive);
    defer kernel.deinit();
    var state = try MatchingState.capture(allocator, &kernel, .{});
    defer state.deinit();
    const original = try state.encodeAlloc(allocator, .{});
    defer allocator.free(original);
    const bytes = try allocator.dupe(u8, original);
    defer allocator.free(bytes);
    const first_order = matching_body_header_len + symbol_spec_len + @sizeOf(u32);
    const second_order = first_order + resting_order_len;

    std.mem.writeInt(i64, bytes[second_order + 4 ..][0..8], 101, .little);
    try std.testing.expectError(error.DuplicateOrderId, MatchingState.decodeAlloc(allocator, bytes, .{}));
    @memcpy(bytes, original);
    std.mem.writeInt(i64, bytes[first_order + 20 ..][0..8], 0, .little);
    try std.testing.expectError(error.InvalidOrder, MatchingState.decodeAlloc(allocator, bytes, .{}));
    @memcpy(bytes, original);
    std.mem.writeInt(i64, bytes[second_order + 12 ..][0..8], 100, .little);
    try std.testing.expectError(error.NonCanonicalOrder, MatchingState.decodeAlloc(allocator, bytes, .{}));
    @memcpy(bytes, original);
    std.mem.writeInt(u32, bytes[matching_body_header_len + symbol_spec_len ..][0..4], std.math.maxInt(u32), .little);
    try std.testing.expectError(error.LimitExceeded, MatchingState.decodeAlloc(allocator, bytes, .{}));
}

test "XMB2 matching fingerprint rejects a same-price FIFO reorder" {
    const allocator = std.testing.allocator;
    var kernel = try makeMatchingKernel(allocator, .naive);
    defer kernel.deinit();
    var state = try MatchingState.capture(allocator, &kernel, .{});
    defer state.deinit();
    const original = try state.encodeAlloc(allocator, .{});
    defer allocator.free(original);
    const reordered = try allocator.dupe(u8, original);
    defer allocator.free(reordered);

    const first_order = matching_body_header_len + symbol_spec_len + @sizeOf(u32);
    const second_order = first_order + resting_order_len;
    var first_record: [resting_order_len]u8 = undefined;
    @memcpy(&first_record, reordered[first_order .. first_order + resting_order_len]);
    @memcpy(
        reordered[first_order .. first_order + resting_order_len],
        reordered[second_order .. second_order + resting_order_len],
    );
    @memcpy(reordered[second_order .. second_order + resting_order_len], &first_record);

    // Both records have ask price 110, so the body remains structurally
    // canonical. Only their FIFO order changed.
    var decoded = try MatchingState.decodeAlloc(allocator, reordered, .{});
    defer decoded.deinit();
    try std.testing.expectEqual(decoded.books.items[0].orders.items[0].price, decoded.books.items[0].orders.items[1].price);
    try std.testing.expectEqual(@as(i64, 102), decoded.books.items[0].orders.items[0].order_id);
    try std.testing.expectEqual(@as(i64, 101), decoded.books.items[0].orders.items[1].order_id);

    var target = try kernel_mod.ExchangeKernel.init(allocator, .{ .orderbook_kind = .naive });
    defer target.deinit();
    try std.testing.expectError(error.MatchingStateHashMismatch, decoded.restoreIntoFreshKernel(&target));
    try std.testing.expect((try matchingBooksStateHash(&target)) != decoded.books_state_hash);
}

test "XRB2 decoder rejects duplicate and noncanonical symbols plus binary corruption" {
    const allocator = std.testing.allocator;
    var source = try makeRiskState(allocator);
    defer source.engine.deinit();
    defer source.assembler.deinit();
    var state = try RiskState.capture(allocator, &source.engine, &source.assembler, .{});
    defer state.deinit();
    const original = try state.encodeAlloc(allocator, .{});
    defer allocator.free(original);
    const bytes = try allocator.dupe(u8, original);
    defer allocator.free(bytes);

    const first_symbol = risk_body_header_len;
    const second_symbol = first_symbol + symbol_spec_len;
    std.mem.writeInt(i32, bytes[second_symbol..][0..4], 240, .little);
    try std.testing.expectError(error.DuplicateSymbol, RiskState.decodeAlloc(allocator, bytes, .{}));
    @memcpy(bytes, original);
    std.mem.writeInt(i32, bytes[first_symbol..][0..4], 242, .little);
    try std.testing.expectError(error.NonCanonicalOrder, RiskState.decodeAlloc(allocator, bytes, .{}));
    @memcpy(bytes, original);
    bytes[16] ^= 1;
    try std.testing.expectError(error.BinaryStateHashMismatch, RiskState.decodeAlloc(allocator, bytes, .{}));
}

test "XMB2 and XRB2 validate stored matching and native risk fingerprints after fresh restore" {
    const allocator = std.testing.allocator;
    var kernel = try makeMatchingKernel(allocator, .naive);
    defer kernel.deinit();
    var matching = try MatchingState.capture(allocator, &kernel, .{});
    defer matching.deinit();
    const matching_bytes = try matching.encodeAlloc(allocator, .{});
    defer allocator.free(matching_bytes);
    const matching_mutable = try allocator.dupe(u8, matching_bytes);
    defer allocator.free(matching_mutable);
    matching_mutable[8] ^= 1;
    var matching_decoded = try MatchingState.decodeAlloc(allocator, matching_mutable, .{});
    defer matching_decoded.deinit();
    var matching_target = try kernel_mod.ExchangeKernel.init(allocator, .{ .orderbook_kind = .naive });
    defer matching_target.deinit();
    try std.testing.expectError(error.MatchingStateHashMismatch, matching_decoded.restoreIntoFreshKernel(&matching_target));

    var source = try makeRiskState(allocator);
    defer source.engine.deinit();
    defer source.assembler.deinit();
    var risk_state = try RiskState.capture(allocator, &source.engine, &source.assembler, .{});
    defer risk_state.deinit();
    const risk_bytes = try risk_state.encodeAlloc(allocator, .{});
    defer allocator.free(risk_bytes);
    const risk_mutable = try allocator.dupe(u8, risk_bytes);
    defer allocator.free(risk_mutable);
    risk_mutable[8] ^= 1;
    var risk_decoded = try RiskState.decodeAlloc(allocator, risk_mutable, .{});
    defer risk_decoded.deinit();
    var risk_target = try risk_mod.RiskEngine.init(allocator, 1, 2, .{ .margin_trading_enabled = true });
    defer risk_target.deinit();
    var assembler = binary_frames.BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();
    try std.testing.expectError(error.RiskStateHashMismatch, risk_decoded.restoreIntoFreshRisk(&risk_target, &assembler));
}

test "XMB2 and XRB2 enforce configured body and collection limits before allocation" {
    const allocator = std.testing.allocator;
    var kernel = try makeMatchingKernel(allocator, .naive);
    defer kernel.deinit();
    var matching = try MatchingState.capture(allocator, &kernel, .{});
    defer matching.deinit();
    try std.testing.expectError(error.LimitExceeded, matching.encodeAlloc(allocator, .{ .max_symbols = 0 }));
    const matching_bytes = try matching.encodeAlloc(allocator, .{});
    defer allocator.free(matching_bytes);
    try std.testing.expectError(error.LimitExceeded, MatchingState.decodeAlloc(allocator, matching_bytes, .{ .max_body_bytes = matching_bytes.len - 1 }));

    var source = try makeRiskState(allocator);
    defer source.engine.deinit();
    defer source.assembler.deinit();
    var risk_state = try RiskState.capture(allocator, &source.engine, &source.assembler, .{});
    defer risk_state.deinit();
    try std.testing.expectError(error.LimitExceeded, risk_state.encodeAlloc(allocator, .{ .max_users = 1 }));
    const risk_bytes = try risk_state.encodeAlloc(allocator, .{});
    defer allocator.free(risk_bytes);
    try std.testing.expectError(error.LimitExceeded, RiskState.decodeAlloc(allocator, risk_bytes, .{ .max_symbols = 1 }));
}

fn matchingDecodeAllocationTest(allocator: std.mem.Allocator, bytes: []const u8) !void {
    var decoded = try MatchingState.decodeAlloc(allocator, bytes, .{});
    defer decoded.deinit();
}

fn riskDecodeAllocationTest(allocator: std.mem.Allocator, bytes: []const u8) !void {
    var decoded = try RiskState.decodeAlloc(allocator, bytes, .{});
    defer decoded.deinit();
}

fn matchingCaptureAllocationTest(allocator: std.mem.Allocator, kernel: *const kernel_mod.ExchangeKernel) !void {
    var captured = try MatchingState.capture(allocator, kernel, .{});
    defer captured.deinit();
}

fn matchingCaptureSameAllocatorAllocationTest(allocator: std.mem.Allocator) !void {
    var kernel = try makeMatchingKernel(allocator, .direct);
    defer kernel.deinit();
    var captured = try MatchingState.capture(allocator, &kernel, .{});
    defer captured.deinit();
}

fn riskCaptureAllocationTest(
    allocator: std.mem.Allocator,
    engine: *const risk_mod.RiskEngine,
    assembler: *const binary_frames.BinaryFrameAssembler,
) !void {
    var captured = try RiskState.capture(allocator, engine, assembler, .{});
    defer captured.deinit();
}

fn matchingEncodeAllocationTest(allocator: std.mem.Allocator, state: *const MatchingState) !void {
    const bytes = try state.encodeAlloc(allocator, .{});
    defer allocator.free(bytes);
}

fn riskEncodeAllocationTest(allocator: std.mem.Allocator, state: *const RiskState) !void {
    const bytes = try state.encodeAlloc(allocator, .{});
    defer allocator.free(bytes);
}

fn matchingRestoreAllocationTest(
    allocator: std.mem.Allocator,
    state: *const MatchingState,
    kind: kernel_mod.OrderBookKind,
) !void {
    var target = try kernel_mod.ExchangeKernel.init(allocator, .{ .orderbook_kind = kind });
    defer target.deinit();
    try state.restoreIntoFreshKernel(&target);
}

fn riskRestoreAllocationTest(allocator: std.mem.Allocator, state: *const RiskState) !void {
    var target = try risk_mod.RiskEngine.init(allocator, 1, 2, .{ .margin_trading_enabled = true });
    defer target.deinit();
    var assembler = binary_frames.BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();
    try state.restoreIntoFreshRisk(&target, &assembler);
}

test "XMB2 and XRB2 capture decode encode and restore release all owned state across every allocation failure" {
    const allocator = std.testing.allocator;
    var kernel = try makeMatchingKernel(allocator, .direct);
    defer kernel.deinit();
    var matching = try MatchingState.capture(allocator, &kernel, .{});
    defer matching.deinit();
    const matching_bytes = try matching.encodeAlloc(allocator, .{});
    defer allocator.free(matching_bytes);
    try std.testing.checkAllAllocationFailures(allocator, matchingDecodeAllocationTest, .{matching_bytes});
    try std.testing.checkAllAllocationFailures(allocator, matchingCaptureAllocationTest, .{&kernel});
    try std.testing.checkAllAllocationFailures(allocator, matchingEncodeAllocationTest, .{&matching});
    try std.testing.checkAllAllocationFailures(allocator, matchingRestoreAllocationTest, .{ &matching, kernel_mod.OrderBookKind.direct });

    var source = try makeRiskState(allocator);
    defer source.engine.deinit();
    defer source.assembler.deinit();
    var risk_state = try RiskState.capture(allocator, &source.engine, &source.assembler, .{});
    defer risk_state.deinit();
    const risk_bytes = try risk_state.encodeAlloc(allocator, .{});
    defer allocator.free(risk_bytes);
    try std.testing.checkAllAllocationFailures(allocator, riskDecodeAllocationTest, .{risk_bytes});
    try std.testing.checkAllAllocationFailures(allocator, riskCaptureAllocationTest, .{ &source.engine, &source.assembler });
    try std.testing.checkAllAllocationFailures(allocator, riskEncodeAllocationTest, .{&risk_state});
    try std.testing.checkAllAllocationFailures(allocator, riskRestoreAllocationTest, .{&risk_state});
}

test "XMB2 capture is leak-free when source state and snapshot share the failing allocator" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        matchingCaptureSameAllocatorAllocationTest,
        .{},
    );
}
