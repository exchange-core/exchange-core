pub const journal_wire = @import("journal_wire.zig");
pub const journal_replay = @import("journal_replay.zig");
pub const journal_converter = @import("journal_converter.zig");
pub const snapshot_wire = @import("snapshot_wire.zig");
pub const snapshot_body = @import("snapshot_body.zig");
pub const snapshot_import = @import("snapshot_import.zig");
pub const converter = @import("converter.zig");
pub const legacy_lz4 = @import("legacy_lz4.zig");

const exchange_core = @import("exchange_core");

comptime {
    if (@hasDecl(exchange_core, "java_journal_converter") or
        @hasDecl(exchange_core, "java_persistence_converter") or
        @hasDecl(exchange_core.core, "legacy_persistence_adapter"))
    {
        @compileError("offline Java persistence interop must remain outside exchange_core");
    }
}

// Preserve the previous names after callers switch their module import from
// `exchange_core` to `java_persistence_interop`.
pub const java_journal_wire = journal_wire;
pub const java_journal_replay = journal_replay;
pub const java_journal_converter = journal_converter;
pub const java_snapshot_wire = snapshot_wire;
pub const java_snapshot_body = snapshot_body;
pub const java_snapshot_import = snapshot_import;
pub const java_persistence_converter = converter;
pub const legacy_persistence_adapter = legacy_lz4;

test {
    _ = journal_wire;
    _ = journal_replay;
    _ = journal_converter;
    _ = snapshot_wire;
    _ = snapshot_body;
    _ = snapshot_import;
    _ = converter;
    _ = legacy_lz4;
}
