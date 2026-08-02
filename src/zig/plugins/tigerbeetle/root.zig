const std = @import("std");
const exchange_core = @import("exchange_core");

pub const settlement = @import("tigerbeetle_settlement.zig");
pub const outbox = @import("tigerbeetle_outbox.zig");
pub const destination_registry = @import("tigerbeetle_destination_registry.zig");
pub const dispatcher = @import("tigerbeetle_dispatcher.zig");
pub const exchange_integration = @import("tigerbeetle_exchange_integration.zig");
pub const parix_adapter = @import("tigerbeetle_parix_adapter.zig");
pub const plugin = @import("tigerbeetle_plugin.zig");

test {
    _ = settlement;
    _ = outbox;
    _ = destination_registry;
    _ = dispatcher;
    _ = exchange_integration;
    _ = parix_adapter;
    _ = plugin;
}

test "exchange core does not re-export the opt-in TigerBeetle plugin" {
    inline for (.{
        "tigerbeetle_settlement",
        "tigerbeetle_outbox",
        "tigerbeetle_destination_registry",
        "tigerbeetle_dispatcher",
        "tigerbeetle_exchange_integration",
        "tigerbeetle_parix_adapter",
        "tigerbeetle_plugin",
    }) |declaration| {
        try std.testing.expect(!@hasDecl(exchange_core.core, declaration));
    }
}
