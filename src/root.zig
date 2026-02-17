const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Client = @import("Client.zig");
pub const Connection = @import("Connection.zig");
pub const Pipeline = @import("Pipeline.zig");
pub const Pool = @import("Pool.zig");
pub const Protocol = @import("Protocol.zig");

// Re-export types
pub const SetOpts = Connection.SetOpts;
pub const Error = Connection.Error;

/// Connect to a single Redis server with default options.
pub fn connect(gpa: Allocator, server: []const u8) !Client {
    return Client.init(gpa, server, .{});
}

test {
    _ = Client;
    _ = Connection;
    _ = Pipeline;
    _ = Pool;
    _ = Protocol;
    _ = @import("testing.zig");
}
