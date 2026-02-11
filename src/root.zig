const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Client = @import("Client.zig");
pub const Connection = @import("Connection.zig");
pub const Pool = @import("Pool.zig");
pub const Protocol = @import("Protocol.zig");
pub const Server = @import("Server.zig");
pub const Hasher = @import("hasher.zig").Hasher;

/// Connect to a single memcached server with default options.
pub fn connect(gpa: Allocator, server: []const u8) !Client {
    return Client.init(gpa, .{ .servers = &.{server} });
}

test {
    _ = Client;
    _ = Connection;
    _ = Pool;
    _ = Protocol;
    _ = Server;
    _ = Hasher;
    _ = @import("testing.zig");
}
