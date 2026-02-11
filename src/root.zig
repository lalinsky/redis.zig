const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Client = @import("Client.zig");
pub const Protocol = @import("Protocol.zig");

/// Connect to a single memcached server with default options.
pub fn connect(gpa: Allocator, server: []const u8) !Client {
    return Client.init(gpa, .{ .servers = &.{server} });
}

test {
    _ = Client;
    _ = Protocol;
}
