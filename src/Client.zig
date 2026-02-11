const std = @import("std");
const Allocator = std.mem.Allocator;
const Connection = @import("Connection.zig");
const Pool = @import("Pool.zig");
const Server = @import("Server.zig");
const Hasher = @import("hasher.zig").Hasher;

const Client = @This();

gpa: Allocator,
servers: []Server,
hasher: Hasher,
round_robin: std.atomic.Value(usize),

pub const Options = struct {
    servers: []const []const u8 = &.{},
    hasher: Hasher = .none,
    max_idle: usize = 2,
    read_buffer_size: usize = 4096,
    write_buffer_size: usize = 4096,
};

// Re-export types for convenience
pub const Info = Connection.Info;
pub const GetOpts = Connection.GetOpts;
pub const SetOpts = Connection.SetOpts;
pub const Error = Connection.Error;

pub fn init(gpa: Allocator, options: Options) !Client {
    if (options.servers.len == 0) return error.NoServers;

    const servers = try gpa.alloc(Server, options.servers.len);
    errdefer gpa.free(servers);

    const pool_opts: Pool.Options = .{
        .max_idle = options.max_idle,
        .read_buffer_size = options.read_buffer_size,
        .write_buffer_size = options.write_buffer_size,
    };

    for (options.servers, 0..) |server_str, i| {
        const host, const port = parseServer(server_str) orelse return error.InvalidServer;
        servers[i] = Server.init(gpa, host, port, pool_opts);
    }

    return .{
        .gpa = gpa,
        .servers = servers,
        .hasher = options.hasher,
        .round_robin = std.atomic.Value(usize).init(0),
    };
}

pub fn deinit(self: *Client) void {
    for (self.servers) |*server| {
        server.deinit();
    }
    self.gpa.free(self.servers);
}

fn pickServer(self: *Client, key: []const u8) *Server {
    if (self.servers.len == 1) return &self.servers[0];

    const index = switch (self.hasher) {
        .none => self.round_robin.fetchAdd(1, .monotonic) % self.servers.len,
        else => self.hasher.pick(self.servers, key),
    };
    return &self.servers[index];
}

fn withConnection(self: *Client, key: []const u8, comptime func: anytype, args: anytype) !ReturnType(func) {
    const server = self.pickServer(key);
    const conn = try server.pool.acquire();
    var ok = false;
    defer server.pool.release(conn, ok);

    const result = try @call(.auto, func, .{conn} ++ args);
    ok = true;
    return result;
}

fn withAnyConnection(self: *Client, comptime func: anytype, args: anytype) !ReturnType(func) {
    // For commands that don't have a key, use round-robin
    const index = self.round_robin.fetchAdd(1, .monotonic) % self.servers.len;
    const server = &self.servers[index];
    const conn = try server.pool.acquire();
    var ok = false;
    defer server.pool.release(conn, ok);

    const result = try @call(.auto, func, .{conn} ++ args);
    ok = true;
    return result;
}

fn ReturnType(comptime func: anytype) type {
    const Return = @typeInfo(@TypeOf(func)).@"fn".return_type.?;
    return @typeInfo(Return).error_union.payload;
}

fn parseServer(server: []const u8) ?struct { []const u8, u16 } {
    const colon_pos = std.mem.lastIndexOfScalar(u8, server, ':') orelse return null;
    const host = server[0..colon_pos];
    const port = std.fmt.parseInt(u16, server[colon_pos + 1 ..], 10) catch return null;
    return .{ host, port };
}

// --- Key-based operations (use pickServer) ---

pub fn get(self: *Client, key: []const u8, buf: []u8, opts: GetOpts) !?Info {
    return self.withConnection(key, Connection.get, .{ key, buf, opts });
}

pub fn set(self: *Client, key: []const u8, value: []const u8, opts: SetOpts) !void {
    return self.withConnection(key, Connection.set, .{ key, value, opts, .set });
}

pub fn add(self: *Client, key: []const u8, value: []const u8, opts: SetOpts) !void {
    return self.withConnection(key, Connection.set, .{ key, value, opts, .add });
}

pub fn replace(self: *Client, key: []const u8, value: []const u8, opts: SetOpts) !void {
    return self.withConnection(key, Connection.set, .{ key, value, opts, .replace });
}

pub fn append(self: *Client, key: []const u8, value: []const u8) !void {
    return self.withConnection(key, Connection.set, .{ key, value, .{}, .append });
}

pub fn prepend(self: *Client, key: []const u8, value: []const u8) !void {
    return self.withConnection(key, Connection.set, .{ key, value, .{}, .prepend });
}

pub fn delete(self: *Client, key: []const u8) !void {
    return self.withConnection(key, Connection.delete, .{key});
}

pub fn incr(self: *Client, key: []const u8, delta: u64) !u64 {
    return self.withConnection(key, Connection.incr, .{ key, delta });
}

pub fn decr(self: *Client, key: []const u8, delta: u64) !u64 {
    return self.withConnection(key, Connection.decr, .{ key, delta });
}

pub fn touch(self: *Client, key: []const u8, ttl: u32) !void {
    return self.withConnection(key, Connection.touch, .{ key, ttl });
}

// --- Non-key operations ---

pub fn version(self: *Client, buf: []u8) ![]u8 {
    return self.withAnyConnection(Connection.version, .{buf});
}

// --- Tests ---

test "parseServer" {
    {
        const result = parseServer("localhost:11211");
        try std.testing.expect(result != null);
        try std.testing.expectEqualStrings("localhost", result.?[0]);
        try std.testing.expectEqual(11211, result.?[1]);
    }
    {
        const result = parseServer("invalid");
        try std.testing.expect(result == null);
    }
}

test "parseServer ipv6" {
    const result = parseServer("[::1]:11211");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("[::1]", result.?[0]);
    try std.testing.expectEqual(11211, result.?[1]);
}
