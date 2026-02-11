const std = @import("std");
const Allocator = std.mem.Allocator;
const Connection = @import("Connection.zig");
const Pool = @import("Pool.zig");

const Client = @This();

pool: Pool,

pub const Options = struct {
    servers: []const []const u8 = &.{},
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
    if (options.servers.len == 0) return error.ConnectionFailed;

    const server = options.servers[0];
    const host, const port = parseServer(server) orelse return error.ConnectionFailed;

    return .{
        .pool = Pool.init(gpa, host, port, .{
            .max_idle = options.max_idle,
            .read_buffer_size = options.read_buffer_size,
            .write_buffer_size = options.write_buffer_size,
        }),
    };
}

pub fn deinit(self: *Client) void {
    self.pool.deinit();
}

fn withConnection(self: *Client, comptime func: anytype, args: anytype) !ReturnType(func) {
    const conn = try self.pool.acquire();
    var ok = false;
    defer self.pool.release(conn, ok);

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

pub fn get(self: *Client, key: []const u8, buf: []u8, opts: GetOpts) !?Info {
    return self.withConnection(Connection.get, .{ key, buf, opts });
}

pub fn set(self: *Client, key: []const u8, value: []const u8, opts: SetOpts) !void {
    return self.withConnection(Connection.set, .{ key, value, opts, .set });
}

pub fn add(self: *Client, key: []const u8, value: []const u8, opts: SetOpts) !void {
    return self.withConnection(Connection.set, .{ key, value, opts, .add });
}

pub fn replace(self: *Client, key: []const u8, value: []const u8, opts: SetOpts) !void {
    return self.withConnection(Connection.set, .{ key, value, opts, .replace });
}

pub fn append(self: *Client, key: []const u8, value: []const u8) !void {
    return self.withConnection(Connection.set, .{ key, value, .{}, .append });
}

pub fn prepend(self: *Client, key: []const u8, value: []const u8) !void {
    return self.withConnection(Connection.set, .{ key, value, .{}, .prepend });
}

pub fn delete(self: *Client, key: []const u8) !void {
    return self.withConnection(Connection.delete, .{key});
}

pub fn incr(self: *Client, key: []const u8, delta: u64) !u64 {
    return self.withConnection(Connection.incr, .{ key, delta });
}

pub fn decr(self: *Client, key: []const u8, delta: u64) !u64 {
    return self.withConnection(Connection.decr, .{ key, delta });
}

pub fn touch(self: *Client, key: []const u8, ttl: u32) !void {
    return self.withConnection(Connection.touch, .{ key, ttl });
}

pub fn flushAll(self: *Client) !void {
    return self.withConnection(Connection.flushAll, .{});
}

pub fn version(self: *Client, buf: []u8) ![]u8 {
    return self.withConnection(Connection.version, .{buf});
}

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
