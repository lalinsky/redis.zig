const std = @import("std");
const zio = @import("zio");
const Allocator = std.mem.Allocator;
const Protocol = @import("Protocol.zig");

const Client = @This();

gpa: Allocator,
stream: zio.net.Stream,
reader: zio.net.Stream.Reader,
writer: zio.net.Stream.Writer,
read_buffer: []u8,
write_buffer: []u8,

pub const Options = struct {
    servers: []const []const u8 = &.{},
    read_buffer_size: usize = 4096,
    write_buffer_size: usize = 4096,
};

// Re-export Protocol types for convenience
pub const Info = Protocol.Info;
pub const GetOpts = Protocol.GetOpts;
pub const SetOpts = Protocol.SetOpts;
pub const Error = Protocol.Error;

pub fn init(gpa: Allocator, options: Options) !Client {
    if (options.servers.len == 0) return error.ConnectionFailed;

    const server = options.servers[0];
    const host, const port = parseServer(server) orelse return error.ConnectionFailed;

    const stream = zio.net.tcpConnectToHost(host, port, .{}) catch return error.ConnectionFailed;
    errdefer stream.close();

    const read_buffer = try gpa.alloc(u8, options.read_buffer_size);
    errdefer gpa.free(read_buffer);

    const write_buffer = try gpa.alloc(u8, options.write_buffer_size);
    errdefer gpa.free(write_buffer);

    return .{
        .gpa = gpa,
        .stream = stream,
        .reader = stream.reader(read_buffer),
        .writer = stream.writer(write_buffer),
        .read_buffer = read_buffer,
        .write_buffer = write_buffer,
    };
}

pub fn deinit(self: *Client) void {
    self.stream.close();
    self.gpa.free(self.read_buffer);
    self.gpa.free(self.write_buffer);
}

fn call(self: *Client, comptime name: []const u8, args: anytype) !CallPayload(name) {
    const proto_fn = @field(Protocol, name);
    const p: Protocol = .{ .reader = &self.reader.interface, .writer = &self.writer.interface };
    return @call(.auto, proto_fn, .{p} ++ args) catch |err| switch (err) {
        error.ReadFailed => return self.reader.err orelse error.ReadFailed,
        error.WriteFailed => return self.writer.err orelse error.ReadFailed,
        else => |e| return e,
    };
}

fn CallPayload(comptime name: []const u8) type {
    const Return = @typeInfo(@TypeOf(@field(Protocol, name))).@"fn".return_type.?;
    return @typeInfo(Return).error_union.payload;
}

fn parseServer(server: []const u8) ?struct { []const u8, u16 } {
    const colon_pos = std.mem.lastIndexOfScalar(u8, server, ':') orelse return null;
    const host = server[0..colon_pos];
    const port = std.fmt.parseInt(u16, server[colon_pos + 1 ..], 10) catch return null;
    return .{ host, port };
}

pub fn get(self: *Client, key: []const u8, buf: []u8, opts: GetOpts) !?Info {
    return self.call("get", .{ key, buf, opts });
}

pub fn set(self: *Client, key: []const u8, value: []const u8, opts: SetOpts) !void {
    return self.call("set", .{ key, value, opts, .set });
}

pub fn add(self: *Client, key: []const u8, value: []const u8, opts: SetOpts) !void {
    return self.call("set", .{ key, value, opts, .add });
}

pub fn replace(self: *Client, key: []const u8, value: []const u8, opts: SetOpts) !void {
    return self.call("set", .{ key, value, opts, .replace });
}

pub fn append(self: *Client, key: []const u8, value: []const u8) !void {
    return self.call("set", .{ key, value, .{}, .append });
}

pub fn prepend(self: *Client, key: []const u8, value: []const u8) !void {
    return self.call("set", .{ key, value, .{}, .prepend });
}

pub fn delete(self: *Client, key: []const u8) !void {
    return self.call("delete", .{key});
}

pub fn incr(self: *Client, key: []const u8, delta: u64) !u64 {
    return self.call("incr", .{ key, delta });
}

pub fn decr(self: *Client, key: []const u8, delta: u64) !u64 {
    return self.call("decr", .{ key, delta });
}

pub fn touch(self: *Client, key: []const u8, ttl: u32) !void {
    return self.call("touch", .{ key, ttl });
}

pub fn flushAll(self: *Client) !void {
    return self.call("flushAll", .{});
}

pub fn version(self: *Client, buf: []u8) ![]u8 {
    return self.call("version", .{buf});
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
