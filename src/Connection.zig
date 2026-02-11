const std = @import("std");
const zio = @import("zio");
const Allocator = std.mem.Allocator;
const Protocol = @import("Protocol.zig");

const Connection = @This();

node: std.SinglyLinkedList.Node = .{},
gpa: Allocator,
stream: zio.net.Stream,
reader: zio.net.Stream.Reader,
writer: zio.net.Stream.Writer,
read_buffer: []u8,
write_buffer: []u8,

pub const Options = struct {
    read_buffer_size: usize = 4096,
    write_buffer_size: usize = 4096,
};

// Re-export Protocol types for convenience
pub const Info = Protocol.Info;
pub const GetOpts = Protocol.GetOpts;
pub const SetOpts = Protocol.SetOpts;
pub const Error = Protocol.Error;

pub fn connect(self: *Connection, gpa: Allocator, host: []const u8, port: u16, options: Options) !void {
    const stream = zio.net.tcpConnectToHost(host, port, .{}) catch return error.ConnectionFailed;
    errdefer stream.close();

    const read_buffer = try gpa.alloc(u8, options.read_buffer_size);
    errdefer gpa.free(read_buffer);

    const write_buffer = try gpa.alloc(u8, options.write_buffer_size);
    errdefer gpa.free(write_buffer);

    self.* = .{
        .gpa = gpa,
        .stream = stream,
        .reader = stream.reader(read_buffer),
        .writer = stream.writer(write_buffer),
        .read_buffer = read_buffer,
        .write_buffer = write_buffer,
    };
}

pub fn close(self: *Connection) void {
    self.stream.close();
    self.gpa.free(self.read_buffer);
    self.gpa.free(self.write_buffer);
}

fn call(self: *Connection, comptime name: []const u8, args: anytype) !CallPayload(name) {
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

pub fn get(self: *Connection, key: []const u8, buf: []u8, opts: GetOpts) !?Info {
    return self.call("get", .{ key, buf, opts });
}

pub fn set(self: *Connection, key: []const u8, value: []const u8, opts: SetOpts, mode: Protocol.SetMode) !void {
    return self.call("set", .{ key, value, opts, mode });
}

pub fn delete(self: *Connection, key: []const u8) !void {
    return self.call("delete", .{key});
}

pub fn incr(self: *Connection, key: []const u8, delta: u64) !u64 {
    return self.call("incr", .{ key, delta });
}

pub fn decr(self: *Connection, key: []const u8, delta: u64) !u64 {
    return self.call("decr", .{ key, delta });
}

pub fn touch(self: *Connection, key: []const u8, ttl: u32) !void {
    return self.call("touch", .{ key, ttl });
}

pub fn flushAll(self: *Connection) !void {
    return self.call("flushAll", .{});
}

pub fn version(self: *Connection, buf: []u8) ![]u8 {
    return self.call("version", .{buf});
}
