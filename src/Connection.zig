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
    connect_timeout: zio.Timeout = .none,
    read_timeout: zio.Timeout = .none,
    write_timeout: zio.Timeout = .none,
};

// Re-export Protocol types for convenience
pub const Info = Protocol.Info;
pub const GetOpts = Protocol.GetOpts;
pub const SetOpts = Protocol.SetOpts;
pub const Error = Protocol.Error;

pub fn connect(self: *Connection, gpa: Allocator, host: []const u8, port: u16, options: Options) !void {
    const stream = zio.net.tcpConnectToHost(host, port, .{
        .timeout = options.connect_timeout,
    }) catch return error.ConnectionFailed;
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

    self.reader.setTimeout(options.read_timeout);
    self.writer.setTimeout(options.write_timeout);
}

pub fn close(self: *Connection) void {
    self.stream.close();
    self.gpa.free(self.read_buffer);
    self.gpa.free(self.write_buffer);
}

fn call(self: *Connection, comptime func: anytype, args: anytype) !Payload(@TypeOf(func)) {
    const p: Protocol = .{ .reader = &self.reader.interface, .writer = &self.writer.interface };
    return @call(.auto, func, .{p} ++ args) catch |err| switch (err) {
        error.ReadFailed => return self.reader.err orelse error.ReadFailed,
        error.WriteFailed => return self.writer.err orelse error.WriteFailed,
        else => |e| return e,
    };
}

fn Payload(comptime F: type) type {
    const Return = @typeInfo(F).@"fn".return_type.?;
    return @typeInfo(Return).error_union.payload;
}

pub fn get(self: *Connection, key: []const u8, buf: []u8, opts: GetOpts) !?Info {
    return self.call(Protocol.get, .{ key, buf, opts });
}

pub fn set(self: *Connection, key: []const u8, value: []const u8, opts: SetOpts, mode: Protocol.SetMode) !void {
    return self.call(Protocol.set, .{ key, value, opts, mode });
}

pub fn delete(self: *Connection, key: []const u8) !void {
    return self.call(Protocol.delete, .{key});
}

pub fn incr(self: *Connection, key: []const u8, delta: u64) !u64 {
    return self.call(Protocol.incr, .{ key, delta });
}

pub fn decr(self: *Connection, key: []const u8, delta: u64) !u64 {
    return self.call(Protocol.decr, .{ key, delta });
}

pub fn touch(self: *Connection, key: []const u8, ttl: u32) !void {
    return self.call(Protocol.touch, .{ key, ttl });
}

pub fn flushAll(self: *Connection) !void {
    return self.call(Protocol.flushAll, .{});
}

pub fn version(self: *Connection, buf: []u8) ![]u8 {
    return self.call(Protocol.version, .{buf});
}

// --- Tests ---

const testing = @import("testing.zig");

test "simple get/set" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("test_key", "test_value", .{}, .set);

    var buf: [1024]u8 = undefined;
    const info = try conn.get("test_key", &buf, .{});

    try std.testing.expect(info != null);
    try std.testing.expectEqualStrings("test_value", info.?.value);
}

test "get non-existent key returns null" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    var buf: [1024]u8 = undefined;
    const info = try conn.get("non_existent_key_12345", &buf, .{});

    try std.testing.expect(info == null);
}

test "set with TTL and flags" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("ttl_key", "ttl_value", .{ .ttl = 60, .flags = 42 }, .set);

    var buf: [1024]u8 = undefined;
    const info = try conn.get("ttl_key", &buf, .{});

    try std.testing.expect(info != null);
    try std.testing.expectEqualStrings("ttl_value", info.?.value);
    try std.testing.expectEqual(@as(u32, 42), info.?.flags);
}

test "delete" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("delete_key", "to_be_deleted", .{}, .set);
    try conn.delete("delete_key");

    var buf: [1024]u8 = undefined;
    const info = try conn.get("delete_key", &buf, .{});
    try std.testing.expect(info == null);
}

test "incr/decr" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("counter", "10", .{}, .set);

    const val1 = try conn.incr("counter", 5);
    try std.testing.expectEqual(@as(u64, 15), val1);

    const val2 = try conn.decr("counter", 3);
    try std.testing.expectEqual(@as(u64, 12), val2);
}

test "version" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    var buf: [64]u8 = undefined;
    const ver = try conn.version(&buf);

    try std.testing.expect(ver.len > 0);
}

test "add only if not exists" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    // Delete first to ensure clean state
    conn.delete("add_key") catch {};

    // First add should succeed
    try conn.set("add_key", "first", .{}, .add);

    // Second add should fail
    try std.testing.expectError(error.NotStored, conn.set("add_key", "second", .{}, .add));

    // Value should still be "first"
    var buf: [1024]u8 = undefined;
    const info = try conn.get("add_key", &buf, .{});
    try std.testing.expectEqualStrings("first", info.?.value);
}

test "replace only if exists" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    // Delete first to ensure clean state
    conn.delete("replace_key") catch {};

    // Replace on non-existent should fail
    try std.testing.expectError(error.NotStored, conn.set("replace_key", "value", .{}, .replace));

    // Set the key
    try conn.set("replace_key", "original", .{}, .set);

    // Replace should now succeed
    try conn.set("replace_key", "replaced", .{}, .replace);

    var buf: [1024]u8 = undefined;
    const info = try conn.get("replace_key", &buf, .{});
    try std.testing.expectEqualStrings("replaced", info.?.value);
}

test "append/prepend" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("concat_key", "hello", .{}, .set);

    try conn.set("concat_key", " world", .{}, .append);

    var buf: [1024]u8 = undefined;
    const info1 = try conn.get("concat_key", &buf, .{});
    try std.testing.expectEqualStrings("hello world", info1.?.value);

    try conn.set("concat_key", "say ", .{}, .prepend);

    const info2 = try conn.get("concat_key", &buf, .{});
    try std.testing.expectEqualStrings("say hello world", info2.?.value);
}

test "CAS (compare and swap)" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("cas_key", "original", .{}, .set);

    var buf: [1024]u8 = undefined;
    const info = try conn.get("cas_key", &buf, .{});
    const cas_token = info.?.cas;

    // CAS with correct token should succeed
    try conn.set("cas_key", "updated", .{ .cas = cas_token }, .set);

    // CAS with old token should fail
    try std.testing.expectError(error.Exists, conn.set("cas_key", "again", .{ .cas = cas_token }, .set));
}
