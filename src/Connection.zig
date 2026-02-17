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

pub const Error = Protocol.Error;

pub fn connect(self: *Connection, gpa: Allocator, host: []const u8, port: u16, options: Options) !void {
    const stream = try zio.net.tcpConnectToHost(host, port, .{
        .timeout = options.connect_timeout,
    });
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

fn protocol(self: *Connection) Protocol {
    return .{ .reader = &self.reader.interface, .writer = &self.writer.interface };
}

fn call(self: *Connection, comptime func: anytype, args: anytype) !Payload(@TypeOf(func)) {
    const p = self.protocol();
    return @call(.auto, func, .{p} ++ args) catch |err| {
        switch (err) {
            error.ReadFailed => return self.reader.err orelse error.ReadFailed,
            error.WriteFailed => return self.writer.err orelse error.WriteFailed,
            else => return err,
        }
    };
}

fn Payload(comptime F: type) type {
    const Return = @typeInfo(F).@"fn".return_type.?;
    return @typeInfo(Return).error_union.payload;
}

// --- String commands ---

/// GET key - Get the value of a key
pub fn get(self: *Connection, key: []const u8, buf: []u8) !?[]u8 {
    return self.call(Protocol.execBulkString, .{ &.{ "GET", key }, buf });
}

/// SET key value [EX seconds] - Set the string value of a key
pub fn set(self: *Connection, key: []const u8, value: []const u8, opts: SetOpts) !void {
    var args_buf: [8][]const u8 = undefined;
    var args_count: usize = 0;
    args_buf[args_count] = "SET";
    args_count += 1;
    args_buf[args_count] = key;
    args_count += 1;
    args_buf[args_count] = value;
    args_count += 1;

    var ex_buf: [32]u8 = undefined;
    if (opts.ex) |seconds| {
        args_buf[args_count] = "EX";
        args_count += 1;
        const ex_str = std.fmt.bufPrint(&ex_buf, "{d}", .{seconds}) catch unreachable;
        args_buf[args_count] = ex_str;
        args_count += 1;
    }

    if (opts.nx) {
        args_buf[args_count] = "NX";
        args_count += 1;
    } else if (opts.xx) {
        args_buf[args_count] = "XX";
        args_count += 1;
    }

    if (opts.get) {
        args_buf[args_count] = "GET";
        args_count += 1;
    }

    try self.call(Protocol.execOkOrNil, .{args_buf[0..args_count]});
}

pub const SetOpts = struct {
    ex: ?u32 = null, // expire seconds
    nx: bool = false, // only set if not exists
    xx: bool = false, // only set if exists
    get: bool = false, // return old value
};

/// DEL key [key ...] - Delete one or more keys
pub fn del(self: *Connection, keys: []const []const u8) !i64 {
    if (keys.len > 64) return error.TooManyKeys;
    var args_buf: [65][]const u8 = undefined;
    args_buf[0] = "DEL";
    @memcpy(args_buf[1 .. 1 + keys.len], keys);
    return self.call(Protocol.execInteger, .{args_buf[0 .. 1 + keys.len]});
}

/// INCR key - Increment the integer value of a key by one
pub fn incr(self: *Connection, key: []const u8) !i64 {
    return self.call(Protocol.execInteger, .{&.{ "INCR", key }});
}

/// INCRBY key increment - Increment the integer value of a key by the given amount
pub fn incrBy(self: *Connection, key: []const u8, delta: i64) !i64 {
    var delta_buf: [32]u8 = undefined;
    const delta_str = std.fmt.bufPrint(&delta_buf, "{d}", .{delta}) catch unreachable;
    return self.call(Protocol.execInteger, .{&.{ "INCRBY", key, delta_str }});
}

/// DECR key - Decrement the integer value of a key by one
pub fn decr(self: *Connection, key: []const u8) !i64 {
    return self.call(Protocol.execInteger, .{&.{ "DECR", key }});
}

/// DECRBY key decrement - Decrement the integer value of a key by the given amount
pub fn decrBy(self: *Connection, key: []const u8, delta: i64) !i64 {
    var delta_buf: [32]u8 = undefined;
    const delta_str = std.fmt.bufPrint(&delta_buf, "{d}", .{delta}) catch unreachable;
    return self.call(Protocol.execInteger, .{&.{ "DECRBY", key, delta_str }});
}

/// EXPIRE key seconds - Set a timeout on key
pub fn expire(self: *Connection, key: []const u8, seconds: u32) !bool {
    var seconds_buf: [32]u8 = undefined;
    const seconds_str = std.fmt.bufPrint(&seconds_buf, "{d}", .{seconds}) catch unreachable;
    const result = try self.call(Protocol.execInteger, .{&.{ "EXPIRE", key, seconds_str }});
    return result == 1;
}

/// TTL key - Get the time to live for a key in seconds
pub fn ttl(self: *Connection, key: []const u8) !i64 {
    return self.call(Protocol.execInteger, .{&.{ "TTL", key }});
}

/// EXISTS key [key ...] - Determine if keys exist
pub fn exists(self: *Connection, keys: []const []const u8) !i64 {
    if (keys.len > 64) return error.TooManyKeys;
    var args_buf: [65][]const u8 = undefined;
    args_buf[0] = "EXISTS";
    @memcpy(args_buf[1 .. 1 + keys.len], keys);
    return self.call(Protocol.execInteger, .{args_buf[0 .. 1 + keys.len]});
}

// --- Server commands ---

/// PING [message] - Ping the server
pub fn ping(self: *Connection, message: ?[]const u8) !void {
    if (message) |msg| {
        var buf: [0]u8 = undefined;
        _ = try self.call(Protocol.execBulkString, .{ &.{ "PING", msg }, &buf });
    } else {
        try self.call(Protocol.execSimpleString, .{&.{"PING"}});
    }
}

/// FLUSHDB - Remove all keys from the current database
pub fn flushDB(self: *Connection) !void {
    try self.call(Protocol.execSimpleString, .{&.{"FLUSHDB"}});
}

/// DBSIZE - Return the number of keys in the current database
pub fn dbSize(self: *Connection) !i64 {
    return self.call(Protocol.execInteger, .{&.{"DBSIZE"}});
}

// --- Tests ---

const testing = @import("testing.zig");

test "simple get/set" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("test_key", "test_value", .{});

    var buf: [1024]u8 = undefined;
    const value = try conn.get("test_key", &buf);

    try std.testing.expect(value != null);
    try std.testing.expectEqualStrings("test_value", value.?);
}

test "get non-existent key returns null" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    var buf: [1024]u8 = undefined;
    const value = try conn.get("non_existent_key_12345", &buf);

    try std.testing.expect(value == null);
}

test "set with expiration" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("ttl_key", "ttl_value", .{ .ex = 60 });

    var buf: [1024]u8 = undefined;
    const value = try conn.get("ttl_key", &buf);

    try std.testing.expect(value != null);
    try std.testing.expectEqualStrings("ttl_value", value.?);

    const ttl_val = try conn.ttl("ttl_key");
    try std.testing.expect(ttl_val > 0 and ttl_val <= 60);
}

test "del" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("delete_key", "to_be_deleted", .{});
    const deleted = try conn.del(&.{"delete_key"});
    try std.testing.expectEqual(@as(i64, 1), deleted);

    var buf: [1024]u8 = undefined;
    const value = try conn.get("delete_key", &buf);
    try std.testing.expect(value == null);
}

test "incr/decr" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("counter", "10", .{});

    const val1 = try conn.incrBy("counter", 5);
    try std.testing.expectEqual(@as(i64, 15), val1);

    const val2 = try conn.decrBy("counter", 3);
    try std.testing.expectEqual(@as(i64, 12), val2);

    const val3 = try conn.incr("counter");
    try std.testing.expectEqual(@as(i64, 13), val3);

    const val4 = try conn.decr("counter");
    try std.testing.expectEqual(@as(i64, 12), val4);
}

test "exists" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("exists_key1", "value1", .{});
    try conn.set("exists_key2", "value2", .{});

    const count = try conn.exists(&.{ "exists_key1", "exists_key2", "nonexistent" });
    try std.testing.expectEqual(@as(i64, 2), count);
}

test "ping" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.ping(null);
}

test "expire and ttl" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("expire_test", "value", .{});

    const was_set = try conn.expire("expire_test", 100);
    try std.testing.expect(was_set);

    const ttl_val = try conn.ttl("expire_test");
    try std.testing.expect(ttl_val > 0 and ttl_val <= 100);
}

test "set NX (only if not exists)" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    // Delete first to ensure clean state
    _ = try conn.del(&.{"nx_test_key"});

    // First set with NX should succeed
    try conn.set("nx_test_key", "first", .{ .nx = true });

    var buf: [1024]u8 = undefined;
    const value = try conn.get("nx_test_key", &buf);
    try std.testing.expectEqualStrings("first", value.?);

    // Second set with NX should fail silently (Redis returns nil, but we don't check)
    // This is different from memcached - Redis SET NX doesn't error
    try conn.set("nx_test_key", "second", .{ .nx = true });

    // Value should still be "first"
    const value2 = try conn.get("nx_test_key", &buf);
    try std.testing.expectEqualStrings("first", value2.?);
}
