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

pub const Error = Protocol.Error2;

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

fn protocol(self: *Connection) Protocol {
    return .{ .reader = &self.reader.interface, .writer = &self.writer.interface };
}

fn mapError(err: anyerror, self: *Connection) anyerror {
    return switch (err) {
        error.ReadFailed => self.reader.err orelse error.ReadFailed,
        error.WriteFailed => self.writer.err orelse error.WriteFailed,
        else => err,
    };
}

// --- String commands ---

/// GET key - Get the value of a key
pub fn get(self: *Connection, key: []const u8, buf: []u8) !?[]u8 {
    const p = self.protocol();
    return p.execBulkString(&.{ "GET", key }, buf) catch |err| mapError(err, self);
}

/// SET key value [EX seconds] - Set the string value of a key
pub fn set(self: *Connection, key: []const u8, value: []const u8, opts: SetOpts) !void {
    const p = self.protocol();

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

    // SET with NX/XX can return +OK or nil, SET with GET returns bulk string or nil
    // For simplicity, we just ignore the response value here
    p.writeCommand(args_buf[0..args_count]) catch |err| return mapError(err, self);
    const line = p.reader.takeDelimiterInclusive('\n') catch |err| return mapError(err, self);
    if (line.len < 2 or line[line.len - 2] != '\r') return error.ProtocolError;

    // Accept +OK (simple string), $-1 (nil), or $N (bulk string with old value for GET)
    if (line[0] == '+') return; // OK
    if (line[0] == '$') {
        // Bulk string response (SET GET) - skip the value
        const len_str = line[1 .. line.len - 2];
        const len = std.fmt.parseInt(i64, len_str, 10) catch return error.ProtocolError;
        if (len == -1) return; // nil is fine (NX/XX failed or GET on non-existent)
        if (len > 0) {
            // Skip the value bytes + \r\n
            const size: usize = @intCast(len);
            var skip_buf: [1024]u8 = undefined;
            var remaining = size;
            while (remaining > 0) {
                const to_skip = @min(remaining, skip_buf.len);
                p.reader.readSliceAll(skip_buf[0..to_skip]) catch |err| return mapError(err, self);
                remaining -= to_skip;
            }
            _ = p.reader.takeDelimiterInclusive('\n') catch |err| return mapError(err, self);
        }
        return;
    }
    if (line[0] == '-') return error.RedisError;
    return error.UnexpectedType;
}

pub const SetOpts = struct {
    ex: ?u32 = null, // expire seconds
    nx: bool = false, // only set if not exists
    xx: bool = false, // only set if exists
    get: bool = false, // return old value
};

/// DEL key [key ...] - Delete one or more keys
pub fn del(self: *Connection, keys: []const []const u8) !i64 {
    const p = self.protocol();

    var args_buf: [65][]const u8 = undefined;
    args_buf[0] = "DEL";
    @memcpy(args_buf[1 .. 1 + keys.len], keys);

    return p.execInteger(args_buf[0 .. 1 + keys.len]) catch |err| mapError(err, self);
}

/// INCR key - Increment the integer value of a key by one
pub fn incr(self: *Connection, key: []const u8) !i64 {
    const p = self.protocol();
    return p.execInteger(&.{ "INCR", key }) catch |err| mapError(err, self);
}

/// INCRBY key increment - Increment the integer value of a key by the given amount
pub fn incrBy(self: *Connection, key: []const u8, delta: i64) !i64 {
    const p = self.protocol();
    var delta_buf: [32]u8 = undefined;
    const delta_str = std.fmt.bufPrint(&delta_buf, "{d}", .{delta}) catch unreachable;
    return p.execInteger(&.{ "INCRBY", key, delta_str }) catch |err| mapError(err, self);
}

/// DECR key - Decrement the integer value of a key by one
pub fn decr(self: *Connection, key: []const u8) !i64 {
    const p = self.protocol();
    return p.execInteger(&.{ "DECR", key }) catch |err| mapError(err, self);
}

/// DECRBY key decrement - Decrement the integer value of a key by the given amount
pub fn decrBy(self: *Connection, key: []const u8, delta: i64) !i64 {
    const p = self.protocol();
    var delta_buf: [32]u8 = undefined;
    const delta_str = std.fmt.bufPrint(&delta_buf, "{d}", .{delta}) catch unreachable;
    return p.execInteger(&.{ "DECRBY", key, delta_str }) catch |err| mapError(err, self);
}

/// EXPIRE key seconds - Set a timeout on key
pub fn expire(self: *Connection, key: []const u8, seconds: u32) !bool {
    const p = self.protocol();
    var seconds_buf: [32]u8 = undefined;
    const seconds_str = std.fmt.bufPrint(&seconds_buf, "{d}", .{seconds}) catch unreachable;
    const result = p.execInteger(&.{ "EXPIRE", key, seconds_str }) catch |err| return mapError(err, self);
    return result == 1;
}

/// TTL key - Get the time to live for a key in seconds
pub fn ttl(self: *Connection, key: []const u8) !i64 {
    const p = self.protocol();
    return p.execInteger(&.{ "TTL", key }) catch |err| mapError(err, self);
}

/// EXISTS key [key ...] - Determine if keys exist
pub fn exists(self: *Connection, keys: []const []const u8) !i64 {
    const p = self.protocol();

    var args_buf: [65][]const u8 = undefined;
    args_buf[0] = "EXISTS";
    @memcpy(args_buf[1 .. 1 + keys.len], keys);

    return p.execInteger(args_buf[0 .. 1 + keys.len]) catch |err| mapError(err, self);
}

// --- Server commands ---

/// PING [message] - Ping the server
pub fn ping(self: *Connection, message: ?[]const u8) !void {
    const p = self.protocol();
    if (message) |msg| {
        var buf: [0]u8 = undefined;
        _ = p.execBulkString(&.{ "PING", msg }, &buf) catch |err| return mapError(err, self);
    } else {
        p.execSimpleString(&.{"PING"}) catch |err| return mapError(err, self);
    }
}

/// FLUSHDB - Remove all keys from the current database
pub fn flushDB(self: *Connection) !void {
    const p = self.protocol();
    p.execSimpleString(&.{"FLUSHDB"}) catch |err| return mapError(err, self);
}

/// DBSIZE - Return the number of keys in the current database
pub fn dbSize(self: *Connection) !i64 {
    const p = self.protocol();
    return p.execInteger(&.{"DBSIZE"}) catch |err| mapError(err, self);
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
