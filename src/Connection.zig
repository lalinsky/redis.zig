const std = @import("std");
const Allocator = std.mem.Allocator;
const Protocol = @import("Protocol.zig");
const Pipeline = @import("Pipeline.zig");

const Connection = @This();

pub const max_keys = 64;

pub const FieldValue = Protocol.FieldValue;

pub fn Result(comptime T: type) type {
    return struct {
        arena: *std.heap.ArenaAllocator,
        value: T,

        pub fn init(allocator: Allocator) !@This() {
            const arena = try allocator.create(std.heap.ArenaAllocator);
            errdefer allocator.destroy(arena);
            arena.* = std.heap.ArenaAllocator.init(allocator);
            return .{ .arena = arena, .value = undefined };
        }

        pub fn deinit(self: @This()) void {
            const allocator = self.arena.child_allocator;
            self.arena.deinit();
            allocator.destroy(self.arena);
        }
    };
}

node: std.SinglyLinkedList.Node = .{},
gpa: Allocator,
io: std.Io,
stream: std.Io.net.Stream,
reader: std.Io.net.Stream.Reader,
writer: std.Io.net.Stream.Writer,
read_buffer: []u8,
write_buffer: []u8,

pub const Options = struct {
    read_buffer_size: usize = 4096,
    write_buffer_size: usize = 4096,
    connect_timeout: std.Io.Timeout = .none,
    read_timeout: std.Io.Timeout = .none,
    write_timeout: std.Io.Timeout = .none,
};

pub const Error = Protocol.Error;

pub fn connect(self: *Connection, gpa: Allocator, io: std.Io, host: []const u8, port: u16, options: Options) !void {
    const stream = try (try std.Io.net.HostName.init(host)).connect(io, port, .{
        .mode = .stream,
        .timeout = options.connect_timeout,
    });
    errdefer stream.close(io);

    const read_buffer = try gpa.alloc(u8, options.read_buffer_size);
    errdefer gpa.free(read_buffer);

    const write_buffer = try gpa.alloc(u8, options.write_buffer_size);
    errdefer gpa.free(write_buffer);

    var reader = stream.reader(io, read_buffer);
    var writer = stream.writer(io, write_buffer);

    if (options.read_timeout != .none) {
        if (@hasField(std.Io.net.Stream.Reader, "timeout")) {
            reader.timeout = options.read_timeout;
        } else {
            @panic("read_timeout is set but std.Io.net.Stream.Reader does not support timeouts yet, see https://codeberg.org/ziglang/zig/issues/32166");
        }
    }

    if (options.write_timeout != .none) {
        if (@hasField(std.Io.net.Stream.Writer, "timeout")) {
            writer.timeout = options.write_timeout;
        } else {
            @panic("write_timeout is set but std.Io.net.Stream.Writer does not support timeouts yet, see https://codeberg.org/ziglang/zig/issues/32166");
        }
    }

    self.* = .{
        .gpa = gpa,
        .io = io,
        .stream = stream,
        .reader = reader,
        .writer = writer,
        .read_buffer = read_buffer,
        .write_buffer = write_buffer,
    };
}

pub fn close(self: *Connection) void {
    self.stream.close(self.io);
    self.gpa.free(self.read_buffer);
    self.gpa.free(self.write_buffer);
}

pub fn protocol(self: *Connection) Protocol {
    return .{ .reader = &self.reader.interface, .writer = &self.writer.interface };
}

pub fn pipeline(self: *Connection) Pipeline {
    return Pipeline.init(self, null);
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
    var ex_buf: [32]u8 = undefined;
    var cmd = opts.buildArgs(key, value, &ex_buf);
    try self.call(Protocol.execOkOrNil, .{cmd.args[0..cmd.len]});
}

pub const SetOpts = struct {
    ex: ?u32 = null, // expire seconds
    nx: bool = false, // only set if not exists
    xx: bool = false, // only set if exists
    get: bool = false, // return old value

    pub const SetArgs = struct { args: [8][]const u8 = undefined, len: usize = 0 };

    pub fn buildArgs(opts: SetOpts, key: []const u8, value: []const u8, ex_buf: *[32]u8) SetArgs {
        var result: SetArgs = .{};
        result.args[result.len] = "SET";
        result.len += 1;
        result.args[result.len] = key;
        result.len += 1;
        result.args[result.len] = value;
        result.len += 1;

        if (opts.ex) |seconds| {
            result.args[result.len] = "EX";
            result.len += 1;
            const ex_str = std.fmt.bufPrint(ex_buf, "{d}", .{seconds}) catch unreachable;
            result.args[result.len] = ex_str;
            result.len += 1;
        }

        if (opts.nx) {
            result.args[result.len] = "NX";
            result.len += 1;
        } else if (opts.xx) {
            result.args[result.len] = "XX";
            result.len += 1;
        }

        if (opts.get) {
            result.args[result.len] = "GET";
            result.len += 1;
        }

        return result;
    }
};

/// DEL key [key ...] - Delete one or more keys
pub fn del(self: *Connection, keys: []const []const u8) !i64 {
    if (keys.len > max_keys) return error.TooManyKeys;
    var args_buf: [max_keys + 1][]const u8 = undefined;
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
    if (keys.len > max_keys) return error.TooManyKeys;
    var args_buf: [max_keys + 1][]const u8 = undefined;
    args_buf[0] = "EXISTS";
    @memcpy(args_buf[1 .. 1 + keys.len], keys);
    return self.call(Protocol.execInteger, .{args_buf[0 .. 1 + keys.len]});
}

// --- Hash commands ---

/// HGET key field - Get the value of a hash field into caller-provided buffer
pub fn hget(self: *Connection, key: []const u8, field: []const u8, buf: []u8) !?[]u8 {
    return self.call(Protocol.execBulkString, .{ &.{ "HGET", key, field }, buf });
}

/// HSET key field value - Set a hash field, returns number of new fields added
pub fn hset(self: *Connection, key: []const u8, field: []const u8, value: []const u8) !i64 {
    return self.call(Protocol.execInteger, .{&.{ "HSET", key, field, value }});
}

/// HSET key field value [field value ...] - Set multiple hash fields
pub fn hmset(self: *Connection, key: []const u8, fields: []const FieldValue) !i64 {
    if (fields.len > max_keys) return error.TooManyKeys;
    var args_buf: [2 + max_keys * 2][]const u8 = undefined;
    args_buf[0] = "HSET";
    args_buf[1] = key;
    for (fields, 0..) |fv, i| {
        args_buf[2 + i * 2] = fv.field;
        args_buf[2 + i * 2 + 1] = fv.value;
    }
    return self.call(Protocol.execInteger, .{args_buf[0 .. 2 + fields.len * 2]});
}

/// HDEL key field [field ...] - Delete one or more hash fields
pub fn hdel(self: *Connection, key: []const u8, fields: []const []const u8) !i64 {
    if (fields.len > max_keys) return error.TooManyKeys;
    var args_buf: [max_keys + 2][]const u8 = undefined;
    args_buf[0] = "HDEL";
    args_buf[1] = key;
    @memcpy(args_buf[2 .. 2 + fields.len], fields);
    return self.call(Protocol.execInteger, .{args_buf[0 .. 2 + fields.len]});
}

/// HEXISTS key field - Determine if a hash field exists
pub fn hexists(self: *Connection, key: []const u8, field: []const u8) !bool {
    const result = try self.call(Protocol.execInteger, .{&.{ "HEXISTS", key, field }});
    return result == 1;
}

/// HLEN key - Get the number of fields in a hash
pub fn hlen(self: *Connection, key: []const u8) !i64 {
    return self.call(Protocol.execInteger, .{&.{ "HLEN", key }});
}

/// HINCRBY key field increment - Increment the integer value of a hash field
pub fn hincrby(self: *Connection, key: []const u8, field: []const u8, delta: i64) !i64 {
    var delta_buf: [32]u8 = undefined;
    const delta_str = std.fmt.bufPrint(&delta_buf, "{d}", .{delta}) catch unreachable;
    return self.call(Protocol.execInteger, .{&.{ "HINCRBY", key, field, delta_str }});
}

/// HGETALL key - Get all fields and values in a hash
pub fn hgetall(self: *Connection, allocator: Allocator, key: []const u8) !Result([]FieldValue) {
    var result = try Result([]FieldValue).init(allocator);
    errdefer result.deinit();
    result.value = try self.call(Protocol.execFieldPairsAlloc, .{ result.arena.allocator(), &.{ "HGETALL", key } });
    return result;
}

/// HKEYS key - Get all field names in a hash
pub fn hkeys(self: *Connection, allocator: Allocator, key: []const u8) !Result([][]u8) {
    var result = try Result([][]u8).init(allocator);
    errdefer result.deinit();
    result.value = try self.call(Protocol.execBulkStringArrayAlloc, .{ result.arena.allocator(), &.{ "HKEYS", key } });
    return result;
}

/// HVALS key - Get all values in a hash
pub fn hvals(self: *Connection, allocator: Allocator, key: []const u8) !Result([][]u8) {
    var result = try Result([][]u8).init(allocator);
    errdefer result.deinit();
    result.value = try self.call(Protocol.execBulkStringArrayAlloc, .{ result.arena.allocator(), &.{ "HVALS", key } });
    return result;
}

/// HMGET key field [field ...] - Get the values of multiple hash fields
pub fn hmget(self: *Connection, allocator: Allocator, key: []const u8, fields: []const []const u8) !Result([]?[]u8) {
    if (fields.len > max_keys) return error.TooManyKeys;
    var args_buf: [max_keys + 2][]const u8 = undefined;
    args_buf[0] = "HMGET";
    args_buf[1] = key;
    @memcpy(args_buf[2 .. 2 + fields.len], fields);
    var result = try Result([]?[]u8).init(allocator);
    errdefer result.deinit();
    result.value = try self.call(Protocol.execOptBulkStringArrayAlloc, .{ result.arena.allocator(), args_buf[0 .. 2 + fields.len] });
    return result;
}

// --- Server commands ---

/// PING - Ping the server
pub fn ping(self: *Connection) !void {
    try self.call(Protocol.execSimpleString, .{&.{"PING"}});
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
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("test_key", "test_value", .{});

    var buf: [1024]u8 = undefined;
    const value = try conn.get("test_key", &buf);

    try std.testing.expect(value != null);
    try std.testing.expectEqualStrings("test_value", value.?);
}

test "get non-existent key returns null" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    var buf: [1024]u8 = undefined;
    const value = try conn.get("non_existent_key_12345", &buf);

    try std.testing.expect(value == null);
}

test "set with expiration" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
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
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("delete_key", "to_be_deleted", .{});
    const deleted = try conn.del(&.{"delete_key"});
    try std.testing.expectEqual(1, deleted);

    var buf: [1024]u8 = undefined;
    const value = try conn.get("delete_key", &buf);
    try std.testing.expect(value == null);
}

test "incr/decr" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("counter", "10", .{});

    const val1 = try conn.incrBy("counter", 5);
    try std.testing.expectEqual(15, val1);

    const val2 = try conn.decrBy("counter", 3);
    try std.testing.expectEqual(12, val2);

    const val3 = try conn.incr("counter");
    try std.testing.expectEqual(13, val3);

    const val4 = try conn.decr("counter");
    try std.testing.expectEqual(12, val4);
}

test "exists" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("exists_key1", "value1", .{});
    try conn.set("exists_key2", "value2", .{});

    const count = try conn.exists(&.{ "exists_key1", "exists_key2", "nonexistent" });
    try std.testing.expectEqual(2, count);
}

test "ping" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.ping();
}

test "expire and ttl" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    try conn.set("expire_test", "value", .{});

    const was_set = try conn.expire("expire_test", 100);
    try std.testing.expect(was_set);

    const ttl_val = try conn.ttl("expire_test");
    try std.testing.expect(ttl_val > 0 and ttl_val <= 100);
}

test "hset/hget" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    _ = try conn.del(&.{"hash_hget"});

    const added = try conn.hset("hash_hget", "field1", "value1");
    try std.testing.expectEqual(1, added);

    var buf: [1024]u8 = undefined;
    const val = try conn.hget("hash_hget", "field1", &buf);
    try std.testing.expectEqualStrings("value1", val.?);

    const missing = try conn.hget("hash_hget", "nofield", &buf);
    try std.testing.expect(missing == null);
}

test "hmset" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    _ = try conn.del(&.{"hash_hmset"});

    const added = try conn.hmset("hash_hmset", &.{
        .{ .field = "f1", .value = "v1" },
        .{ .field = "f2", .value = "v2" },
    });
    try std.testing.expectEqual(2, added);

    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings("v1", (try conn.hget("hash_hmset", "f1", &buf)).?);
    try std.testing.expectEqualStrings("v2", (try conn.hget("hash_hmset", "f2", &buf)).?);
}

test "hdel" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    _ = try conn.del(&.{"hash_hdel"});
    _ = try conn.hmset("hash_hdel", &.{
        .{ .field = "f1", .value = "v1" },
        .{ .field = "f2", .value = "v2" },
    });

    const deleted = try conn.hdel("hash_hdel", &.{ "f1", "f2", "missing" });
    try std.testing.expectEqual(2, deleted);

    var buf: [1024]u8 = undefined;
    try std.testing.expect((try conn.hget("hash_hdel", "f1", &buf)) == null);
}

test "hexists" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    _ = try conn.del(&.{"hash_hexists"});
    _ = try conn.hset("hash_hexists", "field", "value");

    try std.testing.expect(try conn.hexists("hash_hexists", "field"));
    try std.testing.expect(!try conn.hexists("hash_hexists", "missing"));
}

test "hlen" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    _ = try conn.del(&.{"hash_hlen"});
    _ = try conn.hmset("hash_hlen", &.{
        .{ .field = "f1", .value = "v1" },
        .{ .field = "f2", .value = "v2" },
        .{ .field = "f3", .value = "v3" },
    });

    try std.testing.expectEqual(3, try conn.hlen("hash_hlen"));
}

test "hincrby" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    _ = try conn.del(&.{"hash_hincrby"});
    _ = try conn.hset("hash_hincrby", "counter", "10");

    try std.testing.expectEqual(15, try conn.hincrby("hash_hincrby", "counter", 5));
    try std.testing.expectEqual(12, try conn.hincrby("hash_hincrby", "counter", -3));
}

test "hgetall" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    _ = try conn.del(&.{"hash_hgetall"});
    _ = try conn.hmset("hash_hgetall", &.{
        .{ .field = "a", .value = "1" },
        .{ .field = "b", .value = "2" },
    });

    const result = try conn.hgetall(std.testing.allocator, "hash_hgetall");
    defer result.deinit();

    try std.testing.expectEqual(2, result.value.len);
    // Redis returns fields in insertion order for small hashes
    try std.testing.expectEqualStrings("a", result.value[0].field);
    try std.testing.expectEqualStrings("1", result.value[0].value);
    try std.testing.expectEqualStrings("b", result.value[1].field);
    try std.testing.expectEqualStrings("2", result.value[1].value);
}

test "hgetall empty hash" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    _ = try conn.del(&.{"hash_hgetall_empty"});

    const result = try conn.hgetall(std.testing.allocator, "hash_hgetall_empty");
    defer result.deinit();

    try std.testing.expectEqual(0, result.value.len);
}

test "hkeys/hvals" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    _ = try conn.del(&.{"hash_hkeys"});
    _ = try conn.hmset("hash_hkeys", &.{
        .{ .field = "f1", .value = "v1" },
        .{ .field = "f2", .value = "v2" },
    });

    const keys = try conn.hkeys(std.testing.allocator, "hash_hkeys");
    defer keys.deinit();
    try std.testing.expectEqual(2, keys.value.len);
    try std.testing.expectEqualStrings("f1", keys.value[0]);
    try std.testing.expectEqualStrings("f2", keys.value[1]);

    const vals = try conn.hvals(std.testing.allocator, "hash_hkeys");
    defer vals.deinit();
    try std.testing.expectEqual(2, vals.value.len);
    try std.testing.expectEqualStrings("v1", vals.value[0]);
    try std.testing.expectEqualStrings("v2", vals.value[1]);
}

test "hmget" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    _ = try conn.del(&.{"hash_hmget"});
    _ = try conn.hmset("hash_hmget", &.{
        .{ .field = "f1", .value = "v1" },
        .{ .field = "f2", .value = "v2" },
    });

    const result = try conn.hmget(std.testing.allocator, "hash_hmget", &.{ "f1", "missing", "f2" });
    defer result.deinit();

    try std.testing.expectEqual(3, result.value.len);
    try std.testing.expectEqualStrings("v1", result.value[0].?);
    try std.testing.expect(result.value[1] == null);
    try std.testing.expectEqualStrings("v2", result.value[2].?);
}

test "set NX (only if not exists)" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, std.testing.io, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
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
