const std = @import("std");
const zio = @import("zio");
const Allocator = std.mem.Allocator;
const Connection = @import("Connection.zig");
const Pool = @import("Pool.zig");
const Protocol = @import("Protocol.zig");

const log = std.log.scoped(.redis);

const Client = @This();

gpa: Allocator,
host: []const u8,
port: u16,
pool: Pool,
retry_attempts: usize,
retry_interval: zio.Duration,

pub const Options = struct {
    max_idle: usize = 2,
    read_buffer_size: usize = 4096,
    write_buffer_size: usize = 4096,
    connect_timeout: zio.Timeout = .none,
    read_timeout: zio.Timeout = .none,
    write_timeout: zio.Timeout = .none,
    retry_attempts: usize = 2,
    retry_interval: zio.Duration = .zero,
};

// Re-export types for convenience
pub const SetOpts = Connection.SetOpts;
pub const Error = Connection.Error;

pub fn init(gpa: Allocator, server: []const u8, options: Options) !Client {
    const host, const port = parseServer(server) orelse return error.InvalidServer;

    const pool_opts: Pool.Options = .{
        .max_idle = options.max_idle,
        .read_buffer_size = options.read_buffer_size,
        .write_buffer_size = options.write_buffer_size,
        .connect_timeout = options.connect_timeout,
        .read_timeout = options.read_timeout,
        .write_timeout = options.write_timeout,
    };

    return .{
        .gpa = gpa,
        .host = host,
        .port = port,
        .pool = Pool.init(gpa, host, port, pool_opts),
        .retry_attempts = options.retry_attempts,
        .retry_interval = options.retry_interval,
    };
}

pub fn deinit(self: *Client) void {
    self.pool.deinit();
}

fn withConnection(self: *Client, comptime func: anytype, args: anytype) !ReturnType(func) {
    var attempts: usize = 0;
    while (true) {
        log.debug("{s}:{d} attempt {d}", .{ self.host, self.port, attempts });
        const conn = self.pool.acquire() catch |err| {
            if (attempts < self.retry_attempts) {
                attempts += 1;
                log.debug("{s}:{d} acquire failed: {}, retry {d}/{d}", .{
                    self.host,
                    self.port,
                    err,
                    attempts,
                    self.retry_attempts,
                });
                try zio.sleep(self.retry_interval);
                continue;
            }
            return err;
        };
        var ok = false;
        defer self.pool.release(conn, ok);

        log.debug("{s}:{d} calling operation", .{ self.host, self.port });
        const result = @call(.auto, func, .{conn} ++ args) catch |err| {
            log.debug("{s}:{d} operation error: {}", .{ self.host, self.port, err });
            ok = Protocol.isResumable(err);
            if (!ok and attempts < self.retry_attempts) {
                attempts += 1;
                log.debug("{s}:{d} operation failed: {}, retry {d}/{d}", .{
                    self.host,
                    self.port,
                    err,
                    attempts,
                    self.retry_attempts,
                });
                try zio.sleep(self.retry_interval);
                continue;
            }
            return err;
        };
        log.debug("{s}:{d} operation success", .{ self.host, self.port });
        ok = true;
        return result;
    }
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

// --- String commands ---

pub fn get(self: *Client, key: []const u8, buf: []u8) !?[]u8 {
    return self.withConnection(Connection.get, .{ key, buf });
}

pub fn set(self: *Client, key: []const u8, value: []const u8, opts: SetOpts) !void {
    return self.withConnection(Connection.set, .{ key, value, opts });
}

pub fn del(self: *Client, keys: []const []const u8) !i64 {
    return self.withConnection(Connection.del, .{keys});
}

pub fn incr(self: *Client, key: []const u8) !i64 {
    return self.withConnection(Connection.incr, .{key});
}

pub fn incrBy(self: *Client, key: []const u8, delta: i64) !i64 {
    return self.withConnection(Connection.incrBy, .{ key, delta });
}

pub fn decr(self: *Client, key: []const u8) !i64 {
    return self.withConnection(Connection.decr, .{key});
}

pub fn decrBy(self: *Client, key: []const u8, delta: i64) !i64 {
    return self.withConnection(Connection.decrBy, .{ key, delta });
}

pub fn expire(self: *Client, key: []const u8, seconds: u32) !bool {
    return self.withConnection(Connection.expire, .{ key, seconds });
}

pub fn ttl(self: *Client, key: []const u8) !i64 {
    return self.withConnection(Connection.ttl, .{key});
}

pub fn exists(self: *Client, keys: []const []const u8) !i64 {
    return self.withConnection(Connection.exists, .{keys});
}

// --- Server commands ---

pub fn ping(self: *Client, message: ?[]const u8) !void {
    return self.withConnection(Connection.ping, .{message});
}

pub fn flushDB(self: *Client) !void {
    return self.withConnection(Connection.flushDB, .{});
}

pub fn dbSize(self: *Client) !i64 {
    return self.withConnection(Connection.dbSize, .{});
}

// --- Tests ---

test "parseServer" {
    {
        const result = parseServer("localhost:6379");
        try std.testing.expect(result != null);
        try std.testing.expectEqualStrings("localhost", result.?[0]);
        try std.testing.expectEqual(6379, result.?[1]);
    }
    {
        const result = parseServer("invalid");
        try std.testing.expect(result == null);
    }
}

test "parseServer ipv6" {
    const result = parseServer("[::1]:6379");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("[::1]", result.?[0]);
    try std.testing.expectEqual(6379, result.?[1]);
}

test "Client get/set" {
    var client = try Client.init(std.testing.allocator, "127.0.0.1:26379", .{});
    defer client.deinit();

    try client.set("client_test_key", "client_test_value", .{});

    var buf: [1024]u8 = undefined;
    const value = try client.get("client_test_key", &buf);

    try std.testing.expect(value != null);
    try std.testing.expectEqualStrings("client_test_value", value.?);
}

test "Client get non-existent returns null" {
    var client = try Client.init(std.testing.allocator, "127.0.0.1:26379", .{});
    defer client.deinit();

    var buf: [1024]u8 = undefined;
    const value = try client.get("non_existent_client_key", &buf);

    try std.testing.expect(value == null);
}

test "Client incr/decr" {
    var client = try Client.init(std.testing.allocator, "127.0.0.1:26379", .{});
    defer client.deinit();

    try client.set("client_counter", "100", .{});

    const val1 = try client.incrBy("client_counter", 10);
    try std.testing.expectEqual(110, val1);

    const val2 = try client.decrBy("client_counter", 5);
    try std.testing.expectEqual(105, val2);
}

test "Client del" {
    var client = try Client.init(std.testing.allocator, "127.0.0.1:26379", .{});
    defer client.deinit();

    try client.set("client_delete_key", "to_delete", .{});
    const deleted = try client.del(&.{"client_delete_key"});
    try std.testing.expectEqual(@as(i64, 1), deleted);

    var buf: [1024]u8 = undefined;
    const value = try client.get("client_delete_key", &buf);
    try std.testing.expect(value == null);
}

test "Client del/exists with too many keys" {
    var client = try Client.init(std.testing.allocator, "127.0.0.1:26379", .{});
    defer client.deinit();

    // Create array with 65 keys (exceeds the limit of 64)
    var keys: [65][]const u8 = undefined;
    for (&keys, 0..) |*k, i| {
        k.* = if (i == 0) "key0" else "key1";
    }

    // Should return TooManyKeys error
    const del_result = client.del(&keys);
    try std.testing.expectError(error.TooManyKeys, del_result);

    const exists_result = client.exists(&keys);
    try std.testing.expectError(error.TooManyKeys, exists_result);
}

test "Client connection reused after RedisError" {
    var client = try Client.init(std.testing.allocator, "127.0.0.1:26379", .{ .max_idle = 1 });
    defer client.deinit();

    // Set a string value
    try client.set("error_test_key", "not_a_number", .{});

    // Try to increment it - should error but connection should be reused
    _ = client.incr("error_test_key") catch {};

    // Connection should still work
    var buf: [1024]u8 = undefined;
    const value = try client.get("error_test_key", &buf);
    try std.testing.expectEqualStrings("not_a_number", value.?);

    // Verify connection was reused
    try std.testing.expect(!client.pool.isEmpty());
}

test "retry after server restart" {
    const testing = @import("testing.zig");

    var client = try Client.init(std.testing.allocator, "127.0.0.1:26379", .{
        .retry_attempts = 5,
        .retry_interval = .fromMilliseconds(500),
    });
    defer client.deinit();

    // Set a key
    try client.set("retry_test_key", "before_restart", .{});

    // Verify it's there
    var buf: [1024]u8 = undefined;
    const value1 = try client.get("retry_test_key", &buf);
    try std.testing.expectEqualStrings("before_restart", value1.?);

    // Stop immediately (no grace period)
    try testing.runDockerCompose(std.testing.allocator, &.{ "stop", "-t", "0", "redis" });

    // Start in background - will take time to be ready
    var start_thread = try std.Thread.spawn(.{}, struct {
        fn run() void {
            testing.runDockerCompose(std.testing.allocator, &.{ "start", "redis" }) catch {};
        }
    }.run, .{});

    // Try immediately with stale connection - should fail and retry
    try client.set("retry_test_key", "after_restart", .{});

    start_thread.join();

    const value2 = try client.get("retry_test_key", &buf);
    try std.testing.expectEqualStrings("after_restart", value2.?);
}
