const std = @import("std");
const zio = @import("zio");
const Allocator = std.mem.Allocator;
const Connection = @import("Connection.zig");
const Pool = @import("Pool.zig");
const Protocol = @import("Protocol.zig");
const Server = @import("Server.zig");
const Hasher = @import("hasher.zig").Hasher;

const log = std.log.scoped(.memcached);

const Client = @This();

gpa: Allocator,
servers: []Server,
hasher: Hasher,
round_robin: std.atomic.Value(usize),
retry_attempts: usize,
retry_interval: zio.Duration,

pub const Options = struct {
    servers: []const []const u8 = &.{},
    hasher: Hasher = .none,
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
        .connect_timeout = options.connect_timeout,
        .read_timeout = options.read_timeout,
        .write_timeout = options.write_timeout,
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
        .retry_attempts = options.retry_attempts,
        .retry_interval = options.retry_interval,
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
    return self.withServer(self.pickServer(key), func, args);
}

fn withAnyConnection(self: *Client, comptime func: anytype, args: anytype) !ReturnType(func) {
    const index = self.round_robin.fetchAdd(1, .monotonic) % self.servers.len;
    return self.withServer(&self.servers[index], func, args);
}

fn withServer(self: *Client, server: *Server, comptime func: anytype, args: anytype) !ReturnType(func) {
    var attempts: usize = 0;
    while (true) {
        log.debug("{s}:{d} attempt {d}", .{ server.host, server.port, attempts });
        const conn = server.pool.acquire() catch |err| {
            if (attempts < self.retry_attempts) {
                attempts += 1;
                log.debug("{s}:{d} acquire failed: {}, retry {d}/{d}", .{
                    server.host,
                    server.port,
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
        defer server.pool.release(conn, ok);

        log.debug("{s}:{d} calling operation", .{ server.host, server.port });
        const result = @call(.auto, func, .{conn} ++ args) catch |err| {
            log.debug("{s}:{d} operation error: {}", .{ server.host, server.port, err });
            ok = Protocol.isResumable(err);
            if (!ok and attempts < self.retry_attempts) {
                attempts += 1;
                log.debug("{s}:{d} operation failed: {}, retry {d}/{d}", .{
                    server.host,
                    server.port,
                    err,
                    attempts,
                    self.retry_attempts,
                });
                try zio.sleep(self.retry_interval);
                continue;
            }
            return err;
        };
        log.debug("{s}:{d} operation success", .{ server.host, server.port });
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

test "Client get/set" {
    var client = try Client.init(std.testing.allocator, .{
        .servers = &.{"127.0.0.1:21211"},
    });
    defer client.deinit();

    try client.set("client_test_key", "client_test_value", .{});

    var buf: [1024]u8 = undefined;
    const info = try client.get("client_test_key", &buf, .{});

    try std.testing.expect(info != null);
    try std.testing.expectEqualStrings("client_test_value", info.?.value);
}

test "Client get non-existent returns null" {
    var client = try Client.init(std.testing.allocator, .{
        .servers = &.{"127.0.0.1:21211"},
    });
    defer client.deinit();

    var buf: [1024]u8 = undefined;
    const info = try client.get("non_existent_client_key", &buf, .{});

    try std.testing.expect(info == null);
}

test "Client incr/decr" {
    var client = try Client.init(std.testing.allocator, .{
        .servers = &.{"127.0.0.1:21211"},
    });
    defer client.deinit();

    try client.set("client_counter", "100", .{});

    const val1 = try client.incr("client_counter", 10);
    try std.testing.expectEqual(110, val1);

    const val2 = try client.decr("client_counter", 5);
    try std.testing.expectEqual(105, val2);
}

test "Client delete" {
    var client = try Client.init(std.testing.allocator, .{
        .servers = &.{"127.0.0.1:21211"},
    });
    defer client.deinit();

    try client.set("client_delete_key", "to_delete", .{});
    try client.delete("client_delete_key");

    var buf: [1024]u8 = undefined;
    const info = try client.get("client_delete_key", &buf, .{});
    try std.testing.expect(info == null);
}

test "Client connection reused after NotStored error" {
    var client = try Client.init(std.testing.allocator, .{
        .servers = &.{"127.0.0.1:21211"},
        .max_idle = 1,
    });
    defer client.deinit();

    // Ensure key doesn't exist
    client.delete("reuse_test_key") catch {};

    // add() on non-existent key succeeds
    try client.add("reuse_test_key", "first", .{});

    // add() again should fail with NotStored - but connection should be reused
    try std.testing.expectError(error.NotStored, client.add("reuse_test_key", "second", .{}));

    // Connection should still work - this would fail if connection was closed
    var buf: [1024]u8 = undefined;
    const info = try client.get("reuse_test_key", &buf, .{});
    try std.testing.expectEqualStrings("first", info.?.value);

    // Verify connection was reused (pool should not be empty)
    try std.testing.expect(!client.servers[0].pool.isEmpty());
}

test "Client connection reused after Exists error (CAS conflict)" {
    var client = try Client.init(std.testing.allocator, .{
        .servers = &.{"127.0.0.1:21211"},
        .max_idle = 1,
    });
    defer client.deinit();

    try client.set("cas_reuse_key", "original", .{});

    var buf: [1024]u8 = undefined;
    const info = try client.get("cas_reuse_key", &buf, .{});
    const old_cas = info.?.cas;

    // Update the key to invalidate the CAS token
    try client.set("cas_reuse_key", "updated", .{});

    // CAS with old token should fail with Exists - but connection should be reused
    try std.testing.expectError(error.Exists, client.set("cas_reuse_key", "conflict", .{ .cas = old_cas }));

    // Connection should still work
    const info2 = try client.get("cas_reuse_key", &buf, .{});
    try std.testing.expectEqualStrings("updated", info2.?.value);

    // Verify connection was reused
    try std.testing.expect(!client.servers[0].pool.isEmpty());
}

test "key distribution across servers" {
    const servers = &[_][]const u8{
        "127.0.0.1:21211",
        "127.0.0.1:21212",
        "127.0.0.1:21213",
    };

    // Create distributed client
    var client = try Client.init(std.testing.allocator, .{
        .servers = servers,
        .hasher = .rendezvous,
    });
    defer client.deinit();

    // Set 100 keys via distributed client
    const num_keys = 100;
    var key_buf: [32]u8 = undefined;
    for (0..num_keys) |i| {
        const key = std.fmt.bufPrint(&key_buf, "dist_test_{d}", .{i}) catch unreachable;
        try client.set(key, "value", .{});
    }

    // Connect to each server individually and count keys found
    var counts = [_]usize{ 0, 0, 0 };

    for (servers, 0..) |server_str, server_idx| {
        const host, const port = parseServer(server_str).?;

        var conn: Connection = undefined;
        try conn.connect(std.testing.allocator, host, port, .{});
        defer conn.close();

        var buf: [1024]u8 = undefined;
        for (0..num_keys) |i| {
            const key = std.fmt.bufPrint(&key_buf, "dist_test_{d}", .{i}) catch unreachable;
            if (try conn.get(key, &buf, .{})) |_| {
                counts[server_idx] += 1;
            }
        }
    }

    // Verify distribution
    errdefer std.debug.print("\nDistribution: {d} / {d} / {d}\n", .{ counts[0], counts[1], counts[2] });

    var total: usize = 0;
    for (counts) |c| {
        total += c;
        // Each server should have some keys (at least 10%)
        try std.testing.expect(c >= 10);
        // But not all keys (at most 60%)
        try std.testing.expect(c <= 60);
    }

    // Total should equal num_keys (each key on exactly one server)
    try std.testing.expectEqual(num_keys, total);
}

test "retry after server restart" {
    const testing = @import("testing.zig");

    var client = try Client.init(std.testing.allocator, .{
        .servers = &.{"127.0.0.1:21211"},
        .retry_attempts = 5,
        .retry_interval = .fromMilliseconds(500),
    });
    defer client.deinit();

    // Set a key
    try client.set("retry_test_key", "before_restart", .{});

    // Verify it's there
    var buf: [1024]u8 = undefined;
    const info1 = try client.get("retry_test_key", &buf, .{});
    try std.testing.expectEqualStrings("before_restart", info1.?.value);

    // Stop immediately (no grace period)
    try testing.runDockerCompose(std.testing.allocator, &.{ "stop", "-t", "0", "memcached-1" });

    // Start in background - will take time to be ready
    var start_thread = try std.Thread.spawn(.{}, struct {
        fn run() void {
            testing.runDockerCompose(std.testing.allocator, &.{ "start", "memcached-1" }) catch {};
        }
    }.run, .{});

    // Try immediately with stale connection - should fail and retry
    try client.set("retry_test_key", "after_restart", .{});

    start_thread.join();

    const info2 = try client.get("retry_test_key", &buf, .{});
    try std.testing.expectEqualStrings("after_restart", info2.?.value);
}
