const std = @import("std");
const Allocator = std.mem.Allocator;
const Protocol = @import("Protocol.zig");
const Connection = @import("Connection.zig");
const Pool = @import("Pool.zig");

const Pipeline = @This();

const max_commands = 64;

conn: *Connection,
pool: ?*Pool,
pending_buf: [max_commands]ResponseType = undefined,
pending_len: usize = 0,
healthy: bool = true,

pub const ResponseType = enum { simple_string, integer, bulk_string, ok_or_nil };

pub const Result = union(enum) {
    ok: void,
    integer: i64,
    bulk_string: ?[]u8,
    redis_error: void,
};

pub fn init(conn: *Connection, pool: ?*Pool) Pipeline {
    return .{ .conn = conn, .pool = pool };
}

pub fn deinit(self: *Pipeline) void {
    if (self.pool) |pool| {
        pool.release(self.conn, self.healthy);
    }
}

fn protocol(self: *Pipeline) Protocol {
    return self.conn.protocol();
}

fn appendPending(self: *Pipeline, resp_type: ResponseType) !void {
    if (self.pending_len >= max_commands) return error.TooManyCommands;
    self.pending_buf[self.pending_len] = resp_type;
    self.pending_len += 1;
}

fn writeCommand(self: *Pipeline, args: []const []const u8) !void {
    const p = self.protocol();
    return p.writeCommandNoFlush(args) catch |err| switch (err) {
        error.WriteFailed => return self.conn.writer.err orelse error.WriteFailed,
        else => return err,
    };
}

fn readResponse(self: *Pipeline, resp_type: ResponseType, allocator: Allocator) !Result {
    const p = self.protocol();
    switch (resp_type) {
        .simple_string => {
            p.readSimpleStringResponse() catch |err| {
                if (err == error.RedisError) return .{ .redis_error = {} };
                return mapReadError(self, err);
            };
            return .{ .ok = {} };
        },
        .integer => {
            const val = p.readIntegerResponse() catch |err| {
                if (err == error.RedisError) return .{ .redis_error = {} };
                return mapReadError(self, err);
            };
            return .{ .integer = val };
        },
        .bulk_string => {
            const val = p.readBulkStringResponseAlloc(allocator) catch |err| {
                if (err == error.RedisError) return .{ .redis_error = {} };
                return mapReadError(self, err);
            };
            return .{ .bulk_string = val };
        },
        .ok_or_nil => {
            p.readOkOrNilResponse() catch |err| {
                if (err == error.RedisError) return .{ .redis_error = {} };
                return mapReadError(self, err);
            };
            return .{ .ok = {} };
        },
    }
}

fn mapReadError(self: *Pipeline, err: anyerror) anyerror {
    self.healthy = false;
    return switch (err) {
        error.ReadFailed => self.conn.reader.err orelse error.ReadFailed,
        else => err,
    };
}

// --- Command queueing ---

pub fn set(self: *Pipeline, key: []const u8, value: []const u8, opts: Connection.SetOpts) !void {
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

    try self.writeCommand(args_buf[0..args_count]);
    try self.appendPending(.ok_or_nil);
}

pub fn get(self: *Pipeline, key: []const u8) !void {
    try self.writeCommand(&.{ "GET", key });
    try self.appendPending(.bulk_string);
}

pub fn del(self: *Pipeline, keys: []const []const u8) !void {
    if (keys.len > Connection.max_keys) return error.TooManyKeys;
    var args_buf: [Connection.max_keys + 1][]const u8 = undefined;
    args_buf[0] = "DEL";
    @memcpy(args_buf[1 .. 1 + keys.len], keys);
    try self.writeCommand(args_buf[0 .. 1 + keys.len]);
    try self.appendPending(.integer);
}

pub fn incr(self: *Pipeline, key: []const u8) !void {
    try self.writeCommand(&.{ "INCR", key });
    try self.appendPending(.integer);
}

pub fn incrBy(self: *Pipeline, key: []const u8, delta: i64) !void {
    var delta_buf: [32]u8 = undefined;
    const delta_str = std.fmt.bufPrint(&delta_buf, "{d}", .{delta}) catch unreachable;
    try self.writeCommand(&.{ "INCRBY", key, delta_str });
    try self.appendPending(.integer);
}

pub fn decr(self: *Pipeline, key: []const u8) !void {
    try self.writeCommand(&.{ "DECR", key });
    try self.appendPending(.integer);
}

pub fn decrBy(self: *Pipeline, key: []const u8, delta: i64) !void {
    var delta_buf: [32]u8 = undefined;
    const delta_str = std.fmt.bufPrint(&delta_buf, "{d}", .{delta}) catch unreachable;
    try self.writeCommand(&.{ "DECRBY", key, delta_str });
    try self.appendPending(.integer);
}

pub fn expire(self: *Pipeline, key: []const u8, seconds: u32) !void {
    var seconds_buf: [32]u8 = undefined;
    const seconds_str = std.fmt.bufPrint(&seconds_buf, "{d}", .{seconds}) catch unreachable;
    try self.writeCommand(&.{ "EXPIRE", key, seconds_str });
    try self.appendPending(.integer);
}

pub fn ttl(self: *Pipeline, key: []const u8) !void {
    try self.writeCommand(&.{ "TTL", key });
    try self.appendPending(.integer);
}

pub fn exists(self: *Pipeline, keys: []const []const u8) !void {
    if (keys.len > Connection.max_keys) return error.TooManyKeys;
    var args_buf: [Connection.max_keys + 1][]const u8 = undefined;
    args_buf[0] = "EXISTS";
    @memcpy(args_buf[1 .. 1 + keys.len], keys);
    try self.writeCommand(args_buf[0 .. 1 + keys.len]);
    try self.appendPending(.integer);
}

pub fn ping(self: *Pipeline) !void {
    try self.writeCommand(&.{"PING"});
    try self.appendPending(.simple_string);
}

pub fn flushDB(self: *Pipeline) !void {
    try self.writeCommand(&.{"FLUSHDB"});
    try self.appendPending(.simple_string);
}

pub fn dbSize(self: *Pipeline) !void {
    try self.writeCommand(&.{"DBSIZE"});
    try self.appendPending(.integer);
}

// --- Execution ---

pub fn exec(self: *Pipeline, arena: *std.heap.ArenaAllocator) ![]Result {
    const alloc = arena.allocator();
    const count = self.pending_len;

    const results = try alloc.alloc(Result, count);

    // Flush all buffered commands
    self.protocol().writer.flush() catch |err| switch (err) {
        error.WriteFailed => {
            self.healthy = false;
            return self.conn.writer.err orelse error.WriteFailed;
        },
    };

    // Read all responses
    for (self.pending_buf[0..count], results) |resp_type, *result| {
        result.* = try self.readResponse(resp_type, alloc);
    }

    self.pending_len = 0;
    return results;
}

// --- Tests ---

const testing = @import("testing.zig");

test "pipeline set + get" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    var pipe = Pipeline.init(&conn, null);

    try pipe.set("pipe_key1", "value1", .{});
    try pipe.set("pipe_key2", "value2", .{});
    try pipe.get("pipe_key1");
    try pipe.get("pipe_key2");

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const results = try pipe.exec(&arena);
    try std.testing.expectEqual(4, results.len);

    // SET results
    try std.testing.expectEqual(.ok, std.meta.activeTag(results[0]));
    try std.testing.expectEqual(.ok, std.meta.activeTag(results[1]));

    // GET results
    try std.testing.expectEqualStrings("value1", results[2].bulk_string.?);
    try std.testing.expectEqualStrings("value2", results[3].bulk_string.?);
}

test "pipeline multiple incr" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    // Set initial value
    conn.set("pipe_counter", "0", .{}) catch {};

    var pipe = Pipeline.init(&conn, null);

    try pipe.incr("pipe_counter");
    try pipe.incr("pipe_counter");
    try pipe.incr("pipe_counter");

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const results = try pipe.exec(&arena);
    try std.testing.expectEqual(3, results.len);

    try std.testing.expectEqual(1, results[0].integer);
    try std.testing.expectEqual(2, results[1].integer);
    try std.testing.expectEqual(3, results[2].integer);
}

test "pipeline mixed commands" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    var pipe = Pipeline.init(&conn, null);

    try pipe.set("pipe_mix", "100", .{});
    try pipe.incr("pipe_mix");
    try pipe.get("pipe_mix");
    try pipe.del(&.{"pipe_mix"});
    try pipe.get("pipe_mix");

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const results = try pipe.exec(&arena);
    try std.testing.expectEqual(5, results.len);

    try std.testing.expectEqual(.ok, std.meta.activeTag(results[0]));
    try std.testing.expectEqual(101, results[1].integer);
    try std.testing.expectEqualStrings("101", results[2].bulk_string.?);
    try std.testing.expectEqual(1, results[3].integer);
    try std.testing.expect(results[4].bulk_string == null);
}

test "pipeline error handling" {
    var conn: Connection = undefined;
    try conn.connect(std.testing.allocator, "127.0.0.1", @intFromEnum(testing.Node.node1), .{});
    defer conn.close();

    var pipe = Pipeline.init(&conn, null);

    // SET a string value, then INCR it (will fail), then GET it (should still work)
    try pipe.set("pipe_err", "not_a_number", .{});
    try pipe.incr("pipe_err");
    try pipe.get("pipe_err");

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const results = try pipe.exec(&arena);
    try std.testing.expectEqual(3, results.len);

    try std.testing.expectEqual(.ok, std.meta.activeTag(results[0]));
    try std.testing.expectEqual(.redis_error, std.meta.activeTag(results[1]));
    try std.testing.expectEqualStrings("not_a_number", results[2].bulk_string.?);
}
