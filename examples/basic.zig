const std = @import("std");
const zio = @import("zio");
const redis = @import("redis");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    // Connect
    var client = try redis.connect(allocator, "localhost:6379");
    defer client.deinit();

    // Ping
    try client.ping();
    std.debug.print("Connected to Redis server\n", .{});

    // Set
    try client.set("hello", "world", .{});
    std.debug.print("SET hello = world\n", .{});

    // Get
    var buf: [1024]u8 = undefined;
    if (try client.get("hello", &buf)) |value| {
        std.debug.print("GET hello = {s}\n", .{value});
    } else {
        std.debug.print("GET hello = (not found)\n", .{});
    }

    // Set with TTL
    try client.set("test", "value123", .{ .ex = 60 });
    std.debug.print("SET test = value123 (ex=60)\n", .{});

    if (try client.get("test", &buf)) |value| {
        const ttl = try client.ttl("test");
        std.debug.print("GET test = {s} (ttl={d}s)\n", .{ value, ttl });
    }

    // Delete
    const deleted = try client.del(&.{"hello"});
    std.debug.print("DEL hello (deleted={d})\n", .{deleted});

    if (try client.get("hello", &buf)) |value| {
        std.debug.print("GET hello = {s}\n", .{value});
    } else {
        std.debug.print("GET hello = (not found)\n", .{});
    }

    // Counters
    try client.set("counter", "0", .{});
    _ = try client.incrBy("counter", 5);
    const val = try client.get("counter", &buf);
    std.debug.print("Counter value: {s}\n", .{val.?});

    std.debug.print("\nDone!\n", .{});
}
