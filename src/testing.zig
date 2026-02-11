const std = @import("std");
const zio = @import("zio");

const gpa = std.heap.smp_allocator;
pub var runtime: *zio.Runtime = undefined;

pub const Node = enum(u16) {
    node1 = 21211,
    node2 = 21212,
    node3 = 21213,
};

pub fn runDockerComposeCapture(allocator: std.mem.Allocator, compose_args: []const []const u8) !std.process.Child.RunResult {
    var args: std.ArrayListUnmanaged([]const u8) = .{};
    defer args.deinit(allocator);

    try args.appendSlice(allocator, &.{ "docker", "compose", "-f", "docker-compose.test.yml", "-p", "memcached-zig-test" });
    try args.appendSlice(allocator, compose_args);

    return try std.process.Child.run(.{
        .allocator = allocator,
        .argv = args.items,
    });
}

pub fn runDockerCompose(allocator: std.mem.Allocator, compose_args: []const []const u8) !void {
    const result = try runDockerComposeCapture(allocator, compose_args);
    defer allocator.free(result.stderr);
    defer allocator.free(result.stdout);
}

pub fn waitForServices(timeout_ms: i64) !void {
    const nodes = [_]Node{ .node1, .node2, .node3 };
    for (nodes) |node| {
        try waitForNode(node, timeout_ms);
    }
}

pub fn waitForNode(node: Node, timeout_ms: i64) !void {
    const deadline = std.time.milliTimestamp() + timeout_ms;
    while (std.time.milliTimestamp() < deadline) {
        if (tryConnect(node)) return;
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }
    return error.ServiceNotHealthy;
}

fn tryConnect(node: Node) bool {
    const stream = zio.net.tcpConnectToHost("127.0.0.1", @intFromEnum(node), .{}) catch return false;
    stream.close();
    return true;
}

// --- Test lifecycle hooks ---

test "tests:beforeAll" {
    runtime = try zio.Runtime.init(gpa, .{});
    errdefer runtime.deinit();

    try runDockerCompose(gpa, &.{ "up", "-d" });
    errdefer runDockerCompose(gpa, &.{"down"}) catch {};

    try waitForServices(30_000);
}

test "tests:afterAll" {
    try runDockerCompose(gpa, &.{"down"});
    runtime.deinit();
}
