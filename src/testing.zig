const std = @import("std");
const zio = @import("zio");

const gpa = std.heap.smp_allocator;
pub var runtime: *zio.Runtime = undefined;

pub const Node = enum(u16) {
    node1 = 26379,
};

pub fn runDockerComposeCapture(allocator: std.mem.Allocator, compose_args: []const []const u8) !std.process.Child.RunResult {
    var args: std.ArrayListUnmanaged([]const u8) = .{};
    defer args.deinit(allocator);

    try args.appendSlice(allocator, &.{ "docker", "compose", "-f", "docker-compose.test.yml", "-p", "redis-zig-test" });
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
    try waitForNode(.node1, timeout_ms);
}

pub fn waitForNode(node: Node, timeout_ms: i64) !void {
    const deadline = std.time.milliTimestamp() + timeout_ms;
    while (std.time.milliTimestamp() < deadline) {
        if (tryConnect(node)) {
            // Give Redis a bit more time to fully initialize after port opens
            try zio.sleep(.fromMilliseconds(500));
            return;
        }
        try zio.sleep(.fromMilliseconds(100));
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
