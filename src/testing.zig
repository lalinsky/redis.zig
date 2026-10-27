const std = @import("std");

/// The port of the Redis server in docker-compose.test.yml.
pub const node1: u16 = 26379;

pub fn runDockerComposeCapture(allocator: std.mem.Allocator, io: std.Io, compose_args: []const []const u8) !std.process.RunResult {
    var args: std.ArrayListUnmanaged([]const u8) = .empty;
    defer args.deinit(allocator);

    try args.appendSlice(allocator, &.{ "docker", "compose", "-f", "docker-compose.test.yml", "-p", "redis-zig-test" });
    try args.appendSlice(allocator, compose_args);

    return try std.process.run(allocator, io, .{
        .argv = args.items,
    });
}

pub fn runDockerCompose(allocator: std.mem.Allocator, io: std.Io, compose_args: []const []const u8) !void {
    const result = try runDockerComposeCapture(allocator, io, compose_args);
    defer allocator.free(result.stderr);
    defer allocator.free(result.stdout);
}

pub fn waitForServices(io: std.Io, timeout: std.Io.Duration) !void {
    try waitForNode(io, node1, timeout);
}

pub fn waitForNode(io: std.Io, port: u16, timeout: std.Io.Duration) !void {
    const deadline = std.Io.Clock.Timestamp.now(io, .awake).addDuration(.{ .raw = timeout, .clock = .awake });
    while (true) {
        if (try tryConnect(io, port)) {
            try io.sleep(.fromMilliseconds(500), .awake);
            return;
        }
        if (!std.Io.Clock.Timestamp.now(io, .awake).compare(.lt, deadline)) {
            return error.ServiceNotHealthy;
        }
        try io.sleep(.fromMilliseconds(100), .awake);
    }
}

fn tryConnect(io: std.Io, port: u16) std.Io.Cancelable!bool {
    const addr = std.Io.net.IpAddress.parse("127.0.0.1", port) catch return false;
    const stream = addr.connect(io, .{ .mode = .stream }) catch |err| {
        if (err == error.Canceled) return error.Canceled;
        return false;
    };
    stream.close(io);
    return true;
}

// --- Test lifecycle hooks ---

test "tests:beforeAll" {
    const io = std.testing.io;
    try runDockerCompose(std.testing.allocator, io, &.{ "up", "-d" });
    errdefer runDockerCompose(std.testing.allocator, io, &.{"down"}) catch {};

    try waitForServices(io, .fromSeconds(30));
}

test "tests:afterAll" {
    try runDockerCompose(std.testing.allocator, std.testing.io, &.{"down"});
}
