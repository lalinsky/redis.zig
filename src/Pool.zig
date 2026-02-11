const std = @import("std");
const zio = @import("zio");
const Allocator = std.mem.Allocator;
const Connection = @import("Connection.zig");

const log = std.log.scoped(.memcached);

const Pool = @This();

gpa: Allocator,
host: []const u8,
port: u16,
idle: std.SinglyLinkedList = .{},
idle_count: usize = 0,
max_idle: usize,
connection_options: Connection.Options,
mutex: zio.Mutex = .init,

pub const Options = struct {
    max_idle: usize = 2,
    read_buffer_size: usize = 4096,
    write_buffer_size: usize = 4096,
    connect_timeout: zio.Timeout = .none,
    read_timeout: zio.Timeout = .none,
    write_timeout: zio.Timeout = .none,
};

pub fn init(gpa: Allocator, host: []const u8, port: u16, options: Options) Pool {
    return .{
        .gpa = gpa,
        .host = host,
        .port = port,
        .max_idle = options.max_idle,
        .connection_options = .{
            .read_buffer_size = options.read_buffer_size,
            .write_buffer_size = options.write_buffer_size,
            .connect_timeout = options.connect_timeout,
            .read_timeout = options.read_timeout,
            .write_timeout = options.write_timeout,
        },
    };
}

pub fn deinit(self: *Pool) void {
    while (self.idle.popFirst()) |node| {
        const conn: *Connection = @fieldParentPtr("node", node);
        conn.close();
        self.gpa.destroy(conn);
    }
}

pub fn acquire(self: *Pool) !*Connection {
    try self.mutex.lock();

    // Try to get an idle connection
    if (self.idle.popFirst()) |node| {
        self.idle_count -= 1;
        self.mutex.unlock();
        log.debug("pool {s}:{d} reusing connection (idle: {d})", .{ self.host, self.port, self.idle_count });
        return @fieldParentPtr("node", node);
    }

    self.mutex.unlock();

    // Create a new connection (outside of lock)
    log.debug("pool {s}:{d} creating new connection", .{ self.host, self.port });
    const conn = try self.gpa.create(Connection);
    errdefer self.gpa.destroy(conn);

    try conn.connect(self.gpa, self.host, self.port, self.connection_options);
    return conn;
}

pub fn isEmpty(self: *Pool) bool {
    self.mutex.lockUncancelable();
    defer self.mutex.unlock();
    return self.idle_count == 0;
}

pub fn release(self: *Pool, conn: *Connection, ok: bool) void {
    // If error occurred, close the connection
    if (!ok) {
        log.debug("pool {s}:{d} closing connection (error)", .{ self.host, self.port });
        conn.close();
        self.gpa.destroy(conn);
        return;
    }

    self.mutex.lockUncancelable();

    // If pool is full, close the connection
    if (self.idle_count >= self.max_idle) {
        self.mutex.unlock();
        log.debug("pool {s}:{d} closing connection (pool full)", .{ self.host, self.port });
        conn.close();
        self.gpa.destroy(conn);
        return;
    }

    // Return to pool
    self.idle.prepend(&conn.node);
    self.idle_count += 1;
    log.debug("pool {s}:{d} released connection (idle: {d})", .{ self.host, self.port, self.idle_count });
    self.mutex.unlock();
}
