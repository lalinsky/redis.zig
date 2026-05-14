const std = @import("std");
const Allocator = std.mem.Allocator;
const Connection = @import("Connection.zig");

const log = std.log.scoped(.redis);

const Pool = @This();

gpa: Allocator,
io: std.Io,
host: []const u8,
port: u16,
idle: std.SinglyLinkedList = .{},
idle_count: usize = 0,
max_idle: usize,
connection_options: Connection.Options,
mutex: std.Io.Mutex = .init,

pub const Options = struct {
    max_idle: usize = 2,
    read_buffer_size: usize = 4096,
    write_buffer_size: usize = 4096,
    connect_timeout: std.Io.Timeout = .none,
    read_timeout: std.Io.Timeout = .none,
    write_timeout: std.Io.Timeout = .none,
};

pub fn init(gpa: Allocator, io: std.Io, host: []const u8, port: u16, options: Options) Pool {
    return .{
        .gpa = gpa,
        .io = io,
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
    try self.mutex.lock(self.io);

    // Try to get an idle connection
    if (self.idle.popFirst()) |node| {
        self.idle_count -= 1;
        self.mutex.unlock(self.io);
        log.debug("pool {s}:{d} reusing connection (idle: {d})", .{ self.host, self.port, self.idle_count });
        return @fieldParentPtr("node", node);
    }

    self.mutex.unlock(self.io);

    // Create a new connection (outside of lock)
    log.debug("pool {s}:{d} creating new connection", .{ self.host, self.port });
    const conn = try self.gpa.create(Connection);
    errdefer self.gpa.destroy(conn);

    try conn.connect(self.gpa, self.io, self.host, self.port, self.connection_options);
    return conn;
}

pub fn isEmpty(self: *Pool) bool {
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);
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

    self.mutex.lockUncancelable(self.io);

    // If pool is full, close the connection
    if (self.idle_count >= self.max_idle) {
        self.mutex.unlock(self.io);
        log.debug("pool {s}:{d} closing connection (pool full)", .{ self.host, self.port });
        conn.close();
        self.gpa.destroy(conn);
        return;
    }

    // Return to pool
    self.idle.prepend(&conn.node);
    self.idle_count += 1;
    log.debug("pool {s}:{d} released connection (idle: {d})", .{ self.host, self.port, self.idle_count });
    self.mutex.unlock(self.io);
}
