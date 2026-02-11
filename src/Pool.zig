const std = @import("std");
const zio = @import("zio");
const Allocator = std.mem.Allocator;
const Connection = @import("Connection.zig");

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
        return @fieldParentPtr("node", node);
    }

    self.mutex.unlock();

    // Create a new connection (outside of lock)
    const conn = try self.gpa.create(Connection);
    errdefer self.gpa.destroy(conn);

    try conn.connect(self.gpa, self.host, self.port, self.connection_options);
    return conn;
}

pub fn release(self: *Pool, conn: *Connection, ok: bool) void {
    // If error occurred, close the connection
    if (!ok) {
        conn.close();
        self.gpa.destroy(conn);
        return;
    }

    self.mutex.lockUncancelable();

    // If pool is full, close the connection
    if (self.idle_count >= self.max_idle) {
        self.mutex.unlock();
        conn.close();
        self.gpa.destroy(conn);
        return;
    }

    // Return to pool
    self.idle.prepend(&conn.node);
    self.idle_count += 1;
    self.mutex.unlock();
}
