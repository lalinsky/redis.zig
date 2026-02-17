//! RESP2 (Redis Serialization Protocol) encoder/decoder
//!
//! RESP2 data types:
//! - Simple Strings: +OK\r\n
//! - Errors: -ERR message\r\n
//! - Integers: :123\r\n
//! - Bulk Strings: $6\r\nfoobar\r\n (or $-1\r\n for nil)
//! - Arrays: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
//!
//! Commands are sent as arrays of bulk strings.

const std = @import("std");

const Protocol = @This();

reader: *std.Io.Reader,
writer: *std.Io.Writer,

pub const Error = error{
    RedisError,
    ProtocolError,
    UnexpectedType,
    InvalidCharacter,
    Overflow,
    ValueTooLarge,
} || std.Io.Reader.Error || std.Io.Reader.DelimiterError || std.Io.Writer.Error;

/// Returns true if the error is a protocol-level error where the connection
/// is still valid and can be reused.
pub fn isResumable(err: anyerror) bool {
    return switch (err) {
        error.RedisError => true,
        else => false,
    };
}

pub const Value = union(enum) {
    simple_string: []const u8,
    err: []const u8,
    integer: i64,
    bulk_string: ?[]const u8, // null represents nil
    array: []const Value,

    pub fn asSimpleString(self: Value) ![]const u8 {
        return switch (self) {
            .simple_string => |s| s,
            else => error.UnexpectedType,
        };
    }

    pub fn asInteger(self: Value) !i64 {
        return switch (self) {
            .integer => |i| i,
            else => error.UnexpectedType,
        };
    }

    pub fn asBulkString(self: Value) !?[]const u8 {
        return switch (self) {
            .bulk_string => |s| s,
            else => error.UnexpectedType,
        };
    }

    pub fn asArray(self: Value) ![]const Value {
        return switch (self) {
            .array => |a| a,
            else => error.UnexpectedType,
        };
    }
};

// --- Writing (Encoding) ---

/// Write a RESP command as an array of bulk strings
pub fn writeCommand(self: Protocol, args: []const []const u8) Error!void {
    try self.writer.print("*{d}\r\n", .{args.len});
    for (args) |arg| {
        try self.writer.print("${d}\r\n", .{arg.len});
        try self.writer.writeAll(arg);
        try self.writer.writeAll("\r\n");
    }
    try self.writer.flush();
}

// --- Reading (Decoding) ---

/// Read a RESP value. Caller owns returned memory.
pub fn readValue(self: Protocol, allocator: std.mem.Allocator) Error!Value {
    const line = try self.reader.takeDelimiterInclusive('\n');
    if (line.len < 2 or line[line.len - 2] != '\r') return error.ProtocolError;

    const type_byte = line[0];
    const data = line[1 .. line.len - 2]; // strip type byte and \r\n

    return switch (type_byte) {
        '+' => .{ .simple_string = try allocator.dupe(u8, data) },
        '-' => .{ .err = try allocator.dupe(u8, data) },
        ':' => .{ .integer = try std.fmt.parseInt(i64, data, 10) },
        '$' => try self.readBulkString(allocator, data),
        '*' => try self.readArray(allocator, data),
        else => error.ProtocolError,
    };
}

fn readBulkString(self: Protocol, allocator: std.mem.Allocator, len_str: []const u8) Error!Value {
    const len = try std.fmt.parseInt(i64, len_str, 10);
    if (len == -1) return .{ .bulk_string = null }; // nil
    if (len < 0) return error.ProtocolError;

    const size: usize = @intCast(len);
    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);

    try self.reader.readSliceAll(buf);
    const crlf = try self.reader.takeDelimiterInclusive('\n');
    if (crlf.len != 2 or crlf[0] != '\r') return error.ProtocolError;

    return .{ .bulk_string = buf };
}

fn readArray(self: Protocol, allocator: std.mem.Allocator, len_str: []const u8) Error!Value {
    const len = try std.fmt.parseInt(i64, len_str, 10);
    if (len == -1) return .{ .array = &.{} }; // nil array (empty slice)
    if (len < 0) return error.ProtocolError;

    const size: usize = @intCast(len);
    const arr = try allocator.alloc(Value, size);
    errdefer allocator.free(arr);

    for (arr) |*elem| {
        elem.* = try self.readValue(allocator);
    }

    return .{ .array = arr };
}

/// Free memory allocated by readValue
pub fn freeValue(allocator: std.mem.Allocator, value: Value) void {
    switch (value) {
        .simple_string => |s| allocator.free(s),
        .err => |s| allocator.free(s),
        .integer => {},
        .bulk_string => |s| if (s) |str| allocator.free(str),
        .array => |arr| {
            for (arr) |elem| {
                freeValue(allocator, elem);
            }
            allocator.free(arr);
        },
    }
}

// --- High-level command helpers ---

/// Execute a command and expect a simple string response (like "OK")
pub fn execSimpleString(self: Protocol, args: []const []const u8) Error!void {
    try self.writeCommand(args);
    // We can't allocate here without an allocator, so we'll read inline
    const line = try self.reader.takeDelimiterInclusive('\n');
    if (line.len < 2 or line[line.len - 2] != '\r') return error.ProtocolError;

    if (line[0] == '+') return; // OK
    if (line[0] == '-') return error.RedisError;
    return error.UnexpectedType;
}

/// Execute a command and expect an integer response
pub fn execInteger(self: Protocol, args: []const []const u8) Error!i64 {
    try self.writeCommand(args);
    const line = try self.reader.takeDelimiterInclusive('\n');
    if (line.len < 2 or line[line.len - 2] != '\r') return error.ProtocolError;

    if (line[0] == ':') {
        return try std.fmt.parseInt(i64, line[1 .. line.len - 2], 10);
    }
    if (line[0] == '-') return error.RedisError;
    return error.UnexpectedType;
}

/// Execute a command and expect a bulk string response
/// Returns slice into the provided buffer, or null for nil
pub fn execBulkString(self: Protocol, args: []const []const u8, buf: []u8) Error!?[]u8 {
    try self.writeCommand(args);
    const line = try self.reader.takeDelimiterInclusive('\n');
    if (line.len < 2 or line[line.len - 2] != '\r') return error.ProtocolError;

    if (line[0] == '$') {
        const len = try std.fmt.parseInt(i64, line[1 .. line.len - 2], 10);
        if (len == -1) return null; // nil
        if (len < 0) return error.ProtocolError;

        const size: usize = @intCast(len);
        if (size > buf.len) return error.ValueTooLarge;

        try self.reader.readSliceAll(buf[0..size]);
        const crlf = try self.reader.takeDelimiterInclusive('\n');
        if (crlf.len != 2 or crlf[0] != '\r') return error.ProtocolError;

        return buf[0..size];
    }
    if (line[0] == '-') return error.RedisError;
    return error.UnexpectedType;
}

pub const Error2 = Error || error{ValueTooLarge};
