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

pub const FieldValue = struct { field: []const u8, value: []const u8 };

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

/// Write a RESP command as an array of bulk strings without flushing
pub fn writeCommandNoFlush(self: Protocol, args: []const []const u8) Error!void {
    try self.writer.print("*{d}\r\n", .{args.len});
    for (args) |arg| {
        try self.writer.print("${d}\r\n", .{arg.len});
        try self.writer.writeAll(arg);
        try self.writer.writeAll("\r\n");
    }
}

/// Write a RESP command as an array of bulk strings and flush
pub fn writeCommand(self: Protocol, args: []const []const u8) Error!void {
    try self.writeCommandNoFlush(args);
    try self.writer.flush();
}

// --- Reading (Decoding) ---

/// Consume and validate a CRLF sequence
fn consumeCRLF(self: Protocol) Error!void {
    const crlf = try self.reader.take(2);
    if (crlf[0] != '\r' or crlf[1] != '\n') return error.ProtocolError;
}

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
    try self.consumeCRLF();

    return .{ .bulk_string = buf };
}

fn readArray(self: Protocol, allocator: std.mem.Allocator, len_str: []const u8) Error!Value {
    const len = try std.fmt.parseInt(i64, len_str, 10);
    if (len == -1) return .{ .array = &.{} }; // nil array (empty slice)
    if (len < 0) return error.ProtocolError;

    const size: usize = @intCast(len);
    const arr = try allocator.alloc(Value, size);
    var initialized: usize = 0;
    errdefer {
        for (arr[0..initialized]) |elem| {
            freeValue(allocator, elem);
        }
        allocator.free(arr);
    }

    for (arr) |*elem| {
        elem.* = try self.readValue(allocator);
        initialized += 1;
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

// --- Response reading ---

/// Read and validate a RESP status line, returning the type byte and data
fn readResponseLine(self: Protocol) Error!struct { type_byte: u8, data: []const u8 } {
    const line = try self.reader.takeDelimiterInclusive('\n');
    if (line.len < 2 or line[line.len - 2] != '\r') return error.ProtocolError;
    return .{ .type_byte = line[0], .data = line[1 .. line.len - 2] };
}

/// Read a simple string response (expects +OK)
pub fn readSimpleStringResponse(self: Protocol) Error!void {
    const resp = try self.readResponseLine();
    if (resp.type_byte == '+') return;
    if (resp.type_byte == '-') return error.RedisError;
    return error.UnexpectedType;
}

/// Read an integer response
pub fn readIntegerResponse(self: Protocol) Error!i64 {
    const resp = try self.readResponseLine();
    if (resp.type_byte == ':') return try std.fmt.parseInt(i64, resp.data, 10);
    if (resp.type_byte == '-') return error.RedisError;
    return error.UnexpectedType;
}

/// Read a bulk string response into the provided buffer
/// Returns slice into the buffer, or null for nil
pub fn readBulkStringResponse(self: Protocol, buf: []u8) Error!?[]u8 {
    const resp = try self.readResponseLine();
    if (resp.type_byte == '$') {
        const len = try std.fmt.parseInt(i64, resp.data, 10);
        if (len == -1) return null;
        if (len < 0) return error.ProtocolError;

        const size: usize = @intCast(len);
        if (size > buf.len) return error.ValueTooLarge;

        try self.reader.readSliceAll(buf[0..size]);
        try self.consumeCRLF();

        return buf[0..size];
    }
    if (resp.type_byte == '-') return error.RedisError;
    return error.UnexpectedType;
}

/// Read a bulk string response, allocating memory from the given allocator
/// Returns owned slice, or null for nil
pub fn readBulkStringResponseAlloc(self: Protocol, allocator: std.mem.Allocator) (Error || error{OutOfMemory})!?[]u8 {
    const resp = try self.readResponseLine();
    if (resp.type_byte == '$') {
        const len = try std.fmt.parseInt(i64, resp.data, 10);
        if (len == -1) return null;
        if (len < 0) return error.ProtocolError;

        const size: usize = @intCast(len);
        const result = try allocator.alloc(u8, size);
        errdefer allocator.free(result);

        try self.reader.readSliceAll(result);
        try self.consumeCRLF();

        return result;
    }
    if (resp.type_byte == '-') return error.RedisError;
    return error.UnexpectedType;
}

/// Read a response that may be +OK, nil, or a bulk string
/// The response value is discarded
pub fn readOkOrNilResponse(self: Protocol) Error!void {
    const resp = try self.readResponseLine();
    if (resp.type_byte == '+') return;
    if (resp.type_byte == '$') {
        const len = try std.fmt.parseInt(i64, resp.data, 10);
        if (len == -1) return;
        if (len < 0) return error.ProtocolError;
        const size: usize = @intCast(len);
        if (size > 0) try self.reader.discardAll(size);
        try self.consumeCRLF();
        return;
    }
    if (resp.type_byte == '-') return error.RedisError;
    return error.UnexpectedType;
}

// --- High-level command helpers ---

/// Execute a command and expect a simple string response (like "OK")
pub fn execSimpleString(self: Protocol, args: []const []const u8) Error!void {
    try self.writeCommand(args);
    return self.readSimpleStringResponse();
}

/// Execute a command and expect an integer response
pub fn execInteger(self: Protocol, args: []const []const u8) Error!i64 {
    try self.writeCommand(args);
    return self.readIntegerResponse();
}

/// Execute a command and expect a bulk string response
/// Returns slice into the provided buffer, or null for nil
pub fn execBulkString(self: Protocol, args: []const []const u8, buf: []u8) Error!?[]u8 {
    try self.writeCommand(args);
    return self.readBulkStringResponse(buf);
}

/// Execute a command that may return +OK, nil, or a bulk string (like SET with NX/XX/GET)
/// The response is discarded - we only care about success/failure
pub fn execOkOrNil(self: Protocol, args: []const []const u8) Error!void {
    try self.writeCommand(args);
    return self.readOkOrNilResponse();
}

/// Execute a command and expect a bulk string response, allocating memory
pub fn execBulkStringAlloc(self: Protocol, allocator: std.mem.Allocator, args: []const []const u8) (Error || error{OutOfMemory})!?[]u8 {
    try self.writeCommand(args);
    return self.readBulkStringResponseAlloc(allocator);
}

/// Read a RESP array of non-null bulk strings, allocating memory
pub fn readBulkStringArrayAlloc(self: Protocol, allocator: std.mem.Allocator) (Error || error{OutOfMemory})![][]u8 {
    const resp = try self.readResponseLine();
    if (resp.type_byte == '-') return error.RedisError;
    if (resp.type_byte != '*') return error.UnexpectedType;
    const len = try std.fmt.parseInt(i64, resp.data, 10);
    if (len < 0) return error.ProtocolError;
    if (len == 0) return try allocator.alloc([]u8, 0);
    const size: usize = @intCast(len);
    const result = try allocator.alloc([]u8, size);
    var init: usize = 0;
    errdefer {
        for (result[0..init]) |s| allocator.free(s);
        allocator.free(result);
    }
    for (result) |*item| {
        item.* = (try self.readBulkStringResponseAlloc(allocator)) orelse return error.ProtocolError;
        init += 1;
    }
    return result;
}

/// Read a RESP array of optional bulk strings (elements may be nil), allocating memory
pub fn readOptBulkStringArrayAlloc(self: Protocol, allocator: std.mem.Allocator) (Error || error{OutOfMemory})![]?[]u8 {
    const resp = try self.readResponseLine();
    if (resp.type_byte == '-') return error.RedisError;
    if (resp.type_byte != '*') return error.UnexpectedType;
    const len = try std.fmt.parseInt(i64, resp.data, 10);
    if (len < 0) return error.ProtocolError;
    if (len == 0) return try allocator.alloc(?[]u8, 0);
    const size: usize = @intCast(len);
    const result = try allocator.alloc(?[]u8, size);
    var init: usize = 0;
    errdefer {
        for (result[0..init]) |s| if (s) |str| allocator.free(str);
        allocator.free(result);
    }
    for (result) |*item| {
        item.* = try self.readBulkStringResponseAlloc(allocator);
        init += 1;
    }
    return result;
}

/// Execute a command and read an array of non-null bulk strings
pub fn execBulkStringArrayAlloc(self: Protocol, allocator: std.mem.Allocator, args: []const []const u8) (Error || error{OutOfMemory})![][]u8 {
    try self.writeCommand(args);
    return self.readBulkStringArrayAlloc(allocator);
}

/// Execute a command and read an array of optional bulk strings
pub fn execOptBulkStringArrayAlloc(self: Protocol, allocator: std.mem.Allocator, args: []const []const u8) (Error || error{OutOfMemory})![]?[]u8 {
    try self.writeCommand(args);
    return self.readOptBulkStringArrayAlloc(allocator);
}

/// Read a RESP array of alternating field/value bulk strings directly into []FieldValue
pub fn readFieldPairsAlloc(self: Protocol, allocator: std.mem.Allocator) (Error || error{OutOfMemory})![]FieldValue {
    const resp = try self.readResponseLine();
    if (resp.type_byte == '-') return error.RedisError;
    if (resp.type_byte != '*') return error.UnexpectedType;
    const total_len = try std.fmt.parseInt(i64, resp.data, 10);
    if (total_len < 0 or @mod(total_len, 2) != 0) return error.ProtocolError;
    const pair_count: usize = @intCast(@divExact(total_len, 2));
    const pairs = try allocator.alloc(FieldValue, pair_count);
    var init: usize = 0;
    errdefer {
        for (pairs[0..init]) |fv| {
            allocator.free(fv.field);
            allocator.free(fv.value);
        }
        allocator.free(pairs);
    }
    for (pairs) |*fv| {
        const field = (try self.readBulkStringResponseAlloc(allocator)) orelse return error.ProtocolError;
        errdefer allocator.free(field);
        const value = (try self.readBulkStringResponseAlloc(allocator)) orelse return error.ProtocolError;
        errdefer allocator.free(value);
        fv.* = .{ .field = field, .value = value };
        init += 1;
    }
    return pairs;
}

/// Execute a command and read alternating field/value pairs into []FieldValue
pub fn execFieldPairsAlloc(self: Protocol, allocator: std.mem.Allocator, args: []const []const u8) (Error || error{OutOfMemory})![]FieldValue {
    try self.writeCommand(args);
    return self.readFieldPairsAlloc(allocator);
}
