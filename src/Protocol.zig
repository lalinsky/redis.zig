//! Meta protocol encoder/decoder for memcached.
//!
//! Commands:
//! - mg <key> [flags] - meta get
//! - ms <key> <size> [flags] - meta set
//! - md <key> [flags] - meta delete
//! - ma <key> [flags] - meta arithmetic
//! - mn - meta noop
//!
//! Response codes:
//! - HD - hit/stored/deleted (success, no value)
//! - VA <size> - value follows
//! - EN - not found (end, miss)
//! - NS - not stored
//! - EX - exists (CAS conflict)
//! - NF - not found (for delete/arithmetic)

const std = @import("std");

const Protocol = @This();

reader: *std.Io.Reader,
writer: *std.Io.Writer,

pub const Error = error{
    ServerError,
    NotStored,
    NotFound,
    ValueTooLarge,
    Exists,
    ReadFailed,
    WriteFailed,
    EndOfStream,
    StreamTooLong,
};

pub const Info = struct {
    value: []u8,
    flags: u32,
    cas: u64,
};

pub const GetOpts = struct {
    ttl: ?u32 = null,
};

pub const SetOpts = struct {
    ttl: u32 = 0,
    flags: u32 = 0,
    cas: ?u64 = null,
};

pub const SetMode = enum {
    set,
    add,
    replace,
    append,
    prepend,
};

pub fn get(self: Protocol, key: []const u8, buf: []u8, opts: GetOpts) Error!?Info {
    // Build command: mg <key> v f c [T<ttl>]
    self.writer.print("mg {s} v f c", .{key}) catch return error.WriteFailed;
    if (opts.ttl) |ttl| {
        self.writer.print(" T{d}", .{ttl}) catch return error.WriteFailed;
    }
    self.writer.writeAll("\r\n") catch return error.WriteFailed;
    self.writer.flush() catch return error.WriteFailed;

    // Read response
    const response = try self.readResponse();

    switch (response) {
        .value => |info| {
            if (info.size > buf.len) return error.ValueTooLarge;
            self.reader.readSliceAll(buf[0..info.size]) catch return error.ReadFailed;
            _ = self.reader.takeDelimiterInclusive('\n') catch return error.ReadFailed;

            return .{
                .value = buf[0..info.size],
                .flags = info.flags orelse 0,
                .cas = info.cas orelse 0,
            };
        },
        .not_found => return null,
        .server_error => return error.ServerError,
        else => return error.ServerError,
    }
}

// --- Set ---

pub fn set(self: Protocol, key: []const u8, value: []const u8, opts: SetOpts, mode: SetMode) Error!void {
    // Build command: ms <key> <size> [flags]
    self.writer.print("ms {s} {d}", .{ key, value.len }) catch return error.WriteFailed;

    if (opts.ttl > 0) {
        self.writer.print(" T{d}", .{opts.ttl}) catch return error.WriteFailed;
    }
    if (opts.flags > 0) {
        self.writer.print(" F{d}", .{opts.flags}) catch return error.WriteFailed;
    }
    if (opts.cas) |cas| {
        self.writer.print(" C{d}", .{cas}) catch return error.WriteFailed;
    }

    const mode_flag: ?u8 = switch (mode) {
        .set => null,
        .add => 'E',
        .replace => 'R',
        .append => 'A',
        .prepend => 'P',
    };
    if (mode_flag) |m| {
        self.writer.print(" M{c}", .{m}) catch return error.WriteFailed;
    }

    self.writer.writeAll("\r\n") catch return error.WriteFailed;
    self.writer.writeAll(value) catch return error.WriteFailed;
    self.writer.writeAll("\r\n") catch return error.WriteFailed;
    self.writer.flush() catch return error.WriteFailed;

    // Read response
    const response = try self.readResponse();

    switch (response) {
        .hit => return,
        .not_stored => return error.NotStored,
        .exists => return error.Exists,
        .not_found => return error.NotFound,
        .server_error => return error.ServerError,
        else => return error.ServerError,
    }
}

// --- Delete ---

pub fn delete(self: Protocol, key: []const u8) Error!void {
    self.writer.print("md {s}\r\n", .{key}) catch return error.WriteFailed;
    self.writer.flush() catch return error.WriteFailed;

    const response = try self.readResponse();

    switch (response) {
        .hit => return,
        .not_found => return error.NotFound,
        .server_error => return error.ServerError,
        else => return error.ServerError,
    }
}

// --- Arithmetic ---

pub fn incr(self: Protocol, key: []const u8, delta: u64) Error!u64 {
    return self.arithmetic(key, delta, false);
}

pub fn decr(self: Protocol, key: []const u8, delta: u64) Error!u64 {
    return self.arithmetic(key, delta, true);
}

fn arithmetic(self: Protocol, key: []const u8, delta: u64, is_decr: bool) Error!u64 {
    // ma <key> v D<delta> [MD]
    self.writer.print("ma {s} v D{d}", .{ key, delta }) catch return error.WriteFailed;
    if (is_decr) {
        self.writer.writeAll(" MD") catch return error.WriteFailed;
    }
    self.writer.writeAll("\r\n") catch return error.WriteFailed;
    self.writer.flush() catch return error.WriteFailed;

    const response = try self.readResponse();

    switch (response) {
        .value => |info| {
            var buf: [32]u8 = undefined;
            if (info.size > buf.len) return error.ServerError;
            self.reader.readSliceAll(buf[0..info.size]) catch return error.ReadFailed;
            _ = self.reader.takeDelimiterInclusive('\n') catch return error.ReadFailed;

            return std.fmt.parseInt(u64, buf[0..info.size], 10) catch error.ServerError;
        },
        .not_found => return error.NotFound,
        .server_error => return error.ServerError,
        else => return error.ServerError,
    }
}

// --- Touch ---

pub fn touch(self: Protocol, key: []const u8, ttl: u32) Error!void {
    self.writer.print("mg {s} T{d}\r\n", .{ key, ttl }) catch return error.WriteFailed;
    self.writer.flush() catch return error.WriteFailed;

    const response = try self.readResponse();

    switch (response) {
        .hit => return,
        .not_found => return error.NotFound,
        .server_error => return error.ServerError,
        else => return error.ServerError,
    }
}

// --- Admin ---

pub fn flushAll(self: Protocol) Error!void {
    self.writer.writeAll("flush_all\r\n") catch return error.WriteFailed;
    self.writer.flush() catch return error.WriteFailed;

    const line = self.reader.takeDelimiterInclusive('\n') catch return error.ReadFailed;
    const trimmed = std.mem.trimRight(u8, line, "\r\n");

    if (!std.mem.eql(u8, trimmed, "OK")) {
        return error.ServerError;
    }
}

pub fn version(self: Protocol, buf: []u8) Error![]u8 {
    self.writer.writeAll("version\r\n") catch return error.WriteFailed;
    self.writer.flush() catch return error.WriteFailed;

    const line = self.reader.takeDelimiterInclusive('\n') catch return error.ReadFailed;
    const trimmed = std.mem.trimRight(u8, line, "\r\n");

    if (std.mem.startsWith(u8, trimmed, "VERSION ")) {
        const ver = trimmed[8..];
        if (ver.len > buf.len) return error.ValueTooLarge;
        @memcpy(buf[0..ver.len], ver);
        return buf[0..ver.len];
    }
    return error.ServerError;
}

// --- Response parsing ---

const Response = union(enum) {
    hit: HitInfo,
    value: ValueInfo,
    not_found,
    not_stored,
    exists,
    server_error: []const u8,
};

const HitInfo = struct {
    flags: ?u32 = null,
    cas: ?u64 = null,
};

const ValueInfo = struct {
    size: usize,
    flags: ?u32 = null,
    cas: ?u64 = null,
};

fn readResponse(self: Protocol) Error!Response {
    const line = self.reader.takeDelimiterInclusive('\n') catch return error.ReadFailed;
    const trimmed = std.mem.trimRight(u8, line, "\r\n");
    return parseResponse(trimmed);
}

fn parseResponse(line: []const u8) Error!Response {
    if (line.len < 2) return error.ServerError;

    if (std.mem.startsWith(u8, line, "VA ")) {
        return parseValueResponse(line[3..]);
    } else if (std.mem.startsWith(u8, line, "HD")) {
        return .{ .hit = parseFlags(line[2..]) };
    } else if (std.mem.startsWith(u8, line, "EN")) {
        return .not_found;
    } else if (std.mem.startsWith(u8, line, "NS")) {
        return .not_stored;
    } else if (std.mem.startsWith(u8, line, "EX")) {
        return .exists;
    } else if (std.mem.startsWith(u8, line, "NF")) {
        return .not_found;
    } else if (std.mem.startsWith(u8, line, "SERVER_ERROR")) {
        return .{ .server_error = line };
    }

    return error.ServerError;
}

fn parseValueResponse(rest: []const u8) Error!Response {
    var it = std.mem.splitScalar(u8, rest, ' ');
    const size_str = it.next() orelse return error.ServerError;
    const size = std.fmt.parseInt(usize, size_str, 10) catch return error.ServerError;

    var info = ValueInfo{ .size = size };

    while (it.next()) |flag| {
        if (flag.len == 0) continue;
        switch (flag[0]) {
            'f' => info.flags = std.fmt.parseInt(u32, flag[1..], 10) catch null,
            'c' => info.cas = std.fmt.parseInt(u64, flag[1..], 10) catch null,
            else => {},
        }
    }

    return .{ .value = info };
}

fn parseFlags(rest: []const u8) HitInfo {
    var info = HitInfo{};
    var it = std.mem.splitScalar(u8, rest, ' ');

    while (it.next()) |flag| {
        if (flag.len == 0) continue;
        switch (flag[0]) {
            'f' => info.flags = std.fmt.parseInt(u32, flag[1..], 10) catch null,
            'c' => info.cas = std.fmt.parseInt(u64, flag[1..], 10) catch null,
            else => {},
        }
    }

    return info;
}

// --- Tests ---

test "parseResponse VA" {
    const resp = try parseResponse("VA 5 f123 c456789");
    try std.testing.expectEqual(Response{ .value = .{
        .size = 5,
        .flags = 123,
        .cas = 456789,
    } }, resp);
}

test "parseResponse HD" {
    const resp = try parseResponse("HD");
    try std.testing.expectEqual(Response{ .hit = .{} }, resp);
}

test "parseResponse EN" {
    const resp = try parseResponse("EN");
    try std.testing.expectEqual(Response.not_found, resp);
}

test "parseResponse NS" {
    const resp = try parseResponse("NS");
    try std.testing.expectEqual(Response.not_stored, resp);
}

test "parseResponse EX" {
    const resp = try parseResponse("EX");
    try std.testing.expectEqual(Response.exists, resp);
}
