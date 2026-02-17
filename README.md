# redis.zig

A Redis client library for Zig, built on [zio](https://github.com/lalinsky/zio) for async I/O.

## Features

- Async I/O via zio coroutines
- Connection pooling
- RESP2 protocol implementation
- Basic string commands (GET, SET, DEL, INCR, DECR, etc.)
- TTL and expiration support
- Retry logic with configurable attempts and intervals

## Example

```zig
const std = @import("std");
const zio = @import("zio");
const redis = @import("redis");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var rt = try zio.Runtime.init(gpa.allocator(), .{});
    defer rt.deinit();

    var client = try redis.connect(gpa.allocator(), "localhost:6379");
    defer client.deinit();

    // Set a value
    try client.set("hello", "world", .{});

    // Get a value
    var buf: [1024]u8 = undefined;
    if (try client.get("hello", &buf)) |value| {
        std.debug.print("Value: {s}\n", .{value});
    }

    // Set with expiration
    try client.set("temp", "data", .{ .ex = 60 });

    // Increment a counter
    try client.set("counter", "0", .{});
    const val = try client.incrBy("counter", 5);
    std.debug.print("Counter: {d}\n", .{val});
}
```

## API

### String Commands

- `get(key, buf)` - Get the value of a key
- `set(key, value, opts)` - Set the string value of a key
  - Options: `ex` (expire seconds), `nx` (only if not exists), `xx` (only if exists), `get` (return old value)
- `del(keys)` - Delete one or more keys
- `incr(key)` - Increment the integer value of a key by one
- `incrBy(key, delta)` - Increment the integer value by amount
- `decr(key)` - Decrement the integer value of a key by one
- `decrBy(key, delta)` - Decrement the integer value by amount
- `expire(key, seconds)` - Set a timeout on key
- `ttl(key)` - Get the time to live for a key
- `exists(keys)` - Determine if keys exist

### Server Commands

- `ping(message)` - Ping the server
- `flushDB()` - Remove all keys from the current database
- `dbSize()` - Return the number of keys in the current database

## Installation

Add redis.zig as a dependency in your `build.zig.zon`:

```bash
zig fetch --save "git+https://github.com/lalinsky/redis.zig"
```

In your `build.zig`:

```zig
const redis = b.dependency("redis", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("redis", redis.module("redis"));
```

## Development

Run tests:

```bash
zig build test
```

Run example:

```bash
docker compose up -d
zig build run
```

## Roadmap

- [ ] Pipelining support
- [ ] Pub/Sub support
- [ ] Transaction support (MULTI/EXEC)
- [ ] Data structure commands (lists, sets, hashes)
- [ ] Lua scripting support
- [ ] Cluster support

## License

MIT
