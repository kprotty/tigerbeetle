const std = @import("std");
const assert = std.debug.assert;
const os = std.os;

const FIFO = @import("fifo.zig").FIFO;
const IO_Linux = @import("io/linux.zig").IO;
const IO_Darwin = @import("io/darwin.zig").IO;
const IO_Windows = @import("io/windows.zig").IO;

pub const IO = switch (std.Target.current.os.tag) {
    .linux => IO_Linux,
    .windows => IO_Windows,
    .macos, .tvos, .watchos, .ios => IO_Darwin,
    else => @compileError("IO is not supported for platform"),
};

pub fn buffer_limit(buffer_len: usize) usize {
    // Linux limits how much may be written in a `pwrite()/pread()` call, which is `0x7ffff000` on
    // both 64-bit and 32-bit systems, due to using a signed C int as the return value, as well as
    // stuffing the errno codes into the last `4096` values.
    // Darwin limits writes to `0x7fffffff` bytes, more than that returns `EINVAL`.
    // The corresponding POSIX limit is `std.math.maxInt(isize)`.
    const limit = switch (std.Target.current.os.tag) {
        .linux => 0x7ffff000,
        .macos, .ios, .watchos, .tvos => std.math.maxInt(i32),
        else => std.math.maxInt(isize),
    };
    return std.math.min(limit, buffer_len);
}

pub const PollMode = enum {
    blocking,
    non_blocking,
};

pub const Extensions = struct {
    pub fn tick(self: *IO) !void {
        try self.poll(.non_blocking);
    }

    pub fn run_for_ns(self: *IO, nanoseconds: u63) !void {
        var timed_out = false;
        var completion: IO.Completion = undefined;
        const on_timeout = struct {
            fn callback(
                timed_out_ptr: *bool,
                _completion: *IO.Completion,
                _result: IO.TimeoutError!void,
            ) void {
                timed_out_ptr.* = true;
            }
        }.callback;

        // Submit a timeout which sets the timed_out value to true to terminate the loop below.
        self.timeout(
            *bool,
            &timed_out,
            on_timeout,
            &completion,
            nanoseconds,
        );

        // Loop until our timeout completion is processed above, which sets timed_out to true.
        // LLVM shouldn't be able to cache timed_out's value here since its address escapes above.
        while (!timed_out) {
            try self.poll(.blocking);
        }
    }
};

test "I/O" {
    _ = @import("io/test.zig");
}
