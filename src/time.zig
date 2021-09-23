const std = @import("std");
const assert = std.debug.assert;
const config = @import("./config.zig");

const target = std.builtin.target;
const is_darwin = target.isDarwin();
const is_windows = target.os.tag == .windows;

pub const Time = struct {
    const Self = @This();

    /// Hardware and/or software bugs can mean that the monotonic clock may regress.
    /// One example (of many): https://bugzilla.redhat.com/show_bug.cgi?id=448449
    /// We crash the process for safety if this ever happens, to protect against infinite loops.
    /// It's better to crash and come back with a valid monotonic clock than get stuck forever.
    monotonic_guard: u64 = 0,

    /// A timestamp to measure elapsed time, meaningful only on the same system, not across reboots.
    /// Always use a monotonic timestamp if the goal is to measure elapsed time.
    /// This clock is not affected by discontinuous jumps in the system time, for example if the
    /// system administrator manually changes the clock.
    pub fn monotonic(self: *Self) u64 {
        const m = blk: {
            if (is_windows) {
                // Use QPC since it ticks while the system is suspended.
                // No need to cache QPF on modern (Win7+) systems since it just
                // reads from KUSER_SHARED_DATA (vdso memory mapped to the process).
                const counter = std.os.windows.QueryPerformanceCounter();
                const frequency = std.os.windows.QueryPerformanceFrequency();

                // Specialize for a common frequency value which is common for modern systems.
                // This avoids the division heavy computations below.
                // 10Mhz equates to a precision of 100ns which is windows interrupt time precision.
                // https://github.com/microsoft/STL/blob/6d2f8b0ed88ea6cba26cc2151f47f678442c1663/stl/inc/chrono#L694-L701
                const common_freq = 10_000_000;
                if (frequency == common_freq) {
                    return counter * (std.time.ns_per_s / common_freq);
                }

                // Computes (counter * std.time.ns_per_s) / frequency without overflowing 
                // on the multiplication as long as both counter and (std.time.ns_per_s * frequency)
                // fit in i64 which is the case for QPC/QPF readings. 
                const part = ((counter % frequency) * std.time.ns_per_s) / frequency;
                const whole = (counter / frequency) * std.time.ns_per_s;
                return whole + part; 
            }

            // Uses mach_continuous_time() instead of mach_absolute_time() as it ticks while suspended.
            // https://developer.apple.com/documentation/kernel/1646199-mach_continuous_time
            // https://opensource.apple.com/source/Libc/Libc-1158.1.2/gen/clock_gettime.c.auto.html
            if (is_darwin) {
                const darwin = struct {
                    const mach_timebase_info_t = std.os.darwin.mach_timebase_info_data;
                    extern "c" fn mach_timebase_info(info: *mach_timebase_info_t) std.os.darwin.kern_return_t;
                    extern "c" fn mach_continuous_time() u64;
                };

                const now = darwin.mach_continuous_time();
                var info: darwin.mach_timebase_info_t = undefined;
                if (darwin.mach_timebase_info(&info) != 0) @panic("mach_timebase_info() failed");
                return (now * info.numer) / info.denom;
            }

            // The true monotonic clock on Linux is not in fact CLOCK_MONOTONIC:
            // CLOCK_MONOTONIC excludes elapsed time while the system is suspended (e.g. VM migration).
            // CLOCK_BOOTTIME is the same as CLOCK_MONOTONIC but includes elapsed time during a suspend.
            // For more detail and why CLOCK_MONOTONIC_RAW is even worse than CLOCK_MONOTONIC,
            // see https://github.com/ziglang/zig/pull/933#discussion_r656021295.
            var ts: std.os.timespec = undefined;
            std.os.clock_gettime(std.os.CLOCK_BOOTTIME, &ts) catch @panic("CLOCK_BOOTTIME required");
            break :blk @intCast(u64, ts.tv_sec) * std.time.ns_per_s + @intCast(u64, ts.tv_nsec);
        };

        // "Oops!...I Did It Again"
        if (m < self.monotonic_guard) @panic("a hardware/kernel bug regressed the monotonic clock");
        self.monotonic_guard = m;
        return m;
    }

    /// A timestamp to measure real (i.e. wall clock) time, meaningful across systems, and reboots.
    /// This clock is affected by discontinuous jumps in the system time.
    pub fn realtime(self: *Self) i64 {
        if (is_windows) {
            var ft: std.os.windows.FILETIME = undefined;
            std.os.windows.kernel32.GetSystemTimeAsFileTime(&ft);
            const ft64 = (@as(u64, ft.dwHighDateTime) << 32) | ft.dwLowDateTime;
            
            // FILETIME from the system is in units of 100ns since 1 January 1601.
            // Make it relative to the unix epoch of 1 January 1970
            // and scale it back up to nanoseconds.
            const epoch_adj = std.time.epoch.windows * (std.time.ns_per_s / 100);
            return (@bitCast(i64, ft64) + epoch_adj) * 100;
        }

        // macos has supported clock_gettime() since 10.12:
        // https://opensource.apple.com/source/Libc/Libc-1158.1.2/gen/clock_gettime.3.auto.html

        var ts: std.os.timespec = undefined;
        std.os.clock_gettime(std.os.CLOCK_REALTIME, &ts) catch unreachable;
        return @as(i64, ts.tv_sec) * std.time.ns_per_s + ts.tv_nsec;
    }

    pub fn tick(self: *Self) void {}
};