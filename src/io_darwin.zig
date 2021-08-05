const std = @import("std");
const os = std.os;
const assert = std.debug.assert;

pub const CLOCK_MONOTONIC = 1;
pub const __kernel_timespec = os.timespec;

fn monotonic_timestamp() u64 {
    var info: os.darwin.mach_timebase_info_data = undefined;
    os.darwin.mach_timebase_info(&info);
    const now = os.darwin.mach_absolute_time();
    return (now * info.numer) / info.denom;
}

pub fn clock_gettime(clock_id: u32, ts: *os.timespec) !void {
    if (clock_id != CLOCK_MONOTONIC) return error.UnsupportedClock;
    const now = monotonic_timestamp();
    ts.tv_sec = @intCast(@TypeOf(ts.tv_sec), now / std.time.ns_per_s);
    ts.tv_nsec = @intCast(@TypeOf(ts.tv_nsec), now % std.time.ns_per_s);
}

const io_uring_rqe = struct {
    next: ?*io_uring_rqe,
    sqe: io_uring_sqe,
};

pub const io_uring_cqe = struct {
    user_data: u64,
    res: i32,
};

pub const io_uring_sqe = struct {
    user_data: u64,
    operation: union(enum) {
        openat: struct {
            fd: os.fd_t,
            flags: u32,
            mode: os.mode_t,
            path: [*:0]const u8,
        },
        close: struct {
            fd: os.fd_t,
        },
        read: struct {
            fd: os.fd_t,
            len: u32,
            buf: [*]u8,
            offset: u64,
        },
        write: struct {
            fd: os.fd_t,
            len: u32,
            buf: [*]const u8,
            offset: u64,
        },
        fsync: struct {
            fd: os.fd_t,
            flags: u32,
        },
        accept: struct {
            socket: os.socket_t,
            addr: *os.sockaddr,
            addr_len: *os.socklen_t,
        },
        connect: struct {
            socket: os.socket_t,
            addr: *const os.sockaddr,
            addr_len: os.socklen_t,
        },
        send: struct {
            socket: os.socket_t,
            len: u32,
            buf: [*]const u8,
            flags: u32,
        },
        recv: struct {
            socket: os.socket_t,
            len: u32,
            buf: [*]u8,
            flags: u32,
        },
        timeout: struct {
            expires: u64,
            is_absolute: bool,
            min_complete: u32,
        },
    },
};

pub const IORING_TIMEOUT_ABS = 1;
pub const IORING_ENTER_GETEVENTS = 1;

pub const IO_Uring = struct {
    /// Kernel queue of I/O events
    kq: struct {
        fd: os.fd_t,
        events: []os.Kevent,
    },
    /// Timeout queue of I/O requests waiting to expire
    tq: struct {
        pending: ?*io_uring_rqe = null,
        min_complete: ?*io_uring_rqe = null,
    },
    /// Request queue of cached I/O requests  
    rq: struct {
        inflight: u32 = 0,
        list: ?*io_uring_rqe,
        array: []io_uring_rqe,
    },
    /// Completion queue of completed I/O requests
    cq: struct {
        head: u32 = 0,
        tail: u32 = 0,
        array: []io_uring_cqe,
    },
    /// Submission queue of submittable I/O requests
    sq: struct {
        head: u32 = 0,
        tail: u32 = 0,
        sqe_head: u32 = 0,
        sqe_tail: u32 = 0,
        array: []io_uring_sqe,
    },

    // Darwin should always be linked to libc for this to be available
    const allocator = std.heap.c_allocator;

    pub fn init(entries: u12, flags: u32) !IO_Uring {
        // sq_entries requirement of linux io_uring
        if (entries == 0) return error.EntriesZero;
        if (!std.math.isPowerOfTwo(entries)) return error.EntriesNotPowerOfTwo;

        // Allocate the kq instance for evented I/O
        const kq_fd = try os.kqueue();
        assert(kq_fd > 0);
        errdefer os.close(kq_fd);

        // Allocate enough kevent's to register submissions in one go
        const kq_events = try allocator.alloc(os.Kevent, entries);
        errdefer allocator.free(kq_events);

        // Allocate only enough Submission entries to fill kevents 
        const sq_array = try allocator.alloc(io_uring_sqe, kq_events.len);
        errdefer allocator.free(sq_array);

        // Double the completion queue size to account for inflight submissions:
        //
        // (4.2 COMMUNICATION CHANNEL) https://kernel.dk/io_uring.pdf
        // "
        //   However, since the sqe lifetime is only that of the actual submission of it, 
        //   it's possible for the application to drive a higher pending request count 
        //   than the SQ ring size would indicate.
        // 
        //   [...]
        //
        //   By default, the CQ ring is twice the size of the SQ ring. This allows the
        //   application some amount of flexibility in managing this aspect
        // "
        const cq_array = try allocator.alloc(io_uring_cqe, sq_array.len * 2);
        errdefer allocator.free(cq_array);

        // Allocate only enough request entries to fill the completion queue
        const rq_array = try allocator.alloc(io_uring_rqe, cq_array.len);
        errdefer allocator.free(rq_array);

        // Create a free list of request entries
        for (rq_array[0..(rq_array.len - 1)]) |*rqe, i| rqe.next = &rq_array[i + 1];
        rq_array[rq_array.len - 1].next = null;
        const rq_list = &rq_array[0];

        return IO_Uring{
            .kq = .{
                .fd = kq_fd,
                .events = kq_events,
            },
            .tq = .{},
            .rq = .{
                .list = rq_list,
                .array = rq_array,
            },
            .cq = .{ .array = cq_array },
            .sq = .{ .array = sq_array },
        };
    }

    pub fn deinit(self: *IO_Uring) void {
        os.close(self.kq.fd);
        allocator.free(self.rq.array);
        allocator.free(self.cq.array);
        allocator.free(self.sq.array);
    }

    pub fn sq_ready(self: *IO_Uring) u32 {
        const size = self.sq.sqe_tail -% self.sq.head;
        assert(size <= self.sq.array.len);
        return size;
    }

    pub fn get_sqe(self: *IO_Uring) !*io_uring_sqe {
        const capacity = self.sq.array.len;
        if (self.sq_ready() == capacity) return error.SubmissionQueueFull;

        assert(std.math.isPowerOfTwo(capacity));
        const sqe = &self.sq.array[self.sq.sqe_tail & (capacity - 1)];

        self.sq.sqe_tail +%= 1;
        return sqe;
    }

    pub fn flush_sq(self: *IO_Uring) u32 {
        self.sq.sqe_head = self.sq.sqe_tail;
        self.sq.tail = self.sq.sqe_tail;
        return self.sq_ready();
    }

    pub fn cq_ready(self: *IO_Uring) u32 {
        const size = self.cq.tail -% self.cq.head;
        assert(size <= self.cq.array.len);
        return size;
    }

    pub fn copy_cqes(self: *IO_Uring, cqes: []io_uring_cqe, wait_nr: u32) !u32 {
        var num_cqes = self.cq_ready();
        if (num_cqes == 0) {
            if (wait_nr == 0) return 0;
            const submitted = try self.enter(0, wait_nr, IORING_ENTER_GETEVENTS);
            assert(submitted == 0);
            num_cqes = self.cq_ready();
        }

        const capacity = self.cq.array.len;
        assert(num_cqes <= capacity);
        assert(num_cqes > 0);

        const copied = std.math.min(num_cqes, cqes.len);
        for (self.cq.array[0..copied]) |_, i| {
            const index = self.cq.head +% @intCast(u32, i);
            assert(std.math.isPowerOfTwo(capacity));
            cqes[i] = self.cq.array[index & (capacity - 1)];
        }

        self.cq.head +%= copied;
        return copied;
    }

    pub fn submit_and_wait(self: *IO_Uring, wait_nr: u32) !u32 {
        const to_submit = self.flush_sq();
        const flags: u32 = if (wait_nr > 0) IORING_ENTER_GETEVENTS else 0;
        return self.enter(to_submit, wait_nr, flags);
    }

    pub fn enter(self: *IO_Uring, to_submit: u32, min_complete: u32, flags: u32) !u32 {
        var rq_timeouts = false;
        var rq_reserved: u32 = 0;
        var rq_list = self.rq.list;
        var change_events: u32 = 0;

        const submitted = std.math.min(to_submit, self.sq_ready());
        assert(submitted <= self.sq.array.len);
        
        // Check before hand if submitting would over commit the completion queue
        const cq_available = self.cq.array.len - self.cq_ready();
        if (self.rq.inflight + submitted > cq_available) {
            return error.CompletionQueueOvercommitted;
        }

        // Allocate/reserve request entries using the submitted entries.
        // NOTE: Make sure not to change rqe.next as it's only reserving them not committing the reservation.
        // If there's not enough rqes to reserve, it means theres too many inflight and 
        // the caller that we should wait for some to become available through completions. 
        for (self.sq.array[0..submitted]) |_, i| {
            const capacity = self.sq.array.len;
            assert(std.math.isPowerOfTwo(capacity));
            
            const index = self.sq.head +% @intCast(u32, i);
            const sqe = self.sq.array[index & (capacity - 1)];

            const rqe = rq_list orelse return error.SystemResources;
            rq_list = rqe.next;
            rq_reserved += 1;
            rqe.sqe = sqe;

            const event_info = switch (sqe.operation) {
                .accept => |op| [2]c_int{ op.socket, os.EVFILT_READ },
                .connect => |op| [2]c_int{ op.socket, os.EVFILT_WRITE },
                .send => |op| [2]c_int{ op.socket, os.EVFILT_WRITE },
                .recv => |op| [2]c_int{ op.socket, os.EVFILT_READ },
                else => {
                    if (sqe.operation == .timeout) rq_timeouts = true;
                    continue;
                },
            };
            
            // For submissions that are evented, 
            // reserve a Kevent slot to register their filter down below.
            assert(change_events < self.kq.events.len);
            const event = &self.kq.events[change_events];
            change_events += 1;

            assert(self.is_evented(rqe));
            event.* = os.Kevent{
                .ident = @intCast(u32, event_info[0]),
                .filter = @intCast(i16, event_info[1]),
                .flags = os.EV_ADD | os.EV_ENABLE | os.EV_ONESHOT,
                .fflags = 0,
                .data = 0,
                .udata = @ptrToInt(rqe),
            };
        }

        while (true) {
            // Complete the enter() if there's we've submitted everything and there's nothing to wait for
            if (rq_reserved == 0) {
                if (min_complete == 0) return submitted;
                if (flags & IORING_ENTER_GETEVENTS == 0) return submitted;
                if (self.cq_ready() >= min_complete) return submitted;
            }

            // Call into kevent (syscall) if theres evented I/O requests to submit
            // or if there's no requests to process immediately, meaning we need to wait for completions
            var new_events: u32 = 0;
            if (change_events > 0 or (rq_reserved == 0 and self.rq.inflight > 0)) {
                // The default is to poll the kqueue using a 0-timeout, this is non-blocking
                var ts = std.mem.zeroes(os.timespec);
                var timeout_ptr: ?*const os.timespec = &ts;

                // If we have nothing to submit, we must be waiting for completions.
                // Default to waiting forever until an event occurs (timeout_ptr = null).
                if (rq_reserved == 0) {
                    timeout_ptr = null;
                    assert(self.rq.inflight > 0);
                    
                    // Try to expire timeouts and update the timeout_ptr to wait for a timeout to expire.
                    if (self.next_timeout()) |timeout_ns| {
                        timeout_ptr = &ts;
                        ts.tv_sec = @intCast(@TypeOf(ts.tv_sec), timeout_ns / std.time.ns_per_s);
                        ts.tv_nsec = @intCast(@TypeOf(ts.tv_nsec), timeout_ns % std.time.ns_per_s);
                    }

                    // After processing timeouts in next_timeout(), 
                    // it may have completed all pending timeout requests.
                    // If so, loop back to check return condition as we may not need to call into kevent().
                    if (self.rq.inflight == 0) {
                        continue;
                    }
                }

                const rc = os.system.kevent(
                    self.kq.fd,
                    self.kq.events.ptr,
                    @intCast(c_int, change_events),
                    self.kq.events.ptr,
                    @intCast(c_int, self.kq.events.len),
                    timeout_ptr,
                );

                new_events = switch (os.errno(rc)) {
                    0 => @intCast(u32, rc),
                    os.EACCES => return error.SubmissionQueueEntryInvalid, // invalid permission for event filter
                    os.EFAULT => return error.BufferInvalid, // self.kq.events memory was invalidated
                    os.EBADF => return error.FileDescriptorInvalid, // self.kq.fd was invalid
                    os.EINTR => return error.SignalInterrupt, // as usual...
                    os.EINVAL => return error.OpcodeNotSupported, // timeout_ptr or a filter was invalid
                    os.ENOENT => unreachable, // modified/deleted event not found (we only add events)
                    os.ENOMEM => return error.SystemResources, // no memory to register an event
                    os.ESRCH => unreachable, // event filter attached to non-existing process (we only register read/write events)
                    else => |e| return os.unexpectedErrno(e),
                };
            }

            // Commit and execute any reserved I/O requests.
            // This happens only after evented I/O requests were successfully submitted to kevent.
            // NOTE: This only executes non-evented I/O requests since they're not returned from kevent (new_events).
            if (rq_reserved > 0) {
                // Mark the reserved requests as commited/inflight
                self.rq.inflight += rq_reserved;
                rq_reserved = 0;

                // Consume the submitted sqes from the submission queue
                self.sq.head += submitted;
                assert(self.sq_ready() < self.sq.array.len);

                // Skip getting a timestamp if theres no timeout requests to process
                var now: u64 = undefined;
                if (rq_timeouts) {
                    now = monotonic_timestamp();
                    rq_timeouts = false;
                }

                // Execute all the non-evented requests
                var rq_execute = self.rq.list;
                self.rq.list = rq_list;
                while (rq_execute != rq_list) {
                    const rqe = rq_execute orelse unreachable;
                    rq_execute = rqe.next;
                    if (self.is_evented(rqe)) continue;
                    self.execute(rqe, now);
                }
            }

            // Execute and complete any evented I/O requests that were just reported ready by kevent
            change_events = 0;
            for (self.kq.events[0..new_events]) |event| {
                const rqe = @intToPtr(*io_uring_rqe, event.udata);
                assert(self.is_evented(rqe));
                self.execute(rqe, undefined);
            }
        }
    }

    /// Returns true if the I/O request goes through kevent for completion
    fn is_evented(self: *IO_Uring, rqe: *io_uring_rqe) bool {
        return switch (rqe.sqe.operation) {
            .accept, .connect, .send, .recv => true,
            else => false,
        };
    }

    /// Expires and completes timers in `self.tq`.
    /// Returns the minimum amount of time to wait in nanoseconds for the next timeout to expire.
    fn next_timeout(self: *IO_Uring) ?u64 {
        // Avoid getting a timestamp if there's nothing to scan.
        if ((self.tq.pending orelse self.tq.min_complete) == null) {
            return null;
        }
        
        // Get the current timestamp and scan/expire tq.pending using it.
        const now = monotonic_timestamp();
        const pending_deadline = self.next_deadline(&self.tq.pending, now, null);
        
        const min_deadline = blk: {
            while (true) {
                // Scan and expire tq.min_complete using the timestamp.
                // Re-scan if a completion occured as that could chain-complete timeouts waiting with op.timeout.min_complete.
                var completed = false;
                const min_complete_deadline = self.next_deadline(&self.tq.min_complete, now, &completed);
                if (completed) continue;

                // Report the smallest deadline to wait for found between tq.pending and tq.min_complete
                const pending = pending_deadline orelse break :blk min_complete_deadline;
                const min_complete = min_complete_deadline orelse break :blk pending;
                break :blk std.math.min(pending, min_complete);
            }
        };

        const deadline = min_deadline orelse return null;
        assert(deadline > now);

        // Return the amount of time, relative to `now` until the deadline is reached
        const until_expires = deadline - now;
        return until_expires;
    }

    /// Scans a list/stack of timeout requests `tq` while trying to expire/complete them.
    /// Sets `completed` pointer to true when a completion happens and the pointer is non-null.
    /// Returns the soonest expiration absolute timestamp in relation to `now` if there is any.
    fn next_deadline(self: *IO_Uring, tq: *?*io_uring_rqe, now: u64, completed: ?*bool) ?u64 {
        var min_deadline: ?u64 = null;
        var prev: *?*io_uring_rqe = tq;
        var next = tq.*;

        while (true) {
            const rqe = next orelse return min_deadline;
            next = rqe.next;

            const deadline = self.complete_timeout(rqe, now) catch {
                if (completed) |c| c.* = true;
                prev.* = next;
                continue;
            };
            
            const current_deadline = min_deadline orelse std.math.maxInt(u64);
            min_deadline = std.math.min(deadline, current_deadline);
            prev = &rqe.next;
        }
    }

    /// Execute then free the request entry, generating a completion entry for its result
    fn execute(self: *IO_Uring, rqe: *io_uring_rqe, now: u64) void {
        self.complete(rqe, switch (rqe.sqe.operation) {
            .openat => |op| self.syscall(os.system.openat, .{
                op.fd,
                op.path,
                op.flags,
                op.mode,
            }),
            .close => |op| self.syscall(os.system.close, .{
                op.fd,
            }),
            .read => |op| self.syscall(os.system.pread, .{
                op.fd,
                op.buf,
                op.len,
                @bitCast(i64, op.offset),
            }),
            .write => |op| self.syscall(os.system.pwrite, .{
                op.fd,
                op.buf,
                op.len,
                @bitCast(i64, op.offset),
            }),
            .fsync => |op| blk: {
                // Try to use F_FULLFSYNC over fsync() when possible
                //
                // https://github.com/untitaker/python-atomicwrites/issues/6
                // https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fcntl.2.html
                // https://github.com/rust-lang/rust/blob/1195bea5a7b73e079fa14b37ac7e375fc77d368a/library/std/src/sys/unix/fs.rs#L787
                const result = self.syscall(os.system.fcntl, .{op.fd, os.F_FULLFSYNC, @as(usize, 0)});
                if (result >= 0) break :blk result;
                break :blk self.syscall(os.system.fsync, .{op.fd});
            },
            .accept => |op| self.syscall(os.system.accept, .{
                op.socket, 
                op.addr,
                op.addr_len,
            }),
            .connect => |op| self.syscall(os.system.connect, .{
                op.socket, 
                op.addr,
                op.addr_len,
            }),
            .send => |op| self.syscall(os.system.send, .{
                op.socket,
                op.buf,
                op.len,
                op.flags,
            }),
            .recv => |op| self.syscall(os.system.recv, .{
                op.socket,
                op.buf,
                op.len,
                @intCast(c_int, op.flags),
            }),
            .timeout => |*op| {
                // Ensure the timeout is in absolute form.
                if (!op.is_absolute) op.expires += now;
                op.is_absolute = true;

                // Check for expiry while we're here
                _ = self.complete_timeout(rqe, now) catch return;

                // If not expired, push the timeout to the respective queue
                const tq = if (op.min_complete > 0) &self.tq.min_complete else &self.tq.pending;
                rqe.next = tq.*;
                tq.* = rqe;
                return;
            },
        });
    }

    /// Perform a system call, returning the result in a similar format to linux.io_uring_cqe.res
    fn syscall(self: *IO_Uring, comptime func: anytype, args: anytype) i32 {
        const rc = @call(.{}, func, args);
        return switch (os.errno(rc)) {
            0 => @intCast(i32, rc),
            else => |e| -e,
        };
    }

    /// Tries to complete the timeout request in `rqe` using the `now` monotonic timestamp.
    /// If the timeout request is completed, `error.Completed` is returned.
    /// If not, it returns the amount of time in nanoseconds to wait until the timeout request will expire.
    fn complete_timeout(self: *IO_Uring, rqe: *io_uring_rqe, now: u64) error{Completed}!u64 {
        const op = rqe.sqe.operation.timeout;
        assert(op.is_absolute);

        if (op.min_complete > 0 and self.cq_ready() >= op.min_complete) {
            self.complete(rqe, 0);
            return error.Completed;
        }

        if (now >= op.expires) {
            self.complete(rqe, -os.ETIME);
            return error.Completed;
        }

        const until_expires = op.expires - now;
        return until_expires;
    }

    /// Push an completion entry using the request entry + result, then free/reclaim the request entry. 
    fn complete(self: *IO_Uring, rqe: *io_uring_rqe, result: i32) void {
        assert(self.rq.inflight > 0);
        self.rq.inflight -= 1;

        rqe.next = self.rq.list;
        self.rq.list = rqe;

        const capacity = self.cq.array.len;
        assert(self.cq_ready() < capacity);

        assert(std.math.isPowerOfTwo(capacity));
        const cqe = &self.cq.array[self.cq.tail & (capacity - 1)];
        self.cq.tail +%= 1;

        cqe.* = .{
            .user_data = rqe.sqe.user_data,
            .res = result,
        };
    }
};

pub fn io_uring_prep_openat(
    sqe: *io_uring_sqe,
    fd: os.fd_t,
    path: [*:0]const u8,
    flags: u32,
    mode: os.mode_t,
) void {
    sqe.operation = .{
        .openat = .{
            .fd = fd,
            .path = path,
            .flags = flags,
            .mode = mode,
        },
    };
}

pub fn io_uring_prep_close(sqe: *io_uring_sqe, fd: os.fd_t) void {
    sqe.operation = .{
        .close = .{
            .fd = fd,
        },
    };
}

pub fn io_uring_prep_read(sqe: *io_uring_sqe, fd: os.fd_t, buffer: []u8, offset: u64) void {
    sqe.operation = .{
        .read = .{
            .fd = fd,
            .buf = buffer.ptr,
            .len = @intCast(u32, buffer.len),
            .offset = offset,
        },
    };
}

pub fn io_uring_prep_write(sqe: *io_uring_sqe, fd: os.fd_t, buffer: []const u8, offset: u64) void {
    sqe.operation = .{
        .write = .{
            .fd = fd,
            .buf = buffer.ptr,
            .len = @intCast(u32, buffer.len),
            .offset = offset,
        },
    };
}

pub fn io_uring_prep_fsync(sqe: *io_uring_sqe, fd: os.fd_t, flags: u32) void {
    sqe.operation = .{
        .fsync = .{
            .fd = fd,
            .flags = flags,
        },
    };
}

pub fn io_uring_prep_accept(
    sqe: *io_uring_sqe,
    fd: os.fd_t,
    addr: *os.sockaddr,
    addrlen: *os.socklen_t,
    flags: u32,
) void {
    sqe.operation = .{
        .accept = .{
            .socket = fd,
            .addr = addr,
            .addr_len = addrlen,
        },
    };
}

pub fn io_uring_prep_connect(
    sqe: *io_uring_sqe,
    fd: os.fd_t,
    addr: *const os.sockaddr,
    addrlen: os.socklen_t,
) void {
    sqe.operation = .{
        .connect = .{
            .socket = fd,
            .addr = addr,
            .addr_len = addrlen,
        },
    };
}

pub fn io_uring_prep_send(sqe: *io_uring_sqe, fd: os.fd_t, buffer: []const u8, flags: u32) void {
    sqe.operation = .{
        .send = .{
            .socket = fd,
            .buf = buffer.ptr,
            .len = @intCast(u32, buffer.len),
            .flags = flags,
        },
    };
}

pub fn io_uring_prep_recv(sqe: *io_uring_sqe, fd: os.fd_t, buffer: []u8, flags: u32) void {
    sqe.operation = .{
        .recv = .{
            .socket = fd,
            .buf = buffer.ptr,
            .len = @intCast(u32, buffer.len),
            .flags = flags,
        },
    };
}

pub fn io_uring_prep_timeout(
    sqe: *io_uring_sqe,
    ts: *const __kernel_timespec,
    count: u32,
    flags: u32,
) void {
    sqe.operation = .{
        .timeout = .{
            .expires = @intCast(u64, ts.tv_sec) * std.time.ns_per_s + @intCast(u64, ts.tv_nsec),
            .is_absolute = flags & IORING_TIMEOUT_ABS != 0,
            .min_complete = count,
        },
    };
}