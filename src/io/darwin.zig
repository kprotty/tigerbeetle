const std = @import("std");
const os = std.os;
const mem = std.mem;
const assert = std.debug.assert;
const log = std.log.scoped(.io);

const config = @import("../config.zig");
const FIFO = @import("../fifo.zig").FIFO;
const Time = @import("../time.zig").Time;

const _io = @import("../io.zig");
const PollMode = _io.PollMode; 
const buffer_limit = _io.buffer_limit;

pub const IO = struct {
    pub usingnamespace _io.Extensions;

    kq: os.fd_t,
    time: Time = .{},
    io_inflight: usize = 0,
    timeouts: FIFO(Completion) = .{},
    completed: FIFO(Completion) = .{},
    io_pending: FIFO(Completion) = .{},

    pub fn init(entries: u12, flags: u32) !IO {
        const kq = try os.kqueue();
        assert(kq > -1);
        return IO{ .kq = kq };
    }

    pub fn deinit(self: *IO) void {
        assert(self.kq > -1);
        os.close(self.kq);
        self.kq = -1;
    }

    pub fn poll(self: *IO, mode: PollMode) !void {
        var io_pending = self.io_pending.peek();
        var events: [256]os.Kevent = undefined;

        // Check timeouts and fill events with completions in io_pending
        // (they will be submitted through kevent).
        // Timeouts are expired here and possibly pushed to the completed queue.
        const next_timeout = self.flush_timeouts();
        const change_events = self.flush_io(&events, &io_pending);

        // Only call kevent() if we need to submit io events or if we need to wait for completions.
        if (change_events > 0 or self.completed.peek() == null) {
            // Zero timeouts for kevent() implies a non-blocking poll
            var ts = std.mem.zeroes(os.timespec);

            // We need to wait (not poll) on kevent if there's nothing to submit or complete.
            // We should never wait indefinitely (timeout_ptr = null for kevent) 
            // as it's expected to sleep until some enqueued IO/timeout triggers.
            if (change_events == 0 and self.completed.peek() == null) {
                if (mode == .blocking) {
                    const timeout_ns = next_timeout orelse @panic("kevent() blocking forever");
                    ts.tv_nsec = @intCast(@TypeOf(ts.tv_nsec), timeout_ns % std.time.ns_per_s);
                    ts.tv_sec = @intCast(@TypeOf(ts.tv_sec), timeout_ns / std.time.ns_per_s);
                } else if (self.io_inflight == 0) {
                    return;
                }
            }

            const new_events = try os.kevent(
                self.kq,
                events[0..change_events],
                events[0..events.len],
                &ts,
            );

            // Mark the io events submitted only after kevent() successfully processed them
            self.io_pending.out = io_pending;
            if (io_pending == null) {
                self.io_pending.in = null;
            }

            self.io_inflight += change_events;
            self.io_inflight -= new_events;

            for (events[0..new_events]) |event| {
                const completion = @intToPtr(*Completion, event.udata);
                completion.next = null;
                self.completed.push(completion);
            }
        }

        var completed = self.completed;
        self.completed = .{};
        while (completed.pop()) |completion| {
            (completion.callback)(self, completion);
        }
    }

    fn flush_io(self: *IO, events: []os.Kevent, io_pending_top: *?*Completion) usize {
        for (events) |*event, flushed| {
            const completion = io_pending_top.* orelse return flushed;
            io_pending_top.* = completion.next;

            const event_info = switch (completion.operation) {
                .accept => |op| [2]c_int{ op.socket, os.EVFILT_READ },
                .connect => |op| [2]c_int{ op.socket, os.EVFILT_WRITE },
                .read => |op| [2]c_int{ op.fd, os.EVFILT_READ },
                .write => |op| [2]c_int{ op.fd, os.EVFILT_WRITE },
                .recv => |op| [2]c_int{ op.socket, os.EVFILT_READ },
                .send => |op| [2]c_int{ op.socket, os.EVFILT_WRITE },
                else => @panic("invalid completion operation queued for io"),
            };

            event.* = .{
                .ident = @intCast(u32, event_info[0]),
                .filter = @intCast(i16, event_info[1]),
                .flags = os.EV_ADD | os.EV_ENABLE | os.EV_ONESHOT,
                .fflags = 0,
                .data = 0,
                .udata = @ptrToInt(completion),
            };
        }
        return events.len;
    }

    fn flush_timeouts(self: *IO) ?u64 {
        var min_timeout: ?u64 = null;
        var timeouts: ?*Completion = self.timeouts.peek();
        while (timeouts) |completion| {
            timeouts = completion.next;

            // NOTE: We could cache `now` above the loop but monotonic() should be cheap to call.
            const now = self.time.monotonic();
            const expires = completion.operation.timeout.expires;

            // NOTE: remove() could be O(1) here with a doubly-linked-list
            // since we know the previous Completion.
            if (now >= expires) {
                self.timeouts.remove(completion);
                self.completed.push(completion);
                continue;
            }

            const timeout_ns = expires - now;
            if (min_timeout) |min_ns| {
                min_timeout = std.math.min(min_ns, timeout_ns);
            } else {
                min_timeout = timeout_ns;
            }
        }
        return min_timeout;
    }

    /// This struct holds the data needed for a single IO operation
    pub const Completion = struct {
        next: ?*Completion,
        context: ?*c_void,
        callback: fn (*IO, *Completion) void,
        operation: Operation,
    };

    const Operation = union(enum) {
        accept: struct {
            socket: os.socket_t,
        },
        close: struct {
            fd: os.fd_t,
        },
        connect: struct {
            socket: os.socket_t,
            address: std.net.Address,
            initiated: bool,
        },
        read: struct {
            fd: os.fd_t,
            buf: [*]u8,
            len: u32,
            offset: u64,
        },
        recv: struct {
            socket: os.socket_t,
            buf: [*]u8,
            len: u32,
        },
        send: struct {
            socket: os.socket_t,
            buf: [*]const u8,
            len: u32,
        },
        timeout: struct {
            expires: u64,
        },
        write: struct {
            fd: os.fd_t,
            buf: [*]const u8,
            len: u32,
            offset: u64,
        },
    };

    fn submit(
        self: *IO,
        context: anytype,
        comptime callback: anytype,
        completion: *Completion,
        comptime operation_tag: std.meta.Tag(Operation),
        operation_data: anytype,
        comptime OperationImpl: type,
    ) void {
        const Context = @TypeOf(context);
        const onCompleteFn = struct {
            fn onComplete(io: *IO, _completion: *Completion) void {
                // Perform the actual operaton
                const op_data = &@field(_completion.operation, @tagName(operation_tag));
                const result = OperationImpl.do_operation(op_data);

                // Requeue onto io_pending if error.WouldBlock
                switch (operation_tag) {
                    .accept, .connect, .read, .write, .send, .recv => {
                        _ = result catch |err| switch (err) {
                            error.WouldBlock => {
                                _completion.next = null;
                                io.io_pending.push(_completion);
                                return;
                            },
                            else => {},
                        };
                    },
                    else => {},
                }

                // Complete the Completion
                return callback(
                    @intToPtr(Context, @ptrToInt(_completion.context)),
                    _completion,
                    result,
                );
            }
        }.onComplete;

        completion.* = .{
            .next = null,
            .context = context,
            .callback = onCompleteFn,
            .operation = @unionInit(Operation, @tagName(operation_tag), operation_data),
        };

        switch (operation_tag) {
            .timeout => self.timeouts.push(completion),
            else => self.completed.push(completion),
        }
    }

    pub const AcceptError = os.AcceptError || os.SetSockOptError;

    pub fn accept(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: AcceptError!os.socket_t,
        ) void,
        completion: *Completion,
        socket: os.socket_t,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .accept,
            .{
                .socket = socket,
            },
            struct {
                fn do_operation(op: anytype) AcceptError!os.socket_t {
                    const fd = try os.accept(
                        op.socket,
                        null,
                        null,
                        os.SOCK_NONBLOCK | os.SOCK_CLOEXEC,
                    );
                    errdefer os.close(fd);

                    // Darwin doesn't support os.MSG_NOSIGNAL to avoid getting SIGPIPE on socket send().
                    // Instead, it uses the SO_NOSIGPIPE socket option which does the same for all send()s.
                    os.setsockopt(
                        fd,
                        os.SOL_SOCKET,
                        os.SO_NOSIGPIPE,
                        &mem.toBytes(@as(c_int, 1)),
                    ) catch |err| return switch (err) {
                        error.TimeoutTooBig => unreachable,
                        error.PermissionDenied => error.NetworkSubsystemFailed,
                        error.AlreadyConnected => error.NetworkSubsystemFailed,
                        error.InvalidProtocolOption => error.ProtocolFailure,
                        else => |e| e,
                    };

                    return fd;
                }
            },
        );
    }

    pub const CloseError = error{
        FileDescriptorInvalid,
        DiskQuota,
        InputOutput,
        NoSpaceLeft,
    } || os.UnexpectedError;

    pub fn close(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: CloseError!void,
        ) void,
        completion: *Completion,
        fd: os.fd_t,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .close,
            .{
                .fd = fd,
            },
            struct {
                fn do_operation(op: anytype) CloseError!void {
                    return switch (os.errno(os.system.close(op.fd))) {
                        0 => {},
                        os.EBADF => error.FileDescriptorInvalid,
                        os.EINTR => {}, // A success, see https://github.com/ziglang/zig/issues/2425
                        os.EIO => error.InputOutput,
                        else => |errno| os.unexpectedErrno(errno),
                    };
                }
            },
        );
    }

    pub const ConnectError = os.ConnectError;

    pub fn connect(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: ConnectError!void,
        ) void,
        completion: *Completion,
        socket: os.socket_t,
        address: std.net.Address,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .connect,
            .{
                .socket = socket,
                .address = address,
                .initiated = false,
            },
            struct {
                fn do_operation(op: anytype) ConnectError!void {
                    // Don't call connect after being rescheduled by io_pending as it gives EISCONN.
                    // Instead, check the socket error to see if has been connected successfully.
                    const result = switch (op.initiated) {
                        true => os.getsockoptError(op.socket),
                        else => os.connect(op.socket, &op.address.any, op.address.getOsSockLen()),
                    };

                    op.initiated = true;
                    return result;
                }
            },
        );
    }

    pub const ReadError = error{
        WouldBlock,
        NotOpenForReading,
        ConnectionResetByPeer,
        Alignment,
        InputOutput,
        IsDir,
        SystemResources,
        Unseekable,
    } || os.UnexpectedError;

    pub fn read(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: ReadError!usize,
        ) void,
        completion: *Completion,
        fd: os.fd_t,
        buffer: []u8,
        offset: u64,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .read,
            .{
                .fd = fd,
                .buf = buffer.ptr,
                .len = @intCast(u32, buffer_limit(buffer.len)),
                .offset = offset,
            },
            struct {
                fn do_operation(op: anytype) ReadError!usize {
                    while (true) {
                        const rc = os.system.pread(
                            op.fd,
                            op.buf,
                            op.len,
                            @bitCast(isize, op.offset),
                        );
                        return switch (os.errno(rc)) {
                            0 => @intCast(usize, rc),
                            os.EINTR => continue,
                            os.EAGAIN => error.WouldBlock,
                            os.EBADF => error.NotOpenForReading,
                            os.ECONNRESET => error.ConnectionResetByPeer,
                            os.EFAULT => unreachable,
                            os.EINVAL => error.Alignment,
                            os.EIO => error.InputOutput,
                            os.EISDIR => error.IsDir,
                            os.ENOBUFS => error.SystemResources,
                            os.ENOMEM => error.SystemResources,
                            os.ENXIO => error.Unseekable,
                            os.EOVERFLOW => error.Unseekable,
                            os.ESPIPE => error.Unseekable,
                            else => |err| os.unexpectedErrno(err),
                        };
                    }
                }
            },
        );
    }

    pub const RecvError = os.RecvFromError;

    pub fn recv(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: RecvError!usize,
        ) void,
        completion: *Completion,
        socket: os.socket_t,
        buffer: []u8,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .recv,
            .{
                .socket = socket,
                .buf = buffer.ptr,
                .len = @intCast(u32, buffer_limit(buffer.len)),
            },
            struct {
                fn do_operation(op: anytype) RecvError!usize {
                    return os.recv(op.socket, op.buf[0..op.len], 0);
                }
            },
        );
    }

    pub const SendError = os.SendError;

    pub fn send(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: SendError!usize,
        ) void,
        completion: *Completion,
        socket: os.socket_t,
        buffer: []const u8,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .send,
            .{
                .socket = socket,
                .buf = buffer.ptr,
                .len = @intCast(u32, buffer_limit(buffer.len)),
            },
            struct {
                fn do_operation(op: anytype) SendError!usize {
                    return os.send(op.socket, op.buf[0..op.len], 0);
                }
            },
        );
    }

    pub const TimeoutError = error{Canceled} || os.UnexpectedError;

    pub fn timeout(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: TimeoutError!void,
        ) void,
        completion: *Completion,
        nanoseconds: u63,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .timeout,
            .{
                .expires = self.time.monotonic() + nanoseconds,
            },
            struct {
                fn do_operation(_: anytype) TimeoutError!void {
                    return; // timeouts don't have errors for now
                }
            },
        );
    }

    pub const WriteError = os.PWriteError;

    pub fn write(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: WriteError!usize,
        ) void,
        completion: *Completion,
        fd: os.fd_t,
        buffer: []const u8,
        offset: u64,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .write,
            .{
                .fd = fd,
                .buf = buffer.ptr,
                .len = @intCast(u32, buffer_limit(buffer.len)),
                .offset = offset,
            },
            struct {
                fn do_operation(op: anytype) WriteError!usize {
                    return os.pwrite(op.fd, op.buf[0..op.len], op.offset);
                }
            },
        );
    }

    pub const INVALID_SOCKET = -1;

    pub fn open_socket(self: *IO, family: u32, sock_type: u32, protocol: u32) !os.socket_t {
        const fd = try os.socket(family, sock_type | os.SOCK_NONBLOCK, protocol);
        errdefer os.closeSocket(fd);

        // darwin doesn't support os.MSG_NOSIGNAL, but instead a socket option to avoid SIGPIPE.
        try os.setsockopt(fd, os.SOL_SOCKET, os.SO_NOSIGPIPE, &mem.toBytes(@as(c_int, 1)));
        return fd;
    }

    pub fn open_dir(dir_path: [:0]const u8) !os.fd_t {
        return os.openZ(dir_path, os.O_CLOEXEC | os.O_RDONLY, 0);
    }

    pub fn open_file(
        self: *IO,
        dir_fd: os.fd_t,
        relative_path: [:0]const u8,
        size: u64,
        must_create: bool,
    ) !os.fd_t {
        // TODO Use O_EXCL when opening as a block device to obtain a mandatory exclusive lock.
        // This is much stronger than an advisory exclusive lock, and is required on some platforms.

        var flags: u32 = os.O_CLOEXEC | os.O_RDWR | os.O_DSYNC;
        var mode: os.mode_t = 0;

        // TODO Document this and investigate whether this is in fact correct to set here.
        if (@hasDecl(os, "O_LARGEFILE")) flags |= os.O_LARGEFILE;

        if (must_create) {
            log.info("creating \"{s}\"...", .{relative_path});
            flags |= os.O_CREAT;
            flags |= os.O_EXCL;
            mode = 0o666;
        } else {
            log.info("opening \"{s}\"...", .{relative_path});
        }

        // This is critical as we rely on O_DSYNC for fsync() whenever we write to the file:
        assert((flags & os.O_DSYNC) > 0);

        // Be careful with openat(2): "If pathname is absolute, then dirfd is ignored." (man page)
        assert(!std.fs.path.isAbsolute(relative_path));
        const fd = try os.openatZ(dir_fd, relative_path, flags, mode);
        // TODO Return a proper error message when the path exists or does not exist (init/start).
        errdefer os.close(fd);

        // TODO Check that the file is actually a file.

        // On darwin assume that direct_io is always supported.
        // Use F_NOCACHE to disable the page cache as O_DIRECT doesn't exit.
        if (config.direct_io) {
            _ = try os.fcntl(fd, os.F_NOCACHE, 1);
        }

        // Obtain an advisory exclusive lock that works only if all processes actually use flock().
        // LOCK_NB means that we want to fail the lock without waiting if another process has it.
        os.flock(fd, os.LOCK_EX | os.LOCK_NB) catch |err| switch (err) {
            error.WouldBlock => @panic("another process holds the data file lock"),
            else => return err,
        };

        // Ask the file system to allocate contiguous sectors for the file (if possible):
        // If the file system does not support `fallocate()`, then this could mean more seeks or a
        // panic if we run out of disk space (ENOSPC).
        if (must_create) try fs_allocate(fd, size);

        // The best fsync strategy is always to fsync before reading because this prevents us from
        // making decisions on data that was never durably written by a previously crashed process.
        // We therefore always fsync when we open the path, also to wait for any pending O_DSYNC.
        // Thanks to Alex Miller from FoundationDB for diving into our source and pointing this out.
        try fs_sync(fd);

        // We fsync the parent directory to ensure that the file inode is durably written.
        // The caller is responsible for the parent directory inode stored under the grandparent.
        // We always do this when opening because we don't know if this was done before crashing.
        try fs_sync(dir_fd);

        const stat = try os.fstat(fd);
        if (stat.size != size) @panic("data file inode size was truncated or corrupted");

        return fd;
    }

    /// Darwin's version of fsync() assuming O_DSYNC
    fn fs_sync(fd: os.fd_t) !void {
        _ = os.fcntl(fd, os.F_FULLFSYNC, 1) catch return os.fsync(fd);
    }

    /// Allocates a file contiguously using fallocate() if supported.
    /// Alternatively, writes to the last sector so that at least the file size is correct.
    fn fs_allocate(fd: os.fd_t, size: u64) !void {
        log.info("allocating {}...", .{std.fmt.fmtIntSizeBin(size)});

        // darwin doesn't have fallocate() but we can simulate it using fcntl()s
        //
        // https://stackoverflow.com/a/11497568
        // https://api.kde.org/frameworks/kcoreaddons/html/posix__fallocate__mac_8h_source.html
        // http://hg.mozilla.org/mozilla-central/file/3d846420a907/xpcom/glue/FileUtils.cpp#l61
        
        const F_ALLOCATECONTIG = 0x2; // allocate contiguous space
        const F_ALLOCATEALL = 0x4; // allocate all or nothing
        const F_PEOFPOSMODE = 3; // use relative offset from the seek pos mode
        const F_VOLPOSMODE = 4; // use the specified volume offset
        const fstore_t = extern struct {
            fst_flags: c_uint,
            fst_posmode: c_int,
            fst_offset: os.off_t,
            fst_length: os.off_t,
            fst_bytesalloc: os.off_t,
        };

        var store = fstore_t{
            .fst_flags = F_ALLOCATECONTIG | F_ALLOCATEALL,
            .fst_posmode = F_PEOFPOSMODE,
            .fst_offset = 0,
            .fst_length = @intCast(os.off_t, size),
            .fst_bytesalloc = 0,
        };

        // try to pre-allocate contiguous space and fall back to default non-continugous
        var res = os.system.fcntl(fd, os.F_PREALLOCATE, @ptrToInt(&store));
        if (os.errno(res) != 0) {
            store.fst_flags = F_ALLOCATEALL;
            res = os.system.fcntl(fd, os.F_PREALLOCATE, @ptrToInt(&store));
        }

        switch (os.errno(res)) {
            0 => {},
            os.EACCES => unreachable, // F_SETLK or F_SETSIZE of F_WRITEBOOTSTRAP
            os.EBADF => return error.FileDescriptorInvalid,
            os.EDEADLK => unreachable, // F_SETLKW
            os.EINTR => unreachable, // F_SETLKW
            os.EINVAL => return error.ArgumentsInvalid, // for F_PREALLOCATE (offset invalid)
            os.EMFILE => unreachable, // F_DUPFD or F_DUPED
            os.ENOLCK => unreachable, // F_SETLK or F_SETLKW
            os.EOVERFLOW => return error.FileTooBig,
            os.ESRCH => unreachable, // F_SETOWN
            else => |errno| return os.unexpectedErrno(errno),
        }

        // now actually perform the allocation
        return os.ftruncate(fd, size) catch |err| switch (err) {
            error.AccessDenied => error.PermissionDenied,
            else => |e| e,
        };
    }
};
