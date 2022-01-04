const std = @import("std");
const os = std.os;
const assert = std.debug.assert;
const log = std.log.scoped(.io);
const config = @import("../config.zig");

const FIFO = @import("../fifo.zig").FIFO;
const Time = @import("../time.zig").Time;
const buffer_limit = @import("../io.zig").buffer_limit;

pub const IO = struct {
    iocp: os.windows.HANDLE,
    timer: Time = .{},
    io_pending: usize = 0,
    timeouts: FIFO(Completion) = .{},
    completed: FIFO(Completion) = .{},

    pub fn init(entries: u12, flags: u32) !IO {
        _ = entries;
        _ = flags;

        _ = try os.windows.WSAStartup(2, 2);
        errdefer os.windows.WSACleanup() catch unreachable;

        const iocp = try os.windows.CreateIoCompletionPort(os.windows.INVALID_HANDLE_VALUE, null, 0, 0);
        errdefer os.windows.CloseHandle(iocp);

        return IO{
            .afd = afd,
            .iocp = iocp,
        };
    }

    pub fn deinit(self: *IO) void {
        assert(self.iocp != os.windows.INVALID_HANDLE_VALUE);
        os.windows.CloseHandle(self.iocp);
        self.iocp = os.windows.INVALID_HANDLE_VALUE;

        os.windows.WSACleanup() catch unreachable;
    }

    pub fn tick(self: *IO) !void {
        return self.flush(.non_blocking);
    }

    pub fn run_for_ns(self: *IO, nanoseconds: u63) !void {
        const Callback = struct {
            fn onTimeout(timed_out: *bool, completion: *Completion, result: TimeoutError!void) void {
                _ = result catch unreachable;
                _ = completion;
                timed_out.* = true;
            }
        };

        var timed_out = false;
        var completion: Completion = undefined;
        self.timeout(*bool, &timed_out, Callback.onTimeout, &completion, nanoseconds);

        while (!timed_out) {
            try self.flush(.blocking);
        }
    }

    const FlushMode = enum {
        blocking,
        non_blocking,
    };

    fn flush(self: *IO, mode: FlushMode) !void {
        if (self.completed.peek() == null) {
            // Compute how long to poll by flushing timeout completions.
            // NOTE: this may push to completed queue
            var timeout_ms: ?os.windows.DWORD = null;
            if (self.flush_timeouts()) |expires_ns| {
                // 0ns expires should have been completed not returned
                assert(expires_ns != 0); 
                // Round up sub-millisecond expire times to the next millisecond
                const expires_ms = (expires_ns + (std.time.ns_per_ms / 2)) / std.time.ns_per_ms;
                // Saturating cast to DWORD milliseconds
                const expires = std.math.cast(os.windows.DWORD, expires_ms) catch std.math.maxInt(os.windows.DWORD);
                // max DWORD is reserved for INFINITE so cap the cast at max - 1
                timeout_ms = if (expires == os.windows.INFINITE) expires - 1 else expires;
            }
            
            // Poll for IO iff theres IO pending and flush_timeouts() found no ready completions
            if (self.io_pending > 0 and self.completed.peek() == null) {
                // In blocking mode, we're always waiting at least until the timeout by run_for_ns.
                // In non-blocking mode, we shouldn't wait at all.
                const io_timeout = switch (mode) {
                    .blocking => timeout_ms orelse @panic("IO.flush blocking unbounded"),
                    .non_blocking => 0,
                };

                var events: [64]Afd.Event = undefined;
                const num_events = try self.afd.poll(self.iocp, &events, io_timeout);
                
                assert(self.io_pending >= num_events);
                self.io_pending -= num_events;

                for (events[0..num_events]) |event| {
                    const afd_overlapped = event.as_afd_overlapped() orelse unreachable;
                    const completion = @fieldParentPtr(Completion, "afd_overlapped", afd_overlapped);
                    completion.next = null;
                    self.completed.push(completion);
                }
            }
        }

        // Dequeue and invoke all the completions currently ready.
        // Must read all `completions` before invoking the callbacks
        // as the callbacks could potentially submit more completions. 
        var completed = self.completed;
        self.completed = .{};
        while (completed.pop()) |completion| {
            (completion.callback)(self, completion);
        }
    }

    fn flush_timeouts(self: *IO) ?u64 {
        var min_expires: ?u64 = null;
        var current_time: ?u64 = null;
        var timeouts: ?*Completion = self.timeouts.peek();

        // iterate through the timeouts, returning min_expires at the end
        while (timeouts) |completion| {
            timeouts = completion.next;

            // lazily get the current time
            const now = current_time orelse self.timer.monotonic();
            current_time = now;

            // move the completion to completed if it expired
            if (now >= completion.operation.timeout.deadline) {
                self.timeouts.remove(completion);
                self.completed.push(completion);
                continue;
            }

            // if it's still waiting, update min_timeout
            const expires = completion.operation.timeout.deadline - now;
            if (min_expires) |current_min_expires| {
                min_expires = std.math.min(expires, current_min_expires);
            } else {
                min_expires = expires;
            }
        }

        return min_expires;
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
            listen_socket: os.socket_t,
            client_socket: os.socket_t,
            overlapped: os.windows.OVERLAPPED,
            addr_buffer: [(@sizEOf(std.net.Address) + 16) * 2]u8 align(4),
        },
        close: struct {
            fd: os.fd_t,
        },
        connect: struct {
            socket: os.socket_t,
            address: std.net.Address,
            initiated: bool,
        },
        fsync: struct {
            fd: os.fd_t,
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
            deadline: u64,
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
        comptime op_tag: std.meta.Tag(Operation),
        op_data: anytype,
        comptime OperationImpl: type,
    ) void {
        const Context = @TypeOf(context);
        const Callback = struct {
            fn onComplete(io: *IO, _completion: *Completion) void {
                // Perform the operation and get the result
                const _op_data = &@field(_completion.operation, @tagName(op_tag));
                var result = OperationImpl.do_operation(io, _op_data);

                // For OVERLAPPED IO, error.WouldBlock assumes that it will be completed by IOCP.
                switch (op_data) {
                    .accept, .read, .recv, .connect, .write, .send => {
                        _ = result catch |err| switch (err) {
                            error.WouldBlock => return,
                            else => {},
                        };
                    },
                    else => {}
                }
                
                // The completion is finally ready to invoke the callback
                callback(
                    @intToPtr(Context, @ptrToInt(_completion.context)),
                    _completion,
                    result,
                );
            }
        };

        // Setup the completion with the callback wrapper above
        completion.* = .{
            .next = null,
            .context = @ptrCast(?*c_void, context),
            .callback = Callback.onComplete,
            .operation = @unionInit(Operation, @tagName(op_tag), op_data),
        };

        // Submit the completion onto the right queue
        switch (op_tag) {
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
                .listen_socket = socket,
                .client_socket = INVALID_SOCKET,
                .overlapped = undefined,
                .addr_buffer = undefined,
            },
            struct {
                fn do_operation(io: *IO, op: anytype) AcceptError!os.socket_t {
                    var flags: os.windows.DWORD = undefined;
                    var transferred: os.windows.DWORD = undefined;

                    const rc = switch (op.client_socket) {
                        INVALID_SOCKET => blk: {
                            op.client_socket = io.open_socket(
                                os.AF_INET, 
                                os.SOCK_STREAM, 
                                os.IPPROTO_TCP,
                            ) catch |err| switch (err) {
                                else => return err,
                            };
                            
                            var sync_bytes_read: os.windows.DWORD = undefined;
                            op.overlapped = std.mem.zeroes(@TypeOf(op.overlapped));

                            break :blk AcceptEx(
                                op.listen_socket,
                                op.client_socket,
                                &op.addr_buffer,
                                0,
                                @sizeOf(std.net.Address) + 16,
                                @sizeOf(std.net.Address) + 16,
                                &sync_bytes_read,
                                &op.overlapped,
                            );
                        },
                        else => os.windows.ws2_32.WSAGetOverlappedResult(
                            op.listen_socket,
                            &op.overlapped,
                            &transferred,
                            os.windows.FALSE, // dont wait
                            &flags,
                        ),
                    };

                    // return the socket if we succeed in accepting
                    if (rc != os.windows.FALSE) {
                        // enables getsockopt, setsockopt, getsockname, getpeername
                        _ = os.windows.ws2_32.setsockopt(
                            op.client_socket,
                            os.windows.ws2_32.SOL_SOCKET,
                            os.windows.ws2_32.SO_UPDATE_ACCEPT_CONTEXT,
                            null,
                            0,
                        );

                        return op.client_socket;
                    }

                    // destroy the client socket we made if we get a non EAGAIN error code
                    errdefer |err| switch (err) {
                        error.WouldBlock => {},
                        else => os.closeSocket(op.client_socket),
                    }

                    return switch (os.windows.ws2_32.WSAGetLastError()) {
                        .WSA_IO_INCOMPLETE => unreachable, // GetOverlappedResult() called on IOCP
                        .WSA_IO_PENDING, .WSAEWOULDBLOCK => error.WouldBlock,
                        .WSANOTINITIALISED => unreachable, // WSAStartup() was called
                        .WSAENETDOWN => unreachable, // WinSock error
                        .WSAENOTSOCK => error.FileDescriptorNotASocket,
                        .WSAEOPNOTSUPP => error.OperationNotSupported,
                        .WSA_INVALID_HANDLE => error.FileDescriptorInvalid,
                        .WSAEFAULT, WSA_INVALID_PARAMETER => unreachable, // params should be ok
                        .WSAECONNRESET => error.ConnectionAborted,
                        .WSAEMFILE => unreachable, // we create our own descriptor so its available
                        .WSAENOBUFS => error.SystemResources,
                        .WSAEINTR, .WSAEINPROGRESS => unreachable, // no blocking calls
                        else => os.
                    };
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
            .{ .fd = fd },
            struct {
                fn do_operation(io: *IO, op: anytype) CloseError!void {
                    // Check if the fd is a SOCKET by seeing if getsockopt() returns ENOTSOCK
                    // https://stackoverflow.com/a/50981652
                    const socket = @ptrCast(os.socket_t, op.fd);
                    getsockoptError(socket) catch |err| switch (err) {
                        error.FileDescriptorNotASocket => return os.windows.CloseHandle(op.fd),
                        else => {},
                    };
                    os.closeSocket(socket);
                }
            },
        );
    }

    pub const ConnectError = os.ConnectError || error{FileDescriptorNotASocket};

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
                fn do_operation(io: *IO, op: anytype) ConnectError!void {
                    @compileError("TODO: Use ConnectEx");

                    // Don't call connect after being rescheduled by io_pending as it gives EISCONN.
                    // Instead, check the socket error to see if has been connected successfully.
                    if (op.initiated)
                        return getsockoptError(op.socket);

                    op.initiated = true;
                    const rc = os.windows.ws2_32.connect(
                        op.socket, 
                        &op.address.any, 
                        @intCast(i32, op.address.getOsSockLen()),
                    );

                    // Need to hardcode as os.connect has unreachable on .WSAEWOULDBLOCK
                    if (rc == 0) return;                    
                    switch (os.windows.ws2_32.WSAGetLastError()) {
                        .WSAEADDRINUSE => return error.AddressInUse,
                        .WSAEADDRNOTAVAIL => return error.AddressNotAvailable,
                        .WSAECONNREFUSED => return error.ConnectionRefused,
                        .WSAECONNRESET => return error.ConnectionResetByPeer,
                        .WSAETIMEDOUT => return error.ConnectionTimedOut,
                        .WSAEHOSTUNREACH,
                        .WSAENETUNREACH,
                        => return error.NetworkUnreachable,
                        .WSAEFAULT => unreachable,
                        .WSAEINVAL => unreachable,
                        .WSAEISCONN => unreachable,
                        .WSAENOTSOCK => unreachable,
                        .WSAEWOULDBLOCK => return error.WouldBlock,
                        .WSAEACCES => unreachable,
                        .WSAENOBUFS => return error.SystemResources,
                        .WSAEAFNOSUPPORT => return error.AddressFamilyNotSupported,
                        else => |err| return os.windows.unexpectedWSAError(err),
                    }
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
                fn do_operation(io: *IO, op: anytype) ReadError!usize {
                    @compileError("TODO: use ReadFileEx");

                    return os.pread(op.fd, op.buf[0..op.len], op.offset) catch |err| switch (err) {
                        error.OperationAborted => unreachable,
                        error.BrokenPipe => unreachable,
                        error.ConnectionTimedOut => unreachable,
                        error.AccessDenied => error.InputOutput,
                        else => |e| e,
                    };
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
                fn do_operation(io: *IO, op: anytype) RecvError!usize {
                    @compileError("TODO: use WSARecv");

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
                fn do_operation(io: *IO, op: anytype) SendError!usize {
                    @compileError("TODO: use WSASend");

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
            .{ .deadline = self.timer.monotonic() + nanoseconds },
            struct {
                fn do_operation(io: *IO, op: anytype) TimeoutError!void {
                    _ = op;
                    return;
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
                fn do_operation(io: *IO, op: anytype) WriteError!usize {
                    @compileError("TODO: use WriteFileEx");

                    return os.pwrite(op.fd, op.buf[0..op.len], op.offset);
                }
            },
        );
    }

    pub const INVALID_SOCKET = os.windows.ws2_32.INVALID_SOCKET;

    pub fn open_socket(self: *IO, family: u32, sock_type: u32, protocol: u32) !os.socket_t {
        const flags = sock_type | os.SOCK_NONBLOCK | os.SOCK_CLOEXEC;
        const socket = try os.socket(family, flags, protocol);
        errdefer os.closeSocket(socket);

        const socket_iocp = try os.windows.CreateIoCompletionPort(socket, self.iocp, 0, 0);
        assert(socket_iocp == self.iocp);

        // Ensure that synchronous IO completion doesn't queue an unneeded overlapped
        // and that the event for the socket (WaitForSingleObject) doesn't need to be set.
        var flags: os.windows.DWORD = 0;
        flags |= os.windows.FILE_SKIP_COMPLETION_PORT_ON_SUCCESS;
        flags |= os.windows.FILE_SKIP_SET_EVENT_ON_HANDLE; 

        const handle = @ptrCast(os.windows.HANDLE, socket);
        try os.windows.SetFileCompletionNotificationModes(handle, flags);
    }

    pub fn open_dir(dir_path: [:0]const u8) !os.fd_t {
        const dir = try std.fs.cwd().openDirZ(dir_path, .{});
        return dir.fd;
    }

    pub fn open_file(
        self: *IO,
        dir_handle: os.fd_t,
        relative_path: [:0]const u8,
        size: u64,
        must_create: bool,
    ) !os.fd_t {
        const path_w = try os.windows.sliceToPrefixedFileW(relative_path);

        // FILE_CREATE = O_CREAT | O_EXCL
        var creation_disposition: os.windows.DWORD = 0;
        if (must_create) {
            log.info("creating \"{s}\"...", .{relative_path});
            creation_disposition = os.windows.FILE_CREATE;
        } else {
            log.info("opening \"{s}\"...", .{relative_path});
            creation_disposition = os.windows.OPEN_EXISTING;
        }

        // O_EXCL
        var shared_mode: os.windows.DWORD = 0;

        // O_RDWR
        var access_mask: os.windows.DWORD = 0;
        access_mask |= os.windows.GENERIC_READ;
        access_mask |= os.windows.GENERIC_WRITE;

        // O_DIRECT
        var attributes: os.windows.DWORD = 0;
        attributes |= os.windows.FILE_FLAG_NO_BUFFERING;
        attributes |= os.windows.FILE_FLAG_WRITE_THROUGH;
        attributes |= os.windows.FILE_FLAG_OVERLAPPED;

        const handle = os.windows.kernel32.CreateFileW(
            path_w.span(),
            access_mask,
            shared_mode,
            null, // no security attributes required
            creation_disposition,
            attributes,
            null, // no existing template file
        );

        if (handle == os.windows.INVALID_HANDLE_VALUE) {
            return switch (os.windows.kernel32.GetLastError()) {
                .ACCESS_DENIED => error.AccessDenied,
                else => |err| os.windows.unexpectedError(err),
            };
        }

        errdefer os.windows.CloseHandle(handle);

        // Obtain an advisory exclusive lock
        // even when we haven't given shared access to other processes.
        fs_lock(handle, size) catch |err| switch (err) {
            error.WouldBlock => @panic("another process holds the data file lock"),
            else => return err,
        };

        // Ask the file system to allocate contiguous sectors for the file (if possible):
        if (must_create) {
            log.info("allocating {}...", .{std.fmt.fmtIntSizeBin(size)});
            fs_allocate(handle, size) catch {
                log.warn("file system failed to preallocate the file memory", .{});
                log.notice("allocating by writing to the last sector of the file instead...", .{});

                const sector_size = config.sector_size;
                const sector: [sector_size]u8 align(sector_size) = [_]u8{0} ** sector_size;

                // Handle partial writes where the physical sector is less than a logical sector:
                const write_offset = size - sector.len;
                var written: usize = 0;
                while (written < sector.len) {
                    written += try os.pwrite(handle, sector[written..], write_offset + written);
                }
            };
        }

        // The best fsync strategy is always to fsync before reading because this prevents us from
        // making decisions on data that was never durably written by a previously crashed process.
        // We therefore always fsync when we open the path, also to wait for any pending O_DSYNC.
        // Thanks to Alex Miller from FoundationDB for diving into our source and pointing this out.
        try os.fsync(handle);

        // Don't fsync the directory handle as it's not open with write access
        // try os.fsync(dir_handle);

        const file_size = try os.windows.GetFileSizeEx(handle);
        if (file_size != size) @panic("data file inode size was truncated or corrupted");

        return handle;
    }

    fn fs_lock(handle: os.fd_t, size: u64) !void {
        // TODO: Look into using SetFileIoOverlappedRange() for better unbuffered async IO perf
        // NOTE: Requires SeLockMemoryPrivilege.

        const kernel32 = struct {
            const LOCKFILE_EXCLUSIVE_LOCK = 0x2;
            const LOCKFILE_FAIL_IMMEDIATELY = 01;

            extern "kernel32" fn LockFileEx(
                hFile: os.windows.HANDLE,
                dwFlags: os.windows.DWORD,
                dwReserved: os.windows.DWORD,
                nNumberOfBytesToLockLow: os.windows.DWORD,
                nNumberOfBytesToLockHigh: os.windows.DWORD,
                lpOverlapped: ?*os.windows.OVERLAPPED,
            ) callconv(os.windows.WINAPI) os.windows.BOOL;
        };

        // hEvent = null 
        // Offset & OffsetHigh = 0
        var lock_overlapped = std.mem.zeroes(os.windows.OVERLAPPED);

        // LOCK_EX | LOCK_NB
        var lock_flags: os.windows.DWORD = 0;
        lock_flags |= kernel32.LOCKFILE_EXCLUSIVE_LOCK;
        lock_flags |= kernel32.LOCKFILE_FAIL_IMMEDIATELY;        

        const locked = kernel32.LockFileEx(
            handle,
            lock_flags,
            0, // reserved param is always zero
            @truncate(u32, size), // low bits of size
            @truncate(u32, size >> 32), // high bits of size
            &lock_overlapped,
        );

        if (locked == os.windows.FALSE) {
            return switch (os.windows.kernel32.GetLastError()) {
                .IO_PENDING => error.WouldBlock,
                else => |err| os.windows.unexpectedError(err),
            };
        }
    }

    fn fs_allocate(handle: os.fd_t, size: u64) !void {
        // TODO: Look into using SetFileValidData() instead
        // NOTE: Requires SE_MANAGE_VOLUME_NAME privilege

        // Move the file pointer to the start + size
        const seeked = os.windows.kernel32.SetFilePointerEx(
            handle, 
            @intCast(i64, size), 
            null, // no reference to new file pointer
            os.windows.FILE_BEGIN,
        );
        
        if (seeked == os.windows.FALSE) {
            return switch (os.windows.kernel32.GetLastError()) {
                .INVALID_HANDLE => unreachable,
                .INVALID_PARAMETER => unreachable,
                else => |err| os.windows.unexpectedError(err),
            };
        }

        // Mark the moved file pointer (start + size) as the physical EOF.
        const allocated = os.windows.kernel32.SetEndOfFile(handle);
        if (allocated == os.windows.FALSE) {
            const err = os.windows.kernel32.GetLastError();
            return os.windows.unexpectedError(err);
        }
    }
};

/// Introduced in Windows 8.1+ (min supported by zig stdlib)
pub extern "mswsock" fn AcceptEx(
    sListenSocket: SOCKET,
    sAcceptSocket: SOCKET,
    lpOutputBuffer: *c_void,
    dwReceiveDataLength: u32,
    dwLocalAddressLength: u32,
    dwRemoteAddressLength: u32,
    lpdwBytesReceived: *u32,
    lpOverlapped: *OVERLAPPED,
) callconv(WINAPI) BOOL;

// TODO: use os.getsockoptError when fixed in stdlib
fn getsockoptError(socket: os.socket_t) IO.ConnectError!void {
    var err_code: u32 = undefined;
    var size: i32 = @sizeOf(u32);
    const rc = os.windows.ws2_32.getsockopt(
        socket, 
        os.SOL_SOCKET,
        os.SO_ERROR, 
        std.mem.asBytes(&err_code), 
        &size,
    );
    
    if (rc != 0) {
        switch (os.windows.ws2_32.WSAGetLastError()) {
            .WSAENETDOWN => return error.NetworkUnreachable,
            .WSANOTINITIALISED => unreachable, // WSAStartup() was never called
            .WSAEFAULT => unreachable, // The address pointed to by optval or optlen is not in a valid part of the process address space.
            .WSAEINVAL => unreachable, // The level parameter is unknown or invalid
            .WSAENOPROTOOPT => unreachable, // The option is unknown at the level indicated.
            .WSAENOTSOCK => return error.FileDescriptorNotASocket,
            else => |err| return os.unexpectedErrno(@enumToInt(err)),
        }
    }
    
    assert(size == 4);
    if (err_code == 0)
        return;

    const ws_err = @intToEnum(os.windows.ws2_32.WinsockError, @intCast(u16, err_code));
    return switch (ws_err) {
        .WSAEACCES => error.PermissionDenied,
        .WSAEADDRINUSE => error.AddressInUse,
        .WSAEADDRNOTAVAIL => error.AddressNotAvailable,
        .WSAEAFNOSUPPORT => error.AddressFamilyNotSupported,
        .WSAEALREADY => error.ConnectionPending,
        .WSAEBADF => unreachable,
        .WSAECONNREFUSED => error.ConnectionRefused,
        .WSAEFAULT => unreachable,
        .WSAEISCONN => unreachable, // error.AlreadyConnected,
        .WSAENETUNREACH => error.NetworkUnreachable,
        .WSAENOTSOCK => error.FileDescriptorNotASocket,
        .WSAEPROTOTYPE => unreachable,
        .WSAETIMEDOUT => error.ConnectionTimedOut,
        .WSAECONNRESET => error.ConnectionResetByPeer,
        else => |e| blk: {
            std.debug.print("winsock error: {}", .{e});
            break :blk error.Unexpected;
        },
    };
}
