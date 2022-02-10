const std = @import("std");
const os = std.os;
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;

const config = @import("config.zig");
const log = std.log.scoped(.c_client);

const vsr = @import("vsr.zig");
const Header = vsr.Header;

const IO = @import("io.zig").IO;
const Message = @import("message_pool.zig").MessagePool.Message;

const builtin = @import("builtin");
const allocator = if (builtin.link_libc)
    std.heap.c_allocator
else if (builtin.target.os.tag == .windows)
    &(struct { var gpa = std.heap.HeapAllocator.init(); }).gpa.allocator // TODO: update for 0.9
else
    @compileError("tb_client must be built with libc");

// const MessageBus = @import("message_bus.zig").MessageBusClient;
// const StateMachine = @import("state_machine.zig").StateMachine;
// const Operation = StateMachine.Operation;

pub fn TbClient(comptime StateMachine: type, comptime MessageBus: type) type {
    return struct {
        const Client = vsr.Client(StateMachine, MessageBus);
        const Operation = StateMachine.Operation;

        const allowed_operations = [_]Operation{
            .create_accounts,
            .create_transfers,
            .commit_transfers,
            .lookup_accounts,
            .lookup_transfers,
        };

        fn operation_size_of(op: Operation) usize {
            inline for (allowed_operations) |operation| {
                if (op == operation) {
                    return @sizeOf(StateMachine.Event(operation));
                }
            }
            unreachable;
        }

        fn operation_name_of(comptime op: Operation) []const u8 {
            inline for (std.meta.fields(Operation)) |op_field| {
                if (op == @field(Operation, op_field.name)) {
                    return op_field.name;
                }
            }
            unreachable;
        }

        fn OperationUnion(comptime FieldType: anytype) type {
            comptime var fields: [allowed_operations.len]std.builtin.TypeInfo.UnionField = undefined; 
            inline for (fields) |*field, index| {
                const op = allowed_operations[index];
                field.name = operation_name_of(op);
                field.field_type = FieldType(op);
                field.alignment = @alignOf(field.field_type);
            }

            return @Type(std.builtin.TypeInfo{
                .Union = .{
                    .layout = .Extern,
                    .tag_type = null,
                    .fields = &fields,
                    .decls = &.{},
                },
            });
        }

        pub const tb_event_t = OperationUnion(StateMachine.Event);
        pub const tb_result_t = OperationUnion(StateMachine.Result);

        pub const tb_packet_t = extern struct {
            next: ?*tb_packet_t,
            user_data: usize,
            operation: Operation,
            data: extern union {
                request: tb_event_t,
                response: tb_result_t,
            },
        };

        pub const tb_packet_list_t = extern struct {
            head: ?*tb_packet_t = null,
            tail: ?*tb_packet_t = null,

            fn is_empty(self: tb_packet_list_t) bool {
                if (self.head == null) return true;
                assert(self.tail != null);
                return false;
            }

            fn push(self: *tb_packet_list_t, packet: *tb_packet_t) void {
                if (self.tail) |tail| tail.next = packet;
                if (self.head == null) self.head = packet;
                self.tail = packet;
                packet.next = null;
            }

            fn pop(self: *tb_packet_list_t) ?*tb_packet_list_t {
                const packet = self.head orelse return null;
                self.head = packet.next;
                if (self.head == null) self.tail = null;
                return packet;
            }
        };

        pub const tb_client_t = extern struct {
            client_id: u128,
        };

        pub const tb_status_t = enum(c_int) {
            success = 0,
            todo_error = 1,
        };

        pub export fn tb_client_init(
            out_client: **tb_client_t,
            out_packets: *tb_packet_list_t,
            cluster_id: u32,
            addresses_ptr: [*c]const u8,
            addresses_len: u32,
            num_packets: u32,
            on_completion_ctx: usize,
            on_completion_fn: fn (
                usize, 
                *tb_client_t, 
                *tb_packet_list_t,
            ) callconv(.C) void,
        ) tb_status_t {
            const addresses = @ptrCast([*]const u8, addresses_ptr)[0..addresses_len];
            const context = Context.init(
                cluster_id, 
                addresses, 
                num_packets,
                on_completion_ctx,
                on_completion_fn,
            ) catch |err| switch (err) {
                // TODO: handle more errors
                else => return .todo_error,
            };

            var list = tb_packet_list_t{};
            for (context.packets) |*packet| {
                list.push(packet);
            }
            
            out_client.* = &context.tb_client;
            out_packets.* = list;
            return .success;
        }

        pub export fn tb_client_deinit(
            client: *tb_client_t,
        ) void {
            const context = @fieldParentPtr(Context, "tb_client", client);
            context.deinit();
        }

        pub export fn tb_client_submit(
            client: *tb_client_t,
            packets: *tb_packet_list_t,
        ) void {
            const context = @fieldParentPtr(Context, "tb_client", client);
            context.submit(packets.*);
        }

        /////////////////////////////////////////////////////////////////////////

        const Context = struct {
            tb_client: tb_client_t,
            packets: []tb_packet_t,

            addresses: []std.net.Address,
            io: IO,
            message_bus: MessageBus,
            client: Client,

            ready: ?*tb_packet_t,
            submitted: Atomic(?*tb_packet_t),
            on_completion_ctx: usize,
            on_completion_fn: fn (
                usize, 
                *tb_client_t, 
                *tb_packet_list_t,
            ) callconv(.C) void,

            signal: Signal,
            thread: *std.Thread, // TODO: update for 0.9

            pub fn init(
                cluster_id: u32, 
                addresses: []const u8, 
                num_packets: u32,
                on_completion_ctx: usize,
                on_completion_fn: fn (
                    usize, 
                    *tb_client_t, 
                    *tb_packet_list_t,
                ) callconv(.C) void,
            ) !*Context {
                const client_id = std.crypto.random.int(u128);
                log.debug("init: initializing client_id={}.", .{client_id});
                
                log.debug("init: allocating Context.", .{});
                const self = allocator.create(Context) catch |err| {
                    log.err("failed to allocate context: {}.", .{err});
                    return err;
                };
                self.tb_client = .{ .client_id = client_id };
                errdefer allocator.destroy(self);

                log.debug("init: allocating tb_packets.", .{});
                self.packets = allocator.alloc(tb_packet_t, num_packets) catch |err| {
                    log.err("failed to allocate tb_packets: {}", .{err});
                    return err;
                };
                errdefer allocator.free(self.packets);

                log.debug("init: parsing vsr addresses.", .{});
                self.addresses = vsr.parse_addresses(allocator, addresses) catch |err| {
                    log.err("failed to parse addresses: {}.", .{err});
                    return err;
                };
                errdefer allocator.free(self.addresses);

                log.debug("init: initializing IO.", .{});
                self.io = IO.init(32, 0) catch |err| {
                    log.err("failed to initialize IO: {}.", .{err});
                    return err;
                }; 
                errdefer self.io.deinit();

                log.debug("init: initializing MessageBus.", .{});
                self.message_bus = MessageBus.init(
                    allocator,
                    cluster_id,
                    self.addresses,
                    client_id,
                    &self.io,
                ) catch |err| {
                    log.err("failed to initialize message bus: {}.", .{err});
                    return err;
                };
                errdefer self.message_bus.deinit();

                log.debug("init: Initializing client(cluster_id={d}, client_id={d}, addresses={o})", .{
                    cluster_id,
                    client_id,
                    self.addresses,
                });
                self.client = Client.init(
                    allocator,
                    client_id,
                    cluster_id,
                    @intCast(u8, self.addresses.len),
                    &self.message_bus,
                ) catch |err| {
                    log.err("failed to initalize client: {}", .{err});
                    return err;
                };
                errdefer self.client.deinit();

                self.ready = null;
                self.submitted = Atomic(?*tb_packet_t).init(null);
                self.on_completion_ctx = on_completion_ctx;
                self.on_completion_fn = on_completion_fn;

                log.debug("init: initializing Signal.", .{});
                self.signal.init(&self.io, Context.on_signal) catch |err| {
                    log.err("failed to initialize Signal: {}.", .{err});
                    return err;
                };
                errdefer self.signal.deinit();

                // TODO: update for 0.9
                log.debug("init: spawning Context thread.", .{});
                self.thread = std.Thread.spawn(Context.run, self) catch |err| {
                    log.err("failed to spawn context thread: {}.", .{err});
                    return err;
                };

                return self;
            }
            
            pub fn deinit(self: *Context) void {
                self.signal.shutdown();
                self.thread.wait(); // TODO: update for 0.9
                self.signal.deinit();

                self.client.deinit();
                self.message_bus.deinit();
                self.io.deinit();

                allocator.free(self.addresses);
                allocator.free(self.packets);

                self.* = undefined;
                allocator.destroy(self);
            }

            pub fn submit(self: *Context, list: tb_packet_list_t) void {
                const head = list.head orelse return;
                const tail = list.tail orelse unreachable;

                var submitted = self.submitted.load(.Monotonic);
                while (true) {
                    tail.next = submitted;
                    submitted = self.submitted.tryCompareAndSwap(
                        submitted,
                        head,
                        .Release,
                        .Monotonic,
                    ) orelse break;
                }

                self.signal.notify();
            }
            
            fn run(self: *Context) void {
                while (!self.signal.isShutdown()) {
                    self.client.tick();
                    self.io.run_for_ns(config.tick_ms * std.time.ns_per_ms) catch |err| {
                        log.err("IO.run() failed with {}", .{err});
                        return;
                    };
                }
            }

            fn on_signal(signal: *Signal) void {
                const self = @fieldParentPtr(Context, "signal", signal);
                self.client.tick();

                var batch = BatchProcessor(Context.on_complete){ .client = &self.client };
                defer batch.commit();

                while (true) {
                    if (self.ready == null) self.ready = self.submitted.swap(null, .Acquire);
                    if (self.ready == null) break;

                    while (self.ready) |packet| {
                        const next = packet.next;
                        batch.submit(packet) catch return; // TODO: single message
                        self.ready = next; 
                    }
                }
            }

            fn on_complete(client: *Client, packets: tb_packet_list_t, result: Client.Error!void) void {
                const self = @fieldParentPtr(@This(), "client", client);
                self.client.tick();

                _ = result catch |err| {
                    self.submit(packets);
                    return;
                };
                
                var completed = packets;
                (self.on_completion_fn)(
                    self.on_completion_ctx,
                    &self.tb_client,
                    &completed,
                );
            }
        };

        fn BatchProcessor(
            comptime on_complete_fn: fn(*Client, tb_packet_list_t, Client.Error!void) void,
        ) type {
            return struct {
                client: *Client,
                batches: [num_batches]Batch = [_]Batch{.{}} ** num_batches,

                const num_batches = std.meta.fields(Operation).len;
                const Self = @This();

                pub fn submit(self: *Self, packet: *tb_packet_t) !void {
                    const op = packet.operation;
                    const batch = &self.batches[@enumToInt(op)];
                    return batch.submit(self.client, op, packet);
                }

                pub fn commit(self: *Self) void {
                    for (self.batches) |*batch, index| {
                        const op = @intToEnum(Operation, @intCast(u8, index));
                        batch.commit(self.client, op);
                    }
                }

                const Batch = struct {
                    message: ?*Message = null,
                    queued: tb_packet_list_t = .{},
                    written: usize = 0,

                    fn submit(self: *Batch, client: *Client, op: Operation, packet: *tb_packet_t) !void {
                        while (true) {
                            if (self.message == null) self.message = client.get_message();
                            const message = self.message orelse return error.OutOfMessages;

                            assert(op == packet.operation);
                            const bytes = operation_size_of(op);
                            const request = @ptrCast([*]const u8, &packet.data.request)[0..bytes];

                            const writable = message.buffer[(@sizeOf(Header) + self.written)..];
                            assert(writable.len <= config.message_size_max);

                            if (request.len > writable.len) {
                                self.commit(client, op);
                                continue;
                            }

                            std.mem.copy(u8, writable, request);
                            self.written += request.len;

                            self.queued.push(packet);
                            return;
                        }
                    }

                    fn commit(self: *Batch, client: *Client, op: Operation) void {
                        const message = self.message orelse return;
                        self.message = null;

                        const queued = self.queued;
                        assert(!queued.is_empty());
                        self.queued = .{};

                        const written = self.written;
                        assert(written > 0);
                        self.written = 0;

                        client.request(
                            @bitCast(u128, UserData{
                                .client_ptr = @ptrToInt(client),
                                .queued_ptr = @ptrToInt(queued.head),
                            }),
                            Self.on_result,
                            op,
                            message,
                            written,
                        );
                    }
                };

                const UserData = extern struct {
                    client_ptr: usize,
                    queued_ptr: usize,
                };

                fn on_result(_user_data: u128, op: Operation, results: Client.Error![]const u8) void {
                    const user_data = @bitCast(UserData, _user_data);
                    const client = @intToPtr(*Client, user_data.client_ptr);
                    const packets = @intToPtr(*tb_packet_t, user_data.queued_ptr);

                    var completed = tb_packet_list_t{};
                    defer {
                        const err: Client.Error!void = if (results) |_| {} else |e| e;
                        on_complete_fn(client, completed, err);
                    }
                    
                    var responses = results catch null;
                    var processed: ?*tb_packet_t = packets;
                    while (processed) |packet| {
                        processed = packet.next;

                        if (responses) |readable| {
                            const bytes = operation_size_of(op);
                            const response = @ptrCast([*]u8, &packet.data.response)[0..bytes];
                            
                            std.mem.copy(u8, response, readable[0..bytes]);
                            responses = readable[bytes..];
                        }                
                    }
                }
            };
        }

        const Signal = struct {
            io: *IO,
            server_socket: os.socket_t,
            accept_socket: os.socket_t,
            connect_socket: os.socket_t,

            completion: IO.Completion,
            recv_buffer: [1]u8,
            send_buffer: [1]u8,
            
            on_signal_fn: fn (*Signal) void,
            state: Atomic(enum(u8) {
                running,
                waiting,
                notified,
                shutdown,
            }),

            pub fn init(self: *Signal, io: *IO, on_signal_fn: fn (*Signal) void) !void {
                self.io = io;
                self.server_socket = try os.socket(
                    os.AF_INET, 
                    os.SOCK_STREAM | os.SOCK_NONBLOCK, 
                    os.IPPROTO_TCP,
                );
                errdefer os.closeSocket(self.server_socket);

                try os.listen(self.server_socket, 1);
                var addr = std.net.Address.initIp4(undefined, undefined);
                var addr_len = addr.getOsSockLen();
                try os.getsockname(self.server_socket, &addr.any, &addr_len);

                self.connect_socket = try self.io.open_socket(os.AF_INET, os.SOCK_STREAM, os.IPPROTO_TCP);
                errdefer os.closeSocket(self.connect_socket);

                const DoConnect = struct {
                    result: IO.ConnectError!void = undefined,
                    completion: IO.Completion = undefined,
                    is_connected: bool = false,
                    
                    fn on_connect(
                        do_connect: *@This(),
                        completion: *IO.Completion,
                        result: IO.ConnectError!void,
                    ) void {
                        assert(!do_connect.is_connected);
                        do_connect.is_connected = true;
                        do_connect.result = result;
                    }
                };

                var do_connect = DoConnect{};
                self.io.connect(
                    *DoConnect,
                    &do_connect,
                    DoConnect.on_connect,
                    &do_connect.completion,
                    self.connect_socket,
                    addr,
                );
                
                self.accept_socket = IO.INVALID_SOCKET;
                while (!do_connect.is_connected) {
                    try self.io.tick();

                    if (self.accept_socket == IO.INVALID_SOCKET) {
                        self.accept_socket = os.accept(self.server_socket, null, null, 0) catch |err| {
                            if (err == error.WouldBlock) continue;
                            return err;
                        };
                    }
                }

                try do_connect.result;
                assert(self.accept_socket != IO.INVALID_SOCKET);
                assert(self.connect_socket != IO.INVALID_SOCKET);

                self.completion = undefined;
                self.recv_buffer = undefined;
                self.send_buffer = undefined;
                
                self.state = @TypeOf(self.state).init(.running);
                self.on_signal_fn = on_signal_fn;
                self.wait();
            }

            pub fn deinit(self: *Signal) void {
                os.closeSocket(self.server_socket);
                os.closeSocket(self.accept_socket);
                os.closeSocket(self.connect_socket);
            }

            pub fn notify(self: *Signal) void {
                if (self.state.swap(.notified, .Release) == .waiting) {
                    self.wake();
                }
            }

            pub fn shutdown(self: *Signal) void {
                if (self.state.swap(.shutdown, .Release) == .waiting) {
                    self.wake();
                }
            }

            pub fn isShutdown(self: *const Signal) bool {
                return self.state.load(.Acquire) == .shutdown;
            }

            fn wake(self: *Signal) void {
                assert(self.accept_socket != IO.INVALID_SOCKET);
                self.send_buffer[0] = 0;
                _ = os.send(self.accept_socket, &self.send_buffer, 0) catch unreachable;
            }

            fn wait(self: *Signal) void {
                const state = self.state.compareAndSwap(
                    .running,
                    .waiting,
                    .Acquire,
                    .Acquire,
                ) orelse return self.io.recv(
                    *Signal,
                    self,
                    on_recv,
                    &self.completion,
                    self.connect_socket,
                    &self.recv_buffer,
                );

                switch (state) {
                    .running => unreachable, // Not possible due to CAS semantics.
                    .waiting => unreachable, // We should be the only ones who could've started waiting.
                    .notified => {}, // A thread woke us up before we started waiting so reschedule below.
                    .shutdown => return, // A thread shut down the signal before we started waiting.
                }

                self.io.timeout(
                    *Signal,
                    self,
                    on_timeout,
                    &self.completion,
                    0, // zero-timeout functions as a yield
                );
            }

            fn on_recv(
                self: *Signal,
                completion: *IO.Completion,
                result: IO.RecvError!usize,
            ) void {
                assert(completion == &self.completion);
                _ = result catch |err| std.debug.panic("Signal recv error: {}", .{err});
                self.on_signal();
            }

            fn on_timeout(
                self: *Signal,
                completion: *IO.Completion,
                result: IO.TimeoutError!void,
            ) void {
                assert(completion == &self.completion);
                _ = result catch |err| std.debug.panic("Signal timeout error: {}", .{err});
                self.on_signal();
            }

            fn on_signal(self: *Signal) void {
                const state = self.state.compareAndSwap(
                    .notified,
                    .running,
                    .Acquire,
                    .Acquire,
                ) orelse {
                    (self.on_signal_fn)(self);
                    return self.wait();
                };

                switch (state) {
                    .running => unreachable, // Multiple racing calls to on_signal().
                    .waiting => unreachable, // on_signal() called without transitioning to a waking state.
                    .notified => unreachable, // Not possible due to CAS semantics.
                    .shutdown => return, // A thread shut down the signal before we started running.
                }
            }
        };
    };
}

