const std = @import("std");
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;
    
const tb = @import("src/tigerbeetle.zig");
const Account = tb.Account;
const AccountFlags = tb.AccountFlags;
const Transfer = tb.Transfer;
const TransferFlags = tb.TransferFlags;
const Commit = tb.Commit;
const CommitFlags = tb.CommitFlags;
const CreateAccountsResult = tb.CreateAccountsResult;
const CreateTransfersResult = tb.CreateTransfersResult;
const CommitTransfersResult = tb.CommitTransfersResult;

const StateMachine = @import("src/state_machine.zig").StateMachine;
const Operation = StateMachine.Operation;

const MessageBus = @import("src/message_bus.zig").MessageBusClient;
const IO = @import("src/io.zig").IO;

const vsr = @import("src/vsr.zig");
const Header = vsr.Header;
const Client = vsr.Client(StateMachine, MessageBus);

const config = @import("src/config.zig");
const log = std.log.scoped(.tg_client);

pub const tb_request_t = tb_operation_union(.Extern, StateMachine.Event);
pub const tb_result_t = tb_operation_union(.Extern, StateMachine.Result);

fn tb_operation_union(
    comptime layout: std.builtin.TypeInfo.ContainerLayout,
    comptime FieldType: fn(Operation) type,
) type {
    comptime var fields: []const std.builtin.TypeInfo.UnionField = &.{};
    inline for (std.meta.fields(Operation)) |op_field| {
        const operation = @field(Operation, op_field.name);
        const field_type = FieldType(operation);
        fields = fields ++ std.builtin.TypeInfo.UnionField{
            .name = op_field.name,
            .field_type = field_type,
            .alignment = @alignOf(field_type),
        };
    }

    return @Type(std.builtin.TypeInfo{
        .Union = .{
            .layout = layout,
            .tag_type = Operation,
            .fields = fields,
            .decls = &.{},
        },
    });
}

pub const tb_state_t = enum(c_int) {
    running = 0,
    waiting = 1,
    notified = 2,
};

const tb_atomic_event_t = extern struct {
    state: Atomic(tb_state_t) = Atomic(tb_state_t).init(.running),

    // returns true if the caller should block on the state
    fn try_wait(self: *tb_atomic_event_t) bool {
        const state = self.state.compareAndSwap(
            .running,
            .waiting,
            .Acquire,
            .Acquire,
        ) orelse return true;
        assert(state == .notified);

        self.state.store(.running, .Unordered);
        return false;
    }

    // returns true if the caller should unblock the state's waiter
    fn try_wake(self: *tb_atomic_event_t) bool {
        const state = self.state.swap(.notified, .Release);
        if (state == .waiting) {
            self.state.store(.running, .Unordered);
            return true;
        }

        assert(state == .running or state == .notified);
        return false;
    }
};

pub const tb_packet_t = extern struct {
    next: ?*tb_packet_t,
    user_data: usize,
    operation: Operation,
    data: extern union {
        request: tb_request_t,
        result: tb_result_t,    
    },
};

const tb_atomic_stack_t = extern struct {
    top: Atomic(?*tb_packet_t) = Atomic(?*tb_packet_t).init(null),

    fn push(self: *tb_atomic_stack_t, first: *tb_packet_t, last: *tb_packet_t) void {
        var top = self.top.load(.Monotonic);
        while (true) {
            last.next = top;
            top = self.top.tryCompareAndSwap(
                top,
                first,
                .Release,
                .Monotonic,
            ) orelse break;
        }
    }

    fn pop_all(self: *tb_atomic_stack_t) ?*tb_packet_t {
        return self.top.swap(null, .Acquire);
    }
};

pub const tb_queue_t = extern struct {
    event: tb_atomic_event_t = .{},
    ready: tb_atomic_stack_t = .{},
};

pub const tb_client_t = extern struct {
    sq: tb_queue_t = .{},
    cq: tb_queue_t = .{},
};

pub const tb_create_result_t = enum(c_int) {

};

pub export fn tb_client_create(
    // OUT var to allocated client (which is field of Context)
    client: **tb_client_t,
    // OUT var to stack of allocated packets for submission/completion
    packets: **tb_packet_t,
    // maximum number of packets to allocate, OUT var with num actually allocated
    max_packets: *u32,
    // cluster id of the replica
    cluster_id: u32,
    // addresses of the replica to connect to
    addresses: [*c]const u8,
    // size of addresses bytes
    addresses_length: u32,
    // context passed into on_cq_ready
    on_cq_ready_ctx: usize,
    // called when completion queue state is notified after waiting
    on_cq_ready: fn (*tb_client_t, usize) callconv(.C) void,
) tb_create_result_t {
    const context = Context.init(
        max_packets.*,
        cluster_id,
        addresses[0..addresses_length],
        on_cq_ready_ctx,
        on_cq_ready,
    ) catch |err| switch (err) {

    };

    

    max_packets.* = context.num_packets;
    packets.* = &packet_slice[0];
    client.* = &context.client;
    return .success;
}

pub export fn tb_client_destroy(client: *tb_client_t) void {
    // shutdown Context thread + dealloc it + dealloc requests
}

pub export fn tb_client_notify(client: *tb_client_t) void {
    // notify Context.io or something
}

/////////////////////////////////////////////////////////////////////////////

pub const zig = struct {
    const FIFO = @import("src/fifo.zig").FIFO;

    pub const Ring = struct {
        client: *tb_client_t,
        free_list: ?*tb_packet_t,
        reset_event: *ResetEvent,
        submitted: FIFO(tb_packet_t) = .{},
        completed: FIFO(tb_packet_t) = .{},

        const ResetEvent = struct {
            is_set: Atomic(u32) = Atomic(u32).init(0),

            fn reset(self: *ResetEvent) void {
                self.is_set.store(0, .Monotonic);
            }

            fn wait(self: *ResetEvent) void {
                while (self.is_set.load(.Acquire) == 0)
                    std.Thread.Futex.wait(&self.is_set, 0, null) catch unreachable;
            }

            fn set(self: *ResetEvent) void {
                self.is_set.store(1, .Release);
                std.Thread.Futex.wake(&self.is_set, 1);
            }
        };

        pub const Request = tb_operation_union(.Auto, StateMachine.Event);
        pub const Result = tb_operation_union(.Auto, StateMachine.Result);

        fn cast_operation_union(comptime T: type, op: Operation, value: anytype) T {
            const Value = @TypeOf(value);
            const op_fields = std.meta.fields(Operation);
            comptime var mapping: [op_fields.len]fn(Value) T = undefined;

            inline for (op_fields) |op_field| {
                mapping[@enumToInt(@field(Operation, op_field.name))] = struct {
                    fn cast(v: Value) T {
                        return @unionInit(T, op_field.name, @field(v, op_name));
                    }
                }.cast;
            }

            return mapping[@enumToInt(op)](value);
        }

        pub fn init(num_entries: u32, cluster_id: u32, addresses: []const u8) !Ring {
            const reset_event = try allocator.create(ResetEvent);
            errdefer allocator.destroy(reset_event);

            var max_packets = num_entries;
            var client: *tb_client_t = undefined;
            var packets: ?*tb_packet_t = undefined;

            const rc = tb_client_create(
                &client,
                &packets,
                &max_packets,
                cluster_id,
                @ptrCast([*c]const u8, addresses.ptr),
                @intCast(u32, addresses.len),
                @ptrToInt(reset_event),
                struct {
                    fn on_wake(client: *tb_client_t, arg: usize) callconv(.C) void {
                        _ = client;
                        @intToPtr(*ResetEvet, arg).set();
                    }
                }.on_wake,
            );

            switch (rc) {

            }

            return Ring{
                .client = client,
                .free_list = packets,
                .reset_event = reset_event,
            };
        }

        pub fn deinit(self: *Ring) void {
            tb_client_destroy(self.client);
            allocator.destroy(self.reset_event);
            self.* = undefined;
        }

        pub const Submission = struct {
            user_data: usize,
            request: Request,
        };

        pub fn submit(self: *Ring, submission: Submission) !void {
            const packet = self.free_list orelse return error.MustSync;
            self.free_list = packet.next;
            
            packet.user_data = submission.user_data;
            packet.operation = std.meta.activeTag(submission.request);
            packet.data.request = cast_operation_union(tb_request_t, packet.operation, submission.request);

            packet.next = null;
            self.submitted.push(packet);
        }

        pub const Completion = struct {
            user_data: usize,
            result: Result,
        };

        pub fn poll(self: *Ring, completions: []Completion) !usize {
            var pushed: usize = 0;
            while (pushed < completions.length) : (pushed += 1) {
                var popped = self.completed.pop();
                if (popped == null) {
                    self.poll_completed();
                    popped = self.completed.pop();
                }

                const packet = popped orelse return error.MustSync;
                completions[pushed] = Completion{
                    .user_data = packet.user_data,
                    .result = cast_operation_union(Result, packet.operation, packet.data.result),
                };

                packet.next = self.free_list;
                self.free_list = packet;
            }
            return pushed;
        }

        fn poll_completed(self: *Ring) void {
            var completed = self.client.cq.ready.pop_all();
            while (completed) |packet| {
                completed = packet.next;
                packet.next = null;
                self.completed.push(packet);
            }
        }

        pub fn sync(self: *Ring, submit: bool, wait: bool) void {
            if (submit and self.submitted.peek() != null) {
                const first = self.submitted.in.?;
                const last = self.submitted.out.?;
                self.submitted = .{};

                self.client.sq.ready.push(first, last);
                if (self.client.sq.event.try_wake()) {
                    tb_client_notify(self.client);
                }
            }

            if (wait and self.completed.peek() == null) {
                self.poll_completed();
                if (self.completed.peek() != null) {
                    return;
                }

                self.futex.store(0, .Monotonic);
                if (self.client.cq.event.try_wait()) {
                    while (self.futex.load(.Acquire) == 0) {
                        std.Thread.Futex.wait(&self.futex, 0, null) catch unreachable;
                    }
                }

                self.poll_completed();
                assert(self.completed.peek() != null);
            }
        }
    };
};

/////////////////////////////////////////////////////////////////////////////

const Context = struct {
    tb_client: tb_client_t,
    is_running: Atomic(bool),
    on_cq_ready_ctx: usize,
    on_cq_ready: fn (*tb_client_t, usize) callconv(.C) void,
    num_packets: u32,
    io: IO,
    message_bus: MessageBus,
    client: Client,
    thread: std.Thread,

    fn init(
        max_packets: u32,
        cluster_id: u32,
        addresses_raw: []const u8,
        on_cq_ready_ctx: usize,
        on_cq_ready: fn (*tb_client_t, usize) callconv(.C) void,
    ) !*Context {
        const client_id = std.crypto.random.init(u128);
        log.debug("init: initializing client_id={}", .{client_id});

        const addresses = vsr.parse_addresses(allocator, addresses_raw) catch |err| {
            log.err("failed to parse addresses: {}", .{err});
            return err;
        };
        errdefer allocator.free(addresses);

        const num_packets = std.math.min(max_packets, 64 * 1024);
        const context_bytes = std.mem.alignForward(@sizeOf(Context), @alignOf(tb_packet_t));
        const bytes = context_bytes + (@sizeOf(tb_packet_t) * num_packets);

        const raw_bytes = allocator.alignedAlloc(u8, @alignOf(Context), bytes) catch |err| {
            log.err("failed to allocate context memory for tb_client: {}", .{err});
            return err;
        };
        errdefer allocator.free(raw_bytes);

        const self = @ptrCast(*Context, @alingCast(@alignOf(Context), raw_bytes.ptr));
        self.* = .{
            .tb_client = .{},
            .is_running = Atomic(bool).init(true),
            .on_cq_ready_ctx = on_cq_ready_ctx,
            .on_cq_ready = on_cq_ready,
            .num_packets = num_packets,
            .io = undefined,
            .client = undefined,
            .message_bus = undefined,
            .thread = undefined,
        };

        self.io = IO.init(32, 0) catch |err| {
            log.err("failed to init IO: {}", .{err});
            return err;
        };
        errdefer self.io.deinit();

        self.message_bus = MessageBus.init(
            allocator,
            cluster_id,
            addresses,
            client_id,
            &self.io,
        ) catch |err| {
            log.err("failed to init MessageBus: {}", .{err});
            return err;
        };
        errdefer self.message_bus.deinit();

        self.client = Client.init(
            allocator,
            client_id,
            cluster_id,
            @intCast(u8, addresses.len),
            &self.message_bus,
        ) catch |err| {
            log.err("failed to init vsr.Client: {}", .{err});
            return err;
        };
        errdefer self.client.deinit();

        self.message_bus.set_on_message(*Client, &self.client, Client.on_message);
        self.thread = try std.Thread.spawn(.{}, Context.run, .{self});

        const packets = context.get_packets();
        for (packets) |*packet, index| {
            const next_index = index + 1;
            packet.next = &packets[next_index];
            if (next_index == context.num_packets) packet.next = null;
        } 
    }

    fn deinit(self: *Context) void {
        self.is_running.store(false, .Monotonic);
        self.thread.join();

        self.client.deinit();
        self.message_bus.deinit();
        self.io.deinit();

        self.* = undefined;
        allocator.free(self.get_allocation());
    }

    fn get_packets(self: *Context) []tb_packet_t {
        const packet_offset = std.mem.alignForward(@sizeOf(Context), @alignOf(tb_packet_t));
        const packet_start = @ptrCast([*]u8, self)[packet_offset..].ptr;
        const packet_ptr = @ptrCast([*]tb_packet_t, @alignCast(@alignOf(tb_packet_t), packet_start));
        return packet_ptr[0..self.num_packets];
    }

    fn get_allocation(self: *Context) []align(@alignOf(Context)) u8 {
        const packets = self.get_packets();
        const begin = @ptrToInt(self);
        const end = @ptrToInt(&packets[packets.len - 1]);

        const bytes = end - begin;
        const ptr = @ptrCast([*]align(@alignOf(Context)) u8, self);
        return ptr[0..bytes];
    }
};
