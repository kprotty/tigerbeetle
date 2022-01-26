const std = @import("std");
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;
    
const tb = @import("src/tigerbeetle.zig");

const StateMachine = @import("src/state_machine.zig").StateMachine;
const Operation = StateMachine.Operation;

const MessageBus = @import("src/message_bus.zig").MessageBusClient;
const IO = @import("src/io.zig").IO;

const vsr = @import("src/vsr.zig");
const Header = vsr.Header;
const Client = vsr.Client(StateMachine, MessageBus);

const config = @import("src/config.zig");
const log = std.log.scoped(.tg_client);


/// Creates a union type from Operation using the given layout and field generator.
fn OperationUnion(
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

/// Converts an instance of OperationUnion(A, X) to OperationUnion(B, X)
/// where Union = A and @TypeOf(value) = B.
/// This is used for transforming c-style unions with a separate tag into zig-style unions.
fn cast_operation_union(
    comptime Union: type, 
    operation: Operation,
    value: anytype,
) T {
    const Value = @TypeOf(value);
    const op_fields = std.meta.fields(Operation);
    comptime var mapping: [op_fields.len]fn(Value) T = undefined;

    inline for (op_fields) |op_field| {
        const op_name = op_field.name;
        const op = @field(Operation, op_name);
        mapping[@enumToInt(op)] = struct {
            fn cast(v: Value) T {
                return @unionInit(T, op_name, @field(v, op_name));
            }
        }.cast;
    }

    return mapping[@enumToInt(operation)](value);
}

/// Represents the atomic event state of a client queue.
const tb_event_state_t = enum(c_int) {
    /// This side of the queue is running and will eventually process completions.
    running = 0,
    /// This side of the queue is asleep as should be `notified` to start processing completions.
    waiting = 1,
    /// This side of the queue was (or will be) notified and should transition to `running`. 
    notified = 2,
};

/// Each side of the tb_client queues has an associated event.
/// The client or the internal thread updates their event state 
/// to coordinate when to issue notifications to the other side.
const tb_event_t = extern struct {
    state: Atomic(tb_event_state_t) = Atomic(tb_event_state_t).init(.running),

    fn reset(self: *tb_event_t) void {
        self.state.store(.running, .Monotonic);
    }

    fn try_wait(self: *tb_event_t) bool {
        const state = self.state.compareAndSwap(
            .running,
            .waiting,
            .Acquire,
            .Acquire,
        ) orelse return true;
        assert(state == .notified);
        return false;
    }

    fn try_wake(self: *AtomicEvent) bool {
        const state = self.state.swap(.notified, .Release);
        return state == .waiting;
    }
};

/// The kind of request or response encoded in a tb_completion
const tb_operation_t = Operation;

/// C union of the tigerbeetle state machine operations
const tb_request_t = OperationUnion(.Extern, StateMachine.Event);

/// C union of the tigerbeetle state machine operation results
const tb_response_t = OperationUnion(.Extern, StateMachine.Result);

/// A completion represents a node which encodes an asynchronous state machine transaction.
///
/// Completions have their operation and data.request filled in by the client for submission.
/// An optional user_data field can be attached as well to associate the request to external data.
///
/// Once processed by the internal client thread, data.request is overwritten with a data.response.
/// The user_data and operation remain unchanged, allowing the client to interpret the completion.
const tb_completion_t = extern struct {
    next: ?*tb_completion_t,
    user_data: usize,
    operation: tb_operation_t,
    data: extern union {
        request: tb_request_t,
        response: tb_response_t,
    },
};

/// Each side of the client queues has a stack of completions that are pushed to be accessed.
/// These stacks are SPSC where the producer pushes a batch and the consumer pops all at once.
const tb_stack_t = extern struct {
    top: Atomic(?*tb_completion_t) = Atomic(?*tb_completion_t).init(null),

    fn push(
        self: *tb_stack_t, 
        first: *tb_completion_t, 
        last: *tb_completion_t,
    ) void {
        var top = self.top.load(.Monotonic);
        while (true) {
            last.next = top;

            // Optimization since we know we're the only producer
            if (top == null) {
                self.top.store(first, .Release);
                break;
            }

            top = self.top.tryCompareAndSwap(
                top,
                first,
                .Release,
                .Monotonic,
            ) orelse break;
        }
    }

    fn pop_all(self: *tb_stack_t) ?*tb_completion_t {
        // Optimization to avoid the swap below
        if (self.top.load(.Monotonic) == null) {
            return null;
        }
        
        return self.top.swap(null, .Acquire);
    }
};

/// A queue contains an event to synchronize notification and a stack of ready completions.
const tb_queue_t = extern struct {
    event: tb_event_t = .{},
    ready: tb_stack_t = .{},
};

/// A client contains two queues:
/// - the user pushes to the sq stack, notifies the sq event, and waits on the cq event.
/// - the client thread polls the cq stack, notifies the cq event, and waits on the sq event.
const tb_client_t = extern struct {
    sq: tb_queue_t = .{},
    cq: tb_queue_t = .{},
};

const tb_create_status_t = enum(c_int) {

};

/// Allocate and start a tb_client thread which uses the cluster_id + addresses for connection.
/// Takes in a function + context which is invoked by the client thread when user cq is waiting.
///
/// On success, client_out points to the allocated client and
/// completions_out points to a stack/free_list of completions which can be used for submission
/// and reused after appearing in the cq.
export fn tq_client_create(
    client_out: **tb_client_t,
    completions_out: **tb_completion_t,
    cluster_id: u32,
    addresses_ptr: [*c]const u8,
    addresses_len: u32,
    cq_notify_context: usize,
    cq_notify: fn (*tb_client_t, usize) callconv(.C) void,
) tb_create_status_t {
    const client_thread = ClientThread.create(
        cluster_id,
        @ptrCast([*]const u8, addresses_ptr)[0..addresses_len],
        cq_notify_context,
        cq_notify,
    ) catch |err| switch (err) {

    };

    completions_out.* = &client_thread.completions[0];
    client_out.* = &client_thread.tb_client;
    return .success;
};

/// Closes the connection, kills the client thread, and free's any allocates made for the client.
/// Once called, the tb_client pointer and associated tb_completions become invalid to access.
export fn tb_client_destroy(client: *tb_client_t) void {
    const client_thread = @fieldParentPtr(ClientThread, "tb_client", client);
    client_thread.destroy();
}

/// Wakes up the client thread when its sleeping on the sq.
/// The sq.event state must have been waiting and it must be notified before this is called.
export fn tb_client_sq_notify(client: *tb_client_t) void {
    const client_thread = @fieldParentPtr(ClientThread, "tb_client", client);
    client_thread.sq_notify();
}

/////////////////////////////////////////////////////////////

const builtin = @import("builtin");
const allocator = if (builtin.link_libc)
    std.heap.c_allocator
else if (builtin.target.os.tag == .windows)
    (struct { var gpa = std.heap.HeapAllocator.init(); }).gpa.allocator()
else 
    @compileError("must link to libc when building tb_client");

const ClientThread = struct {
    io: IO,
    message_bus: MessageBus,
    client: Client,
    client_id: u128,
    cq_notify: fn (*tb_client_t, usize) callconv(.C) void,
    cq_notify_context: usize,
    tb_client: tb_client_t,
    is_running: Atomic(bool),
    thread: std.Thread,
    completions: [32]tb_completion_t, // TODO: dynamically allocated

    fn create(
        cluster_id: u32,
        addresses_raw: []const u8,
        cq_notify_context: usize,
        cq_notify: fn (*tb_client_t, usize) callconv(.C) void,
    ) !*ClientThread {
        const client_id = std.crypto.random.int(u128);
        log.debug("init: initializing client_id={}", .{client_id});

        const addresses = vsr.parse_addresses(allocator, addresses_raw) catch |err| {
            log.err("failed to parse addresses", .{});
            return err;
        };
        errdefer allocator.free(addresses);

        const self = allocator.create(ClientThread) catch |err| {
            log.err("failed to allocate context", .{});
            return err;
        };
        errdefer allocator.destroy(self);

        self.io = IO.init(32, 0) catch |err| {
            log.err("failed to initialize io", .{});
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
            log.err("failed to initialize message bus", .{});
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
            log.err("failed to initialize zig client", .{});
            return err;
        };
        errdefer self.client.deinit();

        self.client_id = client_id;
        self.message_bus.set_on_message(*Client, client, Client.on_message);

        self.on_cq_ready_ctx = on_cq_ready_ctx;
        self.on_cq_ready = on_cq_ready;
        self.tb_client = .{};

        self.is_running = Atomic(bool).init(true);
        self.thread = try std.Thread.spawn(.{}, ClientThread.run, .{self});
        errdefer self.shutdown_and_join();

        for (self.completions) |*completion, index| {
            const next_index = index + 1;
            completion.next = &self.completions[next_index % self.completions.len];
            if (next_index == self.completions.len) completion.next = null;
        }

        return self;
    }

    fn destroy(self: *ClientThread) void {
        self.shutdown_and_join();

        self.client.deinit();
        self.message_bus.deinit();
        self.io.deinit();
        
        self.* = undefined;
        allocator.destroy(self);
    }

    fn shutdown_and_join(self: *ClientThread) void {
        self.is_running.store(false, .Release);
        if (self.tb_client.sq.event.try_wake()) {
            self.sq_notify();
        }

        self.thread.join();
        self.thread = undefined;
    }

    fn sq_notify(self: *ClientThread) void {
        // TODO: IO.notify()
    }

    fn run(self: *ClientThread) void {
        // TODO: select(IO.wait, IO.timeout with backoff until 10ms)
        while (self.is_running.load(.Acquire)) {
            client.tick();
            io.run_for_ns(config.tick_ms * std.time.ns_per_ms)
        }
    }
};