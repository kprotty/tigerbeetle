
const tb_event_state_t = enum(c_int) {
    running = 0,
    waiting = 1,
    notified = 2,
};

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

const tb_operation_t = Operation;
const tb_request_t = OperationUnion(.Extern, StateMachine.Event);
const tb_response_t = OperationUnion(.Extern, StateMachine.Result);

const tb_completion_t = extern struct {
    next: ?*tb_completion_t,
    user_data: usize,
    operation: tb_operation_t,
    data: extern union {
        request: tb_request_t,
        response: tb_response_t,
    },
};

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
            top = self.top.tryCompareAndSwap(
                top,
                first,
                .Release,
                .Monotonic,
            ) orelse break;
        }
    }

    fn pop_all(self: *tb_stack_t) ?*tb_completion_t {
        return self.top.swap(null, .Acquire);
    }
};

const tb_queue_t = extern struct {
    event: tb_event_t = .{},
    ready: tb_stack_t = .{},
};

const tb_client_t = extern struct {
    sq: tb_queue_t = .{},
    cq: tb_queue_t = .{},
};

const tb_create_status_t = enum(c_int) {

};

export fn tq_client_create(
    client_out: **tb_client_t,
    completions_out: **tb_completion_t,
    cluster_id: u32,
    addresses_ptr: [*c]const u8,
    addresses_len: u32,
    cq_notify_context: usize,
    cq_notify: fn (*tb_client_t, usize) callconv(.C) void,
) tb_create_status_t {

};

export fn tb_client_destroy(client: *tb_client_t) void {

}

export fn tb_client_sq_notify(client: *tb_client_t) void {

}

/////////////////////////////////////////////////////////////


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

const ClientThread = struct {
    tb_client: tb_client_t,
    io: IO,
    message_bus: MessageBus,
    client: Client,
    thread: std.Thread,
};