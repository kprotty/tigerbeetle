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

pub const tb_state_t = enum(c_int) {
    running = 0,
    waiting = 1,
    notified = 2,
};

pub const tb_packet_t = extern struct {
    next: ?*tb_packet_t,
    user_data: usize,
    data: extern union {
        request: extern union {
            create_account: Account,
            lookup_account: u128,
            create_transfer: Transfer,
            commit_transfer: Commit,
        },
        response: extern union {
            create_account: EventResult,
            create_transfer: EventResult,
            lookup_account: Account,
            commit_transfer: EventResult,
        },
    },
};

pub const tb_queue_t = extern struct {
    state: Atomic(tb_state_t),
    ready: Atomic(?*tb_packet_t),
};

pub const tb_client_t = extern struct {
    sq: tb_queue_t,
    cq: tb_queue_t,
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
    // context passed into on_cq_ready
    on_cq_ready_ctx: usize,
    // called when completion queue state is notified after waiting
    on_cq_ready: fn (*tb_client_t, usize) callconv(.C) void,
) tb_create_result_t {
    // alloc max_packet requests + init Context + spawn context thread
}

pub export fn tb_client_destroy(client: *tb_client_t) void {
    // shutdown Context thread + dealloc it + dealloc requests
}

pub export fn tb_client_notify(client: *tb_client_t) void {
    // notify Context.io or something
}

/////////////////////////////////////////////////////////////////////////////

const Context = struct {
    client: tb_client_t,
};
