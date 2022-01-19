const std = @import("std");
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;

const builtin = @import("builtin");
const allocator = if (builtin.link_libc)
    std.heap.c_allocator
else if (builtin.os.tag == .windows)
    (struct { var gpa = std.heap.HeapAllocator.init(); }).gpa.allocator()
else
    (struct { var gpa = std.heap.GeneralPurposeAllocator(.{}){}; }).gpa.allocator();
    
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

pub const Context = struct {
    io: IO,
    client: Client,
    client_id: u128,
    message_bus: MessageBus,
    thread: std.Thread,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        client_id: u128,
        addresses: []const u8,
    ) !*Context {
        const addrs = vsr.parsre_addresses(allocator, addresses) catch |err| {
            log.err("Failed to parse addresses ({})", .{err});
            return err;
        }
    }

    pub fn deinit(self: *Context) void {

    }

    fn run(self: *Context) void {
        self.run_context() catch |err| {
            log.err(
                "tigerbeetle client (id={}) shutdown unexpectedly with {}",
                .{ self.client_id, err },
            );
        };
    }

    fn run_context(self: *Context) !void {

    }
};

