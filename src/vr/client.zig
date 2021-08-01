const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("../config.zig");
const vr = @import("../vr.zig");
const Header = vr.Header;

const StateMachine = @import("../state_machine.zig").StateMachine;
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const Message = @import("../message_pool.zig").MessagePool.Message;

const log = std.log;

const at_least_one_second_in_ticks = std.math.max(1, @divFloor(std.time.ms_per_s, config.tick_ms));

pub fn Client(comptime MessageBus: type) type {
    return struct {
        const Self = @This();

        pub const Error = error{
            TooManyOutstandingRequests,
        };

        const Request = struct {
            const Callback = fn (
                user_data: u128,
                operation: StateMachine.Operation,
                results: Error![]const u8,
            ) void;
            user_data: u128,
            callback: Callback,
            operation: StateMachine.Operation,
            message: *Message,
        };

        allocator: *mem.Allocator,
        id: u128,
        cluster: u32,
        replica_count: u8,
        message_bus: *MessageBus,

        // TODO Track the latest view number received in .pong and .reply messages.
        request_number_min: u32 = 0,
        request_number_max: u32 = 0,

        /// Leave one Message free to receive with:
        request_queue: RingBuffer(Request, config.message_bus_messages_max - 1) = .{},
        request_timeout: vr.Timeout,

        ping_timeout: vr.Timeout,

        pub fn init(
            allocator: *mem.Allocator,
            cluster: u32,
            replica_count: u8,
            message_bus: *MessageBus,
        ) !Self {
            assert(replica_count > 0);

            var id = std.crypto.random.int(u128);
            // We require the client ID to be non-zero for client requests:
            // The probability of a CSPRNG returning zero is very unlikely (more likely a bug).
            assert(id > 0);

            var self = Self{
                .allocator = allocator,
                .id = id,
                .cluster = cluster,
                .replica_count = replica_count,
                .message_bus = message_bus,
                // Start with a conservative timeout while we are still working out the RTT:
                .request_timeout = .{
                    .name = "request_timeout",
                    .replica = std.math.maxInt(u8),
                    .after = at_least_one_second_in_ticks,
                },
                .ping_timeout = .{
                    .name = "ping_timeout",
                    .replica = std.math.maxInt(u8),
                    .after = at_least_one_second_in_ticks,
                },
            };

            self.ping_timeout.start();

            return self;
        }

        pub fn deinit(self: *Self) void {}

        pub fn tick(self: *Self) void {
            self.message_bus.tick();

            self.request_timeout.tick();
            if (self.request_timeout.fired()) self.on_request_timeout();

            self.ping_timeout.tick();
            if (self.ping_timeout.fired()) self.on_ping_timeout();

            // TODO Resend the request to the leader when the request_timeout fires.
            // This covers for dropped packets, when the leader is still the leader.

            // TODO Resend the request to the next replica and so on each time the reply_timeout fires.
            // This anticipates the next view change, without the cost of broadcast against the cluster.

            // TODO Tick ping_timeout and send ping if necessary to all replicas.
            // We need to keep doing this until we discover our latest request_number.
            // Thereafter, we can extend our ping_timeout considerably.
            // The cluster can use this ping information to do LRU eviction from the client table when
            // it is overflowed by the number of unique client IDs.

            // TODO Resend the request to the leader when the request_timeout fires.
            // This covers for dropped packets, when the leader is still the leader.

            // TODO Resend the request to the next replica and so on each time the reply_timeout fires.
            // This anticipates the next view change, without the cost of broadcast against the cluster.
        }

        /// A client is allowed at most one inflight request at a time at the protocol layer.
        /// We therefore queue any further concurrent requests by the application layer.
        pub fn request(
            self: *Self,
            user_data: u128,
            callback: Request.Callback,
            operation: StateMachine.Operation,
            message: *Message,
            body_size: usize,
        ) void {
            const message_size = @intCast(u32, @sizeOf(Header) + body_size);
            assert(message_size <= config.message_size_max);

            self.request_number_max += 1;
            log.debug("{} request: setting request={}", .{ self.id, self.request_number_max });
            message.header.* = .{
                .client = self.id,
                .cluster = self.cluster,
                .request = self.request_number_max,
                .command = .request,
                .operation = vr.Operation.from(StateMachine, operation),
                .size = message_size,
            };
            const body = message.buffer[@sizeOf(Header)..][0..body_size];
            message.header.set_checksum_body(body);
            message.header.set_checksum();

            const was_empty = self.request_queue.empty();
            self.request_queue.push(.{
                .user_data = user_data,
                .callback = callback,
                .operation = operation,
                .message = message.ref(),
            }) catch |err| switch (err) {
                error.NoSpaceLeft => {
                    callback(
                        user_data,
                        operation,
                        error.TooManyOutstandingRequests,
                    );
                    return;
                },
                else => unreachable,
            };

            // If the queue was empty, there is no currently inflight message, so send this one.
            if (was_empty) self.send_request(message);
        }

        /// Helper function to get an available message from the message bus.
        pub fn get_message(self: *Self) ?*Message {
            return self.message_bus.get_message();
        }

        /// Helper function to get the message bus to unref the message.
        pub fn unref(self: *Self, message: *Message) void {
            self.message_bus.unref(message);
        }

        fn on_request_timeout(self: *Self) void {
            const current_request = self.request_queue.peek_ptr() orelse return;

            log.debug("Retrying timed out request {o}.", .{current_request.message.header});
            self.request_timeout.stop();
            self.retry_request(current_request.message);
        }

        fn send(self: *Self, message: *Message, isRetry: bool) void {
            if (!isRetry) self.request_number_min += 1;
            log.debug("{} send: request_number_min={}", .{ self.id, self.request_number_min });
            assert(message.header.valid_checksum());
            assert(message.header.request == self.request_number_min);
            assert(message.header.client == self.id);
            assert(message.header.cluster == self.cluster);
            assert(!self.request_timeout.ticking);

            self.send_message_to_replicas(message);
            self.request_timeout.start();
        }

        fn send_request(self: *Self, message: *Message) void {
            self.send(message, false);
        }

        fn retry_request(self: *Self, message: *Message) void {
            self.send(message, true);
        }

        fn on_reply(self: *Self, reply: *Message) void {
            assert(reply.header.valid_checksum());
            assert(reply.header.valid_checksum_body(reply.body()));

            if (reply.header.client != self.id or reply.header.cluster != self.cluster) {
                log.debug("{} on_reply: Dropping unsolicited message.", .{self.id});
                return;
            }

            const queued_request = self.request_queue.peek_ptr().?;

            if (reply.header.request < queued_request.message.header.request) {
                log.debug(
                    "{} on_reply: Dropping duplicate message. request={}",
                    .{ self.id, reply.header.request },
                );
                return;
            }
            assert(reply.header.request == queued_request.message.header.request);
            assert(reply.header.operation.cast(StateMachine) == queued_request.operation);

            self.request_timeout.stop();
            queued_request.callback(
                queued_request.user_data,
                queued_request.operation,
                reply.body(),
            );
            _ = self.request_queue.pop().?;
            self.message_bus.unref(queued_request.message);

            if (self.request_queue.peek_ptr()) |next_request| {
                self.send_request(next_request.message);
            }
        }

        pub fn on_message(self: *Self, message: *Message) void {
            log.debug("{}: on_message: {}", .{ self.id, message.header });
            if (message.header.invalid()) |reason| {
                log.debug("{}: on_message: invalid ({s})", .{ self.id, reason });
                return;
            }
            if (message.header.cluster != self.cluster) {
                log.warn("{}: on_message: wrong cluster (message.header.cluster={} instead of {})", .{
                    self.id,
                    message.header.cluster,
                    self.cluster,
                });
                return;
            }
            switch (message.header.command) {
                .reply => self.on_reply(message),
                .ping => self.on_ping(message),
                .pong => {
                    // TODO: when we implement proper request number usage, we will
                    // need to get the request number from a pong message on startup.

                    // Set our ticks value when we ping the leader.
                    // Compare our ticks value with this value when we receive the pong back.
                    // Then adjust on_request_timeout to be twice this value.
                },
                else => {
                    log.warn(
                        "{}: on_message: unexpected command {}",
                        .{ self.id, message.header.command },
                    );
                },
            }
        }

        fn on_ping_timeout(self: *Self) void {
            self.ping_timeout.reset();

            const ping = Header{
                .command = .ping,
                .cluster = self.cluster,
                .client = self.id,
            };

            // TODO If we haven't received a pong from a replica since our last ping, then back off.
            self.send_header_to_replicas(ping);
        }

        fn on_ping(self: Self, ping: *const Message) void {
            const pong: Header = .{
                .command = .pong,
                .cluster = self.cluster,
                .client = self.id,
            };
            self.message_bus.send_header_to_replica(ping.header.replica, pong);
        }

        fn send_message_to_leader(self: *Self, message: *Message) void {
            // TODO For this to work, we need to send pings to the cluster every N ticks.
            // Otherwise, the latest leader will have our connection.peer set to .unknown.

            // TODO Use the latest view number modulo the configuration length to find the leader.
            // For now, replica 0 will forward onto the latest leader.
            self.message_bus.send_message_to_replica(0, message);
        }

        fn send_message_to_replicas(self: *Self, message: *Message) void {
            var replica: u8 = 0;
            while (replica < self.replica_count) : (replica += 1) {
                self.message_bus.send_message_to_replica(replica, message);
            }
        }

        fn send_header_to_replicas(self: *Self, header: Header) void {
            var replica: u8 = 0;
            while (replica < self.replica_count) : (replica += 1) {
                self.message_bus.send_header_to_replica(replica, header);
            }
        }
    };
}
