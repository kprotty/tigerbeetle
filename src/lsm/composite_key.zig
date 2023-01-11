const std = @import("std");
const assert = std.debug.assert;
const math = std.math;

pub fn CompositeKey(comptime Field: type) type {
    assert(Field == u128 or Field == u64);

    return extern struct {
        const Self = @This();

        pub const sentinel_key: Self = .{
            .field = math.maxInt(Field),
            .timestamp = math.maxInt(u64),
        };

        const tombstone_bit = 1 << 63;

        // If zeroed padding is needed after the timestamp field.
        const pad = Field == u128;

        pub const Value = extern struct {
            field: Field,
            /// The most significant bit indicates if the value is a tombstone.
            timestamp: u64,
            padding: (if (pad) u64 else [0]u8) = (if (pad) 0 else [_]u8{}),

            comptime {
                assert(@sizeOf(Value) == @sizeOf(Field) * 2);
                assert(@alignOf(Value) >= @alignOf(Field));
                assert(@sizeOf(Value) * 8 == @bitSizeOf(Value));
            }
        };

        field: Field,
        /// The most significant bit must be unset as it is used to indicate a tombstone.
        timestamp: u64,
        padding: (if (pad) u64 else [0]u8) = (if (pad) 0 else [_]u8{}),

        comptime {
            assert(@sizeOf(Self) == @sizeOf(Field) * 2);
            assert(@alignOf(Self) >= @alignOf(Field));
            assert(@sizeOf(Self) * 8 == @bitSizeOf(Self));
        }

        pub fn compare_keys(a: Self, b: Self) math.Order {
            if (a.field < b.field) {
                return .lt;
            } else if (a.field > b.field) {
                return .gt;
            } else if (a.timestamp < b.timestamp) {
                return .lt;
            } else if (a.timestamp > b.timestamp) {
                return .gt;
            } else {
                return .eq;
            }
        }

        pub fn key_from_value(value: *const Value) Self {
            return .{
                .field = value.field,
                .timestamp = @truncate(u63, value.timestamp),
            };
        }

        pub fn tombstone(value: *const Value) bool {
            return (value.timestamp & tombstone_bit) != 0;
        }

        pub fn tombstone_from_key(key: Self) Value {
            return .{
                .field = key.field,
                .timestamp = key.timestamp | tombstone_bit,
            };
        }
    };
}
