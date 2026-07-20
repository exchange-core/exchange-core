# Generic Data Structures in Zig

Use comptime type parameters to create reusable generic containers. Return a type from a function to build type-safe collections.

## When to Use

- Implementing custom containers (queues, stacks, trees)
- Building type-safe wrappers around allocations
- Creating domain-specific collections

## Pattern: Type-Returning Function

```zig
pub fn Queue(comptime Child: type) type {
    return struct {
        const Self = @This();
        const Node = struct {
            data: Child,
            next: ?*Node,
        };

        allocator: std.mem.Allocator,
        start: ?*Node,
        end: ?*Node,

        pub fn init(allocator: std.mem.Allocator) Self {
            return Self{ .allocator = allocator, .start = null, .end = null };
        }

        pub fn enqueue(self: *Self, value: Child) !void {
            const node = try self.allocator.create(Node);
            node.* = .{ .data = value, .next = null };
            if (self.end) |end| end.next = node else self.start = node;
            self.end = node;
        }

        pub fn dequeue(self: *Self) ?Child {
            const start = self.start orelse return null;
            defer self.allocator.destroy(start);
            if (start.next) |next| self.start = next else {
                self.start = null;
                self.end = null;
            }
            return start.data;
        }
    };
}
```

## Key Techniques

- `@This()` returns the enclosing struct type for self-reference
- Nested `Node` struct keeps implementation details private
- Allocator passed to init, stored for later operations
- `defer` for cleanup in dequeue prevents leaks

## Usage

```zig
var queue = Queue(u32).init(allocator);
try queue.enqueue(42);
const value = queue.dequeue(); // ?u32
```
