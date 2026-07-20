---
name: tigerbeetle-deterministic-simulation
description: Test distributed systems in the style of TigerBeetle and Joran Dirk Greef, using deterministic simulation and time compression. Emphasizes controlling all non-determinism, simulating years of operation in minutes, and finding bugs that would take decades to manifest in production. Use when testing distributed systems, consensus protocols, or any system where correctness under failure is critical.
---

# TigerBeetle Deterministic Simulation Style Guide

## Overview

TigerBeetle, created by Joran Dirk Greef, uses deterministic simulation testing to achieve correctness guarantees that traditional testing cannot provide. By controlling all sources of non-determinism (time, network, disk I/O, random numbers), the system can simulate years of operation in minutes, replay any failure scenario exactly, and find bugs that would otherwise take decades to manifest in production. This approach was pioneered by FoundationDB and refined by TigerBeetle for financial-grade reliability.

## Core Philosophy

> "If you can't reproduce it, you can't fix it. If you can't simulate it, you can't test it."

> "Real-world testing finds bugs in hours. Deterministic simulation finds the same bugs in milliseconds."

> "Time compression lets you experience a decade of production failures before your first deployment."

The fundamental insight is that distributed systems are impossible to test exhaustively with real hardware and real time. By simulating the environment deterministically, you can explore the state space millions of times faster than real time, with perfect reproducibility.

## Design Principles

1. **Control All Non-Determinism**: Time, network, disk, random—everything must be injectable.

2. **Simulate, Don't Emulate**: The simulation is the source of truth, not an approximation.

3. **Time Compression**: Simulate years of operation in minutes.

4. **Perfect Reproducibility**: Any bug can be replayed with a single seed.

5. **Failure Injection at Scale**: Test every possible failure mode, not just the ones you imagine.

## VOPR: Viewstamped Operation Replication Tester

TigerBeetle's VOPR runs deterministic simulations:

```
┌─────────────────────────────────────────────────────────────┐
│                    VOPR Simulation Loop                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│   Seed (u64) ─────► Deterministic PRNG                      │
│                          │                                   │
│                          ▼                                   │
│   ┌──────────────────────────────────────────────────────┐  │
│   │              Simulated Environment                    │  │
│   │  ┌─────────┐  ┌─────────┐  ┌─────────┐              │  │
│   │  │ Node 1  │  │ Node 2  │  │ Node 3  │  ...         │  │
│   │  └────┬────┘  └────┬────┘  └────┬────┘              │  │
│   │       │            │            │                    │  │
│   │       └────────────┼────────────┘                    │  │
│   │                    │                                 │  │
│   │            ┌───────▼───────┐                        │  │
│   │            │ Simulated     │                        │  │
│   │            │ Network/Disk  │                        │  │
│   │            │ Time/Faults   │                        │  │
│   │            └───────────────┘                        │  │
│   └──────────────────────────────────────────────────────┘  │
│                          │                                   │
│                          ▼                                   │
│              Invariant Checking                              │
│              (after every operation)                         │
│                          │                                   │
│                          ▼                                   │
│              Pass/Fail + Reproducible Seed                   │
└─────────────────────────────────────────────────────────────┘
```

## When Building Deterministic Simulation

### Always

- Inject all sources of non-determinism through interfaces
- Use a seeded PRNG for all random decisions
- Make time a controllable parameter, not `clock_gettime()`
- Log enough state to reproduce any execution
- Check invariants after every state transition
- Run millions of simulations, not dozens
- Make the simulation faster than real-time

### Never

- Call real system time in simulation mode
- Use real network or disk I/O in simulation
- Rely on thread scheduling for ordering
- Assume "it worked 1000 times, it's correct"
- Skip failure injection (partitions, crashes, slowness)
- Trust that rare events won't happen

### Prefer

- Deterministic single-threaded simulation over multi-threaded tests
- Injected time over real time
- Simulated I/O over mocked I/O
- Invariant assertions over output checking
- Seed-based reproduction over log analysis
- Exhaustive failure injection over happy path

## Code Patterns

### Time Abstraction

```zig
const std = @import("std");

/// Abstract time source - real or simulated
pub const Time = struct {
    /// Real time implementation
    pub const Real = struct {
        pub fn now() i64 {
            return std.time.milliTimestamp();
        }
        
        pub fn sleep(ns: u64) void {
            std.time.sleep(ns);
        }
    };
    
    /// Simulated time - fully controlled
    pub const Simulated = struct {
        current_tick: u64 = 0,
        pending_events: std.PriorityQueue(Event, void, Event.compare),
        
        pub fn now(self: *Simulated) i64 {
            return @intCast(self.current_tick);
        }
        
        pub fn sleep(self: *Simulated, ns: u64) void {
            // In simulation, sleep is instant - just advance the clock
            _ = ns;
        }
        
        pub fn advanceTo(self: *Simulated, tick: u64) void {
            std.debug.assert(tick >= self.current_tick);
            self.current_tick = tick;
        }
        
        pub fn scheduleAt(self: *Simulated, tick: u64, callback: Event) void {
            self.pending_events.add(.{
                .tick = tick,
                .callback = callback,
            }) catch unreachable;
        }
        
        pub fn processNextEvent(self: *Simulated) ?Event {
            if (self.pending_events.peek()) |event| {
                if (event.tick <= self.current_tick) {
                    return self.pending_events.remove();
                }
            }
            return null;
        }
    };
};
```

### Deterministic PRNG

```zig
/// Deterministic random number generator
/// Same seed = same sequence = reproducible bugs
pub const PRNG = struct {
    state: u64,
    
    pub fn init(seed: u64) PRNG {
        return .{ .state = seed };
    }
    
    /// Xorshift64* - fast, deterministic, good distribution
    pub fn next(self: *PRNG) u64 {
        var x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        return x *% 0x2545F4914F6CDD1D;
    }
    
    pub fn boolean(self: *PRNG) bool {
        return self.next() & 1 == 0;
    }
    
    pub fn range(self: *PRNG, min: u64, max: u64) u64 {
        return min + (self.next() % (max - min + 1));
    }
    
    /// Weighted boolean - returns true with given probability (0.0-1.0)
    pub fn chance(self: *PRNG, probability: f64) bool {
        const threshold = @as(u64, @intFromFloat(probability * @as(f64, std.math.maxInt(u64))));
        return self.next() < threshold;
    }
};
```

### Simulated Network

```zig
/// Simulated network with controllable failures
pub const SimulatedNetwork = struct {
    prng: *PRNG,
    time: *Time.Simulated,
    partitions: std.AutoHashMap(NodePair, void),
    in_flight: std.ArrayList(Message),
    config: NetworkConfig,
    
    pub const NetworkConfig = struct {
        /// Probability of message loss (0.0-1.0)
        message_loss_rate: f64 = 0.01,
        /// Probability of message duplication
        duplication_rate: f64 = 0.001,
        /// Probability of message reordering
        reorder_rate: f64 = 0.05,
        /// Min/max latency in ticks
        min_latency: u64 = 1,
        max_latency: u64 = 100,
        /// Probability of partition occurring
        partition_rate: f64 = 0.001,
        /// Average partition duration in ticks
        partition_duration: u64 = 1000,
    };
    
    pub fn send(self: *SimulatedNetwork, from: NodeId, to: NodeId, msg: Message) void {
        // Check for partition
        if (self.isPartitioned(from, to)) {
            // Message is silently dropped
            return;
        }
        
        // Random message loss
        if (self.prng.chance(self.config.message_loss_rate)) {
            return;
        }
        
        // Calculate delivery time
        const latency = self.prng.range(self.config.min_latency, self.config.max_latency);
        const delivery_tick = self.time.current_tick + latency;
        
        // Schedule delivery
        self.time.scheduleAt(delivery_tick, .{
            .kind = .message_delivery,
            .message = msg,
            .to = to,
        });
        
        // Possible duplication
        if (self.prng.chance(self.config.duplication_rate)) {
            const dup_latency = self.prng.range(self.config.min_latency, self.config.max_latency);
            self.time.scheduleAt(self.time.current_tick + latency + dup_latency, .{
                .kind = .message_delivery,
                .message = msg,
                .to = to,
            });
        }
    }
    
    pub fn injectPartition(self: *SimulatedNetwork, nodes: []const NodeId) void {
        for (nodes) |a| {
            for (nodes) |b| {
                if (a != b) {
                    self.partitions.put(.{ .a = a, .b = b }, {}) catch unreachable;
                }
            }
        }
        
        // Schedule partition heal
        const duration = self.prng.range(
            self.config.partition_duration / 2,
            self.config.partition_duration * 2
        );
        self.time.scheduleAt(self.time.current_tick + duration, .{
            .kind = .heal_partition,
            .nodes = nodes,
        });
    }
    
    fn isPartitioned(self: *SimulatedNetwork, a: NodeId, b: NodeId) bool {
        return self.partitions.contains(.{ .a = a, .b = b }) or
               self.partitions.contains(.{ .a = b, .b = a });
    }
};
```

### Simulated Storage

```zig
/// Simulated storage with crash and corruption injection
pub const SimulatedStorage = struct {
    prng: *PRNG,
    data: std.AutoHashMap(u64, Block),
    pending_writes: std.ArrayList(PendingWrite),
    config: StorageConfig,
    
    pub const StorageConfig = struct {
        /// Probability of read error
        read_error_rate: f64 = 0.0001,
        /// Probability of write error  
        write_error_rate: f64 = 0.0001,
        /// Probability of bit flip (corruption)
        corruption_rate: f64 = 0.00001,
        /// Probability of losing pending writes on crash
        crash_lose_pending: f64 = 0.5,
        /// Simulate torn writes
        torn_write_enabled: bool = true,
    };
    
    pub fn read(self: *SimulatedStorage, block_id: u64) !Block {
        // Simulate read error
        if (self.prng.chance(self.config.read_error_rate)) {
            return error.ReadError;
        }
        
        const block = self.data.get(block_id) orelse return error.NotFound;
        
        // Simulate corruption
        if (self.prng.chance(self.config.corruption_rate)) {
            var corrupted = block;
            const bit_position = self.prng.range(0, block.data.len * 8 - 1);
            const byte_idx = bit_position / 8;
            const bit_idx: u3 = @intCast(bit_position % 8);
            corrupted.data[byte_idx] ^= (@as(u8, 1) << bit_idx);
            return corrupted;
        }
        
        return block;
    }
    
    pub fn write(self: *SimulatedStorage, block_id: u64, block: Block) !void {
        // Simulate write error
        if (self.prng.chance(self.config.write_error_rate)) {
            return error.WriteError;
        }
        
        // Add to pending (not yet durable)
        self.pending_writes.append(.{
            .block_id = block_id,
            .block = block,
        }) catch unreachable;
    }
    
    pub fn fsync(self: *SimulatedStorage) !void {
        // Move pending writes to durable storage
        for (self.pending_writes.items) |write| {
            self.data.put(write.block_id, write.block) catch unreachable;
        }
        self.pending_writes.clearRetainingCapacity();
    }
    
    pub fn simulateCrash(self: *SimulatedStorage) void {
        // Decide which pending writes survive
        var survivors = std.ArrayList(PendingWrite).init(self.pending_writes.allocator);
        
        for (self.pending_writes.items) |write| {
            if (!self.prng.chance(self.config.crash_lose_pending)) {
                // This write survived the crash
                if (self.config.torn_write_enabled and self.prng.chance(0.1)) {
                    // Torn write - partial data written
                    var torn = write;
                    const tear_point = self.prng.range(0, torn.block.data.len);
                    @memset(torn.block.data[tear_point..], 0);
                    survivors.append(torn) catch unreachable;
                } else {
                    survivors.append(write) catch unreachable;
                }
            }
        }
        
        // Apply survivors
        for (survivors.items) |write| {
            self.data.put(write.block_id, write.block) catch unreachable;
        }
        
        self.pending_writes.clearRetainingCapacity();
    }
};
```

### The Simulation Harness

```zig
/// Main simulation harness - runs millions of deterministic simulations
pub const Simulator = struct {
    seed: u64,
    prng: PRNG,
    time: Time.Simulated,
    network: SimulatedNetwork,
    storage: []SimulatedStorage,
    nodes: []Node,
    invariant_checker: InvariantChecker,
    stats: SimulationStats,
    
    pub fn init(seed: u64, node_count: usize) Simulator {
        var prng = PRNG.init(seed);
        var time = Time.Simulated{};
        
        return .{
            .seed = seed,
            .prng = prng,
            .time = time,
            .network = SimulatedNetwork.init(&prng, &time),
            .storage = allocateStorages(node_count, &prng),
            .nodes = allocateNodes(node_count),
            .invariant_checker = InvariantChecker.init(),
            .stats = .{},
        };
    }
    
    /// Run simulation for given number of ticks
    pub fn run(self: *Simulator, max_ticks: u64) SimulationResult {
        while (self.time.current_tick < max_ticks) {
            // Process all events at current tick
            while (self.time.processNextEvent()) |event| {
                self.processEvent(event);
                
                // Check invariants after every state change
                if (self.invariant_checker.check(self.nodes)) |violation| {
                    return .{
                        .outcome = .invariant_violation,
                        .seed = self.seed,
                        .tick = self.time.current_tick,
                        .violation = violation,
                    };
                }
            }
            
            // Random fault injection
            self.maybeInjectFault();
            
            // Advance time
            self.time.advanceTo(self.time.current_tick + 1);
            self.stats.ticks_simulated += 1;
        }
        
        return .{
            .outcome = .success,
            .seed = self.seed,
            .tick = self.time.current_tick,
            .stats = self.stats,
        };
    }
    
    fn maybeInjectFault(self: *Simulator) void {
        // Network partition
        if (self.prng.chance(0.001)) {
            const partition_size = self.prng.range(1, self.nodes.len / 2);
            var partition = std.ArrayList(NodeId).init(allocator);
            // Randomly select nodes for partition
            for (0..partition_size) |_| {
                partition.append(@intCast(self.prng.range(0, self.nodes.len - 1))) catch {};
            }
            self.network.injectPartition(partition.items);
            self.stats.partitions_injected += 1;
        }
        
        // Node crash
        if (self.prng.chance(0.0001)) {
            const node_id = self.prng.range(0, self.nodes.len - 1);
            self.crashNode(@intCast(node_id));
            self.stats.crashes_injected += 1;
        }
        
        // Disk corruption
        if (self.prng.chance(0.00001)) {
            const node_id = self.prng.range(0, self.nodes.len - 1);
            // Corruption happens in storage simulation
            self.stats.corruptions_injected += 1;
        }
    }
    
    fn crashNode(self: *Simulator, node_id: NodeId) void {
        // Simulate crash: lose in-memory state, maybe lose pending writes
        self.storage[node_id].simulateCrash();
        self.nodes[node_id].restart();
    }
    
    fn processEvent(self: *Simulator, event: Event) void {
        switch (event.kind) {
            .message_delivery => {
                self.nodes[event.to].receiveMessage(event.message);
            },
            .timeout => {
                self.nodes[event.node].handleTimeout(event.timeout_id);
            },
            .heal_partition => {
                for (event.nodes) |a| {
                    for (event.nodes) |b| {
                        self.network.partitions.remove(.{ .a = a, .b = b });
                    }
                }
            },
        }
    }
};

/// Run millions of simulations with different seeds
pub fn runSimulationCampaign(config: CampaignConfig) CampaignResult {
    var results = CampaignResult{};
    
    var seed: u64 = config.starting_seed;
    while (seed < config.starting_seed + config.num_simulations) : (seed += 1) {
        var sim = Simulator.init(seed, config.node_count);
        const result = sim.run(config.ticks_per_simulation);
        
        if (result.outcome != .success) {
            results.failures.append(.{
                .seed = seed,
                .tick = result.tick,
                .violation = result.violation,
            }) catch {};
        }
        
        results.total_ticks += result.tick;
        results.simulations_run += 1;
        
        if (results.simulations_run % 10000 == 0) {
            std.debug.print("Completed {} simulations, {} failures\n", .{
                results.simulations_run,
                results.failures.items.len,
            });
        }
    }
    
    return results;
}
```

### Invariant Checking

```zig
/// Invariant checker - validates system correctness after every operation
pub const InvariantChecker = struct {
    /// All invariants that must hold
    pub fn check(self: *InvariantChecker, nodes: []Node) ?Violation {
        // Invariant 1: Consensus safety - no two nodes disagree on committed values
        if (self.checkConsensusSafety(nodes)) |v| return v;
        
        // Invariant 2: Replication - committed data exists on quorum
        if (self.checkReplication(nodes)) |v| return v;
        
        // Invariant 3: Monotonicity - committed log only grows
        if (self.checkMonotonicity(nodes)) |v| return v;
        
        // Invariant 4: Linearizability - operations appear atomic
        if (self.checkLinearizability(nodes)) |v| return v;
        
        return null;
    }
    
    fn checkConsensusSafety(self: *InvariantChecker, nodes: []Node) ?Violation {
        // For each log index, all nodes must agree on committed value
        var max_commit_index: u64 = 0;
        for (nodes) |node| {
            max_commit_index = @max(max_commit_index, node.commit_index);
        }
        
        for (0..max_commit_index) |index| {
            var reference_value: ?Entry = null;
            
            for (nodes, 0..) |node, node_id| {
                if (node.commit_index > index) {
                    const entry = node.log.get(index);
                    
                    if (reference_value) |ref| {
                        if (!std.mem.eql(u8, &entry.data, &ref.data)) {
                            return Violation{
                                .kind = .consensus_disagreement,
                                .message = "Nodes disagree on committed value",
                                .index = index,
                                .nodes = .{ node_id, 0 },  // First disagreement
                            };
                        }
                    } else {
                        reference_value = entry;
                    }
                }
            }
        }
        
        return null;
    }
    
    fn checkLinearizability(self: *InvariantChecker, nodes: []Node) ?Violation {
        // Track operation history and verify linearizability
        // This requires a history of all operations and their results
        // Using a linearizability checker like Jepsen's Knossos
        
        const history = self.buildOperationHistory(nodes);
        
        if (!isLinearizable(history)) {
            return Violation{
                .kind = .linearizability,
                .message = "Operation history is not linearizable",
                .history = history,
            };
        }
        
        return null;
    }
};
```

### Time Compression Demonstration

```python
# Conceptual demonstration of time compression value

def time_compression_analysis():
    """
    Why deterministic simulation is so powerful.
    """
    
    # Real-world testing
    real_time = {
        'ticks_per_second': 1,
        'test_hours_per_day': 8,
        'test_days_per_year': 250,
        'ticks_per_year': 8 * 3600 * 250,  # 7.2M ticks/year
    }
    
    # Deterministic simulation (TigerBeetle achieves ~1M ticks/second)
    simulation = {
        'ticks_per_second': 1_000_000,
        'seconds_to_simulate_year': real_time['ticks_per_year'] / 1_000_000,
        # 7.2 seconds to simulate a year of real testing
    }
    
    # Bug that occurs once per million operations
    rare_bug = {
        'occurrence_rate': 1 / 1_000_000,
        'real_time_to_find': '~139 days of continuous testing',
        'simulation_time_to_find': '~1 second',
    }
    
    # Run 1000 different seeds
    coverage = {
        'seeds': 1000,
        'ticks_per_seed': 10_000_000,
        'total_ticks': 10_000_000_000,
        'real_time_equivalent': '1,389 years of testing',
        'simulation_time': '~3 hours',
    }
    
    return {
        'compression_ratio': simulation['ticks_per_second'] / real_time['ticks_per_second'],
        'years_simulated_per_hour': 500,  # Approximate
        'reproducibility': 'Perfect - same seed = same execution',
    }
```

## Mental Model

TigerBeetle approaches testing by asking:

1. **Can I reproduce this?** If not, make it deterministic
2. **Am I testing time?** Compress it—simulate years in seconds
3. **Am I testing failure?** Inject it—don't wait for it
4. **Am I checking invariants?** Every operation, not just at the end
5. **How many scenarios?** Millions, not hundreds

## The Simulation Checklist

```
□ All non-determinism abstracted (time, network, disk, random)
□ PRNG seeded and reproducible
□ Time is injectable and compressible
□ Network failures simulated (partition, loss, reorder, duplicate)
□ Disk failures simulated (crash, corruption, torn writes)
□ Invariants checked after every state transition
□ Simulation faster than real-time (1M+ ticks/sec target)
□ Millions of seeds tested, not dozens
□ Any failure reproducible with just the seed
□ Bug report includes seed for exact reproduction
```

## Signature TigerBeetle Moves

- VOPR (Viewstamped Operation Replication tester)
- Time compression (years in minutes)
- Deterministic single-threaded simulation
- Seed-based perfect reproducibility
- Continuous invariant checking
- Simulated network partitions and message loss
- Crash recovery testing with torn writes
- Million-seed simulation campaigns
