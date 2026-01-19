# RustLog Architecture

**Audience:** Systems engineers familiar with Apache Kafka, log-structured storage, and distributed systems tradeoffs.

## System Philosophy

RustLog is an **intentionally constrained** message broker that prioritizes simplicity and correctness over scalability. It embodies the principle that **limitations enable clarity**: by eliminating distributed coordination, we achieve deterministic behavior and crash safety that is impossible in multi-node systems.

### Non-Goals (Explicit Constraints)

- **No replication**: Single node, single point of failure
- **No distributed coordination**: No Raft, no leader election  
- **No transactions**: No multi-partition atomicity
- **No exactly-once delivery**: At-least-once semantics only
- **No async I/O**: Synchronous request processing
- **No background compaction**: Deterministic, on-demand only

These constraints are **features, not limitations**. They enable:
- **Deterministic behavior**: No race conditions between background tasks
- **Crash safety**: Simple atomic operations without coordination
- **Testability**: Reproducible behavior under all conditions
- **Operational simplicity**: Single-process failure domain

## Core Design Principles

### 1. Synchronous Request Processing

Every request blocks until completion. No pipelining, no async handling, no background processing that could interfere with determinism.

**Reasoning**: Async I/O increases complexity exponentially:
- Race conditions between requests
- Complex backpressure propagation  
- Non-deterministic failure modes
- Harder crash recovery

**Tradeoff**: Lower throughput for higher correctness guarantees.

### 2. Pull-Based Consumer Model

Consumers explicitly fetch data at their own pace. The broker never pushes data.

**Reasoning**: Push models require the broker to understand consumer capacity and implement complex backpressure. Pull models naturally provide flow control without broker-side state.

### 3. Stateless Connections

Each TCP connection is independent. No session state, no heartbeats, no liveness detection.

**Reasoning**: Connection state is a distributed systems problem in disguise. Stateless connections eliminate:
- Session recovery after connection drops
- Heartbeat/timeout coordination 
- Client failure detection complexity

Consumer state lives in the offset manager, not in connections.

### 4. Append-Only Storage

All mutations are appends. No in-place updates, no complex transactional behavior.

**Reasoning**: Append-only eliminates:
- Partial write corruption
- Complex recovery procedures
- Read/write coordination issues
- Crash consistency complexity

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Client                              │
│              (Pull-based, Stateless TCP)                   │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                  Broker Layer                               │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐│
│  │  Connection     │ │  Request        │ │  Response       ││ 
│  │  Handler        │ │  Router         │ │  Handler        ││
│  └─────────────────┘ └─────────────────┘ └─────────────────┘│
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                 Protocol Layer                              │
│              (Binary, Length-Prefixed)                     │
└─────────────────────┬───────────────────────────────────────┘
                      │
           ┌──────────┼──────────┐
           ▼          ▼          ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ Partition   │ │ Offset      │ │ Admin       │
│ Registry    │ │ Manager     │ │ Manager     │
│             │ │             │ │             │
│ • Produce   │ │ • Commit    │ │ • Create    │
│ • Fetch     │ │ • Fetch     │ │   Topic     │
│ • Retention │ │ • Group     │ │ • List      │
│ • Compact   │ │   Isolation │ │ • Describe  │
└─────────────┘ └─────────────┘ └─────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                  Storage Layer                              │
│                                                             │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐│
│  │    Segments     │ │     Indexes     │ │   Checkpoints   ││
│  │                 │ │                 │ │                 ││
│  │ • Active        │ │ • Rebuild       │ │ • Retention     ││
│  │ • Sealed        │ │ • Corruption    │ │ • Compaction    ││
│  │ • mmap reads    │ │   Detection     │ │ • Crash Safety  ││
│  └─────────────────┘ └─────────────────┘ └─────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

## Threading Model

RustLog uses a **minimal threading model** with explicit ownership:

```rust
// Main thread owns broker state
pub struct BrokerState {
    partitions: Arc<RwLock<PartitionRegistry>>,     // Shared read, exclusive write
    offset_manager: Arc<OffsetManager>,             // Thread-safe internally
    admin_manager: Arc<AdminManager>,               // Thread-safe internally
}

// One thread per TCP connection
async fn connection_handler(socket: TcpStream, broker_state: Arc<BrokerState>) {
    // Process one request at a time (no pipelining)
    while let request = read_request(&socket) {
        let response = handle_request(request, &broker_state);  // Synchronous
        write_response(&socket, response);
    }
}
```

### Thread Ownership Rules

1. **Connection threads**: Own TCP socket, borrow broker state
2. **Partition registry**: Shared via `RwLock` - many readers, one writer
3. **Storage layer**: Each partition owns its segments exclusively
4. **Offset manager**: Internal synchronization, externally thread-safe

### Why Minimal Threading?

- **Deterministic execution**: No race conditions between background tasks
- **Simple failure modes**: Only connection failures, no coordination failures
- **Testable**: Reproducible behavior under all conditions
- **Memory safety**: Clear ownership prevents data races

## Request Processing Flow

```
Client Request
      │
      ▼
┌─────────────────┐
│ Frame Decode    │ ← Length-prefixed binary protocol
│ (Protocol)      │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│ Request Routing │ ← Match on request type
│ (Broker)        │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐ 
│ Business Logic  │ ← Partition/Offset/Admin operations
│ (Domain Layer)  │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│ Storage Access  │ ← Segment read/write, index lookup
│ (Storage)       │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│ Response Encode │ ← Binary response construction  
│ (Protocol)      │
└─────────┬───────┘
          │
          ▼
    Client Response
```

### Request Types and Routing

```rust
pub enum Request {
    // Core message operations
    Produce { topic, partition, records },           // → PartitionRegistry
    Fetch { group_id, topic, partition, offset, max_bytes },  // → PartitionRegistry
    
    // Consumer group operations  
    OffsetCommit { group, topic, partition, offset }, // → OffsetManager
    OffsetFetch { group, topic, partition },         // → OffsetManager
    
    // Admin control plane
    CreateTopic { topic, partition_count },          // → AdminManager
    ListTopics,                                      // → AdminManager  
    DescribeTopic { topic },                         // → AdminManager
    DescribePartition { topic, partition },          // → AdminManager
    
    // Monitoring
    MetricsFetch,                                    // → MetricsRegistry
    Ping,                                           // → Immediate pong
}
```

### Why Synchronous Processing?

Each request blocks the connection until completion:

1. **Deterministic ordering**: Requests process in arrival order
2. **Simple backpressure**: TCP window provides natural flow control
3. **Clear failure semantics**: Request either succeeds or fails, no partial state
4. **Easy testing**: No timing-dependent behavior

**Tradeoff**: Lower connection-level throughput for higher correctness guarantees.

## Module Boundaries

### Partition Layer (`src/topics/partition.rs`)

**Responsibility**: Orchestrate reads across multiple segments.

```rust
pub struct Partition {
    id: u32,
    sealed_segments: Vec<(Arc<Segment>, Arc<Mutex<Index>>)>, // Immutable, mmap-enabled
    active_segment: (Arc<Mutex<Segment>>, Arc<Mutex<Index>>), // Mutable, append-only
    cleaner_checkpoint: Mutex<CleanerCheckpoint>,            // Retention/compaction state
}
```

**Key responsibilities**:
- Route fetch requests to appropriate segments  
- Coordinate retention across sealed segments
- Manage segment lifecycle (active → sealed → deleted)
- Ensure high watermark protection during cleanup

**Ownership model**: Each partition exclusively owns its segments. Segments are immutable after sealing.

### Storage Layer (`src/storage/`)

**Responsibility**: File-level operations with crash safety.

**Key modules**:
- `segment.rs`: Append-only log files with memory mapping
- `index.rs`: Sparse offset-to-position mapping  
- `cleaner_checkpoint.rs`: Crash-safe retention/compaction progress
- `index_rebuild.rs`: Corruption detection and atomic repair
- `compaction.rs`: Copy-on-write log compaction

**Crash safety invariants**:
- All mutations use atomic file operations
- Directory fsync after atomic renames
- No partial state visible after crash
- Log segments are authoritative for recovery

### Offset Manager (`src/offsets/manager.rs`)

**Responsibility**: Consumer group state isolation.

```rust
pub struct OffsetManager {
    // (group_id, topic, partition) → committed_offset
    offsets: RwLock<HashMap<(String, String, u32), u64>>,
}
```

**Isolation guarantees**:
- Different consumer groups see independent offset state
- Same group on different topics/partitions are isolated
- Thread-safe concurrent access within same group

**Persistence**: In-memory only (explicit design choice).

### Admin Manager (`src/admin/manager.rs`)

**Responsibility**: Metadata operations separate from data path.

**Philosophy**: Admin operations should not interfere with data operations. Separate code paths prevent admin complexity from affecting produce/fetch performance.

## Error Handling Philosophy  

RustLog uses **explicit error propagation** without exceptions:

```rust
pub enum BrokerError {
    // Client errors (4xx equivalent)
    TopicNotFound(String),
    PartitionNotFound(u32),
    OffsetOutOfRange(u64),
    RecordTooLarge(usize),
    
    // Server errors (5xx equivalent)
    StorageError(String),
    CorruptedData(String),
    InsufficientSpace,
}
```

### Error Recovery Strategy

1. **Client errors**: Return error response, connection remains healthy
2. **Storage errors**: Return error response, log for operator attention
3. **Corruption errors**: Attempt automatic recovery (index rebuild), fail if impossible
4. **System errors**: Terminate connection, preserve other connections

**No silent failures**: Every error is either returned to client or logged for operators.

## Performance Characteristics

### Write Path
- **Sequential disk writes**: ~200-500 MB/s on consumer SSD
- **Index updates**: Batched every 100 records (configurable)
- **fsync frequency**: Every record (safety) or batched (performance)

### Read Path  
- **mmap-based reads**: Zero-copy, OS page cache friendly
- **Index lookup**: O(log N) binary search over sparse entries
- **Fetch across segments**: Sequential scan with early termination

### Memory Usage
- **Segment mmap**: Memory usage proportional to active working set
- **Index caching**: OS handles page cache, no explicit caching
- **Connection state**: ~8KB per connection (TCP buffers)

**Design choice**: Rely on OS page cache instead of application-level caching. Simpler implementation, and OS cache replacement policies are well-tuned.

## Operational Characteristics

### Single Point of Failure
- **By design**: No replication, no failover
- **Recovery**: Restart broker, all data preserved on disk
- **Monitoring**: Process liveness is sufficient

### Capacity Planning
- **Storage**: Linear growth with data retention
- **Memory**: Working set determined by active segments + OS page cache
- **CPU**: Single-threaded per connection, scales with connection count

### Configuration Surface
- **Minimal**: Retention policies, file size limits, index density
- **No tuning**: No cache sizes, no thread pools, no timeout values

**Philosophy**: Fewer configuration options means fewer ways to misconfigure.

## Limitations and Tradeoffs

### Intentional Limitations

1. **Single node**: No distributed availability
2. **Synchronous I/O**: Lower throughput than async systems
3. **No compression**: Simplicity over wire efficiency  
4. **No authentication**: Security via network isolation
5. **No transactions**: Simple at-least-once semantics

### Scaling Boundaries

- **Throughput**: Bottlenecked by single-threaded request processing
- **Storage**: Limited by single disk capacity
- **Connections**: Limited by file descriptor limits (~64k)
- **Partitions**: Limited by partition count × segment count

### When to Choose RustLog

**Good fit**:
- Single-datacenter deployments
- Predictable traffic patterns
- Strong consistency requirements  
- Operational simplicity over scale

**Poor fit**:
- Multi-datacenter deployments
- Unpredictable traffic spikes
- Maximum throughput requirements
- Complex processing topologies

## Conclusion

RustLog demonstrates that **constraints enable simplicity**. By eliminating distributed coordination, async complexity, and background processing, we achieve a message broker that is:

- **Predictable**: Deterministic behavior under all conditions
- **Reliable**: Simple crash recovery and corruption handling
- **Understandable**: Clear failure modes and ownership rules
- **Testable**: Reproducible behavior enables comprehensive testing

The system optimizes for **correctness and simplicity** over maximum throughput or distributed availability. This is an intentional design choice that enables strong guarantees impossible in more complex systems.
