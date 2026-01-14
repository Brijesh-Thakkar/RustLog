# RustLog Architecture

This document describes the high-level architecture, design decisions, and component responsibilities of RustLog.

## Design Principles

### 1. Single-Node Simplicity

RustLog is intentionally designed as a **single-broker system** with no distributed coordination. This eliminates:
- Leader election complexity
- Replica synchronization
- Split-brain scenarios
- Network partition handling
- Consensus protocols

**Trade-off:** No fault tolerance, but significantly simpler implementation and reasoning.

### 2. Pull-Based Fetch Model

Consumers explicitly fetch data at their own pace. The broker:
- Does NOT push data to consumers
- Does NOT track consumer connections
- Does NOT implement backpressure

**Rationale:** Consumers know their processing capacity better than the broker. Pull-based models naturally provide flow control and allow consumers to process at different rates.

### 3. Stateless Connections

Each TCP connection is independent and stateless. The broker:
- Does NOT maintain session state between requests
- Does NOT track active consumers
- Does NOT implement heartbeats or liveness detection

**Rationale:** Simplifies broker implementation and makes it resilient to client failures. Consumer state is managed via explicit offset commits, not connection state.

### 4. Separation of Concerns

RustLog is organized into distinct layers with clear responsibilities:

```
Protocol Layer    → Request/Response encoding/decoding
Broker Layer      → Connection handling, request routing
Partition Layer   → Multi-segment read orchestration
Storage Layer     → Segment and index file management
Offset Layer      → Consumer group offset tracking
```

Each layer has minimal dependencies on others, enabling independent testing and reasoning.

## Component Architecture

### Broker Layer

**Responsibility:** TCP server and connection lifecycle management.

```rust
// src/broker/server.rs
pub async fn run(
    addr: &str,
    partitions: PartitionRegistry,
    offset_manager: OffsetManagerRef,
) -> Result<(), BrokerError>
```

**Design decisions:**
- One Tokio task per connection (task-per-connection model)
- No connection pooling or multiplexing
- No shared mutable state between connections
- Partitions and offsets are `Arc`-wrapped for concurrent read access

**Why not connection pooling?**
- Simpler implementation
- Rust's async runtime (Tokio) handles task scheduling efficiently
- For MVP scale (hundreds of connections), task overhead is negligible

### Connection Handler

**Responsibility:** Process requests from a single client connection.

```rust
// src/broker/connection.rs
pub async fn handle_connection(
    stream: TcpStream,
    partitions: PartitionRegistry,
    offset_manager: OffsetManagerRef,
)
```

**Request processing flow:**
1. Read length-prefixed frame from TCP stream
2. Decode frame into `Request` enum
3. Call `Response::handle(request, partitions, offset_manager)`
4. Encode `Response` enum into bytes
5. Write length-prefixed frame to TCP stream
6. Repeat until connection closes

**Error handling:**
- Connection errors (EOF, timeout) → close connection silently
- Protocol errors (bad frame, decode failure) → send error response
- Internal errors (storage failure) → send error response

**Why synchronous request processing?**
- Each connection processes one request at a time (no pipelining)
- Simpler than concurrent request handling on same connection
- Sufficient for MVP workloads

### Protocol Layer

**Responsibility:** Binary protocol encoding/decoding.

```rust
// src/protocol/request.rs
pub enum Request {
    Produce { topic, partition, records },
    Fetch { group_id, topic, partition, offset, max_bytes },
    OffsetCommit { group, topic, partition, offset },
    OffsetFetch { group, topic, partition },
    Ping,
}

// src/protocol/response.rs
pub enum Response {
    ProduceResponse { base_offset, record_count },
    FetchResponse { records, next_offset },
    OffsetCommitResponse { success },
    OffsetFetchResponse { offset },
    Error { code, message },
    Pong,
}
```

**Design decisions:**
- Custom binary protocol (not Kafka-compatible)
- Length-prefixed framing (4-byte big-endian length + payload)
- Request type byte (0x01 = Produce, 0x02 = Fetch, etc.)
- Big-endian encoding for cross-platform compatibility

**Why custom protocol instead of Kafka protocol?**
- Kafka protocol is complex (100+ request/response types)
- Educational goal: understand wire protocol design
- Simplifies client implementation for examples

### Partition Layer

**Responsibility:** Orchestrate reads across multiple segments.

```rust
// src/topics/partition.rs
pub struct Partition {
    pub id: u32,
    pub sealed_segments: Vec<(Arc<Segment>, Arc<Mutex<Index>>)>,
    pub active_segment: (Arc<Segment>, Arc<Mutex<Index>>),
}

impl Partition {
    pub fn fetch(&self, start_offset: u64, max_bytes: usize) 
        -> Result<FetchResult, BrokerError>
}
```

**Fetch algorithm:**
1. Find first sealed segment containing `start_offset`
2. Read records from that segment up to `max_bytes`
3. If `max_bytes` not exhausted, continue to next segment
4. Stop when budget exhausted or no more segments
5. Read from active segment if needed

**Design decisions:**
- Sealed segments are `Arc`-wrapped for concurrent reads
- Indexes are `Mutex`-wrapped (need `&mut` for seek operations)
- Active segment uses file I/O (not mmap) because it's mutable

**Why Arc<Mutex<Index>> instead of RwLock?**
- Index operations (`lookup`) require `&mut self` for file seeking
- RwLock would require exclusive write lock anyway
- Mutex is simpler and avoids false sense of concurrency

### Storage Layer

**Responsibility:** Low-level segment and index file management.

```rust
// src/storage/segment.rs
pub struct Segment {
    base_offset: u64,
    next_offset: u64,
    file: File,
    size: u64,
    mmap: Option<MmapRegion>,  // Only for sealed segments
}

impl Segment {
    pub fn append(&mut self, payload: &[u8]) -> Result<u64, BrokerError>;
    pub fn seal(&mut self) -> anyhow::Result<()>;
    pub fn read_from_offset(&self, ...) -> anyhow::Result<ReadResult>;
    pub fn read_active_from_offset(&self, ...) -> anyhow::Result<ReadResult>;
}
```

**Segment lifecycle:**
1. **Active:** Open for writes, uses `File::append()`, no mmap
2. **Sealing:** Call `seal()` to create mmap, no more writes allowed
3. **Sealed:** Read-only, uses mmap for zero-copy reads

**Design decisions:**
- Active segments never use mmap (unsafe for mutable files)
- Sealed segments create mmap once, never modified again
- Index is separate from segment (sparse, sampled entries)

**Why separate active vs sealed read methods?**
- Active segments: `read_active_from_offset()` uses file I/O
- Sealed segments: `read_from_offset()` uses mmap
- Prevents accidental mmap on active segment (safety)

### Offset Layer

**Responsibility:** Track committed offsets per consumer group.

```rust
// src/offsets/manager.rs
pub struct OffsetManager {
    // HashMap<group_id, HashMap<topic, HashMap<partition, offset>>>
    offsets: RwLock<HashMap<String, HashMap<String, HashMap<u32, u64>>>>,
}

impl OffsetManager {
    pub fn commit(&self, group: &str, topic: &str, 
                  partition: u32, offset: u64) -> Result<()>;
    pub fn fetch_committed(&self, group: &str, topic: &str, 
                           partition: u32) -> Option<u64>;
}
```

**Design decisions:**
- In-memory only (offsets lost on restart)
- `RwLock` for concurrent read access (fetch) vs exclusive write (commit)
- No offset validation (consumer can commit any offset)
- No retention or garbage collection

**Why in-memory instead of durable storage?**
- MVP simplicity
- Adding durability requires offset log storage (circular dependency)
- Production systems (Kafka) use internal topic for offsets
- For learning purposes, in-memory is sufficient

**Why RwLock instead of Mutex?**
- Many concurrent fetches (reads) are common
- Commits (writes) are less frequent
- RwLock allows multiple readers or one writer

## Request Handling Flow

### Produce Request

```
Client                  Broker                 Partition              Storage
  |                       |                       |                      |
  |-- Produce ----------->|                       |                      |
  |   (topic, records)    |                       |                      |
  |                       |-- lookup partition -->|                      |
  |                       |                       |-- append records --->|
  |                       |                       |                      |-- write to file
  |                       |                       |<-- base_offset ------|
  |                       |<-- base_offset -------|                      |
  |<- ProduceResponse ----|                       |                      |
  |   (base_offset, count)|                       |                      |
```

### Fetch Request (with Consumer Group)

```
Client                  Broker                 Offset Mgr           Partition
  |                       |                       |                      |
  |-- Fetch ------------->|                       |                      |
  |   (group_id="g1",     |                       |                      |
  |    topic, partition)  |                       |                      |
  |                       |-- fetch_committed --->|                      |
  |                       |<-- offset=100 --------|                      |
  |                       |-- fetch(100, max) ------------------->      |
  |                       |                                              |
  |                       |                       Read from segments     |
  |                       |                       (sealed mmap + active) |
  |                       |                                              |
  |                       |<-- records, next=105 -----------------------|
  |<- FetchResponse ------|                       |                      |
  |   (records, next=105) |                       |                      |
```

**Note:** Broker does NOT auto-commit. Consumer must explicitly commit:

```
  |-- OffsetCommit ------>|                       |                      |
  |   (group="g1",        |                       |                      |
  |    offset=105)        |-- commit(g1, 105) --->|                      |
  |                       |<-- success ------------|                      |
  |<- CommitResponse -----|                       |                      |
```

## Concurrency Model

### Thread Safety

**Shared State:**
- `PartitionRegistry: Arc<HashMap<(String, u32), Arc<Partition>>>`
  - Immutable after initialization (no runtime topic/partition creation)
  - Safe to share across connections via `Arc`

- `OffsetManager: Arc<OffsetManager>`
  - Internal `RwLock` for synchronized access
  - Multiple readers (fetch) or one writer (commit)

**Per-Connection State:**
- Each connection runs in its own Tokio task
- No shared mutable state between connections
- Connection handler owns `TcpStream`

### Lock Ordering

No lock ordering issues because:
- Only one lock type (`RwLock` in OffsetManager)
- Partition reads don't require locks (immutable `Arc`)
- Index `Mutex` is per-segment, no cross-segment locking

### Deadlock Prevention

Deadlock is impossible because:
- No lock is held while waiting for I/O
- No nested locking (single lock hierarchy)
- Async runtime handles task scheduling

## Performance Considerations

### Zero-Copy Reads

Sealed segments use mmap for zero-copy reads:
- No buffer allocation
- No `read()` syscall overhead
- Kernel directly maps file pages to process memory
- ~10x faster than file I/O for sequential reads

### Write Path

Active segments use synchronous writes:
- `File::write_all()` for append
- `File::flush()` for durability (fsync)
- No write batching (each record flushed immediately)

**Trade-off:** Durability over throughput. For higher write performance, batch writes and group fsync.

### Memory Usage

- mmap regions share kernel page cache (efficient)
- Index files are small (sparse sampling)
- No in-memory record cache (rely on OS page cache)

## Failure Modes

### Connection Failures

- Client disconnect → connection task exits
- Broker does not track disconnections
- Consumer offsets persist (in-memory)
- No impact on other connections

### Storage Failures

- Disk full → write fails, error returned to client
- Corrupted segment → read fails, error returned to client
- No automatic recovery or repair

### Broker Crash

- All in-memory offsets lost
- Segments and indexes on disk are durable
- Consumers must re-commit offsets after restart

## Design Trade-offs

### Why No Replication?

**Complexity vs Benefit:**
- Replication requires leader election, ISR tracking, log reconciliation
- Adds network protocols, failure detection, split-brain handling
- For educational goals, complexity outweighs benefits

**Alternative:** Run multiple RustLog instances for different topics.

### Why No Auto-Commit?

**Explicit Control:**
- Consumer knows when processing is complete
- No risk of message loss from premature commits
- Simpler broker implementation (no connection state tracking)

**Trade-off:** More client responsibility, less convenience.

### Why No Retention?

**Scope Management:**
- Retention requires background deletion, compaction policies
- Interaction with offset management (can't delete consumed records)
- Manual management is sufficient for MVP

**Alternative:** Implement external cleanup script.

### Why Custom Protocol?

**Educational Value:**
- Understand wire protocol design principles
- Avoid Kafka protocol complexity (100+ request types)
- Simpler client implementation for examples

**Trade-off:** Not compatible with existing Kafka clients.

## Future Architecture Considerations

**If RustLog were to evolve beyond MVP, consider:**

1. **Persistent Offsets:**
   - Store offsets in internal topic (like Kafka)
   - Requires handling circular dependency (offsets are consumers)

2. **Retention Policies:**
   - Background deletion of old segments
   - Time-based or size-based retention
   - Coordination with offset storage (can't delete unconsumed data)

3. **Write Batching:**
   - Buffer multiple produce requests
   - Group fsync for better throughput
   - Add latency vs throughput tuning

4. **Connection Multiplexing:**
   - Support multiple in-flight requests per connection
   - Add request ID for response matching
   - Better network utilization

5. **Admin API:**
   - Create/delete topics and partitions at runtime
   - Inspect broker state (offsets, segments, indexes)
   - Metrics and monitoring endpoints

**These are intentionally NOT implemented in current scope.**

## Summary

RustLog's architecture prioritizes:
- **Simplicity** over feature completeness
- **Clarity** over performance optimization
- **Safety** over convenience
- **Educational value** over production readiness

The single-broker, stateless-connection, pull-based design eliminates entire categories of distributed systems complexity while retaining core log broker concepts.
