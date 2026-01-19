# RustLog Design Decisions

**Audience:** Systems engineers evaluating architectural tradeoffs and understanding the reasoning behind RustLog's implementation choices.

## Decision Framework

Every design decision in RustLog was made according to these **priority rankings**:

1. **Correctness** > Performance
2. **Simplicity** > Feature richness  
3. **Determinism** > Optimization
4. **Explainability** > Abstraction
5. **Testing** > Elegance

This document explains **major design decisions**, the **alternatives considered**, and the **explicit tradeoffs** made.

## Architectural Decisions

### Decision: Single-Node Architecture

**What we built**: Single-broker system with no replication or distribution.

**Alternatives considered**:

1. **Multi-node with consensus**: Raft or PBFT for leadership election
   - **Pros**: High availability, fault tolerance
   - **Cons**: Distributed consensus complexity, split-brain scenarios, network partition handling
   - **Rejected because**: Consensus protocols introduce non-deterministic behavior and complex failure modes

2. **Multi-node with primary/replica**: Simple leader-follower replication
   - **Pros**: Simpler than consensus, read scaling
   - **Cons**: Leader election during failure, replica lag handling, data consistency
   - **Rejected because**: Still requires distributed coordination for failover

3. **Shared-nothing with client-side partitioning**: Multiple independent brokers
   - **Pros**: Linear scaling, simple failure isolation
   - **Cons**: Client complexity, no cross-partition atomicity, operational overhead
   - **Rejected because**: Pushes distribution complexity to client applications

**Decision rationale**:
- **Testing**: Single-node behavior is completely deterministic and reproducible
- **Correctness**: No distributed race conditions, no split-brain scenarios
- **Simplicity**: One process, one failure domain, one state machine

**Tradeoffs accepted**:
- **No fault tolerance**: Single point of failure by design
- **Vertical scaling only**: Cannot scale beyond single machine capacity
- **No geographic distribution**: All data in one location

### Decision: Synchronous Request Processing

**What we built**: Each connection processes one request at a time, blocking until completion.

**Alternatives considered**:

1. **Pipelined requests per connection**: Multiple in-flight requests with request IDs
   - **Pros**: Higher throughput per connection, better network utilization
   - **Cons**: Complex request/response matching, partial failure handling
   - **Rejected because**: Introduces non-deterministic ordering of operations

2. **Async request processing**: Handle requests concurrently within connection
   - **Pros**: Better resource utilization, lower latency for independent requests
   - **Cons**: Race conditions between requests, complex synchronization
   - **Rejected because**: Async I/O introduces non-deterministic timing behavior

3. **Batched request processing**: Group multiple requests into single operation
   - **Pros**: Higher throughput, amortized costs
   - **Cons**: Complex batching logic, increased latency for small requests
   - **Rejected because**: Adds complexity without addressing core simplicity goals

**Decision rationale**:
- **Determinism**: Requests process in arrival order
- **Testing**: No timing-dependent behavior, reproducible test results  
- **Simplicity**: No concurrency control needed within connection
- **Clarity**: Easy to reason about request isolation

**Tradeoffs accepted**:
- **Lower throughput**: Each connection limited to sequential processing
- **Network underutilization**: TCP connections not fully utilized
- **Higher latency**: No request pipelining

### Decision: Pull-Based Consumer Model

**What we built**: Consumers explicitly fetch data with offset/max_bytes parameters.

**Alternatives considered**:

1. **Push-based with backpressure**: Broker pushes data, consumers signal capacity
   - **Pros**: Lower latency, automatic flow control
   - **Cons**: Complex backpressure protocols, consumer capacity estimation
   - **Rejected because**: Requires broker to understand consumer processing rates

2. **Push-based with buffering**: Broker pushes to client-side buffers
   - **Pros**: Predictable broker behavior, client-side flow control
   - **Cons**: Memory management complexity, buffer overflow handling
   - **Rejected because**: Shifts complexity to client without eliminating it

3. **Hybrid push/pull**: Push notifications, pull data transfer
   - **Pros**: Low latency notifications, explicit flow control
   - **Cons**: Two-phase protocol complexity, notification reliability
   - **Rejected because**: Notification failures introduce additional failure modes

**Decision rationale**:
- **Simplicity**: Broker never tracks consumer state or capacity
- **Flow control**: Consumers naturally rate-limit themselves
- **Replay capability**: Consumers can re-read data by adjusting fetch offset
- **Stateless**: No broker-side consumer tracking required

**Tradeoffs accepted**:
- **Higher latency**: Consumers must poll for new data
- **Client complexity**: Consumers implement polling and offset management
- **Network chattiness**: Empty fetch responses when no new data

### Decision: Stateless Connections

**What we built**: Each TCP connection operates independently with no server-side session state.

**Alternatives considered**:

1. **Session-based connections**: Server tracks client state, capabilities, preferences
   - **Pros**: Personalized behavior, connection-level optimizations
   - **Cons**: Session recovery after disconnect, state synchronization complexity
   - **Rejected because**: Session state is distributed coordination in disguise

2. **Heartbeat-based liveness**: Periodic ping/pong to detect client failures
   - **Pros**: Fast failure detection, resource cleanup
   - **Cons**: Timeout configuration, false positives during network issues
   - **Rejected because**: Heartbeat protocols introduce timing dependencies

3. **Connection pooling**: Multiple logical streams over single TCP connection
   - **Pros**: Better network efficiency, reduced connection overhead
   - **Cons**: Stream management, head-of-line blocking
   - **Rejected because**: Multiplexing complexity without clear benefit

**Decision rationale**:
- **Independence**: Connection failures don't affect other clients
- **Simplicity**: No connection state to manage or recover
- **Scalability**: Linear scaling with connection count

**Tradeoffs accepted**:
- **Resource waste**: Connection resources held until OS timeout
- **No personalization**: Cannot optimize behavior per client
- **No early failure detection**: Rely on TCP timeouts

## Storage Decisions

### Decision: Append-Only Log Segments

**What we built**: All writes append to current segment, reads via memory mapping.

**Alternatives considered**:

1. **B-Tree storage engine**: Sorted key-value pairs with in-place updates
   - **Pros**: Efficient random access, space efficiency after updates
   - **Cons**: Complex crash recovery, read/write coordination, page splitting
   - **Rejected because**: B-Trees require complex concurrency control

2. **LSM-Tree storage**: Multiple levels with compaction
   - **Pros**: Good write performance, eventual space efficiency
   - **Cons**: Complex compaction scheduling, read amplification
   - **Rejected because**: LSM compaction introduces non-deterministic timing

3. **Memory-only with snapshots**: Keep data in RAM, periodic disk snapshots
   - **Pros**: Extremely fast access, simple data structures
   - **Cons**: Data loss between snapshots, memory capacity limits
   - **Rejected because**: Violates durability requirements

**Decision rationale**:
- **Crash safety**: Append-only is naturally crash-consistent
- **Write performance**: Sequential writes leverage disk characteristics
- **Simplicity**: No complex recovery procedures needed
- **Memory mapping**: OS handles caching efficiently

**Tradeoffs accepted**:
- **Space inefficiency**: No compaction of deleted records
- **Sequential access**: Not optimized for random key lookups
- **Large index files**: Sparse indexing for offset lookups

### Decision: Sparse Index Structure

**What we built**: Index every 100th record (configurable), binary search for lookups.

**Alternatives considered**:

1. **Dense indexing**: Index every record for O(1) offset lookup
   - **Pros**: Constant-time lookups, no binary search needed
   - **Cons**: Large index files, memory usage, index maintenance overhead
   - **Rejected because**: Index size grows proportional to record count

2. **Clustered indexing**: Store records sorted by offset in B-Tree
   - **Pros**: Automatic sorting, efficient range queries
   - **Cons**: Complex insertion, rebalancing, crash recovery
   - **Rejected because**: B-Tree complexity contradicts simplicity goals

3. **Hash indexing**: Hash table mapping offset to file position
   - **Pros**: O(1) average lookup time, space efficient
   - **Cons**: No range queries, hash collision handling, poor locality
   - **Rejected because**: Cannot support range fetches efficiently

**Decision rationale**:
- **Space efficiency**: Index size O(log N) instead of O(N)
- **Cache locality**: Binary search has good memory access patterns
- **Simplicity**: Straightforward binary search algorithm
- **Flexibility**: Index density configurable based on access patterns

**Tradeoffs accepted**:
- **O(log N) lookup time**: Not constant time like dense index
- **Cold cache penalty**: Initial binary search may miss cache
- **Sequential scan**: Must scan between index entries

### Decision: Memory Mapping for Reads

**What we built**: Sealed segments use mmap() for zero-copy reads.

**Alternatives considered**:

1. **Buffered I/O**: Standard file reads with userspace buffering
   - **Pros**: Predictable memory usage, explicit buffer management
   - **Cons**: Data copying overhead, buffer cache management
   - **Rejected because**: OS page cache is more efficient than userspace buffering

2. **Direct I/O**: Bypass OS page cache entirely
   - **Pros**: Predictable performance, no cache pollution
   - **Cons**: Requires alignment, no read amplification benefits
   - **Rejected because**: OS cache provides valuable read amplification

3. **Async I/O**: io_uring or epoll-based async file operations
   - **Pros**: Better CPU utilization, overlap computation and I/O
   - **Cons**: Complex completion handling, non-deterministic timing
   - **Rejected because**: Async I/O introduces timing dependencies

**Decision rationale**:
- **Zero-copy**: Memory mapping eliminates data copying
- **OS optimization**: Page cache replacement policies are well-tuned
- **Lazy loading**: Pages loaded on demand, not upfront
- **Shared memory**: Multiple processes can share mapped pages

**Tradeoffs accepted**:
- **Virtual memory overhead**: Address space usage grows with data size
- **Page fault latency**: Cold pages incur fault overhead
- **No control over eviction**: Cannot prioritize which pages stay resident

## Protocol Decisions

### Decision: Binary Framing with JSON Payloads

**What we built**: 4-byte length prefix + request type byte + JSON payload.

**Alternatives considered**:

1. **Pure JSON over HTTP**: REST API with HTTP/JSON
   - **Pros**: Human readable, standard tools, widespread support
   - **Cons**: HTTP overhead, text parsing cost, no streaming
   - **Rejected because**: HTTP adds unnecessary complexity for broker protocol

2. **Pure binary protocol**: Custom binary encoding like Kafka protocol
   - **Pros**: Maximum efficiency, compact wire format
   - **Cons**: Complex serialization, debugging difficulty, schema evolution
   - **Rejected because**: Binary protocols are hard to debug and evolve

3. **Protocol Buffers**: Schema-based binary serialization
   - **Pros**: Efficient encoding, schema evolution, language support
   - **Cons**: Schema management, compilation step, debugging difficulty
   - **Rejected because**: Protobufs add tooling complexity for small protocol

4. **MessagePack**: Binary JSON encoding
   - **Pros**: Compact binary format, JSON-compatible
   - **Cons**: Additional dependency, less human-readable
   - **Rejected because**: Marginal efficiency gain not worth debugging cost

**Decision rationale**:
- **Debuggability**: JSON payloads readable in network captures
- **Evolution**: Adding fields doesn't break existing clients
- **Simplicity**: No schema compilation or code generation
- **Efficiency**: Binary framing avoids HTTP overhead

**Tradeoffs accepted**:
- **Wire size**: JSON larger than binary encoding
- **Parse cost**: JSON parsing more expensive than binary
- **No schema validation**: Malformed JSON handled at runtime

### Decision: Length-Prefixed Framing

**What we built**: 4-byte big-endian length, followed by payload of that length.

**Alternatives considered**:

1. **Delimiter-based framing**: Use newline or special character to separate frames
   - **Pros**: Simple parsing, human readable
   - **Cons**: Payload escaping required, variable delimiter scanning
   - **Rejected because**: Escaping adds complexity and payload restrictions

2. **Fixed-size frames**: All frames exactly N bytes, pad if necessary
   - **Pros**: Extremely simple parsing, no length field needed
   - **Cons**: Wastes space for small payloads, limits maximum message size
   - **Rejected because**: Fixed size doesn't fit variable message patterns

3. **HTTP-style headers**: Content-Length header followed by payload
   - **Pros**: Human readable, follows web standards
   - **Cons**: Header parsing complexity, larger overhead
   - **Rejected because**: HTTP headers add unnecessary complexity

**Decision rationale**:
- **Simplicity**: Single length field, no escaping needed
- **Efficiency**: Minimal overhead (4 bytes per frame)
- **Streaming**: Can process frame as soon as length is known
- **Robustness**: Clear frame boundaries prevent parsing ambiguity

**Tradeoffs accepted**:
- **Fixed overhead**: 4 bytes per frame regardless of size
- **Integer endianness**: Must handle big-endian consistently
- **Frame size limits**: 4-byte length limits frames to 4GB

## Concurrency Decisions

### Decision: Task-per-Connection Threading

**What we built**: Spawn one async task per TCP connection.

**Alternatives considered**:

1. **Thread pool**: Fixed number of threads processing connection queue
   - **Pros**: Bounded resource usage, predictable parallelism
   - **Cons**: Work queue management, load balancing complexity
   - **Rejected because**: Thread pools add coordination complexity

2. **Single-threaded event loop**: All connections on one thread
   - **Pros**: No synchronization needed, extremely simple
   - **Cons**: Cannot utilize multiple CPU cores, blocking operations stall all connections
   - **Rejected because**: Single thread limits throughput unnecessarily

3. **Actor model**: Lightweight processes with message passing
   - **Pros**: Isolated failure domains, functional concurrency
   - **Cons**: Message passing overhead, complex supervision trees
   - **Rejected because**: Actor complexity doesn't justify benefits

**Decision rationale**:
- **Isolation**: Connection failures don't affect other connections
- **Scalability**: Scales naturally with connection count
- **Simplicity**: Each task is independent, no coordination needed
- **Rust async**: Tokio makes task spawning extremely lightweight

**Tradeoffs accepted**:
- **Resource per connection**: Memory usage scales with connection count
- **Context switching**: OS task switching overhead
- **No pooling**: Cannot reuse connection handling logic

### Decision: RwLock for Partition Registry

**What we built**: `Arc<RwLock<PartitionRegistry>>` shared across connection tasks.

**Alternatives considered**:

1. **Mutex for exclusive access**: Single lock for all partition access
   - **Pros**: Simpler lock semantics, no reader/writer complexity
   - **Cons**: All reads serialize, poor read concurrency
   - **Rejected because**: Most operations are reads, serialization wastes CPU

2. **Lock-free data structures**: Atomic operations for registry updates
   - **Pros**: No blocking, maximum concurrency
   - **Cons**: ABA problems, memory ordering complexity, limited operation support
   - **Rejected because**: Lock-free programming extremely error-prone

3. **Message passing**: Send requests to registry actor via channels
   - **Pros**: No shared state, clear ownership model
   - **Cons**: Message queue management, latency overhead, backpressure handling
   - **Rejected because**: Message passing adds latency without clear benefit

**Decision rationale**:
- **Read concurrency**: Multiple fetch operations can run simultaneously
- **Write safety**: Topic creation/deletion requires exclusive access
- **Deadlock prevention**: Single lock eliminates lock ordering issues
- **Clear semantics**: Readers vs writers explicit in code

**Tradeoffs accepted**:
- **Write blocking**: Topic operations block all reads
- **Fairness issues**: Writers can starve readers under high load
- **Lock contention**: Single lock creates bottleneck

## Persistence Decisions

### Decision: In-Memory Consumer Offsets

**What we built**: Consumer group offsets stored in `HashMap`, lost on restart.

**Alternatives considered**:

1. **Persistent offset storage**: Store offsets in special internal topic
   - **Pros**: Offsets survive broker restart, cross-session continuity
   - **Cons**: Circular dependency (offsets are consumers), bootstrap complexity
   - **Rejected because**: Internal topics add significant complexity

2. **File-based offset storage**: Store offsets in local files
   - **Pros**: Simple persistence, no distributed coordination
   - **Cons**: File corruption handling, concurrent access control, backup/restore
   - **Rejected because**: File management contradicts stateless design

3. **External offset storage**: Store offsets in external database
   - **Pros**: Reliable persistence, operational tools available
   - **Cons**: External dependency, network failures, consistency guarantees
   - **Rejected because**: External dependencies contradict single-node simplicity

**Decision rationale**:
- **Simplicity**: No persistence layer needed for offsets
- **Stateless**: Broker restart returns to clean state
- **Testing**: Deterministic behavior without persistent state
- **Scope limitation**: Offset persistence not core to log storage goals

**Tradeoffs accepted**:
- **Data loss**: Consumer offsets lost on broker restart
- **Client responsibility**: Consumers must track offsets externally if needed
- **Cold start**: All consumers reset to earliest/latest after restart

## Evolution Decisions

### Decision: No Backward Compatibility Guarantees

**What we built**: Protocol and storage format can change between versions.

**Alternatives considered**:

1. **Strict backward compatibility**: Never break existing clients or data
   - **Pros**: Operational stability, gradual upgrades possible
   - **Cons**: Design constraints, technical debt accumulation
   - **Rejected because**: Educational project should prioritize learning over stability

2. **Versioned compatibility**: Support N previous versions simultaneously
   - **Pros**: Gradual migration, reduced operational risk
   - **Cons**: Multiple code paths, testing complexity, version negotiation
   - **Rejected because**: Version support complexity exceeds project scope

**Decision rationale**:
- **Learning priority**: Educational value more important than production stability
- **Design freedom**: Can fix mistakes without legacy constraints
- **Testing simplicity**: Only current version needs comprehensive testing
- **Scope management**: Compatibility support would triple implementation complexity

**Tradeoffs accepted**:
- **Breaking changes**: Upgrades may require client/data migration
- **Operational risk**: No gradual upgrade path available
- **Adoption barrier**: Production use requires version pinning

## Summary

RustLog's design decisions consistently prioritize:

1. **Correctness over performance**: Choose deterministic behavior
2. **Simplicity over features**: Eliminate unnecessary complexity
3. **Testability over optimization**: Enable comprehensive testing
4. **Clarity over abstraction**: Make behavior obvious

Every decision involves explicit **tradeoffs**. We accept:
- Lower throughput for higher correctness
- Operational limitations for implementation simplicity  
- Feature gaps for maintainable complexity
- Breaking changes for design freedom

These decisions create a system that is **understandable**, **testable**, and **correct** within its intended scope. The constraints enable guarantees that would be impossible in a more complex system.

**Key insight**: Design decisions are **permanent**. Choose the tradeoffs that align with your system's primary goals, and accept the limitations that result.