# RustLog

## Project Overview

RustLog is a single-node, log-structured message broker that demonstrates fundamental distributed systems concepts through intentional design constraints. It implements core message broker semantics—append-only logs, pull-based consumption, and consumer group isolation—while eliminating distributed coordination complexity.

RustLog explores the tradeoffs between **correctness and performance**, **simplicity and features**, and **determinism and optimization**. It serves as a reference implementation for understanding log-structured storage, crash recovery mechanisms, and stateless protocol design.

**RustLog is NOT Apache Kafka-compatible**. It uses a custom binary protocol and storage format designed for educational clarity rather than production compatibility.

## Design Philosophy

RustLog embodies specific design priorities:

- **Determinism over throughput**: Synchronous processing ensures reproducible behavior
- **Correctness over scale**: Single-node design eliminates distributed coordination bugs  
- **Simplicity over features**: Constrained scope enables comprehensive understanding
- **Testability over optimization**: 162 tests verify behavior under all conditions

These constraints enable guarantees impossible in distributed systems: deterministic failure modes, complete crash safety, and zero race conditions.

### Explicit Non-Goals

RustLog intentionally excludes features that would compromise its educational value:

- **Replication or clustering**: No distributed coordination protocols
- **Exactly-once semantics**: At-least-once delivery semantics only
- **Background processing**: No async operations that introduce timing dependencies  
- **Dynamic configuration**: All behavior determined at startup
- **Multi-tenancy**: No authentication, authorization, or isolation beyond consumer groups

## Core Capabilities

**Append-Only Log Storage**
- Sequential writes to immutable segment files
- Memory-mapped reads with OS page cache optimization
- Sparse indexing for O(log N) offset lookups

**Pull-Based Consumer Model**  
- Consumers explicitly fetch data at their own pace
- No broker-side backpressure or flow control complexity
- Consumer group offset isolation

**Crash-Safe Operations**
- Atomic file operations with directory fsync
- Automatic index rebuild after corruption detection
- Deterministic recovery from any failure state

**Incremental Log Compaction**
- Copy-on-write deduplication by message key
- Atomic segment replacement preserving offset semantics
- Configurable compaction policies

**Index Rebuild & Corruption Recovery**
- Automatic detection of index inconsistencies  
- Deterministic index reconstruction from log segments
- Zero data loss during corruption scenarios

**Metrics-First Observability**
- Lock-free counters and latency histograms
- Consumer lag tracking and partition-level statistics
- Prometheus-compatible metrics export

## What RustLog Intentionally Does NOT Implement

- Replication or distributed consensus (Raft, PBFT, etc.)
- Consensus protocols for coordination or leadership election
- Exactly-once delivery semantics or multi-partition transactions
- Background async processing or non-deterministic timing behavior
- Auto topic creation or dynamic partition assignment
- Authentication, authorization, or access control mechanisms
- Wire format compatibility with Apache Kafka or other message brokers
- Schema evolution or payload validation
- Multi-datacenter deployment or geographic distribution
- Auto-scaling, load balancing, or dynamic resource management
- Complex retention policies beyond time and size-based cleanup

## Repository Structure

```
src/           Production-quality Rust implementation
├── broker/    TCP server and connection handling
├── storage/   Segment files, indexing, and crash recovery  
├── topics/    Partition management and multi-segment reads
├── offsets/   Consumer group state and isolation
├── protocol/  Binary wire format and request routing
├── admin/     Topic management and metadata operations
├── metrics/   Lock-free observability subsystem
└── bin/       Admin CLI tools

docs/          Technical documentation for systems engineers
├── ARCHITECTURE.md     System design and component interactions
├── STORAGE.md          Log-structured storage and crash recovery  
├── PROTOCOL.md         Binary wire format specification
├── FAILURE_MODES.md    Comprehensive failure analysis
└── DESIGN_DECISIONS.md Architecture rationale and tradeoffs

tests/         162 comprehensive tests covering all failure modes
├── integration_test.rs           Core produce/fetch functionality
├── phase*_test.rs               Feature-specific test suites  
├── test_minimal_isolation.rs    Consumer group isolation
└── ...

benches/       Performance benchmarks and analysis tools
examples/      Reference client implementations
```

## How to Read This Codebase

For systems engineers and interview preparation, follow this reading order:

1. **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** - Start here for system overview, design principles, and module boundaries

2. **[STORAGE.md](docs/STORAGE.md)** - Deep dive into log-structured storage, crash recovery, and memory mapping

3. **[FAILURE_MODES.md](docs/FAILURE_MODES.md)** - Understand failure domains, recovery procedures, and operational characteristics

4. **[DESIGN_DECISIONS.md](docs/DESIGN_DECISIONS.md)** - Explore architectural tradeoffs and rejected alternatives

5. **[PROTOCOL.md](docs/PROTOCOL.md)** - Review binary protocol specification and wire format details

After reviewing documentation, examine the implementation:

1. `src/broker/server.rs` - Request routing and connection handling
2. `src/storage/segment.rs` - Core append-only storage implementation  
3. `src/topics/partition.rs` - Multi-segment read coordination
4. `tests/integration_test.rs` - End-to-end behavior verification

## Quick Start

**Build and run broker**:
```bash
cargo build --release
./target/release/kafka-lite
```

**Run test suite** (162 tests):
```bash
cargo test
```

**Example client interactions**:
```bash
# See examples/ directory for:
# - Producer sending records
# - Consumer fetching with offsets  
# - Admin topic management
# - Metrics collection
```

## Project Status

RustLog is a deliberately constrained, single-node log broker. The system is feature-complete by design. No new features will be added to the core system.

The codebase demonstrates production-quality Rust implementations of:
- Log-structured storage with crash safety
- Memory-mapped I/O with sparse indexing  
- Binary protocol design and parsing
- Consumer group state management
- Comprehensive failure mode testing
- Lock-free metrics and observability

RustLog serves as a reference for understanding distributed systems concepts, systems programming techniques, and the tradeoffs inherent in message broker design.
