# RustLog

**A Kafka-inspired append-only log broker for learning distributed systems.**

RustLog is a single-node message broker implementing core Kafka concepts: append-only logs, consumer groups, pull-based fetching, and consumer-owned offsets. It is designed for backend engineers and distributed systems learners who want to understand log storage internals without the operational complexity of a production Kafka cluster.

## Project Scope

**This is an educational project, not production software.**

RustLog demonstrates:
- Append-only segment-based storage
- Consumer group offset tracking
- Binary TCP protocol with framing
- Memory-mapped reads for sealed segments
- Index-based offset lookup

RustLog intentionally omits:
- ❌ Replication and clustering
- ❌ Persistent offset storage (in-memory only)
- ❌ Log retention or compaction
- ❌ Authentication and authorization
- ❌ Exactly-once semantics
- ❌ Consumer rebalancing

**Use this to learn. Not for production.**

## Architecture

RustLog follows a layered design:

**Broker Layer**
- Stateless TCP connections (one task per connection)
- Request routing (Produce, Fetch, OffsetCommit, OffsetFetch)
- Partition registry (topic + partition → Partition instance)
- Offset manager (in-memory consumer group offsets)

**Storage Layer**
- Active segment: writable, uses File I/O
- Sealed segments: read-only, uses mmap for zero-copy reads
- Index files: sparse offset → byte position mapping
- Partition: orchestrates reads across multiple segments

**Protocol**
- Binary TCP with 4-byte length-prefixed frames
- No Kafka wire format compatibility
- Request/response model (no streaming)

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed design.
- Linux, macOS, or WSL (mmap implementation)

### Build

```bash
# Clone the repository
cd Kafka-lite

# Build in release mode
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench
```

### Run the Broker

```bash
# Build release binary
cargo build --release

# Run broker (listens on 127.0.0.1:9092)
cargo run --release
```

Output:
```
╔═══════════════════════════════════════╗
║        RustLog Broker v0.1.0          ║
║     Kafka-lite Distributed Log        ║
╚═══════════════════════════════════════╝

Broker started on 127.0.0.1:9092
```

### Demo: Producer

In a new terminal, run the producer example:

```bash
cargo run --example producer
```

This produces 3 messages (`["hello", "from", "rustlog"]`) to topic `demo`, partition 0.

**Expected output:**
```
RustLog Producer Demo
=====================

Connecting to broker at 127.0.0.1:9092...
Connected

Producing 3 messages to topic 'demo', partition 0...

Production successful!
Base offset: 0
Records written: 3

  Offset 0: hello
  Offset 1: from
  Offset 2: rustlog

Done
```

### Demo: Consumer (Offset-Based)

Run the consumer in offset-based mode (default):

```bash
cargo run --example consumer
```

This fetches from offset 0 without consumer group tracking.

**Expected output:**
```
RustLog Consumer Demo
=====================

Connecting to broker at 127.0.0.1:9092...
Connected

Offset-based consumer (no group)
Starting offset: 0

Fetching from topic 'demo', partition 0...

Fetch successful!
Records fetched: 3
Next offset: 3

Records:
  Offset 0: hello
  Offset 1: from
  Offset 2: rustlog

Done
```

### Demo: Consumer (Consumer Group)

Run the consumer with a consumer group:

```bash
cargo run --example consumer -- --group analytics
```

This uses consumer group `analytics` to track committed offsets.

**First run (no committed offset):**
```
RustLog Consumer Demo
=====================

Connecting to broker at 127.0.0.1:9092...
Connected

Consumer group: analytics
No committed offset found, starting from 0
Starting offset: 0

Fetching from topic 'demo', partition 0...

Fetch successful!
Records fetched: 3
Next offset: 3

Records:
  Offset 0: hello
  Offset 1: from
  Offset 2: rustlog

Committing offset 3 for group 'analytics'...
Committed

Done
```

**Second run (resumes from committed offset):**
```
Consumer group: analytics
Found committed offset: 3
Starting offset: 3

Fetching from topic 'demo', partition 0...

Fetch successful!
Records fetched: 0
Next offset: 3

No records available at offset 3
```

### Consumer Offset Management

RustLog uses **consumer-owned offsets** (Kafka model):

- Offsets are **explicitly committed** by consumer
- No auto-commit behavior
- Broker stores offsets in-memory per `(group, topic, partition)`
- Fetch with `group_id` uses `max(committed_offset, requested_offset)`
- **Offsets lost on broker restart** (in-memory only)

See [docs/OFFSETS.md](docs/OFFSETS.md) for implementation details.

## Testing

```bash
# Run all tests (unit + integration)
cargo test

# Run only integration tests
cargo test --test integration_test

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_fetch_uses_committed_offset
```

**Test coverage:**
- 53 unit tests (storage, index, segment, partition, offsets)
- 11 integration tests (end-to-end TCP protocol)
- **64 total tests passing**

## Performance Benchmarks

```bash
# Run read path benchmarks
cargo bench --bench read_path

# Results are saved to target/criterion/
```

**Key findings:**
- **mmap reads: 2.5-3.9 GiB/s** (sealed segments)
- **File I/O reads: 220-1024 MiB/s** (active segments)
- **mmap is 10-18x faster** than file I/O for sequential reads
- **Partition reads: 147 GiB/s** (multi-segment aggregation)

See [docs/PERFORMANCE.md](docs/PERFORMANCE.md) for detailed analysis.

## Documentation

- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** - System design and component responsibilities
- **[STORAGE.md](docs/STORAGE.md)** - Segment lifecycle, record format, index structure
- **[PROTOCOL.md](docs/PROTOCOL.md)** - Binary protocol specification
- **[OFFSETS.md](docs/OFFSETS.md)** - Consumer group offset management
- **[PERFORMANCE.md](docs/PERFORMANCE.md)** - Benchmark results and analysis
- **[LIMITATIONS.md](docs/LIMITATIONS.md)** - Known limitations and non-features

## Project Structure

```
Kafka-lite/
├── src/
│   ├── lib.rs              # Library entry point
│   ├── main.rs             # Broker executable
│   ├── error.rs            # Error types
│   ├── broker/             # TCP server and connection handling
│   │   ├── mod.rs
│   │   ├── server.rs       # TCP listener
│   │   └── connection.rs   # Per-connection handler
│   ├── protocol/           # Wire protocol
│   │   ├── mod.rs
│   │   ├── frame.rs        # Length-prefixed framing
│   │   ├── request.rs      # Request encoding/decoding
│   │   └── response.rs     # Response encoding/decoding
│   ├── storage/            # Log storage primitives
│   │   ├── mod.rs
│   │   ├── segment.rs      # Append-only log file
│   │   ├── index.rs        # Offset → position mapping
│   │   └── mmap.rs         # Memory-mapped read region
│   ├── topics/             # Partition management
│   │   ├── mod.rs
│   │   └── partition.rs    # Multi-segment read orchestration
│   └── offsets/            # Consumer offset tracking
│       ├── mod.rs
│       └── manager.rs      # In-memory offset storage
├── tests/
│   └── integration_test.rs # End-to-end broker tests
├── benches/
│   └── read_path.rs        # Storage read performance
├── examples/
│   └── simple_client.rs    # Example TCP client
└── docs/                   # Detailed documentation
```

## Design Philosophy

RustLog prioritizes **clarity and correctness over features**:

1. **Simple is better than complex** - Single broker, no distributed coordination
2. **Explicit is better than implicit** - No auto-commit, consumer-driven offsets
3. **Safe is better than fast** - mmap only for read-only sealed segments
4. **Local is better than remote** - File-based storage, no network dependencies
5. **Testable is better than clever** - Extensive unit and integration tests

## Comparison with Apache Kafka

| Feature | Kafka | RustLog |
|---------|-------|---------|
| Replication | ✅ Multi-replica with ISR | ❌ Single copy only |
| Clustering | ✅ Multi-broker with ZooKeeper/KRaft | ❌ Single broker |
| Offset Storage | ✅ Durable (internal topic) | ❌ In-memory only |
| Retention | ✅ Time/size-based deletion | ❌ Manual management |
| Compaction | ✅ Log compaction | ❌ Not implemented |
| Security | ✅ SASL, SSL, ACLs | ❌ No security |
| Protocol | ✅ Kafka wire protocol | ❌ Custom binary protocol |
| Transactions | ✅ Exactly-once semantics | ❌ At-least-once only |
| Zero-copy | ✅ sendfile() syscall | ✅ mmap for reads |
| Consumer Groups | ✅ With rebalancing | ✅ Without rebalancing |

**RustLog is NOT a Kafka replacement. It's a learning tool and reference implementation.**

## Contributing

This is an educational project. Contributions that improve clarity, fix bugs, or enhance documentation are welcome. New features should align with the project's scope (no clustering, no replication).

## License

MIT License - See LICENSE file for details.

## Acknowledgments

Design inspired by:
- Apache Kafka (log-structured storage)
- Jay Kreps' "The Log" article
- Martin Kleppmann's "Designing Data-Intensive Applications"

Implementation references:
- Tokio async runtime
- memmap2 for memory mapping
- criterion for benchmarking

---

**Built with Rust 🦀 for learning and exploration.**
