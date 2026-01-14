# Limitations and Non-Features

This document explicitly describes what RustLog **does NOT do**, why these features were excluded, and what trade-offs were made.

## Philosophy

RustLog is a **minimal viable product (MVP)** designed for:
- **Learning:** Understanding log-based systems architecture
- **Prototyping:** Quickly building event-driven applications
- **Testing:** Local development without heavy infrastructure

**RustLog is NOT:**
- ❌ A production-ready Kafka replacement
- ❌ A distributed system
- ❌ A fault-tolerant broker
- ❌ A feature-complete message queue

**Design principle:** Build the simplest possible system that demonstrates core concepts. Sacrifice advanced features for clarity and maintainability.

## Major Limitations

### 1. Single Broker (No Clustering)

**What this means:**
- RustLog runs on **one machine only**
- No coordination with other brokers
- No distributed consensus (no ZooKeeper, no Raft)
- Cannot scale horizontally by adding brokers

**Consequences:**
- ❌ **Single point of failure:** Broker crash = total outage
- ❌ **Capacity limit:** Storage/throughput bounded by single machine
- ❌ **No geo-distribution:** All data in one datacenter/AZ

**Why excluded:**
- Distributed systems are complex (consensus, leader election, split-brain)
- Requires significant engineering effort (replication, coordination)
- MVP focuses on single-node log semantics, not clustering

**Production alternative:** Use Apache Kafka, Redpanda, or Apache Pulsar.

**What you'd need to add:**
1. Broker discovery (service registry)
2. Partition replication (leader/follower model)
3. Consensus protocol (Raft or Paxos)
4. Controller node (manage partition assignments)
5. Failover logic (automatic leader election)

**Estimated complexity:** 10,000+ lines of code, months of engineering.

---

### 2. No Replication (Data Durability)

**What this means:**
- Each partition exists in **one copy only**
- No replicas on other brokers
- No synchronous or asynchronous replication

**Consequences:**
- ❌ **Permanent data loss on hardware failure:** Disk crash = all data lost
- ❌ **No high availability:** Broker failure = no reads/writes
- ❌ **No disaster recovery:** No backups, no failover

**Why excluded:**
- Replication requires multiple brokers (see limitation #1)
- Complex: leader election, replica sync, ISR (in-sync replicas)
- Significant performance overhead (network I/O, acknowledgments)

**Production requirement:** Minimum 3 replicas (1 leader + 2 followers).

**Kafka comparison:**
- Kafka: Replication factor = 3 (default)
- RustLog: Replication factor = 1 (hardcoded)

**What you'd need to add:**
1. Partition assignment (which brokers host replicas)
2. Replication protocol (producer writes to leader, leader replicates)
3. ISR tracking (which replicas are caught up)
4. Log compaction (replicas converge on same log state)
5. Failure detection (leader health checks)

**Estimated complexity:** 5,000+ lines of code, significant testing effort.

---

### 3. No Offset Persistence (Consumer State Loss)

**What this means:**
- Consumer offsets stored **in memory only**
- Broker restart → all offsets lost
- No write-ahead log (WAL) for offsets

**Consequences:**
- ❌ **Consumer reset on broker restart:** Must start from offset 0 or latest
- ❌ **No offset history:** Cannot query historical commits
- ❌ **No crash recovery:** Offset state is ephemeral

**Why excluded:**
- Simplifies implementation (no disk I/O for commits)
- Focus on log storage, not offset storage
- External systems (database, Redis) can store offsets if needed

**Production requirement:** Offsets must be durable (Kafka stores in internal topic).

**Workaround:**
```rust
// Application stores offsets externally
let offset = db.load_offset(group_id)?;
fetch(topic, partition, offset, max_bytes, None);  // No group_id
db.save_offset(group_id, next_offset)?;
```

**What you'd need to add:**
1. Offset write-ahead log (fsync on commit)
2. Periodic snapshots (compact offset log)
3. Crash recovery (replay WAL on startup)
4. Offset expiration (delete old offsets after TTL)

**Estimated complexity:** 1,000+ lines of code, careful crash testing.

---

### 4. No Consumer Rebalancing (Manual Assignment)

**What this means:**
- Application must **manually assign partitions** to consumers
- No automatic rebalancing when consumers join/leave
- No group coordinator protocol

**Consequences:**
- ❌ **Consumers cannot dynamically scale:** Adding consumer requires code change
- ❌ **No fairness guarantees:** Partitions may be unevenly distributed
- ❌ **No failure detection:** Dead consumer holds partitions indefinitely

**Why excluded:**
- Rebalancing is complex (group membership, partition assignment algorithm)
- Requires broker coordination (group coordinator, heartbeats)
- Distributed consensus for group state

**Production alternative:** Kafka's consumer group protocol (automatic rebalancing).

**Manual assignment example:**
```rust
// Consumer A
fetch(topic, partition=0, group_id="workers");

// Consumer B
fetch(topic, partition=1, group_id="workers");

// Application must coordinate which consumer reads which partition
```

**What you'd need to add:**
1. Group coordinator (broker tracks group membership)
2. Heartbeat protocol (detect consumer failures)
3. Partition assignment strategy (range, round-robin, sticky)
4. Rebalance protocol (stop-the-world or incremental)

**Estimated complexity:** 3,000+ lines of code, complex state machine.

---

### 5. No Log Retention Policies (Unbounded Growth)

**What this means:**
- Segments are **never deleted**
- No time-based retention (e.g., delete after 7 days)
- No size-based retention (e.g., keep only 100 GB)

**Consequences:**
- ❌ **Disk fills up:** Application must manually delete old segments
- ❌ **No automatic cleanup:** Operators must script deletion
- ❌ **Old data persists forever:** Privacy/compliance issues

**Why excluded:**
- Simple MVP focuses on append-only log
- Deletion is orthogonal to core log semantics
- Can be scripted externally (e.g., cron job)

**Manual cleanup:**
```bash
# Delete segments older than 7 days
find data/topics/*/partition-*/*.log -mtime +7 -delete
find data/topics/*/partition-*/*.index -mtime +7 -delete
```

**What you'd need to add:**
1. Retention configuration (per-topic TTL or size limit)
2. Background cleaner thread (periodic scan for old segments)
3. Safe deletion (ensure no readers accessing segment)
4. Metadata tracking (which offsets are still valid)

**Estimated complexity:** 500+ lines of code, careful testing for race conditions.

---

### 6. No Log Compaction (Cannot Delete Old Keys)

**What this means:**
- All records are kept **forever** (within retention policy)
- No key-based deduplication
- Cannot use log as a database table (no "last write wins")

**Consequences:**
- ❌ **Log grows unbounded for keyed data:** Same key appears multiple times
- ❌ **Cannot model changelog streams:** No snapshot of latest state
- ❌ **No tombstone support:** Cannot mark records as deleted

**Why excluded:**
- Compaction is complex (merge segments, preserve latest keys)
- Requires segment rewriting (high disk I/O)
- Not essential for event logs (only for state logs)

**Use case Kafka supports (but RustLog doesn't):**
```
Topic: user_profiles (compacted)
  Record 1: key="alice", value={"age": 25}
  Record 2: key="bob", value={"age": 30}
  Record 3: key="alice", value={"age": 26}  ← Overwrites Record 1

After compaction:
  Record 2: key="bob", value={"age": 30}
  Record 3: key="alice", value={"age": 26}  ← Record 1 deleted
```

**What you'd need to add:**
1. Key extraction (records must have keys)
2. Compaction algorithm (merge segments, keep latest per key)
3. Tombstone handling (special marker to delete keys)
4. Background compaction thread (periodic merge)

**Estimated complexity:** 2,000+ lines of code, complex correctness testing.

---

### 7. No Exactly-Once Semantics (At-Least-Once Only)

**What this means:**
- Consumer may **process records multiple times**
- No transactional support (producer or consumer side)
- No idempotent producer (duplicate detection)

**Consequences:**
- ❌ **Duplicate delivery on consumer crash:** Records re-processed after restart
- ❌ **No atomic writes across partitions:** Cannot commit to multiple partitions atomically
- ❌ **Application must handle duplicates:** Idempotent processing required

**Why excluded:**
- Exactly-once requires transactions (complex state coordination)
- Needs idempotent producer (deduplication on broker side)
- Transactional offsets (commit + offset update in one atomic operation)

**At-least-once pattern (RustLog supports):**
```rust
loop {
    let records = fetch(...);
    process(records)?;  // May fail here and re-process after restart
    commit(next_offset)?;
}
```

**Exactly-once pattern (RustLog does NOT support):**
```rust
// Would require transaction API:
let txn = begin_transaction()?;
txn.produce(topic, records)?;
txn.commit_offsets(group_id, offsets)?;
txn.commit()?;  // Atomic: both produce and offset commit
```

**What you'd need to add:**
1. Producer transactions (begin, commit, abort)
2. Transaction coordinator (manage transaction state)
3. Idempotent producer (deduplicate writes)
4. Transactional offsets (link offsets to transactions)

**Estimated complexity:** 5,000+ lines of code, distributed transaction protocol.

---

### 8. No Authentication or Authorization (Open Access)

**What this means:**
- **Anyone can connect** to broker (no username/password)
- **No TLS/SSL** (plaintext TCP)
- **No ACLs** (all topics/partitions accessible to all clients)

**Consequences:**
- ❌ **Security risk:** Unauthorized access to data
- ❌ **No audit trail:** Cannot track who accessed what
- ❌ **Network sniffing:** Data visible on network

**Why excluded:**
- MVP focuses on log semantics, not security
- Authentication/authorization is orthogonal to log design
- TLS adds complexity (certificate management, handshake)

**Production requirement:** Must add authentication and encryption.

**What you'd need to add:**
1. TLS support (certificate-based encryption)
2. SASL authentication (username/password, tokens)
3. ACL system (per-topic, per-partition permissions)
4. Audit logging (track access events)

**Estimated complexity:** 1,500+ lines of code, security testing.

---

### 9. No Compression (Uncompressed Storage)

**What this means:**
- Records stored **as-is** (no compression)
- No gzip, snappy, lz4, or zstd support
- Network bandwidth = full payload size

**Consequences:**
- ❌ **Higher disk usage:** Large payloads consume more space
- ❌ **Higher network usage:** Clients must transfer full data
- ❌ **Slower over WAN:** Bandwidth-limited networks suffer

**Why excluded:**
- Compression adds CPU overhead (encode/decode)
- Requires protocol changes (compression codec field)
- Simpler to handle raw bytes (no codec negotiation)

**Typical Kafka compression ratios:**
- Text/JSON: 5-10x (gzip)
- Binary/Protobuf: 2-3x (snappy, lz4)

**What you'd need to add:**
1. Compression codec field (request/response)
2. Compress on write (producer side or broker side)
3. Decompress on read (consumer side)
4. Per-segment compression (sealed segments)

**Estimated complexity:** 500+ lines of code, benchmark trade-offs.

---

### 10. No Schema Registry (No Schema Validation)

**What this means:**
- RustLog treats records as **opaque byte arrays**
- No schema enforcement (JSON, Avro, Protobuf, etc.)
- No schema evolution or compatibility checks

**Consequences:**
- ❌ **No type safety:** Consumers must parse and validate manually
- ❌ **Breaking changes possible:** Producer can send incompatible data
- ❌ **No schema versioning:** Cannot track schema history

**Why excluded:**
- Schema management is application-layer concern
- Kafka also doesn't enforce schemas (uses external registry)
- Adds complexity (schema storage, compatibility checks)

**Production pattern:** Use Confluent Schema Registry or Apicurio.

**Application-side validation:**
```rust
let record = fetch(...).records[0];
let payload: MySchema = serde_json::from_slice(&record.payload)?;  // Parse + validate
```

**What you'd need to add:**
1. Schema storage (database or internal topic)
2. Schema validation (on produce, enforce schema ID)
3. Compatibility checks (backward, forward, full)
4. Schema evolution rules (add/remove fields)

**Estimated complexity:** 2,000+ lines of code, external service integration.

---

### 11. No Metrics or Monitoring (No Observability)

**What this means:**
- No built-in metrics (Prometheus, Grafana)
- No health checks (HTTP endpoint, liveness probes)
- No tracing (OpenTelemetry, Jaeger)

**Consequences:**
- ❌ **Cannot monitor broker health:** Unknown if broker is overloaded
- ❌ **No lag tracking:** Cannot see consumer group lag
- ❌ **No performance insights:** Blind to bottlenecks

**Why excluded:**
- MVP focuses on core functionality, not operations
- Observability is orthogonal to log design
- Can be added via external tools (e.g., log scraping)

**Production requirement:** Metrics, logs, tracing are essential.

**What you'd need to add:**
1. Prometheus metrics (produce rate, fetch latency, etc.)
2. Health check endpoint (HTTP `/health`)
3. Structured logging (JSON logs, log levels)
4. Distributed tracing (span propagation)

**Estimated complexity:** 1,000+ lines of code, instrumentation overhead.

---

### 12. No Admin API (No Runtime Management)

**What this means:**
- **Cannot create topics at runtime** (must restart broker)
- **Cannot add partitions dynamically**
- **Cannot query broker metadata** (list topics, partitions)

**Consequences:**
- ❌ **No self-service topic creation:** Must edit code and restart
- ❌ **No tooling:** Cannot build CLI or UI for management
- ❌ **No operational flexibility:** Schema changes require downtime

**Why excluded:**
- Admin API is orthogonal to log semantics
- Simplifies implementation (static configuration only)
- Can be scripted externally (edit config file, restart)

**Kafka admin API supports:**
- Create/delete topics
- Add/remove partitions
- Change configurations
- List consumer groups

**What you'd need to add:**
1. Admin protocol (CreateTopic, DeleteTopic, etc.)
2. Dynamic partition creation (grow topic without restart)
3. Metadata cache (track topics/partitions)
4. Authorization (who can create topics)

**Estimated complexity:** 1,500+ lines of code, careful state management.

---

### 13. No Multi-Datacenter Replication (No Geo-Distribution)

**What this means:**
- RustLog runs in **one datacenter/AZ only**
- No cross-region replication (MirrorMaker, Kafka Streams)
- No active-active setup

**Consequences:**
- ❌ **No disaster recovery:** AZ outage = total data loss
- ❌ **High latency for remote clients:** All traffic to single location
- ❌ **No compliance:** Cannot keep data in specific regions

**Why excluded:**
- Requires multi-broker clustering (see limitation #1)
- Complex: cross-DC network, replication lag, conflict resolution
- Edge case for MVP (single-node focus)

**Production requirement:** Use Kafka MirrorMaker or Confluent Replicator.

**What you'd need to add:**
1. Remote replication protocol (async, eventually consistent)
2. Conflict resolution (last-write-wins, custom logic)
3. Failover strategy (manual or automatic)
4. Monitoring cross-DC lag

**Estimated complexity:** 10,000+ lines of code, complex operational model.

---

### 14. No Client Libraries (Raw TCP Protocol)

**What this means:**
- No official client libraries (Python, Go, Node.js)
- Clients must implement protocol manually
- No high-level consumer/producer abstractions

**Consequences:**
- ❌ **Higher integration cost:** Every client writes protocol code
- ❌ **No connection pooling:** Clients manage connections manually
- ❌ **No retry logic:** Clients handle errors themselves

**Why excluded:**
- MVP focuses on broker implementation, not client SDKs
- Protocol is simple (JSON over TCP, easy to implement)
- Community can contribute clients if needed

**Kafka ecosystem:**
- Official clients: Java, Python, Go, .NET, etc.
- Third-party clients: Ruby, Elixir, Rust, etc.

**What you'd need to add:**
1. Client library per language (producer, consumer classes)
2. Connection management (pooling, reconnect)
3. Retry logic (exponential backoff)
4. Error handling (dead-letter queues, callbacks)

**Estimated complexity:** 5,000+ lines of code per language.

---

### 15. No Transactions (No Atomic Multi-Partition Writes)

**What this means:**
- Cannot **atomically write to multiple partitions**
- No `begin()`, `commit()`, `abort()` API
- No read-committed isolation level

**Consequences:**
- ❌ **Partial failures possible:** Write to partition A succeeds, partition B fails
- ❌ **No exactly-once across partitions:** Cannot guarantee all-or-nothing
- ❌ **Application must handle inconsistency:** Manual compensation logic

**Why excluded:**
- Transactions require distributed coordination (2PC or similar)
- Complex state machine (transaction log, coordinator)
- Significant performance overhead (latency, throughput)

**Kafka transactions example (not supported in RustLog):**
```java
producer.beginTransaction();
producer.send(topic1, record1);
producer.send(topic2, record2);
producer.commitTransaction();  // Atomic: both or neither
```

**What you'd need to add:**
1. Transaction coordinator (manage transaction state)
2. Transaction log (track pending transactions)
3. Two-phase commit protocol (prepare, commit)
4. Rollback logic (abort failed transactions)

**Estimated complexity:** 5,000+ lines of code, distributed systems expertise.

---

## Summary Table

| Feature | Kafka | RustLog | Reason for Exclusion |
|---------|-------|---------|----------------------|
| **Clustering** | ✅ Multi-broker | ❌ Single broker | Distributed consensus too complex |
| **Replication** | ✅ Configurable RF | ❌ No replication | Requires clustering |
| **Offset persistence** | ✅ Durable | ❌ In-memory only | Simplifies implementation |
| **Consumer rebalancing** | ✅ Automatic | ❌ Manual | Complex group coordination |
| **Log retention** | ✅ Time/size-based | ❌ None | Can be scripted externally |
| **Log compaction** | ✅ Key-based | ❌ Not supported | Complex merge logic |
| **Exactly-once** | ✅ Transactional | ❌ At-least-once only | Requires transactions |
| **Authentication** | ✅ SASL, TLS | ❌ None | Security out of scope |
| **Compression** | ✅ gzip, snappy, etc. | ❌ None | Adds CPU overhead |
| **Schema registry** | ✅ (external) | ❌ None | Application-layer concern |
| **Metrics/monitoring** | ✅ JMX, Prometheus | ❌ None | Observability out of scope |
| **Admin API** | ✅ Create/delete topics | ❌ Static config | Simplifies broker |
| **Multi-DC replication** | ✅ MirrorMaker | ❌ None | Requires clustering |
| **Client libraries** | ✅ Many languages | ❌ None | MVP is broker-only |
| **Transactions** | ✅ Cross-partition | ❌ None | Complex coordination |

## When to Use RustLog

**Good fit:**
- ✅ Learning distributed log concepts
- ✅ Local development and testing
- ✅ Single-machine event sourcing
- ✅ Prototyping event-driven apps
- ✅ Teaching/educational projects

**Poor fit:**
- ❌ Production workloads (no fault tolerance)
- ❌ High-availability requirements (no replication)
- ❌ Multi-datacenter deployments (single node)
- ❌ Large-scale systems (no clustering)
- ❌ Mission-critical data (no durability guarantees)

## Migration Path

**If you outgrow RustLog, migrate to:**

1. **Apache Kafka** (industry standard, full-featured)
2. **Redpanda** (Kafka-compatible, C++, faster)
3. **Apache Pulsar** (multi-tenancy, geo-replication)
4. **NATS JetStream** (lightweight, cloud-native)

**Migration strategy:**
- Keep RustLog for local dev/testing
- Deploy production system with real log broker
- Use same application logic (producer/consumer pattern)

## Design Trade-offs

**Explicit choices made in RustLog:**

| Trade-off | RustLog Choice | Rationale |
|-----------|----------------|-----------|
| **Simplicity vs Features** | Simplicity | Educational MVP, not production |
| **Performance vs Durability** | Performance | fsync per write (could batch) |
| **mmap vs Safety** | mmap (sealed only) | High read perf, safe usage |
| **JSON vs Binary** | JSON | Debuggability over speed |
| **Manual vs Auto** | Manual commits | Consumer owns offsets explicitly |
| **Stateless vs Stateful** | Stateless connections | Simpler broker logic |

## Future Enhancements (Out of Scope)

**If RustLog were to evolve beyond MVP, consider adding:**

1. **Offset persistence:** Write-ahead log for commits
2. **Log retention:** Time/size-based segment deletion
3. **Compression:** ZSTD for sealed segments
4. **Metrics:** Prometheus endpoint
5. **Admin API:** Create topics at runtime
6. **Client libraries:** Python, Go, Rust clients
7. **TLS support:** Encrypted connections
8. **Write batching:** Group fsync calls
9. **Consumer rebalancing:** Automatic partition assignment
10. **Replication:** Multi-broker setup (major effort)

**Priority ranking (if building production system):**
1. Offset persistence (critical for consumer restart)
2. Log retention (prevent disk exhaustion)
3. Metrics (operational visibility)
4. TLS (security baseline)
5. Replication (high availability)

**Estimated total effort:** 6-12 months of development, extensive testing.

## Disclaimer

**RustLog is an educational project, not a production system.**

**Do NOT use RustLog for:**
- ❌ Production workloads
- ❌ Financial transactions
- ❌ Healthcare data
- ❌ Personally identifiable information (PII)
- ❌ Any use case requiring high availability

**Use at your own risk. No warranties or guarantees provided.**

## Final Thoughts

The limitations listed here are **intentional design decisions**, not bugs or oversights. RustLog demonstrates core log-based system concepts without the complexity of a full-featured broker.

By understanding what RustLog **doesn't do**, you can better appreciate what Apache Kafka and similar systems **do** provide, and the engineering effort required to build production-grade distributed systems.

If you need features beyond RustLog's scope, consider it a learning tool and a stepping stone to understanding real-world log brokers.
