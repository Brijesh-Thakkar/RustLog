# Offset Management

This document describes how RustLog tracks consumer offsets for consumer groups, enabling at-least-once delivery semantics.

## Overview

RustLog implements **broker-managed offset storage** where:
- **Consumers own their offsets** (choose when to commit)
- **Broker stores offsets in memory** (not persisted to disk)
- **No automatic commits** (manual commit only)
- **At-least-once delivery** (consumer processes before committing)

**Key insight:** Offsets are treated as **consumer state**, not log state. The log doesn't care where consumers are reading; it only stores records.

## Consumer Groups

### What is a Consumer Group?

A **consumer group** is an identifier that allows multiple consumers to coordinate consumption of a topic partition.

**Properties:**
- **Group ID:** String identifier (e.g., "order-processor")
- **Partition assignment:** Each partition assigned to one consumer in group
- **Offset tracking:** Broker tracks committed offset per group+topic+partition

**RustLog simplification:** No automatic rebalancing. Application must manually assign partitions to consumers.

### Consumer Group Use Cases

**Use Case 1: Multiple consumers, one partition**
```
Consumer A (group="processors", partition=0) 
  → reads from offset 100, commits 150

Consumer B (same group, same partition) restarts
  → fetches from offset 0, but broker returns from offset 150
```

**Use Case 2: Multiple partitions, one group**
```
Consumer A (group="processors", partition=0) → offset 100
Consumer B (group="processors", partition=1) → offset 200
Consumer C (group="processors", partition=2) → offset 300
```

Each consumer tracks independent offsets for their assigned partition.

**Use Case 3: Multiple groups, one partition**
```
Group "processors" → offset 1000
Group "analytics"  → offset 500
Group "backup"     → offset 0
```

Different consumer groups read the same partition at different rates.

## Offset Semantics

### Logical Offsets

**Definition:** A **logical offset** is a monotonically increasing integer identifying a record's position in a partition.

**Properties:**
- First record in partition: offset = 0
- Second record: offset = 1
- Offsets never decrease, never reused
- Gaps are not allowed (no tombstones in MVP)

**Example:**
```
Partition "orders-0":
  Offset 0:  {"order_id": 1, ...}
  Offset 1:  {"order_id": 2, ...}
  Offset 2:  {"order_id": 3, ...}
  ...
```

### Committed Offset

**Definition:** The **committed offset** is the offset that a consumer has **successfully processed** and acknowledges as durable.

**Semantics:**
- Committed offset = N means "I have processed all records up to and including offset N-1"
- Next fetch should start at offset N (committed offset is exclusive)

**Example:**
```
Consumer fetches offsets 100-104 (5 records)
Consumer processes all 5 records successfully
Consumer commits offset 105

On restart:
  Consumer fetches from offset 105 (not 104)
```

**Why exclusive?** Prevents duplicate processing of the last committed record.

### At-Least-Once Delivery

RustLog guarantees **at-least-once delivery** when consumers follow this pattern:

```
1. Fetch records
2. Process records
3. Commit offset (AFTER processing)
```

**Guarantee:** If consumer crashes between steps 2 and 3:
- Committed offset is old (before processing)
- Consumer re-fetches and re-processes records on restart
- Records are delivered at least once (possibly more)

**Trade-off:** Idempotent processing required (application must handle duplicates).

**What RustLog does NOT guarantee:**
- ❌ Exactly-once delivery (not supported)
- ❌ At-most-once delivery (manual commits prevent this)

## Offset Operations

### 1. OffsetCommit

**Purpose:** Store the current offset for a consumer group.

**Request:**
```json
{
  "OffsetCommit": {
    "topic": "orders",
    "partition": 0,
    "group_id": "order-processor",
    "offset": 1500
  }
}
```

**Broker behavior:**
```rust
pub fn commit(&mut self, 
              group: &str, 
              topic: &str, 
              partition: u32, 
              offset: u64) {
    let key = (group.to_string(), topic.to_string(), partition);
    self.offsets.insert(key, offset);
}
```

**Storage:** In-memory `HashMap<(String, String, u32), u64>` mapping `(group_id, topic, partition) → offset`.

**Persistence:** ❌ NOT persisted to disk. Lost on broker restart.

**Concurrency:** Protected by `Arc<Mutex<OffsetManager>>` (thread-safe).

**Response:**
```json
{
  "OffsetCommit": {
    "success": true
  }
}
```

**Error cases:**
- Topic or partition doesn't exist
- Broker internal error

### 2. OffsetFetch

**Purpose:** Retrieve the committed offset for a consumer group.

**Request:**
```json
{
  "OffsetFetch": {
    "topic": "orders",
    "partition": 0,
    "group_id": "order-processor"
  }
}
```

**Broker behavior:**
```rust
pub fn fetch(&self, 
             group: &str, 
             topic: &str, 
             partition: u32) -> Option<u64> {
    let key = (group.to_string(), topic.to_string(), partition);
    self.offsets.get(&key).copied()
}
```

**Response (offset exists):**
```json
{
  "OffsetFetch": {
    "offset": 1500
  }
}
```

**Response (no committed offset):**
```json
{
  "OffsetFetch": {
    "offset": null
  }
}
```

**Use case:** Consumer restarts and wants to resume from last committed offset.

### 3. Fetch with Consumer Group

**Purpose:** Read records, using committed offset if available.

**Request:**
```json
{
  "Fetch": {
    "topic": "orders",
    "partition": 0,
    "offset": 100,              // Requested offset
    "max_bytes": 1048576,
    "group_id": "order-processor"  // Optional
  }
}
```

**Broker behavior:**
```rust
pub fn handle_fetch(&self, req: FetchRequest) -> Result<ReadResult> {
    let start_offset = if let Some(group_id) = &req.group_id {
        // Check committed offset
        let committed = self.offsets
            .fetch(&group_id, &req.topic, req.partition);
        
        // Use higher of committed vs requested
        committed.unwrap_or(req.offset).max(req.offset)
    } else {
        req.offset  // No consumer group, use requested offset
    };
    
    // Read from partition starting at start_offset
    partition.read_from_offset(start_offset, req.max_bytes)
}
```

**Key logic:** `max(committed_offset, requested_offset)`

**Examples:**

| Requested | Committed | Used | Reason |
|-----------|-----------|------|--------|
| 100 | None | 100 | No committed offset (first fetch) |
| 100 | 150 | 150 | Committed ahead (prevent re-reading) |
| 200 | 150 | 200 | Requested ahead (consumer wants to skip) |

**Response:**
```json
{
  "Fetch": {
    "records": [
      { "offset": 150, "payload": [1, 2, 3] },
      { "offset": 151, "payload": [4, 5, 6] }
    ],
    "next_offset": 152
  }
}
```

**next_offset:** Offset for the next fetch (exclusive).

## Consumer Patterns

### Pattern 1: At-Least-Once Processing

**Goal:** Guarantee every record is processed at least once.

**Implementation:**
```rust
loop {
    // 1. Fetch records
    let response = fetch(topic, partition, offset, max_bytes, group_id);
    
    // 2. Process each record
    for record in response.records {
        process_record(record)?;  // Can fail here (no commit yet)
    }
    
    // 3. Commit AFTER processing
    commit(topic, partition, group_id, response.next_offset)?;
    
    // 4. Update offset for next iteration
    offset = response.next_offset;
}
```

**Crash scenarios:**
- **Crash before commit:** Re-processes records (duplicate delivery)
- **Crash after commit:** No re-processing (safe)

**Requirement:** Application must handle duplicate records (idempotent processing).

### Pattern 2: Batch Processing

**Goal:** Process multiple records before committing.

**Implementation:**
```rust
loop {
    let mut batch = Vec::new();
    
    // Accumulate records
    while batch.len() < BATCH_SIZE {
        let response = fetch(...);
        batch.extend(response.records);
        offset = response.next_offset;
    }
    
    // Process entire batch
    process_batch(&batch)?;
    
    // Commit once after batch
    commit(..., offset)?;
}
```

**Trade-off:** Higher throughput (fewer commits), but more re-processing on crash.

### Pattern 3: No Consumer Group (Stateless)

**Goal:** Read records without tracking offsets (e.g., backfill, audit).

**Implementation:**
```rust
let response = fetch(topic, partition, offset, max_bytes, None);  // No group_id
// Process records (no commit)
```

**Use cases:**
- One-time data export
- Log replay for debugging
- Independent consumers (no coordination)

### Pattern 4: Manual Offset Management

**Goal:** Application stores offsets externally (e.g., in database).

**Implementation:**
```rust
// Load offset from database
let offset = db.load_offset(group_id)?;

// Fetch records
let response = fetch(topic, partition, offset, max_bytes, None);  // No group tracking

// Process and store offset in same transaction
db.transaction(|tx| {
    for record in response.records {
        process_record(tx, record)?;
    }
    tx.store_offset(group_id, response.next_offset)?;
    tx.commit()?;
})?;
```

**Guarantee:** Exactly-once processing (via database transactions).

**Trade-off:** Requires transactional database, more complex implementation.

## Offset Manager Implementation

### Data Structure

```rust
pub struct OffsetManager {
    // Map: (group_id, topic, partition) → committed_offset
    offsets: HashMap<(String, String, u32), u64>,
}
```

**Key:** `(group_id: String, topic: String, partition: u32)`
**Value:** `offset: u64`

**Thread safety:** Wrapped in `Arc<Mutex<OffsetManager>>` for concurrent access.

### API

```rust
impl OffsetManager {
    pub fn new() -> Self {
        OffsetManager {
            offsets: HashMap::new(),
        }
    }
    
    pub fn commit(&mut self, group: &str, topic: &str, partition: u32, offset: u64) {
        let key = (group.to_string(), topic.to_string(), partition);
        self.offsets.insert(key, offset);
    }
    
    pub fn fetch(&self, group: &str, topic: &str, partition: u32) -> Option<u64> {
        let key = (group.to_string(), topic.to_string(), partition);
        self.offsets.get(&key).copied()
    }
}
```

**Simplicity:** No expiration, no cleanup, no persistence.

### Concurrency Model

**Access pattern:**
```rust
// In Connection handler
let offsets = self.offsets.lock().unwrap();
let committed = offsets.fetch(group_id, topic, partition);
drop(offsets);  // Release lock
```

**Lock granularity:** Global lock on entire OffsetManager (acceptable for MVP).

**Production improvement:** Shard offsets by `(topic, partition)` to reduce contention.

## Offset Durability

### Current State (MVP)

**Offsets are stored in memory only:**
- Fast access (no disk I/O)
- Simple implementation (no WAL, no fsync)
- **Lost on broker restart** ❌

**Consequences:**
- Consumers must restart from offset 0 after broker crash
- Or use external offset storage (database, Redis)

**Acceptable for:** Development, testing, ephemeral workloads.

**NOT acceptable for:** Production systems requiring fault tolerance.

### Production Requirements

**For persistent offsets, add:**
1. **Write-ahead log** for offset commits
2. **Periodic snapshots** to disk
3. **Crash recovery** on broker restart

**Alternative:** Use external offset store (e.g., PostgreSQL, Redis).

**Kafka approach:** Offsets are stored in an internal `__consumer_offsets` topic (replicated log).

## Offset Reset Behavior

### Scenario: No Committed Offset

**Consumer fetches for the first time:**
```
fetch(offset=0, group_id="new-group")
  → No committed offset for "new-group"
  → Broker uses requested offset (0)
  → Returns records from beginning
```

**Consumer decides starting offset:**
- `offset=0`: Read from beginning (default)
- `offset=latest`: Read only new records (skip history)

**No automatic offset reset:** Application must choose starting offset.

### Scenario: Offset Out of Range

**Consumer requests offset before first segment:**
```
Partition log: offsets 1000-2000 (older segments deleted)
Consumer: fetch(offset=500, group_id="old-group")
  → Broker returns error: "offset out of range"
```

**Consumer must handle error:**
- Reset to earliest available offset (1000)
- Or skip to latest (2000)

**RustLog does NOT auto-reset** (fail loudly, let application decide).

## Comparison with Kafka

| Feature | Kafka | RustLog |
|---------|-------|---------|
| Offset storage | Replicated log (`__consumer_offsets`) | In-memory HashMap |
| Persistence | Durable (replicated) | ❌ Not persisted |
| Auto-commit | Yes (configurable) | ❌ No auto-commit |
| Rebalancing | Automatic | ❌ Manual assignment |
| Offset reset | Configurable (earliest, latest) | Application chooses |
| Transactional commits | Yes (with producer transactions) | ❌ No transactions |
| Exactly-once | Yes (with idempotent producer) | ❌ At-least-once only |

**RustLog philosophy:** Keep it simple. Consumer owns offsets and commits manually.

## Best Practices

### 1. Commit After Processing

**Good:**
```rust
let records = fetch(...);
process(records)?;        // Can fail here
commit(next_offset)?;     // Commit after success
```

**Bad:**
```rust
let records = fetch(...);
commit(next_offset)?;     // Commit before processing
process(records)?;        // If this fails, records are lost!
```

### 2. Handle Duplicates

**Because at-least-once delivery, application must handle duplicates:**
```rust
fn process_record(record: &Record) {
    if db.already_processed(record.offset) {
        return;  // Skip duplicate
    }
    
    // Process record
    db.insert(record);
    db.mark_processed(record.offset);
}
```

### 3. Use Batch Commits

**Instead of:**
```rust
for record in records {
    process(record)?;
    commit(record.offset + 1)?;  // N commits
}
```

**Do:**
```rust
for record in records {
    process(record)?;
}
commit(last_offset + 1)?;  // 1 commit
```

**Benefit:** Fewer broker round-trips, higher throughput.

### 4. Monitor Lag

**Consumer lag** = (log end offset) - (committed offset)

**High lag indicates:**
- Consumer is slow
- Consumer is down
- Need to scale out (add more consumers)

**RustLog does NOT track lag** (implement in monitoring layer).

## Error Handling

### Commit Failures

**Scenarios:**
- Network error (connection lost)
- Broker unavailable
- Partition doesn't exist

**Client should:**
- Retry commit (idempotent operation)
- Log error and continue (commit is best-effort)
- Or fail and restart (conservative approach)

**RustLog does NOT retry commits automatically.**

### Fetch with Stale Committed Offset

**Scenario:**
```
Committed offset: 5000
Partition log: offsets 10000-11000 (older segments compacted)

Fetch with group_id:
  → Broker tries to use committed offset 5000
  → Error: "offset out of range"
```

**Client should:**
- Catch error
- Reset offset to earliest (10000) or latest (11000)
- Commit new offset

**RustLog does NOT auto-reset** (explicit error, application decides).

## Limitations

1. **No offset persistence** (lost on broker restart)
2. **No automatic rebalancing** (manual partition assignment)
3. **No offset expiration** (old offsets accumulate in memory)
4. **No transactional commits** (no exactly-once)
5. **No offset reset policies** (application must handle)

**These are intentional for MVP scope.**

## Future Enhancements

**If RustLog were to evolve:**
1. **Persistent offsets:** Write to disk, recover on restart
2. **Auto-commit:** Optional automatic commits after fetch
3. **Offset retention:** Expire old offsets after TTL
4. **Rebalancing protocol:** Automatic partition assignment
5. **Exactly-once semantics:** Transactional commits with idempotent producer

**These are NOT implemented in current scope.**

## Summary

RustLog's offset management provides:
- **Broker-managed storage** in memory (not persisted)
- **Manual commits** (no auto-commit)
- **At-least-once delivery** (consumer controls commit timing)
- **Consumer group support** (shared offsets per group+topic+partition)
- **Simple implementation** (HashMap, no WAL)

The design prioritizes simplicity and clarity over advanced features. It is suitable for educational use and systems that can tolerate offset loss on broker restart.
