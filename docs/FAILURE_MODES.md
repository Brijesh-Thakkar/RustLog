# RustLog Failure Modes

**Audience:** Systems engineers responsible for operating, monitoring, and troubleshooting RustLog deployments.

## Failure Mode Taxonomy

RustLog has **three failure domains**:
1. **Client failures**: Network, protocol, or client application issues
2. **Storage failures**: Disk, filesystem, or corruption issues  
3. **Broker failures**: Process crash, OOM, or resource exhaustion

Each domain has **distinct failure characteristics** and **different recovery procedures**.

## Client Failure Domain

### Connection Failures

#### TCP Connection Drop

**Manifestation**: Client socket disconnects unexpectedly.

**Broker behavior**:
- Connection handler task terminates silently
- No cleanup required (stateless connections)
- No impact on other connections or broker state
- No offset rollback or transaction abort

**Client impact**:
- In-flight requests lost (no response received)
- Client must reconnect and retry failed operations
- Consumer group offsets preserved (explicit commits only)

**Guarantees**:
- **No data loss**: Committed records remain durable
- **No duplicate prevention**: Client may retry and create duplicates
- **Offset preservation**: Committed offsets survive disconnection

#### Protocol Violations

**Manifestation**: Client sends malformed frames or invalid requests.

**Detection**:
```rust
// Frame size exceeds maximum
if frame_len > MAX_FRAME_SIZE {
    return Err(ProtocolError::FrameTooLarge);
}

// Invalid request type byte
match request_type {
    0x01..=0x0A => { /* valid */ },
    _ => return Err(ProtocolError::UnknownRequestType),
}
```

**Broker behavior**:
- Send error response to client
- Log protocol violation for monitoring
- Keep connection alive for subsequent requests
- No broker state corruption

**Error responses**:
- `ProtocolError { message: "Frame too large", details: ... }`
- `ProtocolError { message: "Unknown request type", details: ... }`
- `ProtocolError { message: "Malformed JSON payload", details: ... }`

**Recovery**: Client fixes protocol implementation and retries.

#### Request Timeout

**Manifestation**: Client stops reading responses but keeps connection open.

**Broker behavior**:
- Connection handler blocks on write indefinitely
- TCP backpressure eventually triggers connection failure
- No explicit timeout enforcement (relies on OS TCP timeouts)
- Connection resources held until OS timeout

**Resource impact**: One thread blocked per hung connection.

**Monitoring**: Track connections with long response times.

### Application-Level Failures

#### Invalid Topic/Partition Requests

**Manifestation**: Client requests non-existent resources.

```rust
// Topic not found
Request::Produce { topic: "nonexistent", .. }
→ Response::Error { code: 404, message: "Topic 'nonexistent' not found" }

// Partition not found  
Request::Fetch { topic: "logs", partition: 999, .. }
→ Response::Error { code: 404, message: "Partition 999 not found in topic 'logs'" }
```

**Broker behavior**:
- Return error response immediately
- No side effects (no partial state changes)
- Connection remains healthy

**Client recovery**: Verify topic/partition exists via admin API.

#### Offset Out of Range

**Manifestation**: Consumer requests data from invalid offset.

```rust
Request::Fetch { offset: 50000, .. }  // Only 1000 records exist
→ Response::Error { code: 416, message: "Offset 50000 out of range [0, 1000)" }
```

**Broker guarantees**:
- Return exact valid range in error message
- No data corruption or inconsistency
- Consumer can adjust offset and retry

**Client recovery strategies**:
- Reset to earliest: `offset = 0`
- Reset to latest: `offset = high_watermark - 1`
- Resume from last known good offset

## Storage Failure Domain

### Disk Space Exhaustion

**Detection**:
```rust
// During record append
match segment.append(record_bytes) {
    Err(io::Error { kind: WriteZero, .. }) => {
        return Err(BrokerError::InsufficientSpace);
    }
}
```

**Broker behavior**:
- **Writes fail immediately**: No partial writes, no data corruption
- **Reads continue normally**: Existing data remains accessible
- **Admin operations blocked**: Cannot create new topics/partitions

**Client impact**:
- Produce requests return `InsufficientSpace` error
- Fetch requests succeed for existing data
- Offset commits succeed (in-memory only)

**Recovery procedures**:
1. **Immediate**: Free disk space (delete old segments, logs, tmp files)
2. **Short-term**: Implement retention policy to auto-delete old data
3. **Long-term**: Add storage capacity or partition data across nodes

**Monitoring**:
- Disk usage percentage
- Failed write requests per second
- Available space alerts at 85% and 95%

### File Corruption

#### Segment File Corruption

**Detection**: Occurs during mmap-based reads.

```rust
// Incomplete record header
if segment_data.len() < 8 {
    return Err(BrokerError::CorruptedData("Truncated record header"));
}

// Invalid record length
let record_len = read_u32_be(&segment_data[4..8]);
if record_len > MAX_RECORD_SIZE {
    return Err(BrokerError::CorruptedData("Invalid record length"));
}
```

**Broker behavior**:
- Return error for affected fetch requests
- Mark segment as corrupted (refuse further reads)
- Continue serving other segments normally
- Log corruption details for investigation

**Client impact**:
- Specific fetch ranges return corruption errors
- Other partitions/segments unaffected
- Client must handle gaps in data

**Recovery**: Manual intervention required (restore from backup or accept data loss).

#### Index File Corruption

**Detection**: Automatic corruption detection during lookup.

```rust
// Index entry points beyond segment end
if position >= segment.file_size() {
    return Err(IndexCorruption::PositionOutOfRange);
}

// Index entries not monotonically increasing
if entry.offset <= previous_entry.offset {
    return Err(IndexCorruption::NonMonotonic);
}
```

**Broker behavior**:
- **Automatic recovery**: Rebuild index from segment data
- Block reads during rebuild (30-60 seconds typical)
- Resume normal operation after successful rebuild
- Log corruption and recovery for monitoring

**Client impact**:
- Temporary fetch failures during rebuild
- Automatic recovery without data loss
- No client action required

**Recovery guarantee**: Index rebuild is **deterministic** and **crash-safe**.

### Filesystem Issues

#### Permission Errors

**Manifestation**: Process lacks read/write permissions on data directory.

**Detection**:
```rust
// Cannot create segment file
std::fs::File::create(&segment_path)?;
// → Error: Permission denied (os error 13)
```

**Broker behavior**:
- Startup fails with clear error message
- No partial initialization (fail-fast principle)
- No data corruption risk

**Recovery**: Fix filesystem permissions and restart broker.

#### Directory Corruption

**Manifestation**: Filesystem metadata corruption affecting data directory.

**Broker behavior**:
- Startup fails during partition discovery
- Log specific filesystem errors for diagnosis
- No automatic recovery (requires operator intervention)

**Recovery**: Filesystem repair tools (`fsck`) or restore from backup.

## Broker Failure Domain

### Process Crash

#### Segmentation Fault / Panic

**Causes**:
- Memory access violations (unlikely in safe Rust)
- Explicit `panic!()` from assertion failures
- Stack overflow from infinite recursion

**Crash safety guarantees**:
- **No data loss**: All writes use `fsync()` for durability
- **No corruption**: Append-only log structure is crash-consistent
- **Fast recovery**: Restart reads existing segments without repair

**Recovery procedure**:
1. **Automatic restart** (via systemd, supervisor, or container orchestration)
2. **Partition discovery**: Scan data directory, rebuild in-memory structures
3. **Index validation**: Verify/rebuild corrupted indexes automatically
4. **Resume operations**: Accept new connections immediately

**Data preservation**:
- **Log segments**: Fully preserved (durable writes)
- **Index files**: Automatically rebuilt if corrupted
- **Consumer offsets**: **LOST** (in-memory only)

#### Out of Memory (OOM)

**Causes**:
- Too many concurrent connections
- Memory leak in application code
- OS memory pressure from other processes

**Prevention**:
```rust
// Connection limiting
if active_connections >= MAX_CONNECTIONS {
    tcp_stream.shutdown(Shutdown::Both)?;
    return Ok(());
}

// Memory monitoring
if resident_memory() > MEMORY_LIMIT {
    log::warn!("Memory usage high: {} MB", resident_memory() / 1024 / 1024);
}
```

**Failure behavior**:
- OS kills process (SIGKILL)
- Same crash recovery as above
- Consumer offsets lost

**Prevention**:
- Monitor memory usage trends
- Set connection limits appropriate to available RAM
- Configure OS memory limits (`ulimit -m`)

### Resource Exhaustion

#### File Descriptor Exhaustion

**Manifestation**: Cannot accept new TCP connections.

```rust
// Accept new connection
match tcp_listener.accept().await {
    Err(e) if e.kind() == io::ErrorKind::Other => {
        // Likely "Too many open files"
        log::error!("Connection accept failed: {}", e);
    }
}
```

**Broker behavior**:
- Refuse new connections (clients see connection refused)
- Existing connections continue operating normally
- No data corruption or loss

**Recovery**:
- Wait for connections to close naturally
- Or restart broker to reset file descriptor count
- Increase `ulimit -n` for higher limits

**Prevention**: Monitor connection count trends, set appropriate limits.

#### CPU Exhaustion

**Manifestation**: High CPU utilization, slow response times.

**Causes**:
- Too many concurrent connections for available CPU cores
- CPU-intensive operations (large fetches, index rebuilds)
- Inefficient client patterns (many small requests)

**Broker behavior**:
- Slower response times (requests still complete)
- No data corruption or loss
- Fairness issues between connections

**Mitigation**:
- Connection rate limiting
- Request size limits
- Client education (batching recommendations)

## Cross-Domain Failures

### Cascading Failures

#### Storage Full → Connection Failures

**Sequence**:
1. Disk space exhausted
2. All produce requests fail
3. Clients retry aggressively
4. Connection count spikes
5. File descriptor exhaustion
6. New connections refused

**Prevention**: Circuit breaker pattern in clients, exponential backoff.

#### Index Rebuild → Client Timeouts

**Sequence**:
1. Index corruption detected
2. Partition blocked during rebuild (60+ seconds)
3. Client requests timeout
4. Clients reconnect and retry
5. Thundering herd of retries

**Prevention**: Client timeout tuning, retry jitter.

## Failure Monitoring

### Critical Metrics

**Connection health**:
- Active connection count
- Connection accept failures
- Protocol error rate per connection

**Storage health**:
- Disk usage percentage
- Write failure rate
- Index rebuild frequency

**Broker health**:
- Memory usage trend
- CPU utilization
- Request latency distribution

### Alert Thresholds

**Immediate action required**:
- Disk usage > 95%
- Memory usage > 90%
- Write failure rate > 1%

**Warning levels**:
- Disk usage > 85%
- Connection count > 80% of limit
- Request latency P99 > 100ms

## Operational Runbooks

### Disk Space Emergency

1. **Check space**: `df -h /data`
2. **Find large files**: `du -sh /data/topics/*/partitions/*/* | sort -hr | head -20`
3. **Safe cleanup**: Delete old segments (check offset positions first)
4. **Monitor**: Watch write success rate recovery

### Index Corruption

1. **Identify affected partition**: Check logs for corruption messages
2. **Trigger rebuild**: Restart broker (automatic rebuild on startup)
3. **Monitor progress**: Watch for "Index rebuild complete" log message
4. **Verify**: Test fetch requests on previously failing offsets

### Memory Leak Investigation

1. **Capture heap dump**: `kill -USR1 <pid>` (if implemented)
2. **Monitor trends**: Memory usage over 24 hours
3. **Check connections**: Active connection count vs memory usage
4. **Rolling restart**: If memory growth confirmed

## Testing Failure Modes

### Chaos Engineering

**Disk failures**: Fill filesystem during testing
**Network failures**: Drop TCP connections randomly  
**Memory pressure**: Limit available memory with cgroups
**Clock skew**: Test with incorrect system time

### Failure Injection

```rust
#[cfg(test)]
mod failure_tests {
    #[test]
    fn test_disk_full_during_write() {
        // Fill disk to capacity
        // Attempt produce request
        // Verify error response and no corruption
    }
    
    #[test] 
    fn test_index_corruption_recovery() {
        // Corrupt index file deliberately
        // Trigger read operation
        // Verify automatic rebuild
    }
}
```

## Summary

RustLog's failure modes are **well-defined** and **deterministic**:

- **Storage failures**: Fail-stop semantics, no silent corruption
- **Client failures**: Isolated impact, no cross-contamination  
- **Broker failures**: Fast recovery, preserved data integrity

The single-node design **eliminates distributed failure complexity** while providing **clear recovery procedures** for each failure domain. Every failure mode has been **explicitly designed** and **tested** to ensure predictable behavior.

**Key insight**: Simplicity enables comprehensive failure mode analysis. With fewer moving parts, every failure scenario can be enumerated, tested, and documented.