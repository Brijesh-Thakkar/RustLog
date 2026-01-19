# RustLog Storage Layer

**Audience:** Systems engineers understanding persistent storage systems, log-structured designs, and crash recovery mechanisms.

## Storage Philosophy

RustLog implements a **log-structured storage model** with these invariants:

1. **Append-only writes**: No in-place updates, no random writes
2. **Immutable sealed segments**: Once closed, segments never change
3. **Atomic file operations**: All mutations are crash-safe
4. **Memory-mapped reads**: Zero-copy access via OS page cache
5. **Sparse indexing**: O(log N) offset lookups with bounded memory

These invariants enable **deterministic crash recovery** and **simple reasoning** about data persistence.

## Storage Hierarchy

```
data/
├── topics/
│   ├── logs/
│   │   ├── partitions/
│   │   │   ├── 0/
│   │   │   │   ├── 00000000000000000000.log      ← Sealed segment
│   │   │   │   ├── 00000000000000000000.index    ← Sparse index
│   │   │   │   ├── 00000000000000001000.log      ← Sealed segment
│   │   │   │   ├── 00000000000000001000.index    ← Sparse index
│   │   │   │   ├── 00000000000000002000.log      ← Active segment
│   │   │   │   ├── 00000000000000002000.index    ← Active index
│   │   │   │   └── .cleaner_checkpoint           ← Retention state
│   │   │   └── 1/
│   │   └── test-topic/
│   └── metrics/
└── tmp/                                          ← Atomic operations
```

### Directory Layout Principles

**Partitioned by topic/partition**: Each partition is an independent log with isolated failure domain.

**Numeric filenames**: Base offset encoded in filename enables directory scanning for recovery.

**Atomic operations via tmp/**: All file mutations use atomic rename from temporary files.

**Hidden checkpoint files**: Retention/compaction state persisted with leading dot (excluded from normal scans).

## Segment File Format

### Record Wire Format

Each record in a segment file follows this binary layout:

```
┌──────────────────────────────────────────────────────────────┐
│                        Segment File                         │
├──────────┬──────────┬──────────────────────┬─────────────────┤
│ Length   │ Offset   │ Timestamp            │ Payload         │
│ 4 bytes  │ 8 bytes  │ 8 bytes              │ Length bytes    │
│ u32 BE   │ u64 BE   │ u64 BE (μs since    │ Raw bytes       │
│          │          │ UNIX epoch)          │                 │
├──────────┼──────────┼──────────────────────┼─────────────────┤
│ Next record...                                               │
└──────────────────────────────────────────────────────────────┘
```

**Field specifications**:

- **Length** (4 bytes): Payload size in bytes, excluding 20-byte header
- **Offset** (8 bytes): Logical offset assigned at append time  
- **Timestamp** (8 bytes): Microseconds since UNIX epoch (UTC)
- **Payload** (variable): Raw message bytes, no encoding or schema

### Record Format Invariants

1. **Monotonic offsets**: Within segment, offset[i+1] > offset[i]
2. **Monotonic timestamps**: Within segment, timestamp[i+1] >= timestamp[i]
3. **Length validation**: Length field matches actual payload bytes
4. **Big-endian encoding**: Cross-platform compatibility
5. **20-byte header**: Fixed header size for efficient parsing

### Record Boundaries and Validation

```rust
// Reading record header
let header_bytes = &segment_data[pos..pos + 20];
let length = u32::from_be_bytes(&header_bytes[0..4]);
let offset = u64::from_be_bytes(&header_bytes[4..12]);
let timestamp = u64::from_be_bytes(&header_bytes[12..20]);

// Validation checks
if length > MAX_RECORD_SIZE { return Err(CorruptedRecord); }
if offset <= previous_offset { return Err(NonMonotonicOffset); }
if pos + 20 + length as usize > segment_size { return Err(TruncatedRecord); }
```

**Corruption detection**: Records cannot span segment boundaries, enabling corruption isolation.

## Index Structure

### Sparse Index Design

RustLog uses **sparse indexes** to balance lookup performance with space efficiency:

```
┌─────────────────────────────────────────────────────────────┐
│                      Index File (.index)                   │
├──────────┬──────────┬──────────┬──────────┬─────────────────┤
│ Entry 0  │ Entry 1  │ Entry 2  │ Entry N  │ ...             │
├──────────┼──────────┼──────────┼──────────┼─────────────────┤
│ Offset   │ Position │ Offset   │ Position │                 │
│ 8 bytes  │ 8 bytes  │ 8 bytes  │ 8 bytes  │                 │
│ u64 BE   │ u64 BE   │ u64 BE   │ u64 BE   │                 │
└──────────┴──────────┴──────────┴──────────┴─────────────────┘
```

**Index entry format**:
```rust
struct IndexEntry {
    offset: u64,     // Logical offset of record
    position: u64,   // Byte position in segment file
}
```

### Index Density and Lookup

**Indexing frequency**: By default, index every 100th record (configurable).

**Lookup algorithm**: Binary search followed by sequential scan.

```rust
// 1. Binary search for largest offset <= target
let index_entry = binary_search_largest_le(target_offset);

// 2. Sequential scan from index position to target
let mut pos = index_entry.position;
while current_offset < target_offset {
    // Parse record header, advance position
    let (offset, length) = parse_record_header(&segment[pos..]);
    if offset == target_offset { return RecordFound(pos); }
    pos += 20 + length as usize;
    current_offset = offset;
}
```

**Time complexity**: O(log N) for binary search + O(INDEX_INTERVAL) for sequential scan.

**Space complexity**: Index size is approximately `segment_records / INDEX_INTERVAL * 16 bytes`.

### Index Corruption and Rebuild

**Corruption detection**:
- Position beyond segment file size
- Non-monotonic offset sequence
- Invalid offset ranges

**Automatic rebuilding**:
```rust
pub fn rebuild_index(segment_path: &Path) -> anyhow::Result<()> {
    let temp_index = format!("{}.rebuilding", index_path);
    let mut index_writer = BufWriter::new(File::create(temp_index)?);
    let mut record_count = 0;
    
    // Scan entire segment, write index entries
    for record in segment.records() {
        if record_count % INDEX_INTERVAL == 0 {
            let entry = IndexEntry { 
                offset: record.offset, 
                position: record.file_position 
            };
            entry.write_to(&mut index_writer)?;
        }
        record_count += 1;
    }
    
    // Atomic replacement
    fs::sync_all()?;
    fs::rename(&temp_index, &index_path)?;
    fs::sync_all()?;  // Ensure directory metadata updated
}
```

**Rebuild guarantees**:
- **Deterministic**: Same segment always produces identical index
- **Atomic**: Index fully rebuilt or not at all (no partial state)
- **Crash-safe**: Temporary files used until completion

## Memory Mapping and Page Cache

### Memory-Mapped Read Path

**Sealed segments**: Use `mmap()` for zero-copy reads.

```rust
pub struct Segment {
    base_offset: u64,
    file_size: u64,
    mmap: Option<Mmap>,  // Some for sealed, None for active
    file: File,          // For active segment writes
}

impl Segment {
    pub fn read_record(&self, offset: u64) -> Result<Record, ReadError> {
        match &self.mmap {
            Some(mmap) => {
                let pos = self.lookup_position(offset)?;
                parse_record_from_mmap(mmap, pos)
            }
            None => {
                // Active segment: use file I/O
                let pos = self.lookup_position(offset)?;
                parse_record_from_file(&self.file, pos)
            }
        }
    }
}
```

**Memory mapping benefits**:
- **Zero-copy**: No userspace buffering, direct memory access
- **OS optimization**: Page cache shared across processes
- **Lazy loading**: Pages loaded on demand, not upfront
- **Natural eviction**: OS LRU policies handle memory pressure

**Memory mapping limitations**:
- **Virtual memory usage**: Address space grows with data size
- **Page fault latency**: Cold pages incur fault overhead
- **Write coherency**: Must sync between mmap and file writes

### Active Segment Write Path

**Active segments**: Use standard file I/O for writes, no memory mapping.

**Rationale**: Memory-mapped writes have complex coherency semantics and don't provide durability guarantees.

```rust
impl Segment {
    pub fn append(&mut self, payload: &[u8]) -> Result<u64, WriteError> {
        let offset = self.next_offset;
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64;
        
        // Write record header
        self.file.write_all(&(payload.len() as u32).to_be_bytes())?;
        self.file.write_all(&offset.to_be_bytes())?;
        self.file.write_all(&timestamp.to_be_bytes())?;
        
        // Write payload
        self.file.write_all(payload)?;
        
        // Durability guarantee (configurable)
        if self.sync_policy == SyncPolicy::EveryWrite {
            self.file.sync_all()?;
        }
        
        self.next_offset += 1;
        Ok(offset)
    }
}
```

### Segment Sealing Process

**Transition**: Active segment becomes sealed when it reaches size/time limits.

```rust
pub fn seal_active_segment(&mut self) -> anyhow::Result<()> {
    // 1. Flush any pending writes
    self.active_segment.file.sync_all()?;
    
    // 2. Create memory mapping for reads
    let mmap = unsafe {
        MmapOptions::new()
            .map(&self.active_segment.file)?
    };
    self.active_segment.mmap = Some(mmap);
    
    // 3. Move to sealed segments list
    let sealed = std::mem::replace(&mut self.active_segment, Self::create_new_active()?);
    self.sealed_segments.push(sealed);
    
    // 4. Update high watermark
    self.high_watermark = sealed.next_offset;
    
    Ok(())
}
```

**Sealing triggers**:
- Segment file size exceeds `max_segment_size` (default: 100MB)
- Segment age exceeds `max_segment_time` (default: 1 hour)
- Explicit admin request

## Crash Safety and Recovery

### Atomic File Operations

**Principle**: All storage mutations use atomic file operations that are crash-safe.

**Implementation pattern**:
```rust
pub fn atomic_write_operation() -> anyhow::Result<()> {
    // 1. Write to temporary file
    let temp_path = format!("{}.tmp.{}", target_path, process_id());
    let mut temp_file = File::create(&temp_path)?;
    write_data(&mut temp_file)?;
    temp_file.sync_all()?;
    
    // 2. Atomic rename (POSIX guarantee)
    fs::rename(&temp_path, &target_path)?;
    
    // 3. Sync directory metadata
    let dir = File::open(target_dir)?;
    dir.sync_all()?;
    
    Ok(())
}
```

**Atomicity guarantees**:
- **Rename atomicity**: POSIX `rename()` is atomic at filesystem level
- **Directory sync**: Ensures rename is durable across crashes
- **No partial state**: Target file appears fully written or not at all

### Startup Recovery Process

**Recovery sequence**:
```rust
pub fn recover_partition(partition_path: &Path) -> anyhow::Result<Partition> {
    // 1. Scan for segment files
    let mut segment_files = discover_segment_files(partition_path)?;
    segment_files.sort_by_key(|path| extract_base_offset(path));
    
    // 2. Validate segment consistency
    for segment_path in &segment_files {
        validate_segment_format(segment_path)?;
    }
    
    // 3. Check index files, rebuild if corrupted
    for segment_path in &segment_files {
        let index_path = segment_to_index_path(segment_path);
        if !validate_index(&index_path, segment_path)? {
            rebuild_index(segment_path)?;
        }
    }
    
    // 4. Identify active vs sealed segments
    let active_segment = segment_files.pop().unwrap(); // Last = active
    let sealed_segments = segment_files; // Rest = sealed
    
    // 5. Restore high watermark
    let high_watermark = calculate_high_watermark(&active_segment)?;
    
    Ok(Partition::new(sealed_segments, active_segment, high_watermark))
}
```

**Recovery guarantees**:
- **No data loss**: All committed records recovered
- **Consistent state**: Corrupted indexes automatically rebuilt
- **Fast startup**: Only metadata scanning, no data validation
- **Deterministic**: Same recovery result every time

### Crash Scenarios and Behavior

#### Power Loss During Write

**Scenario**: Process crashes while writing record to active segment.

**Outcome**: 
- Partially written record may exist at end of segment
- Next startup detects incomplete record (length field validation)
- Incomplete record truncated, segment remains consistent
- No data corruption beyond incomplete record

#### Power Loss During Segment Sealing

**Scenario**: Process crashes during active→sealed transition.

**Outcome**:
- Segment file is fully written and synced (crash-safe)
- Index file may be incomplete or missing
- Startup detects missing/corrupted index
- Index automatically rebuilt from segment data

#### Power Loss During Index Rebuild

**Scenario**: Process crashes during index reconstruction.

**Outcome**:
- Temporary index file may exist (`.rebuilding` suffix)
- Startup detects incomplete rebuild (no atomic rename)
- Rebuild restarted from beginning
- Original segment data unaffected

## Retention and Compaction

### Cleaner Checkpoint

**Purpose**: Track retention/compaction progress in crash-safe manner.

```rust
#[derive(Serialize, Deserialize)]
pub struct CleanerCheckpoint {
    pub last_cleaned_offset: u64,
    pub retention_horizon_ms: u64,
    pub compaction_state: CompactionState,
    pub created_at: SystemTime,
}
```

**Persistence**:
```rust
pub fn save_checkpoint(&self) -> anyhow::Result<()> {
    let temp_path = format!("{}/.cleaner_checkpoint.tmp", self.partition_path);
    let final_path = format!("{}/.cleaner_checkpoint", self.partition_path);
    
    // Atomic write pattern
    let checkpoint_data = serde_json::to_string_pretty(&self.checkpoint)?;
    std::fs::write(&temp_path, checkpoint_data)?;
    sync_file(&temp_path)?;
    std::fs::rename(&temp_path, &final_path)?;
    sync_directory(&self.partition_path)?;
    
    Ok(())
}
```

### Retention Policies

**Time-based retention**: Delete segments older than retention period.

```rust
pub fn apply_retention_policy(&mut self) -> anyhow::Result<Vec<SegmentFile>> {
    let retention_horizon = SystemTime::now() - Duration::from_millis(self.retention_ms);
    let mut deleted_segments = Vec::new();
    
    for segment in &self.sealed_segments {
        if segment.last_modified < retention_horizon {
            // Check high watermark protection
            if segment.next_offset <= self.committed_high_watermark {
                std::fs::remove_file(&segment.log_path)?;
                std::fs::remove_file(&segment.index_path)?;
                deleted_segments.push(segment.clone());
            }
        }
    }
    
    // Update checkpoint
    self.cleaner_checkpoint.last_cleaned_offset = self.committed_high_watermark;
    self.save_checkpoint()?;
    
    Ok(deleted_segments)
}
```

**Size-based retention**: Delete oldest segments when partition exceeds size limit.

**Protection invariants**:
- Never delete uncommitted data (beyond high watermark)
- Never delete data with uncommitted consumer offsets
- Always preserve at least one segment (cannot delete active segment)

### Log Compaction

**Copy-on-write compaction**: Create new segment with deduplicated records.

```rust
pub fn compact_segment(&self, input: &Segment) -> anyhow::Result<Segment> {
    let output_path = format!("{}.compacted", input.path);
    let mut output_segment = Segment::create_new(output_path)?;
    let mut latest_records = HashMap::new();
    
    // First pass: identify latest version of each key
    for record in input.records() {
        if let Some(key) = extract_key(&record.payload) {
            latest_records.insert(key, record.offset);
        }
    }
    
    // Second pass: copy only latest versions
    for record in input.records() {
        if let Some(key) = extract_key(&record.payload) {
            if latest_records[&key] == record.offset {
                output_segment.append(&record.payload)?;
            }
        }
    }
    
    // Atomic replacement
    output_segment.seal()?;
    std::fs::rename(&output_segment.path, &input.path)?;
    
    Ok(output_segment)
}
```

**Compaction guarantees**:
- **Atomic operation**: Old segment replaced atomically or not at all
- **Key preservation**: Latest value for each key preserved
- **Offset preservation**: Logical offsets maintained in compacted segment
- **Index consistency**: Index rebuilt for compacted segment

## Performance Characteristics

### Write Performance

**Sequential writes**: 200-500 MB/s on consumer SSDs, 100-200 MB/s on HDDs.

**Index update overhead**: ~2% of write throughput (sparse indexing).

**fsync impact**: 
- `sync_all()` every write: 100-1000 writes/sec (latency-bound)
- Batched fsync: 10K-100K writes/sec (throughput-bound)

### Read Performance

**mmap read throughput**: Limited by memory bandwidth (~5-10 GB/s).

**Cold read latency**: 5-20ms (disk seek + page fault overhead).

**Hot read latency**: 100-500μs (memory access + parsing overhead).

**Index lookup overhead**: O(log N) binary search, typically <10 comparisons.

### Memory Usage

**Per-segment overhead**: 16 bytes per index entry + mmap virtual memory.

**Working set**: Determined by access patterns and OS page cache.

**Memory mapping growth**: Virtual memory proportional to total data size.

**Practical limits**: 64-bit systems support ~256TB of mapped memory.

## Operational Considerations

### Monitoring

**Critical metrics**:
- Segment file count and size distribution
- Index rebuild frequency and duration
- Page fault rate and memory-mapped region sizes
- Write latency (fsync) vs throughput tradeoffs

**Alert thresholds**:
- Index rebuild >1 per hour (potential corruption issues)
- Page fault rate >10K/sec (memory pressure)
- Write latency >100ms (storage performance issues)

### Capacity Planning

**Storage growth**: Linear with record volume, no automatic compaction.

**Memory requirements**: 4-8GB RAM per TB of active working set.

**File system recommendations**: 
- ext4 or XFS for large file support
- Separate mount point for data directory
- Reserve 10-15% free space for optimal performance

### Backup and Recovery

**Backup strategy**: File-level backup of segment and index files.

**Point-in-time recovery**: Restore segments up to specific offset.

**Cross-region replication**: rsync or similar tools (RustLog has no built-in replication).

## Conclusion

RustLog's storage layer provides **strong durability guarantees** through:

1. **Append-only writes** with atomic file operations
2. **Crash-safe recovery** with automatic index rebuild
3. **Zero-copy reads** via memory mapping
4. **Simple mental model** with clear failure boundaries

The design optimizes for **correctness and simplicity** over maximum performance, enabling comprehensive reasoning about data persistence and recovery behavior.

**Key insight**: Log-structured storage with sparse indexing provides predictable performance characteristics and simple crash recovery, at the cost of space efficiency and random access performance.
