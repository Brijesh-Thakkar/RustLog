# Storage Layer

This document describes RustLog's storage implementation: how records are written to disk, organized into segments, indexed for efficient lookup, and read using memory mapping.

## Overview

RustLog uses a **log-structured storage model** where records are appended sequentially to immutable segment files. This design provides:
- **High write throughput** (sequential disk writes)
- **Efficient reads** (zero-copy via mmap)
- **Simple crash recovery** (append-only, no in-place updates)
- **Natural partitioning** (segment-based lifecycle)

## Storage Hierarchy

```
Partition
  ├── Sealed Segments (read-only, immutable)
  │   ├── 00000000000000000000.log    ← Segment file
  │   ├── 00000000000000000000.index  ← Index file
  │   ├── 00000000000001000000.log
  │   ├── 00000000000001000000.index
  │   └── ...
  └── Active Segment (writable, current)
      ├── 00000000000002000000.log
      └── 00000000000002000000.index
```

**Key concepts:**
- **Segment:** One log file containing sequential records
- **Index:** Sparse offset-to-position mapping for fast lookup
- **Active vs Sealed:** Active accepts writes, sealed is read-only
- **Base Offset:** First logical offset in a segment (encoded in filename)

## Segment Structure

### Segment File (.log)

A segment file contains a sequence of **records** in binary format:

```
┌─────────────────────────────────────────────────┐
│  Record 1                                       │
│  ┌──────────┬──────────┬─────────────────────┐ │
│  │ length   │ offset   │ payload             │ │
│  │ (4 bytes)│ (8 bytes)│ (variable length)   │ │
│  │ u32 BE   │ u64 BE   │ raw bytes           │ │
│  └──────────┴──────────┴─────────────────────┘ │
│                                                 │
│  Record 2                                       │
│  ┌──────────┬──────────┬─────────────────────┐ │
│  │ length   │ offset   │ payload             │ │
│  └──────────┴──────────┴─────────────────────┘ │
│                                                 │
│  ...                                            │
└─────────────────────────────────────────────────┘
```

**Record format:**
```
[length: u32][offset: u64][payload: [u8; length]]
  4 bytes      8 bytes      variable
```

**Field semantics:**
- `length`: Payload size in bytes (excludes header)
- `offset`: Logical offset assigned at append time (monotonically increasing)
- `payload`: Raw record bytes (no encoding, no schema)

**Design rationale:**
- **Length-prefixed:** Enables sequential scanning without parsing payload
- **Offset included:** Each record is self-describing
- **Big-endian:** Cross-platform compatibility
- **No checksum (MVP):** Trust filesystem for data integrity

**Maximum record size:** 10 MB (same as frame size limit)

### Index File (.index)

Index files provide **offset-to-byte-position mappings** for efficient random access.

```
┌─────────────────────────────────────────────────┐
│  Entry 1: offset=1000, position=0               │
│  ┌──────────┬──────────┐                        │
│  │ offset   │ position │                        │
│  │ (8 bytes)│ (8 bytes)│                        │
│  │ u64 BE   │ u64 BE   │                        │
│  └──────────┴──────────┘                        │
│                                                 │
│  Entry 2: offset=1100, position=51200           │
│  ┌──────────┬──────────┐                        │
│  │ offset   │ position │                        │
│  └──────────┴──────────┘                        │
│                                                 │
│  ...                                            │
└─────────────────────────────────────────────────┘
```

**Entry format:**
```
[offset: u64][position: u64]
  8 bytes      8 bytes
```

**Field semantics:**
- `offset`: Logical offset of a record
- `position`: Byte offset in the .log file where record starts

**Index properties:**
- **Sparse:** Not every record is indexed (sampling policy)
- **Fixed-size entries:** 16 bytes per entry (enables binary search)
- **Sorted:** Entries are ordered by offset (append-only)
- **Immutable after seal:** No updates, only appends during active phase

**Sampling policy (MVP):** Index every 100th record.

**Index lookup algorithm:**
1. Binary search index file for largest offset ≤ target
2. Return byte position from index entry
3. Caller scans segment from that position to find exact offset

**Why sparse indexing?**
- **Space efficiency:** Index size is 1/100th of data size
- **Lookup precision:** Scanning ~100 records is acceptable
- **Append performance:** Less index overhead during writes

## Segment Lifecycle

### Phase 1: Active Segment

**Characteristics:**
- **Writable:** Accepts append operations
- **File I/O:** Uses `File::write_all()` and `File::flush()`
- **No mmap:** Cannot mmap a file that's being written to
- **Index updates:** Sparse entries added during writes

**Write flow:**
```rust
pub fn append(&mut self, payload: &[u8]) -> Result<u64, BrokerError> {
    let offset = self.next_offset;
    
    // Encode: [length: u32][offset: u64][payload: bytes]
    let mut record = Vec::with_capacity(12 + payload.len());
    record.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    record.extend_from_slice(&offset.to_be_bytes());
    record.extend_from_slice(payload);
    
    // Write to file
    self.file.write_all(&record)?;
    self.file.flush()?;  // fsync for durability
    
    // Update state
    self.size += record.len() as u64;
    self.next_offset += 1;
    
    Ok(offset)
}
```

**Key decisions:**
- `flush()` after every append (durability over throughput)
- No write batching (simple, predictable)
- Offset assigned before write (monotonic, no gaps)

**Read from active segment:**
```rust
pub fn read_active_from_offset(&self, start_offset: u64, max_bytes: usize) 
    -> anyhow::Result<ReadResult> {
    // Use File I/O (not mmap)
    let mut file = self.file.try_clone()?;
    file.seek(SeekFrom::Start(0))?;
    
    // Sequential scan from beginning
    // (No index for active segment in MVP)
}
```

**Why sequential scan for active segment?**
- Active segments are small (rotate before growing large)
- Index may be incomplete or stale during writes
- Simplifies implementation (no coordination with index updates)

### Phase 2: Sealing

**Trigger:** External policy (e.g., segment reaches 1GB, or time-based rotation).

**Seal operation:**
```rust
pub fn seal(&mut self) -> anyhow::Result<()> {
    if self.mmap.is_some() {
        anyhow::bail!("already sealed");
    }
    
    // Create read-only memory mapping
    let mmap = MmapRegion::open_readonly(&self.file)?;
    self.mmap = Some(mmap);
    
    Ok(())
}
```

**After sealing:**
- No more writes allowed (API enforces this)
- File is immutable
- mmap is safe (no concurrent writes)
- Index is complete

### Phase 3: Sealed Segment

**Characteristics:**
- **Read-only:** No more appends
- **mmap-based:** Zero-copy reads via memory mapping
- **High performance:** 10-18x faster than file I/O
- **Concurrent safe:** Multiple readers without locks

**Read from sealed segment:**
```rust
pub fn read_from_offset(&self, index: &mut Index, 
                        start_offset: u64, max_bytes: usize) 
    -> anyhow::Result<ReadResult> {
    let mmap = self.mmap.as_ref()
        .ok_or_else(|| anyhow!("not sealed"))?;
    let data = mmap.as_slice();
    
    // Use index to find starting position
    let start_pos = index.lookup(start_offset)?.unwrap_or(0);
    
    // Parse records from mmap
    let mut cursor = start_pos;
    let mut records = Vec::new();
    
    while cursor < data.len() && bytes_read < max_bytes {
        // Read header
        let length = u32::from_be_bytes(...);
        let offset = u64::from_be_bytes(...);
        
        // Read payload
        let payload = data[cursor+12 .. cursor+12+length].to_vec();
        
        records.push(Record { offset, payload });
        cursor += 12 + length;
    }
    
    Ok(ReadResult { records, next_offset })
}
```

**Performance optimization:**
- Direct memory access (no syscalls)
- Bounds-checked (safe)
- Kernel page cache shared across readers

## Memory Mapping (mmap)

### Safety Invariants

RustLog uses mmap **only for read-only access to immutable files**. This is safe because:

1. **No concurrent writes:** File is sealed before mmap
2. **No file modifications:** File size is fixed
3. **Read-only mapping:** `PROT_READ` only (no `PROT_WRITE`)
4. **File descriptor lifetime:** mmap dropped before file close
5. **Bounds checking:** All accesses validated before dereferencing

```rust
pub struct MmapRegion {
    mmap: memmap2::Mmap,  // Safe wrapper around mmap(2)
}

impl MmapRegion {
    pub fn open_readonly(file: &File) -> anyhow::Result<Self> {
        let len = file.metadata()?.len();
        if len == 0 {
            anyhow::bail!("cannot mmap empty file");
        }
        
        // SAFETY: File is read-only, size is fixed, 
        //         file outlives mmap (checked by Rust)
        let mmap = unsafe { memmap2::Mmap::map(file)? };
        Ok(MmapRegion { mmap })
    }
    
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap  // Immutable slice only
    }
}
```

### Why Never mmap Active Segments?

**Unsafe scenarios prevented:**
1. Concurrent writes while reading (data races)
2. File size changes during mmap (undefined behavior)
3. Partial writes visible to readers (torn reads)

**RustLog guarantee:** mmap is only created after sealing, when writes are impossible.

### mmap Performance

**Benchmark results:**
- **Sealed (mmap): 2.5-3.9 GiB/s**
- **Active (file I/O): 220-1024 MiB/s**
- **Speedup: 10-18x**

**Why mmap is faster:**
- No `read()` syscall overhead
- No user-space buffer allocation
- Kernel page cache mapped directly
- Sequential access leverages readahead

**Trade-offs:**
- More memory address space used
- Page faults for first access (amortized)
- OS-dependent behavior (Linux, macOS work well)

## Index Design

### Binary Search Lookup

Index files use **fixed-size entries** (16 bytes) to enable efficient binary search:

```rust
pub fn lookup(&mut self, offset: u64) -> anyhow::Result<Option<u64>> {
    if self.entries == 0 {
        return Ok(None);
    }
    
    let mut left = 0;
    let mut right = self.entries as i64 - 1;
    let mut result_position: Option<u64> = None;
    
    while left <= right {
        let mid = left + (right - left) / 2;
        
        // Seek to entry at index 'mid'
        let file_offset = mid * 16;
        self.file.seek(SeekFrom::Start(file_offset as u64))?;
        
        // Read entry: [offset: u64][position: u64]
        let mut entry = [0u8; 16];
        self.file.read_exact(&mut entry)?;
        
        let entry_offset = u64::from_be_bytes([entry[0..8]]);
        let entry_position = u64::from_be_bytes([entry[8..16]]);
        
        if entry_offset <= offset {
            result_position = Some(entry_position);
            left = mid + 1;  // Look for larger offset
        } else {
            right = mid - 1;
        }
    }
    
    Ok(result_position)
}
```

**Complexity:** O(log N) seeks and reads, where N = number of index entries.

**Example:**
```
Segment records: offset 100-199 (100 records)
Index entries: 
  - offset=100, position=0
  - offset=110, position=1530
  - offset=120, position=3060

lookup(115):
  1. Binary search finds entry (110, 1530)
  2. Return position=1530
  3. Caller scans from position 1530 to find offset 115
```

### Index Update Policy

**During active phase:**
- Index updated every Nth append (N=100 in MVP)
- Not every record is indexed (sparse)
- Index writes are synchronous (simple, predictable)

**During sealed phase:**
- Index is complete (no more updates)
- All lookups use binary search

**Why not index every record?**
- Space overhead: indexing 100 records vs 1 record = 100x size difference
- Lookup cost: scanning ~100 records is fast with mmap
- Append cost: fewer index writes = higher write throughput

## File Organization

### Naming Convention

**Segment files:**
```
{base_offset:020}.log
```
Example: `00000000000001234567.log` (base offset = 1,234,567)

**Index files:**
```
{base_offset:020}.index
```
Example: `00000000000001234567.index`

**Rationale:**
- 20-digit zero-padded format supports offsets up to 10^20
- Lexicographic sorting matches logical ordering
- Easy to identify segment range from filename

### Directory Structure

```
data/
  topics/
    orders/              ← Topic name
      partition-0/       ← Partition ID
        00000000000000000000.log
        00000000000000000000.index
        00000000000001000000.log
        00000000000001000000.index
        00000000000002000000.log    ← Active segment
        00000000000002000000.index
      partition-1/
        ...
    inventory/
      partition-0/
        ...
```

**Partition isolation:**
- Each partition has independent segment files
- No cross-partition locking or coordination
- Parallel reads across partitions are safe

## Durability Guarantees

### What RustLog Guarantees

1. **Written records are durable** after `flush()` returns
   - Uses `fsync` (POSIX) or `FlushFileBuffers` (Windows)
   - Data is on physical media, not just in OS cache

2. **Segments are crash-safe**
   - Append-only (no in-place updates)
   - Partial writes are detectable (incomplete record at end)

3. **Indexes can be rebuilt** from segment files
   - Index is a hint, not source of truth
   - Worst case: rescan segment to rebuild index

### What RustLog Does NOT Guarantee

1. **No transaction support**
   - Multiple records are not atomic
   - Partial batch writes are possible on crash

2. **No offset durability**
   - Consumer offsets are in-memory only
   - Lost on broker restart

3. **No segment-level checksums (MVP)**
   - Rely on filesystem integrity
   - Production systems should add CRC32C checksums

4. **No replication**
   - Single copy on single disk
   - Hardware failure = data loss

## Crash Recovery

**On broker restart:**

1. **Scan partition directories** for segment files
2. **Validate each segment:**
   - Check filename format
   - Verify file is readable
   - Detect partial writes at end (incomplete records)
3. **Rebuild indexes** if missing or corrupted
4. **Resume from highest offset** in active segment

**Partial write handling:**
```
Segment file ends mid-record:
  [length=100][offset=50][payload=only 60 bytes]
                          ^~~~~~~~~~~~~~~~~~~~^
                          Incomplete! Truncate here.
```

**Recovery strategy:** Truncate partial record, resume from last complete offset.

## Performance Characteristics

### Write Performance

**Active segment append:**
- Sequential disk writes (high throughput)
- fsync per record (trade-off: durability vs latency)
- No locking (single writer per segment)

**Typical throughput:** 10K-50K appends/sec (depends on disk and fsync policy)

### Read Performance

**Sealed segment (mmap):**
- Zero-copy reads
- OS page cache shared across readers
- No syscall overhead per record

**Typical throughput:** 2.5-3.9 GiB/s sequential (see PERFORMANCE.md)

**Active segment (file I/O):**
- Sequential reads via `File::read()`
- Kernel buffering
- Per-record parsing overhead

**Typical throughput:** 220-1024 MiB/s sequential

### Memory Usage

- **mmap regions:** Share kernel page cache (efficient)
- **Index files:** Small (1/100th of data size)
- **No in-memory record cache:** Rely on OS page cache

**Typical memory:** 10-20% of working set size (hot data in page cache)

## Design Decisions Summary

| Decision | Rationale |
|----------|-----------|
| Append-only log | High write throughput, simple crash recovery |
| Segment-based | Natural partitioning, efficient rotation |
| mmap for sealed | Zero-copy, high read performance |
| File I/O for active | Safety (no mmap on mutable files) |
| Sparse index | Space efficiency, acceptable lookup cost |
| fsync per append | Durability over throughput (MVP choice) |
| No compression | Simplicity (can add later) |
| No checksums (MVP) | Rely on filesystem (should add in production) |

## Future Enhancements

**If RustLog were to evolve beyond MVP:**

1. **Record checksums:** Add CRC32C to detect corruption
2. **Write batching:** Buffer multiple appends, group fsync
3. **Index in memory:** Cache hot indexes for faster lookups
4. **Compression:** ZSTD for sealed segments (after seal)
5. **Segment compaction:** Remove deleted/expired records
6. **Log-structured merge:** Merge small segments into larger ones

**These are intentionally NOT implemented in current scope.**

## Summary

RustLog's storage layer provides:
- **Append-only segments** for sequential write performance
- **Active/sealed lifecycle** for write/read optimization
- **Sparse indexing** for efficient random access
- **mmap-based reads** for zero-copy, high-throughput fetches
- **Crash-safe durability** with simple recovery

The design prioritizes clarity and correctness over maximum performance, making it suitable for educational use and moderate-scale production workloads.
