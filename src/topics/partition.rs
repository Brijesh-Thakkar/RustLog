use crate::error::BrokerError;
use crate::storage::segment::{Record, Segment};
use crate::storage::index::Index;
use std::sync::{Arc, Mutex};

/// Result of a fetch operation on a partition.
/// 
/// Contains the records fetched and the offset to continue from.
#[derive(Debug)]
pub struct FetchResult {
    /// Records fetched from the partition, ordered by offset.
    pub records: Vec<Record>,
    /// Next offset to fetch from.
    /// - If records were returned: last_record.offset + 1
    /// - If no records: the start_offset that was requested
    pub next_offset: u64,
}

/// A Partition represents one ordered log stream for a topic.
/// 
/// Responsibilities:
/// - Route read requests across multiple segments
/// - Maintain segment ordering by base_offset
/// - Enforce max_bytes across segment boundaries
/// - Choose between sealed vs active segments
/// 
/// NOT responsible for:
/// - Writing records (Segment handles that)
/// - Sealing segments (higher layer decides)
/// - Decoding record payloads (caller's job)
/// 
/// ## Segment organization:
/// ```ignore
/// sealed_segments: [(seg0, idx0), (seg1, idx1), ...]  (immutable, base_offset ascending)
/// active_segment: (seg3, idx3)                         (mutable, highest offsets)
/// ```
/// 
/// Offsets are globally monotonic within the partition.
/// 
/// ## Read flow:
/// - Sealed segments are read via mmap (zero-copy)
/// - Active segment is NOT read in this MVP phase
/// - Reads span segments sequentially until max_bytes exhausted
/// 
/// ## Thread safety:
/// - Partition is immutable for reads (fetch is &self)
/// - Segments are wrapped in Arc for sharing
/// - Indexes need &mut for seek operations, so wrapped in Arc<Mutex<Index>>
pub struct Partition {
    /// Partition ID within the topic.
    pub id: u32,
    
    /// Sealed segments with their indexes, ordered by base_offset ascending.
    /// These segments are immutable and safe to read concurrently.
    /// Indexes require &mut for seek, so wrapped in Mutex.
    pub sealed_segments: Vec<(Arc<Segment>, Arc<Mutex<Index>>)>,
    
    /// Active segment with its index (highest offsets).
    /// Wrapped in Mutex to allow mutable access for appends.
    pub active_segment: (Arc<Mutex<Segment>>, Arc<Mutex<Index>>),
}

impl Partition {
    /// Fetch records from the partition starting at a given offset.
    /// 
    /// This is the core read path for the partition layer:
    /// 1. Find the first sealed segment containing start_offset
    /// 2. Read from that segment with max_bytes limit
    /// 3. If max_bytes not exhausted, read from subsequent segments
    /// 4. Stop when max_bytes reached or no more sealed segments
    /// 
    /// ## Ordering guarantees:
    /// - Records are returned in offset order (monotonically increasing)
    /// - No duplicates (each offset appears at most once)
    /// - No gaps (if offset N exists, all offsets < N exist)
    /// 
    /// ## max_bytes semantics:
    /// - Soft limit: we may exceed it slightly (one record past the limit)
    /// - Applied across all segments in the fetch
    /// - Caller should size appropriately (e.g., 1MB)
    /// 
    /// ## Errors:
    /// - start_offset < partition.base_offset → OffsetOutOfRange
    /// - Any segment read error bubbles up
    /// 
    /// ## Future work (NOT in this MVP):
    /// - Reading from active segment (requires coordination with writes)
    /// - Timeout handling (needs async context)
    /// - Zero-copy to network buffer (needs integration with protocol layer)
    /// 
    /// # Arguments
    /// - `start_offset`: Logical offset to begin reading from (inclusive)
    /// - `max_bytes`: Maximum bytes to return (soft limit)
    /// 
    /// # Returns
    /// FetchResult containing:
    /// - records: Vec of records fetched (may be empty)
    /// - next_offset: Offset to continue fetching from
    /// 
    /// # Errors
    /// - OffsetOutOfRange if start_offset is below partition's base offset
    /// - StorageError if segment read fails
    pub fn fetch(&self, start_offset: u64, max_bytes: usize) -> Result<FetchResult, BrokerError> {
        // Get the partition's base offset (lowest offset across all segments)
        let partition_base_offset = self.base_offset();
        
        // Validate that start_offset is not before the partition's base
        if start_offset < partition_base_offset {
            return Err(BrokerError::OffsetOutOfRange {
                requested: start_offset,
                base: partition_base_offset,
            });
        }
        
        let mut accumulated_records: Vec<Record> = Vec::new();
        let mut accumulated_bytes: usize = 0;
        let mut current_start_offset = start_offset;
        
        // Iterate through sealed segments in offset order
        for (segment, index) in &self.sealed_segments {
            let segment_base = segment.base_offset();
            let segment_max = segment.max_offset();
            
            // Skip segments that are entirely before our start_offset
            // segment_max < current_start_offset means this segment ends before we want to start
            if segment_max < current_start_offset {
                continue;
            }
            
            // Skip segments that start after our current position
            // (This handles gaps, though in normal Kafka-like systems there shouldn't be gaps)
            if segment_base > current_start_offset {
                // In a real system, this might indicate a gap in the log
                // For now, we just skip to this segment's base
                current_start_offset = segment_base;
            }
            
            // Calculate remaining bytes budget
            let remaining_bytes = max_bytes.saturating_sub(accumulated_bytes);
            if remaining_bytes == 0 {
                // We've hit our byte limit
                break;
            }
            
            // Read from this segment
            // - First segment: start at requested offset
            // - Later segments: start at segment's base_offset
            // Lock the index for this segment (needed for seek operations)
            let mut index_guard = index.lock().unwrap();
            let read_result = segment.read_from_offset(&mut index_guard, current_start_offset, remaining_bytes)
                .map_err(|e| BrokerError::Storage(format!("failed to read segment: {}", e)))?;
            
            // If we got records, accumulate them
            if !read_result.records.is_empty() {
                // Calculate bytes for these records
                // Each record adds: 4 (length) + 8 (offset) + payload.len()
                for record in &read_result.records {
                    accumulated_bytes += 4 + 8 + record.payload.len();
                }
                
                accumulated_records.extend(read_result.records);
                current_start_offset = read_result.next_offset;
            } else {
                // No records returned from this segment
                // Move to next segment's base offset
                current_start_offset = read_result.next_offset;
            }
            
            // If we've exhausted max_bytes, stop
            if accumulated_bytes >= max_bytes {
                break;
            }
        }
        
        // Read from active segment if:
        // 1. We still have bytes budget remaining
        // 2. We've processed all sealed segments (or start_offset is in active range)
        let remaining_bytes = max_bytes.saturating_sub(accumulated_bytes);
        if remaining_bytes > 0 {
            // Lock the active segment for reading
            let active_segment_guard = self.active_segment.0.lock()
                .map_err(|_| BrokerError::LockPoisoned)?;
            let active_base = active_segment_guard.base_offset();
            
            // Determine where to start reading in active segment:
            // - If current_start_offset is within or past active segment: use it
            // - Otherwise: start from active segment's base
            let active_start_offset = std::cmp::max(current_start_offset, active_base);
            
            // Only read if the active segment contains offsets we care about
            // (i.e., active_start_offset is at or after the segment's base)
            if active_start_offset >= active_base {
                // Read from active segment (no index needed, uses sequential scan)
                let active_result = active_segment_guard
                    .read_active_from_offset(active_start_offset, remaining_bytes)
                    .map_err(|e| BrokerError::Storage(format!("failed to read active segment: {}", e)))?;
                
                // Accumulate records from active segment
                if !active_result.records.is_empty() {
                    // Note: We don't update accumulated_bytes or current_start_offset here
                    // because they're not used after this point. The loop is done.
                    accumulated_records.extend(active_result.records);
                }
            }
        }
        
        // Compute next_offset:
        // - If we got records: last record's offset + 1
        // - Otherwise: the start_offset we were given
        let next_offset = if let Some(last_record) = accumulated_records.last() {
            last_record.offset + 1
        } else {
            start_offset
        };
        
        Ok(FetchResult {
            records: accumulated_records,
            next_offset,
        })
    }
    
    /// Get the base offset of this partition.
    /// This is the lowest offset across all segments (sealed and active).
    fn base_offset(&self) -> u64 {
        // The partition's base offset is the base offset of the first sealed segment,
        // or the active segment's base if there are no sealed segments
        self.sealed_segments
            .first()
            .map(|(seg, _idx)| seg.base_offset())
            .unwrap_or_else(|| {
                self.active_segment.0.lock()
                    .map(|seg| seg.base_offset())
                    .unwrap_or(0)
            })
    }
    
    /// Append a record to the partition's active segment.
    /// Returns the assigned offset for the record.
    pub fn append(&self, payload: &[u8]) -> Result<u64, BrokerError> {
        let mut segment = self.active_segment.0.lock()
            .map_err(|_| BrokerError::LockPoisoned)?;
        segment.append(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::index::Index;
    use tempfile::TempDir;

    /// Helper to create a segment with test data.
    /// Returns (segment, index, temp_dir).
    /// temp_dir must be kept alive for the segment to remain valid.
    /// If records is not empty, the segment will be sealed for reading.
    fn create_test_segment(
        base_offset: u64,
        records: Vec<Vec<u8>>,
    ) -> (Arc<Segment>, Arc<Mutex<Index>>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join(format!("{:020}.log", base_offset));
        let index_path = temp_dir.path().join(format!("{:020}.index", base_offset));

        let mut segment = Segment::open(&log_path, base_offset).unwrap();
        let mut index = Index::open(&index_path, base_offset).unwrap();

        // Append records and index them
        for payload in records {
            let offset = segment.append(&payload).unwrap();
            // For testing, we need to track the position manually
            // In production, the segment would handle this internally or via a higher layer
            // For now, we'll compute position based on record format: 4 + 8 + len
            let position = segment.size() - (4 + 8 + payload.len() as u64);
            index.append(offset, position).unwrap();
        }

        // Only seal the segment if it has records (cannot mmap empty files)
        if segment.record_count() > 0 {
            segment.seal().unwrap();
        }

        (Arc::new(segment), Arc::new(Mutex::new(index)), temp_dir)
    }
    
    /// Helper to create an active (mutable) segment for testing.
    fn create_active_segment(
        base_offset: u64,
    ) -> (Arc<Mutex<Segment>>, Arc<Mutex<Index>>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join(format!("{:020}.log", base_offset));
        let index_path = temp_dir.path().join(format!("{:020}.index", base_offset));

        let segment = Segment::open(&log_path, base_offset).unwrap();
        let index = Index::open(&index_path, base_offset).unwrap();

        (Arc::new(Mutex::new(segment)), Arc::new(Mutex::new(index)), temp_dir)
    }

    #[test]
    fn test_fetch_from_single_sealed_segment() {
        // Create a segment with 3 records
        let (segment, index, _temp_dir) = create_test_segment(
            100,
            vec![b"record0".to_vec(), b"record1".to_vec(), b"record2".to_vec()],
        );

        // Create a partition with one sealed segment and a dummy active segment
        let (active_segment, active_index, _active_temp_dir) = create_active_segment(103);

        let partition = Partition {
            id: 0,
            sealed_segments: vec![(segment, index)],
            active_segment: (active_segment, active_index),
        };

        // Fetch from offset 100 with large max_bytes
        let result = partition.fetch(100, 1024).unwrap();

        // Should get all 3 records
        assert_eq!(result.records.len(), 3);
        assert_eq!(result.records[0].offset, 100);
        assert_eq!(result.records[0].payload, b"record0");
        assert_eq!(result.records[1].offset, 101);
        assert_eq!(result.records[1].payload, b"record1");
        assert_eq!(result.records[2].offset, 102);
        assert_eq!(result.records[2].payload, b"record2");
        assert_eq!(result.next_offset, 103);
    }

    #[test]
    fn test_fetch_across_multiple_sealed_segments() {
        // Create 3 sealed segments
        let (seg1, idx1, _temp1) = create_test_segment(
            100,
            vec![b"seg1_rec0".to_vec(), b"seg1_rec1".to_vec()],
        );

        let (seg2, idx2, _temp2) = create_test_segment(
            200,
            vec![b"seg2_rec0".to_vec(), b"seg2_rec1".to_vec()],
        );

        let (seg3, idx3, _temp3) = create_test_segment(
            300,
            vec![b"seg3_rec0".to_vec(), b"seg3_rec1".to_vec()],
        );

        // Create partition
        let (active_segment, active_index, _active_temp) = create_active_segment(400);

        let partition = Partition {
            id: 0,
            sealed_segments: vec![(seg1, idx1), (seg2, idx2), (seg3, idx3)],
            active_segment: (active_segment, active_index),
        };

        // Fetch from offset 100 with large max_bytes (should span all segments)
        let result = partition.fetch(100, 10000).unwrap();

        // Should get all 6 records across 3 segments
        assert_eq!(result.records.len(), 6);
        assert_eq!(result.records[0].offset, 100);
        assert_eq!(result.records[0].payload, b"seg1_rec0");
        assert_eq!(result.records[1].offset, 101);
        assert_eq!(result.records[1].payload, b"seg1_rec1");
        assert_eq!(result.records[2].offset, 200);
        assert_eq!(result.records[2].payload, b"seg2_rec0");
        assert_eq!(result.records[3].offset, 201);
        assert_eq!(result.records[3].payload, b"seg2_rec1");
        assert_eq!(result.records[4].offset, 300);
        assert_eq!(result.records[4].payload, b"seg3_rec0");
        assert_eq!(result.records[5].offset, 301);
        assert_eq!(result.records[5].payload, b"seg3_rec1");
        assert_eq!(result.next_offset, 302);
    }

    #[test]
    fn test_fetch_respects_max_bytes_across_segments() {
        // Create 2 segments with known-size records
        let (seg1, idx1, _temp1) = create_test_segment(
            100,
            vec![b"AAAA".to_vec(), b"BBBB".to_vec()],  // 4 bytes each
        );

        let (seg2, idx2, _temp2) = create_test_segment(
            200,
            vec![b"CCCC".to_vec(), b"DDDD".to_vec()],  // 4 bytes each
        );

        let (active_segment, active_index, _active_temp) = create_active_segment(300);

        let partition = Partition {
            id: 0,
            sealed_segments: vec![(seg1, idx1), (seg2, idx2)],
            active_segment: (active_segment, active_index),
        };

        // Each record is 4 (length) + 8 (offset) + 4 (payload) = 16 bytes
        // Set max_bytes to 32 bytes → should get 2 records
        let result = partition.fetch(100, 32).unwrap();

        // Should get 2 records from seg1 (32 bytes total)
        assert_eq!(result.records.len(), 2);
        assert_eq!(result.records[0].offset, 100);
        assert_eq!(result.records[1].offset, 101);
        assert_eq!(result.next_offset, 102);

        // Fetch more from next_offset
        let result2 = partition.fetch(result.next_offset, 32).unwrap();

        // Should get 2 records from seg2
        assert_eq!(result2.records.len(), 2);
        assert_eq!(result2.records[0].offset, 200);
        assert_eq!(result2.records[1].offset, 201);
        assert_eq!(result2.next_offset, 202);
    }

    #[test]
    fn test_fetch_offset_before_first_segment() {
        // Create a segment starting at offset 100
        let (segment, index, _temp_dir) = create_test_segment(
            100,
            vec![b"record0".to_vec()],
        );

        let (active_segment, active_index, _active_temp) = create_active_segment(101);

        let partition = Partition {
            id: 0,
            sealed_segments: vec![(segment, index)],
            active_segment: (active_segment, active_index),
        };

        // Try to fetch from offset 50 (before partition base of 100)
        let result = partition.fetch(50, 1024);

        // Should get OffsetOutOfRange error
        assert!(result.is_err());
        match result {
            Err(BrokerError::OffsetOutOfRange { requested, base }) => {
                assert_eq!(requested, 50);
                assert_eq!(base, 100);
            }
            _ => panic!("Expected OffsetOutOfRange error"),
        }
    }

    #[test]
    fn test_fetch_offset_between_segments() {
        // Create 2 segments with a gap: [100-101], [200-201]
        let (seg1, idx1, _temp1) = create_test_segment(
            100,
            vec![b"seg1_rec0".to_vec(), b"seg1_rec1".to_vec()],
        );

        let (seg2, idx2, _temp2) = create_test_segment(
            200,
            vec![b"seg2_rec0".to_vec(), b"seg2_rec1".to_vec()],
        );

        let (active_segment, active_index, _active_temp) = create_active_segment(300);

        let partition = Partition {
            id: 0,
            sealed_segments: vec![(seg1, idx1), (seg2, idx2)],
            active_segment: (active_segment, active_index),
        };

        // Fetch from offset 150 (between seg1 and seg2)
        let result = partition.fetch(150, 1024).unwrap();

        // Should start reading from seg2 (offset 200)
        // because we skip past seg1 and find seg2
        assert_eq!(result.records.len(), 2);
        assert_eq!(result.records[0].offset, 200);
        assert_eq!(result.records[0].payload, b"seg2_rec0");
        assert_eq!(result.records[1].offset, 201);
        assert_eq!(result.records[1].payload, b"seg2_rec1");
        assert_eq!(result.next_offset, 202);
    }

    #[test]
    fn test_fetch_from_middle_of_segment() {
        // Create a segment with 5 records
        let (segment, index, _temp_dir) = create_test_segment(
            100,
            vec![
                b"rec0".to_vec(),
                b"rec1".to_vec(),
                b"rec2".to_vec(),
                b"rec3".to_vec(),
                b"rec4".to_vec(),
            ],
        );

        let (active_segment, active_index, _active_temp) = create_active_segment(105);

        let partition = Partition {
            id: 0,
            sealed_segments: vec![(segment, index)],
            active_segment: (active_segment, active_index),
        };

        // Fetch from offset 102 (middle of segment)
        let result = partition.fetch(102, 1024).unwrap();

        // Should get records from 102 onwards
        assert_eq!(result.records.len(), 3);
        assert_eq!(result.records[0].offset, 102);
        assert_eq!(result.records[0].payload, b"rec2");
        assert_eq!(result.records[1].offset, 103);
        assert_eq!(result.records[1].payload, b"rec3");
        assert_eq!(result.records[2].offset, 104);
        assert_eq!(result.records[2].payload, b"rec4");
        assert_eq!(result.next_offset, 105);
    }

    #[test]
    fn test_fetch_with_no_sealed_segments() {
        // Create partition with only active segment (with records)
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("000000000000.log");
        let index_path = temp_dir.path().join("000000000000.index");

        let mut segment = Segment::open(&log_path, 0).unwrap();
        let index = Index::open(&index_path, 0).unwrap();

        // Append some records to active segment (don't seal it)
        segment.append(b"active0").unwrap();
        segment.append(b"active1").unwrap();

        let partition = Partition {
            id: 0,
            sealed_segments: vec![],
            active_segment: (Arc::new(Mutex::new(segment)), Arc::new(Mutex::new(index))),
        };

        // Fetch from offset 0 - should now read from active segment
        let result = partition.fetch(0, 1024).unwrap();

        // Should get records from active segment
        assert_eq!(result.records.len(), 2);
        assert_eq!(result.records[0].offset, 0);
        assert_eq!(result.records[0].payload, b"active0");
        assert_eq!(result.records[1].offset, 1);
        assert_eq!(result.records[1].payload, b"active1");
        assert_eq!(result.next_offset, 2);
    }

    #[test]
    fn test_fetch_reads_from_active_when_sealed_exhausted() {
        // Create a sealed segment
        let (sealed_seg, sealed_idx, _temp1) = create_test_segment(
            100,
            vec![b"sealed0".to_vec(), b"sealed1".to_vec()],
        );

        // Create an active segment (not sealed)
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("000000000200.log");
        let index_path = temp_dir.path().join("000000000200.index");

        let mut active_seg = Segment::open(&log_path, 200).unwrap();
        let active_idx = Index::open(&index_path, 200).unwrap();

        // Append records to active segment (don't seal)
        active_seg.append(b"active0").unwrap();
        active_seg.append(b"active1").unwrap();

        let partition = Partition {
            id: 0,
            sealed_segments: vec![(sealed_seg, sealed_idx)],
            active_segment: (Arc::new(Mutex::new(active_seg)), Arc::new(Mutex::new(active_idx))),
        };

        // Fetch from offset 100 with large max_bytes
        // Should read all sealed records, then continue to active
        let result = partition.fetch(100, 10000).unwrap();

        // Should get 2 from sealed + 2 from active = 4 total
        assert_eq!(result.records.len(), 4);
        assert_eq!(result.records[0].offset, 100);
        assert_eq!(result.records[0].payload, b"sealed0");
        assert_eq!(result.records[1].offset, 101);
        assert_eq!(result.records[1].payload, b"sealed1");
        assert_eq!(result.records[2].offset, 200);
        assert_eq!(result.records[2].payload, b"active0");
        assert_eq!(result.records[3].offset, 201);
        assert_eq!(result.records[3].payload, b"active1");
        assert_eq!(result.next_offset, 202);
    }

    #[test]
    fn test_fetch_reads_across_sealed_and_active() {
        // Create 2 sealed segments
        let (seg1, idx1, _temp1) = create_test_segment(
            100,
            vec![b"seg1_rec0".to_vec(), b"seg1_rec1".to_vec()],
        );

        let (seg2, idx2, _temp2) = create_test_segment(
            200,
            vec![b"seg2_rec0".to_vec(), b"seg2_rec1".to_vec()],
        );

        // Create active segment
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("000000000300.log");
        let index_path = temp_dir.path().join("000000000300.index");

        let mut active_seg = Segment::open(&log_path, 300).unwrap();
        let active_idx = Index::open(&index_path, 300).unwrap();

        active_seg.append(b"active_rec0").unwrap();
        active_seg.append(b"active_rec1").unwrap();

        let partition = Partition {
            id: 0,
            sealed_segments: vec![(seg1, idx1), (seg2, idx2)],
            active_segment: (Arc::new(Mutex::new(active_seg)), Arc::new(Mutex::new(active_idx))),
        };

        // Fetch all records
        let result = partition.fetch(100, 10000).unwrap();

        // Should get all 6 records in order
        assert_eq!(result.records.len(), 6);
        assert_eq!(result.records[0].offset, 100);
        assert_eq!(result.records[1].offset, 101);
        assert_eq!(result.records[2].offset, 200);
        assert_eq!(result.records[3].offset, 201);
        assert_eq!(result.records[4].offset, 300);
        assert_eq!(result.records[4].payload, b"active_rec0");
        assert_eq!(result.records[5].offset, 301);
        assert_eq!(result.records[5].payload, b"active_rec1");
        assert_eq!(result.next_offset, 302);
    }

    #[test]
    fn test_fetch_active_respects_remaining_bytes() {
        // Create a sealed segment
        let (sealed_seg, sealed_idx, _temp1) = create_test_segment(
            100,
            vec![b"AAAA".to_vec(), b"BBBB".to_vec()],  // 4 bytes each
        );

        // Create active segment
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("000000000200.log");
        let index_path = temp_dir.path().join("000000000200.index");

        let mut active_seg = Segment::open(&log_path, 200).unwrap();
        let active_idx = Index::open(&index_path, 200).unwrap();

        active_seg.append(b"CCCC").unwrap();  // 4 bytes
        active_seg.append(b"DDDD").unwrap();  // 4 bytes

        let partition = Partition {
            id: 0,
            sealed_segments: vec![(sealed_seg, sealed_idx)],
            active_segment: (Arc::new(Mutex::new(active_seg)), Arc::new(Mutex::new(active_idx))),
        };

        // Each record is 16 bytes (4 + 8 + 4)
        // Set max_bytes to 48 bytes → should get 3 records total
        // 2 from sealed (32 bytes) + 1 from active (16 bytes) = 48 bytes
        let result = partition.fetch(100, 48).unwrap();

        assert_eq!(result.records.len(), 3);
        assert_eq!(result.records[0].offset, 100);
        assert_eq!(result.records[1].offset, 101);
        assert_eq!(result.records[2].offset, 200);
        assert_eq!(result.records[2].payload, b"CCCC");
        assert_eq!(result.next_offset, 201);
    }

    #[test]
    fn test_fetch_starting_in_active_only() {
        // Create sealed segments that end before our start offset
        let (seg1, idx1, _temp1) = create_test_segment(
            100,
            vec![b"old0".to_vec(), b"old1".to_vec()],
        );

        // Create active segment at higher offset range
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("000000000200.log");
        let index_path = temp_dir.path().join("000000000200.index");

        let mut active_seg = Segment::open(&log_path, 200).unwrap();
        let active_idx = Index::open(&index_path, 200).unwrap();

        active_seg.append(b"new0").unwrap();
        active_seg.append(b"new1").unwrap();
        active_seg.append(b"new2").unwrap();

        let partition = Partition {
            id: 0,
            sealed_segments: vec![(seg1, idx1)],
            active_segment: (Arc::new(Mutex::new(active_seg)), Arc::new(Mutex::new(active_idx))),
        };

        // Fetch starting at offset 200 (start of active segment)
        let result = partition.fetch(200, 1024).unwrap();

        // Should only get records from active segment
        assert_eq!(result.records.len(), 3);
        assert_eq!(result.records[0].offset, 200);
        assert_eq!(result.records[0].payload, b"new0");
        assert_eq!(result.records[1].offset, 201);
        assert_eq!(result.records[1].payload, b"new1");
        assert_eq!(result.records[2].offset, 202);
        assert_eq!(result.records[2].payload, b"new2");
        assert_eq!(result.next_offset, 203);
    }

    #[test]
    fn test_fetch_start_offset_after_all_segments() {
        // Create sealed segment
        let (seg1, idx1, _temp1) = create_test_segment(
            100,
            vec![b"rec0".to_vec(), b"rec1".to_vec()],
        );

        // Create active segment
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("000000000200.log");
        let index_path = temp_dir.path().join("000000000200.index");

        let mut active_seg = Segment::open(&log_path, 200).unwrap();
        let active_idx = Index::open(&index_path, 200).unwrap();

        active_seg.append(b"active0").unwrap();
        active_seg.append(b"active1").unwrap();

        let partition = Partition {
            id: 0,
            sealed_segments: vec![(seg1, idx1)],
            active_segment: (Arc::new(Mutex::new(active_seg)), Arc::new(Mutex::new(active_idx))),
        };

        // Fetch from offset 300 (after all segments)
        let result = partition.fetch(300, 1024).unwrap();

        // Should get no records (offset is beyond any data)
        assert_eq!(result.records.len(), 0);
        assert_eq!(result.next_offset, 300);
    }
}

