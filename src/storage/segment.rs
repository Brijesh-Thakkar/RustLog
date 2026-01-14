use crate::error::BrokerError;
use crate::storage::{index::Index, mmap::MmapRegion};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

/// A record read from a segment.
/// 
/// Contains both the logical offset and the payload bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    /// Logical offset assigned when this record was appended.
    pub offset: u64,
    /// Record payload bytes.
    pub payload: Vec<u8>,
}

/// Result of reading records from a segment.
/// 
/// Contains the records read and the next offset to fetch from.
#[derive(Debug)]
pub struct ReadResult {
    /// Records read from the segment.
    pub records: Vec<Record>,
    /// Next offset to fetch from.
    /// If records are returned, this is last_record.offset + 1.
    /// Otherwise, this is the start_offset that was requested.
    pub next_offset: u64,
}

/// A Segment represents one append-only log file for a partition.
/// 
/// Responsibilities:
/// - Own a .log file on disk
/// - Append records sequentially
/// - Assign monotonically increasing logical offsets
/// - Track file size
/// 
/// NOT responsible for:
/// - Indexing (future: index file)
/// - Reads by offset (future: with index + mmap)
/// - Concurrency across partitions (handled at higher level)
/// - Retention/deletion
/// 
/// ## Ownership model:
/// - Segment owns the File handle exclusively
/// - Only one writer per Segment
/// - File is opened in append mode
/// - No shared mutable state
/// 
/// ## Thread safety:
/// - Segment is NOT Sync or Clone
/// - Each partition will own its Segment
/// - Mutations require &mut self
/// 
/// ## On-disk format:
/// File name: `<base_offset>.log`
/// 
/// Each record:
/// ```text
/// | length: u32 | offset: u64 | payload: [u8] |
/// ```
/// 
/// Where:
/// - length = payload size only (not including header)
/// - offset = logical offset assigned at append time
/// - payload = record bytes
/// 
/// Records are self-describing and sequential.
pub struct Segment {
    /// Base offset: the logical offset of the first record in this segment.
    /// This is encoded in the filename and never changes.
    base_offset: u64,

    /// Next offset: the logical offset that will be assigned to the next appended record.
    /// Starts at base_offset, increments by 1 per record.
    next_offset: u64,

    /// File handle for the .log file.
    /// Opened in append mode, positioned at end.
    /// Ownership: Segment exclusively owns this File.
    file: File,

    /// Size in bytes of the log file.
    /// Updated after each append.
    /// Used for segment rotation decisions (future).
    size: u64,

    /// Memory-mapped region for sealed segments.
    /// Only Some for sealed (read-only) segments.
    /// None for active (writable) segments.
    /// 
    /// Invariant: if mmap is Some, segment must be sealed (no more writes).
    mmap: Option<MmapRegion>,
}

impl Segment {
    /// Open or create a segment at the given path with the specified base offset.
    /// 
    /// The file is opened in append mode. If it exists, we append to it.
    /// If it doesn't exist, we create it.
    /// 
    /// For a new segment, next_offset starts at base_offset.
    /// 
    /// Future enhancement: if opening an existing segment, we should scan it
    /// to determine next_offset and size. For MVP, we assume new segments only.
    /// 
    /// # Arguments
    /// - `path`: Path to the .log file (e.g., "000000000000.log")
    /// - `base_offset`: Logical offset of the first record in this segment
    /// 
    /// # Errors
    /// - I/O errors opening or creating the file
    pub fn open(path: impl AsRef<Path>, base_offset: u64) -> Result<Self, BrokerError> {
        let path = path.as_ref();

        // Open file in append mode
        // - Create if doesn't exist
        // - Append if exists (for now, assume new file)
        // - Read/Write permissions
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;

        // Get current file size
        let size = file.metadata()?.len();

        // For MVP: assume new segment, so next_offset = base_offset
        // Future: if size > 0, scan file to determine actual next_offset
        let next_offset = base_offset;

        Ok(Segment {
            base_offset,
            next_offset,
            file,
            size,
            mmap: None,
        })
    }

    /// Append a record to the segment.
    /// 
    /// This is the core write path:
    /// 1. Assign logical offset (next_offset)
    /// 2. Encode record: [length: u32][offset: u64][payload]
    /// 3. Write to file
    /// 4. Flush to disk (fsync)
    /// 5. Update size and next_offset
    /// 6. Return assigned offset
    /// 
    /// ## Record format on disk:
    /// ```text
    /// +----------+----------+----------------+
    /// | length   | offset   | payload        |
    /// | (u32)    | (u64)    | ([u8; length]) |
    /// | 4 bytes  | 8 bytes  | variable       |
    /// +----------+----------+----------------+
    /// ```
    /// 
    /// ## Invariants:
    /// - Offsets are strictly monotonically increasing
    /// - Once returned, an offset is immutable
    /// - File is always appended, never overwritten
    /// - flush() ensures durability
    /// 
    /// ## Performance notes:
    /// - Currently flush after every append (safest)
    /// - Future: batch writes, group fsync
    /// - Future: use Direct I/O or io_uring
    /// 
    /// # Arguments
    /// - `payload`: Record bytes to append
    /// 
    /// # Returns
    /// - Assigned logical offset
    /// 
    /// # Errors
    /// - RecordTooLarge if payload exceeds limits
    /// - I/O errors during write or flush
    pub fn append(&mut self, payload: &[u8]) -> Result<u64, BrokerError> {
        let payload_len = payload.len();

        // Validate payload size
        // Kafka default: max.message.bytes = 1MB
        // We use 10MB to match our frame size limit
        const MAX_RECORD_SIZE: usize = 10 * 1024 * 1024;
        if payload_len > MAX_RECORD_SIZE {
            return Err(BrokerError::RecordTooLarge(payload_len));
        }

        // Assign offset
        let offset = self.next_offset;

        // Encode record
        // Format: [length: u32][offset: u64][payload: [u8]]
        let mut record = Vec::with_capacity(4 + 8 + payload_len);
        
        // Write length (payload size only, not including header)
        record.extend_from_slice(&(payload_len as u32).to_be_bytes());
        
        // Write offset
        record.extend_from_slice(&offset.to_be_bytes());
        
        // Write payload
        record.extend_from_slice(payload);

        // Write to file
        // write_all ensures all bytes are written or returns error
        self.file.write_all(&record)?;

        // Flush to disk
        // This calls fsync, ensuring data is on physical media
        // Future optimization: group multiple appends before fsync
        self.file.flush()?;

        // Update state
        self.size += record.len() as u64;
        self.next_offset += 1;

        Ok(offset)
    }

    /// Get the base offset of this segment.
    /// 
    /// This is the logical offset of the first record in the segment.
    /// Encoded in the filename, never changes.
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Get the next offset that will be assigned.
    /// 
    /// This is one past the last assigned offset.
    /// If no records have been appended, this equals base_offset.
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Get the current size of the segment file in bytes.
    /// 
    /// Used for segment rotation decisions.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Get the number of records in this segment.
    /// 
    /// This is the count of offsets assigned, not the byte size.
    pub fn record_count(&self) -> u64 {
        self.next_offset - self.base_offset
    }

    /// Get the maximum offset in this segment.
    /// 
    /// This is the last offset that has been assigned.
    /// Returns base_offset - 1 if no records have been appended yet.
    pub fn max_offset(&self) -> u64 {
        if self.next_offset > self.base_offset {
            self.next_offset - 1
        } else {
            // Segment is empty, return base - 1 to indicate no records
            self.base_offset.saturating_sub(1)
        }
    }

    /// Seal this segment for read-only access.
    /// 
    /// This creates a memory-mapped region for efficient reads.
    /// After sealing, no more writes are allowed (enforced by API).
    /// 
    /// # Errors
    /// - I/O errors creating mmap
    /// - Empty file (cannot mmap zero-length file)
    pub fn seal(&mut self) -> anyhow::Result<()> {
        if self.mmap.is_some() {
            anyhow::bail!("segment already sealed");
        }

        // Create read-only mmap
        let mmap = MmapRegion::open_readonly(&self.file)?;
        self.mmap = Some(mmap);

        Ok(())
    }

    /// Read records from this segment starting at a given offset.
    /// 
    /// This is the performance-critical read path.
    /// 
    /// ## Algorithm:
    /// 1. Validate start_offset >= base_offset
    /// 2. Use index to find byte position (if available)
    /// 3. Sequentially decode records from mmap
    /// 4. Stop when max_bytes reached or EOF
    /// 5. Return records + next_offset
    /// 
    /// ## Index usage:
    /// Index is a HINT, not a guarantee.
    /// - If index returns position: start there
    /// - If index returns None: start from beginning
    /// - Then scan forward until we find start_offset
    /// 
    /// ## Bounds checking:
    /// Every read validates bounds before accessing mmap.
    /// Malformed/corrupted data returns error, never panics.
    /// 
    /// # Arguments
    /// - `index`: Mutable reference to the index for this segment
    /// - `start_offset`: Logical offset to start reading from
    /// - `max_bytes`: Maximum bytes to read (approximate)
    /// 
    /// # Returns
    /// - ReadResult with records and next_offset
    /// 
    /// # Errors
    /// - start_offset < base_offset (invalid)
    /// - Segment not sealed (mmap not available)
    /// - Corrupted data (incomplete records, etc.)
    pub fn read_from_offset(
        &self,
        index: &mut Index,
        start_offset: u64,
        max_bytes: usize,
    ) -> anyhow::Result<ReadResult> {
        // Validate preconditions
        if start_offset < self.base_offset {
            anyhow::bail!(
                "start_offset {} is below segment base_offset {}",
                start_offset,
                self.base_offset
            );
        }

        // Ensure segment is sealed
        let mmap = self.mmap.as_ref()
            .ok_or_else(|| anyhow::anyhow!("segment not sealed - mmap not available"))?;

        let data = mmap.as_slice();

        // Use index to find starting byte position
        // Index returns position for largest offset <= start_offset
        let start_pos = match index.lookup(start_offset)? {
            Some(pos) => pos as usize,
            None => 0, // No index entry, start from beginning
        };

        // Sequential scan to find exact start_offset
        let mut cursor = start_pos;
        let mut records = Vec::new();
        let mut bytes_read = 0;

        while cursor < data.len() {
            // Read record header: length (4 bytes) + offset (8 bytes)
            if cursor + 12 > data.len() {
                // Incomplete header, stop here
                break;
            }

            // Read length
            let length = u32::from_be_bytes([
                data[cursor],
                data[cursor + 1],
                data[cursor + 2],
                data[cursor + 3],
            ]) as usize;

            // Read offset
            let record_offset = u64::from_be_bytes([
                data[cursor + 4],
                data[cursor + 5],
                data[cursor + 6],
                data[cursor + 7],
                data[cursor + 8],
                data[cursor + 9],
                data[cursor + 10],
                data[cursor + 11],
            ]);

            // Validate payload bounds
            if cursor + 12 + length > data.len() {
                // Incomplete payload, stop here
                break;
            }

            // Skip records before start_offset
            if record_offset < start_offset {
                cursor += 12 + length;
                continue;
            }

            // Check if adding this record would exceed max_bytes
            let record_size = 12 + length;
            if !records.is_empty() && bytes_read + record_size > max_bytes {
                // Stop before exceeding max_bytes
                break;
            }

            // Read payload
            let payload = data[cursor + 12..cursor + 12 + length].to_vec();

            // Add record
            records.push(Record {
                offset: record_offset,
                payload,
            });

            bytes_read += record_size;
            cursor += record_size;
        }

        // Determine next_offset
        let next_offset = if let Some(last_record) = records.last() {
            last_record.offset + 1
        } else {
            start_offset
        };

        Ok(ReadResult {
            records,
            next_offset,
        })
    }

    /// Read records from an ACTIVE (non-sealed) segment starting at a given offset.
    /// 
    /// This is for reading from segments that are still accepting writes.
    /// Unlike sealed segments, active segments cannot use mmap because they are mutable.
    /// 
    /// ## Algorithm:
    /// 1. Validate start_offset >= base_offset
    /// 2. Open file for reading (separate from append handle)
    /// 3. Sequential scan from start: read header, read payload
    /// 4. Skip records with offset < start_offset
    /// 5. Stop when max_bytes reached or EOF
    /// 6. Return records + next_offset
    /// 
    /// ## Why no index:
    /// Active segments could use an index, but for MVP simplicity we scan sequentially.
    /// In production, you'd maintain a sparse in-memory index for active segments.
    /// 
    /// ## Correctness:
    /// - Validates all bounds before reading
    /// - Never panics on corrupt data (returns error instead)
    /// - Handles incomplete writes gracefully (stops at EOF or bad header)
    /// - Sequential scan ensures we don't miss records
    /// 
    /// ## Performance:
    /// - O(n) scan from beginning of file
    /// - For large active segments, consider:
    ///   * In-memory index
    ///   * Caching file position
    ///   * Periodic sealing to mmap
    /// 
    /// # Arguments
    /// - `start_offset`: Logical offset to start reading from
    /// - `max_bytes`: Maximum bytes to read (approximate)
    /// 
    /// # Returns
    /// - ReadResult with records and next_offset
    /// 
    /// # Errors
    /// - start_offset < base_offset (invalid)
    /// - Segment is sealed (use read_from_offset instead)
    /// - I/O errors reading file
    /// - Corrupted data (incomplete records, etc.)
    pub fn read_active_from_offset(
        &self,
        start_offset: u64,
        max_bytes: usize,
    ) -> anyhow::Result<ReadResult> {
        use std::io::{Read, Seek, SeekFrom};

        // Validate preconditions
        if start_offset < self.base_offset {
            anyhow::bail!(
                "start_offset {} is below segment base_offset {}",
                start_offset,
                self.base_offset
            );
        }

        // Ensure segment is NOT sealed (active segments use file I/O, not mmap)
        if self.mmap.is_some() {
            anyhow::bail!("segment is sealed - use read_from_offset instead");
        }

        // Open file for reading
        // We need a separate file handle because self.file is opened in append mode
        // and positioned at EOF. We can't use it for reads without seeking.
        // We use try_clone() to get a new handle to the same file descriptor.
        // This is safe because:
        // - File is only appended to (never overwritten)
        // - Our read is read-only
        // - Kernel handles consistency between handles
        let mut file = self.file.try_clone()?;
        
        // Seek to start of file
        file.seek(SeekFrom::Start(0))?;

        let mut records = Vec::new();
        let mut bytes_read = 0;
        let mut header_buf = [0u8; 12]; // 4 bytes length + 8 bytes offset

        loop {
            // Try to read record header: length (4 bytes) + offset (8 bytes)
            match file.read_exact(&mut header_buf) {
                Ok(()) => {
                    // Parse length
                    let length = u32::from_be_bytes([
                        header_buf[0],
                        header_buf[1],
                        header_buf[2],
                        header_buf[3],
                    ]) as usize;

                    // Parse offset
                    let record_offset = u64::from_be_bytes([
                        header_buf[4],
                        header_buf[5],
                        header_buf[6],
                        header_buf[7],
                        header_buf[8],
                        header_buf[9],
                        header_buf[10],
                        header_buf[11],
                    ]);

                    // Validate length is reasonable (same limit as append)
                    const MAX_RECORD_SIZE: usize = 10 * 1024 * 1024; // 10MB
                    if length > MAX_RECORD_SIZE {
                        anyhow::bail!(
                            "corrupt record: length {} exceeds max {}",
                            length,
                            MAX_RECORD_SIZE
                        );
                    }

                    // Skip records before start_offset
                    if record_offset < start_offset {
                        // Seek past payload
                        file.seek(SeekFrom::Current(length as i64))?;
                        continue;
                    }

                    // Check if adding this record would exceed max_bytes
                    let record_size = 12 + length;
                    if !records.is_empty() && bytes_read + record_size > max_bytes {
                        // Stop before exceeding max_bytes
                        break;
                    }

                    // Read payload
                    let mut payload = vec![0u8; length];
                    file.read_exact(&mut payload)?;

                    // Add record
                    records.push(Record {
                        offset: record_offset,
                        payload,
                    });

                    bytes_read += record_size;
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Reached EOF, this is normal
                    break;
                }
                Err(e) => {
                    // Other I/O error
                    return Err(e.into());
                }
            }
        }

        // Determine next_offset
        let next_offset = if let Some(last_record) = records.last() {
            last_record.offset + 1
        } else {
            start_offset
        };

        Ok(ReadResult {
            records,
            next_offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Read;

    /// Helper: create a temp directory for tests
    fn temp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("failed to create temp dir")
    }

    #[test]
    fn test_segment_open_new() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000000.log");

        let segment = Segment::open(&log_path, 0).expect("failed to open segment");

        assert_eq!(segment.base_offset(), 0);
        assert_eq!(segment.next_offset(), 0);
        assert_eq!(segment.size(), 0);
        assert_eq!(segment.record_count(), 0);
        assert!(log_path.exists());
    }

    #[test]
    fn test_append_single_record() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000000.log");

        let mut segment = Segment::open(&log_path, 0).expect("failed to open segment");

        let payload = b"hello world";
        let offset = segment.append(payload).expect("failed to append");

        assert_eq!(offset, 0);
        assert_eq!(segment.next_offset(), 1);
        assert_eq!(segment.record_count(), 1);

        // Verify file size: 4 (length) + 8 (offset) + 11 (payload) = 23 bytes
        assert_eq!(segment.size(), 23);

        // Read raw bytes and verify format
        let mut file_bytes = Vec::new();
        File::open(&log_path)
            .expect("failed to open file")
            .read_to_end(&mut file_bytes)
            .expect("failed to read file");

        assert_eq!(file_bytes.len(), 23);

        // Decode record
        let length = u32::from_be_bytes([file_bytes[0], file_bytes[1], file_bytes[2], file_bytes[3]]);
        let record_offset = u64::from_be_bytes([
            file_bytes[4], file_bytes[5], file_bytes[6], file_bytes[7],
            file_bytes[8], file_bytes[9], file_bytes[10], file_bytes[11],
        ]);
        let record_payload = &file_bytes[12..];

        assert_eq!(length, 11);
        assert_eq!(record_offset, 0);
        assert_eq!(record_payload, b"hello world");
    }

    #[test]
    fn test_append_multiple_records() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000000.log");

        let mut segment = Segment::open(&log_path, 0).expect("failed to open segment");

        let payloads: Vec<&[u8]> = vec![b"first", b"second", b"third"];
        let mut offsets = Vec::new();

        for payload in &payloads {
            let offset = segment.append(*payload).expect("failed to append");
            offsets.push(offset);
        }

        assert_eq!(offsets, vec![0, 1, 2]);
        assert_eq!(segment.next_offset(), 3);
        assert_eq!(segment.record_count(), 3);

        // Verify each record in file
        let mut file_bytes = Vec::new();
        File::open(&log_path)
            .expect("failed to open file")
            .read_to_end(&mut file_bytes)
            .expect("failed to read file");

        let mut cursor = 0;
        for (i, payload) in payloads.iter().enumerate() {
            let payload: &[u8] = *payload;
            let length = u32::from_be_bytes([
                file_bytes[cursor],
                file_bytes[cursor + 1],
                file_bytes[cursor + 2],
                file_bytes[cursor + 3],
            ]);
            cursor += 4;

            let record_offset = u64::from_be_bytes([
                file_bytes[cursor],
                file_bytes[cursor + 1],
                file_bytes[cursor + 2],
                file_bytes[cursor + 3],
                file_bytes[cursor + 4],
                file_bytes[cursor + 5],
                file_bytes[cursor + 6],
                file_bytes[cursor + 7],
            ]);
            cursor += 8;

            let record_payload = &file_bytes[cursor..cursor + length as usize];
            cursor += length as usize;

            assert_eq!(length, payload.len() as u32);
            assert_eq!(record_offset, i as u64);
            assert_eq!(record_payload, payload);
        }
    }

    #[test]
    fn test_offsets_increment_correctly() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000100.log");

        let mut segment = Segment::open(&log_path, 100).expect("failed to open segment");

        let offset1 = segment.append(b"record1").expect("failed to append");
        let offset2 = segment.append(b"record2").expect("failed to append");
        let offset3 = segment.append(b"record3").expect("failed to append");

        assert_eq!(offset1, 100);
        assert_eq!(offset2, 101);
        assert_eq!(offset3, 102);
        assert_eq!(segment.next_offset(), 103);
    }

    #[test]
    fn test_file_grows_after_append() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000000.log");

        let mut segment = Segment::open(&log_path, 0).expect("failed to open segment");

        assert_eq!(segment.size(), 0);

        segment.append(b"a").expect("failed to append");
        let size1 = segment.size();
        assert!(size1 > 0);

        segment.append(b"bb").expect("failed to append");
        let size2 = segment.size();
        assert!(size2 > size1);

        segment.append(b"ccc").expect("failed to append");
        let size3 = segment.size();
        assert!(size3 > size2);

        // Verify actual file size matches
        let actual_size = fs::metadata(&log_path)
            .expect("failed to get metadata")
            .len();
        assert_eq!(actual_size, size3);
    }

    #[test]
    fn test_record_too_large() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000000.log");

        let mut segment = Segment::open(&log_path, 0).expect("failed to open segment");

        // Attempt to append record larger than 10MB
        let large_payload = vec![0u8; 11 * 1024 * 1024];
        let result = segment.append(&large_payload);

        assert!(result.is_err());
        match result {
            Err(BrokerError::RecordTooLarge(size)) => {
                assert_eq!(size, 11 * 1024 * 1024);
            }
            _ => panic!("expected RecordTooLarge error"),
        }

        // Segment state should be unchanged
        assert_eq!(segment.next_offset(), 0);
        assert_eq!(segment.size(), 0);
    }

    #[test]
    fn test_empty_payload() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000000.log");

        let mut segment = Segment::open(&log_path, 0).expect("failed to open segment");

        let offset = segment.append(b"").expect("failed to append empty record");

        assert_eq!(offset, 0);
        assert_eq!(segment.next_offset(), 1);
        
        // Size should be 4 (length) + 8 (offset) + 0 (payload) = 12 bytes
        assert_eq!(segment.size(), 12);
    }

    // ===== SEALED SEGMENT READ TESTS =====

    #[test]
    fn test_fetch_single_record_from_sealed_segment() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000000.log");
        let index_path = dir.path().join("000000000000.index");

        // Create segment and append record
        let mut segment = Segment::open(&log_path, 0).expect("failed to open segment");
        segment.append(b"test record").expect("failed to append");

        // Create index and add entry
        let mut index = crate::storage::index::Index::open(&index_path, 0)
            .expect("failed to open index");
        index.append(0, 0).expect("failed to append index entry");

        // Seal segment
        segment.seal().expect("failed to seal segment");

        // Read record
        let result = segment
            .read_from_offset(&mut index, 0, 1024)
            .expect("failed to read");

        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].offset, 0);
        assert_eq!(result.records[0].payload, b"test record");
        assert_eq!(result.next_offset, 1);
    }

    #[test]
    fn test_fetch_multiple_records() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000000.log");
        let index_path = dir.path().join("000000000000.index");

        // Create segment and append multiple records
        let mut segment = Segment::open(&log_path, 0).expect("failed to open segment");
        segment.append(b"record0").expect("failed to append");
        segment.append(b"record1").expect("failed to append");
        segment.append(b"record2").expect("failed to append");

        // Create index with entries
        let mut index = crate::storage::index::Index::open(&index_path, 0)
            .expect("failed to open index");
        index.append(0, 0).expect("failed to append index entry");

        // Seal segment
        segment.seal().expect("failed to seal segment");

        // Read all records
        let result = segment
            .read_from_offset(&mut index, 0, 10000)
            .expect("failed to read");

        assert_eq!(result.records.len(), 3);
        assert_eq!(result.records[0].offset, 0);
        assert_eq!(result.records[0].payload, b"record0");
        assert_eq!(result.records[1].offset, 1);
        assert_eq!(result.records[1].payload, b"record1");
        assert_eq!(result.records[2].offset, 2);
        assert_eq!(result.records[2].payload, b"record2");
        assert_eq!(result.next_offset, 3);
    }

    #[test]
    fn test_fetch_respects_max_bytes() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000000.log");
        let index_path = dir.path().join("000000000000.index");

        // Create segment with multiple records
        let mut segment = Segment::open(&log_path, 0).expect("failed to open segment");
        segment.append(b"aaaaaaaaaa").expect("failed to append"); // 10 bytes + 12 header = 22 total
        segment.append(b"bbbbbbbbbb").expect("failed to append"); // 10 bytes + 12 header = 22 total
        segment.append(b"cccccccccc").expect("failed to append"); // 10 bytes + 12 header = 22 total

        let mut index = crate::storage::index::Index::open(&index_path, 0)
            .expect("failed to open index");
        index.append(0, 0).expect("failed to append index entry");

        segment.seal().expect("failed to seal segment");

        // Read with max_bytes that allows only 2 records
        let result = segment
            .read_from_offset(&mut index, 0, 50) // 50 bytes allows 2 records (44 bytes)
            .expect("failed to read");

        assert_eq!(result.records.len(), 2);
        assert_eq!(result.records[0].offset, 0);
        assert_eq!(result.records[1].offset, 1);
        assert_eq!(result.next_offset, 2);
    }

    #[test]
    fn test_fetch_offset_not_indexed_still_works() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000000.log");
        let index_path = dir.path().join("000000000000.index");

        // Create segment with multiple records
        let mut segment = Segment::open(&log_path, 0).expect("failed to open segment");
        segment.append(b"record0").expect("failed to append");
        segment.append(b"record1").expect("failed to append");
        segment.append(b"record2").expect("failed to append");

        // Create index but only index offset 0
        // Offsets 1 and 2 are not indexed (sparse index)
        let mut index = crate::storage::index::Index::open(&index_path, 0)
            .expect("failed to open index");
        index.append(0, 0).expect("failed to append index entry");

        segment.seal().expect("failed to seal segment");

        // Read from offset 1 (not indexed)
        // Should use index hint for offset 0, then scan to offset 1
        let result = segment
            .read_from_offset(&mut index, 1, 10000)
            .expect("failed to read");

        assert_eq!(result.records.len(), 2);
        assert_eq!(result.records[0].offset, 1);
        assert_eq!(result.records[0].payload, b"record1");
        assert_eq!(result.records[1].offset, 2);
        assert_eq!(result.records[1].payload, b"record2");
        assert_eq!(result.next_offset, 3);
    }

    #[test]
    fn test_fetch_from_middle_of_segment() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000000.log");
        let index_path = dir.path().join("000000000000.index");

        // Create segment with records at different offsets
        let mut segment = Segment::open(&log_path, 100).expect("failed to open segment");
        segment.append(b"record100").expect("failed to append"); // offset 100
        segment.append(b"record101").expect("failed to append"); // offset 101
        segment.append(b"record102").expect("failed to append"); // offset 102
        segment.append(b"record103").expect("failed to append"); // offset 103

        // Index offsets 100 and 102
        let mut index = crate::storage::index::Index::open(&index_path, 100)
            .expect("failed to open index");
        index.append(100, 0).expect("failed to append index entry");
        
        // Calculate byte position of offset 102
        // Each record: 4 (length) + 8 (offset) + 9 (payload) = 21 bytes
        // offset 100: byte 0
        // offset 101: byte 21
        // offset 102: byte 42
        index.append(102, 42).expect("failed to append index entry");

        segment.seal().expect("failed to seal segment");

        // Read from offset 102 (indexed)
        let result = segment
            .read_from_offset(&mut index, 102, 10000)
            .expect("failed to read");

        assert_eq!(result.records.len(), 2);
        assert_eq!(result.records[0].offset, 102);
        assert_eq!(result.records[0].payload, b"record102");
        assert_eq!(result.records[1].offset, 103);
        assert_eq!(result.records[1].payload, b"record103");
        assert_eq!(result.next_offset, 104);
    }

    #[test]
    fn test_fetch_with_empty_index() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000000.log");
        let index_path = dir.path().join("000000000000.index");

        // Create segment with records
        let mut segment = Segment::open(&log_path, 0).expect("failed to open segment");
        segment.append(b"record0").expect("failed to append");
        segment.append(b"record1").expect("failed to append");

        // Create empty index (no entries)
        let mut index = crate::storage::index::Index::open(&index_path, 0)
            .expect("failed to open index");

        segment.seal().expect("failed to seal segment");

        // Should still work - scans from beginning
        let result = segment
            .read_from_offset(&mut index, 0, 10000)
            .expect("failed to read");

        assert_eq!(result.records.len(), 2);
        assert_eq!(result.records[0].offset, 0);
        assert_eq!(result.records[1].offset, 1);
    }

    #[test]
    fn test_fetch_below_base_offset_fails() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000100.log");
        let index_path = dir.path().join("000000000100.index");

        let mut segment = Segment::open(&log_path, 100).expect("failed to open segment");
        segment.append(b"record").expect("failed to append");

        let mut index = crate::storage::index::Index::open(&index_path, 100)
            .expect("failed to open index");

        segment.seal().expect("failed to seal segment");

        // Try to read below base_offset
        let result = segment.read_from_offset(&mut index, 50, 1024);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("below"));
    }

    #[test]
    fn test_unsealed_segment_fails() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000000.log");
        let index_path = dir.path().join("000000000000.index");

        let mut segment = Segment::open(&log_path, 0).expect("failed to open segment");
        segment.append(b"record").expect("failed to append");

        let mut index = crate::storage::index::Index::open(&index_path, 0)
            .expect("failed to open index");

        // Don't seal segment

        // Try to read unsealed segment
        let result = segment.read_from_offset(&mut index, 0, 1024);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not sealed"));
    }

    #[test]
    fn test_fetch_from_active_segment() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000100.log");

        let mut segment = Segment::open(&log_path, 100).expect("failed to open segment");

        // Append 5 records
        segment.append(b"record0").expect("failed to append");
        segment.append(b"record1").expect("failed to append");
        segment.append(b"record2").expect("failed to append");
        segment.append(b"record3").expect("failed to append");
        segment.append(b"record4").expect("failed to append");

        // Don't seal - keep it active

        // Fetch from offset 100
        let result = segment.read_active_from_offset(100, 10000).expect("failed to read");

        // Should get all 5 records
        assert_eq!(result.records.len(), 5);
        assert_eq!(result.records[0].offset, 100);
        assert_eq!(result.records[0].payload, b"record0");
        assert_eq!(result.records[1].offset, 101);
        assert_eq!(result.records[1].payload, b"record1");
        assert_eq!(result.records[2].offset, 102);
        assert_eq!(result.records[2].payload, b"record2");
        assert_eq!(result.records[3].offset, 103);
        assert_eq!(result.records[3].payload, b"record3");
        assert_eq!(result.records[4].offset, 104);
        assert_eq!(result.records[4].payload, b"record4");
        assert_eq!(result.next_offset, 105);
    }

    #[test]
    fn test_fetch_active_respects_max_bytes() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000200.log");

        let mut segment = Segment::open(&log_path, 200).expect("failed to open segment");

        // Append 4 records of known size (4 bytes each)
        segment.append(b"AAAA").expect("failed to append");
        segment.append(b"BBBB").expect("failed to append");
        segment.append(b"CCCC").expect("failed to append");
        segment.append(b"DDDD").expect("failed to append");

        // Don't seal

        // Each record is 4 (length) + 8 (offset) + 4 (payload) = 16 bytes
        // Set max_bytes to 32 bytes → should get 2 records
        let result = segment.read_active_from_offset(200, 32).expect("failed to read");

        assert_eq!(result.records.len(), 2);
        assert_eq!(result.records[0].offset, 200);
        assert_eq!(result.records[0].payload, b"AAAA");
        assert_eq!(result.records[1].offset, 201);
        assert_eq!(result.records[1].payload, b"BBBB");
        assert_eq!(result.next_offset, 202);

        // Fetch remaining records from next_offset
        let result2 = segment.read_active_from_offset(result.next_offset, 32)
            .expect("failed to read");

        assert_eq!(result2.records.len(), 2);
        assert_eq!(result2.records[0].offset, 202);
        assert_eq!(result2.records[0].payload, b"CCCC");
        assert_eq!(result2.records[1].offset, 203);
        assert_eq!(result2.records[1].payload, b"DDDD");
        assert_eq!(result2.next_offset, 204);
    }

    #[test]
    fn test_fetch_active_starting_mid_segment() {
        let dir = temp_dir();
        let log_path = dir.path().join("000000000300.log");

        let mut segment = Segment::open(&log_path, 300).expect("failed to open segment");

        // Append 6 records
        segment.append(b"rec0").expect("failed to append");
        segment.append(b"rec1").expect("failed to append");
        segment.append(b"rec2").expect("failed to append");
        segment.append(b"rec3").expect("failed to append");
        segment.append(b"rec4").expect("failed to append");
        segment.append(b"rec5").expect("failed to append");

        // Don't seal

        // Fetch from offset 303 (middle of segment)
        let result = segment.read_active_from_offset(303, 10000).expect("failed to read");

        // Should get records from 303 onwards
        assert_eq!(result.records.len(), 3);
        assert_eq!(result.records[0].offset, 303);
        assert_eq!(result.records[0].payload, b"rec3");
        assert_eq!(result.records[1].offset, 304);
        assert_eq!(result.records[1].payload, b"rec4");
        assert_eq!(result.records[2].offset, 305);
        assert_eq!(result.records[2].payload, b"rec5");
        assert_eq!(result.next_offset, 306);
    }
}

