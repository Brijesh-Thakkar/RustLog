use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

/// An Index maps logical offsets to byte positions in a segment log.
/// 
/// Purpose:
/// - Enable fast offset-based lookups without scanning entire log
/// - The log is the source of truth; the index is a hint
/// 
/// Responsibilities:
/// - Own a `.index` file on disk
/// - Append index entries (offset, position) pairs
/// - Perform binary search lookup by offset
/// - Track number of entries
/// 
/// NOT responsible for:
/// - Reading log data
/// - Memory mapping (mmap)
/// - Segment rolling
/// - Cross-partition coordination
/// 
/// ## On-disk format:
/// File name: `<base_offset>.index`
/// 
/// Each entry is exactly 16 bytes:
/// ```text
/// | offset: u64 | position: u64 |
/// | 8 bytes     | 8 bytes       |
/// ```
/// 
/// Entries are:
/// - Fixed-size (enables binary search via seek)
/// - Append-only
/// - Monotonically increasing by offset
/// 
/// ## Sampling policy:
/// The index does NOT store every record. Typically:
/// - Index every Nth record (e.g., N=100)
/// - Caller decides when to add entry
/// - Trade-off: index size vs lookup precision
/// 
/// ## Lookup behavior:
/// When looking up offset O:
/// - Find largest indexed offset ≤ O
/// - Return its byte position
/// - Caller scans log from that position
/// 
/// ## Ownership:
/// - Index owns the File handle exclusively
/// - Not Sync or Clone
/// - Single writer per Index
/// - Mutations require &mut self
pub struct Index {
    /// Base offset: the logical offset of the first record in the segment.
    /// This is encoded in both the filename and never changes.
    /// Used for validating lookups.
    base_offset: u64,

    /// File handle for the `.index` file.
    /// Opened in append + read mode.
    /// Positioned at end for appends.
    file: File,

    /// Number of entries in the index.
    /// Each entry is exactly 16 bytes.
    /// Used to determine file size and for binary search bounds.
    entries: u64,
}

impl Index {
    /// Open or create an index file at the given path.
    /// 
    /// The file is opened in append + read mode.
    /// If it exists, we determine entry count from file size.
    /// If it doesn't exist, we create it.
    /// 
    /// # Arguments
    /// - `path`: Path to the `.index` file (e.g., "000000000000.index")
    /// - `base_offset`: Logical offset of the first record in the segment
    /// 
    /// # Errors
    /// - I/O errors opening or creating the file
    /// - File size not divisible by 16 (corrupted index)
    pub fn open(path: impl AsRef<Path>, base_offset: u64) -> anyhow::Result<Self> {
        let path = path.as_ref();

        // Open file in append + read mode
        // - Create if doesn't exist
        // - Append mode for writes
        // - Read mode for lookups
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;

        // Determine number of entries from file size
        let size = file.metadata()?.len();

        // Each entry is exactly 16 bytes
        const ENTRY_SIZE: u64 = 16;

        if size % ENTRY_SIZE != 0 {
            anyhow::bail!(
                "corrupted index file: size {} is not divisible by {}",
                size,
                ENTRY_SIZE
            );
        }

        let entries = size / ENTRY_SIZE;

        Ok(Index {
            base_offset,
            file,
            entries,
        })
    }

    /// Append an index entry.
    /// 
    /// This maps a logical offset to a byte position in the log file.
    /// 
    /// ## Entry format:
    /// ```text
    /// [offset: u64][position: u64]
    /// ```
    /// 
    /// ## Invariants:
    /// - Entries must be appended in monotonically increasing offset order
    /// - No duplicate offsets
    /// - offset >= base_offset
    /// 
    /// ## Usage pattern:
    /// Typically called every Nth record append to the segment.
    /// For MVP: N = 100
    /// 
    /// # Arguments
    /// - `offset`: Logical offset of the record
    /// - `position`: Byte position in the log file
    /// 
    /// # Errors
    /// - I/O errors during write or flush
    pub fn append(&mut self, offset: u64, position: u64) -> anyhow::Result<()> {
        // Encode entry: [offset: u64][position: u64]
        let mut entry = [0u8; 16];
        entry[0..8].copy_from_slice(&offset.to_be_bytes());
        entry[8..16].copy_from_slice(&position.to_be_bytes());

        // Write to file
        self.file.write_all(&entry)?;

        // Flush to disk
        // Ensures durability
        self.file.flush()?;

        // Update entry count
        self.entries += 1;

        Ok(())
    }

    /// Lookup the byte position for a given offset.
    /// 
    /// Returns the position of the largest indexed offset ≤ target offset.
    /// 
    /// ## Algorithm:
    /// - Binary search over fixed-size entries
    /// - Find rightmost entry where entry.offset ≤ target
    /// - Return entry.position
    /// 
    /// ## Return values:
    /// - `Some(position)`: Found an indexed offset ≤ target
    /// - `None`: Target offset < base_offset (invalid)
    /// 
    /// ## Example:
    /// ```text
    /// Index contains: [(100, 0), (200, 1500), (300, 3000)]
    /// 
    /// lookup(150) → Some(0)      // Start from offset 100
    /// lookup(200) → Some(1500)   // Exact match
    /// lookup(250) → Some(1500)   // Start from offset 200
    /// lookup(50)  → None         // Below base offset
    /// ```
    /// 
    /// ## Performance:
    /// O(log N) seeks and reads, where N = number of entries
    /// 
    /// # Arguments
    /// - `offset`: Target logical offset to lookup
    /// 
    /// # Errors
    /// - I/O errors during seek or read
    pub fn lookup(&mut self, offset: u64) -> anyhow::Result<Option<u64>> {
        // Check if offset is below base
        if offset < self.base_offset {
            return Ok(None);
        }

        // Empty index
        if self.entries == 0 {
            return Ok(None);
        }

        // Binary search bounds
        let mut left: i64 = 0;
        let mut right: i64 = self.entries as i64 - 1;
        let mut result_position: Option<u64> = None;

        // Binary search: find rightmost entry where entry.offset <= target
        while left <= right {
            let mid = left + (right - left) / 2;

            // Seek to entry at index 'mid'
            let file_offset = (mid as u64) * 16;
            self.file.seek(SeekFrom::Start(file_offset))?;

            // Read entry
            let mut entry = [0u8; 16];
            self.file.read_exact(&mut entry)?;

            let entry_offset = u64::from_be_bytes([
                entry[0], entry[1], entry[2], entry[3],
                entry[4], entry[5], entry[6], entry[7],
            ]);
            let entry_position = u64::from_be_bytes([
                entry[8], entry[9], entry[10], entry[11],
                entry[12], entry[13], entry[14], entry[15],
            ]);

            if entry_offset <= offset {
                // This entry is a candidate
                result_position = Some(entry_position);
                // Look for a larger offset in the right half
                left = mid + 1;
            } else {
                // entry_offset > offset, look in left half
                right = mid - 1;
            }
        }

        Ok(result_position)
    }

    /// Get the base offset of this index.
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Get the number of entries in the index.
    pub fn entries(&self) -> u64 {
        self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a temp directory for tests
    fn temp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("failed to create temp dir")
    }

    #[test]
    fn test_index_open_new() {
        let dir = temp_dir();
        let index_path = dir.path().join("000000000000.index");

        let index = Index::open(&index_path, 0).expect("failed to open index");

        assert_eq!(index.base_offset(), 0);
        assert_eq!(index.entries(), 0);
        assert!(index_path.exists());
    }

    #[test]
    fn test_append_single_entry() {
        let dir = temp_dir();
        let index_path = dir.path().join("000000000000.index");

        let mut index = Index::open(&index_path, 0).expect("failed to open index");

        index.append(100, 1500).expect("failed to append");

        assert_eq!(index.entries(), 1);

        // Verify file size: 1 entry * 16 bytes = 16
        let file_size = std::fs::metadata(&index_path)
            .expect("failed to get metadata")
            .len();
        assert_eq!(file_size, 16);

        // Read raw bytes and verify format
        let mut file_bytes = Vec::new();
        File::open(&index_path)
            .expect("failed to open file")
            .read_to_end(&mut file_bytes)
            .expect("failed to read file");

        assert_eq!(file_bytes.len(), 16);

        let offset = u64::from_be_bytes([
            file_bytes[0], file_bytes[1], file_bytes[2], file_bytes[3],
            file_bytes[4], file_bytes[5], file_bytes[6], file_bytes[7],
        ]);
        let position = u64::from_be_bytes([
            file_bytes[8], file_bytes[9], file_bytes[10], file_bytes[11],
            file_bytes[12], file_bytes[13], file_bytes[14], file_bytes[15],
        ]);

        assert_eq!(offset, 100);
        assert_eq!(position, 1500);
    }

    #[test]
    fn test_append_multiple_entries() {
        let dir = temp_dir();
        let index_path = dir.path().join("000000000000.index");

        let mut index = Index::open(&index_path, 0).expect("failed to open index");

        let entries_to_add = vec![
            (100, 0),
            (200, 1500),
            (300, 3000),
            (400, 4500),
        ];

        for (offset, position) in &entries_to_add {
            index.append(*offset, *position).expect("failed to append");
        }

        assert_eq!(index.entries(), 4);

        // Verify file size: 4 entries * 16 bytes = 64
        let file_size = std::fs::metadata(&index_path)
            .expect("failed to get metadata")
            .len();
        assert_eq!(file_size, 64);

        // Read and verify all entries
        let mut file_bytes = Vec::new();
        File::open(&index_path)
            .expect("failed to open file")
            .read_to_end(&mut file_bytes)
            .expect("failed to read file");

        for (i, (expected_offset, expected_position)) in entries_to_add.iter().enumerate() {
            let base = i * 16;
            let offset = u64::from_be_bytes([
                file_bytes[base], file_bytes[base + 1], file_bytes[base + 2], file_bytes[base + 3],
                file_bytes[base + 4], file_bytes[base + 5], file_bytes[base + 6], file_bytes[base + 7],
            ]);
            let position = u64::from_be_bytes([
                file_bytes[base + 8], file_bytes[base + 9], file_bytes[base + 10], file_bytes[base + 11],
                file_bytes[base + 12], file_bytes[base + 13], file_bytes[base + 14], file_bytes[base + 15],
            ]);

            assert_eq!(offset, *expected_offset);
            assert_eq!(position, *expected_position);
        }
    }

    #[test]
    fn test_lookup_exact_offset() {
        let dir = temp_dir();
        let index_path = dir.path().join("000000000000.index");

        let mut index = Index::open(&index_path, 0).expect("failed to open index");

        // Add entries
        index.append(100, 0).expect("failed to append");
        index.append(200, 1500).expect("failed to append");
        index.append(300, 3000).expect("failed to append");

        // Lookup exact offsets
        let pos = index.lookup(100).expect("lookup failed");
        assert_eq!(pos, Some(0));

        let pos = index.lookup(200).expect("lookup failed");
        assert_eq!(pos, Some(1500));

        let pos = index.lookup(300).expect("lookup failed");
        assert_eq!(pos, Some(3000));
    }

    #[test]
    fn test_lookup_in_between_offset() {
        let dir = temp_dir();
        let index_path = dir.path().join("000000000000.index");

        let mut index = Index::open(&index_path, 0).expect("failed to open index");

        // Add entries
        index.append(100, 0).expect("failed to append");
        index.append(200, 1500).expect("failed to append");
        index.append(300, 3000).expect("failed to append");

        // Lookup offsets between indexed values
        // Should return position of largest indexed offset <= target

        let pos = index.lookup(150).expect("lookup failed");
        assert_eq!(pos, Some(0)); // Closest is offset 100

        let pos = index.lookup(250).expect("lookup failed");
        assert_eq!(pos, Some(1500)); // Closest is offset 200

        let pos = index.lookup(350).expect("lookup failed");
        assert_eq!(pos, Some(3000)); // Closest is offset 300

        let pos = index.lookup(1000).expect("lookup failed");
        assert_eq!(pos, Some(3000)); // Closest is still offset 300
    }

    #[test]
    fn test_lookup_below_base_offset() {
        let dir = temp_dir();
        let index_path = dir.path().join("000000000100.index");

        let mut index = Index::open(&index_path, 100).expect("failed to open index");

        // Add entries
        index.append(100, 0).expect("failed to append");
        index.append(200, 1500).expect("failed to append");

        // Lookup offset below base_offset
        let pos = index.lookup(50).expect("lookup failed");
        assert_eq!(pos, None);

        let pos = index.lookup(99).expect("lookup failed");
        assert_eq!(pos, None);
    }

    #[test]
    fn test_lookup_empty_index() {
        let dir = temp_dir();
        let index_path = dir.path().join("000000000000.index");

        let mut index = Index::open(&index_path, 0).expect("failed to open index");

        // Lookup in empty index
        let pos = index.lookup(100).expect("lookup failed");
        assert_eq!(pos, None);
    }

    #[test]
    fn test_reopen_existing_index() {
        let dir = temp_dir();
        let index_path = dir.path().join("000000000000.index");

        // Create and populate index
        {
            let mut index = Index::open(&index_path, 0).expect("failed to open index");
            index.append(100, 0).expect("failed to append");
            index.append(200, 1500).expect("failed to append");
        }

        // Reopen and verify
        {
            let mut index = Index::open(&index_path, 0).expect("failed to reopen index");
            assert_eq!(index.entries(), 2);

            let pos = index.lookup(100).expect("lookup failed");
            assert_eq!(pos, Some(0));

            let pos = index.lookup(200).expect("lookup failed");
            assert_eq!(pos, Some(1500));
        }
    }
}
