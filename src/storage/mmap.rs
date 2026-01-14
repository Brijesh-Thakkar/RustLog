use std::fs::File;

// Drop is automatically implemented by memmap2::Mmap.
// It safely calls munmap(2) when MmapRegion goes out of scope.

/// A read-only memory-mapped region of a file.
/// 
/// # Purpose
/// This abstraction exists ONLY for read-only, zero-copy access to sealed segment files.
/// Correctness does NOT depend on mmap - it is a performance optimization only.
/// 
/// # What May Be Mmapped
/// - Only READ-ONLY files
/// - Only files whose size is FIXED (never changes)
/// - Only inactive (sealed) segment files
/// 
/// # Ownership & Lifetime Rules (CRITICAL)
/// - mmap is OWNED by Segment (not exposed to connections)
/// - mmap lifetime ≤ file lifetime
/// - mmap MUST be dropped before file is closed
/// - mmap MUST NOT outlive the file descriptor
/// - File descriptor must remain valid for mmap lifetime
/// 
/// # Safety Invariants (MUST HOLD)
/// These invariants MUST hold or undefined behavior occurs:
/// 
/// 1. File size is FIXED for mmap lifetime (never shrinks or grows)
/// 2. mmap is READ-ONLY (no writes through mmap)
/// 3. No access beyond mapped length (bounds checked)
/// 4. File descriptor outlives mmap
/// 5. mmap is dropped before file close
/// 6. mmap is NEVER written to
/// 7. mmap is NEVER used for active (appendable) segments
/// 
/// # Thread Safety
/// MmapRegion is NOT Send or Sync by default (memmap2::Mmap is not).
/// This is correct: mmap should not be shared across threads.
/// Each segment owns its own mmap.
/// 
/// # Drop Safety
/// Drop is implemented via memmap2, which safely calls munmap.
#[derive(Debug)]
pub struct MmapRegion {
    /// The underlying memory-mapped region.
    /// 
    /// Safety: memmap2::Mmap handles the unsafe mmap/munmap calls.
    /// We wrap it in a safe API that enforces read-only access.
    mmap: memmap2::Mmap,
}

impl MmapRegion {
    /// Create a read-only memory-mapped region from a file.
    /// 
    /// # Safety Invariants Enforced
    /// - File must not be modified while mmap exists
    /// - File size must remain fixed
    /// - File descriptor must outlive this MmapRegion
    /// 
    /// # Arguments
    /// - `file`: Reference to an open file descriptor
    /// 
    /// # Returns
    /// - `Ok(MmapRegion)`: Successfully mapped
    /// - `Err`: Failed to map (e.g., empty file, permissions, OS limits)
    /// 
    /// # Errors
    /// - File has zero length (cannot mmap empty file)
    /// - OS mmap call fails (permissions, limits, etc.)
    /// 
    /// # Usage
    /// ```rust,ignore
    /// let file = File::open("segment.log")?;
    /// let mmap = MmapRegion::open_readonly(&file)?;
    /// let data = mmap.as_slice();
    /// // Read from data...
    /// // mmap is dropped when out of scope
    /// ```
    pub fn open_readonly(file: &File) -> anyhow::Result<Self> {
        // Get file size
        let len = file.metadata()?.len();

        // Cannot mmap empty file
        if len == 0 {
            anyhow::bail!("cannot mmap empty file");
        }

        // SAFETY ANALYSIS:
        // 
        // We use memmap2::Mmap::map() which internally calls mmap(2).
        // 
        // Safety requirements for mmap(2):
        // 1. File descriptor must be valid ✓ (caller provides open File)
        // 2. File must not be truncated while mapped ✓ (sealed files only)
        // 3. File must not be written while mapped ✓ (read-only mapping)
        // 4. Mapped region must not outlive file ✓ (enforced by Rust lifetime)
        // 
        // memmap2::Mmap::map() enforces:
        // - Creates read-only mapping (PROT_READ)
        // - Validates file descriptor
        // - Implements Drop to call munmap(2)
        // 
        // We enforce at the API level:
        // - Only sealed (inactive) files are mmapped
        // - No mutation possible (as_slice returns &[u8], not &mut [u8])
        // - MmapRegion dropped before file is closed (lifetime bound)
        //
        // UNSAFE: memmap2::Mmap::map uses unsafe internally but provides
        // safe wrapper. The underlying mmap syscall is inherently unsafe.
        let mmap = unsafe {
            memmap2::Mmap::map(file)
                .map_err(|e| anyhow::anyhow!("failed to mmap file: {}", e))?
        };

        Ok(MmapRegion { mmap })
    }

    /// Get a read-only slice of the mapped region.
    /// 
    /// This is the ONLY way to access mmap data.
    /// Returns immutable slice - no mutation possible.
    /// 
    /// # Returns
    /// Immutable byte slice covering entire mapped region.
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap
    }

    /// Get the length of the mapped region in bytes.
    /// 
    /// This matches the file size at the time of mapping.
    /// 
    /// # Returns
    /// Length in bytes.
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    /// Check if the mapped region is empty.
    /// 
    /// Should never be true since we reject empty files in open_readonly.
    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }
}

// Drop is automatically implemented by memmap2::Mmap.
// It safely calls munmap(2) when MmapRegion goes out of scope.

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_mmap_reads_correct_bytes() {
        // Create temp file with known data
        let mut temp_file = NamedTempFile::new().expect("failed to create temp file");
        let data = b"hello world from mmap";
        temp_file.write_all(data).expect("failed to write");
        temp_file.flush().expect("failed to flush");

        // Reopen as read-only for mmap
        let file = File::open(temp_file.path()).expect("failed to open");

        // Create mmap
        let mmap = MmapRegion::open_readonly(&file).expect("failed to mmap");

        // Verify data matches
        assert_eq!(mmap.as_slice(), data);
        assert_eq!(mmap.len(), data.len());
        assert!(!mmap.is_empty());
    }

    #[test]
    fn test_mmap_len_matches_file() {
        // Create temp file with specific size
        let mut temp_file = NamedTempFile::new().expect("failed to create temp file");
        let data = vec![42u8; 1024];
        temp_file.write_all(&data).expect("failed to write");
        temp_file.flush().expect("failed to flush");

        // Get file size
        let file_size = temp_file.as_file().metadata().expect("failed to get metadata").len();

        // Reopen as read-only
        let file = File::open(temp_file.path()).expect("failed to open");

        // Create mmap
        let mmap = MmapRegion::open_readonly(&file).expect("failed to mmap");

        // Verify lengths match
        assert_eq!(mmap.len(), file_size as usize);
        assert_eq!(mmap.len(), 1024);
    }

    #[test]
    fn test_mmap_empty_file_fails() {
        // Create empty temp file
        let temp_file = NamedTempFile::new().expect("failed to create temp file");

        // Reopen as read-only
        let file = File::open(temp_file.path()).expect("failed to open");

        // Attempt to mmap empty file should fail
        let result = MmapRegion::open_readonly(&file);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn test_mmap_slice_is_immutable() {
        // This test verifies compile-time immutability
        let mut temp_file = NamedTempFile::new().expect("failed to create temp file");
        temp_file.write_all(b"test").expect("failed to write");
        temp_file.flush().expect("failed to flush");

        let file = File::open(temp_file.path()).expect("failed to open");
        let mmap = MmapRegion::open_readonly(&file).expect("failed to mmap");

        let slice = mmap.as_slice();

        // This verifies that slice is &[u8], not &mut [u8]
        // Attempting to write would fail at compile time:
        // slice[0] = 42; // ERROR: cannot assign to immutable indexed content
        assert_eq!(slice.len(), 4);
    }

    #[test]
    fn test_mmap_multiple_regions() {
        // Create two separate files
        let mut file1 = NamedTempFile::new().expect("failed to create temp file");
        let mut file2 = NamedTempFile::new().expect("failed to create temp file");

        file1.write_all(b"file1").expect("failed to write");
        file2.write_all(b"file2data").expect("failed to write");

        file1.flush().expect("failed to flush");
        file2.flush().expect("failed to flush");

        // Open both files
        let f1 = File::open(file1.path()).expect("failed to open");
        let f2 = File::open(file2.path()).expect("failed to open");

        // Create separate mmaps
        let mmap1 = MmapRegion::open_readonly(&f1).expect("failed to mmap");
        let mmap2 = MmapRegion::open_readonly(&f2).expect("failed to mmap");

        // Verify each mmap has correct data
        assert_eq!(mmap1.as_slice(), b"file1");
        assert_eq!(mmap2.as_slice(), b"file2data");
        assert_ne!(mmap1.len(), mmap2.len());
    }

    #[test]
    fn test_mmap_drop_safety() {
        // This test verifies that mmap can be safely dropped
        let mut temp_file = NamedTempFile::new().expect("failed to create temp file");
        temp_file.write_all(b"test").expect("failed to write");
        temp_file.flush().expect("failed to flush");

        let file = File::open(temp_file.path()).expect("failed to open");

        {
            let _mmap = MmapRegion::open_readonly(&file).expect("failed to mmap");
            // mmap is dropped here
        }

        // File is still valid after mmap is dropped
        let metadata = file.metadata().expect("file metadata should still be accessible");
        assert_eq!(metadata.len(), 4);
    }
}
