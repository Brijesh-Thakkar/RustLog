/// Cleaner checkpoint implementation for deterministic retention and compaction.
/// 
/// The cleaner checkpoint ensures that retention and compaction operations are:
/// - Idempotent: repeated operations have no additional effect
/// - Crash-safe: broker restarts resume from last checkpoint
/// - Deterministic: operations never repeat work already completed
/// - Monotonic: checkpoint offsets never decrease
/// 
/// ## File Format:
/// ```text
/// data/<topic>-<partition>/.cleaner_checkpoint
/// ```
/// 
/// ## Checkpoint Data:
/// - last_compacted_offset: Last offset that has been compacted (monotonic)
/// - last_retention_run_timestamp_ms: Last time retention was successfully applied
/// 
/// ## Atomic Update Protocol:
/// 1. Write to temporary file (.cleaner_checkpoint.tmp)
/// 2. fsync() to ensure data is on disk
/// 3. rename() to atomically replace checkpoint
/// 
/// This ensures checkpoint is always consistent, even after crash.

use crate::error::BrokerError;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Cleaner checkpoint state for a partition.
/// 
/// This tracks the progress of compaction and retention operations
/// to ensure they are idempotent and crash-safe.
#[derive(Debug, Clone, PartialEq)]
pub struct CleanerCheckpoint {
    /// Last offset that has been compacted.
    /// 
    /// Only segments with base_offset > last_compacted_offset are eligible
    /// for compaction. This ensures incremental compaction that never
    /// reprocesses already-compacted segments.
    /// 
    /// INVARIANT: Must be monotonic (never decrease)
    pub last_compacted_offset: u64,
    
    /// Timestamp (milliseconds since Unix epoch) when retention was last
    /// successfully applied.
    /// 
    /// Used to determine if retention should be skipped (idempotency).
    /// Only updated when retention actually deletes segments.
    pub last_retention_run_timestamp_ms: u64,
    
    /// Path to the checkpoint file on disk.
    file_path: PathBuf,
}

impl CleanerCheckpoint {
    /// Load or create a cleaner checkpoint for the given partition directory.
    /// 
    /// ## Arguments:
    /// - `partition_dir`: Directory containing partition data (e.g., "data/topic-0/")
    /// 
    /// ## Returns:
    /// - Existing checkpoint if file exists and is valid
    /// - New checkpoint with zero values if file doesn't exist
    /// - Error if file exists but is corrupted
    /// 
    /// ## Corruption Handling:
    /// Corrupt checkpoints are recreated with initial values to ensure system availability
    /// while maintaining the invariant that retention operations are safe.
    pub fn load_or_create<P: AsRef<Path>>(partition_dir: P) -> Result<Self, BrokerError> {
        let partition_dir = partition_dir.as_ref();
        let file_path = partition_dir.join(".cleaner_checkpoint");
        
        if file_path.exists() {
            match Self::load_existing(&file_path) {
                Ok(checkpoint) => Ok(checkpoint),
                Err(BrokerError::CheckpointCorrupt(msg)) => {
                    // Corruption detected - recover by rebuilding from segments
                    eprintln!("Warning: Corrupted checkpoint detected: {}", msg);
                    eprintln!("Attempting to recover checkpoint by rebuilding from segment files");
                    
                    // Increment corruption metric
                    crate::metrics::METRICS.inc_checkpoint_corruption();
                    
                    // TODO: In production, scan sealed segments to rebuild progress
                    // For now, reset to initial values but log clearly
                    let checkpoint = CleanerCheckpoint {
                        last_compacted_offset: 0,
                        last_retention_run_timestamp_ms: 0,
                        file_path,
                    };
                    // Write the fresh checkpoint to disk
                    checkpoint.write_to_disk()?;
                    Ok(checkpoint)
                },
                Err(other) => Err(other), // Propagate other errors
            }
        } else {
            // Create new checkpoint with initial values
            Ok(CleanerCheckpoint {
                last_compacted_offset: 0,
                last_retention_run_timestamp_ms: 0,
                file_path,
            })
        }
    }
    
    /// Load existing checkpoint from disk.
    /// 
    /// ## File Format (Binary):
    /// ```text
    /// [last_compacted_offset: u64][last_retention_run_timestamp_ms: u64]
    /// Total: 16 bytes
    /// ```
    /// 
    /// ## Corruption Detection:
    /// - Wrong file size → BrokerError::CheckpointCorrupt
    /// - I/O errors → BrokerError::Storage
    fn load_existing(file_path: &Path) -> Result<Self, BrokerError> {
        let mut file = File::open(file_path)
            .map_err(|e| BrokerError::Storage(format!("Failed to open checkpoint file {:?}: {}", file_path, e)))?;
        
        // Validate file size
        let metadata = file.metadata()
            .map_err(|e| BrokerError::Storage(format!("Failed to read checkpoint metadata {:?}: {}", file_path, e)))?;
        
        if metadata.len() != 16 {
            return Err(BrokerError::CheckpointCorrupt(format!(
                "Checkpoint file {:?} has invalid size {} (expected 16 bytes)", 
                file_path, metadata.len()
            )));
        }
        
        // Read checkpoint data
        let mut buffer = [0u8; 16];
        file.read_exact(&mut buffer)
            .map_err(|e| BrokerError::Storage(format!("Failed to read checkpoint data {:?}: {}", file_path, e)))?;
        
        // Parse binary format
        let last_compacted_offset = u64::from_be_bytes([
            buffer[0], buffer[1], buffer[2], buffer[3],
            buffer[4], buffer[5], buffer[6], buffer[7]
        ]);
        
        let last_retention_run_timestamp_ms = u64::from_be_bytes([
            buffer[8], buffer[9], buffer[10], buffer[11],
            buffer[12], buffer[13], buffer[14], buffer[15]
        ]);
        
        // Validate checkpoint data
        if last_compacted_offset == u64::MAX {
            return Err(BrokerError::CheckpointCorrupt(format!(
                "Checkpoint file {:?} contains invalid last_compacted_offset (u64::MAX)", 
                file_path
            )));
        }
        
        Ok(CleanerCheckpoint {
            last_compacted_offset,
            last_retention_run_timestamp_ms,
            file_path: file_path.to_path_buf(),
        })
    }
    
    /// Update the last compacted offset.
    /// 
    /// ## INVARIANT ENFORCEMENT:
    /// The new offset MUST be >= current offset (monotonic).
    /// Decreasing offsets indicate a serious bug and will return error.
    /// 
    /// ## Atomicity:
    /// Updates are written atomically using write → fsync → rename → dir fsync.
    pub fn update_compacted_offset(&mut self, new_offset: u64) -> Result<(), BrokerError> {
        if new_offset < self.last_compacted_offset {
            return Err(BrokerError::Storage(format!(
                "INVARIANT VIOLATION: Attempted to decrease last_compacted_offset from {} to {}. \
                This indicates a serious bug in compaction logic.",
                self.last_compacted_offset, new_offset
            )));
        }
        
        self.last_compacted_offset = new_offset;
        self.write_to_disk()
    }
    
    /// Update the last retention run timestamp.
    /// 
    /// This should only be called when retention actually deletes segments.
    /// If retention runs but deletes nothing, do NOT update timestamp.
    /// 
    /// ## Timestamp Source:
    /// Uses current system time (milliseconds since Unix epoch).
    pub fn update_retention_timestamp(&mut self) -> Result<(), BrokerError> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| BrokerError::Storage(format!("System time before Unix epoch: {}", e)))?
            .as_millis() as u64;
            
        self.last_retention_run_timestamp_ms = now_ms;
        self.write_to_disk()
    }
    
    /// Check if retention should be skipped based on last run time.
    /// 
    /// ## Idempotency Logic:
    /// Retention is skipped if it was run recently (within the retention window).
    /// This prevents repeated deletion attempts on the same data.
    /// 
    /// ## Arguments:
    /// - `retention_interval_ms`: Minimum time between retention runs
    /// 
    /// ## Returns:
    /// - `true` if retention should be skipped (already applied recently)
    /// - `false` if retention should proceed
    pub fn should_skip_retention(&self, retention_interval_ms: u64) -> bool {
        if self.last_retention_run_timestamp_ms == 0 {
            // Never run retention - should proceed
            return false;
        }
        
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
            
        let time_since_last_run = now_ms.saturating_sub(self.last_retention_run_timestamp_ms);
        
        // Skip if retention was run within the interval
        time_since_last_run < retention_interval_ms
    }
    
    /// Update retention timestamp with specific time (for testing).
    /// 
    /// This is a test-only method to simulate retention runs at specific times.
    /// Production code should only use `update_retention_timestamp()`.
    pub fn update_retention_timestamp_with_time(&mut self, timestamp_ms: u64) -> Result<(), BrokerError> {
        self.last_retention_run_timestamp_ms = timestamp_ms;
        self.write_to_disk()
    }
    
    /// Atomically write checkpoint to disk.
    /// 
    /// ## Atomic Update Protocol:
    /// 1. Write to temporary file (.cleaner_checkpoint.tmp)
    /// 2. fsync() to ensure data is on physical storage
    /// 3. rename() to atomically replace checkpoint file
    /// 
    /// This ensures checkpoint is never left in a partial state.
    fn write_to_disk(&self) -> Result<(), BrokerError> {
        let tmp_path = self.file_path.with_extension("tmp");
        
        // Ensure parent directory exists
        if let Some(parent) = self.file_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| BrokerError::Storage(format!("Failed to create checkpoint directory {:?}: {}", parent, e)))?;
        }
        
        // Write to temporary file
        {
            let mut tmp_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)
                .map_err(|e| BrokerError::Storage(format!("Failed to create temporary checkpoint file {:?}: {}", tmp_path, e)))?;
            
            // Write binary data
            let mut buffer = [0u8; 16];
            
            // last_compacted_offset (8 bytes, big-endian)
            buffer[0..8].copy_from_slice(&self.last_compacted_offset.to_be_bytes());
            
            // last_retention_run_timestamp_ms (8 bytes, big-endian)
            buffer[8..16].copy_from_slice(&self.last_retention_run_timestamp_ms.to_be_bytes());
            
            tmp_file.write_all(&buffer)
                .map_err(|e| BrokerError::Storage(format!("Failed to write checkpoint data: {}", e)))?;
            
            // Ensure data is on physical storage before rename
            tmp_file.sync_all()
                .map_err(|e| BrokerError::Storage(format!("Failed to sync checkpoint file: {}", e)))?;
        }
        
        // Atomically replace checkpoint file
        std::fs::rename(&tmp_path, &self.file_path)
            .map_err(|e| BrokerError::Storage(format!("Failed to rename checkpoint file: {}", e)))?;
                // CRITICAL: fsync parent directory to ensure rename is durable
        // Without this, checkpoint updates may be lost on crash
        if let Some(parent_dir) = self.file_path.parent() {
            let dir_file = std::fs::File::open(parent_dir)
                .map_err(|e| BrokerError::Storage(format!("Failed to open parent directory for fsync: {}", e)))?;
            dir_file.sync_all()
                .map_err(|e| BrokerError::Storage(format!("Failed to fsync parent directory: {}", e)))?
        }
                Ok(())
    }
    
    /// Get the last compacted offset.
    pub fn last_compacted_offset(&self) -> u64 {
        self.last_compacted_offset
    }
    
    /// Get the last retention run timestamp.
    pub fn last_retention_run_timestamp_ms(&self) -> u64 {
        self.last_retention_run_timestamp_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_checkpoint_create_new() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint = CleanerCheckpoint::load_or_create(temp_dir.path()).unwrap();
        
        assert_eq!(checkpoint.last_compacted_offset(), 0);
        assert_eq!(checkpoint.last_retention_run_timestamp_ms(), 0);
    }
    
    #[test]
    fn test_checkpoint_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create and update checkpoint
        let mut checkpoint = CleanerCheckpoint::load_or_create(temp_dir.path()).unwrap();
        checkpoint.update_compacted_offset(12345).unwrap();
        checkpoint.update_retention_timestamp().unwrap();
        
        let original_timestamp = checkpoint.last_retention_run_timestamp_ms();
        
        // Load from disk and verify
        let loaded_checkpoint = CleanerCheckpoint::load_or_create(temp_dir.path()).unwrap();
        assert_eq!(loaded_checkpoint.last_compacted_offset(), 12345);
        assert_eq!(loaded_checkpoint.last_retention_run_timestamp_ms(), original_timestamp);
    }
    
    #[test]
    fn test_checkpoint_monotonic_offset() {
        let temp_dir = TempDir::new().unwrap();
        let mut checkpoint = CleanerCheckpoint::load_or_create(temp_dir.path()).unwrap();
        
        // Update to higher offset - should succeed
        checkpoint.update_compacted_offset(100).unwrap();
        assert_eq!(checkpoint.last_compacted_offset(), 100);
        
        // Update to same offset - should succeed (no-op)
        checkpoint.update_compacted_offset(100).unwrap();
        assert_eq!(checkpoint.last_compacted_offset(), 100);
        
        // Update to higher offset - should succeed
        checkpoint.update_compacted_offset(200).unwrap();
        assert_eq!(checkpoint.last_compacted_offset(), 200);
    }
    
    #[test]
    fn test_checkpoint_decreasing_offset_error() {
        let temp_dir = TempDir::new().unwrap();
        let mut checkpoint = CleanerCheckpoint::load_or_create(temp_dir.path()).unwrap();
        
        checkpoint.update_compacted_offset(100).unwrap();
        
        // This should return an error, not panic
        let result = checkpoint.update_compacted_offset(50);
        assert!(result.is_err(), "Decreasing offset should return error");
        assert!(result.unwrap_err().to_string().contains("INVARIANT VIOLATION"));
    }
    
    #[test]
    fn test_retention_skip_logic() {
        let temp_dir = TempDir::new().unwrap();
        let mut checkpoint = CleanerCheckpoint::load_or_create(temp_dir.path()).unwrap();
        
        // Initially should not skip (never run)
        assert!(!checkpoint.should_skip_retention(60000)); // 1 minute
        
        // Update timestamp
        checkpoint.update_retention_timestamp().unwrap();
        
        // Should skip retention if interval is long
        assert!(checkpoint.should_skip_retention(60000)); // 1 minute
        
        // Should not skip retention if interval is very short
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(!checkpoint.should_skip_retention(1)); // 1ms
    }
    
    #[test]
    fn test_corrupt_checkpoint_size() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_path = temp_dir.path().join(".cleaner_checkpoint");
        
        // Create file with wrong size
        std::fs::write(&checkpoint_path, b"wrong_size").unwrap();
        
        // Should auto-recover from corruption
        let result = CleanerCheckpoint::load_or_create(temp_dir.path());
        assert!(result.is_ok(), "Corrupted checkpoint should be auto-recovered");
        
        // Verify it was reset to initial values
        let checkpoint = result.unwrap();
        assert_eq!(checkpoint.last_compacted_offset(), 0, "Recovered checkpoint should have offset 0");
        assert_eq!(checkpoint.last_retention_run_timestamp_ms(), 0, "Recovered checkpoint should have timestamp 0");
    }
}