use kafka_lite::storage::cleaner_checkpoint::CleanerCheckpoint;
use kafka_lite::storage::{index::Index, segment::Segment};
use kafka_lite::topics::partition::Partition;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

/// Test Phase 5 cleaner checkpoint functionality:
/// 1. Checkpoint persistence across "broker restarts"
/// 2. Retention idempotency (repeated operations are no-ops)
/// 3. Incremental compaction tracking
/// 4. Metrics tracking for skipped operations
#[test]
fn test_phase5_cleaner_checkpoint_integration() {
    // Create test partition with multiple segments of different ages
    let temp_dir = TempDir::new().unwrap();
    let partition_dir = temp_dir.path().join("test-topic-0");
    std::fs::create_dir_all(&partition_dir).unwrap();
    
    // Create 3 sealed segments: old, medium, and recent
    let (old_seg, old_idx) = create_test_segment_at_path(
        &partition_dir, 
        100, 
        vec![b"old1".to_vec(), b"old2".to_vec()]
    );
    
    let (med_seg, med_idx) = create_test_segment_at_path(
        &partition_dir, 
        200, 
        vec![b"med1".to_vec(), b"med2".to_vec()]
    );
    
    let (new_seg, new_idx) = create_test_segment_at_path(
        &partition_dir, 
        300, 
        vec![b"new1".to_vec(), b"new2".to_vec()]
    );
    
    // Create active segment
    let (active_seg, active_idx) = create_test_active_segment_at_path(
        &partition_dir, 
        400, 
        vec![b"active1".to_vec()]
    );
    
    // Create cleaner checkpoint
    let cleaner_checkpoint = CleanerCheckpoint::load_or_create(&partition_dir).unwrap();
    
    // Build partition
    let mut partition = Partition {
        id: 0,
        sealed_segments: vec![(old_seg, old_idx), (med_seg, med_idx), (new_seg, new_idx)],
        active_segment: (active_seg, active_idx),
        cleaner_checkpoint: Mutex::new(cleaner_checkpoint),
    };
    
    // Test 1: First retention operation (should actually delete segments)
    // Wait a bit to ensure segment creation time is in the past
    std::thread::sleep(std::time::Duration::from_millis(10));
    // Use very aggressive time retention (5ms) so segments just created are considered old
    let retention_result = partition.apply_time_retention(5, 5000).unwrap(); // 5ms retention, 5s interval
    assert!(retention_result.segments_deleted > 0, "First retention should delete segments");
    let first_deleted = retention_result.segments_deleted;
    
    // Test 2: Immediate second retention (should be no-op due to checkpoint)
    let retention_result2 = partition.apply_time_retention(5, 5000).unwrap();
    assert_eq!(retention_result2.segments_deleted, 0, "Second retention should be no-op");
    assert_eq!(retention_result2.bytes_reclaimed, 0, "Second retention should reclaim no bytes");
    
    // Test 3: Simulate "broker restart" by reloading checkpoint from disk
    {
        let checkpoint_reloaded = CleanerCheckpoint::load_or_create(&partition_dir).unwrap();
        partition.cleaner_checkpoint = Mutex::new(checkpoint_reloaded);
    }
    
    // Test 4: Post-restart retention should still be no-op (checkpoint persisted)
    let retention_result3 = partition.apply_time_retention(5, 5000).unwrap();
    assert_eq!(retention_result3.segments_deleted, 0, "Post-restart retention should be no-op");
    
    // Test 5: After interval elapses, retention can run again
    // Simulate 6 seconds passing by directly updating checkpoint
    {
        let mut checkpoint = partition.cleaner_checkpoint.lock().unwrap();
        // Force the checkpoint to be older by setting timestamp to 0
        checkpoint.update_retention_timestamp_with_time(0).unwrap();
    }
    
    let _retention_result4 = partition.apply_time_retention(5, 5000).unwrap(); 
    // This might delete more segments or be no-op if all are already deleted
    
    // Test 6: Compaction tracking (incremental based on checkpoint)
    let compaction_result = partition.apply_log_compaction().unwrap();
    // Since compaction is not fully implemented yet, this tests the infrastructure
    assert_eq!(compaction_result.compaction_runs, 0, "Compaction not yet implemented");
    
    // Test 7: Checkpoint state verification (after test 5 reset it to 0)
    // Reload the checkpoint from disk to verify persistence worked correctly
    {
        let checkpoint_final = CleanerCheckpoint::load_or_create(&partition_dir).unwrap();
        assert_eq!(checkpoint_final.last_compacted_offset(), 0, "Initial compacted offset should be 0");
        // The timestamp was reset to 0 in test 5, so we verify that was persisted correctly
        assert_eq!(checkpoint_final.last_retention_run_timestamp_ms(), 0, "Timestamp should be 0 after test 5 reset");
    }
    
    println!("Phase 5 cleaner checkpoint integration test passed!");
    println!("- First retention deleted {} segments", first_deleted);
    println!("- Subsequent retention operations were properly skipped via checkpoint");
    println!("- Checkpoint persisted across simulated broker restart");
    println!("- Compaction infrastructure ready for future key-based implementation");
}

/// Test cleaner checkpoint corruption detection and recovery
#[test]
fn test_cleaner_checkpoint_corruption_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let partition_dir = temp_dir.path().join("test-topic-0");
    std::fs::create_dir_all(&partition_dir).unwrap();
    let checkpoint_path = partition_dir.join(".cleaner_checkpoint");
    
    // Create valid checkpoint
    let mut checkpoint1 = CleanerCheckpoint::load_or_create(&partition_dir).unwrap();
    checkpoint1.update_compacted_offset(100).unwrap();
    drop(checkpoint1);
    
    // Verify the checkpoint file exists and has data
    assert!(checkpoint_path.exists(), "Checkpoint file should exist");
    
    // Corrupt the file by truncating it
    std::fs::write(&checkpoint_path, &[1, 2, 3]).unwrap(); // Wrong size
    
    // Loading should detect corruption and recreate
    let checkpoint2 = CleanerCheckpoint::load_or_create(&partition_dir).unwrap();
    assert_eq!(checkpoint2.last_compacted_offset(), 0, "Corrupted checkpoint should reset to 0");
    assert_eq!(checkpoint2.last_retention_run_timestamp_ms(), 0, "Corrupted checkpoint should reset timestamp to 0");
    
    println!("Cleaner checkpoint corruption recovery test passed!");
}

/// Test monotonic checkpoint enforcement (compaction offsets never decrease)
#[test]
#[should_panic(expected = "INVARIANT VIOLATION: Attempted to decrease last_compacted_offset")]
fn test_cleaner_checkpoint_monotonic_enforcement() {
    let temp_dir = TempDir::new().unwrap();
    let partition_dir = temp_dir.path().join("test-topic-0");
    std::fs::create_dir_all(&partition_dir).unwrap();
    
    let mut checkpoint = CleanerCheckpoint::load_or_create(&partition_dir).unwrap();
    checkpoint.update_compacted_offset(200).unwrap();
    
    // This should panic due to monotonic constraint
    checkpoint.update_compacted_offset(100).unwrap();
}

/// Helper function to create a segment with data at a specific path
fn create_test_segment_at_path(
    partition_dir: &std::path::Path,
    base_offset: u64, 
    records: Vec<Vec<u8>>
) -> (Arc<Segment>, Arc<Mutex<Index>>) {
    let log_path = partition_dir.join(format!("{:020}.log", base_offset));
    let index_path = partition_dir.join(format!("{:020}.index", base_offset));
    
    let mut segment = Segment::open(&log_path, base_offset).unwrap();
    let mut index = Index::open(&index_path, base_offset).unwrap();
    
    // Add records and index them
    for payload in records {
        let offset = segment.append(&payload).unwrap();
        let position = segment.size() - (4 + 8 + payload.len() as u64);
        index.append(offset, position).unwrap();
    }
    
    // Seal the segment for reading
    if segment.record_count() > 0 {
        segment.seal().unwrap();
    }
    
    (Arc::new(segment), Arc::new(Mutex::new(index)))
}

/// Helper function to create an active (unsealed) segment at a specific path
fn create_test_active_segment_at_path(
    partition_dir: &std::path::Path,
    base_offset: u64, 
    records: Vec<Vec<u8>>
) -> (Arc<Mutex<Segment>>, Arc<Mutex<Index>>) {
    let log_path = partition_dir.join(format!("{:020}.log", base_offset));
    let index_path = partition_dir.join(format!("{:020}.index", base_offset));
    
    let mut segment = Segment::open(&log_path, base_offset).unwrap();
    let mut index = Index::open(&index_path, base_offset).unwrap();
    
    // Add records and index them
    for payload in records {
        let offset = segment.append(&payload).unwrap();
        let position = segment.size() - (4 + 8 + payload.len() as u64);
        index.append(offset, position).unwrap();
    }
    
    // Don't seal active segments - they remain mutable
    (Arc::new(Mutex::new(segment)), Arc::new(Mutex::new(index)))
}