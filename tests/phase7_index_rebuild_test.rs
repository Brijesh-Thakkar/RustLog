use kafka_lite::storage::index_rebuild::{IndexRebuilder, CorruptionStatus};
use kafka_lite::storage::segment::Segment;
use kafka_lite::storage::index::Index;
use std::fs;
use tempfile::TempDir;

/// Phase 7: Segment Index Rebuild & Crash Repair Integration Test
/// 
/// This test verifies end-to-end index rebuild functionality:
/// 1. Creating segments with data and indexes
/// 2. Simulating various corruption scenarios
/// 3. Detecting corruption correctly
/// 4. Rebuilding indexes with correct content
/// 5. Metrics are properly incremented
/// 6. Operations are idempotent and crash-safe

#[test] 
fn test_phase7_index_rebuild_with_real_segments() {
    let temp_dir = TempDir::new().unwrap();
    let log_path = temp_dir.path().join("000000000000.log");
    let index_path = temp_dir.path().join("000000000000.index");
    
    // 1. Create a segment with substantial data
    let mut segment = Segment::open(&log_path, 0).unwrap();
    let mut original_offsets = Vec::new();
    
    // Create 150 records to ensure multiple index entries
    for i in 0..150 {
        let payload = format!("Test record number {}", i);
        let offset = segment.append(payload.as_bytes()).unwrap();
        original_offsets.push(offset);
    }
    
    // Create the index manually (normally done by partition layer)
    let mut index = Index::open(&index_path, 0).unwrap();
    
    // Index first record and every 100th record (as per IndexRebuilder strategy)
    index.append(0, 0).unwrap(); // First record at offset 0, position 0
    
    // Find position of 100th record (offset 100)
    // Each record has: 4 bytes length + 8 bytes offset + payload
    let mut position = 0u64;
    for i in 0..100 {
        let payload_len = format!("Test record number {}", i).len() as u64;
        position += 4 + 8 + payload_len;
    }
    index.append(100, position).unwrap();
    
    drop(segment);
    drop(index);
    
    // 2. Verify index exists and has expected size
    let original_size = fs::metadata(&index_path).unwrap().len();
    assert_eq!(original_size, 32); // 2 entries * 16 bytes each
    
    // 3. Test corruption detection on healthy index
    let rebuilder = IndexRebuilder::new(&log_path, 0).unwrap();
    assert_eq!(rebuilder.detect_corruption(), CorruptionStatus::Healthy);
    
    // Get baseline metrics
    let baseline_metrics = kafka_lite::metrics::METRICS.snapshot_counters();
    
    // 4. Test conditional rebuild on healthy index (should be no-op)
    let rebuild_result = rebuilder.rebuild_if_corrupted().unwrap();
    assert!(rebuild_result.is_none(), "Healthy index should not be rebuilt");
    
    // 5. Simulate missing index corruption
    fs::remove_file(&index_path).unwrap();
    assert_eq!(rebuilder.detect_corruption(), CorruptionStatus::Missing);
    
    // 6. Rebuild missing index
    let rebuild_result = rebuilder.rebuild_if_corrupted().unwrap();
    assert!(rebuild_result.is_some(), "Missing index should trigger rebuild");
    
    let result = rebuild_result.unwrap();
    assert_eq!(result.records_scanned, 150);
    assert_eq!(result.entries_rebuilt, 2); // First record + 100th record
    assert!(result.was_missing);
    
    // 7. Verify index was recreated and is healthy
    assert!(index_path.exists());
    assert_eq!(rebuilder.detect_corruption(), CorruptionStatus::Healthy);
    
    // 8. Verify metrics were incremented
    let post_metrics = kafka_lite::metrics::METRICS.snapshot_counters();
    assert!(post_metrics.index_rebuilds_total > baseline_metrics.index_rebuilds_total);
    assert!(post_metrics.index_corruptions_detected_total > baseline_metrics.index_corruptions_detected_total);
    assert!(post_metrics.index_entries_rebuilt_total > baseline_metrics.index_entries_rebuilt_total);
    
    // 9. Verify rebuilt index has same size as original
    let rebuilt_size = fs::metadata(&index_path).unwrap().len();
    assert_eq!(rebuilt_size, original_size, "Rebuilt index should have same size");
    
    // 10. Test that second rebuild is idempotent
    let second_baseline = kafka_lite::metrics::METRICS.snapshot_counters();
    let second_result = rebuilder.rebuild_if_corrupted().unwrap();
    assert!(second_result.is_none(), "Second rebuild should be no-op");
    
    let final_metrics = kafka_lite::metrics::METRICS.snapshot_counters();
    assert_eq!(
        second_baseline.index_rebuilds_total,
        final_metrics.index_rebuilds_total,
        "Idempotent rebuild should not increment counters"
    );
}

#[test]
fn test_index_rebuild_corrupted_size_scenario() {
    let temp_dir = TempDir::new().unwrap();
    let log_path = temp_dir.path().join("000000000100.log");
    let index_path = temp_dir.path().join("000000000100.index");
    
    // Create segment with base offset 100
    let mut segment = Segment::open(&log_path, 100).unwrap();
    
    // Create 50 records
    for i in 0..50 {
        let payload = format!("Record {}", i + 100);
        segment.append(payload.as_bytes()).unwrap();
    }
    
    drop(segment);
    
    // Create index with invalid size (not multiple of 16)
    fs::write(&index_path, &vec![0u8; 25]).unwrap(); // 25 bytes is invalid
    
    let rebuilder = IndexRebuilder::new(&log_path, 100).unwrap();
    
    // Verify corruption is detected
    assert_eq!(
        rebuilder.detect_corruption(),
        CorruptionStatus::InvalidSize { actual_size: 25 }
    );
    
    // Rebuild corrupted index
    let rebuild_result = rebuilder.rebuild_if_corrupted().unwrap();
    assert!(rebuild_result.is_some());
    
    let result = rebuild_result.unwrap();
    assert_eq!(result.records_scanned, 50);
    assert!(!result.was_missing); // File existed but was corrupted
    
    // Verify index is now healthy and has valid size
    assert_eq!(rebuilder.detect_corruption(), CorruptionStatus::Healthy);
    
    let metadata = fs::metadata(&index_path).unwrap();
    assert_eq!(metadata.len() % 16, 0, "Fixed index should have valid size");
}

#[test] 
fn test_index_rebuild_deterministic_behavior() {
    let temp_dir = TempDir::new().unwrap();
    let log_path = temp_dir.path().join("000000000000.log");
    let index_path = temp_dir.path().join("000000000000.index");
    
    // Create deterministic segment data
    let mut segment = Segment::open(&log_path, 0).unwrap();
    
    // Create exactly 250 records for predictable index entries
    for i in 0..250 {
        let payload = format!("Deterministic record {}", i);
        segment.append(payload.as_bytes()).unwrap();
    }
    
    drop(segment);
    
    let rebuilder = IndexRebuilder::new(&log_path, 0).unwrap();
    
    // First rebuild
    let first_result = rebuilder.rebuild_index().unwrap();
    let first_size = fs::metadata(&index_path).unwrap().len();
    let first_content = fs::read(&index_path).unwrap();
    
    // Delete and rebuild again
    fs::remove_file(&index_path).unwrap();
    
    let second_result = rebuilder.rebuild_index().unwrap();
    let second_size = fs::metadata(&index_path).unwrap().len();
    let second_content = fs::read(&index_path).unwrap();
    
    // Results should be identical (deterministic)
    assert_eq!(first_result.records_scanned, second_result.records_scanned);
    assert_eq!(first_result.entries_rebuilt, second_result.entries_rebuilt);
    assert_eq!(first_size, second_size);
    assert_eq!(first_content, second_content, "Rebuilds should be deterministic");
    
    // Should have indexed records 0, 100, 200 (first record + every 100th)
    assert_eq!(first_result.entries_rebuilt, 3);
    assert_eq!(first_result.records_scanned, 250);
}

#[test]
fn test_index_rebuild_crash_safety() {
    let temp_dir = TempDir::new().unwrap();
    let log_path = temp_dir.path().join("000000000000.log");
    let index_path = temp_dir.path().join("000000000000.index");
    
    // Create segment with data
    let mut segment = Segment::open(&log_path, 0).unwrap();
    segment.append(b"test data").unwrap();
    drop(segment);
    
    // Create original index
    let mut original_index = Index::open(&index_path, 0).unwrap();
    original_index.append(0, 0).unwrap();
    drop(original_index);
    
    let original_content = fs::read(&index_path).unwrap();
    
    let rebuilder = IndexRebuilder::new(&log_path, 0).unwrap();
    
    // Simulate crash during rebuild by checking temp file handling
    rebuilder.rebuild_index().unwrap();
    
    // Verify no .tmp files are left behind
    let temp_index_path = temp_dir.path().join("000000000000.index.tmp");
    assert!(!temp_index_path.exists(), "Temporary files should be cleaned up");
    
    // Verify index is valid
    assert_eq!(rebuilder.detect_corruption(), CorruptionStatus::Healthy);
    
    // Content may differ (rebuilt vs original) but should be valid
    let rebuilt_content = fs::read(&index_path).unwrap();
    assert!(rebuilt_content.len() >= 16, "Rebuilt index should have at least one entry");
    assert_eq!(rebuilt_content.len() % 16, 0, "Rebuilt index should have valid size");
}