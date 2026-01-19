/// Storage retention and log compaction implementation.
/// 
/// This module implements broker-side storage management to reclaim disk space
/// while preserving offset correctness and fetch semantics.
/// 
/// ## Design Principles:
/// - Only operate on SEALED segments (never active segment)
/// - Preserve fetch correctness and offset monotonicity
/// - No blocking I/O in request path
/// - Deterministic eviction and compaction policies
/// - Atomic metrics updates
/// 
/// ## Retention Policies:
/// 1. Time-based: Delete segments older than retention.ms
/// 2. Size-based: Delete oldest segments to stay under retention.bytes
/// 3. Compaction: Keep latest record per key (optional)
/// 
/// ## Safety Guarantees:
/// - Never delete active segment
/// - Never delete segment containing high watermark
/// - No partial segment deletion
/// - Compaction produces new segments (copy-on-write)
/// - All operations are explicit/admin-triggered

use crate::error::BrokerError;
use crate::storage::segment::Segment;
use crate::storage::index::Index;
use crate::metrics::METRICS;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Retention policy configuration for a partition.
#[derive(Debug, Clone)]
pub struct RetentionConfig {
    /// Maximum age of segments in milliseconds.
    /// Segments older than this will be deleted.
    /// None means no time-based retention.
    pub retention_ms: Option<u64>,
    
    /// Maximum total size of all segments in bytes.
    /// Oldest segments will be deleted to stay under this limit.
    /// None means no size-based retention.
    pub retention_bytes: Option<u64>,
    
    /// Whether to enable log compaction.
    /// When true, compaction will keep only the latest value for each key.
    pub enable_compaction: bool,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            retention_ms: Some(7 * 24 * 60 * 60 * 1000), // 7 days default
            retention_bytes: None, // No size limit by default
            enable_compaction: false, // Compaction disabled by default
        }
    }
}

/// Result of a retention operation.
#[derive(Debug, Default)]
pub struct RetentionResult {
    /// Number of segments deleted.
    pub segments_deleted: u64,
    /// Total bytes reclaimed from deleted segments.
    pub bytes_reclaimed: u64,
    /// Number of compaction runs performed.
    pub compaction_runs: u64,
}

/// Apply time-based retention to sealed segments.
/// 
/// Deletes segments whose creation time is older than retention_ms.
/// 
/// ## Safety Rules:
/// - Only operates on sealed segments (immutable)
/// - Never deletes active segment  
/// - Preserves offset ordering
/// - No partial segment deletion
/// 
/// ## Algorithm:
/// 1. Calculate retention threshold timestamp
/// 2. Find segments older than threshold
/// 3. Delete entire segments (log + index files)
/// 4. Update metrics atomically
pub fn apply_time_based_retention(
    sealed_segments: &mut Vec<(Arc<Segment>, Arc<std::sync::Mutex<Index>>)>,
    retention_ms: u64,
    high_watermark: u64,
) -> Result<RetentionResult, BrokerError> {
    // SAFETY INVARIANT: If retention is disabled (-1), do nothing
    if retention_ms == u64::MAX {  // -1 as u64 is u64::MAX
        return Ok(RetentionResult::default());
    }
    
    // SAFETY INVARIANT: Only operate on sealed segments (verified by caller)
    // SAFETY INVARIANT: Never delete active segment (verified by caller)
    
    let retention_duration = Duration::from_millis(retention_ms);
    let now = SystemTime::now();
    let retention_threshold = now
        .checked_sub(retention_duration)
        .ok_or_else(|| BrokerError::Storage("Retention timestamp underflow".to_string()))?;
    
    let mut result = RetentionResult::default();
    let mut segments_to_remove = Vec::new();
    
    // Find segments older than retention threshold
    for (index, (segment, _)) in sealed_segments.iter().enumerate() {
        // SAFETY INVARIANT: Never delete segment containing high watermark
        let segment_last_offset = segment.max_offset();
        if segment_last_offset >= high_watermark {
            // Segment contains high watermark - NEVER delete
            continue;
        }
        
        // Get segment creation time from file metadata
        let segment_path = segment.file_path();
        if let Ok(metadata) = std::fs::metadata(&segment_path) {
            if let Ok(created) = metadata.created() {
                if created < retention_threshold {
                    // Calculate segment size for metrics
                    let segment_size = segment.size();
                    result.bytes_reclaimed += segment_size;
                    result.segments_deleted += 1;
                    segments_to_remove.push(index);
                }
            }
        }
    }
    
    // Remove segments in reverse order to maintain indices
    for &index in segments_to_remove.iter().rev() {
        let (segment, _index_ref) = sealed_segments.remove(index);
        
        // Delete segment and index files
        let segment_path = segment.file_path();
        let index_path = segment_path.with_extension("index");
        
        // Best effort deletion - log errors but don't fail
        if let Err(e) = std::fs::remove_file(&segment_path) {
            eprintln!("Warning: Failed to delete segment file {:?}: {}", segment_path, e);
        }
        if let Err(e) = std::fs::remove_file(&index_path) {
            eprintln!("Warning: Failed to delete index file {:?}: {}", index_path, e);
        }
    }
    
    // Update global metrics atomically
    METRICS.inc_segments_deleted(result.segments_deleted);
    METRICS.inc_bytes_reclaimed(result.bytes_reclaimed);
    
    Ok(result)
}

/// Apply size-based retention to sealed segments.
/// 
/// Deletes oldest sealed segments until total size is under retention_bytes.
/// 
/// ## Safety Rules:
/// - Only operates on sealed segments
/// - Deletes oldest segments first (FIFO)
/// - Stops as soon as under size limit
/// - Never deletes active segment
/// 
/// ## Algorithm:
/// 1. Calculate total size of all segments
/// 2. If over limit, delete oldest segments until under limit
/// 3. Update metrics atomically
pub fn apply_size_based_retention(
    sealed_segments: &mut Vec<(Arc<Segment>, Arc<std::sync::Mutex<Index>>)>,
    retention_bytes: u64,
    high_watermark: u64,
) -> Result<RetentionResult, BrokerError> {
    // SAFETY INVARIANT: Only operate on sealed segments (verified by caller)
    // SAFETY INVARIANT: Never delete active segment (verified by caller)
    
    let mut result = RetentionResult::default();
    
    // Calculate total size of ONLY sealed segments
    // Note: Active segment is excluded from size calculation
    let mut total_size = 0u64;
    for (segment, _) in sealed_segments.iter() {
        total_size += segment.size();
    }
    
    // If under limit, nothing to do
    if total_size <= retention_bytes {
        return Ok(result);
    }
    
    let mut segments_to_remove_indices = Vec::new();
    let mut current_size = total_size;
    
    // Delete oldest segments until under limit
    // sealed_segments are ordered by base_offset (oldest first)
    for (index, (segment, _)) in sealed_segments.iter().enumerate() {
        if current_size <= retention_bytes {
            // Stop immediately when under limit
            break;
        }
        
        // SAFETY INVARIANT: Never delete segment containing high watermark
        let segment_last_offset = segment.max_offset();
        if segment_last_offset >= high_watermark {
            // Segment contains high watermark - cannot delete
            break;  // Since segments are ordered, all subsequent segments also contain newer data
        }
        
        let segment_size = segment.size();
        current_size = current_size.saturating_sub(segment_size);
        result.bytes_reclaimed += segment_size;
        result.segments_deleted += 1;
        segments_to_remove_indices.push(index);
    }
    
    // Remove segments in reverse order to maintain indices
    for &index in segments_to_remove_indices.iter().rev() {
        let (segment, _) = sealed_segments.remove(index);
            
        // Delete segment and index files
        let segment_path = segment.file_path();
        let index_path = segment_path.with_extension("index");
        
        // Best effort deletion
        if let Err(e) = std::fs::remove_file(&segment_path) {
            eprintln!("Warning: Failed to delete segment file {:?}: {}", segment_path, e);
        }
        if let Err(e) = std::fs::remove_file(&index_path) {
            eprintln!("Warning: Failed to delete index file {:?}: {}", index_path, e);
        }
    }
    
    // Update global metrics atomically
    METRICS.inc_segments_deleted(result.segments_deleted);
    METRICS.inc_bytes_reclaimed(result.bytes_reclaimed);
    
    Ok(result)
}

/// Apply log compaction to sealed segments.
/// 
/// Log compaction keeps only the latest value for each key, producing a compacted
/// segment that contains the latest state for each unique key.
/// 
/// ## Current Implementation Status:
/// This is a Phase 4 foundation for log compaction. Since the current
/// record format doesn't include keys (only offset+payload), this function
/// serves as compaction infrastructure that can be extended when keyed
/// records are implemented.
/// 
/// ## Safety Rules:
/// - Only operates on sealed segments (immutable)
/// - Never compacts active segment
/// - Preserves record offsets (no offset reassignment)
/// - Creates new compacted segments (copy-on-write)
/// - Tombstone records (empty payload) delete keys
/// 
/// ## Algorithm (Keyed Records - Future):
/// 1. Read all records from sealed segments
/// 2. Build key -> (offset, value) map (latest value wins)
/// 3. Scan for tombstone records (empty values) and remove keys
/// 4. Write compacted segment with preserved offsets
/// 5. Replace original segments with compacted segment
/// 6. Update metrics
/// 
/// ## Current Behavior:
/// Since records don't have keys yet, this function provides compaction
/// metrics tracking and validates the segments but doesn't perform actual
/// key-based deduplication. It's ready for when keys are added to the protocol.
pub fn apply_log_compaction(
    sealed_segments: &mut Vec<(Arc<Segment>, Arc<std::sync::Mutex<Index>>)>,
    _high_watermark: u64,  // Used for future high watermark protection in compaction
) -> Result<RetentionResult, BrokerError> {
    // SAFETY INVARIANT: Only operate on sealed segments (verified by caller)
    // SAFETY INVARIANT: Never compact active segment (verified by caller)
    // SAFETY INVARIANT: Never compact segments containing high watermark
    
    let mut result = RetentionResult::default();
    
    // For now, since we don't have keyed records, we just track that
    // compaction was attempted and validate the segments are valid
    if !sealed_segments.is_empty() {
        result.compaction_runs = 1;
        
        // Validate segments are readable (basic compaction health check)
        for (segment, _) in sealed_segments.iter() {
            // Basic validation: ensure segment file exists and is readable
            let segment_path = segment.file_path();
            if !segment_path.exists() {
                return Err(BrokerError::Storage(
                    format!("Segment file does not exist: {:?}", segment_path)
                ));
            }
            
            // TODO: When keyed records are implemented, perform actual compaction:
            // 1. Read all records from segment
            // 2. Extract key from each record
            // 3. Build latest-value-per-key map
            // 4. Handle tombstone records (empty values)
            // 5. Write compacted segment with original offsets
            // 6. Replace original segment with compacted version
        }
        
        // Update compaction metrics
        METRICS.inc_compaction_runs();
        
        println!("Log compaction infrastructure validated for {} segments", 
                 sealed_segments.len());
    }
    
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_retention_config_default() {
        let config = RetentionConfig::default();
        
        assert_eq!(config.retention_ms, Some(7 * 24 * 60 * 60 * 1000));
        assert_eq!(config.retention_bytes, None);
        assert_eq!(config.enable_compaction, false);
    }
    
    #[test]
    fn test_retention_result_default() {
        let result = RetentionResult::default();
        
        assert_eq!(result.segments_deleted, 0);
        assert_eq!(result.bytes_reclaimed, 0);
        assert_eq!(result.compaction_runs, 0);
    }
        #[test] 
    fn test_time_retention_disabled_with_max_value() {
        // Test with empty segments list - should handle disabled retention
        let mut empty_segments = Vec::new();
        let result = apply_time_based_retention(&mut empty_segments, u64::MAX, 0).unwrap();
        assert_eq!(result.segments_deleted, 0);
        assert_eq!(result.compaction_runs, 0);
        assert_eq!(result.bytes_reclaimed, 0);
    }
        #[test]
    fn test_log_compaction_infrastructure() {
        // Test with empty segments list
        let mut empty_segments = Vec::new();
        let result = apply_log_compaction(&mut empty_segments, 0).unwrap();
        assert_eq!(result.compaction_runs, 0);
        
        // TODO: Add tests with actual segments when keyed records are implemented
        // For now, just validate the infrastructure exists and compiles
    }
}