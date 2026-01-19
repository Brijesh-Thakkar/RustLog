use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use std::sync::RwLock;
use super::histogram::Histogram;

/// Global metrics registry.
/// 
/// Design principles:
/// - Zero allocations on hot path (atomic increments only)
/// - No locks during produce/fetch (atomics only)
/// - Read-only snapshots for metrics export
/// - Lock-free counters, minimal locking for histograms
/// 
/// Why atomic Ordering::Relaxed?
/// - Metrics are inherently approximate
/// - We don't need sequential consistency between different counters
/// - Relaxed is fastest and sufficient for monotonic counters
/// 
/// Why global singleton?
/// - Simplifies access from all hot paths
/// - Avoids Arc/Mutex overhead
/// - Matches production monitoring systems (Prometheus, etc.)
pub struct Metrics {
    // Total counters (monotonically increasing)
    pub messages_produced_total: AtomicU64,
    pub messages_fetched_total: AtomicU64,
    pub bytes_in_total: AtomicU64,
    pub bytes_out_total: AtomicU64,
    
    // Active connections (can increase/decrease)
    pub active_connections: AtomicU64,
    
    // Consumer safety and backpressure counters (Phase 3)
    pub throttled_consumers_total: AtomicU64,
    pub slow_consumers_total: AtomicU64,
    
    // Storage retention counters (Phase 4)
    pub segments_deleted_total: AtomicU64,
    pub bytes_reclaimed_total: AtomicU64,
    pub compaction_runs_total: AtomicU64,
    
    // Cleaner checkpoint counters (Phase 5)
    pub compaction_skipped_segments_total: AtomicU64,
    pub retention_noop_runs_total: AtomicU64,
    pub checkpoint_corruption_total: AtomicU64,
    
    // Compaction metrics (Phase 6)
    pub compaction_segments_rewritten_total: AtomicU64,
    pub compaction_records_dropped_total: AtomicU64,
    pub compaction_bytes_rewritten_total: AtomicU64,
    
    // Index rebuild metrics (Phase 7)
    pub index_rebuilds_total: AtomicU64,
    pub index_corruptions_detected_total: AtomicU64,
    pub index_entries_rebuilt_total: AtomicU64,
    
    // Latency histograms
    // RwLock is acceptable here because:
    // - Writes are infrequent (only during histogram recording)
    // - Histogram has internal lock-free ring buffer
    // - Reads are only for snapshot (not in hot path)
    pub fetch_latency_ns: RwLock<Histogram>,
    pub produce_latency_ns: RwLock<Histogram>,
    
    // Per-partition consumer lag
    // Maps (topic, partition, group_id) -> lag
    // Updated on each fetch request
    pub consumer_lag: RwLock<HashMap<(String, u32, String), u64>>,
}

impl Metrics {
    /// Create a new metrics registry.
    pub fn new() -> Self {
        Self {
            messages_produced_total: AtomicU64::new(0),
            messages_fetched_total: AtomicU64::new(0),
            bytes_in_total: AtomicU64::new(0),
            bytes_out_total: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            throttled_consumers_total: AtomicU64::new(0),
            slow_consumers_total: AtomicU64::new(0),
            segments_deleted_total: AtomicU64::new(0),
            bytes_reclaimed_total: AtomicU64::new(0),
            compaction_runs_total: AtomicU64::new(0),
            compaction_skipped_segments_total: AtomicU64::new(0),
            retention_noop_runs_total: AtomicU64::new(0),
            checkpoint_corruption_total: AtomicU64::new(0),
            compaction_segments_rewritten_total: AtomicU64::new(0),
            compaction_records_dropped_total: AtomicU64::new(0),
            compaction_bytes_rewritten_total: AtomicU64::new(0),
            index_rebuilds_total: AtomicU64::new(0),
            index_corruptions_detected_total: AtomicU64::new(0),
            index_entries_rebuilt_total: AtomicU64::new(0),
            fetch_latency_ns: RwLock::new(Histogram::new()),
            produce_latency_ns: RwLock::new(Histogram::new()),
            consumer_lag: RwLock::new(HashMap::new()),
        }
    }
    
    // ==================== Counter Operations (Hot Path) ====================
    
    /// Increment messages produced counter.
    /// Called on every produce request.
    #[inline]
    pub fn inc_messages_produced(&self, count: u64) {
        self.messages_produced_total.fetch_add(count, Ordering::Relaxed);
    }
    
    /// Increment messages fetched counter.
    /// Called on every fetch response.
    #[inline]
    pub fn inc_messages_fetched(&self, count: u64) {
        self.messages_fetched_total.fetch_add(count, Ordering::Relaxed);
    }
    
    /// Increment bytes in counter.
    /// Called on every produce request.
    #[inline]
    pub fn inc_bytes_in(&self, bytes: u64) {
        self.bytes_in_total.fetch_add(bytes, Ordering::Relaxed);
    }
    
    /// Increment bytes out counter.
    /// Called on every fetch response.
    #[inline]
    pub fn inc_bytes_out(&self, bytes: u64) {
        self.bytes_out_total.fetch_add(bytes, Ordering::Relaxed);
    }
    
    /// Increment active connections.
    /// Called when a connection is accepted.
    #[inline]
    pub fn inc_active_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Decrement active connections.
    /// Called when a connection closes.
    #[inline]
    pub fn dec_active_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
    
    /// Record fetch latency.
    /// Called after each fetch request completes.
    #[inline]
    pub fn record_fetch_latency(&self, nanos: u64) {
        if let Ok(hist) = self.fetch_latency_ns.read() {
            hist.record(nanos);
        }
        // If lock fails, drop measurement (acceptable for metrics)
    }
    
    /// Record produce latency.
    /// Called after each produce request completes.
    #[inline]
    pub fn record_produce_latency(&self, nanos: u64) {
        if let Ok(hist) = self.produce_latency_ns.read() {
            hist.record(nanos);
        }
    }
    
    /// Update consumer lag for a specific partition/group.
    /// Called on each fetch request.
    /// 
    /// Lag = high_water_mark - committed_offset
    pub fn update_consumer_lag(&self, topic: String, partition: u32, group_id: String, lag: u64) {
        if let Ok(mut map) = self.consumer_lag.write() {
            map.insert((topic, partition, group_id), lag);
        }
    }
    
    /// Increment throttled consumers counter.
    /// Called when a consumer is throttled due to rate limiting or in-flight limits.
    #[inline]
    pub fn inc_throttled_consumers(&self) {
        self.throttled_consumers_total.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment slow consumers counter.
    /// Called when a consumer is detected as slow (high lag accumulation).
    #[inline]
    pub fn inc_slow_consumers(&self) {
        self.slow_consumers_total.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment segments deleted counter.
    /// Called when retention deletes old segments.
    #[inline]
    pub fn inc_segments_deleted(&self, count: u64) {
        self.segments_deleted_total.fetch_add(count, Ordering::Relaxed);
    }
    
    /// Increment bytes reclaimed counter.
    /// Called when retention reclaims disk space.
    #[inline]
    pub fn inc_bytes_reclaimed(&self, bytes: u64) {
        self.bytes_reclaimed_total.fetch_add(bytes, Ordering::Relaxed);
    }
    
    /// Increment compaction runs counter.
    /// Called when log compaction completes.
    #[inline]
    pub fn inc_compaction_runs(&self) {
        self.compaction_runs_total.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment compaction skipped segments counter.
    /// Called when segments are skipped because they were already compacted.
    #[inline]
    pub fn inc_compaction_skipped_segments(&self, count: u64) {
        self.compaction_skipped_segments_total.fetch_add(count, Ordering::Relaxed);
    }
    
    /// Increment retention no-op runs counter.
    /// Called when retention runs but deletes no segments.
    #[inline]
    pub fn inc_retention_noop_runs(&self) {
        self.retention_noop_runs_total.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment the checkpoint corruption counter.
    /// This tracks corrupted checkpoint files that needed recovery.
    #[inline]
    pub fn inc_checkpoint_corruption(&self) {
        self.checkpoint_corruption_total.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment the number of segments rewritten during compaction.
    #[inline]
    pub fn inc_compaction_segments_rewritten(&self, count: u64) {
        self.compaction_segments_rewritten_total.fetch_add(count, Ordering::Relaxed);
    }
    
    /// Increment the number of records dropped during compaction.
    #[inline]
    pub fn inc_compaction_records_dropped(&self, count: u64) {
        self.compaction_records_dropped_total.fetch_add(count, Ordering::Relaxed);
    }
    
    /// Increment the bytes rewritten during compaction.
    #[inline]
    pub fn inc_compaction_bytes_rewritten(&self, count: u64) {
        self.compaction_bytes_rewritten_total.fetch_add(count, Ordering::Relaxed);
    }
    
    // ==================== Index Rebuild Counters (Phase 7) ====================
    
    /// Increment index rebuilds counter.
    /// Called whenever an index rebuild operation is performed.
    #[inline]
    pub fn inc_index_rebuilds(&self) {
        self.index_rebuilds_total.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment index corruptions detected counter.
    /// Called when corruption is detected during index validation.
    #[inline]
    pub fn inc_index_corruptions_detected(&self) {
        self.index_corruptions_detected_total.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment index entries rebuilt counter.
    /// Called to track the total number of index entries rebuilt.
    #[inline]
    pub fn inc_index_entries_rebuilt(&self, count: u64) {
        self.index_entries_rebuilt_total.fetch_add(count, Ordering::Relaxed);
    }
    
    // ==================== Snapshot Operations (Export Path) ====================
    
    /// Get a snapshot of all counters.
    /// Called by MetricsFetch request handler.
    pub fn snapshot_counters(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            messages_produced_total: self.messages_produced_total.load(Ordering::Relaxed),
            messages_fetched_total: self.messages_fetched_total.load(Ordering::Relaxed),
            bytes_in_total: self.bytes_in_total.load(Ordering::Relaxed),
            bytes_out_total: self.bytes_out_total.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            throttled_consumers_total: self.throttled_consumers_total.load(Ordering::Relaxed),
            slow_consumers_total: self.slow_consumers_total.load(Ordering::Relaxed),
            segments_deleted_total: self.segments_deleted_total.load(Ordering::Relaxed),
            bytes_reclaimed_total: self.bytes_reclaimed_total.load(Ordering::Relaxed),
            compaction_runs_total: self.compaction_runs_total.load(Ordering::Relaxed),
            compaction_skipped_segments_total: self.compaction_skipped_segments_total.load(Ordering::Relaxed),
            retention_noop_runs_total: self.retention_noop_runs_total.load(Ordering::Relaxed),
            checkpoint_corruption_total: self.checkpoint_corruption_total.load(Ordering::Relaxed),
            compaction_segments_rewritten_total: self.compaction_segments_rewritten_total.load(Ordering::Relaxed),
            compaction_records_dropped_total: self.compaction_records_dropped_total.load(Ordering::Relaxed),
            compaction_bytes_rewritten_total: self.compaction_bytes_rewritten_total.load(Ordering::Relaxed),
            index_rebuilds_total: self.index_rebuilds_total.load(Ordering::Relaxed),
            index_corruptions_detected_total: self.index_corruptions_detected_total.load(Ordering::Relaxed),
            index_entries_rebuilt_total: self.index_entries_rebuilt_total.load(Ordering::Relaxed),
        }
    }
    
    /// Get fetch latency percentiles.
    pub fn fetch_latency_percentiles(&self) -> LatencyPercentiles {
        if let Ok(hist) = self.fetch_latency_ns.read() {
            hist.percentiles()
        } else {
            LatencyPercentiles::default()
        }
    }
    
    /// Get produce latency percentiles.
    pub fn produce_latency_percentiles(&self) -> LatencyPercentiles {
        if let Ok(hist) = self.produce_latency_ns.read() {
            hist.percentiles()
        } else {
            LatencyPercentiles::default()
        }
    }
    
    /// Get consumer lag snapshot.
    pub fn snapshot_consumer_lag(&self) -> Vec<ConsumerLagEntry> {
        if let Ok(map) = self.consumer_lag.read() {
            map.iter()
                .map(|((topic, partition, group_id), lag)| ConsumerLagEntry {
                    topic: topic.clone(),
                    partition: *partition,
                    group_id: group_id.clone(),
                    lag: *lag,
                })
                .collect()
        } else {
            vec![]
        }
    }
    
    /// Export all metrics in Prometheus text format.
    /// 
    /// Format:
    /// # HELP metric_name Description
    /// # TYPE metric_name counter
    /// metric_name value
    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();
        
        let snapshot = self.snapshot_counters();
        let fetch_p = self.fetch_latency_percentiles();
        let produce_p = self.produce_latency_percentiles();
        let lag = self.snapshot_consumer_lag();
        
        // Counters
        output.push_str("# HELP messages_produced_total Total number of messages produced\n");
        output.push_str("# TYPE messages_produced_total counter\n");
        output.push_str(&format!("messages_produced_total {}\n", snapshot.messages_produced_total));
        
        output.push_str("# HELP messages_fetched_total Total number of messages fetched\n");
        output.push_str("# TYPE messages_fetched_total counter\n");
        output.push_str(&format!("messages_fetched_total {}\n", snapshot.messages_fetched_total));
        
        output.push_str("# HELP bytes_in_total Total bytes received (produced)\n");
        output.push_str("# TYPE bytes_in_total counter\n");
        output.push_str(&format!("bytes_in_total {}\n", snapshot.bytes_in_total));
        
        output.push_str("# HELP bytes_out_total Total bytes sent (fetched)\n");
        output.push_str("# TYPE bytes_out_total counter\n");
        output.push_str(&format!("bytes_out_total {}\n", snapshot.bytes_out_total));
        
        output.push_str("# HELP active_connections Current number of active TCP connections\n");
        output.push_str("# TYPE active_connections gauge\n");
        output.push_str(&format!("active_connections {}\n", snapshot.active_connections));
        
        // Phase 3: Consumer safety and backpressure metrics
        output.push_str("# HELP throttled_consumers_total Total number of consumer throttle events\n");
        output.push_str("# TYPE throttled_consumers_total counter\n");
        output.push_str(&format!("throttled_consumers_total {}\n", snapshot.throttled_consumers_total));
        
        output.push_str("# HELP slow_consumers_total Total number of slow consumer detections\n");
        output.push_str("# TYPE slow_consumers_total counter\n");
        output.push_str(&format!("slow_consumers_total {}\n", snapshot.slow_consumers_total));
        
        // Phase 4: Storage retention metrics
        output.push_str("# HELP segments_deleted_total Total number of segments deleted by retention\n");
        output.push_str("# TYPE segments_deleted_total counter\n");
        output.push_str(&format!("segments_deleted_total {}\n", snapshot.segments_deleted_total));
        
        output.push_str("# HELP bytes_reclaimed_total Total bytes reclaimed by retention and compaction\n");
        output.push_str("# TYPE bytes_reclaimed_total counter\n");
        output.push_str(&format!("bytes_reclaimed_total {}\n", snapshot.bytes_reclaimed_total));
        
        output.push_str("# HELP compaction_runs_total Total number of log compaction runs\n");
        output.push_str("# TYPE compaction_runs_total counter\n");
        output.push_str(&format!("compaction_runs_total {}\n", snapshot.compaction_runs_total));
        
        output.push_str("# HELP compaction_skipped_segments_total Total number of segments skipped during compaction\n");
        output.push_str("# TYPE compaction_skipped_segments_total counter\n");
        output.push_str(&format!("compaction_skipped_segments_total {}\n", snapshot.compaction_skipped_segments_total));
        
        output.push_str("# HELP retention_noop_runs_total Total number of retention runs that deleted no segments\n");
        output.push_str("# TYPE retention_noop_runs_total counter\n");
        output.push_str(&format!("retention_noop_runs_total {}\n", snapshot.retention_noop_runs_total));
        
        output.push_str("# HELP checkpoint_corruption_total Total number of corrupted checkpoint files recovered\n");
        output.push_str("# TYPE checkpoint_corruption_total counter\n");
        output.push_str(&format!("checkpoint_corruption_total {}\n", snapshot.checkpoint_corruption_total));
        
        output.push_str("# HELP compaction_segments_rewritten_total Total number of segments rewritten during compaction\n");
        output.push_str("# TYPE compaction_segments_rewritten_total counter\n");
        output.push_str(&format!("compaction_segments_rewritten_total {}\n", snapshot.compaction_segments_rewritten_total));
        
        output.push_str("# HELP compaction_records_dropped_total Total number of records dropped during compaction\n");
        output.push_str("# TYPE compaction_records_dropped_total counter\n");
        output.push_str(&format!("compaction_records_dropped_total {}\n", snapshot.compaction_records_dropped_total));
        
        output.push_str("# HELP compaction_bytes_rewritten_total Total bytes rewritten during compaction\n");
        output.push_str("# TYPE compaction_bytes_rewritten_total counter\n");
        output.push_str(&format!("compaction_bytes_rewritten_total {}\n", snapshot.compaction_bytes_rewritten_total));
        
        // Fetch latency percentiles
        output.push_str("# HELP fetch_latency_ns_p50 Fetch latency 50th percentile (ns)\n");
        output.push_str("# TYPE fetch_latency_ns_p50 gauge\n");
        output.push_str(&format!("fetch_latency_ns_p50 {}\n", fetch_p.p50));
        
        output.push_str("# HELP fetch_latency_ns_p95 Fetch latency 95th percentile (ns)\n");
        output.push_str("# TYPE fetch_latency_ns_p95 gauge\n");
        output.push_str(&format!("fetch_latency_ns_p95 {}\n", fetch_p.p95));
        
        output.push_str("# HELP fetch_latency_ns_p99 Fetch latency 99th percentile (ns)\n");
        output.push_str("# TYPE fetch_latency_ns_p99 gauge\n");
        output.push_str(&format!("fetch_latency_ns_p99 {}\n", fetch_p.p99));
        
        // Produce latency percentiles
        output.push_str("# HELP produce_latency_ns_p50 Produce latency 50th percentile (ns)\n");
        output.push_str("# TYPE produce_latency_ns_p50 gauge\n");
        output.push_str(&format!("produce_latency_ns_p50 {}\n", produce_p.p50));
        
        output.push_str("# HELP produce_latency_ns_p95 Produce latency 95th percentile (ns)\n");
        output.push_str("# TYPE produce_latency_ns_p95 gauge\n");
        output.push_str(&format!("produce_latency_ns_p95 {}\n", produce_p.p95));
        
        output.push_str("# HELP produce_latency_ns_p99 Produce latency 99th percentile (ns)\n");
        output.push_str("# TYPE produce_latency_ns_p99 gauge\n");
        output.push_str(&format!("produce_latency_ns_p99 {}\n", produce_p.p99));
        
        // Consumer lag (labeled metric)
        output.push_str("# HELP consumer_lag Consumer lag per partition and group\n");
        output.push_str("# TYPE consumer_lag gauge\n");
        for entry in lag {
            output.push_str(&format!(
                "consumer_lag{{topic=\"{}\",partition=\"{}\",group=\"{}\"}} {}\n",
                entry.topic, entry.partition, entry.group_id, entry.lag
            ));
        }
        
        output
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of counter values at a point in time.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub messages_produced_total: u64,
    pub messages_fetched_total: u64,
    pub bytes_in_total: u64,
    pub bytes_out_total: u64,
    pub active_connections: u64,
    pub throttled_consumers_total: u64,
    pub slow_consumers_total: u64,
    pub segments_deleted_total: u64,
    pub bytes_reclaimed_total: u64,
    pub compaction_runs_total: u64,
    pub compaction_skipped_segments_total: u64,
    pub retention_noop_runs_total: u64,
    pub checkpoint_corruption_total: u64,
    pub compaction_segments_rewritten_total: u64,
    pub compaction_records_dropped_total: u64,
    pub compaction_bytes_rewritten_total: u64,
    pub index_rebuilds_total: u64,
    pub index_corruptions_detected_total: u64,
    pub index_entries_rebuilt_total: u64,
}

/// Latency percentiles (in nanoseconds).
#[derive(Debug, Clone, Default)]
pub struct LatencyPercentiles {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
}

/// Consumer lag entry for a specific partition/group.
#[derive(Debug, Clone)]
pub struct ConsumerLagEntry {
    pub topic: String,
    pub partition: u32,
    pub group_id: String,
    pub lag: u64,
}

// Global metrics singleton.
// 
// Why use lazy_static instead of OnceCell?
// - lazy_static is well-tested and widely used
// - OnceCell requires nightly for const initialization
// - For metrics, initialization cost is negligible
lazy_static::lazy_static! {
    pub static ref METRICS: Metrics = Metrics::new();
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_counter_increments() {
        let m = Metrics::new();
        
        m.inc_messages_produced(10);
        m.inc_messages_produced(5);
        assert_eq!(m.messages_produced_total.load(Ordering::Relaxed), 15);
        
        m.inc_bytes_in(1024);
        m.inc_bytes_in(512);
        assert_eq!(m.bytes_in_total.load(Ordering::Relaxed), 1536);
    }
    
    #[test]
    fn test_active_connections() {
        let m = Metrics::new();
        
        m.inc_active_connections();
        m.inc_active_connections();
        assert_eq!(m.active_connections.load(Ordering::Relaxed), 2);
        
        m.dec_active_connections();
        assert_eq!(m.active_connections.load(Ordering::Relaxed), 1);
    }
    
    #[test]
    fn test_snapshot() {
        let m = Metrics::new();
        
        m.inc_messages_produced(100);
        m.inc_messages_fetched(50);
        m.inc_bytes_in(4096);
        m.inc_bytes_out(2048);
        m.inc_active_connections();
        
        let snapshot = m.snapshot_counters();
        assert_eq!(snapshot.messages_produced_total, 100);
        assert_eq!(snapshot.messages_fetched_total, 50);
        assert_eq!(snapshot.bytes_in_total, 4096);
        assert_eq!(snapshot.bytes_out_total, 2048);
        assert_eq!(snapshot.active_connections, 1);
    }
    
    #[test]
    fn test_consumer_lag() {
        let m = Metrics::new();
        
        m.update_consumer_lag("test-topic".to_string(), 0, "group1".to_string(), 100);
        m.update_consumer_lag("test-topic".to_string(), 1, "group1".to_string(), 200);
        
        let lag = m.snapshot_consumer_lag();
        assert_eq!(lag.len(), 2);
    }
    
    #[test]
    fn test_prometheus_export() {
        let m = Metrics::new();
        
        m.inc_messages_produced(42);
        m.inc_bytes_out(8192);
        
        let export = m.export_prometheus();
        assert!(export.contains("messages_produced_total 42"));
        assert!(export.contains("bytes_out_total 8192"));
        assert!(export.contains("# HELP"));
        assert!(export.contains("# TYPE"));
    }
}
