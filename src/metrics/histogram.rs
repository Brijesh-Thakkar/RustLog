use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use super::registry::LatencyPercentiles;

/// Lock-free histogram using ring buffer and atomic operations.
/// 
/// Design:
/// - Fixed-size ring buffer (1024 samples)
/// - Lock-free writes using atomic index
/// - Approximate percentiles (sufficient for monitoring)
/// - O(1) record operation (no sorting on write)
/// - O(n log n) percentile calculation (only on read/snapshot)
/// 
/// Why ring buffer instead of HdrHistogram?
/// - Simpler implementation
/// - Zero external dependencies
/// - Sufficient for broker monitoring
/// - HdrHistogram has more overhead
/// 
/// Why 1024 samples?
/// - Good balance between memory and statistical accuracy
/// - 1024 * 8 bytes = 8KB per histogram
/// - Old samples naturally age out
/// 
/// Trade-offs:
/// - Percentiles are approximate (sampling based)
/// - Very old samples may be overwritten
/// - Acceptable for real-time monitoring (not audit logs)
const RING_SIZE: usize = 1024;

pub struct Histogram {
    // Ring buffer of samples
    samples: Box<[AtomicU64; RING_SIZE]>,
    
    // Current write index (wraps around at RING_SIZE)
    index: AtomicUsize,
}

impl Histogram {
    /// Create a new histogram.
    pub fn new() -> Self {
        Self {
            samples: Box::new([(); RING_SIZE].map(|_| AtomicU64::new(0))),
            index: AtomicUsize::new(0),
        }
    }
    
    /// Record a sample (nanoseconds).
    /// 
    /// Lock-free operation:
    /// 1. Atomically increment index and get slot
    /// 2. Write sample to slot
    /// 
    /// Why Ordering::Relaxed?
    /// - We don't need synchronization between different histogram samples
    /// - Each sample is independent
    /// - Worst case: slightly stale percentiles (acceptable for metrics)
    #[inline]
    pub fn record(&self, nanos: u64) {
        // Get next slot (wraps around automatically with modulo)
        let idx = self.index.fetch_add(1, Ordering::Relaxed) % RING_SIZE;
        
        // Write sample to slot
        self.samples[idx].store(nanos, Ordering::Relaxed);
    }
    
    /// Calculate percentiles from current samples.
    /// 
    /// Algorithm:
    /// 1. Snapshot all samples into Vec
    /// 2. Sort samples
    /// 3. Pick percentile values
    /// 
    /// Cost: O(n log n) where n = RING_SIZE
    /// 
    /// Why not maintain sorted structure?
    /// - Sorting on write would block hot path
    /// - Reads are infrequent (metrics export)
    /// - 1024 elements sorts in ~10 microseconds
    pub fn percentiles(&self) -> LatencyPercentiles {
        // Snapshot all samples
        let mut values: Vec<u64> = self.samples
            .iter()
            .map(|s| s.load(Ordering::Relaxed))
            .filter(|&v| v > 0) // Filter uninitialized slots
            .collect();
        
        if values.is_empty() {
            return LatencyPercentiles::default();
        }
        
        // Sort samples
        values.sort_unstable();
        
        let len = values.len();
        
        // Calculate percentile indices
        let p50_idx = (len * 50) / 100;
        let p95_idx = (len * 95) / 100;
        let p99_idx = (len * 99) / 100;
        
        LatencyPercentiles {
            p50: values.get(p50_idx.saturating_sub(1)).copied().unwrap_or(0),
            p95: values.get(p95_idx.saturating_sub(1)).copied().unwrap_or(0),
            p99: values.get(p99_idx.saturating_sub(1)).copied().unwrap_or(0),
        }
    }
    
    /// Clear all samples.
    /// Used in tests.
    #[allow(dead_code)]
    pub fn clear(&self) {
        for sample in self.samples.iter() {
            sample.store(0, Ordering::Relaxed);
        }
        self.index.store(0, Ordering::Relaxed);
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_histogram_record() {
        let hist = Histogram::new();
        
        // Record 100 samples with value 1000
        for _ in 0..100 {
            hist.record(1000);
        }
        
        let p = hist.percentiles();
        assert_eq!(p.p50, 1000);
        assert_eq!(p.p95, 1000);
        assert_eq!(p.p99, 1000);
    }
    
    #[test]
    fn test_histogram_percentiles() {
        let hist = Histogram::new();
        
        // Record values 1..=100
        for i in 1..=100 {
            hist.record(i * 10); // 10, 20, 30, ..., 1000
        }
        
        let p = hist.percentiles();
        
        // p50 should be around 500 (50th value)
        assert!(p.p50 >= 400 && p.p50 <= 600, "p50 = {}", p.p50);
        
        // p95 should be around 950 (95th value)
        assert!(p.p95 >= 900 && p.p95 <= 1000, "p95 = {}", p.p95);
        
        // p99 should be around 990 (99th value)
        assert!(p.p99 >= 950 && p.p99 <= 1000, "p99 = {}", p.p99);
    }
    
    #[test]
    fn test_histogram_ring_wrap() {
        let hist = Histogram::new();
        
        // Fill ring buffer beyond capacity
        for i in 0..RING_SIZE * 2 {
            hist.record(i as u64);
        }
        
        // Should not panic or corrupt memory
        let p = hist.percentiles();
        assert!(p.p50 > 0);
    }
    
    #[test]
    fn test_histogram_empty() {
        let hist = Histogram::new();
        
        let p = hist.percentiles();
        assert_eq!(p.p50, 0);
        assert_eq!(p.p95, 0);
        assert_eq!(p.p99, 0);
    }
    
    #[test]
    fn test_histogram_concurrency() {
        use std::sync::Arc;
        use std::thread;
        
        let hist = Arc::new(Histogram::new());
        let mut handles = vec![];
        
        // Spawn 10 threads, each recording 100 samples
        for t in 0..10 {
            let h = Arc::clone(&hist);
            let handle = thread::spawn(move || {
                for i in 0..100 {
                    h.record((t * 100 + i) as u64);
                }
            });
            handles.push(handle);
        }
        
        // Wait for all threads
        for h in handles {
            h.join().unwrap();
        }
        
        // Should have recorded 1000 samples without data races
        let p = hist.percentiles();
        assert!(p.p50 > 0);
        assert!(p.p99 > 0);
    }
}
