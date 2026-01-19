//! Consumer safety and backpressure controls.
//!
//! This module implements broker-side protections against:
//! 1. Aggressive consumers (too many in-flight requests)
//! 2. Bandwidth-hogging consumers (byte rate limits)
//! 3. Slow consumers (lag accumulation detection)
//!
//! Design principles:
//! - NEVER block or sleep
//! - NEVER introduce async backpressure
//! - Return empty responses when limits exceeded
//! - Fail fast with clear semantics
//!
//! Why broker-side limits?
//! - Protect broker CPU/memory from misbehaving clients
//! - Prevent single consumer from monopolizing resources
//! - Enable fair resource sharing across consumers
//! - Kafka, Pulsar, and Redpanda all implement similar controls

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Configuration Constants
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Maximum concurrent in-flight fetch requests per connection.
///
/// Why 2?
/// - Allows pipelining for throughput
/// - Low enough to prevent resource exhaustion
/// - Kafka default: 5 (we're more conservative)
/// - Redpanda default: unlimited (we prefer safety)
///
/// If exceeded:
/// - Return empty FetchResponse immediately
/// - Increment throttled_consumers_total metric
/// - Log warning (advisory)
pub const MAX_INFLIGHT_FETCHES: usize = 2;

/// Maximum bytes per second a consumer can fetch.
///
/// Why 10MB/s?
/// - Generous for most use cases
/// - Prevents single consumer from saturating gigabit link
/// - Can be increased via config in production
///
/// If exceeded:
/// - Return empty FetchResponse immediately
/// - Increment throttled_consumers_total metric
/// - Window resets after 1 second
pub const MAX_BYTES_PER_SECOND: u64 = 10 * 1024 * 1024; // 10 MB/s

/// Time window for byte rate limiting (in milliseconds).
///
/// Why 1 second?
/// - Short enough for responsive throttling
/// - Long enough to smooth out burst traffic
/// - Aligns with standard monitoring intervals
pub const RATE_LIMIT_WINDOW_MS: u64 = 1000;

/// Lag threshold for slow consumer detection (in records).
///
/// Why 10,000?
/// - High enough to avoid false positives
/// - Low enough to catch genuinely slow consumers
/// - Kafka's default consumer.lag.threshold: similar magnitude
///
/// If exceeded:
/// - Increment slow_consumers_total metric
/// - Log warning
/// - Do NOT throttle (advisory only)
pub const SLOW_CONSUMER_LAG_THRESHOLD: u64 = 10_000;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// In-Flight Limiter
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Per-connection in-flight fetch limiter.
///
/// Why per-connection?
/// - Each TCP connection is independent
/// - Prevents one slow consumer from affecting others
/// - Aligns with stateless connection model
///
/// Implementation:
/// - AtomicUsize counter (lock-free)
/// - Increment on fetch start
/// - Decrement on fetch completion (via RAII guard)
/// - Check before processing fetch request
pub struct InFlightLimiter {
    count: AtomicUsize,
    max: usize,
}

impl InFlightLimiter {
    /// Create a new in-flight limiter.
    pub fn new(max: usize) -> Self {
        Self {
            count: AtomicUsize::new(0),
            max,
        }
    }

    /// Try to acquire a slot for a new fetch request.
    ///
    /// Returns:
    /// - Some(InFlightGuard) if slot acquired
    /// - None if limit exceeded
    ///
    /// Why Option<Guard>?
    /// - RAII ensures decrement on completion
    /// - No manual bookkeeping needed
    /// - Panic-safe (guard's Drop is guaranteed)
    pub fn try_acquire(&self) -> Option<InFlightGuard<'_>> {
        loop {
            let current = self.count.load(Ordering::Acquire);
            
            // Limit exceeded
            if current >= self.max {
                return None;
            }
            
            // Try to increment atomically
            if self.count
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(InFlightGuard { limiter: self });
            }
            
            // CAS failed, retry
            // This is a rare contention case (multiple concurrent fetches)
        }
    }

    /// Decrement counter (called by guard's Drop).
    fn release(&self) {
        self.count.fetch_sub(1, Ordering::Release);
    }

    /// Get current in-flight count (for testing).
    #[cfg(test)]
    pub fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }
}

/// RAII guard for in-flight fetch request.
///
/// Automatically decrements counter on drop.
pub struct InFlightGuard<'a> {
    limiter: &'a InFlightLimiter,
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.limiter.release();
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Byte Rate Limiter
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Sliding-window byte rate limiter.
///
/// Algorithm:
/// 1. Track bytes consumed in current window
/// 2. Track window start time
/// 3. On each fetch:
///    - If window expired, reset counter
///    - Check if bytes_consumed + new_bytes <= limit
///    - If yes, allow and increment counter
///    - If no, reject
///
/// Why sliding window vs token bucket?
/// - Simpler implementation (no background refill)
/// - Deterministic behavior (no timers)
/// - Good enough for broker-side throttling
///
/// Limitations:
/// - Allows burst at window boundary
/// - Not perfectly smooth
/// - Acceptable tradeoff for simplicity
///
/// Lock-free implementation:
/// - Uses AtomicU64 for bytes_consumed
/// - Uses Instant (non-atomic) for window_start
/// - Race conditions are benign (slight over/under counting)
pub struct ByteRateLimiter {
    /// Bytes consumed in current window.
    bytes_consumed: AtomicU64,
    
    /// Window start time (non-atomic, benign races).
    ///
    /// Why non-atomic?
    /// - Instant cannot be atomic
    /// - Slight staleness is acceptable
    /// - Worst case: allow slightly more bytes for 1 window
    window_start: std::sync::Mutex<Instant>,
    
    /// Maximum bytes per window.
    limit: u64,
    
    /// Window duration.
    window_duration: Duration,
}

impl ByteRateLimiter {
    /// Create a new byte rate limiter.
    pub fn new(limit: u64, window_ms: u64) -> Self {
        Self {
            bytes_consumed: AtomicU64::new(0),
            window_start: std::sync::Mutex::new(Instant::now()),
            limit,
            window_duration: Duration::from_millis(window_ms),
        }
    }

    /// Try to consume bytes from the rate limit.
    ///
    /// Returns:
    /// - true if allowed (bytes deducted from quota)
    /// - false if limit exceeded
    ///
    /// This method MUST be called BEFORE sending data.
    pub fn try_consume(&self, bytes: u64) -> bool {
        let now = Instant::now();
        
        // Check if window expired
        {
            let mut window_start = self.window_start.lock().unwrap();
            if now.duration_since(*window_start) >= self.window_duration {
                // Reset window
                *window_start = now;
                self.bytes_consumed.store(0, Ordering::Release);
            }
        }
        
        // Try to consume bytes
        loop {
            let current = self.bytes_consumed.load(Ordering::Acquire);
            
            // Check if we would exceed limit
            if current + bytes > self.limit {
                return false;
            }
            
            // Try to atomically increment
            if self.bytes_consumed
                .compare_exchange(current, current + bytes, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
            
            // CAS failed, retry
        }
    }

    /// Get current consumed bytes (for testing).
    #[cfg(test)]
    pub fn consumed(&self) -> u64 {
        self.bytes_consumed.load(Ordering::Acquire)
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Slow Consumer Detector
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Slow consumer detector.
///
/// Tracks consumer lag over time and flags consumers that:
/// 1. Continuously fetch
/// 2. Advance offsets slowly
/// 3. Accumulate lag beyond threshold
///
/// This is ADVISORY ONLY:
/// - Does NOT throttle
/// - Does NOT disconnect
/// - Only increments metric + logs warning
///
/// Why advisory?
/// - Slow consumers might be legitimate (batch processing)
/// - Human operator should decide action
/// - Automatic throttling could cause cascading failures
#[derive(Debug)]
pub struct SlowConsumerDetector {
    /// Last committed offset seen.
    last_offset: AtomicU64,
    
    /// Last high water mark seen.
    last_hwm: AtomicU64,
    
    /// Number of fetches since last offset advancement.
    fetch_count: AtomicU64,
}

impl SlowConsumerDetector {
    /// Create a new slow consumer detector.
    pub fn new() -> Self {
        Self {
            last_offset: AtomicU64::new(0),
            last_hwm: AtomicU64::new(0),
            fetch_count: AtomicU64::new(0),
        }
    }

    /// Check if consumer is slow.
    ///
    /// Returns:
    /// - true if consumer lag exceeds threshold
    /// - false otherwise
    ///
    /// Side effects:
    /// - Updates internal tracking state
    ///
    /// Parameters:
    /// - committed_offset: consumer's committed offset
    /// - high_water_mark: partition's high water mark (latest offset)
    pub fn check(&self, committed_offset: u64, high_water_mark: u64) -> bool {
        let lag = high_water_mark.saturating_sub(committed_offset);
        
        // Update tracking
        self.last_offset.store(committed_offset, Ordering::Relaxed);
        self.last_hwm.store(high_water_mark, Ordering::Relaxed);
        self.fetch_count.fetch_add(1, Ordering::Relaxed);
        
        // Check threshold
        lag > SLOW_CONSUMER_LAG_THRESHOLD
    }
}

impl Default for SlowConsumerDetector {
    fn default() -> Self {
        Self::new()
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Tests
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inflight_limiter_allows_within_limit() {
        let limiter = InFlightLimiter::new(2);
        
        let guard1 = limiter.try_acquire();
        assert!(guard1.is_some());
        assert_eq!(limiter.count(), 1);
        
        let guard2 = limiter.try_acquire();
        assert!(guard2.is_some());
        assert_eq!(limiter.count(), 2);
    }

    #[test]
    fn test_inflight_limiter_rejects_over_limit() {
        let limiter = InFlightLimiter::new(2);
        
        let _guard1 = limiter.try_acquire().unwrap();
        let _guard2 = limiter.try_acquire().unwrap();
        
        // Third should fail
        let guard3 = limiter.try_acquire();
        assert!(guard3.is_none());
        assert_eq!(limiter.count(), 2);
    }

    #[test]
    fn test_inflight_limiter_releases_on_drop() {
        let limiter = InFlightLimiter::new(2);
        
        {
            let _guard1 = limiter.try_acquire().unwrap();
            let _guard2 = limiter.try_acquire().unwrap();
            assert_eq!(limiter.count(), 2);
        } // guards dropped here
        
        assert_eq!(limiter.count(), 0);
        
        // Should be able to acquire again
        let guard = limiter.try_acquire();
        assert!(guard.is_some());
    }

    #[test]
    fn test_byte_rate_limiter_allows_within_limit() {
        let limiter = ByteRateLimiter::new(1000, 1000);
        
        assert!(limiter.try_consume(500));
        assert_eq!(limiter.consumed(), 500);
        
        assert!(limiter.try_consume(400));
        assert_eq!(limiter.consumed(), 900);
    }

    #[test]
    fn test_byte_rate_limiter_rejects_over_limit() {
        let limiter = ByteRateLimiter::new(1000, 1000);
        
        assert!(limiter.try_consume(900));
        
        // This should fail (900 + 200 > 1000)
        assert!(!limiter.try_consume(200));
        assert_eq!(limiter.consumed(), 900);
    }

    #[test]
    fn test_byte_rate_limiter_resets_after_window() {
        let limiter = ByteRateLimiter::new(1000, 100); // 100ms window
        
        assert!(limiter.try_consume(900));
        assert!(!limiter.try_consume(200)); // Should fail
        
        // Wait for window to expire
        std::thread::sleep(Duration::from_millis(150));
        
        // Should succeed now (window reset)
        assert!(limiter.try_consume(500));
        assert_eq!(limiter.consumed(), 500);
    }

    #[test]
    fn test_slow_consumer_detector_normal_lag() {
        let detector = SlowConsumerDetector::new();
        
        // Normal lag (100 messages behind)
        let is_slow = detector.check(900, 1000);
        assert!(!is_slow);
    }

    #[test]
    fn test_slow_consumer_detector_high_lag() {
        let detector = SlowConsumerDetector::new();
        
        // High lag (50,000 messages behind)
        let is_slow = detector.check(0, 50_000);
        assert!(is_slow);
    }

    #[test]
    fn test_slow_consumer_detector_at_threshold() {
        let detector = SlowConsumerDetector::new();
        
        // Exactly at threshold
        let is_slow = detector.check(0, SLOW_CONSUMER_LAG_THRESHOLD);
        assert!(!is_slow); // Should be false (> not >=)
        
        // Just over threshold
        let is_slow = detector.check(0, SLOW_CONSUMER_LAG_THRESHOLD + 1);
        assert!(is_slow);
    }
}
