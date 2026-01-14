/// RustLog Read Path Benchmarks
///
/// Validates storage layer read performance:
/// - Sequential read throughput
/// - mmap (sealed) vs active segment performance
/// - max_bytes behavior under varying limits
///
/// These benchmarks measure ONLY the storage read path.
/// No networking, no async, no offset management.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

use kafka_lite::storage::index::Index;
use kafka_lite::storage::segment::Segment;

/// Test fixture: a prepared segment with known data.
struct SegmentFixture {
    _temp_dir: TempDir,
    segment_path: PathBuf,
    index_path: PathBuf,
    base_offset: u64,
    record_count: usize,
    record_size: usize,
}

impl SegmentFixture {
    /// Create a new segment fixture with specified parameters.
    ///
    /// Creates a real segment on disk with deterministic payloads.
    /// Used to benchmark read performance under realistic conditions.
    fn new(base_offset: u64, record_count: usize, record_size: usize) -> Self {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let segment_path = temp_dir.path().join(format!("{:020}.log", base_offset));
        let index_path = temp_dir.path().join(format!("{:020}.index", base_offset));

        // Create segment
        let mut segment = Segment::open(&segment_path, base_offset)
            .expect("failed to create segment");

        // Create index
        let _index = Index::open(&index_path, base_offset)
            .expect("failed to create index");

        // Write deterministic records
        // Pattern: each record is filled with (offset % 256) as byte value
        for i in 0..record_count {
            let byte_value = ((base_offset + i as u64) % 256) as u8;
            let payload = vec![byte_value; record_size];
            segment.append(&payload).expect("failed to append record");
        }

        Self {
            _temp_dir: temp_dir,
            segment_path,
            index_path,
            base_offset,
            record_count,
            record_size,
        }
    }

    /// Seal the segment to enable mmap reads.
    ///
    /// After sealing, the segment can be read via read_from_offset (mmap-based).
    fn seal(&self) -> (Arc<Segment>, Arc<Mutex<Index>>) {
        let mut segment = Segment::open(&self.segment_path, self.base_offset)
            .expect("failed to open segment for sealing");
        
        segment.seal().expect("failed to seal segment");

        let index = Index::open(&self.index_path, self.base_offset)
            .expect("failed to open index");

        (Arc::new(segment), Arc::new(Mutex::new(index)))
    }

    /// Open as active segment for File-based reads.
    fn open_active(&self) -> Arc<Segment> {
        let segment = Segment::open(&self.segment_path, self.base_offset)
            .expect("failed to open active segment");
        Arc::new(segment)
    }

    /// Total bytes written (excluding metadata).
    fn total_payload_bytes(&self) -> usize {
        self.record_count * self.record_size
    }
}

/// Benchmark 1: Sequential Read Throughput
///
/// Measures raw read performance for sealed (mmap) and active (File) segments.
/// Tests different record sizes to understand performance characteristics.
///
/// Why this matters:
/// - Validates that mmap delivers expected zero-copy performance
/// - Identifies overhead of record framing
/// - Establishes baseline throughput numbers
fn benchmark_sequential_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_reads");

    // Test three record sizes: small (128B), medium (512B), large (1KB)
    let record_sizes = vec![
        (128, "128B"),
        (512, "512B"),
        (1024, "1KB"),
    ];

    for (record_size, size_label) in record_sizes {
        let record_count = 10_000;

        // Benchmark sealed segment (mmap-based)
        {
            let fixture = SegmentFixture::new(0, record_count, record_size);
            let (segment, index) = fixture.seal();
            let total_bytes = fixture.total_payload_bytes();

            group.throughput(Throughput::Bytes(total_bytes as u64));

            group.bench_function(
                BenchmarkId::new("sealed_mmap", size_label),
                |b| {
                    b.iter(|| {
                        let mut index_guard = index.lock().unwrap();
                        let result = segment
                            .read_from_offset(&mut *index_guard, black_box(0), black_box(usize::MAX))
                            .expect("read failed");
                        black_box(result);
                    });
                },
            );
        }

        // Benchmark active segment (File-based)
        {
            let fixture = SegmentFixture::new(10_000, record_count, record_size);
            let segment = fixture.open_active();
            let total_bytes = fixture.total_payload_bytes();

            group.throughput(Throughput::Bytes(total_bytes as u64));

            group.bench_function(
                BenchmarkId::new("active_file", size_label),
                |b| {
                    b.iter(|| {
                        let result = segment
                            .read_active_from_offset(black_box(10_000), black_box(usize::MAX))
                            .expect("read failed");
                        black_box(result);
                    });
                },
            );
        }
    }

    group.finish();
}

/// Benchmark 2: mmap vs Active Direct Comparison
///
/// Side-by-side comparison of sealed vs active segment reads.
/// Same dataset, same offsets, same max_bytes.
///
/// Why this matters:
/// - Quantifies performance difference between mmap and File I/O
/// - Validates that active segment reads are "good enough" for write path
/// - Identifies when to seal segments for optimal read performance
fn benchmark_mmap_vs_active(c: &mut Criterion) {
    let mut group = c.benchmark_group("mmap_vs_active");

    let record_count = 10_000;
    let record_size = 512; // Medium-sized records
    let max_bytes = 256 * 1024; // 256KB fetch limit

    // Create dataset that both sealed and active can read
    let fixture = SegmentFixture::new(0, record_count, record_size);

    // Sealed segment (mmap)
    let (sealed_segment, sealed_index) = fixture.seal();

    // Active segment (File) - reuse same data
    let active_segment = fixture.open_active();

    let total_bytes = (record_count * record_size).min(max_bytes);
    group.throughput(Throughput::Bytes(total_bytes as u64));

    // Benchmark sealed reads
    group.bench_function("sealed_segment", |b| {
        b.iter(|| {
            let mut index_guard = sealed_index.lock().unwrap();
            let result = sealed_segment
                .read_from_offset(&mut *index_guard, black_box(0), black_box(max_bytes))
                .expect("sealed read failed");
            black_box(result);
        });
    });

    // Benchmark active reads
    group.bench_function("active_segment", |b| {
        b.iter(|| {
            let result = active_segment
                .read_active_from_offset(black_box(0), black_box(max_bytes))
                .expect("active read failed");
            black_box(result);
        });
    });

    group.finish();
}

/// Benchmark 3: max_bytes Stress Test
///
/// Tests fetch behavior under different max_bytes limits.
/// Validates that the system correctly stops reading and maintains performance.
///
/// Why this matters:
/// - Consumer may request small fetches (low latency) or large fetches (throughput)
/// - System must respect limits precisely (no over-read)
/// - Performance should scale predictably with fetch size
fn benchmark_max_bytes_behavior(c: &mut Criterion) {
    let mut group = c.benchmark_group("max_bytes_stress");

    let record_count = 10_000;
    let record_size = 512;

    // Create a sealed segment for consistent performance
    let fixture = SegmentFixture::new(0, record_count, record_size);
    let (segment, index) = fixture.seal();

    // Test three fetch sizes:
    // 1. Single record (minimum fetch)
    // 2. Partial batch (~10 records)
    // 3. Full segment (maximum throughput)
    let fetch_sizes = vec![
        (1024, "single_record"),       // ~1KB (single record + overhead)
        (10 * 1024, "partial_batch"),  // ~10KB (multiple records)
        (1024 * 1024, "full_segment"), // 1MB (full segment read)
    ];

    for (max_bytes, label) in fetch_sizes {
        group.throughput(Throughput::Bytes(max_bytes as u64));

        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            b.iter(|| {
                let mut index_guard = index.lock().unwrap();
                let result = segment
                    .read_from_offset(&mut *index_guard, black_box(0), black_box(max_bytes))
                    .expect("read failed");

                // Verify correct behavior: result should not exceed max_bytes
                let total_size: usize = result
                    .records
                    .iter()
                    .map(|r| r.payload.len() + 12) // 12 bytes overhead per record (offset + length)
                    .sum();

                assert!(
                    total_size <= max_bytes || result.records.len() == 1,
                    "exceeded max_bytes: {} > {}",
                    total_size,
                    max_bytes
                );

                black_box(result);
            });
        });
    }

    group.finish();
}

/// Benchmark 4: Partition-Level Sequential Reads
///
/// Tests reading across multiple sealed segments via Partition::fetch().
/// Simulates real consumer behavior reading from a multi-segment partition.
///
/// Why this matters:
/// - Validates partition routing logic doesn't add overhead
/// - Tests segment boundary crossing performance
/// - Measures real-world consumer read path
fn benchmark_partition_reads(c: &mut Criterion) {
    use kafka_lite::topics::partition::Partition;

    let mut group = c.benchmark_group("partition_reads");

    // Create partition with 3 sealed segments
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let base_path = temp_dir.path();

    let record_size = 512;
    let records_per_segment = 1000;

    // Create 3 segments
    let mut sealed_segments = Vec::new();

    for seg_idx in 0..3 {
        let base_offset = (seg_idx * records_per_segment) as u64;
        let segment_path = base_path.join(format!("{:020}.log", base_offset));
        let index_path = base_path.join(format!("{:020}.index", base_offset));

        // Create and populate
        let mut segment = Segment::open(&segment_path, base_offset)
            .expect("failed to create segment");

        for i in 0..records_per_segment {
            let payload = vec![((base_offset + i as u64) % 256) as u8; record_size];
            segment.append(&payload).expect("failed to append");
        }

        // Seal segment in place
        segment.seal().expect("failed to seal segment");

        let index = Index::open(&index_path, base_offset).expect("failed to open index");

        sealed_segments.push((Arc::new(segment), Arc::new(Mutex::new(index))));
    }

    // Create active segment
    let active_base = (3 * records_per_segment) as u64;
    let active_segment_path = base_path.join(format!("{:020}.log", active_base));
    let active_index_path = base_path.join(format!("{:020}.index", active_base));

    let mut active_segment = Segment::open(&active_segment_path, active_base)
        .expect("failed to create active segment");

    for i in 0..records_per_segment {
        let payload = vec![((active_base + i as u64) % 256) as u8; record_size];
        active_segment.append(&payload).expect("failed to append");
    }

    let active_segment = Arc::new(active_segment);
    let active_index = Arc::new(Mutex::new(
        Index::open(&active_index_path, active_base).expect("failed to open active index"),
    ));

    // Create partition
    let partition = Partition {
        id: 0,
        sealed_segments,
        active_segment: (active_segment, active_index),
    };

    let max_bytes = 128 * 1024; // 128KB
    let total_records = 4 * records_per_segment;
    let total_bytes = total_records * record_size;

    group.throughput(Throughput::Bytes(total_bytes as u64));

    // Benchmark reading from start of partition (crosses all segments)
    group.bench_function("cross_segment_read", |b| {
        b.iter(|| {
            let result = partition
                .fetch(black_box(0), black_box(max_bytes))
                .expect("partition fetch failed");
            black_box(result);
        });
    });

    // Benchmark reading from middle segment
    group.bench_function("mid_segment_read", |b| {
        b.iter(|| {
            let start_offset = records_per_segment as u64;
            let result = partition
                .fetch(black_box(start_offset), black_box(max_bytes))
                .expect("partition fetch failed");
            black_box(result);
        });
    });

    // Benchmark reading from active segment only
    group.bench_function("active_only_read", |b| {
        b.iter(|| {
            let start_offset = (3 * records_per_segment) as u64;
            let result = partition
                .fetch(black_box(start_offset), black_box(max_bytes))
                .expect("partition fetch failed");
            black_box(result);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_sequential_reads,
    benchmark_mmap_vs_active,
    benchmark_max_bytes_behavior,
    benchmark_partition_reads
);
criterion_main!(benches);
