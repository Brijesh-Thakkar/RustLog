use anyhow::Result;
use std::collections::HashMap;
use std::sync::RwLock;

/// Type alias for the nested offset storage structure.
/// Maps: group_id -> topic -> partition_id -> committed_offset
type OffsetStorage = HashMap<String, HashMap<String, HashMap<u32, u64>>>;

/// In-memory consumer offset tracking for Kafka-style consumer groups.
///
/// Maintains the committed offsets per consumer group, per topic, per partition.
/// This is purely in-memory; no persistence to disk.
///
/// ## Data Model:
/// ```text
/// group_id
///  └── topic
///      └── partition_id → committed_offset
/// ```
///
/// ## Semantics:
/// - Offsets are consumer-owned and must be explicitly committed
/// - commit() overwrites the previous offset for that (group, topic, partition)
/// - fetch_committed() returns None if no offset has been committed yet
/// - At-least-once delivery: duplicates are allowed, no exactly-once guarantees
///
/// ## Thread Safety:
/// - Uses RwLock for concurrent access (read-heavy optimization)
/// - Multiple readers can fetch offsets concurrently
/// - Writers acquire exclusive lock
pub struct OffsetManager {
    /// Nested map: group_id -> topic -> partition_id -> committed_offset
    /// 
    /// Why nested HashMap instead of composite key:
    /// - Natural hierarchy matches Kafka's model
    /// - Efficient lookup per group or topic
    /// - Clear ownership boundaries
    offsets: RwLock<OffsetStorage>,
}

impl OffsetManager {
    /// Create a new empty OffsetManager.
    ///
    /// No groups, topics, or offsets exist until explicitly committed.
    pub fn new() -> Self {
        Self {
            offsets: RwLock::new(HashMap::new()),
        }
    }

    /// Commit an offset for a consumer group on a specific topic-partition.
    ///
    /// This overwrites any previously committed offset for the same
    /// (group, topic, partition) tuple.
    ///
    /// # Arguments
    /// - `group`: Consumer group ID
    /// - `topic`: Topic name
    /// - `partition`: Partition ID within the topic
    /// - `offset`: The offset to commit (typically last consumed offset + 1)
    ///
    /// # Returns
    /// - Ok(()) on success
    /// - Err if lock poisoned (which should never happen in normal operation)
    ///
    /// # Thread Safety
    /// Acquires write lock on the entire offset map.
    pub fn commit(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        // Acquire write lock (exclusive access)
        let mut offsets = self.offsets.write()
            .map_err(|e| anyhow::anyhow!("offset lock poisoned: {}", e))?;

        // Navigate or create the nested structure:
        // offsets[group][topic][partition] = offset
        offsets
            .entry(group.to_string())
            .or_insert_with(HashMap::new)
            .entry(topic.to_string())
            .or_insert_with(HashMap::new)
            .insert(partition, offset);

        Ok(())
    }

    /// Fetch the committed offset for a consumer group on a specific topic-partition.
    ///
    /// Returns None if:
    /// - The group has never committed any offset
    /// - The group has not committed for this topic
    /// - The group has not committed for this partition within the topic
    ///
    /// # Arguments
    /// - `group`: Consumer group ID
    /// - `topic`: Topic name
    /// - `partition`: Partition ID within the topic
    ///
    /// # Returns
    /// - Some(offset) if a committed offset exists
    /// - None otherwise
    ///
    /// # Thread Safety
    /// Acquires read lock (shared access with other readers).
    pub fn fetch_committed(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
    ) -> Option<u64> {
        // Acquire read lock (shared access)
        let offsets = self.offsets.read().ok()?;

        // Navigate the nested structure safely
        offsets
            .get(group)?
            .get(topic)?
            .get(&partition)
            .copied()
    }
}

impl Default for OffsetManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_commit_and_fetch_single_offset() {
        let manager = OffsetManager::new();

        // Commit an offset
        manager
            .commit("group1", "topic1", 0, 42)
            .expect("commit should succeed");

        // Fetch it back
        let offset = manager.fetch_committed("group1", "topic1", 0);
        assert_eq!(offset, Some(42));
    }

    #[test]
    fn test_commit_overwrites_previous_offset() {
        let manager = OffsetManager::new();

        // Commit initial offset
        manager
            .commit("group1", "topic1", 0, 100)
            .expect("first commit should succeed");

        // Verify it's stored
        assert_eq!(manager.fetch_committed("group1", "topic1", 0), Some(100));

        // Overwrite with new offset
        manager
            .commit("group1", "topic1", 0, 200)
            .expect("second commit should succeed");

        // Verify the new offset replaced the old one
        assert_eq!(manager.fetch_committed("group1", "topic1", 0), Some(200));
    }

    #[test]
    fn test_different_groups_are_isolated() {
        let manager = OffsetManager::new();

        // Commit offsets for two different groups on the same topic-partition
        manager
            .commit("group1", "topic1", 0, 100)
            .expect("group1 commit should succeed");

        manager
            .commit("group2", "topic1", 0, 200)
            .expect("group2 commit should succeed");

        // Each group should see only its own offset
        assert_eq!(manager.fetch_committed("group1", "topic1", 0), Some(100));
        assert_eq!(manager.fetch_committed("group2", "topic1", 0), Some(200));

        // Verify they don't interfere with each other
        manager
            .commit("group1", "topic1", 0, 150)
            .expect("group1 update should succeed");

        assert_eq!(manager.fetch_committed("group1", "topic1", 0), Some(150));
        assert_eq!(
            manager.fetch_committed("group2", "topic1", 0),
            Some(200),
            "group2 should be unaffected"
        );
    }

    #[test]
    fn test_different_partitions_are_isolated() {
        let manager = OffsetManager::new();

        // Commit offsets for the same group-topic but different partitions
        manager
            .commit("group1", "topic1", 0, 100)
            .expect("partition 0 commit should succeed");

        manager
            .commit("group1", "topic1", 1, 200)
            .expect("partition 1 commit should succeed");

        manager
            .commit("group1", "topic1", 2, 300)
            .expect("partition 2 commit should succeed");

        // Each partition should have its own offset
        assert_eq!(manager.fetch_committed("group1", "topic1", 0), Some(100));
        assert_eq!(manager.fetch_committed("group1", "topic1", 1), Some(200));
        assert_eq!(manager.fetch_committed("group1", "topic1", 2), Some(300));

        // Update one partition shouldn't affect others
        manager
            .commit("group1", "topic1", 1, 250)
            .expect("partition 1 update should succeed");

        assert_eq!(
            manager.fetch_committed("group1", "topic1", 0),
            Some(100),
            "partition 0 should be unaffected"
        );
        assert_eq!(manager.fetch_committed("group1", "topic1", 1), Some(250));
        assert_eq!(
            manager.fetch_committed("group1", "topic1", 2),
            Some(300),
            "partition 2 should be unaffected"
        );
    }

    #[test]
    fn test_fetch_nonexistent_offset_returns_none() {
        let manager = OffsetManager::new();

        // Fetch from non-existent group
        assert_eq!(manager.fetch_committed("nonexistent", "topic1", 0), None);

        // Commit to one group, fetch from another
        manager
            .commit("group1", "topic1", 0, 100)
            .expect("commit should succeed");
        assert_eq!(manager.fetch_committed("group2", "topic1", 0), None);

        // Fetch non-existent topic
        assert_eq!(manager.fetch_committed("group1", "nonexistent", 0), None);

        // Fetch non-existent partition
        assert_eq!(manager.fetch_committed("group1", "topic1", 999), None);
    }

    #[test]
    fn test_concurrent_commits_do_not_corrupt_state() {
        let manager = Arc::new(OffsetManager::new());
        let num_threads = 10;
        let commits_per_thread = 100;

        // Spawn multiple threads that commit offsets concurrently
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let manager = Arc::clone(&manager);
                thread::spawn(move || {
                    for i in 0..commits_per_thread {
                        let offset = (thread_id * commits_per_thread + i) as u64;
                        manager
                            .commit("group1", "topic1", thread_id, offset)
                            .expect("concurrent commit should succeed");
                    }
                })
            })
            .collect();

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("thread should not panic");
        }

        // Verify that each partition has the last offset committed by its thread
        for thread_id in 0..num_threads {
            let expected_offset = (thread_id * commits_per_thread + commits_per_thread - 1) as u64;
            let actual_offset = manager
                .fetch_committed("group1", "topic1", thread_id)
                .expect("offset should exist after concurrent commits");

            assert_eq!(
                actual_offset, expected_offset,
                "partition {} should have the final offset from its thread",
                thread_id
            );
        }

        // Additional verification: different group should not be affected
        assert_eq!(
            manager.fetch_committed("group2", "topic1", 0),
            None,
            "other groups should not be affected by concurrent commits"
        );
    }

    #[test]
    fn test_concurrent_reads_and_writes() {
        let manager = Arc::new(OffsetManager::new());

        // Pre-populate with some offsets
        for partition in 0..5 {
            manager
                .commit("group1", "topic1", partition, 1000 + partition as u64)
                .expect("initial commit should succeed");
        }

        let num_readers = 20;
        let num_writers = 5;
        let operations_per_thread = 50;

        // Spawn reader threads
        let reader_handles: Vec<_> = (0..num_readers)
            .map(|_| {
                let manager = Arc::clone(&manager);
                thread::spawn(move || {
                    for _ in 0..operations_per_thread {
                        // Read from random partitions
                        for partition in 0..5 {
                            let offset = manager.fetch_committed("group1", "topic1", partition);
                            // Offset should exist and be reasonable
                            assert!(
                                offset.is_some(),
                                "readers should see committed offsets"
                            );
                        }
                    }
                })
            })
            .collect();

        // Spawn writer threads
        let writer_handles: Vec<_> = (0..num_writers)
            .map(|writer_id| {
                let manager = Arc::clone(&manager);
                thread::spawn(move || {
                    for i in 0..operations_per_thread {
                        let partition = (writer_id % 5) as u32;
                        let offset = 2000 + (writer_id * operations_per_thread + i) as u64;
                        manager
                            .commit("group1", "topic1", partition, offset)
                            .expect("concurrent write should succeed");
                    }
                })
            })
            .collect();

        // Wait for all threads
        for handle in reader_handles.into_iter().chain(writer_handles.into_iter()) {
            handle.join().expect("thread should not panic");
        }

        // Verify state is still consistent - all partitions should have offsets
        for partition in 0..5 {
            let offset = manager.fetch_committed("group1", "topic1", partition);
            assert!(
                offset.is_some(),
                "partition {} should still have an offset after concurrent operations",
                partition
            );
        }
    }
}
