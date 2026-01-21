use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::error::BrokerError;
use crate::topics::partition::Partition;

/// Metadata for a single partition.
/// 
/// Currently tracks only base offset (starting logical offset).
/// Future: add high water mark, leader epoch, etc.
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub partition_id: u32,
    pub base_offset: u64,
}

/// Metadata for a topic.
/// 
/// Topics are explicitly created with a fixed partition count.
/// Partition count is immutable after creation.
/// 
/// Why immutable partition count?
/// - Simplifies partition assignment
/// - No need for partition rebalancing
/// - Predictable routing (hash % partition_count)
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub name: String,
    pub partition_count: u32,
    pub partitions: Vec<PartitionMetadata>,
}

impl TopicMetadata {
    /// Create new topic metadata with specified partition count.
    /// 
    /// All partitions start at offset 0.
    pub fn new(name: String, partition_count: u32) -> Self {
        let partitions = (0..partition_count)
            .map(|id| PartitionMetadata {
                partition_id: id,
                base_offset: 0,
            })
            .collect();

        Self {
            name,
            partition_count,
            partitions,
        }
    }
}

/// Admin manager for topic lifecycle.
/// 
/// Responsibilities:
/// - Create topics with explicit partition count
/// - List all topics
/// - Describe topic metadata
/// - Validate topic/partition existence
/// 
/// Thread-safety:
/// - Uses Arc<RwLock<>> for concurrent access
/// - Reads do not block each other (RwLock allows multiple readers)
/// - Writes block both reads and other writes
/// - No locks held across .await points
/// 
/// Design decisions:
/// - In-memory only (persistence is future work)
/// - Topics are immutable after creation (no partition count changes)
/// - No auto-creation (explicit CreateTopic required)
/// - Idempotent create (creating same topic twice is an error)
pub struct AdminManager {
    topics: Arc<RwLock<HashMap<String, TopicMetadata>>>,
    partitions: Arc<RwLock<HashMap<(String, u32), Arc<Partition>>>>,
}

impl AdminManager {
    /// Create a new admin manager with no topics.
    pub fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
            partitions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new admin manager that shares a partition registry.
    pub fn with_partitions(partitions: Arc<RwLock<HashMap<(String, u32), Arc<Partition>>>>) -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
            partitions,
        }
    }

    /// Create a topic with specified partition count.
    /// 
    /// Returns error if:
    /// - Topic already exists (not idempotent)
    /// - Partition count is 0
    /// 
    /// Why not idempotent?
    /// - Prevents accidental overwrites
    /// - Client should check existence first (DescribeTopic)
    /// - Clear error message guides user
    pub fn create_topic(&self, name: String, partition_count: u32) -> Result<(), BrokerError> {
        if partition_count == 0 {
            return Err(BrokerError::InvalidPartitionCount(0));
        }

        let mut topics = self.topics.write()
            .map_err(|_| BrokerError::LockPoisoned)?;

        if topics.contains_key(&name) {
            return Err(BrokerError::TopicAlreadyExists(name));
        }

        let metadata = TopicMetadata::new(name.clone(), partition_count);
        
        // Create actual partitions in the partition registry
        {
            let mut partitions = self.partitions.write()
                .map_err(|_| BrokerError::LockPoisoned)?;
            
            for partition_id in 0..partition_count {
                let partition_key = (name.clone(), partition_id);
                
                // Create a minimal in-memory partition for testing
                // This is a placeholder until proper segment storage is implemented
                let minimal_partition = create_in_memory_partition(partition_id);
                partitions.insert(partition_key, Arc::new(minimal_partition));
            }
        }
        
        topics.insert(name, metadata);

        Ok(())
    }

    /// List all topic names.
    /// 
    /// Returns sorted list for deterministic output.
    pub fn list_topics(&self) -> Result<Vec<String>, BrokerError> {
        let topics = self.topics.read()
            .map_err(|_| BrokerError::LockPoisoned)?;

        let mut names: Vec<String> = topics.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    /// Get metadata for a specific topic.
    /// 
    /// Returns error if topic does not exist.
    pub fn describe_topic(&self, name: &str) -> Result<TopicMetadata, BrokerError> {
        let topics = self.topics.read()
            .map_err(|_| BrokerError::LockPoisoned)?;

        topics.get(name)
            .cloned()
            .ok_or_else(|| BrokerError::TopicNotFound(name.to_string()))
    }

    /// Get metadata for a specific partition.
    /// 
    /// Returns error if:
    /// - Topic does not exist
    /// - Partition ID is out of range
    pub fn describe_partition(&self, topic: &str, partition_id: u32) -> Result<PartitionMetadata, BrokerError> {
        let metadata = self.describe_topic(topic)?;

        metadata.partitions
            .iter()
            .find(|p| p.partition_id == partition_id)
            .cloned()
            .ok_or_else(|| BrokerError::PartitionNotFound {
                topic: topic.to_string(),
                partition: partition_id,
            })
    }

    /// Check if a topic exists.
    /// 
    /// Used for validation in produce/fetch paths.
    pub fn topic_exists(&self, name: &str) -> bool {
        self.topics.read()
            .map(|topics| topics.contains_key(name))
            .unwrap_or(false)
    }

    /// Check if a partition exists for a topic.
    /// 
    /// Used for validation in produce/fetch paths.
    pub fn partition_exists(&self, topic: &str, partition_id: u32) -> bool {
        self.describe_partition(topic, partition_id).is_ok()
    }
}

impl Default for AdminManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Creates a minimal in-memory partition for development/testing.
/// Uses temporary files for segments to avoid persistence issues.
fn create_in_memory_partition(_partition_id: u32) -> Partition {
    use tempfile::TempDir;
    use crate::storage::{segment::Segment, index::Index};
    use std::sync::Mutex;
    
    // Create empty segment and index using temporary files
    let temp_dir = TempDir::new().unwrap();
    let log_path = temp_dir.path().join("00000000000000000000.log");
    let index_path = temp_dir.path().join("00000000000000000000.index");

    let segment = Segment::open(&log_path, 0).unwrap();
    let index = Index::open(&index_path, 0).unwrap();
    
    // Note: temp_dir is dropped here, but the file handles remain valid
    // This works for testing but in production, proper directory management is needed
    std::mem::forget(temp_dir); // Leak the temp dir to keep files alive
    
    Partition {
        id: _partition_id,
        sealed_segments: vec![],
        active_segment: (Arc::new(Mutex::new(segment)), Arc::new(Mutex::new(index))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_topic() {
        let admin = AdminManager::new();
        
        // Create topic with 3 partitions
        let result = admin.create_topic("test-topic".to_string(), 3);
        assert!(result.is_ok());

        // Verify topic metadata
        let metadata = admin.describe_topic("test-topic").unwrap();
        assert_eq!(metadata.name, "test-topic");
        assert_eq!(metadata.partition_count, 3);
        assert_eq!(metadata.partitions.len(), 3);
    }

    #[test]
    fn test_create_topic_duplicate() {
        let admin = AdminManager::new();
        
        admin.create_topic("test-topic".to_string(), 3).unwrap();
        
        // Creating same topic again should fail
        let result = admin.create_topic("test-topic".to_string(), 3);
        assert!(result.is_err());
        assert!(matches!(result, Err(BrokerError::TopicAlreadyExists(_))));
    }

    #[test]
    fn test_create_topic_zero_partitions() {
        let admin = AdminManager::new();
        
        let result = admin.create_topic("test-topic".to_string(), 0);
        assert!(result.is_err());
        assert!(matches!(result, Err(BrokerError::InvalidPartitionCount(0))));
    }

    #[test]
    fn test_list_topics() {
        let admin = AdminManager::new();
        
        admin.create_topic("topic-a".to_string(), 1).unwrap();
        admin.create_topic("topic-c".to_string(), 1).unwrap();
        admin.create_topic("topic-b".to_string(), 1).unwrap();

        let topics = admin.list_topics().unwrap();
        assert_eq!(topics, vec!["topic-a", "topic-b", "topic-c"]); // sorted
    }

    #[test]
    fn test_describe_topic_not_found() {
        let admin = AdminManager::new();
        
        let result = admin.describe_topic("nonexistent");
        assert!(result.is_err());
        assert!(matches!(result, Err(BrokerError::TopicNotFound(_))));
    }

    #[test]
    fn test_describe_partition() {
        let admin = AdminManager::new();
        admin.create_topic("test-topic".to_string(), 3).unwrap();

        // Valid partition
        let result = admin.describe_partition("test-topic", 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().partition_id, 1);

        // Invalid partition (out of range)
        let result = admin.describe_partition("test-topic", 99);
        assert!(result.is_err());
        assert!(matches!(result, Err(BrokerError::PartitionNotFound { .. })));
    }

    #[test]
    fn test_topic_exists() {
        let admin = AdminManager::new();
        admin.create_topic("test-topic".to_string(), 1).unwrap();

        assert!(admin.topic_exists("test-topic"));
        assert!(!admin.topic_exists("nonexistent"));
    }

    #[test]
    fn test_partition_exists() {
        let admin = AdminManager::new();
        admin.create_topic("test-topic".to_string(), 3).unwrap();

        assert!(admin.partition_exists("test-topic", 0));
        assert!(admin.partition_exists("test-topic", 2));
        assert!(!admin.partition_exists("test-topic", 99));
        assert!(!admin.partition_exists("nonexistent", 0));
    }
}
