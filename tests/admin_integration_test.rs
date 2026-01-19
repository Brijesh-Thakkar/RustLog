/// Integration tests for Admin Control Plane
/// 
/// These tests validate:
/// - CreateTopic, ListTopics, DescribeTopic, DescribePartition commands
/// - Explicit topic management (NO auto-creation)
/// - Proper error codes for non-existent topics/partitions
/// - Produce/Fetch rejection when topics don't exist

use kafka_lite::{
    broker,
    offsets::manager::OffsetManager,
    protocol::{frame, request::Request, response::Response},
    admin::manager::AdminManager,
};
use std::collections::HashMap;
use std::sync::{atomic::{AtomicU16, Ordering}, Arc, RwLock};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::sleep;

/// Global port allocator for test isolation.
static TEST_PORT_COUNTER: AtomicU16 = AtomicU16::new(19000);

/// Test context providing isolated broker instance per test.
struct TestContext {
    broker_addr: String,
    _temp_dir: tempfile::TempDir,
    shutdown_handle: Option<tokio::task::JoinHandle<()>>,
}

impl TestContext {
    async fn new(test_name: &str) -> Self {
        let port = TEST_PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let broker_addr = format!("127.0.0.1:{}", port);
        
        let temp_dir = tempfile::TempDir::new()
            .unwrap_or_else(|_| panic!("Failed to create temp directory for test {}", test_name));
        
        // Clean up any existing data directory to ensure isolation
        if std::path::Path::new("data").exists() {
            let _ = std::fs::remove_dir_all("data");
        }
        
        let broker_addr_clone = broker_addr.clone();
        let data_dir = temp_dir.path().to_path_buf();
        
        let shutdown_handle = tokio::spawn(async move {
            start_test_broker(&broker_addr_clone, data_dir).await.ok();
        });
        
        sleep(Duration::from_millis(100)).await;
        
        Self {
            broker_addr,
            _temp_dir: temp_dir,
            shutdown_handle: Some(shutdown_handle),
        }
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        if let Some(handle) = self.shutdown_handle.take() {
            handle.abort();
        }
    }
}

async fn start_test_broker(addr: &str, _data_dir: std::path::PathBuf) -> anyhow::Result<()> {
    let partitions = Arc::new(RwLock::new(HashMap::new()));
    let offset_manager = Arc::new(OffsetManager::new());
    let admin_manager = Arc::new(AdminManager::new());

    broker::server::run(addr, partitions, offset_manager, admin_manager).await
}

async fn start_test_broker_on_port(port: u16) -> anyhow::Result<()> {
    let addr = format!("127.0.0.1:{}", port);
    let partitions = Arc::new(RwLock::new(HashMap::new()));
    let offset_manager = Arc::new(OffsetManager::new());
    let admin_manager = Arc::new(AdminManager::new());

    broker::server::run(&addr, partitions, offset_manager, admin_manager).await
}



async fn connect_to_broker(port: u16) -> anyhow::Result<TcpStream> {
    let addr = format!("127.0.0.1:{}", port);
    let stream = TcpStream::connect(&addr).await?;
    Ok(stream)
}

async fn send_request(stream: &mut TcpStream, request: Request) -> anyhow::Result<Response> {
    let request_bytes = request.encode();
    frame::write_frame(stream, &request_bytes).await?;
    let response_bytes = frame::read_frame(stream).await?;
    let response = Response::decode(&response_bytes)?;
    Ok(response)
}

#[tokio::test]
async fn test_create_topic_success() {
    let port = 19001;
    tokio::spawn(async move {
        start_test_broker_on_port(port).await.ok();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = connect_to_broker(port).await.unwrap();

    let request = Request::CreateTopic {
        topic: "test-topic".to_string(),
        partition_count: 3,
    };

    let response = send_request(&mut stream, request).await.unwrap();

    match response {
        Response::CreateTopicResponse { success } => {
            assert!(success, "Expected topic creation to succeed");
        }
        Response::Error { code, message } => {
            panic!("Expected success, got error: code={}, message={}", code, message);
        }
        _ => panic!("Unexpected response type"),
    }
}

#[tokio::test]
async fn test_create_topic_duplicate_error() {
    let port = 19002;
    tokio::spawn(async move {
        start_test_broker_on_port(port).await.ok();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = connect_to_broker(port).await.unwrap();

    // Create topic first time
    let request = Request::CreateTopic {
        topic: "duplicate-topic".to_string(),
        partition_count: 2,
    };
    let response = send_request(&mut stream, request).await.unwrap();
    assert!(matches!(response, Response::CreateTopicResponse { success: true }));

    // Try to create same topic again
    let request = Request::CreateTopic {
        topic: "duplicate-topic".to_string(),
        partition_count: 2,
    };
    let response = send_request(&mut stream, request).await.unwrap();

    match response {
        Response::Error { code, message } => {
            assert_eq!(code, 11, "Expected error code 11 (TopicAlreadyExists)");
            assert!(message.contains("already exists"), "Expected 'already exists' in message");
        }
        Response::CreateTopicResponse { success: true } => {
            panic!("Expected error for duplicate topic creation");
        }
        _ => panic!("Unexpected response type"),
    }
}

#[tokio::test]
async fn test_list_topics() {
    let port = 19003;
    tokio::spawn(async move {
        start_test_broker_on_port(port).await.ok();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = connect_to_broker(port).await.unwrap();

    // Create two topics
    let request = Request::CreateTopic {
        topic: "topic-a".to_string(),
        partition_count: 1,
    };
    send_request(&mut stream, request).await.unwrap();

    let request = Request::CreateTopic {
        topic: "topic-b".to_string(),
        partition_count: 2,
    };
    send_request(&mut stream, request).await.unwrap();

    // List topics
    let request = Request::ListTopics;
    let response = send_request(&mut stream, request).await.unwrap();

    match response {
        Response::ListTopicsResponse { topics } => {
            assert_eq!(topics.len(), 2, "Expected 2 topics");
            assert!(topics.contains(&"topic-a".to_string()), "Expected topic-a");
            assert!(topics.contains(&"topic-b".to_string()), "Expected topic-b");
            // Verify sorted order
            assert_eq!(topics[0], "topic-a");
            assert_eq!(topics[1], "topic-b");
        }
        _ => panic!("Expected ListTopicsResponse"),
    }
}

#[tokio::test]
async fn test_describe_topic() {
    let port = 19004;
    tokio::spawn(async move {
        start_test_broker_on_port(port).await.ok();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = connect_to_broker(port).await.unwrap();

    // Create topic
    let request = Request::CreateTopic {
        topic: "describe-me".to_string(),
        partition_count: 5,
    };
    send_request(&mut stream, request).await.unwrap();

    // Describe topic
    let request = Request::DescribeTopic {
        topic: "describe-me".to_string(),
    };
    let response = send_request(&mut stream, request).await.unwrap();

    match response {
        Response::DescribeTopicResponse {
            topic,
            partition_count,
        } => {
            assert_eq!(topic, "describe-me");
            assert_eq!(partition_count, 5);
        }
        _ => panic!("Expected DescribeTopicResponse"),
    }
}

#[tokio::test]
async fn test_describe_topic_not_found() {
    let port = 19005;
    tokio::spawn(async move {
        start_test_broker_on_port(port).await.ok();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = connect_to_broker(port).await.unwrap();

    // Describe non-existent topic
    let request = Request::DescribeTopic {
        topic: "nonexistent".to_string(),
    };
    let response = send_request(&mut stream, request).await.unwrap();

    match response {
        Response::Error { code, message } => {
            assert_eq!(code, 13, "Expected error code 13 (DescribeTopic error)");
            assert!(message.contains("failed"), "Expected 'failed' in message");
        }
        _ => panic!("Expected Error response"),
    }
}

#[tokio::test]
async fn test_describe_partition() {
    let port = 19006;
    tokio::spawn(async move {
        start_test_broker_on_port(port).await.ok();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = connect_to_broker(port).await.unwrap();

    // Create topic
    let request = Request::CreateTopic {
        topic: "part-test".to_string(),
        partition_count: 3,
    };
    send_request(&mut stream, request).await.unwrap();

    // Describe partition
    let request = Request::DescribePartition {
        topic: "part-test".to_string(),
        partition: 1,
    };
    let response = send_request(&mut stream, request).await.unwrap();

    match response {
        Response::DescribePartitionResponse {
            partition,
            base_offset,
        } => {
            assert_eq!(partition, 1);
            assert_eq!(base_offset, 0, "New partition should have base_offset=0");
        }
        _ => panic!("Expected DescribePartitionResponse"),
    }
}

#[tokio::test]
async fn test_describe_partition_not_found() {
    let port = 19007;
    tokio::spawn(async move {
        start_test_broker_on_port(port).await.ok();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = connect_to_broker(port).await.unwrap();

    // Create topic with 2 partitions
    let request = Request::CreateTopic {
        topic: "part-test2".to_string(),
        partition_count: 2,
    };
    send_request(&mut stream, request).await.unwrap();

    // Try to describe partition 5 (out of range)
    let request = Request::DescribePartition {
        topic: "part-test2".to_string(),
        partition: 5,
    };
    let response = send_request(&mut stream, request).await.unwrap();

    match response {
        Response::Error { code, message } => {
            assert_eq!(code, 14, "Expected error code 14 (DescribePartition error)");
            assert!(message.contains("failed"), "Expected 'failed' in message");
        }
        _ => panic!("Expected Error response"),
    }
}

#[tokio::test]
async fn test_produce_to_nonexistent_topic_fails() {
    let port = 19008;
    tokio::spawn(async move {
        start_test_broker_on_port(port).await.ok();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = connect_to_broker(port).await.unwrap();

    // Try to produce without creating topic first
    let request = Request::Produce {
        topic: "no-topic".to_string(),
        partition: 0,
        records: vec![b"test data".to_vec()],
    };
    let response = send_request(&mut stream, request).await.unwrap();

    match response {
        Response::Error { code, message } => {
            assert_eq!(code, 10, "Expected error code 10 (TopicOrPartitionNotFound)");
            assert!(message.contains("does not exist"), "Expected 'does not exist' in message");
        }
        Response::ProduceResponse { .. } => {
            panic!("Expected error, but produce succeeded (auto-creation not allowed)");
        }
        _ => panic!("Unexpected response type"),
    }
}

#[tokio::test]
async fn test_fetch_from_nonexistent_topic_fails() {
    let port = 19009;
    tokio::spawn(async move {
        start_test_broker_on_port(port).await.ok();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = connect_to_broker(port).await.unwrap();

    // Try to fetch without creating topic first
    let request = Request::Fetch {
        group_id: String::new(),
        topic: "no-topic".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 1024,
    };
    let response = send_request(&mut stream, request).await.unwrap();

    match response {
        Response::Error { code, message } => {
            assert_eq!(code, 10, "Expected error code 10 (TopicOrPartitionNotFound)");
            assert!(message.contains("does not exist"), "Expected 'does not exist' in message");
        }
        Response::FetchResponse { .. } => {
            panic!("Expected error, but fetch succeeded (auto-creation not allowed)");
        }
        _ => panic!("Unexpected response type"),
    }
}

#[tokio::test]
async fn test_produce_after_create_topic_succeeds() {
    let port = 19010;
    tokio::spawn(async move {
        start_test_broker_on_port(port).await.ok();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = connect_to_broker(port).await.unwrap();

    // Create topic first
    let request = Request::CreateTopic {
        topic: "prod-topic".to_string(),
        partition_count: 1,
    };
    send_request(&mut stream, request).await.unwrap();

    // Now produce should work
    let request = Request::Produce {
        topic: "prod-topic".to_string(),
        partition: 0,
        records: vec![b"test data".to_vec()],
    };
    let response = send_request(&mut stream, request).await.unwrap();

    match response {
        Response::ProduceResponse { base_offset, record_count } => {
            assert_eq!(base_offset, 0, "First batch should have base_offset 0");
            assert_eq!(record_count, 1, "Should have 1 record");
        }
        Response::Error { code, message } => {
            panic!("Expected success, got error: code={}, message={}", code, message);
        }
        _ => panic!("Unexpected response type"),
    }
}

#[tokio::test]
async fn test_end_to_end_demo_flow() {
    // This test validates the complete demo flow:
    // 1. CreateTopic
    // 2. Produce records
    // 3. Fetch records successfully
    // 
    // This test must pass for the demo walkthrough to work.
    // If this test fails, it means CreateTopic is not properly initializing partition storage.
    
    let test_ctx = TestContext::new("demo_flow").await;
    let mut stream = TcpStream::connect(&test_ctx.broker_addr).await.unwrap();

    // Step 1: Create topic
    let request = Request::CreateTopic {
        topic: "demo".to_string(),
        partition_count: 1,
    };
    let response = send_request(&mut stream, request).await.unwrap();
    match response {
        Response::CreateTopicResponse { success: true } => {},
        _ => panic!("CreateTopic failed: {:?}", response),
    }

    // Step 2: Produce records
    let produce_request = Request::Produce {
        topic: "demo".to_string(),
        partition: 0,
        records: vec![
            b"message-1".to_vec(),
            b"message-2".to_vec(),
            b"message-3".to_vec(),
        ],
    };
    let response = send_request(&mut stream, produce_request).await.unwrap();
    match response {
        Response::ProduceResponse { base_offset, record_count } => {
            assert_eq!(base_offset, 0, "First batch should start at offset 0");
            assert_eq!(record_count, 3, "Should have produced 3 records");
        }
        Response::Error { code, message } => {
            panic!("Produce failed with error code {}: {}", code, message);
        }
        _ => panic!("Unexpected response: {:?}", response),
    }

    // Step 3: Fetch records
    let fetch_request = Request::Fetch {
        group_id: String::new(),  // No consumer group
        topic: "demo".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 1024,
    };
    let response = send_request(&mut stream, fetch_request).await.unwrap();
    match response {
        Response::FetchResponse { records, next_offset } => {
            // This is the critical assertion: if CreateTopic didn't initialize storage,
            // this would fail with "Partition storage not initialized"
            assert_eq!(records.len(), 3, "Should fetch 3 records");
            assert_eq!(records[0], b"message-1");
            assert_eq!(records[1], b"message-2");
            assert_eq!(records[2], b"message-3");
            assert_eq!(next_offset, 3, "Next offset should be 3");
        }
        Response::Error { code, message } => {
            panic!("Fetch failed with error code {}: {}. This means CreateTopic did not properly initialize partition storage.", code, message);
        }
        _ => panic!("Unexpected response: {:?}", response),
    }
}
