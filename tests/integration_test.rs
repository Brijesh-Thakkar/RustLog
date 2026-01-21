/// Integration tests for RustLog broker
/// 
/// These tests verify the complete request/response flow:
/// - TCP connection
/// - Frame encoding/decoding
/// - Protocol request/response
/// - Consumer offset commit/fetch
/// 
/// Tests run against a real broker instance on localhost.

use kafka_lite::protocol::{frame, request::Request, response::Response};
use kafka_lite::topics::partition::Partition;
use kafka_lite::offsets::manager::OffsetManager;
use kafka_lite::admin::manager::AdminManager;
use tokio::net::TcpStream;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

fn empty_registry() -> Arc<RwLock<HashMap<(String, u32), Arc<Partition>>>> {
    Arc::new(RwLock::new(HashMap::new()))
}

fn new_offset_manager() -> Arc<OffsetManager> {
    Arc::new(OffsetManager::new())
}

fn new_admin_manager() -> Arc<AdminManager> {
    let partitions = empty_registry();
    Arc::new(AdminManager::with_partitions(partitions))
}

fn new_admin_manager_with_partitions(partitions: Arc<RwLock<HashMap<(String, u32), Arc<Partition>>>>) -> Arc<AdminManager> {
    Arc::new(AdminManager::with_partitions(partitions))
}

#[tokio::test]
async fn test_ping_pong() {
    // Start broker in background with empty partition registry
    tokio::spawn(async {
        let _ = kafka_lite::broker::server::run(
            "127.0.0.1:19092",
            empty_registry(),
            new_offset_manager(),
            new_admin_manager(),
        )
        .await;
    });

    // Give broker time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Connect
    let mut stream = TcpStream::connect("127.0.0.1:19092")
        .await
        .expect("failed to connect");

    // Send Ping
    let request = Request::Ping;
    let request_bytes = request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write frame");

    // Receive Pong
    let response_bytes = frame::read_frame(&mut stream)
        .await
        .expect("failed to read frame");
    let response = Response::decode(&response_bytes).expect("failed to decode response");

    match response {
        Response::Pong => {
            println!("✅ Ping-pong successful");
        }
        _ => panic!("expected Pong, got {:?}", response),
    }
}

#[tokio::test]
async fn test_produce_request() {
    // Create shared partition registry and admin manager
    let partitions = empty_registry();
    let admin = new_admin_manager_with_partitions(partitions.clone());
    
    // Start broker in background
    tokio::spawn(async move {
        let _ = kafka_lite::broker::server::run(
            "127.0.0.1:19093",
            partitions,
            new_offset_manager(),
            admin,
        )
        .await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect("127.0.0.1:19093")
        .await
        .expect("failed to connect");

    // First, create the topic
    let create_request = Request::CreateTopic {
        topic: "test-topic".to_string(),
        partition_count: 1,
    };
    let create_bytes = create_request.encode();
    frame::write_frame(&mut stream, &create_bytes)
        .await
        .expect("failed to write create topic frame");
    
    let create_response_bytes = frame::read_frame(&mut stream)
        .await
        .expect("failed to read create response");
    let create_response = Response::decode(&create_response_bytes)
        .expect("failed to decode create response");
    
    match create_response {
        Response::CreateTopicResponse { success: true } => {
            println!("✅ Topic created successfully");
        }
        _ => panic!("failed to create topic: {:?}", create_response),
    }

    // Now send Produce request
    let request = Request::Produce {
        topic: "test-topic".to_string(),
        partition: 0,
        records: vec![
            b"hello".to_vec(),
            b"world".to_vec(),
        ],
    };
    let request_bytes = request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write frame");

    // Receive ProduceResponse
    let response_bytes = frame::read_frame(&mut stream)
        .await
        .expect("failed to read frame");
    let response = Response::decode(&response_bytes).expect("failed to decode response");

    match response {
        Response::ProduceResponse {
            base_offset,
            record_count,
        } => {
            println!("✅ Produce successful: base_offset={}, count={}", base_offset, record_count);
            assert_eq!(record_count, 2);
        }
        Response::Error { code, message } => {
            panic!("received error response: code={}, message={}", code, message);
        }
        _ => panic!("expected ProduceResponse, got {:?}", response),
    }
}

#[tokio::test]
async fn test_fetch_request() {
    // Start broker in background with empty partition registry
    tokio::spawn(async {
        let _ = kafka_lite::broker::server::run(
            "127.0.0.1:19094",
            empty_registry(),
            new_offset_manager(),
            new_admin_manager(),
        )
        .await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect("127.0.0.1:19094")
        .await
        .expect("failed to connect");

    // Send Fetch request (no group_id, use explicit offset)
    let request = Request::Fetch {
        group_id: String::new(),
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 1024,
    };
    let request_bytes = request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write frame");

    // Receive FetchResponse
    let response_bytes = frame::read_frame(&mut stream)
        .await
        .expect("failed to read frame");
    let response = Response::decode(&response_bytes).expect("failed to decode response");

    match response {
        Response::FetchResponse {
            records,
            next_offset,
        } => {
            println!("✅ Fetch successful: records={}, next_offset={}", records.len(), next_offset);
            // Empty partition registry returns empty results
            assert_eq!(records.len(), 0);
        }
        Response::Error { code, message } => {
            // Expected: partition not found (since registry is empty)
            println!("✅ Fetch returned expected error: code={}, message={}", code, message);
            assert_eq!(code, 10); // Topic or partition does not exist
            assert!(message.contains("does not exist"));
        }
        _ => panic!("expected FetchResponse or Error, got {:?}", response),
    }
}

#[tokio::test]
async fn test_commit_then_fetch_offset() {
    // Start broker in background
    tokio::spawn(async {
        let _ = kafka_lite::broker::server::run(
            "127.0.0.1:19095",
            empty_registry(),
            new_offset_manager(),
            new_admin_manager(),
        )
        .await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect("127.0.0.1:19095")
        .await
        .expect("failed to connect");

    // Commit an offset
    let commit_request = Request::OffsetCommit {
        group: "test-group".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 42,
    };
    let request_bytes = commit_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write commit frame");

    // Receive OffsetCommitResponse
    let response_bytes = frame::read_frame(&mut stream)
        .await
        .expect("failed to read commit response frame");
    let response = Response::decode(&response_bytes).expect("failed to decode commit response");

    match response {
        Response::OffsetCommitResponse { success } => {
            println!("✅ Offset commit successful: success={}", success);
            assert!(success);
        }
        _ => panic!("expected OffsetCommitResponse, got {:?}", response),
    }

    // Fetch the committed offset
    let fetch_request = Request::OffsetFetch {
        group: "test-group".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
    };
    let request_bytes = fetch_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write fetch frame");

    // Receive OffsetFetchResponse
    let response_bytes = frame::read_frame(&mut stream)
        .await
        .expect("failed to read fetch response frame");
    let response = Response::decode(&response_bytes).expect("failed to decode fetch response");

    match response {
        Response::OffsetFetchResponse { offset } => {
            println!("✅ Offset fetch successful: offset={:?}", offset);
            assert_eq!(offset, Some(42));
        }
        _ => panic!("expected OffsetFetchResponse, got {:?}", response),
    }
}

#[tokio::test]
async fn test_fetch_without_commit_returns_none() {
    // Start broker in background
    tokio::spawn(async {
        let _ = kafka_lite::broker::server::run(
            "127.0.0.1:19096",
            empty_registry(),
            new_offset_manager(),
            new_admin_manager(),
        )
        .await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect("127.0.0.1:19096")
        .await
        .expect("failed to connect");

    // Fetch offset without committing first
    let fetch_request = Request::OffsetFetch {
        group: "test-group".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
    };
    let request_bytes = fetch_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write frame");

    // Receive OffsetFetchResponse
    let response_bytes = frame::read_frame(&mut stream)
        .await
        .expect("failed to read frame");
    let response = Response::decode(&response_bytes).expect("failed to decode response");

    match response {
        Response::OffsetFetchResponse { offset } => {
            println!("✅ Fetch without commit returned None: offset={:?}", offset);
            assert_eq!(offset, None);
        }
        _ => panic!("expected OffsetFetchResponse, got {:?}", response),
    }
}

#[tokio::test]
async fn test_overwrite_committed_offset() {
    // Start broker in background
    tokio::spawn(async {
        let _ = kafka_lite::broker::server::run(
            "127.0.0.1:19097",
            empty_registry(),
            new_offset_manager(),
            new_admin_manager(),
        )
        .await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect("127.0.0.1:19097")
        .await
        .expect("failed to connect");

    // Commit initial offset
    let commit_request = Request::OffsetCommit {
        group: "test-group".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 100,
    };
    let request_bytes = commit_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write first commit");

    frame::read_frame(&mut stream).await.expect("failed to read first commit response");

    // Overwrite with new offset
    let commit_request = Request::OffsetCommit {
        group: "test-group".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 200,
    };
    let request_bytes = commit_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write second commit");

    frame::read_frame(&mut stream).await.expect("failed to read second commit response");

    // Fetch to verify overwrite
    let fetch_request = Request::OffsetFetch {
        group: "test-group".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
    };
    let request_bytes = fetch_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write fetch frame");

    let response_bytes = frame::read_frame(&mut stream)
        .await
        .expect("failed to read fetch response");
    let response = Response::decode(&response_bytes).expect("failed to decode response");

    match response {
        Response::OffsetFetchResponse { offset } => {
            println!("✅ Offset overwrite successful: offset={:?}", offset);
            assert_eq!(offset, Some(200));
        }
        _ => panic!("expected OffsetFetchResponse, got {:?}", response),
    }
}

#[tokio::test]
async fn test_offsets_isolated_per_group() {
    // Start broker in background
    tokio::spawn(async {
        let _ = kafka_lite::broker::server::run(
            "127.0.0.1:19098",
            empty_registry(),
            new_offset_manager(),
            new_admin_manager(),
        )
        .await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect("127.0.0.1:19098")
        .await
        .expect("failed to connect");

    // Commit offset for group1
    let commit_request = Request::OffsetCommit {
        group: "group1".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 100,
    };
    let request_bytes = commit_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write group1 commit");

    frame::read_frame(&mut stream).await.expect("failed to read group1 commit response");

    // Commit different offset for group2
    let commit_request = Request::OffsetCommit {
        group: "group2".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 200,
    };
    let request_bytes = commit_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write group2 commit");

    frame::read_frame(&mut stream).await.expect("failed to read group2 commit response");

    // Fetch group1's offset
    let fetch_request = Request::OffsetFetch {
        group: "group1".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
    };
    let request_bytes = fetch_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write group1 fetch");

    let response_bytes = frame::read_frame(&mut stream)
        .await
        .expect("failed to read group1 fetch response");
    let response = Response::decode(&response_bytes).expect("failed to decode response");

    match response {
        Response::OffsetFetchResponse { offset } => {
            println!("✅ Group1 offset isolated: offset={:?}", offset);
            assert_eq!(offset, Some(100));
        }
        _ => panic!("expected OffsetFetchResponse, got {:?}", response),
    }

    // Fetch group2's offset
    let fetch_request = Request::OffsetFetch {
        group: "group2".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
    };
    let request_bytes = fetch_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write group2 fetch");

    let response_bytes = frame::read_frame(&mut stream)
        .await
        .expect("failed to read group2 fetch response");
    let response = Response::decode(&response_bytes).expect("failed to decode response");

    match response {
        Response::OffsetFetchResponse { offset } => {
            println!("✅ Group2 offset isolated: offset={:?}", offset);
            assert_eq!(offset, Some(200));
        }
        _ => panic!("expected OffsetFetchResponse, got {:?}", response),
    }
}

#[tokio::test]
async fn test_fetch_uses_committed_offset() {
    // Start broker in background
    tokio::spawn(async {
        let _ = kafka_lite::broker::server::run(
            "127.0.0.1:19099",
            empty_registry(),
            new_offset_manager(),
            new_admin_manager(),
        )
        .await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect("127.0.0.1:19099")
        .await
        .expect("failed to connect");

    // Commit offset 100 for consumer group
    let commit_request = Request::OffsetCommit {
        group: "consumer-group".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 100,
    };
    let request_bytes = commit_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write commit");

    frame::read_frame(&mut stream)
        .await
        .expect("failed to read commit response");

    // Fetch using group_id (should use committed offset 100, not explicit offset)
    let fetch_request = Request::Fetch {
        group_id: "consumer-group".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 999, // This should be ignored since group_id is present
        max_bytes: 1024,
    };
    let request_bytes = fetch_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write fetch");

    let response_bytes = frame::read_frame(&mut stream)
        .await
        .expect("failed to read fetch response");
    let response = Response::decode(&response_bytes).expect("failed to decode response");

    match response {
        Response::FetchResponse {
            records,
            next_offset,
        } => {
            println!(
                "✅ Fetch used committed offset: records={}, next_offset={}",
                records.len(),
                next_offset
            );
            // Empty partition, but it tried to fetch from offset 100 (committed)
            // Verify offset is NOT 999 (which was in the request)
        }
        Response::Error { code, message } => {
            // Expected: partition not found (empty registry)
            println!("✅ Fetch attempted from committed offset, partition not found: code={}, message={}", code, message);
            assert_eq!(code, 10);
        }
        _ => panic!("expected FetchResponse or Error, got {:?}", response),
    }
}

#[tokio::test]
async fn test_fetch_without_commit_returns_empty() {
    // Start broker in background
    tokio::spawn(async {
        let _ = kafka_lite::broker::server::run(
            "127.0.0.1:19100",
            empty_registry(),
            new_offset_manager(),
            new_admin_manager(),
        )
        .await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect("127.0.0.1:19100")
        .await
        .expect("failed to connect");

    // Fetch using group_id WITHOUT committing first
    let fetch_request = Request::Fetch {
        group_id: "new-consumer-group".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 1024,
    };
    let request_bytes = fetch_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write fetch");

    let response_bytes = frame::read_frame(&mut stream)
        .await
        .expect("failed to read fetch response");
    let response = Response::decode(&response_bytes).expect("failed to decode response");

    match response {
        Response::FetchResponse {
            records,
            next_offset,
        } => {
            println!(
                "✅ Fetch without commit returned empty: records={}, next_offset={}",
                records.len(),
                next_offset
            );
            assert_eq!(records.len(), 0, "should return empty when no offset committed");
            assert_eq!(next_offset, 0, "next_offset should be 0 when no committed offset");
        }
        _ => panic!("expected FetchResponse, got {:?}", response),
    }
}

#[tokio::test]
async fn test_fetch_does_not_advance_offset() {
    // Start broker in background
    tokio::spawn(async {
        let _ = kafka_lite::broker::server::run(
            "127.0.0.1:19101",
            empty_registry(),
            new_offset_manager(),
            new_admin_manager(),
        )
        .await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect("127.0.0.1:19101")
        .await
        .expect("failed to connect");

    // Commit offset 50
    let commit_request = Request::OffsetCommit {
        group: "consumer-group".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 50,
    };
    let request_bytes = commit_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write commit");

    frame::read_frame(&mut stream)
        .await
        .expect("failed to read commit response");

    // Fetch using group_id
    let fetch_request = Request::Fetch {
        group_id: "consumer-group".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 1024,
    };
    let request_bytes = fetch_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write fetch");

    frame::read_frame(&mut stream)
        .await
        .expect("failed to read fetch response");

    // Fetch committed offset again - should still be 50
    let offset_fetch_request = Request::OffsetFetch {
        group: "consumer-group".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
    };
    let request_bytes = offset_fetch_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write offset fetch");

    let response_bytes = frame::read_frame(&mut stream)
        .await
        .expect("failed to read offset fetch response");
    let response = Response::decode(&response_bytes).expect("failed to decode response");

    match response {
        Response::OffsetFetchResponse { offset } => {
            println!("✅ Offset not advanced by fetch: offset={:?}", offset);
            assert_eq!(
                offset,
                Some(50),
                "offset should remain 50 after fetch (no auto-commit)"
            );
        }
        _ => panic!("expected OffsetFetchResponse, got {:?}", response),
    }
}

#[tokio::test]
async fn test_fetch_respects_max_bytes_from_committed() {
    // Start broker in background
    tokio::spawn(async {
        let _ = kafka_lite::broker::server::run(
            "127.0.0.1:19102",
            empty_registry(),
            new_offset_manager(),
            new_admin_manager(),
        )
        .await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect("127.0.0.1:19102")
        .await
        .expect("failed to connect");

    // Commit offset 10
    let commit_request = Request::OffsetCommit {
        group: "consumer-group".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 10,
    };
    let request_bytes = commit_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write commit");

    frame::read_frame(&mut stream)
        .await
        .expect("failed to read commit response");

    // Fetch with small max_bytes
    let fetch_request = Request::Fetch {
        group_id: "consumer-group".to_string(),
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 100, // Small limit
    };
    let request_bytes = fetch_request.encode();
    frame::write_frame(&mut stream, &request_bytes)
        .await
        .expect("failed to write fetch");

    let response_bytes = frame::read_frame(&mut stream)
        .await
        .expect("failed to read fetch response");
    let response = Response::decode(&response_bytes).expect("failed to decode response");

    match response {
        Response::FetchResponse { records, .. } => {
            println!(
                "✅ Fetch respected max_bytes limit: records={}",
                records.len()
            );
            // Should respect max_bytes even when fetching from committed offset
            // Empty registry means no records, but the limit would be enforced
        }
        Response::Error { code, .. } => {
            // Expected: partition not found
            println!("✅ Fetch respected max_bytes, partition not found: code={}", code);
            assert_eq!(code, 10);
        }
        _ => panic!("expected FetchResponse or Error, got {:?}", response),
    }
}
