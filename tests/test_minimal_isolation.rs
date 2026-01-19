//! Minimal test to debug data accumulation issue
//!
//! This creates the simplest possible isolated test to see
//! where the data accumulation is coming from.

use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::time::sleep;
use tokio::net::TcpStream;

use kafka_lite::protocol::{frame, request::Request, response::Response};

static TEST_PORT_COUNTER: AtomicU16 = AtomicU16::new(20000);

#[tokio::test]
async fn minimal_produce_fetch_test() {
    let port = TEST_PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
    let broker_addr = format!("127.0.0.1:{}", port);
    
    println!("=== Starting minimal broker on {} ===", broker_addr);
    
    // Clean up any existing data directory to ensure isolation
    if std::path::Path::new("data").exists() {
        let _ = std::fs::remove_dir_all("data");
    }
    
    // Start fresh broker instance
    let broker_addr_clone = broker_addr.clone();
    let broker_handle = tokio::spawn(async move {
        start_minimal_broker(&broker_addr_clone).await;
    });
    
    // Give broker time to start
    sleep(Duration::from_millis(200)).await;
    
    // Create topic
    let mut stream = TcpStream::connect(&broker_addr).await.unwrap();
    let create_request = Request::CreateTopic {
        topic: "minimal-test".to_string(),
        partition_count: 1,
    };
    frame::write_frame(&mut stream, &create_request.encode()).await.unwrap();
    let _response = frame::read_frame(&mut stream).await.unwrap();
    drop(stream); // Close connection
    println!("✅ Created topic 'minimal-test'");
    
    // Produce exactly 2 records
    let mut stream = TcpStream::connect(&broker_addr).await.unwrap();
    let produce_request = Request::Produce {
        topic: "minimal-test".to_string(),
        partition: 0,
        records: vec![b"record1".to_vec(), b"record2".to_vec()],
    };
    frame::write_frame(&mut stream, &produce_request.encode()).await.unwrap();
    let response_bytes = frame::read_frame(&mut stream).await.unwrap();
    let response = Response::decode(&response_bytes).unwrap();
    match response {
        Response::ProduceResponse { base_offset, record_count } => {
            println!("✅ Produced {} records at base_offset {}", record_count, base_offset);
        }
        _ => panic!("Unexpected produce response: {:?}", response),
    }
    drop(stream); // Close connection
    
    // Fetch records  
    let mut stream = TcpStream::connect(&broker_addr).await.unwrap();
    let fetch_request = Request::Fetch {
        group_id: "".to_string(),
        topic: "minimal-test".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 1024,
    };
    frame::write_frame(&mut stream, &fetch_request.encode()).await.unwrap();
    let response_bytes = frame::read_frame(&mut stream).await.unwrap();
    let response = Response::decode(&response_bytes).unwrap();
    match response {
        Response::FetchResponse { records, .. } => {
            println!("✅ Fetched {} records:", records.len());
            for (i, record) in records.iter().enumerate() {
                println!("  Record {}: {:?}", i, String::from_utf8_lossy(record));
            }
            assert_eq!(records.len(), 2, "Should fetch exactly 2 records");
        }
        _ => panic!("Unexpected fetch response: {:?}", response),
    }
    drop(stream); // Close connection
    
    // Shut down broker
    broker_handle.abort();
    println!("=== Minimal test completed successfully ===");
}

async fn start_minimal_broker(addr: &str) {
    use kafka_lite::{
        admin::AdminManager, 
        offsets::manager::OffsetManager,
        broker::{server, connection::PartitionRegistry},
    };
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    
    println!("Starting minimal broker on {}", addr);
    
    // Initialize completely fresh broker state
    let partitions: PartitionRegistry = Arc::new(RwLock::new(HashMap::new()));
    let offset_manager = Arc::new(OffsetManager::new());
    let admin_manager = Arc::new(AdminManager::new());
    
    // Start broker server
    if let Err(e) = server::run(addr, partitions, offset_manager, admin_manager).await {
        eprintln!("Minimal broker error: {}", e);
    }
}