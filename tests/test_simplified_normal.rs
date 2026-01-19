//! Simplified normal consumer test to debug the data accumulation issue

use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::time::sleep;
use tokio::net::TcpStream;

use kafka_lite::protocol::{frame, request::Request, response::Response};

static TEST_PORT_COUNTER: AtomicU16 = AtomicU16::new(21000);

#[tokio::test]
async fn simplified_normal_consumer_test() {
    let port = TEST_PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
    let broker_addr = format!("127.0.0.1:{}", port);
    
    println!("=== Starting normal consumer broker on {} ===", broker_addr);
    
    // Clean up any existing data directory to ensure isolation
    if std::path::Path::new("data").exists() {
        let _ = std::fs::remove_dir_all("data");
    }
    
    // Start fresh broker instance
    let broker_addr_clone = broker_addr.clone();
    let broker_handle = tokio::spawn(async move {
        start_simple_broker(&broker_addr_clone).await;
    });
    
    // Give broker time to start
    sleep(Duration::from_millis(200)).await;
    
    // Create topic
    let mut stream = TcpStream::connect(&broker_addr).await.unwrap();
    let create_request = Request::CreateTopic {
        topic: "normal-consumer-test".to_string(),
        partition_count: 1,
    };
    frame::write_frame(&mut stream, &create_request.encode()).await.unwrap();
    let _response = frame::read_frame(&mut stream).await.unwrap();
    drop(stream);
    println!("✅ Created topic normal-consumer-test");
    
    // Produce exactly 3 records
    let mut stream = TcpStream::connect(&broker_addr).await.unwrap();
    let produce_request = Request::Produce {
        topic: "normal-consumer-test".to_string(),
        partition: 0,
        records: vec![b"msg1".to_vec(), b"msg2".to_vec(), b"msg3".to_vec()],
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
    drop(stream);
    
    // Get initial throttling metrics
    let mut stream = TcpStream::connect(&broker_addr).await.unwrap();
    let metrics_request = Request::MetricsFetch;
    frame::write_frame(&mut stream, &metrics_request.encode()).await.unwrap();
    let response_bytes = frame::read_frame(&mut stream).await.unwrap();
    let response = Response::decode(&response_bytes).unwrap();
    let initial_metrics = match response {
        Response::MetricsResponse { metrics } => metrics,
        _ => panic!("Expected MetricsResponse"),
    };
    let initial_throttled = parse_metric_value(&initial_metrics, "throttled_consumers_total").unwrap_or(0);
    drop(stream);
    println!("Initial throttled consumers: {}", initial_throttled);
    
    // Fetch records (normal consumer behavior)
    let mut stream = TcpStream::connect(&broker_addr).await.unwrap();
    let fetch_request = Request::Fetch {
        group_id: "".to_string(),
        topic: "normal-consumer-test".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 1024,  // Small fetch
    };
    frame::write_frame(&mut stream, &fetch_request.encode()).await.unwrap();
    let response_bytes = frame::read_frame(&mut stream).await.unwrap();
    let response = Response::decode(&response_bytes).unwrap();
    match response {
        Response::FetchResponse { records, .. } => {
            println!("Fetched {} records from broker", records.len());
            for (i, record) in records.iter().enumerate() {
                let record_str = String::from_utf8_lossy(record);
                println!("  Record {}: {:?}", i, record_str);
            }
            assert_eq!(records.len(), 3, "Normal fetch should return all 3 records");
            println!("✅ Normal consumer successfully fetched {} records", records.len());
        }
        _ => panic!("Expected FetchResponse"),
    }
    drop(stream);
    
    // Verify no throttling occurred
    let mut stream = TcpStream::connect(&broker_addr).await.unwrap();
    let metrics_request = Request::MetricsFetch;
    frame::write_frame(&mut stream, &metrics_request.encode()).await.unwrap();
    let response_bytes = frame::read_frame(&mut stream).await.unwrap();
    let response = Response::decode(&response_bytes).unwrap();
    let final_metrics = match response {
        Response::MetricsResponse { metrics } => metrics,
        _ => panic!("Expected MetricsResponse"),
    };
    let final_throttled = parse_metric_value(&final_metrics, "throttled_consumers_total").unwrap_or(0);
    drop(stream);
    println!("Final throttled consumers: {}", final_throttled);
    
    assert_eq!(final_throttled, initial_throttled, 
        "Normal consumers should not trigger throttling: {} -> {}", 
        initial_throttled, final_throttled);
    
    // Shut down broker
    broker_handle.abort();
    println!("=== Simplified normal consumer test completed successfully ===");
}

async fn start_simple_broker(addr: &str) {
    use kafka_lite::{
        admin::AdminManager, 
        offsets::manager::OffsetManager,
        broker::{server, connection::PartitionRegistry},
    };
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    
    println!("Starting simple broker on {}", addr);
    
    // Initialize completely fresh broker state
    let partitions: PartitionRegistry = Arc::new(RwLock::new(HashMap::new()));
    let offset_manager = Arc::new(OffsetManager::new());
    let admin_manager = Arc::new(AdminManager::new());
    
    // Start broker server
    if let Err(e) = server::run(addr, partitions, offset_manager, admin_manager).await {
        eprintln!("Simple broker error: {}", e);
    }
}

fn parse_metric_value(metrics_text: &str, metric_name: &str) -> Option<u64> {
    for line in metrics_text.lines() {
        if line.starts_with(metric_name) && !line.starts_with('#') {
            if let Some(value_str) = line.split_whitespace().nth(1) {
                return value_str.parse().ok();
            }
        }
    }
    None
}