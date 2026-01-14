/// Example client demonstrating RustLog protocol usage
/// 
/// This shows how to:
/// - Connect to broker
/// - Send Produce requests
/// - Send Fetch requests
/// - Handle responses
/// 
/// Run the broker first:
///   cargo run
/// 
/// Then run this client:
///   cargo run --example simple_client

use kafka_lite::protocol::{frame, request::Request, response::Response};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("🔌 Connecting to broker at 127.0.0.1:9092...");
    let mut stream = TcpStream::connect("127.0.0.1:9092").await?;
    println!("✅ Connected!");
    println!();

    // Test 1: Ping
    println!("📤 Sending Ping...");
    let request = Request::Ping;
    let request_bytes = request.encode();
    frame::write_frame(&mut stream, &request_bytes).await?;

    let response_bytes = frame::read_frame(&mut stream).await?;
    let response = Response::decode(&response_bytes)?;
    match response {
        Response::Pong => println!("📥 Received Pong"),
        _ => println!("❌ Unexpected response: {:?}", response),
    }
    println!();

    // Test 2: Produce
    println!("📤 Producing 3 records to 'events' topic, partition 0...");
    let request = Request::Produce {
        topic: "events".to_string(),
        partition: 0,
        records: vec![
            b"event1: user_login".to_vec(),
            b"event2: page_view".to_vec(),
            b"event3: purchase".to_vec(),
        ],
    };
    let request_bytes = request.encode();
    frame::write_frame(&mut stream, &request_bytes).await?;

    let response_bytes = frame::read_frame(&mut stream).await?;
    let response = Response::decode(&response_bytes)?;
    match response {
        Response::ProduceResponse {
            base_offset,
            record_count,
        } => {
            println!("📥 ProduceResponse:");
            println!("   Base offset: {}", base_offset);
            println!("   Records written: {}", record_count);
            for i in 0..record_count {
                println!("      - Record {} has offset {}", i, base_offset + i as u64);
            }
        }
        Response::Error { code, message } => {
            println!("❌ Error: code={}, message={}", code, message);
        }
        _ => println!("❌ Unexpected response: {:?}", response),
    }
    println!();

    // Test 3: Fetch
    println!("📤 Fetching from 'events' topic, partition 0, offset 0...");
    let request = Request::Fetch {
        group_id: String::new(), // No consumer group, use explicit offset
        topic: "events".to_string(),
        partition: 0,
        offset: 0,
        max_bytes: 1024 * 1024, // 1MB
    };
    let request_bytes = request.encode();
    frame::write_frame(&mut stream, &request_bytes).await?;

    let response_bytes = frame::read_frame(&mut stream).await?;
    let response = Response::decode(&response_bytes)?;
    match response {
        Response::FetchResponse {
            records,
            next_offset,
        } => {
            println!("📥 FetchResponse:");
            println!("   Records returned: {}", records.len());
            println!("   Next offset: {}", next_offset);
            if records.is_empty() {
                println!("   (Storage not implemented yet - stub returns empty)");
            } else {
                for (i, record) in records.iter().enumerate() {
                    println!("      - Record {}: {:?}", i, String::from_utf8_lossy(record));
                }
            }
        }
        Response::Error { code, message } => {
            println!("❌ Error: code={}, message={}", code, message);
        }
        _ => println!("❌ Unexpected response: {:?}", response),
    }
    println!();

    println!("✅ Client demo complete!");
    Ok(())
}
