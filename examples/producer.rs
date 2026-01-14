//! RustLog Producer Demo
//!
//! This is a reference implementation showing how to produce messages to RustLog.
//!
//! ## Usage
//! ```bash
//! # Start broker first:
//! cargo run --release
//!
//! # In another terminal:
//! cargo run --example producer
//! ```
//!
//! ## What this does
//! - Connects to broker at 127.0.0.1:9092
//! - Produces 3 messages to topic "demo", partition 0
//! - Prints assigned offsets
//!
//! ## Protocol used
//! - Binary framing (4-byte length prefix)
//! - Produce request (0x01)
//! - ProduceResponse with base_offset

use kafka_lite::protocol::{frame, request::Request, response::Response};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("RustLog Producer Demo");
    println!("=====================\n");

    // Connect to broker
    println!("Connecting to broker at 127.0.0.1:9092...");
    let mut stream = TcpStream::connect("127.0.0.1:9092").await?;
    println!("Connected\n");

    // Prepare messages to produce
    let messages = vec![
        "hello".as_bytes().to_vec(),
        "from".as_bytes().to_vec(),
        "rustlog".as_bytes().to_vec(),
    ];

    println!("Producing {} messages to topic 'demo', partition 0...", messages.len());

    // Build Produce request
    let request = Request::Produce {
        topic: "demo".to_string(),
        partition: 0,
        records: messages.clone(),
    };

    // Encode and send request
    let request_bytes = request.encode();
    frame::write_frame(&mut stream, &request_bytes).await?;

    // Read and decode response
    let response_bytes = frame::read_frame(&mut stream).await?;
    let response = Response::decode(&response_bytes)?;

    // Handle response
    match response {
        Response::ProduceResponse {
            base_offset,
            record_count,
        } => {
            println!("\nProduction successful!");
            println!("Base offset: {}", base_offset);
            println!("Records written: {}\n", record_count);

            // Show offset for each message
            for (i, msg) in messages.iter().enumerate() {
                let offset = base_offset + i as u64;
                let msg_str = String::from_utf8_lossy(msg);
                println!("  Offset {}: {}", offset, msg_str);
            }
        }
        Response::Error { code, message } => {
            eprintln!("\nError producing messages:");
            eprintln!("  Code: {}", code);
            eprintln!("  Message: {}", message);
            std::process::exit(1);
        }
        _ => {
            eprintln!("\nUnexpected response type: {:?}", response);
            std::process::exit(1);
        }
    }

    println!("\nDone");
    Ok(())
}
