/// Example: Simple ping test for broker connectivity
/// 
/// Usage:
///   cargo run --example ping
/// 
/// Sends a Ping request and expects Pong response.

use kafka_lite::protocol::{frame, request::Request, response::Response};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("🏓 RustLog Ping Test");
    println!("Connecting to broker at 127.0.0.1:9092...\n");

    // Connect to broker
    let mut stream = TcpStream::connect("127.0.0.1:9092").await?;
    println!("✅ Connected\n");

    // Send Ping
    println!("Sending Ping...");
    let request = Request::Ping;
    let request_bytes = request.encode();
    frame::write_frame(&mut stream, &request_bytes).await?;

    // Read Pong
    let response_bytes = frame::read_frame(&mut stream).await?;
    let response = Response::decode(&response_bytes)?;

    match response {
        Response::Pong => {
            println!("✅ Received Pong\n");
            println!("🎉 Broker is healthy!");
        }
        Response::Error { code, message } => {
            eprintln!("❌ Error {}: {}", code, message);
        }
        _ => {
            eprintln!("❌ Unexpected response type");
        }
    }

    Ok(())
}
