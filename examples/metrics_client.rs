/// Example: Fetch and display broker metrics
/// 
/// Usage:
///   cargo run --example metrics_client
/// 
/// Connects to broker on localhost:9092 and fetches metrics.

use kafka_lite::protocol::{frame, request::Request, response::Response};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("📊 RustLog Metrics Client");
    println!("Connecting to broker at 127.0.0.1:9092...\n");

    // Connect to broker
    let mut stream = TcpStream::connect("127.0.0.1:9092").await?;
    println!("✅ Connected\n");

    // Send MetricsFetch request
    let request = Request::MetricsFetch;
    let request_bytes = request.encode();
    frame::write_frame(&mut stream, &request_bytes).await?;

    // Read response
    let response_bytes = frame::read_frame(&mut stream).await?;
    let response = Response::decode(&response_bytes)?;

    match response {
        Response::MetricsResponse { metrics } => {
            println!("📈 Broker Metrics (Prometheus Format):\n");
            println!("{}", metrics);
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
