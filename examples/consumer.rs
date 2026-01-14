//! RustLog Consumer Demo

use kafka_lite::protocol::{frame, request::Request, response::Response};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("RustLog Consumer Demo");
    println!("=====================\n");

    let args: Vec<String> = std::env::args().collect();
    let group_id = parse_group_arg(&args);

    println!("Connecting to broker at 127.0.0.1:9092...");
    let mut stream = TcpStream::connect("127.0.0.1:9092").await?;
    println!("Connected\n");

    let topic = "demo";
    let partition = 0;
    let max_bytes = 1024 * 1024;

    let start_offset = if let Some(group) = &group_id {
        println!("Consumer group: {}", group);
        get_committed_offset(&mut stream, group, topic, partition).await?
    } else {
        println!("Offset-based consumer (no group)");
        0
    };

    println!("Starting offset: {}\n", start_offset);

    println!("Fetching from topic '{}', partition {}...", topic, partition);
    let request = Request::Fetch {
        group_id: group_id.clone().unwrap_or_default(),
        topic: topic.to_string(),
        partition,
        offset: start_offset,
        max_bytes,
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
            println!("\nFetch successful!");
            println!("Records fetched: {}", records.len());
            println!("Next offset: {}\n", next_offset);

            if records.is_empty() {
                println!("No records available at offset {}", start_offset);
            } else {
                println!("Records:");
                for (i, record) in records.iter().enumerate() {
                    let offset = start_offset + i as u64;
                    let content = String::from_utf8_lossy(record);
                    println!("  Offset {}: {}", offset, content);
                }
            }

            if let Some(group) = &group_id {
                println!("\nCommitting offset {} for group '{}'...", next_offset, group);
                commit_offset(&mut stream, group, topic, partition, next_offset).await?;
                println!("Committed");
            }
        }
        Response::Error { code, message } => {
            eprintln!("\nError fetching records:");
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

fn parse_group_arg(args: &[String]) -> Option<String> {
    let mut iter = args.iter().skip(1);
    while let Some(arg) = iter.next() {
        if arg == "--group" {
            return iter.next().cloned();
        }
    }
    None
}

async fn get_committed_offset(
    stream: &mut TcpStream,
    group: &str,
    topic: &str,
    partition: u32,
) -> anyhow::Result<u64> {
    let request = Request::OffsetFetch {
        group: group.to_string(),
        topic: topic.to_string(),
        partition,
    };

    let request_bytes = request.encode();
    frame::write_frame(stream, &request_bytes).await?;

    let response_bytes = frame::read_frame(stream).await?;
    let response = Response::decode(&response_bytes)?;

    match response {
        Response::OffsetFetchResponse { offset } => {
            if let Some(committed) = offset {
                println!("Found committed offset: {}", committed);
                Ok(committed)
            } else {
                println!("No committed offset found, starting from 0");
                Ok(0)
            }
        }
        Response::Error { code, message } => {
            eprintln!("Error fetching committed offset:");
            eprintln!("  Code: {}", code);
            eprintln!("  Message: {}", message);
            std::process::exit(1);
        }
        _ => {
            eprintln!("Unexpected response type: {:?}", response);
            std::process::exit(1);
        }
    }
}

async fn commit_offset(
    stream: &mut TcpStream,
    group: &str,
    topic: &str,
    partition: u32,
    offset: u64,
) -> anyhow::Result<()> {
    let request = Request::OffsetCommit {
        group: group.to_string(),
        topic: topic.to_string(),
        partition,
        offset,
    };

    let request_bytes = request.encode();
    frame::write_frame(stream, &request_bytes).await?;

    let response_bytes = frame::read_frame(stream).await?;
    let response = Response::decode(&response_bytes)?;

    match response {
        Response::OffsetCommitResponse { success } => {
            if !success {
                eprintln!("Warning: Commit reported failure");
            }
            Ok(())
        }
        Response::Error { code, message } => {
            eprintln!("Error committing offset:");
            eprintln!("  Code: {}", code);
            eprintln!("  Message: {}", message);
            std::process::exit(1);
        }
        _ => {
            eprintln!("Unexpected response type: {:?}", response);
            std::process::exit(1);
        }
    }
}
