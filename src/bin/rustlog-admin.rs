/// rustlog-admin: Admin CLI for RustLog broker
/// 
/// Commands:
/// - create-topic <name> --partitions N
/// - list-topics
/// - describe-topic <name>
/// - describe-partition <name> <partition>
/// 
/// All output is JSON for script-friendly parsing.

use kafka_lite::protocol::{frame, request::Request, response::Response};
use tokio::net::TcpStream;
use serde_json::json;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        print_usage();
        return Ok(());
    }

    let command = &args[1];

    match command.as_str() {
        "create-topic" => {
            if args.len() < 4 || args[3] != "--partitions" || args.len() < 5 {
                eprintln!("Usage: rustlog-admin create-topic <name> --partitions <count>");
                std::process::exit(1);
            }
            let topic = &args[2];
            let partition_count: u32 = args[4].parse()
                .map_err(|_| anyhow::anyhow!("Invalid partition count"))?;
            create_topic(topic, partition_count).await?;
        }
        "list-topics" => {
            list_topics().await?;
        }
        "describe-topic" => {
            if args.len() < 3 {
                eprintln!("Usage: rustlog-admin describe-topic <name>");
                std::process::exit(1);
            }
            let topic = &args[2];
            describe_topic(topic).await?;
        }
        "describe-partition" => {
            if args.len() < 4 {
                eprintln!("Usage: rustlog-admin describe-partition <topic> <partition>");
                std::process::exit(1);
            }
            let topic = &args[2];
            let partition: u32 = args[3].parse()
                .map_err(|_| anyhow::anyhow!("Invalid partition number"))?;
            describe_partition(topic, partition).await?;
        }
        _ => {
            print_usage();
        }
    }

    Ok(())
}

fn print_usage() {
    println!("rustlog-admin: Admin CLI for RustLog broker");
    println!();
    println!("Usage:");
    println!("  rustlog-admin create-topic <name> --partitions <count>");
    println!("  rustlog-admin list-topics");
    println!("  rustlog-admin describe-topic <name>");
    println!("  rustlog-admin describe-partition <topic> <partition>");
    println!();
    println!("Examples:");
    println!("  rustlog-admin create-topic orders --partitions 3");
    println!("  rustlog-admin list-topics");
    println!("  rustlog-admin describe-topic orders");
    println!("  rustlog-admin describe-partition orders 0");
}

async fn connect() -> anyhow::Result<TcpStream> {
    let stream = TcpStream::connect("127.0.0.1:9092").await?;
    Ok(stream)
}

async fn create_topic(topic: &str, partition_count: u32) -> anyhow::Result<()> {
    let mut stream = connect().await?;

    let request = Request::CreateTopic {
        topic: topic.to_string(),
        partition_count,
    };

    let request_bytes = request.encode();
    frame::write_frame(&mut stream, &request_bytes).await?;

    let response_bytes = frame::read_frame(&mut stream).await?;
    let response = Response::decode(&response_bytes)?;

    match response {
        Response::CreateTopicResponse { success } => {
            if success {
                let output = json!({
                    "status": "success",
                    "message": format!("Topic '{}' created with {} partitions", topic, partition_count),
                    "topic": topic,
                    "partitions": partition_count
                });
                println!("{}", serde_json::to_string_pretty(&output)?);
                Ok(())
            } else {
                let output = json!({
                    "status": "error",
                    "message": "Failed to create topic"
                });
                println!("{}", serde_json::to_string_pretty(&output)?);
                std::process::exit(1);
            }
        }
        Response::Error { code, message } => {
            let output = json!({
                "status": "error",
                "error_code": code,
                "message": message
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
            std::process::exit(1);
        }
        _ => {
            anyhow::bail!("Unexpected response type");
        }
    }
}

async fn list_topics() -> anyhow::Result<()> {
    let mut stream = connect().await?;

    let request = Request::ListTopics;
    let request_bytes = request.encode();
    frame::write_frame(&mut stream, &request_bytes).await?;

    let response_bytes = frame::read_frame(&mut stream).await?;
    let response = Response::decode(&response_bytes)?;

    match response {
        Response::ListTopicsResponse { topics } => {
            let output = json!({
                "status": "success",
                "count": topics.len(),
                "topics": topics
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
            Ok(())
        }
        Response::Error { code, message } => {
            let output = json!({
                "status": "error",
                "error_code": code,
                "message": message
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
            std::process::exit(1);
        }
        _ => {
            anyhow::bail!("Unexpected response type");
        }
    }
}

async fn describe_topic(topic: &str) -> anyhow::Result<()> {
    let mut stream = connect().await?;

    let request = Request::DescribeTopic {
        topic: topic.to_string(),
    };
    let request_bytes = request.encode();
    frame::write_frame(&mut stream, &request_bytes).await?;

    let response_bytes = frame::read_frame(&mut stream).await?;
    let response = Response::decode(&response_bytes)?;

    match response {
        Response::DescribeTopicResponse {
            topic,
            partition_count,
            partitions,
        } => {
            let output = json!({
                "status": "success",
                "topic": topic,
                "partition_count": partition_count
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
            Ok(())
        }
        Response::Error { code, message } => {
            let output = json!({
                "status": "error",
                "error_code": code,
                "message": message
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
            std::process::exit(1);
        }
        _ => {
            anyhow::bail!("Unexpected response type");
        }
    }
}

async fn describe_partition(topic: &str, partition: u32) -> anyhow::Result<()> {
    let mut stream = connect().await?;

    let request = Request::DescribePartition {
        topic: topic.to_string(),
        partition,
    };
    let request_bytes = request.encode();
    frame::write_frame(&mut stream, &request_bytes).await?;

    let response_bytes = frame::read_frame(&mut stream).await?;
    let response = Response::decode(&response_bytes)?;

    match response {
        Response::DescribePartitionResponse {
            topic: topic_name,
            partition,
            high_watermark,
        } => {
            let output = json!({
                "status": "success",
                "topic": topic_name,
                "partition": partition,
                "high_watermark": high_watermark
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
            Ok(())
        }
        Response::Error { code, message } => {
            let output = json!({
                "status": "error",
                "error_code": code,
                "message": message
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
            std::process::exit(1);
        }
        _ => {
            anyhow::bail!("Unexpected response type");
        }
    }
}
