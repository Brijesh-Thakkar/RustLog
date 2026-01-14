//! RustLog: A distributed log system
//!
//! This is the broker entry point.

use kafka_lite::offsets::manager::OffsetManager;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("╔═══════════════════════════════════════╗");
    println!("║        RustLog Broker v0.1.0          ║");
    println!("║      Distributed Log System           ║");
    println!("╚═══════════════════════════════════════╝");
    println!();

    let partitions = Arc::new(HashMap::new());
    let offset_manager = Arc::new(OffsetManager::new());

    kafka_lite::broker::server::run("127.0.0.1:9092", partitions, offset_manager).await?;

    Ok(())
}


