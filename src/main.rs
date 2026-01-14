//! RustLog: A Kafka-lite distributed log system
//! 
//! This is the broker entry point.
//! 
//! Current capabilities:
//! - TCP server with length-prefixed framing
//! - Produce/Fetch protocol with real storage backend
//! - Connection lifecycle management
//! - Partition-based log storage
//! - Consumer offset tracking (in-memory)
//! 
//! Not yet implemented:
//! - Dynamic topic/partition creation
//! - Offset persistence
//! - Replication

use kafka_lite::offsets::manager::OffsetManager;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("╔═══════════════════════════════════════╗");
    println!("║        RustLog Broker v0.1.0          ║");
    println!("║     Kafka-lite Distributed Log        ║");
    println!("╚═══════════════════════════════════════╝");
    println!();

    // Create empty partition registry
    // In production, this would be:
    // - Loaded from disk metadata
    // - Populated via admin API
    // - Synced via cluster coordination (Raft/ZooKeeper)
    //
    // For now: empty registry means Fetch requests will fail with "Partition not found"
    // To test with real data, partitions must be registered externally
    let partitions = Arc::new(HashMap::new());

    // Create offset manager for consumer group coordination
    // In-memory for MVP; would be persisted in production
    let offset_manager = Arc::new(OffsetManager::new());

    // Start server
    // Will run forever until process is killed
    kafka_lite::broker::server::run("127.0.0.1:9092", partitions, offset_manager).await?;

    Ok(())
}


