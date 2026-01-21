//! RustLog: A distributed log system
//!
//! This is the broker entry point.

use kafka_lite::offsets::manager::OffsetManager;
use kafka_lite::admin::manager::AdminManager;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("╔═══════════════════════════════════════╗");
    println!("║        RustLog Broker v0.1.0          ║");
    println!("║      Distributed Log System           ║");
    println!("╚═══════════════════════════════════════╝");
    println!();

    let partitions = Arc::new(RwLock::new(HashMap::new()));
    let offset_manager = Arc::new(OffsetManager::new());
    let admin_manager = Arc::new(AdminManager::with_partitions(partitions.clone()));

    kafka_lite::broker::server::run("127.0.0.1:9092", partitions, offset_manager, admin_manager).await?;

    Ok(())
}


