use crate::broker::connection::{self, PartitionRegistry};
use crate::protocol::response::{OffsetManagerRef, AdminManagerRef};
use tokio::net::TcpListener;
use std::net::SocketAddr;
use std::sync::Arc;

/// Run the broker TCP server.
/// 
/// Architecture:
/// - Main loop accepts connections
/// - Each connection spawned into independent async task
/// - Tasks are detached (fire-and-forget)
/// - Server runs forever until process exits
/// 
/// Why spawn per connection?
/// - Isolates failures: one bad connection doesn't crash others
/// - Natural concurrency: tokio schedules tasks efficiently
/// - Simple mental model: each connection = one task
/// 
/// Production considerations (future):
/// - Connection limiting (max concurrent connections)
/// - Graceful shutdown (tokio::signal + CancellationToken)
/// - Connection metrics (active count, request rate)
/// - Listener backlog tuning
/// 
/// Why not use channels for request routing?
/// - Direct task-per-connection is simpler and sufficient for MVP
/// - Channels would add overhead without clear benefit
/// - Future: if we need work-stealing or priority queues, reconsider
pub async fn run(addr: &str, partitions: PartitionRegistry, offset_manager: OffsetManagerRef, admin_manager: AdminManagerRef) -> anyhow::Result<()> {
    // Bind to address
    // This will fail fast if address is in use or invalid
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    
    println!("🚀 Broker listening on {}", local_addr);
    println!("   Protocol: binary, length-prefixed");
    println!("   Max frame size: 10MB");
    println!("   Partitions loaded: {}", partitions.read().map(|p| p.len()).unwrap_or(0));
    println!();
    
    // Accept loop
    loop {
        // Wait for incoming connection
        // This yields to the runtime, allowing other tasks to run
        let (stream, peer_addr) = listener.accept().await?;
        
        println!("📥 Accepted connection from {}", peer_addr);
        
        // Clone Arc for this connection
        // This is cheap (just incrementing ref count)
        let partitions_clone = Arc::clone(&partitions);
        let offset_manager_clone = Arc::clone(&offset_manager);
        let admin_manager_clone = Arc::clone(&admin_manager);
        
        // Spawn independent task for this connection
        // The task is detached: we don't await it
        // This allows the server to immediately accept the next connection
        tokio::spawn(async move {
            if let Err(e) = connection::handle_connection(stream, partitions_clone, offset_manager_clone, admin_manager_clone).await {
                // Connection error (protocol violation, network error, etc.)
                // Log and continue
                // In production: structured logging (tracing crate)
                eprintln!("❌ Connection error from {}: {:?}", peer_addr, e);
            } else {
                // Connection closed gracefully
                println!("👋 Connection closed: {}", peer_addr);
            }
        });
    }
}

/// Server configuration (for future use).
/// 
/// Not used yet, but this is where we'll add:
/// - max_connections
/// - read_timeout / write_timeout
/// - enable_tls
/// - bind address
#[allow(dead_code)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub max_connections: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9092".parse().unwrap(),
            max_connections: 1000,
        }
    }
}

