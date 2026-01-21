use crate::error::BrokerError;
use crate::protocol::{frame, request::Request, response::{Response, OffsetManagerRef, AdminManagerRef}};
use crate::topics::partition::Partition;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::net::TcpStream;

/// Partition registry for the broker.
/// 
/// Maps (topic, partition_id) → Partition instance.
/// In a full Kafka impl, this would be:
/// - Persisted to disk (metadata)
/// - Replicated via Raft/ZooKeeper
/// - Dynamically updated on topic creation
/// 
/// For MVP: we use a simple in-memory RwLock<HashMap> to allow topic creation.
/// Partitions are immutable once registered (no dynamic topic creation yet).
pub type PartitionRegistry = Arc<RwLock<HashMap<(String, u32), Arc<Partition>>>>;

/// Handle a single client connection.
/// 
/// Connection lifecycle:
/// 1. Accept TCP connection
/// 2. Loop: read frame → decode request → handle → encode response → write frame
/// 3. On error or EOF, close connection
/// 
/// Each connection runs in its own async task, spawned by the server.
/// 
/// Key design decisions:
/// - Stateless: each request is independent
/// - No connection pooling needed (clients can maintain persistent connections)
/// - Errors close the connection (fail-fast)
/// - No authentication yet (future: SASL)
/// 
/// Why not hold locks across .await?
/// - Holding a lock across .await can deadlock the tokio runtime
/// - When we add storage, we'll acquire locks only for critical sections,
///   then release before any I/O
/// 
/// Backpressure:
/// - TCP flow control provides natural backpressure
/// - If client stops reading, kernel buffers fill, broker's write stalls
/// - If broker stops reading, client's kernel buffers fill, client's write stalls
pub async fn handle_connection(
    mut stream: TcpStream,
    partitions: PartitionRegistry,
    offset_manager: OffsetManagerRef,
    admin_manager: AdminManagerRef,
) -> Result<(), BrokerError> {
    loop {
        // 1. Read frame from network
        //    This will block (yield to runtime) until a complete frame arrives
        let frame_bytes = match frame::read_frame(&mut stream).await {
            Ok(bytes) => bytes,
            Err(BrokerError::ConnectionClosed) => {
                // Client closed connection gracefully
                return Ok(());
            }
            Err(e) => {
                // Protocol error or I/O error
                // Send error response if possible, then close
                let error_response = Response::Error {
                    code: 1,
                    message: format!("Frame error: {}", e),
                };
                let response_bytes = error_response.encode();
                let _ = frame::write_frame(&mut stream, &response_bytes).await;
                return Err(e);
            }
        };

        // 2. Decode request
        let request = match Request::decode(&frame_bytes) {
            Ok(req) => req,
            Err(e) => {
                // Send error response
                let error_response = Response::Error {
                    code: 2,
                    message: format!("Decode error: {}", e),
                };
                let response_bytes = error_response.encode();
                frame::write_frame(&mut stream, &response_bytes).await?;
                continue;
            }
        };

        // 3. Handle request
        //    This now routes to actual partition storage for Fetch requests
        //    and offset manager for OffsetCommit/OffsetFetch requests.
        //    Produce is still stubbed (write path not in scope).
        let response = Response::handle(request, &partitions, &offset_manager, &admin_manager);

        // 4. Encode response
        let response_bytes = response.encode();

        // 5. Write frame to network
        frame::write_frame(&mut stream, &response_bytes).await?;
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_connection_handler_compiles() {
        // This test just ensures the module compiles
        // Real tests will use tokio::net::TcpListener in integration tests
    }
}

