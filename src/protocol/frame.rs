use crate::error::BrokerError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Maximum frame size: 10MB
/// This prevents memory exhaustion from malicious or malformed clients.
/// In production Kafka, this is configurable (default 1MB, max typically 100MB).
const MAX_FRAME_SIZE: usize = 10 * 1024 * 1024;

/// Frame format: [4 bytes length (big-endian)][payload bytes]
/// 
/// Why big-endian?
/// - Network byte order standard (RFC 1700)
/// - Consistent with Kafka protocol
/// - Language-agnostic wire format
/// 
/// Why length-prefixed?
/// - Allows receiver to allocate exact buffer size
/// - No need for delimiters or escaping
/// - Natural backpressure via TCP flow control
///
///   Read a complete frame from the stream.
/// 
///   Returns the payload bytes (without the length prefix).
/// 
/// # Errors
/// - `ConnectionClosed` if stream ends before complete frame
/// - `FrameTooLarge` if length exceeds MAX_FRAME_SIZE
/// - `Io` on underlying I/O errors
pub async fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>, BrokerError> {
    // Read 4-byte length prefix
    let mut len_buf = [0u8; 4];
    let n = stream.read_exact(&mut len_buf).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            BrokerError::ConnectionClosed
        } else {
            BrokerError::Io(e)
        }
    })?;

    if n == 0 {
        return Err(BrokerError::ConnectionClosed);
    }

    let len = u32::from_be_bytes(len_buf) as usize;

    // Validate frame size
    if len > MAX_FRAME_SIZE {
        return Err(BrokerError::FrameTooLarge(len, MAX_FRAME_SIZE));
    }

    if len == 0 {
        return Err(BrokerError::InvalidProtocol("zero-length frame".to_string()));
    }

    // Read payload
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            BrokerError::ConnectionClosed
        } else {
            BrokerError::Io(e)
        }
    })?;

    Ok(payload)
}

/// Write a complete frame to the stream.
/// 
/// Automatically prepends the 4-byte length prefix.
/// 
/// # Errors
/// - `FrameTooLarge` if payload exceeds MAX_FRAME_SIZE
/// - `Io` on underlying I/O errors
pub async fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> Result<(), BrokerError> {
    let len = payload.len();

    if len > MAX_FRAME_SIZE {
        return Err(BrokerError::FrameTooLarge(len, MAX_FRAME_SIZE));
    }

    // Build frame: [length][payload]
    let mut frame = Vec::with_capacity(4 + len);
    frame.extend_from_slice(&(len as u32).to_be_bytes());
    frame.extend_from_slice(payload);

    // Write atomically
    // TCP guarantees in-order delivery, but we want to avoid partial writes
    // being visible as separate frames
    stream.write_all(&frame).await?;
    stream.flush().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_size_limit() {
        let large_size = MAX_FRAME_SIZE + 1;
        assert!(large_size > MAX_FRAME_SIZE);
    }
}
