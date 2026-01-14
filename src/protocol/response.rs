use crate::error::BrokerError;
use crate::protocol::request::Request;
use crate::topics::partition::Partition;
use crate::offsets::manager::OffsetManager;
use std::collections::HashMap;
use std::sync::Arc;

/// Partition registry type for broker-level partition lookup.
pub type PartitionRegistry = Arc<HashMap<(String, u32), Arc<Partition>>>;

/// Offset manager type for broker-level offset tracking.
pub type OffsetManagerRef = Arc<OffsetManager>;

/// Wire protocol response format:
/// 
/// [1 byte: response_type]
/// [remaining bytes: response-specific payload]
/// 
/// Response types:
/// - 0x01: ProduceResponse
/// - 0x02: FetchResponse
/// - 0x03: OffsetCommitResponse
/// - 0x04: OffsetFetchResponse
/// - 0xFE: ErrorResponse
/// - 0xFF: Pong

#[derive(Debug, Clone)]
pub enum Response {
    /// Acknowledgment for a Produce request.
    /// 
    /// Payload format:
    /// - base_offset (8 bytes, big-endian): the starting logical offset assigned to the batch
    /// - record_count (4 bytes, big-endian): number of records successfully written
    /// 
    /// The producer can compute the offset of each record:
    /// record[i] has offset = base_offset + i
    ProduceResponse {
        base_offset: u64,
        record_count: u32,
    },

    /// Response for a Fetch request.
    /// 
    /// Payload format:
    /// - record_count (4 bytes, big-endian)
    /// - records (array of length-prefixed byte arrays)
    /// - next_offset (8 bytes, big-endian): offset for next fetch
    /// 
    /// If no records available, record_count = 0 and next_offset = requested offset
    FetchResponse {
        records: Vec<Vec<u8>>,
        next_offset: u64,
    },

    /// Response for an OffsetCommit request.
    /// 
    /// Payload format:
    /// - success (1 byte): 1 if committed, 0 if failed
    OffsetCommitResponse { success: bool },

    /// Response for an OffsetFetch request.
    /// 
    /// Payload format:
    /// - has_offset (1 byte): 1 if offset exists, 0 if not
    /// - offset (8 bytes, big-endian): only present if has_offset = 1
    OffsetFetchResponse { offset: Option<u64> },

    /// Error response for any failed request.
    /// 
    /// Payload format:
    /// - error_code (2 bytes, big-endian)
    /// - message_len (2 bytes, big-endian)
    /// - message (utf8 string)
    Error { code: u16, message: String },

    /// Simple health check response.
    Pong,
}

impl Response {
    /// Handle a request and produce a response.
    /// 
    /// For Fetch requests:
    /// - Looks up the partition from the registry
    /// - Calls partition.fetch(offset, max_bytes)
    /// - Converts Record to wire format (payload only, offset included)
    /// - Returns FetchResponse with records and next_offset
    /// 
    /// For Produce requests:
    /// - Still stubbed (write path not in scope)
    /// 
    /// For OffsetCommit requests:
    /// - Calls offset_manager.commit()
    /// - Returns success or error
    /// 
    /// For OffsetFetch requests:
    /// - Calls offset_manager.fetch_committed()
    /// - Returns committed offset or None
    /// 
    /// Error handling:
    /// - Partition not found → Error response
    /// - OffsetOutOfRange → FetchResponse with empty records, next_offset = requested offset
    /// - Storage errors → Error response
    pub fn handle(req: Request, partitions: &PartitionRegistry, offset_manager: &OffsetManagerRef) -> Self {
        match req {
            Request::Produce { records, .. } => {
                // Stub: pretend we wrote at offset 0
                // Write path not in scope for this task
                Response::ProduceResponse {
                    base_offset: 0,
                    record_count: records.len() as u32,
                }
            }
            Request::Fetch {
                group_id,
                topic,
                partition,
                offset,
                max_bytes,
            } => {
                // Determine the starting offset:
                // - If group_id is provided (non-empty), fetch committed offset from OffsetManager
                // - If no committed offset exists, return empty result
                // - If group_id is empty, use explicit offset from request
                let start_offset = if !group_id.is_empty() {
                    match offset_manager.fetch_committed(&group_id, &topic, partition) {
                        Some(committed) => committed,
                        None => {
                            // No committed offset for this consumer group
                            // Return empty FetchResponse (consumer must commit an offset first)
                            return Response::FetchResponse {
                                records: vec![],
                                next_offset: 0,
                            };
                        }
                    }
                } else {
                    // No group_id provided, use explicit offset
                    offset
                };

                // Look up partition in registry
                let partition_key = (topic.clone(), partition);
                let partition_instance = match partitions.get(&partition_key) {
                    Some(p) => p,
                    None => {
                        // Partition not found
                        return Response::Error {
                            code: 3,
                            message: format!(
                                "Partition not found: topic={}, partition={}",
                                topic, partition
                            ),
                        };
                    }
                };

                // Call partition.fetch() with the determined start_offset
                // This is synchronous storage I/O, but it's acceptable because:
                // - Reads are typically fast (mmap for sealed, sequential for active)
                // - Tokio allows blocking in task threads (not in core runtime)
                // - Alternative would be spawn_blocking, but adds complexity for MVP
                match partition_instance.fetch(start_offset, max_bytes as usize) {
                    Ok(fetch_result) => {
                        // Convert Record { offset, payload } to just payload bytes
                        // The wire protocol includes offset implicitly via ordering
                        let records: Vec<Vec<u8>> = fetch_result
                            .records
                            .into_iter()
                            .map(|record| record.payload)
                            .collect();

                        Response::FetchResponse {
                            records,
                            next_offset: fetch_result.next_offset,
                        }
                    }
                    Err(BrokerError::OffsetOutOfRange { requested, .. }) => {
                        // OffsetOutOfRange: return empty fetch with next_offset = requested
                        // Client can retry or adjust their offset
                        Response::FetchResponse {
                            records: vec![],
                            next_offset: requested,
                        }
                    }
                    Err(e) => {
                        // Other storage errors
                        Response::Error {
                            code: 4,
                            message: format!("Fetch failed: {}", e),
                        }
                    }
                }
            }
            Request::OffsetCommit {
                group,
                topic,
                partition,
                offset,
            } => {
                // Commit the offset to the offset manager
                match offset_manager.commit(&group, &topic, partition, offset) {
                    Ok(()) => Response::OffsetCommitResponse { success: true },
                    Err(e) => Response::Error {
                        code: 5,
                        message: format!("Offset commit failed: {}", e),
                    },
                }
            }
            Request::OffsetFetch {
                group,
                topic,
                partition,
            } => {
                // Fetch committed offset from the offset manager
                let offset = offset_manager.fetch_committed(&group, &topic, partition);
                Response::OffsetFetchResponse { offset }
            }
            Request::Ping => Response::Pong,
        }
    }

    /// Encode a response into raw bytes.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Response::ProduceResponse {
                base_offset,
                record_count,
            } => {
                let mut buf = Vec::new();
                buf.push(0x01); // response type
                buf.extend_from_slice(&base_offset.to_be_bytes());
                buf.extend_from_slice(&record_count.to_be_bytes());
                buf
            }
            Response::FetchResponse {
                records,
                next_offset,
            } => {
                let mut buf = Vec::new();
                buf.push(0x02); // response type

                // Record count
                buf.extend_from_slice(&(records.len() as u32).to_be_bytes());

                // Records (each length-prefixed)
                for record in records {
                    buf.extend_from_slice(&(record.len() as u32).to_be_bytes());
                    buf.extend_from_slice(record);
                }

                // Next offset
                buf.extend_from_slice(&next_offset.to_be_bytes());

                buf
            }
            Response::OffsetCommitResponse { success } => {
                let mut buf = Vec::new();
                buf.push(0x03); // response type
                buf.push(if *success { 1 } else { 0 });
                buf
            }
            Response::OffsetFetchResponse { offset } => {
                let mut buf = Vec::new();
                buf.push(0x04); // response type
                match offset {
                    Some(off) => {
                        buf.push(1); // has_offset = true
                        buf.extend_from_slice(&off.to_be_bytes());
                    }
                    None => {
                        buf.push(0); // has_offset = false
                    }
                }
                buf
            }
            Response::Error { code, message } => {
                let mut buf = Vec::new();
                buf.push(0xFE); // response type
                buf.extend_from_slice(&code.to_be_bytes());
                buf.extend_from_slice(&(message.len() as u16).to_be_bytes());
                buf.extend_from_slice(message.as_bytes());
                buf
            }
            Response::Pong => vec![0xFF],
        }
    }

    /// Decode a response from raw bytes.
    /// 
    /// Used primarily for testing and client implementations.
    pub fn decode(buf: &[u8]) -> Result<Self, BrokerError> {
        if buf.is_empty() {
            return Err(BrokerError::DecodeError("empty buffer".to_string()));
        }

        let response_type = buf[0];
        let payload = &buf[1..];

        match response_type {
            0x01 => Self::decode_produce_response(payload),
            0x02 => Self::decode_fetch_response(payload),
            0x03 => Self::decode_offset_commit_response(payload),
            0x04 => Self::decode_offset_fetch_response(payload),
            0xFE => Self::decode_error_response(payload),
            0xFF => Ok(Response::Pong),
            _ => Err(BrokerError::UnknownResponseType(response_type)),
        }
    }

    fn decode_produce_response(buf: &[u8]) -> Result<Self, BrokerError> {
        if buf.len() < 12 {
            return Err(BrokerError::DecodeError(
                "truncated produce response".to_string(),
            ));
        }

        let base_offset = u64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        let record_count = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);

        Ok(Response::ProduceResponse {
            base_offset,
            record_count,
        })
    }

    fn decode_fetch_response(buf: &[u8]) -> Result<Self, BrokerError> {
        let mut offset = 0;

        if buf.len() < 4 {
            return Err(BrokerError::DecodeError("missing record count".to_string()));
        }

        let record_count =
            u32::from_be_bytes([buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]])
                as usize;
        offset += 4;

        // Read records
        let mut records = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            if buf.len() < offset + 4 {
                return Err(BrokerError::DecodeError("truncated record length".to_string()));
            }
            let record_len = u32::from_be_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ]) as usize;
            offset += 4;

            if buf.len() < offset + record_len {
                return Err(BrokerError::DecodeError("truncated record".to_string()));
            }
            let record = buf[offset..offset + record_len].to_vec();
            offset += record_len;

            records.push(record);
        }

        // Read next_offset
        if buf.len() < offset + 8 {
            return Err(BrokerError::DecodeError("missing next_offset".to_string()));
        }
        let next_offset = u64::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
            buf[offset + 4],
            buf[offset + 5],
            buf[offset + 6],
            buf[offset + 7],
        ]);

        Ok(Response::FetchResponse {
            records,
            next_offset,
        })
    }

    fn decode_error_response(buf: &[u8]) -> Result<Self, BrokerError> {
        let mut offset = 0;

        if buf.len() < 2 {
            return Err(BrokerError::DecodeError("missing error code".to_string()));
        }
        let code = u16::from_be_bytes([buf[offset], buf[offset + 1]]);
        offset += 2;

        if buf.len() < offset + 2 {
            return Err(BrokerError::DecodeError("missing message length".to_string()));
        }
        let message_len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;

        if buf.len() < offset + message_len {
            return Err(BrokerError::DecodeError("truncated message".to_string()));
        }
        let message = String::from_utf8(buf[offset..offset + message_len].to_vec())
            .map_err(|e| BrokerError::DecodeError(format!("invalid utf8 message: {}", e)))?;

        Ok(Response::Error { code, message })
    }

    fn decode_offset_commit_response(buf: &[u8]) -> Result<Self, BrokerError> {
        if buf.is_empty() {
            return Err(BrokerError::DecodeError(
                "missing success flag".to_string(),
            ));
        }

        let success = buf[0] != 0;
        Ok(Response::OffsetCommitResponse { success })
    }

    fn decode_offset_fetch_response(buf: &[u8]) -> Result<Self, BrokerError> {
        if buf.is_empty() {
            return Err(BrokerError::DecodeError(
                "missing has_offset flag".to_string(),
            ));
        }

        let has_offset = buf[0] != 0;
        if has_offset {
            if buf.len() < 9 {
                return Err(BrokerError::DecodeError("truncated offset".to_string()));
            }
            let offset = u64::from_be_bytes([
                buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
            ]);
            Ok(Response::OffsetFetchResponse {
                offset: Some(offset),
            })
        } else {
            Ok(Response::OffsetFetchResponse { offset: None })
        }
    }
}

