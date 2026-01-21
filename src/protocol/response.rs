use crate::error::BrokerError;
use crate::protocol::request::Request;
use crate::topics::partition::Partition;
use crate::offsets::manager::OffsetManager;
use crate::admin::manager::AdminManager;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Partition registry type for broker-level partition lookup.
pub type PartitionRegistry = Arc<RwLock<HashMap<(String, u32), Arc<Partition>>>>;

/// Offset manager type for broker-level offset tracking.
pub type OffsetManagerRef = Arc<OffsetManager>;

/// Admin manager type for broker-level topic management.
pub type AdminManagerRef = Arc<AdminManager>;

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

    /// Response for a CreateTopic request.
    CreateTopicResponse { success: bool },

    /// Response for a ListTopics request.
    ListTopicsResponse { topics: Vec<(String, u32)> },

    /// Response for a DescribeTopic request.
    DescribeTopicResponse { 
        topic: String,
        partition_count: u32,
        partitions: Vec<(u32, u64)>, // (partition_id, high_watermark)
    },

    /// Response for a DescribePartition request.
    DescribePartitionResponse {
        topic: String,
        partition: u32,
        high_watermark: u64,
    },

    /// Error response for any failed request.
    /// 
    /// Payload format:
    /// - error_code (2 bytes, big-endian)
    /// - message_len (2 bytes, big-endian)
    /// - message (utf8 string)
    Error { code: u16, message: String },

    /// Response for a MetricsFetch request.
    MetricsResponse { metrics: String },

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
    pub fn handle(req: Request, partitions: &PartitionRegistry, offset_manager: &OffsetManagerRef, admin_manager: &AdminManagerRef) -> Self {
        match req {
            Request::Produce { records, topic, partition, .. } => {
                // Check if the partition exists before allowing produce
                let partition_key = (topic.clone(), partition);
                match partitions.read() {
                    Ok(registry) => match registry.get(&partition_key) {
                        Some(partition_instance) => {
                            // Partition exists - append records to active segment
                            let mut base_offset = 0u64;
                            let record_count = records.len() as u32;
                            
                            for (i, payload) in records.iter().enumerate() {
                                match partition_instance.append(payload) {
                                    Ok(offset) => {
                                        if i == 0 {
                                            base_offset = offset;
                                        }
                                    }
                                    Err(e) => {
                                        return Response::Error {
                                            code: 500,
                                            message: format!("Failed to write record: {}", e),
                                        };
                                    }
                                }
                            }
                            
                            Response::ProduceResponse {
                                base_offset,
                                record_count,
                            }
                        }
                        None => {
                            // Topic or partition doesn't exist - return error
                            Response::Error {
                                code: 10,
                                message: format!(
                                    "Topic or partition does not exist: topic={}, partition={}",
                                    topic, partition
                                ),
                            }
                        }
                    },
                    Err(_) => Response::Error {
                        code: 500,
                        message: "Internal server error accessing partitions".to_string(),
                    },
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
                let partition_instance = match partitions.read() {
                    Ok(registry) => match registry.get(&partition_key) {
                        Some(p) => p.clone(),
                        None => {
                            // Partition not found - this could be either topic doesn't exist 
                            // or partition doesn't exist in topic
                            return Response::Error {
                                code: 10,
                                message: format!(
                                    "Topic or partition does not exist: topic={}, partition={}",
                                    topic, partition
                                ),
                            };
                        }
                    },
                    Err(_) => {
                        return Response::Error {
                            code: 1,
                            message: "Internal error: failed to access partition registry".to_string(),
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
            Request::CreateTopic { topic, partition_count } => {
                match admin_manager.create_topic(topic, partition_count) {
                    Ok(()) => Response::CreateTopicResponse { success: true },
                    Err(BrokerError::TopicAlreadyExists(name)) => Response::Error {
                        code: 11,
                        message: format!("Topic '{}' already exists", name),
                    },
                    Err(BrokerError::InvalidPartitionCount(count)) => Response::Error {
                        code: 12,
                        message: format!("Invalid partition count: {}", count),
                    },
                    Err(_) => Response::Error {
                        code: 500,
                        message: "Internal server error during topic creation".to_string(),
                    },
                }
            }
            Request::ListTopics => {
                match admin_manager.list_topics() {
                    Ok(topic_names) => {
                        let topics = topic_names.into_iter()
                            .filter_map(|name| {
                                admin_manager.describe_topic(&name).ok()
                                    .map(|metadata| (metadata.name, metadata.partition_count))
                            })
                            .collect();
                        Response::ListTopicsResponse { topics }
                    }
                    Err(_) => Response::Error { code: 500, message: "Failed to list topics".to_string() },
                }
            }
            Request::DescribeTopic { topic } => {
                match admin_manager.describe_topic(&topic) {
                    Ok(metadata) => {
                        let partitions = metadata.partitions
                            .iter()
                            .map(|p| (p.partition_id, p.base_offset))
                            .collect();
                        Response::DescribeTopicResponse { 
                            topic: metadata.name,
                            partition_count: metadata.partition_count,
                            partitions,
                        }
                    }
                    Err(_) => Response::Error {
                        code: 13,
                        message: format!("DescribeTopic failed: Topic '{}' not found", topic),
                    },
                }
            }
            Request::DescribePartition { topic, partition } => {
                match admin_manager.describe_partition(&topic, partition) {
                    Ok(metadata) => {
                        Response::DescribePartitionResponse {
                            topic,
                            partition,
                            high_watermark: metadata.base_offset,
                        }
                    }
                    Err(_) => Response::Error {
                        code: 14,
                        message: format!("DescribePartition failed: Partition {} not found in topic '{}'", partition, topic),
                    },
                }
            }
            Request::MetricsFetch => {
                // Export metrics in JSON format for simplicity
                use crate::metrics::METRICS;
                let metrics_data = METRICS.export_prometheus();
                Response::MetricsResponse { metrics: metrics_data }
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
            Response::CreateTopicResponse { success } => {
                let mut buf = Vec::new();
                buf.push(0x05); // response type
                buf.push(if *success { 1 } else { 0 });
                buf
            }
            Response::ListTopicsResponse { topics } => {
                let mut buf = Vec::new();
                buf.push(0x06); // response type
                buf.extend_from_slice(&(topics.len() as u32).to_be_bytes());
                for (topic, partition_count) in topics {
                    buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
                    buf.extend_from_slice(topic.as_bytes());
                    buf.extend_from_slice(&partition_count.to_be_bytes());
                }
                buf
            }
            Response::DescribeTopicResponse { topic, partition_count, partitions } => {
                let mut buf = Vec::new();
                buf.push(0x07); // response type
                buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
                buf.extend_from_slice(topic.as_bytes());
                buf.extend_from_slice(&partition_count.to_be_bytes());
                buf.extend_from_slice(&(partitions.len() as u32).to_be_bytes());
                for (partition_id, high_watermark) in partitions {
                    buf.extend_from_slice(&partition_id.to_be_bytes());
                    buf.extend_from_slice(&high_watermark.to_be_bytes());
                }
                buf
            }
            Response::DescribePartitionResponse { topic, partition, high_watermark } => {
                let mut buf = Vec::new();
                buf.push(0x08); // response type
                buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
                buf.extend_from_slice(topic.as_bytes());
                buf.extend_from_slice(&partition.to_be_bytes());
                buf.extend_from_slice(&high_watermark.to_be_bytes());
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
            Response::MetricsResponse { metrics } => {
                let mut buf = Vec::new();
                buf.push(0x09); // response type
                buf.extend_from_slice(&(metrics.len() as u32).to_be_bytes());
                buf.extend_from_slice(metrics.as_bytes());
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
            0x05 => Self::decode_create_topic_response(payload),
            0x06 => Self::decode_list_topics_response(payload),
            0x07 => Self::decode_describe_topic_response(payload),
            0x08 => Self::decode_describe_partition_response(payload),
            0x09 => Self::decode_metrics_response(payload),
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

    fn decode_create_topic_response(buf: &[u8]) -> Result<Self, BrokerError> {
        if buf.is_empty() {
            return Err(BrokerError::DecodeError("truncated create topic response".to_string()));
        }
        let success = buf[0] == 1;
        Ok(Response::CreateTopicResponse { success })
    }

    fn decode_list_topics_response(buf: &[u8]) -> Result<Self, BrokerError> {
        if buf.len() < 4 {
            return Err(BrokerError::DecodeError("truncated list topics response".to_string()));
        }
        
        let topic_count = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let mut offset = 4;
        let mut topics = Vec::new();
        
        for _ in 0..topic_count {
            if buf.len() < offset + 2 {
                return Err(BrokerError::DecodeError("truncated topic name length".to_string()));
            }
            let name_len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
            offset += 2;
            
            if buf.len() < offset + name_len {
                return Err(BrokerError::DecodeError("truncated topic name".to_string()));
            }
            let name = String::from_utf8(buf[offset..offset + name_len].to_vec())
                .map_err(|e| BrokerError::DecodeError(format!("invalid utf8 topic name: {}", e)))?;
            offset += name_len;
            
            if buf.len() < offset + 4 {
                return Err(BrokerError::DecodeError("truncated partition count".to_string()));
            }
            let partition_count = u32::from_be_bytes([buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]]);
            offset += 4;
            
            topics.push((name, partition_count));
        }
        
        Ok(Response::ListTopicsResponse { topics })
    }

    fn decode_describe_topic_response(buf: &[u8]) -> Result<Self, BrokerError> {
        if buf.len() < 2 {
            return Err(BrokerError::DecodeError("truncated topic name length".to_string()));
        }
        
        let mut offset = 0;
        let name_len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;
        
        if buf.len() < offset + name_len {
            return Err(BrokerError::DecodeError("truncated topic name".to_string()));
        }
        let topic = String::from_utf8(buf[offset..offset + name_len].to_vec())
            .map_err(|e| BrokerError::DecodeError(format!("invalid utf8 topic name: {}", e)))?;
        offset += name_len;
        
        if buf.len() < offset + 8 {
            return Err(BrokerError::DecodeError("truncated partition count and partition list".to_string()));
        }
        let partition_count = u32::from_be_bytes([buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]]);
        offset += 4;
        
        let partitions_len = u32::from_be_bytes([buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]]) as usize;
        offset += 4;
        
        let mut partitions = Vec::new();
        for _ in 0..partitions_len {
            if buf.len() < offset + 12 {
                return Err(BrokerError::DecodeError("truncated partition info".to_string()));
            }
            let partition_id = u32::from_be_bytes([buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]]);
            offset += 4;
            let high_watermark = u64::from_be_bytes([
                buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3],
                buf[offset + 4], buf[offset + 5], buf[offset + 6], buf[offset + 7],
            ]);
            offset += 8;
            partitions.push((partition_id, high_watermark));
        }
        
        Ok(Response::DescribeTopicResponse { topic, partition_count, partitions })
    }

    fn decode_describe_partition_response(buf: &[u8]) -> Result<Self, BrokerError> {
        if buf.len() < 2 {
            return Err(BrokerError::DecodeError("truncated topic name length".to_string()));
        }
        
        let mut offset = 0;
        let name_len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;
        
        if buf.len() < offset + name_len {
            return Err(BrokerError::DecodeError("truncated topic name".to_string()));
        }
        let topic = String::from_utf8(buf[offset..offset + name_len].to_vec())
            .map_err(|e| BrokerError::DecodeError(format!("invalid utf8 topic name: {}", e)))?;
        offset += name_len;
        
        if buf.len() < offset + 12 {
            return Err(BrokerError::DecodeError("truncated partition and high watermark".to_string()));
        }
        let partition = u32::from_be_bytes([buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]]);
        offset += 4;
        let high_watermark = u64::from_be_bytes([
            buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3],
            buf[offset + 4], buf[offset + 5], buf[offset + 6], buf[offset + 7],
        ]);
        
        Ok(Response::DescribePartitionResponse { topic, partition, high_watermark })
    }

    fn decode_metrics_response(buf: &[u8]) -> Result<Self, BrokerError> {
        if buf.len() < 4 {
            return Err(BrokerError::DecodeError("missing metrics length".to_string()));
        }
        
        let metrics_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if buf.len() < 4 + metrics_len {
            return Err(BrokerError::DecodeError("truncated metrics data".to_string()));
        }
        
        let metrics = String::from_utf8(buf[4..4 + metrics_len].to_vec())
            .map_err(|e| BrokerError::DecodeError(format!("invalid utf8 metrics: {}", e)))?;
        
        Ok(Response::MetricsResponse { metrics })
    }
}

