use crate::error::BrokerError;

/// Wire protocol request format:
/// 
/// [1 byte: request_type]
/// [remaining bytes: request-specific payload]
/// 
/// Request types:
/// - 0x01: Produce
/// - 0x02: Fetch
/// - 0x03: OffsetCommit
/// - 0x04: OffsetFetch
/// - 0xFF: Ping (for connection health checks)

#[derive(Debug, Clone)]
pub enum Request {
    /// Produce records to a topic partition.
    /// 
    /// Payload format:
    /// - topic_len (2 bytes, big-endian)
    /// - topic (utf8 string)
    /// - partition (4 bytes, big-endian)
    /// - record_count (4 bytes, big-endian)
    /// - records (array of length-prefixed byte arrays)
    Produce {
        topic: String,
        partition: u32,
        records: Vec<Vec<u8>>,
    },

    /// Fetch records from a topic partition.
    /// 
    /// If group_id is non-empty, fetch from committed offset for that consumer group.
    /// Otherwise, fetch from explicit offset.
    /// 
    /// Payload format:
    /// - group_id_len (2 bytes, big-endian)
    /// - group_id (utf8 string) - empty string means use explicit offset
    /// - topic_len (2 bytes, big-endian)
    /// - topic (utf8 string)
    /// - partition (4 bytes, big-endian)
    /// - offset (8 bytes, big-endian) - used only if group_id is empty
    /// - max_bytes (4 bytes, big-endian)
    Fetch {
        group_id: String,
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    },

    /// Commit a consumer offset for a consumer group.
    /// 
    /// Payload format:
    /// - group_len (2 bytes, big-endian)
    /// - group (utf8 string)
    /// - topic_len (2 bytes, big-endian)
    /// - topic (utf8 string)
    /// - partition (4 bytes, big-endian)
    /// - offset (8 bytes, big-endian)
    OffsetCommit {
        group: String,
        topic: String,
        partition: u32,
        offset: u64,
    },

    /// Fetch the committed offset for a consumer group.
    /// 
    /// Payload format:
    /// - group_len (2 bytes, big-endian)
    /// - group (utf8 string)
    /// - topic_len (2 bytes, big-endian)
    /// - topic (utf8 string)
    /// - partition (4 bytes, big-endian)
    OffsetFetch {
        group: String,
        topic: String,
        partition: u32,
    },

    /// Simple health check request.
    Ping,
}

impl Request {
    /// Decode a request from raw bytes.
    /// 
    /// The buffer contains the complete payload (after length-prefix was stripped).
    pub fn decode(buf: &[u8]) -> Result<Self, BrokerError> {
        if buf.is_empty() {
            return Err(BrokerError::DecodeError("empty buffer".to_string()));
        }

        let request_type = buf[0];
        let payload = &buf[1..];

        match request_type {
            0x01 => Self::decode_produce(payload),
            0x02 => Self::decode_fetch(payload),
            0x03 => Self::decode_offset_commit(payload),
            0x04 => Self::decode_offset_fetch(payload),
            0xFF => Ok(Request::Ping),
            _ => Err(BrokerError::UnknownRequestType(request_type)),
        }
    }

    fn decode_produce(buf: &[u8]) -> Result<Self, BrokerError> {
        let mut offset = 0;

        // Read topic
        if buf.len() < offset + 2 {
            return Err(BrokerError::DecodeError("missing topic length".to_string()));
        }
        let topic_len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;

        if buf.len() < offset + topic_len {
            return Err(BrokerError::DecodeError("truncated topic".to_string()));
        }
        let topic = String::from_utf8(buf[offset..offset + topic_len].to_vec())
            .map_err(|e| BrokerError::DecodeError(format!("invalid utf8 topic: {}", e)))?;
        offset += topic_len;

        // Read partition
        if buf.len() < offset + 4 {
            return Err(BrokerError::DecodeError("missing partition".to_string()));
        }
        let partition = u32::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);
        offset += 4;

        // Read record count
        if buf.len() < offset + 4 {
            return Err(BrokerError::DecodeError("missing record count".to_string()));
        }
        let record_count = u32::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]) as usize;
        offset += 4;

        // Read records (each is length-prefixed)
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

        Ok(Request::Produce {
            topic,
            partition,
            records,
        })
    }

    fn decode_fetch(buf: &[u8]) -> Result<Self, BrokerError> {
        let mut offset = 0;

        // Read group_id
        if buf.len() < offset + 2 {
            return Err(BrokerError::DecodeError("missing group_id length".to_string()));
        }
        let group_id_len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;

        if buf.len() < offset + group_id_len {
            return Err(BrokerError::DecodeError("truncated group_id".to_string()));
        }
        let group_id = String::from_utf8(buf[offset..offset + group_id_len].to_vec())
            .map_err(|e| BrokerError::DecodeError(format!("invalid utf8 group_id: {}", e)))?;
        offset += group_id_len;

        // Read topic
        if buf.len() < offset + 2 {
            return Err(BrokerError::DecodeError("missing topic length".to_string()));
        }
        let topic_len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;

        if buf.len() < offset + topic_len {
            return Err(BrokerError::DecodeError("truncated topic".to_string()));
        }
        let topic = String::from_utf8(buf[offset..offset + topic_len].to_vec())
            .map_err(|e| BrokerError::DecodeError(format!("invalid utf8 topic: {}", e)))?;
        offset += topic_len;

        // Read partition
        if buf.len() < offset + 4 {
            return Err(BrokerError::DecodeError("missing partition".to_string()));
        }
        let partition = u32::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);
        offset += 4;

        // Read offset
        if buf.len() < offset + 8 {
            return Err(BrokerError::DecodeError("missing offset".to_string()));
        }
        let offset_val = u64::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
            buf[offset + 4],
            buf[offset + 5],
            buf[offset + 6],
            buf[offset + 7],
        ]);
        offset += 8;

        // Read max_bytes
        if buf.len() < offset + 4 {
            return Err(BrokerError::DecodeError("missing max_bytes".to_string()));
        }
        let max_bytes = u32::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);

        Ok(Request::Fetch {
            group_id,
            topic,
            partition,
            offset: offset_val,
            max_bytes,
        })
    }

    /// Encode a request into raw bytes.
    /// 
    /// Used primarily for testing or building clients.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Request::Produce {
                topic,
                partition,
                records,
            } => {
                let mut buf = Vec::new();
                buf.push(0x01); // request type

                // Topic
                buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
                buf.extend_from_slice(topic.as_bytes());

                // Partition
                buf.extend_from_slice(&partition.to_be_bytes());

                // Record count
                buf.extend_from_slice(&(records.len() as u32).to_be_bytes());

                // Records
                for record in records {
                    buf.extend_from_slice(&(record.len() as u32).to_be_bytes());
                    buf.extend_from_slice(record);
                }

                buf
            }
            Request::Fetch {
                group_id,
                topic,
                partition,
                offset,
                max_bytes,
            } => {
                let mut buf = Vec::new();
                buf.push(0x02); // request type

                // Group ID
                buf.extend_from_slice(&(group_id.len() as u16).to_be_bytes());
                buf.extend_from_slice(group_id.as_bytes());

                // Topic
                buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
                buf.extend_from_slice(topic.as_bytes());

                // Partition
                buf.extend_from_slice(&partition.to_be_bytes());

                // Offset
                buf.extend_from_slice(&offset.to_be_bytes());

                // Max bytes
                buf.extend_from_slice(&max_bytes.to_be_bytes());

                buf
            }
            Request::OffsetCommit {
                group,
                topic,
                partition,
                offset,
            } => {
                let mut buf = Vec::new();
                buf.push(0x03); // request type

                // Group
                buf.extend_from_slice(&(group.len() as u16).to_be_bytes());
                buf.extend_from_slice(group.as_bytes());

                // Topic
                buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
                buf.extend_from_slice(topic.as_bytes());

                // Partition
                buf.extend_from_slice(&partition.to_be_bytes());

                // Offset
                buf.extend_from_slice(&offset.to_be_bytes());

                buf
            }
            Request::OffsetFetch {
                group,
                topic,
                partition,
            } => {
                let mut buf = Vec::new();
                buf.push(0x04); // request type

                // Group
                buf.extend_from_slice(&(group.len() as u16).to_be_bytes());
                buf.extend_from_slice(group.as_bytes());

                // Topic
                buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
                buf.extend_from_slice(topic.as_bytes());

                // Partition
                buf.extend_from_slice(&partition.to_be_bytes());

                buf
            }
            Request::Ping => vec![0xFF],
        }
    }

    fn decode_offset_commit(buf: &[u8]) -> Result<Self, BrokerError> {
        let mut offset = 0;

        // Read group
        if buf.len() < offset + 2 {
            return Err(BrokerError::DecodeError("missing group length".to_string()));
        }
        let group_len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;

        if buf.len() < offset + group_len {
            return Err(BrokerError::DecodeError("truncated group".to_string()));
        }
        let group = String::from_utf8(buf[offset..offset + group_len].to_vec())
            .map_err(|e| BrokerError::DecodeError(format!("invalid utf8 group: {}", e)))?;
        offset += group_len;

        // Read topic
        if buf.len() < offset + 2 {
            return Err(BrokerError::DecodeError("missing topic length".to_string()));
        }
        let topic_len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;

        if buf.len() < offset + topic_len {
            return Err(BrokerError::DecodeError("truncated topic".to_string()));
        }
        let topic = String::from_utf8(buf[offset..offset + topic_len].to_vec())
            .map_err(|e| BrokerError::DecodeError(format!("invalid utf8 topic: {}", e)))?;
        offset += topic_len;

        // Read partition
        if buf.len() < offset + 4 {
            return Err(BrokerError::DecodeError("missing partition".to_string()));
        }
        let partition = u32::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);
        offset += 4;

        // Read offset
        if buf.len() < offset + 8 {
            return Err(BrokerError::DecodeError("missing offset".to_string()));
        }
        let offset_val = u64::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
            buf[offset + 4],
            buf[offset + 5],
            buf[offset + 6],
            buf[offset + 7],
        ]);

        Ok(Request::OffsetCommit {
            group,
            topic,
            partition,
            offset: offset_val,
        })
    }

    fn decode_offset_fetch(buf: &[u8]) -> Result<Self, BrokerError> {
        let mut offset = 0;

        // Read group
        if buf.len() < offset + 2 {
            return Err(BrokerError::DecodeError("missing group length".to_string()));
        }
        let group_len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;

        if buf.len() < offset + group_len {
            return Err(BrokerError::DecodeError("truncated group".to_string()));
        }
        let group = String::from_utf8(buf[offset..offset + group_len].to_vec())
            .map_err(|e| BrokerError::DecodeError(format!("invalid utf8 group: {}", e)))?;
        offset += group_len;

        // Read topic
        if buf.len() < offset + 2 {
            return Err(BrokerError::DecodeError("missing topic length".to_string()));
        }
        let topic_len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;

        if buf.len() < offset + topic_len {
            return Err(BrokerError::DecodeError("truncated topic".to_string()));
        }
        let topic = String::from_utf8(buf[offset..offset + topic_len].to_vec())
            .map_err(|e| BrokerError::DecodeError(format!("invalid utf8 topic: {}", e)))?;
        offset += topic_len;

        // Read partition
        if buf.len() < offset + 4 {
            return Err(BrokerError::DecodeError("missing partition".to_string()));
        }
        let partition = u32::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);

        Ok(Request::OffsetFetch {
            group,
            topic,
            partition,
        })
    }
}

