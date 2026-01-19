# RustLog Protocol Specification

**Audience:** Systems engineers implementing clients, analyzing network traces, or debugging protocol interactions.

## Protocol Philosophy

RustLog uses a **binary-framed, JSON-payload protocol** over TCP that prioritizes:

1. **Debuggability**: JSON payloads are human-readable in network captures
2. **Simplicity**: Minimal frame overhead, straightforward parsing
3. **Evolution**: JSON enables backward-compatible field additions
4. **Type safety**: Strong typing prevents invalid message construction

**Explicit non-goals**:
- Kafka wire format compatibility
- Maximum throughput optimization  
- Complex authentication/authorization
- Multi-version protocol support

## Wire Format Specification

### Frame Structure

**All messages use length-prefixed binary framing**:

```
┌─────────────────────────────────────────────────────────────┐
│                         TCP Stream                          │
├─────────────────────────────────────────────────────────────┤
│  Frame 1                                                    │
│  ┌──────────────────┬───────────────────────────────────┐   │
│  │ Length Header    │ Payload                           │   │
│  │ 4 bytes          │ {length} bytes                    │   │
│  │ u32 big-endian   │ JSON-encoded request/response     │   │
│  └──────────────────┴───────────────────────────────────┘   │
│                                                             │
│  Frame 2                                                    │
│  ┌──────────────────┬───────────────────────────────────┐   │
│  │ Length Header    │ Payload                           │   │
│  └──────────────────┴───────────────────────────────────┘   │
│                                                             │
│  ...                                                        │
└─────────────────────────────────────────────────────────────┘
```

**Frame parsing algorithm**:
```rust
async fn read_frame<R: AsyncRead>(reader: &mut R) -> Result<Vec<u8>, FrameError> {
    // 1. Read 4-byte length header
    let mut length_bytes = [0u8; 4];
    reader.read_exact(&mut length_bytes).await?;
    let payload_length = u32::from_be_bytes(length_bytes);
    
    // 2. Validate length
    if payload_length > MAX_FRAME_SIZE {
        return Err(FrameError::TooLarge(payload_length));
    }
    if payload_length == 0 {
        return Err(FrameError::EmptyFrame);
    }
    
    // 3. Read payload
    let mut payload = vec![0u8; payload_length as usize];
    reader.read_exact(&mut payload).await?;
    
    Ok(payload)
}
```

**Protocol constants**:
- **MAX_FRAME_SIZE**: 10,485,760 bytes (10MB)
- **MIN_FRAME_SIZE**: 1 byte  
- **LENGTH_FIELD_SIZE**: 4 bytes
- **ENDIANNESS**: Big-endian (network byte order)

### JSON Payload Format

**All payloads are UTF-8 encoded JSON** with these requirements:

1. **Valid JSON**: Must parse successfully with standard JSON parser
2. **UTF-8 encoding**: No binary data outside Base64 encoding  
3. **Object root**: Top-level must be JSON object `{...}`
4. **Type field**: Must contain `"type": "RequestType"` or `"type": "ResponseType"`

**Example frame**:
```
Length: [0x00, 0x00, 0x00, 0x4A]  (74 bytes)
Payload: {"type":"Produce","topic":"logs","partition":0,"records":["SGVsbG8gV29ybGQ="]}
```

## Message Types

### Message Type Enumeration

```rust
// Request types
pub enum Request {
    Produce(ProduceRequest),           // 0x01
    Fetch(FetchRequest),               // 0x02  
    OffsetCommit(OffsetCommitRequest), // 0x03
    OffsetFetch(OffsetFetchRequest),   // 0x04
    CreateTopic(CreateTopicRequest),   // 0x05
    ListTopics,                        // 0x06
    DescribeTopic(DescribeTopicRequest), // 0x07
    DescribePartition(DescribePartitionRequest), // 0x08
    MetricsFetch,                      // 0x09
    Ping,                              // 0x0A
}

// Response types  
pub enum Response {
    ProduceResponse(ProduceResponseData),     // 0x81
    FetchResponse(FetchResponseData),         // 0x82
    OffsetCommitResponse(OffsetCommitResponseData), // 0x83
    OffsetFetchResponse(OffsetFetchResponseData),   // 0x84
    CreateTopicResponse(CreateTopicResponseData),   // 0x85
    ListTopicsResponse(ListTopicsResponseData),     // 0x86
    DescribeTopicResponse(DescribeTopicResponseData), // 0x87
    DescribePartitionResponse(DescribePartitionResponseData), // 0x88
    MetricsFetchResponse(MetricsData),        // 0x89
    Pong,                                     // 0x8A
    Error(ErrorResponse),                     // 0xFF
}
```

**Type mapping**: Response type = Request type + 0x80 (except Error = 0xFF).

## Core Data Operations

### Produce Request

**Purpose**: Append records to a partition log.

**JSON Schema**:
```json
{
  "type": "Produce",
  "topic": "string",
  "partition": "u32", 
  "records": ["base64_string", "base64_string", ...]
}
```

**Field specifications**:
- **topic**: Topic name (1-255 characters, alphanumeric + underscore/hyphen)
- **partition**: Partition number (0 to partition_count-1)
- **records**: Array of Base64-encoded record payloads

**Record encoding**: Raw binary data encoded as Base64 strings.

**Example**:
```json
{
  "type": "Produce",
  "topic": "user-events", 
  "partition": 0,
  "records": [
    "eyJ1c2VyIjoiYWxpY2UiLCJhY3Rpb24iOiJsb2dpbiJ9",
    "eyJ1c2VyIjoiYm9iIiwiYWN0aW9uIjoic2lnbnVwIn0="
  ]
}
```

**Validation rules**:
- Topic must exist (return Error if not)
- Partition must exist (return Error if not)
- Each record ≤ 10MB after Base64 decoding
- Records array ≤ 1000 elements per batch

### Produce Response

**JSON Schema**:
```json
{
  "type": "ProduceResponse",
  "base_offset": "u64",
  "record_count": "usize"
}
```

**Field specifications**:
- **base_offset**: Logical offset assigned to first record
- **record_count**: Number of successfully appended records

**Offset assignment**: Records get consecutive offsets starting from base_offset.

**Example**:
```json
{
  "type": "ProduceResponse", 
  "base_offset": 42,
  "record_count": 2
}
```

### Fetch Request

**Purpose**: Read records from a partition starting at specified offset.

**JSON Schema**:
```json
{
  "type": "Fetch",
  "group_id": "string",
  "topic": "string", 
  "partition": "u32",
  "offset": "u64",
  "max_bytes": "usize"
}
```

**Field specifications**:
- **group_id**: Consumer group identifier (for metrics, not isolation)
- **topic**: Topic name 
- **partition**: Partition number
- **offset**: Starting offset (inclusive)
- **max_bytes**: Maximum bytes to return (soft limit)

**Fetch semantics**:
- Returns records starting from `offset` (inclusive)
- Stops when `max_bytes` limit approached (may slightly exceed)
- Empty response if `offset` beyond high watermark
- Error response if `offset` before log start offset

**Example**:
```json
{
  "type": "Fetch",
  "group_id": "analytics-consumer",
  "topic": "user-events",
  "partition": 0, 
  "offset": 100,
  "max_bytes": 1048576
}
```

### Fetch Response

**JSON Schema**:
```json
{
  "type": "FetchResponse",
  "records": [
    {
      "offset": "u64",
      "timestamp": "u64", 
      "payload": "base64_string"
    },
    ...
  ],
  "next_offset": "u64"
}
```

**Field specifications**:
- **records**: Array of record objects
- **offset**: Logical offset of this record
- **timestamp**: Microseconds since UNIX epoch (UTC)
- **payload**: Base64-encoded record data
- **next_offset**: Offset to use for next fetch request

**Response guarantees**:
- Records in offset order (monotonic)
- `next_offset` = last record offset + 1
- Empty `records` array if no data available

**Example**:
```json
{
  "type": "FetchResponse",
  "records": [
    {
      "offset": 100,
      "timestamp": 1641024000000000,
      "payload": "eyJ1c2VyIjoiYWxpY2UifQ=="
    },
    {
      "offset": 101, 
      "timestamp": 1641024001000000,
      "payload": "eyJ1c2VyIjoiYm9iIn0="
    }
  ],
  "next_offset": 102
}
```

## Consumer Group Operations

### Offset Commit Request

**Purpose**: Store consumer group's committed offset for a partition.

**JSON Schema**:
```json
{
  "type": "OffsetCommit",
  "group": "string",
  "topic": "string",
  "partition": "u32", 
  "offset": "u64"
}
```

**Commit semantics**:
- Offset represents "next record to process" (exclusive)
- Commits are **per-group, per-topic, per-partition**
- Groups are isolated (different groups can have different offsets)
- Commits are **in-memory only** (lost on broker restart)

**Example**:
```json
{
  "type": "OffsetCommit",
  "group": "analytics-pipeline", 
  "topic": "user-events",
  "partition": 0,
  "offset": 150
}
```

### Offset Commit Response

**JSON Schema**:
```json
{
  "type": "OffsetCommitResponse",
  "success": "bool"
}
```

**Response semantics**:
- `success: true` if commit accepted
- `success: false` if topic/partition doesn't exist

### Offset Fetch Request

**Purpose**: Retrieve consumer group's committed offset for a partition.

**JSON Schema**:
```json
{
  "type": "OffsetFetch", 
  "group": "string",
  "topic": "string",
  "partition": "u32"
}
```

### Offset Fetch Response

**JSON Schema**:
```json
{
  "type": "OffsetFetchResponse",
  "offset": "u64 | null"
}
```

**Response semantics**:
- `offset`: Committed offset if previously committed
- `null`: No commit found for this group/topic/partition

## Admin Control Plane

### Create Topic Request

**Purpose**: Create new topic with specified partition count.

**JSON Schema**:
```json
{
  "type": "CreateTopic",
  "topic": "string",
  "partition_count": "u32"
}
```

**Validation rules**:
- Topic name: 1-255 characters, alphanumeric plus underscore/hyphen
- Partition count: 1-1000 partitions
- Topic must not already exist

### Create Topic Response

**JSON Schema**:
```json
{
  "type": "CreateTopicResponse",
  "success": "bool",
  "partitions_created": "u32"
}
```

### List Topics Request/Response

**Request**:
```json
{
  "type": "ListTopics"
}
```

**Response**:
```json
{
  "type": "ListTopicsResponse",
  "topics": [
    {
      "name": "string",
      "partition_count": "u32"
    },
    ...
  ]
}
```

### Describe Topic Request

**JSON Schema**:
```json
{
  "type": "DescribeTopic",
  "topic": "string"
}
```

### Describe Topic Response

**JSON Schema**:
```json
{
  "type": "DescribeTopicResponse",
  "topic": "string",
  "partitions": [
    {
      "partition": "u32",
      "high_watermark": "u64",
      "log_start_offset": "u64",
      "segment_count": "usize"
    },
    ...
  ]
}
```

**Field specifications**:
- **high_watermark**: Next offset that would be assigned
- **log_start_offset**: Earliest available offset (after retention)
- **segment_count**: Number of log segments (active + sealed)

## Error Handling

### Error Response Format

**JSON Schema**:
```json
{
  "type": "Error",
  "code": "u16",
  "message": "string",
  "details": "object | null"
}
```

### Error Codes

```rust
pub enum ErrorCode {
    // Client errors (4xx)
    TopicNotFound = 404,
    PartitionNotFound = 405,
    OffsetOutOfRange = 416,
    RecordTooLarge = 413,
    InvalidRequest = 400,
    
    // Server errors (5xx)
    InternalServerError = 500,
    StorageError = 503,
    InsufficientSpace = 507,
}
```

**Error response examples**:

```json
{
  "type": "Error",
  "code": 404,
  "message": "Topic 'nonexistent' not found",
  "details": {
    "available_topics": ["logs", "metrics", "events"]
  }
}
```

```json
{
  "type": "Error", 
  "code": 416,
  "message": "Offset 5000 out of range",
  "details": {
    "valid_range": {"start": 0, "end": 1250},
    "high_watermark": 1250
  }
}
```

## Connection Management

### Connection Lifecycle

**Connection states**:
1. **TCP_CONNECTING**: TCP handshake in progress
2. **CONNECTED**: Socket established, ready for frames
3. **REQUEST_ACTIVE**: Processing request, response pending  
4. **ERROR**: Protocol violation, connection terminated
5. **CLOSED**: Connection terminated

**State transitions**:
```
TCP_CONNECTING → CONNECTED → REQUEST_ACTIVE → CONNECTED → CLOSED
                      ↑            ↓
                      └── ERROR ←───┘
```

### Connection Error Handling

**Protocol violations that terminate connection**:
- Frame length exceeds MAX_FRAME_SIZE
- Invalid UTF-8 in payload
- Malformed JSON structure
- TCP socket errors

**Recoverable errors that keep connection alive**:
- Unknown request type (return Error response)
- Invalid request fields (return Error response)
- Business logic errors (return Error response)

**Client responsibilities**:
- Handle connection drops gracefully
- Retry failed requests with exponential backoff
- Implement circuit breaker for repeated failures

### Flow Control

**TCP-level flow control**: Standard TCP windowing provides backpressure.

**Application-level flow control**: None implemented.
- No rate limiting per connection
- No request queuing or buffering
- Synchronous request processing (one at a time)

**Backpressure mechanism**: TCP backpressure naturally limits client send rate when broker is overloaded.

## Performance Characteristics

### Latency Components

**Total request latency** = Network + Parse + Business Logic + Serialize + Network

Typical breakdown:
- **Network latency**: 0.1-50ms (LAN-WAN)
- **JSON parsing**: 10-100μs (small requests)
- **Business logic**: 100μs-10ms (depends on disk I/O)
- **JSON serialization**: 10-100μs (small responses)

### Throughput Limits

**Frame parsing**: ~100K-1M frames/sec per core.

**JSON processing**: ~10K-100K messages/sec per core (depends on payload size).

**Disk I/O bound**: Typically limited by storage, not protocol processing.

### Memory Usage

**Per-connection overhead**: ~8KB (TCP buffers + parsing state).

**Message memory**: Temporary allocation during parse/serialize.

**Base64 expansion**: 33% overhead for binary record encoding.

## Implementation Considerations

### Client Implementation Guidelines

**Frame reading**:
```python
def read_frame(socket):
    # Read length header
    length_bytes = socket.recv(4)
    if len(length_bytes) < 4:
        raise ConnectionError("Incomplete frame header")
    
    length = struct.unpack(">I", length_bytes)[0]  # big-endian u32
    
    if length > MAX_FRAME_SIZE:
        raise ProtocolError(f"Frame too large: {length}")
    
    # Read payload
    payload = socket.recv(length)
    if len(payload) < length:
        raise ConnectionError("Incomplete frame payload")
    
    return json.loads(payload.decode('utf-8'))
```

**Error handling patterns**:
```python
def handle_response(response):
    if response.get("type") == "Error":
        error_code = response["code"]
        if 400 <= error_code < 500:
            # Client error - fix request and retry
            raise ClientError(response["message"])
        else:
            # Server error - retry with backoff
            raise ServerError(response["message"])
    
    return response
```

### Wire Format Evolution

**Backward compatible changes**:
- Add optional fields to existing messages
- Add new request/response types
- Add new error codes

**Breaking changes**:
- Remove or rename existing fields
- Change field types or semantics
- Modify frame format

**Version negotiation**: Not implemented (single protocol version only).

### Security Considerations

**No authentication**: Protocol assumes trusted network environment.

**No encryption**: All data transmitted in plaintext.

**DoS protection**: Frame size limits prevent memory exhaustion attacks.

**Input validation**: JSON schema validation prevents injection attacks.

**Production deployment**: Use TLS tunnel (stunnel, nginx) for encryption.

## Protocol Testing

### Unit Test Patterns

```rust
#[test]
fn test_produce_request_serialization() {
    let request = ProduceRequest {
        topic: "test".to_string(),
        partition: 0,
        records: vec![b"hello".to_vec(), b"world".to_vec()],
    };
    
    let json = serde_json::to_string(&request).unwrap();
    let parsed: ProduceRequest = serde_json::from_str(&json).unwrap();
    
    assert_eq!(request, parsed);
}

#[test]
fn test_frame_parsing_limits() {
    // Test maximum frame size
    let large_payload = vec![0u8; MAX_FRAME_SIZE];
    let frame = create_frame(&large_payload);
    assert!(parse_frame(&frame).is_ok());
    
    // Test oversized frame
    let oversized_payload = vec![0u8; MAX_FRAME_SIZE + 1];
    let frame = create_frame(&oversized_payload);
    assert!(parse_frame(&frame).is_err());
}
```

### Integration Test Scenarios

1. **Happy path**: Successful produce→fetch cycle
2. **Error cases**: Invalid topics, out-of-range offsets
3. **Connection handling**: Reconnect after disconnect  
4. **Large messages**: Maximum frame size handling
5. **Unicode handling**: UTF-8 encoding edge cases

## Summary

RustLog's protocol provides:

1. **Simple implementation**: Binary framing + JSON payloads
2. **Clear semantics**: Explicit request/response types
3. **Debuggability**: Human-readable network traces
4. **Extensibility**: Easy to add new message types

The protocol optimizes for **simplicity and debuggability** over **maximum performance**, making it suitable for educational use and moderate-scale deployments.

**Key insight**: Hybrid binary/JSON protocols offer a good balance between efficiency and maintainability for application-specific message brokers.
