# Protocol Specification

This document describes RustLog's custom binary TCP protocol for client-broker communication.

## Overview

RustLog uses a **length-prefixed binary protocol** over TCP. It is **not compatible with Apache Kafka** but is inspired by similar design principles:
- Binary encoding (not text-based like HTTP)
- Request-response model (client sends request, broker sends response)
- Stateless connections (no session tracking)
- Pull-based fetching (client requests records)

**Design goals:**
- **Simple to implement:** Minimal overhead, easy to debug
- **Efficient:** Binary encoding, no JSON/XML parsing
- **Extensible:** Request/response types can be added
- **Type-safe:** Rust enums prevent invalid messages

**Non-goals:**
- Kafka wire format compatibility
- Authentication (no TLS, SASL)
- Compression
- Transactions

## Wire Format

### Frame Structure

All messages use **length-prefixed framing** to delimit requests/responses:

```
┌────────────────────────────────────────┐
│  Frame Header (4 bytes)                │
│  ┌──────────────────────────────────┐  │
│  │  Length: u32 (big-endian)        │  │
│  │  = size of payload in bytes      │  │
│  └──────────────────────────────────┘  │
│                                        │
│  Payload (variable length)             │
│  ┌──────────────────────────────────┐  │
│  │  Request or Response message     │  │
│  │  (encoded as JSON)               │  │
│  └──────────────────────────────────┘  │
└────────────────────────────────────────┘
```

**Encoding:**
- **Length:** `u32` big-endian (network byte order)
- **Payload:** JSON-encoded request or response

**Maximum frame size:** 10 MB (10,485,760 bytes)

**Why JSON inside binary framing?**
- Simple to debug (human-readable)
- Easy schema evolution (add fields without breaking)
- Trade-off: slightly slower than pure binary (acceptable for MVP)

**Production consideration:** Use MessagePack, Protocol Buffers, or Cap'n Proto for performance-critical systems.

### Message Flow

**Client connects → sends requests → receives responses → closes connection:**

```
Client                          Broker
  │                               │
  ├──── TCP connect ─────────────>│
  │                               │
  ├──── [Length][Request JSON]───>│
  │                               │
  │<──── [Length][Response JSON]──┤
  │                               │
  ├──── [Length][Request JSON]───>│
  │                               │
  │<──── [Length][Response JSON]──┤
  │                               │
  ├──── TCP close ───────────────>│
  │                               │
```

**Connection model:**
- **Stateless:** Each request is independent
- **No pipelining:** Request-response pairs are sequential
- **No keep-alive headers:** Connection can be reused or closed after each request

## Request Types

### 1. Produce Request

**Purpose:** Append records to a partition.

**Schema:**
```rust
pub struct ProduceRequest {
    pub topic: String,
    pub partition: u32,
    pub records: Vec<Vec<u8>>,  // Raw record payloads
}
```

**JSON example:**
```json
{
  "Produce": {
    "topic": "orders",
    "partition": 0,
    "records": [
      [98, 121, 116, 101, 115],  // "bytes" as byte array
      [109, 111, 114, 101]       // "more"
    ]
  }
}
```

**Field semantics:**
- `topic`: Topic name (must exist)
- `partition`: Partition ID within topic
- `records`: Array of raw byte payloads (no schema enforcement)

**Validation:**
- Topic and partition must exist (broker returns error if not)
- Records array can be empty (no-op)
- Each record payload must be ≤ 10 MB

**Broker behavior:**
1. Lookup partition by topic + partition ID
2. Append each record sequentially to active segment
3. Assign monotonically increasing offsets
4. Return assigned offsets in response

### 2. Fetch Request

**Purpose:** Read records from a partition, optionally tracking consumer group offsets.

**Schema:**
```rust
pub struct FetchRequest {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub max_bytes: usize,
    pub group_id: Option<String>,  // Consumer group ID (optional)
}
```

**JSON example:**
```json
{
  "Fetch": {
    "topic": "orders",
    "partition": 0,
    "offset": 1000,
    "max_bytes": 1048576,
    "group_id": "order-processor"
  }
}
```

**Field semantics:**
- `topic`: Topic name
- `partition`: Partition ID
- `offset`: Starting offset to read from
- `max_bytes`: Maximum bytes to return (soft limit)
- `group_id`: Optional consumer group ID for offset tracking

**Consumer group behavior:**
- If `group_id` is provided:
  - Broker checks if committed offset exists for this group
  - If committed offset > requested offset: use committed offset instead
  - If no committed offset: use requested offset
- If `group_id` is `None`: use requested offset directly (no tracking)

**max_bytes handling:**
- Broker returns **at least one record** if available (even if > max_bytes)
- Prevents clients from getting stuck on large records
- Returns up to max_bytes total payload size

**Broker behavior:**
1. Lookup partition
2. If group_id provided: check committed offset, use higher value
3. Read records from segment starting at offset
4. Return records + next_offset

**Edge cases:**
- Offset beyond log end: return empty records, next_offset = log end
- Partition doesn't exist: return error
- Offset too old (before first segment): return error

### 3. OffsetCommit Request

**Purpose:** Commit the current offset for a consumer group.

**Schema:**
```rust
pub struct OffsetCommitRequest {
    pub topic: String,
    pub partition: u32,
    pub group_id: String,
    pub offset: u64,
}
```

**JSON example:**
```json
{
  "OffsetCommit": {
    "topic": "orders",
    "partition": 0,
    "group_id": "order-processor",
    "offset": 1500
  }
}
```

**Field semantics:**
- `topic`: Topic name
- `partition`: Partition ID
- `group_id`: Consumer group ID
- `offset`: Offset to commit

**Broker behavior:**
1. Store offset in OffsetManager (in-memory hash map)
2. Return success or error

**Important:** Offsets are **not persisted to disk** (in-memory only). Lost on broker restart.

**Commit semantics:**
- **At-least-once:** Client should commit AFTER processing records
- **Manual commits:** No auto-commit (client controls when to commit)
- **Overwrite:** New commit overwrites previous commit for same group+topic+partition

### 4. OffsetFetch Request

**Purpose:** Retrieve the committed offset for a consumer group.

**Schema:**
```rust
pub struct OffsetFetchRequest {
    pub topic: String,
    pub partition: u32,
    pub group_id: String,
}
```

**JSON example:**
```json
{
  "OffsetFetch": {
    "topic": "orders",
    "partition": 0,
    "group_id": "order-processor"
  }
}
```

**Field semantics:**
- `topic`: Topic name
- `partition`: Partition ID
- `group_id`: Consumer group ID

**Broker behavior:**
1. Lookup committed offset in OffsetManager
2. Return offset if exists, or `None`

**Use case:** Client wants to resume from last committed offset after restart.

## Response Types

### 1. Produce Response

**Schema:**
```rust
pub struct ProduceResponse {
    pub offsets: Vec<u64>,
}
```

**JSON example:**
```json
{
  "Produce": {
    "offsets": [1000, 1001, 1002]
  }
}
```

**Field semantics:**
- `offsets`: Array of assigned offsets (one per record in request)

**Ordering:** Offsets correspond to records in request by index.

### 2. Fetch Response

**Schema:**
```rust
pub struct FetchResponse {
    pub records: Vec<Record>,
    pub next_offset: u64,
}

pub struct Record {
    pub offset: u64,
    pub payload: Vec<u8>,
}
```

**JSON example:**
```json
{
  "Fetch": {
    "records": [
      { "offset": 1000, "payload": [98, 121, 116, 101, 115] },
      { "offset": 1001, "payload": [109, 111, 114, 101] }
    ],
    "next_offset": 1002
  }
}
```

**Field semantics:**
- `records`: Array of records read from partition
- `next_offset`: Offset to use for next fetch (exclusive)

**next_offset behavior:**
- If records returned: `next_offset = last_record.offset + 1`
- If no records (at log end): `next_offset = current log end offset`

**Empty response:** `records` array is empty if no data at requested offset.

### 3. OffsetCommit Response

**Schema:**
```rust
pub struct OffsetCommitResponse {
    pub success: bool,
}
```

**JSON example:**
```json
{
  "OffsetCommit": {
    "success": true
  }
}
```

**Field semantics:**
- `success`: `true` if commit succeeded, `false` if error

**Failure reasons:**
- Topic or partition doesn't exist
- Internal broker error

### 4. OffsetFetch Response

**Schema:**
```rust
pub struct OffsetFetchResponse {
    pub offset: Option<u64>,
}
```

**JSON example (offset exists):**
```json
{
  "OffsetFetch": {
    "offset": 1500
  }
}
```

**JSON example (no committed offset):**
```json
{
  "OffsetFetch": {
    "offset": null
  }
}
```

**Field semantics:**
- `offset`: Committed offset if exists, `None` if no commit for this group

**None case:** Consumer group has never committed an offset for this topic+partition.

### 5. Error Response

**Schema:**
```rust
pub struct ErrorResponse {
    pub message: String,
}
```

**JSON example:**
```json
{
  "Error": {
    "message": "partition not found: topic=orders, partition=99"
  }
}
```

**Field semantics:**
- `message`: Human-readable error description

**Common errors:**
- "partition not found" (topic or partition doesn't exist)
- "offset out of range" (requested offset is too old or too new)
- "max frame size exceeded" (request > 10 MB)
- "failed to parse request" (invalid JSON or schema)

## Request-Response Pairing

All request types have corresponding response types:

| Request | Response |
|---------|----------|
| `Produce` | `ProduceResponse` or `Error` |
| `Fetch` | `FetchResponse` or `Error` |
| `OffsetCommit` | `OffsetCommitResponse` or `Error` |
| `OffsetFetch` | `OffsetFetchResponse` or `Error` |

**Error handling:**
- Broker returns `Error` response if request cannot be processed
- Client should handle both success and error variants

## Protocol Flow Examples

### Example 1: Produce-Append-Produce

```
Client -> Broker:
{
  "Produce": {
    "topic": "orders",
    "partition": 0,
    "records": [[10, 20], [30, 40]]
  }
}

Broker -> Client:
{
  "Produce": {
    "offsets": [1000, 1001]
  }
}

Client -> Broker:
{
  "Produce": {
    "topic": "orders",
    "partition": 0,
    "records": [[50, 60]]
  }
}

Broker -> Client:
{
  "Produce": {
    "offsets": [1002]
  }
}
```

### Example 2: Fetch with Consumer Group

**First fetch (no committed offset):**
```
Client -> Broker:
{
  "Fetch": {
    "topic": "orders",
    "partition": 0,
    "offset": 0,
    "max_bytes": 1048576,
    "group_id": "processor"
  }
}

Broker -> Client:
{
  "Fetch": {
    "records": [
      { "offset": 0, "payload": [10, 20] },
      { "offset": 1, "payload": [30, 40] }
    ],
    "next_offset": 2
  }
}
```

**Client processes records, commits offset:**
```
Client -> Broker:
{
  "OffsetCommit": {
    "topic": "orders",
    "partition": 0,
    "group_id": "processor",
    "offset": 2
  }
}

Broker -> Client:
{
  "OffsetCommit": {
    "success": true
  }
}
```

**Client restarts, fetches from offset 0 (but broker uses committed offset 2):**
```
Client -> Broker:
{
  "Fetch": {
    "topic": "orders",
    "partition": 0,
    "offset": 0,        // Client requests 0
    "max_bytes": 1048576,
    "group_id": "processor"
  }
}

Broker -> Client:
{
  "Fetch": {
    "records": [
      { "offset": 2, "payload": [50, 60] }  // Broker returns from offset 2
    ],
    "next_offset": 3
  }
}
```

**Broker behavior:** Used committed offset (2) instead of requested offset (0) because committed > requested.

### Example 3: Fetch Without Consumer Group

```
Client -> Broker:
{
  "Fetch": {
    "topic": "orders",
    "partition": 0,
    "offset": 100,
    "max_bytes": 1048576,
    "group_id": null    // No consumer group
  }
}

Broker -> Client:
{
  "Fetch": {
    "records": [
      { "offset": 100, "payload": [10, 20] },
      { "offset": 101, "payload": [30, 40] }
    ],
    "next_offset": 102
  }
}
```

**No offset tracking:** Broker uses requested offset directly.

### Example 4: Error Response

```
Client -> Broker:
{
  "Fetch": {
    "topic": "nonexistent",
    "partition": 0,
    "offset": 0,
    "max_bytes": 1048576,
    "group_id": null
  }
}

Broker -> Client:
{
  "Error": {
    "message": "partition not found: topic=nonexistent, partition=0"
  }
}
```

## Implementation Details

### Framing Logic

**Sender (client or broker):**
```rust
async fn send_message(stream: &mut TcpStream, msg: &Request) -> Result<()> {
    // Serialize request to JSON
    let json = serde_json::to_string(msg)?;
    let payload = json.as_bytes();
    
    // Check frame size
    if payload.len() > MAX_FRAME_SIZE {
        return Err("frame too large");
    }
    
    // Write length prefix
    let len_bytes = (payload.len() as u32).to_be_bytes();
    stream.write_all(&len_bytes).await?;
    
    // Write payload
    stream.write_all(payload).await?;
    stream.flush().await?;
    
    Ok(())
}
```

**Receiver (broker or client):**
```rust
async fn recv_message(stream: &mut TcpStream) -> Result<Response> {
    // Read length prefix
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await?;
    let length = u32::from_be_bytes(len_bytes) as usize;
    
    // Validate frame size
    if length > MAX_FRAME_SIZE {
        return Err("frame too large");
    }
    
    // Read payload
    let mut payload = vec![0u8; length];
    stream.read_exact(&mut payload).await?;
    
    // Deserialize JSON
    let response: Response = serde_json::from_slice(&payload)?;
    
    Ok(response)
}
```

**Key properties:**
- `read_exact()` blocks until all bytes received (no partial reads)
- `to_be_bytes()` ensures network byte order (big-endian)
- `MAX_FRAME_SIZE = 10 MB` prevents memory exhaustion attacks

### Request Routing

**Broker dispatch logic:**
```rust
pub async fn handle(&mut self) -> anyhow::Result<()> {
    let request = self.recv_request().await?;
    
    let response = match request {
        Request::Produce(req) => {
            let offsets = self.handle_produce(req)?;
            Response::Produce(ProduceResponse { offsets })
        }
        Request::Fetch(req) => {
            let result = self.handle_fetch(req)?;
            Response::Fetch(FetchResponse {
                records: result.records,
                next_offset: result.next_offset,
            })
        }
        Request::OffsetCommit(req) => {
            let success = self.handle_offset_commit(req)?;
            Response::OffsetCommit(OffsetCommitResponse { success })
        }
        Request::OffsetFetch(req) => {
            let offset = self.handle_offset_fetch(req)?;
            Response::OffsetFetch(OffsetFetchResponse { offset })
        }
    };
    
    self.send_response(&response).await?;
    Ok(())
}
```

**Error conversion:**
```rust
let response = match handle_request() {
    Ok(resp) => resp,
    Err(e) => Response::Error(ErrorResponse {
        message: e.to_string(),
    }),
};
```

## Performance Considerations

### JSON vs Binary

**JSON overhead:**
- Parsing cost: ~1-5 µs per message (negligible for log operations)
- Size overhead: ~20-30% larger than binary

**When JSON is acceptable:**
- Request/response rate < 100K/sec
- Network is not bottleneck (local or LAN)
- Debugging ease outweighs performance cost

**When to use binary:**
- High message rate (> 100K/sec)
- WAN with bandwidth limits
- Sub-millisecond latency requirements

### Framing Overhead

**Length prefix cost:**
- 4 bytes per frame (negligible)
- One extra read syscall per message (amortized by TCP buffering)

**Alternative:** HTTP/2 framing (more complex, not needed for MVP)

### max_bytes and Bandwidth Control

**Client controls bandwidth:**
- Set `max_bytes` to limit memory usage
- Typical values: 64 KB (low latency) to 10 MB (high throughput)

**Broker respects soft limit:**
- Returns at least one record (prevents starvation)
- May exceed max_bytes if first record is large

## Protocol Evolution

### Adding New Request Types

**Steps:**
1. Add enum variant to `Request` and `Response`
2. Implement handler in broker
3. Update client library

**Backward compatibility:**
- Old clients send unknown request type → broker returns error
- Old brokers receive unknown request type → fail to deserialize, close connection

**Versioning (future):** Add protocol version field in frame header.

### Adding Fields to Existing Types

**JSON allows schema evolution:**
- New optional fields: old clients ignore unknown fields
- New required fields: breaks old clients (major version bump)

**Best practice:** Make all new fields optional with defaults.

## Security Considerations

**RustLog MVP has NO security features:**
- ❌ No TLS/SSL (plaintext TCP)
- ❌ No authentication (anyone can connect)
- ❌ No authorization (no ACLs)
- ❌ No encryption (payloads visible on network)

**Production requirements:**
- Add TLS for transport encryption
- Add SASL for authentication (username/password, tokens)
- Add ACLs for topic-level authorization
- Rate limiting to prevent DoS

## Limitations

1. **No Kafka compatibility:** Cannot use Kafka clients
2. **No compression:** Records transmitted uncompressed
3. **No batching optimization:** Each record sent individually
4. **No request pipelining:** Synchronous request-response
5. **No multiplexing:** One request at a time per connection

**These are intentional for MVP simplicity.**

## Summary

RustLog's protocol provides:
- **Length-prefixed framing** for reliable message delimiting
- **JSON encoding** for human-readable debugging
- **Four request types** covering core log operations
- **Stateless connections** for simple implementation
- **Consumer group support** via optional group_id field

The protocol is designed for clarity and ease of implementation, not maximum performance. It is suitable for educational use and moderate-scale systems where sub-millisecond latency is not required.
