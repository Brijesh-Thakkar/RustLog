/// Broker-level errors
/// These represent failures in the broker's core operations.
#[derive(thiserror::Error, Debug)]
pub enum BrokerError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid protocol: {0}")]
    InvalidProtocol(String),

    #[error("frame too large: {0} bytes (max: {1})")]
    FrameTooLarge(usize, usize),

    #[error("connection closed")]
    ConnectionClosed,

    #[error("unknown request type: {0}")]
    UnknownRequestType(u8),

    #[error("unknown response type: {0}")]
    UnknownResponseType(u8),

    #[error("decode error: {0}")]
    DecodeError(String),

    #[error("encode error: {0}")]
    EncodeError(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("record too large: {0} bytes")]
    RecordTooLarge(usize),

    #[error("invalid offset: {0}")]
    InvalidOffset(String),

    #[error("offset out of range: requested {requested}, partition base {base}")]
    OffsetOutOfRange { requested: u64, base: u64 },

    #[error("topic already exists: {0}")]
    TopicAlreadyExists(String),

    #[error("topic not found: {0}")]
    TopicNotFound(String),

    #[error("invalid partition count: {0}")]
    InvalidPartitionCount(u32),

    #[error("lock poisoned")]
    LockPoisoned,

    #[error("partition not found: topic {topic}, partition {partition}")]
    PartitionNotFound { topic: String, partition: u32 },
}
