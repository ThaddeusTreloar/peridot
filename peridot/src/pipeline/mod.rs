use rdkafka::message::{BorrowedMessage, Message as KafkaMessage, BorrowedHeaders, Headers as KafkaHeaders};

pub mod message;
pub mod serde_ext;
pub mod pipeline;