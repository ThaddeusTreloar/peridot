use rdkafka::message::{BorrowedMessage, Message as KafkaMessage, BorrowedHeaders, Headers as KafkaHeaders};

use crate::serde_ext::PDeserialize;

pub mod message;
pub mod stream;