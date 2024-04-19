use std::fmt::Display;

use rdkafka::message::{
    BorrowedHeaders, BorrowedMessage, Header, Headers as KafkaHeaders, Message as KafkaMessage,
    OwnedHeaders, OwnedMessage,
};

use crate::engine::wrapper::serde::PDeserialize;

use super::{MessageHeaders, PeridotTimestamp};

#[derive(Debug, serde::Serialize, Clone)]
pub struct Message<K, V> {
    pub(crate) topic: String,
    pub(crate) timestamp: PeridotTimestamp,
    pub(crate) partition: i32,
    pub(crate) offset: i64,
    pub(crate) headers: MessageHeaders,
    pub(crate) key: K,   // TODO: Option?
    pub(crate) value: V, // TODO: Option?
}

impl<K, V> Display for Message<K, V>
where
    K: Display,
    V: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Message {{ topic: {}, timestamp: {:?}, partition: {}, offset: {}, headers: {:?}, key: {}, value: {} }}",
            self.topic, self.timestamp, self.partition, self.offset, self.headers, self.key, self.value)
    }
}

impl<K, V> Message<K, V> {
    pub fn key(&self) -> &K {
        &self.key
    }

    pub fn value(&self) -> &V {
        &self.value
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn timestamp<'a>(&'a self) -> &'a PeridotTimestamp {
        &self.timestamp
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn headers(&self) -> &MessageHeaders {
        &self.headers
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum TryFromKafkaMessageError {
    #[error("Deserialization error: {0}")]
    DeserializationError(String),
}

pub trait TryFromOwnedMessage<'a, KS, VS> {
    fn try_from_owned_message(msg: OwnedMessage) -> Result<Self, TryFromKafkaMessageError>
    where
        Self: Sized;
}

impl<'a, KS, VS> TryFromOwnedMessage<'a, KS, VS> for Message<KS::Output, VS::Output>
where
    KS: PDeserialize,
    VS: PDeserialize,
{
    fn try_from_owned_message(msg: OwnedMessage) -> Result<Self, TryFromKafkaMessageError> {
        let raw_key = msg.key().unwrap();

        let key = KS::deserialize(raw_key)
            .map_err(|e| TryFromKafkaMessageError::DeserializationError(e.to_string()))?;

        let raw_value = msg.payload().unwrap();

        let value = VS::deserialize(raw_value)
            .map_err(|e| TryFromKafkaMessageError::DeserializationError(e.to_string()))?;

        let headers = match msg.headers() {
            Some(h) => MessageHeaders::from(h),
            None => MessageHeaders::default(),
        };

        Ok(Self {
            topic: msg.topic().to_string(),
            timestamp: msg.timestamp().into(),
            partition: msg.partition(),
            offset: msg.offset(),
            headers,
            key,
            value,
        })
    }
}

pub trait TryFromBorrowedMessage<'a, KS, VS> {
    fn try_from_borrowed_message(
        msg: BorrowedMessage<'a>,
    ) -> Result<Self, TryFromKafkaMessageError>
    where
        Self: Sized;
}

impl<'a, KS, VS> TryFromBorrowedMessage<'a, KS, VS> for Message<KS::Output, VS::Output>
where
    KS: PDeserialize,
    VS: PDeserialize,
{
    /*  TODO: Make the deserialisation, and cloning of each field lazy.
     *  Currently all fields are cloned into this object, even if they are not used.
     *  We could implement this so that Message contains a field &BorrowedMessage<'a>,
     *  and then we an extractor references a field, a reference is returned, then
     *  when the field is modified, it can be cloned.
     */
    fn try_from_borrowed_message(
        msg: BorrowedMessage<'a>,
    ) -> Result<Self, TryFromKafkaMessageError> {
        let raw_key = msg.key().unwrap();

        let key = KS::deserialize(raw_key)
            .map_err(|e| TryFromKafkaMessageError::DeserializationError(e.to_string()))?;

        let raw_value = msg.payload().unwrap();

        let value = VS::deserialize(raw_value)
            .map_err(|e| TryFromKafkaMessageError::DeserializationError(e.to_string()))?;

        let headers = match msg.headers() {
            Some(h) => MessageHeaders::from(h),
            None => MessageHeaders::default(),
        };

        Ok(Self {
            topic: msg.topic().to_string(),
            timestamp: msg.timestamp().into(),
            partition: msg.partition(),
            offset: msg.offset(),
            headers,
            key,
            value,
        })
    }
}