/*
 * Copyright 2024 Thaddeus Treloar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

use std::fmt::Display;

use rdkafka::message::{
    BorrowedHeaders, BorrowedMessage, Header, Headers as KafkaHeaders, Message as KafkaMessage,
    OwnedHeaders, OwnedMessage,
};
use tracing::debug;

use crate::engine::wrapper::serde::PeridotDeserializer;

use super::{MessageHeaders, PartialMessageOwned, PeridotTimestamp};

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

impl Default for Message<(), ()> {
    fn default() -> Self {
        Self {
            topic: Default::default(),
            timestamp: Default::default(), // TODO: Maybe set to ingestion time to allow synchronisation.
            partition: Default::default(),
            offset: -1,
            headers: Default::default(),
            key: Default::default(),
            value: Default::default(),
        }
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

    pub fn timestamp(&self) -> &PeridotTimestamp {
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

    pub(crate) fn headers_mut(&mut self) -> &mut MessageHeaders {
        &mut self.headers
    }

    pub(crate) fn override_offset(&mut self, offset: i64) {
        self.offset = offset;
    }
}

impl From<Message<(), ()>> for PartialMessageOwned<(), ()> {
    fn from(value: Message<(), ()>) -> Self {
        PartialMessageOwned {
            topic: Some(value.topic),
            timestamp: Some(value.timestamp), // TODO: Maybe set to ingestion time to allow synchronisation.
            partition: Some(value.partition),
            offset: Some(value.offset),
            headers: Some(value.headers),
            key: Some(()),
            value: Some(()),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum TryFromKafkaMessageError {
    #[error("KeyDeserialization error: {0}")]
    KeyDeserializationError(String),
    #[error("ValueDeserialization error: {0}")]
    ValueDeserializationError(String),
}

pub trait TryFromOwnedMessage<'a, KS, VS> {
    fn try_from_owned_message(msg: OwnedMessage) -> Result<Self, TryFromKafkaMessageError>
    where
        Self: Sized;
}

impl<'a, KS, VS> TryFromOwnedMessage<'a, KS, VS> for Message<KS::Output, VS::Output>
where
    KS: PeridotDeserializer,
    VS: PeridotDeserializer,
{
    fn try_from_owned_message(msg: OwnedMessage) -> Result<Self, TryFromKafkaMessageError> {
        let raw_key = msg.key().unwrap();

        tracing::trace!(
            "Raw input key bytes for topic: {}, partition: {}, offset: {}, key: {:?}",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            raw_key
        );

        let key = KS::deserialize(raw_key)
            .map_err(|e| TryFromKafkaMessageError::KeyDeserializationError(e.to_string()))?;

        let raw_value = msg.payload().unwrap();

        tracing::trace!(
            "Raw input value bytes for topic: {}, partition: {}, offset: {}, value: {:?}",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            raw_value
        );

        let value = VS::deserialize(raw_value)
            .map_err(|e| TryFromKafkaMessageError::ValueDeserializationError(e.to_string()))?;

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
    KS: PeridotDeserializer,
    VS: PeridotDeserializer,
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
            .map_err(|e| TryFromKafkaMessageError::KeyDeserializationError(e.to_string()))?;

        let raw_value = msg.payload().unwrap();

        let value = VS::deserialize(raw_value)
            .map_err(|e| TryFromKafkaMessageError::ValueDeserializationError(e.to_string()))?;

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
