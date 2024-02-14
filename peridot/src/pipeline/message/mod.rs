use rdkafka::message::{BorrowedMessage, Message as KafkaMessage, BorrowedHeaders, Headers as KafkaHeaders};

use crate::serde_ext::PDeserialize;


#[derive(Debug, Clone, Default)]
pub struct MessageHeaders {
    headers: Vec<(String, Vec<u8>)>
}

impl MessageHeaders {
    fn iter(&self) -> impl Iterator<Item = &(String, Vec<u8>)> {
        self.headers.iter()
    }

    fn list(&self, key: &str) -> Vec<&Vec<u8>> {
        self.headers.iter().filter_map(|(k, v)| {
            if k == key {
                Some(v)
            } else {
                None
            }
        }).collect()
    }
}

impl From<&BorrowedHeaders> for MessageHeaders {
    fn from(headers: &BorrowedHeaders) -> Self {
        let headers: Vec<_> = headers.iter().map(
            |h| (String::from(h.key), h.value.unwrap_or_default().to_vec())
        ).collect::<Vec<(String, Vec<u8>)>>();

        Self {
            headers: headers.iter().map(|(k, v)| (k.to_string(), v.to_vec())).collect()
        }
    }
}

pub struct Message<K, V> {
    topic: String,
    timestamp: rdkafka::message::Timestamp,
    partition: i32,
    offset: i64,
    headers: MessageHeaders,
    key: K, // TODO: Option?
    value: V // TODO: Option?
}

impl <K, V> Message<K, V> {
    fn key(&self) -> &K {
        &self.key
    }

    fn value(&self) -> &V {
        &self.value
    }

    fn topic(&self) -> &str {
        &self.topic
    }

    fn timestamp(&self) -> rdkafka::message::Timestamp {
        self.timestamp
    }

    fn partition(&self) -> i32 {
        self.partition
    }

    fn offset(&self) -> i64 {
        self.offset
    }

    fn headers(&self) -> &MessageHeaders {
        &self.headers
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum TryFromBorrowedMessageError {
    #[error("Deserialization error: {0}")]
    DeserializationError(String)
}

pub trait TryFromBorrowedMessage<'a, KS, VS> {
    fn try_from_borrowed_message(msg: BorrowedMessage<'a>) -> 
        Result<Self, TryFromBorrowedMessageError> where Self: Sized;
}

impl <'a, KS, VS> TryFromBorrowedMessage<'a, KS, VS> for Message<KS::Output, VS::Output> 
where KS: PDeserialize,
      VS: PDeserialize
{
    /*  TODO: Make the deserialisation, and cloning of each field lazy.
     *  Currently all fields are cloned into this object, even if they are not used.
     *  We could implement this so that Message contains a field &BorrowedMessage<'a>, 
     *  and then we an extractor references a field, a reference is returned, then
     *  when the field is modified, it can be cloned.
    */
    fn try_from_borrowed_message(msg: BorrowedMessage<'a>) -> Result<Self, TryFromBorrowedMessageError> {
        let raw_key = msg.key().unwrap();

        let key = KS::deserialize(raw_key).map_err(|e| TryFromBorrowedMessageError::DeserializationError(e.to_string()))?;

        let raw_value = msg.payload().unwrap();

        let value = VS::deserialize(raw_value).map_err(|e| TryFromBorrowedMessageError::DeserializationError(e.to_string()))?;

        let headers = match msg.headers() {
            Some(h) => MessageHeaders::from(h),
            None => MessageHeaders::default()
        };

        Ok(Self {
            topic: msg.topic().to_string(),
            timestamp: msg.timestamp(),
            partition: msg.partition(),
            offset: msg.offset(),
            headers,
            key,
            value
        })
    }
}

pub trait FromMessage<K, V> {
    fn from_message(msg: &Message<K, V>) -> Self;
}

pub trait PatchMessage<K, V, RK, RV> {
    fn patch(self, msg: Message<K, V>) -> Message<RK, RV>;
}


pub struct Value<K> {
    value: K
}

impl <K, V> FromMessage<K, V> for Value<V> 
where V: Clone
{
    fn from_message(msg: &Message<K, V>) -> Self {
        Self {
            value: msg.value().clone()
        }
    }
}

impl <K, V, VR> PatchMessage<K, V, K, VR> for Value<VR> 
where K: Clone,
    V: Clone
{
    fn patch(self, Message { topic, timestamp, partition, offset, headers, key, value }: Message<K, V>) -> Message<K, VR> {
        let _ = value;

        Message {
            topic,
            timestamp,
            partition,
            offset,
            headers,
            key,
            value: self.value
        }
    }
}

pub struct KeyValue<K, V> {
    key: K,
    value: V
}

impl <K, V> FromMessage<K, V> for KeyValue<K, V> 
where K: Clone,
      V: Clone
{
    fn from_message(msg: &Message<K, V>) -> Self {
        Self {
            key: msg.key().clone(),
            value: msg.value().clone()
        }
    }
}

impl <K, V, KR, VR> PatchMessage<K, V, KR, VR> for KeyValue<KR, VR> 
{
    fn patch(self, Message { topic, timestamp, partition, offset, headers, key, value }: Message<K, V>) -> Message<KR, VR> {
        let _ = key;
        let _ = value;

        Message {
            topic,
            timestamp,
            partition,
            offset,
            headers,
            key: self.key,
            value: self.value
        }
    }
}

pub struct Headers{
    headers: MessageHeaders
}

impl <K, V> FromMessage<K, V> for Headers {
    fn from_message(msg: &Message<K, V>) -> Self {
        Self {
            headers: msg.headers().clone()
        }
    }
}

impl <K, V> PatchMessage<K, V, K, V> for Headers {
    fn patch(self, Message { topic, timestamp, partition, offset, headers, key, value }: Message<K, V>) -> Message<K, V> {
        let _ = headers;

        Message {
            topic,
            timestamp,
            partition,
            offset,
            headers: self.headers,
            key,
            value
        }
    }
}