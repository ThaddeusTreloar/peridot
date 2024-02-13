use rdkafka::message::{BorrowedMessage, Message, Headers, OwnedMessage};
use serde::de::DeserializeOwned;
use tracing::info;

use crate::{serde_ext::{PDeserialize, PSerialize}, stream::types::{IntoRecordParts, RecordParts, TryIntoRecordParts}};


pub trait TransferMessageContext<K, V>: MessageContext + MessageHeaders + Sized
{
    fn map<R>(self, key: K, value: V) -> R where R: RecieveMessageContext<K, V> {
        let ctx = GenericMessageContext::new(self.consumer_topic(), self.consumer_partition(), self.consumer_offset());
        R::recieve(ctx, self.headers(), key, value)
    }
}

pub trait TransferMessageContextAndKey<K, V>: MessageContext + MessageHeaders + MessageKey<K> + Sized
where K: Clone
{
    fn map_value<R>(self, value: V) -> R where R: RecieveMessageContext<K, V> {
        let ctx = GenericMessageContext::new(self.consumer_topic(), self.consumer_partition(), self.consumer_offset());

        R::recieve(ctx, self.headers(), self.key().clone(), value)
    }
}

impl <T, K, V> TransferMessageContext<K, V> for T where T: MessageContext + MessageHeaders {}

impl <T, K, V> TransferMessageContextAndKey<K, V> for T 
    where T: MessageContext + MessageHeaders + MessageKey<K> + Sized, K: Clone {}

pub trait RecieveMessageContext<K, V> 
//where C: MessageContext, H: MessageHeaders
{
    fn recieve(ctx: GenericMessageContext, headers: Vec<(String, Vec<u8>)>, key: K, value: V) -> Self;
}

pub trait RecieveMessageContent {
    fn recieve_content<H, K, V>(headers: H, key: K, value: V) -> Self;
    fn recieve_value<H, V>(headers: H, value: V) -> Self;
}

pub struct GenericMessageContext {
    consumer_topic: String,
    consumer_partition: i32,
    consumer_offset: i64,
}

impl GenericMessageContext {
    pub fn new(consumer_topic: String, consumer_partition: i32, consumer_offset: i64) -> Self {
        Self {
            consumer_topic,
            consumer_partition,
            consumer_offset,
        }
    }
}

impl MessageContext for GenericMessageContext {
    fn consumer_topic(&self) -> String {
        self.consumer_topic.clone()
    }

    fn consumer_partition(&self) -> i32 {
        self.consumer_partition
    }

    fn consumer_offset(&self) -> i64 {
        self.consumer_offset
    }
}

impl MessageContext for (String, i32, i64) {
    fn consumer_topic(&self) -> String {
        self.0.clone()
    }

    fn consumer_partition(&self) -> i32 {
        self.1
    }

    fn consumer_offset(&self) -> i64 {
        self.2
    }
}


pub trait MessageContext {
    fn consumer_topic(&self) -> String;
    fn consumer_partition(&self) -> i32;
    fn consumer_offset(&self) -> i64;
}

pub trait MessageHeaders { // Vec<(String, Vec<u8>)>
    fn headers(&self) -> Vec<(String, Vec<u8>)>;
}

pub trait MessageValue<K> {
    fn value(&self) -> &K;
}

pub trait MessageKey<V> {
    fn key(&self) -> &V;
}

pub trait FromOwnedMessage<V> 
{
    fn from_owned_message(msg: OwnedMessage) -> Self;
}

pub struct ValueMessage<V> {
    consumer_topic: String,
    consumer_partition: i32,
    consumer_offset: i64,
    headers: Vec<(String, Vec<u8>)>,
    key: Vec<u8>,
    value: V,
}

impl <V> FromOwnedMessage<V> for ValueMessage<V::Output> 
where V: PDeserialize
{
    fn from_owned_message(msg: OwnedMessage) -> Self {
        let raw_value = msg.payload().unwrap();
        let value = V::deserialize(raw_value).unwrap();

        let headers: Vec<_> = msg.headers()
            .map(
                |h| h.iter().map(
                    |h| (String::from(h.key), h.value.expect("No Header").to_vec())
                ).collect::<Vec<(String, Vec<u8>)>>()
            ).unwrap();

        info!("Consuming message: topic: {}, partition: {}, offset: {}", msg.topic(), msg.partition(), msg.offset());

        Self {
            consumer_topic: msg.topic().to_string(),
            consumer_partition: msg.partition(),
            consumer_offset: msg.offset(),
            headers,
            key: msg.key().map(|k| k.to_vec()).expect("Failed to get key"),
            value,
        }
    }
}

impl <V> MessageContext for ValueMessage<V> {
    fn consumer_topic(&self) -> String {
        self.consumer_topic.clone()
    }

    fn consumer_partition(&self) -> i32 {
        self.consumer_partition
    }

    fn consumer_offset(&self) -> i64 {
        self.consumer_offset
    }
}

impl <V> MessageHeaders for ValueMessage<V> {
    fn headers(&self) -> Vec<(String, Vec<u8>)> {
        self.headers.clone()
    }
}

impl <V> MessageKey<Vec<u8>> for ValueMessage<V> {
    fn key(&self) -> &Vec<u8> {
        &self.key
    }
}

impl <V> MessageValue<V> for ValueMessage<V> 
where V: serde::Serialize
{
    fn value(&self) -> &V {
        &self.value
    }
}

pub struct KeyValueMessage<K, V> {
    consumer_topic: String,
    consumer_partition: i32,
    consumer_offset: i64,
    headers: Vec<(String, Vec<u8>)>,
    key: K,
    value: V,
}

impl <K, V> FromOwnedMessage<(K, V)> for KeyValueMessage<K::Output, V::Output> 
where K: PDeserialize,
    V: PDeserialize
{
    fn from_owned_message(msg: OwnedMessage) -> Self {
        let raw_key = msg.key().unwrap();
        let key = K::deserialize(raw_key).expect("Failed to deserialise key");

        let raw_value = msg.payload().unwrap();
        let value = V::deserialize(raw_value).expect("Failed to deserialise value");

        let headers: Vec<_> = msg.headers()
            .map(
                |h| h.iter().map(
                    |h| (String::from(h.key), h.value.expect("No Header").to_vec())
                ).collect::<Vec<(String, Vec<u8>)>>()
            ).unwrap();

        Self {
            consumer_topic: msg.topic().to_string(),
            consumer_partition: msg.partition(),
            consumer_offset: msg.offset(),
            headers,
            key,
            value,
        }
    }
}

impl <K, V> MessageContext for KeyValueMessage<K, V> {
    fn consumer_topic(&self) -> String {
        self.consumer_topic.clone()
    }

    fn consumer_partition(&self) -> i32 {
        self.consumer_partition
    }

    fn consumer_offset(&self) -> i64 {
        self.consumer_offset
    }
}

impl <K, V> MessageHeaders for KeyValueMessage<K, V> {
    fn headers(&self) -> Vec<(String, Vec<u8>)> {
        self.headers.clone()
    }
}

impl <K, V> MessageKey<K> for KeyValueMessage<K, V> {
    fn key(&self) -> &K {
        &self.key
    }
}

impl <K, V> MessageValue<V> for KeyValueMessage<K, V> 
{
    fn value(&self) -> &V {
        &self.value
    }
}

impl <K, V> RecieveMessageContext<K, V> for KeyValueMessage<K, V> 
{
    fn recieve(ctx: GenericMessageContext, headers: Vec<(String, Vec<u8>)>, key: K, value: V) -> Self {
        Self {
            consumer_topic: ctx.consumer_topic,
            consumer_partition: ctx.consumer_partition,
            consumer_offset: ctx.consumer_offset,
            headers,
            key,
            value,
        }
    }
}