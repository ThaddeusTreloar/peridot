use rdkafka::{message::{OwnedMessage, Headers, FromBytes}, Message, producer::{BaseRecord}};
use tracing::info;

use crate::{serde_ext::PSerialize, app::wrappers::{MessageKey, MessageValue}};

pub struct RecordParts {
    headers: (),
    key: Vec<u8>,
    value: Vec<u8>,
}

impl RecordParts {
    pub fn new(headers: (), key: Vec<u8>, value: Vec<u8>) -> Self {
        RecordParts { headers, key, value }
    }

    pub fn headers(&self) -> &() {
        &self.headers
    }

    pub fn key(&self) -> &Vec<u8> {
        &self.key
    }

    pub fn value(&self) -> &Vec<u8> {
        &self.value
    }

    pub fn into_record<'a>(&'a self, topic: &'a str) -> BaseRecord<'a, Vec<u8>, Vec<u8>> {
        BaseRecord::to(topic)
            .key(&self.key)
            .payload(&self.value)
    }
}

pub trait IntoRecordParts {
    fn into_record_parts(self) -> RecordParts;
}

#[derive(Debug, thiserror::Error)]
pub enum RecordPartsError {
    #[error("Failed to serialise key: {0}")]
    KeySerialisationError(String),
    #[error("Failed to serialise value: {0}")]
    ValueSerialisationError(String),
}

pub trait TryIntoRecordParts<K, V> 
where K: PSerialize, 
    V: PSerialize, 
    Self: MessageKey<K::Input> + MessageValue<V::Input>
{
    fn try_into_record_parts(&self) -> Result<RecordParts, RecordPartsError> {
        let key = K::serialize(self.key())
            .map_err(
                |e| RecordPartsError::KeySerialisationError(e.to_string())
            )?;
        let value = V::serialize(self.value())
            .map_err(
                |e| RecordPartsError::ValueSerialisationError(e.to_string())
            )?;

        Ok(RecordParts::new((), key, value))
    }
}

impl <K, V, T> TryIntoRecordParts<K, V> for T
where K: PSerialize, 
    V: PSerialize, 
    T: MessageKey<K::Input> + MessageValue<V::Input>, 
{}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StringKeyValue<V> 
{
    pub key: String,
    pub value: V,
}

impl<V> StringKeyValue<V> 
{
    pub fn new(key: String, value: V) -> Self {
        StringKeyValue { key, value }
    }

    pub fn key(&self) -> &String {
        &self.key
    }

    pub fn value(&self) -> &V {
        &self.value
    }
}
impl <T> IntoRecordParts for StringKeyValue<T> 
where T: serde::Serialize
{
    fn into_record_parts(self) -> RecordParts {
        let ser_key = self.key.as_bytes().to_vec();
        let ser_value = serde_json::to_vec(self.value()).unwrap();

        //info!("Serialised key: {:?}", ser_key);
        //info!("Serialised value: {:?}", ser_value);

        RecordParts::new((), ser_key, ser_value)
    }
}

impl<'a, V> From<OwnedMessage> for StringKeyValue<V>
where
    V: serde::de::DeserializeOwned,
{
    fn from(message: OwnedMessage) -> Self {
        let maybe_key = message.key();
        let maybe_value = message.payload();

        let key = String::from_utf8_lossy(maybe_key.unwrap()).to_string();
        let value = maybe_value.map(|v| serde_json::from_slice(v).expect("Failed to deserialise value: ")).expect("No value found");
        StringKeyValue { key, value }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KeyValue<K, V> 
{
    pub key: K,
    pub value: V,
}

impl<K, V> KeyValue<K, V> 
{
    pub fn new(key: K, value: V) -> Self {
        KeyValue { key, value }
    }

    pub fn key(&self) -> &K {
        &self.key
    }

    pub fn value(&self) -> &V {
        &self.value
    }

    pub fn into_parts(self) -> (K, V) {
        (self.key, self.value)
    }
}

impl<'a, K, V> From<OwnedMessage> for KeyValue<K, V>
where
    K: serde::de::DeserializeOwned,
    V: serde::de::DeserializeOwned,
{
    fn from(message: OwnedMessage) -> Self {
        let maybe_key = message.key();
        let maybe_value = message.payload();

        let key = maybe_key.map(|k| serde_json::from_slice(k).expect("Failed to deserialise key: ")).expect("No key found");
        let value = maybe_value.map(|v| serde_json::from_slice(v).expect("Failed to deserialise value: ")).expect("No value found");
        KeyValue { key, value }
    }
}


impl<K, V> IntoRecordParts for KeyValue<K, V>
where
    K: serde::Serialize,
    V: serde::Serialize,
{
    fn into_record_parts(self) -> RecordParts {
        let ser_key = serde_json::to_vec(&self.key).unwrap();
        let ser_value = serde_json::to_vec(&self.value).unwrap();

        info!("Serialised key: {:?}", ser_key);
        info!("Serialised value: {:?}", ser_value);

        RecordParts::new((), ser_key, ser_value)
    }
}


#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Record<K, V> {
    pub headers: Vec<(String, Option<Vec<u8>>)>,
    pub key: K,
    pub value: V,
}

impl<'a, K, V> From<OwnedMessage> for Record<K, V>
where
    K: serde::de::DeserializeOwned,
    V: serde::de::DeserializeOwned,
{
    fn from(message: OwnedMessage) -> Self {
        let headers = message.headers()
            .map(
                |headers| headers
                    .iter()
                    .map(|h| (
                        h.key.to_string(), 
                        h.value.map(Vec::from)
                    ))
                    .collect()
            ).unwrap();

        let key = message.key().map(|k| serde_json::from_slice(k).unwrap()).unwrap();
        let value = message.payload().map(|v| serde_json::from_slice(v).unwrap()).unwrap();
        Record { headers, key, value }
    }
}

impl<K, V> IntoRecordParts for Record<K, V>
where
    K: serde::Serialize,
    V: serde::Serialize,
{
    fn into_record_parts(self) -> RecordParts{
        let ser_key = serde_json::to_vec(&self.key).unwrap();
        let ser_value = serde_json::to_vec(&self.value).unwrap();

        info!("Serialised key: {:?}", ser_key);
        info!("Serialised value: {:?}", ser_value);

        RecordParts::new((), ser_key, ser_value)
    }
}