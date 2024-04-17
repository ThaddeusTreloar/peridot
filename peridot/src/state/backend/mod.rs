use futures::Future;

use crate::message::types::PeridotTimestamp;

pub mod error;
pub mod facade;
pub mod in_memory;
pub mod persistent;

struct VersionedRecord<V> {
    pub value: V,
    pub timestamp: i64,
}

pub trait IntoView {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;

    fn into_view(
        &self,
        parition: i32,
    ) -> impl ReadableStateView<KeyType = Self::KeyType, ValueType = Self::ValueType, Error = Self::Error>;
}

pub trait StateBackendContext {
    fn with_topic_name_and_partition(
        topic_name: &str,
        partition: i32,
    ) -> impl Future<Output = Self>;

    fn get_state_store_time(&self) -> PeridotTimestamp;
}

pub trait StateBackend {
    type Error: std::error::Error;

    fn get<K, V>(
        &self,
        key: &K,
        store: &str,
    ) -> impl Future<Output = Result<Option<V>, Self::Error>> + Send;
    fn put<K, V>(
        &self,
        key: &K,
        value: &V,
        store: &str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn put_range<K, V>(
        &self,
        range: Vec<(&K, &V)>,
        store: &str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn delete<K>(
        &self,
        key: &K,
        store: &str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait VersionedStateBackend {
    type Error: std::error::Error;

    fn get_version<K, V>(
        &self,
        key: &K,
        store: &str,
        at_timestamp: i64,
    ) -> impl Future<Output = Result<Option<VersionedRecord<V>>, Self::Error>> + Send;
    fn get_latest_version<K, V>(
        &self,
        key: &K,
        store: &str,
    ) -> impl Future<Output = Result<Option<VersionedRecord<V>>, Self::Error>> + Send;
    fn put_version<K, V>(
        &self,
        key: &K,
        value: &V,
        store: &str,
        timestamp: i64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn put_range_version<K, V>(
        &self,
        range: Vec<(&K, &V, i64)>,
        store: &str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn delete_version<K>(
        &self,
        key: &K,
        store: &str,
        timestamp: i64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait ReadableStateView {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;

    fn get<'a>(
        &'a self,
        key: &'a Self::KeyType,
    ) -> impl Future<Output = Result<Option<Self::ValueType>, Self::Error>> + Send + 'a;
}
pub trait WriteableStateView {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;

    fn put<'a>(
        &'a self,
        key: &'a Self::KeyType,
        value: &'a Self::ValueType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    fn put_range<'a>(
        &'a self,
        range: Vec<(&'a Self::KeyType, &'a Self::ValueType)>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    fn delete<'a>(
        &'a self,
        key: &'a Self::KeyType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

pub trait ReadableVersionedStateView {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;

    fn get_version<'a>(
        &'a self,
        key: &'a Self::KeyType,
        at_timestamp: i64,
    ) -> impl Future<Output = Result<Option<VersionedRecord<Self::ValueType>>, Self::Error>> + Send + 'a;
}

pub trait WriteableVersionedStateView {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;

    fn put_version<'a>(
        &'a self,
        key: &'a Self::KeyType,
        value: &'a Self::ValueType,
        timestamp: i64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    fn put_version_range<'a>(
        &'a self,
        range: Vec<(&'a Self::KeyType, &'a Self::ValueType, i64)>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    fn delete_version<'a>(
        &'a self,
        key: &'a Self::KeyType,
        timestamp: i64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

pub trait ReadWriteStateView<K, V>:
    ReadableStateView<KeyType = K, ValueType = V> + WriteableStateView<KeyType = K, ValueType = V>
{
}

impl<K, V, T> ReadWriteStateView<K, V> for T where
    T: ReadableStateView<KeyType = K, ValueType = V>
        + WriteableStateView<KeyType = K, ValueType = V>
{
}
