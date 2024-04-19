use std::sync::Arc;

use futures::Future;
use serde::{de::DeserializeOwned, Serialize};

use crate::message::types::PeridotTimestamp;

use self::facade::{FacadeDistributor, StateFacade};

pub mod error;
pub mod facade;
pub mod in_memory;
//pub mod persistent;

struct VersionedRecord<V> {
    pub value: V,
    pub timestamp: i64,
}

pub trait GetViewDistributor {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;
    type Backend;

    fn get_view_distributor(
        &self,
    ) -> FacadeDistributor<Self::KeyType, Self::ValueType, Self::Backend>;
}

pub trait GetView {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;
    type Backend;

    fn get_view (
        &self,
        partition: i32,
    ) -> StateFacade<Self::KeyType, Self::ValueType, Self::Backend>;
}

pub trait StateBackendContext {
    fn with_topic_name_and_partition(
        topic_name: &str,
        partition: i32,
    ) -> impl Future<Output = Self>;

    fn get_state_store_time(&self) -> PeridotTimestamp;
}

#[trait_variant::make(Send)]
pub trait StateBackend: Send {
    type Error: std::error::Error;

    async fn get<K, V>(
        &self,
        key: K,
        store: &str,
    ) -> Result<Option<V>, Self::Error>
    where
        K: Serialize + Send,
        V: DeserializeOwned;

    async fn put<K, V>(
        &self,
        key: K,
        value: V,
        store: &str,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
        V: Serialize + Send;

    async fn put_range<K, V>(
        &self,
        range: Vec<(K, V)>,
        store: &str,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
        V: Serialize + Send;

    async fn delete<K>(
        &self,
        key: K,
        store: &str,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send;
}

pub trait VersionedStateBackend {
    type Error: std::error::Error;

    fn get_version<K, V>(
        self: Arc<Self>,
        key: K,
        store: Arc<String>,
        at_timestamp: i64,
    ) -> impl Future<Output = Result<Option<VersionedRecord<V>>, Self::Error>> + Send
    where
        K: Serialize,
        V: DeserializeOwned;
    fn get_latest_version<K, V>(
        self: Arc<Self>,
        key: K,
        store: Arc<String>,
    ) -> impl Future<Output = Result<Option<VersionedRecord<V>>, Self::Error>> + Send
    where
        K: Serialize,
        V: DeserializeOwned;
    fn put_version<K, V>(
        self: Arc<Self>,
        key: K,
        value: V,
        store: Arc<String>,
        timestamp: i64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        K: Serialize,
        V: DeserializeOwned;
    fn put_range_version<K, V>(
        self: Arc<Self>,
        range: Vec<(K, V, i64)>,
        store: Arc<String>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        K: Serialize,
        V: DeserializeOwned;
    fn delete_version<K>(
        self: Arc<Self>,
        key: K,
        store: Arc<String>,
        timestamp: i64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        K: Serialize;
}

#[trait_variant::make(Send)]
pub trait ReadableStateView {
    type Error: std::error::Error;
    type KeyType: Serialize + Send;
    type ValueType: DeserializeOwned + Send;

    async fn get(
        self: Arc<Self>,
        key: Self::KeyType,
    ) -> Result<Option<Self::ValueType>, Self::Error>;
}

pub trait WriteableStateView {
    type Error: std::error::Error;
    type KeyType: Serialize + Send;
    type ValueType: Serialize + Send;

    fn put(
        self: Arc<Self>,
        key: Self::KeyType,
        value: Self::ValueType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn put_range(
        self: Arc<Self>,
        range: Vec<(Self::KeyType, Self::ValueType)>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn delete(
        self: Arc<Self>,
        key: Self::KeyType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[trait_variant::make(Send)]
pub trait ReadableVersionedStateView {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;

    async fn get_version(
        self: Arc<Self>,
        key: Self::KeyType,
        at_timestamp: i64,
    ) -> Result<Option<VersionedRecord<Self::ValueType>>, Self::Error>;
}

pub trait WriteableVersionedStateView {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;

    fn put_version(
        self: Arc<Self>,
        key: Self::KeyType,
        value: Self::ValueType,
        timestamp: i64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn put_version_range(
        self: Arc<Self>,
        range: Vec<(Self::KeyType, Self::ValueType, i64)>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn delete_version(
        self: Arc<Self>,
        key: Self::KeyType,
        timestamp: i64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
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
