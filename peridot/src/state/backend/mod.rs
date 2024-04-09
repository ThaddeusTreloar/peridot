use std::sync::Arc;

use dashmap::DashMap;
use futures::{Future, FutureExt, TryFutureExt};
use rdkafka::producer::{NoCustomPartitioner, Partitioner};
use serde::Serialize;

use crate::{
    engine::AppEngine,
    message::types::Message,
    serde_ext::{PDeserialize, PSerialize},
    util::hash::get_partition_for_key,
};

pub mod error;
pub mod in_memory;
pub mod persistent;

// Commit logs are a temporary solution to the problem of tracking offsets
// This will be deprecated in favour of a upstreaming persitent storage
// to the AppEngine
#[derive(Debug, Default)]
pub struct CommitLogs {
    logs: DashMap<String, CommitLog>,
}

impl CommitLogs {
    pub fn get_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        let key = format!("{}-{}", topic, partition);
        self.logs
            .get(&key)
            .and_then(|log| log.get_offset(topic, partition))
    }
}

#[derive(Debug, Default)]
pub struct CommitLog {
    store: DashMap<String, i64>,
}

impl CommitLog {
    fn commit_offset(&self, topic: &str, partition: i32, offset: i64) {
        let key = format!("{}-{}", topic, partition);
        self.store.insert(key, offset);
    }

    pub fn get_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        let key = format!("{}-{}", topic, partition);
        self.store.get(&key).map(|offset| *offset)
    }
}

pub trait StateBackend {
    fn with_topic_name(topic_name: &str) -> impl Future<Output = Self>;
    fn with_topic_name_and_commit_log(
        topic_name: &str,
        commit_log: Arc<CommitLog>,
    ) -> impl Future<Output = Self>;
    fn get_commit_log(&self) -> Arc<CommitLog>;
    fn commit_offset(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> impl Future<Output = ()> + Send;
    fn get_offset(&self, topic: &str, partition: i32) -> impl Future<Output = Option<i64>> + Send;
}

pub trait ReadableStateBackend {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;

    fn get(
        &self,
        key: &Self::KeyType,
    ) -> impl Future<Output = Result<Option<Self::ValueType>, Self::Error>> + Send;
}

pub trait WriteableStateBackend<K, V> {
    type Error: std::error::Error;

    fn commit_update(
        self: Arc<Self>,
        message: Message<K, V>,
    ) -> impl Future<Output = Result<Option<Message<K, V>>, Self::Error>> + Send + 'static;

    fn delete(
        self: Arc<Self>,
        key: &K,
    ) -> impl Future<Output = Result<Option<Message<K, V>>, Self::Error>> + Send;
}

// User facing API interface for interacting with a state store
pub trait BackendView {
    type KeyType;
    type ValueType;

    async fn get(&self, key: &Self::KeyType) -> Option<Self::ValueType>;
    // fn all
    // fn range
    // fn prefix
    // fn count
}

#[derive(Clone)]
pub struct EngineBackendView<KS, VD, B> {
    engine_ref: Arc<AppEngine<B>>,
    table_name: String,
    partitioner: (),
    _key_type: std::marker::PhantomData<KS>,
    _value_type: std::marker::PhantomData<VD>,
}

impl<KS, VD, B> EngineBackendView<KS, VD, B> {
    pub fn new(table_name: String, engine_ref: Arc<AppEngine<B>>) -> Self {
        Self {
            engine_ref,
            table_name,
            partitioner: Default::default(),
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

impl<KS, VD, B> BackendView for EngineBackendView<KS, VD, B>
where
    KS: PSerialize,
    VD: PDeserialize,
    B: ReadableStateBackend<KeyType = KS::Input, ValueType = VD::Output> + Send + Sync + 'static,
{
    type KeyType = KS::Input;
    type ValueType = VD::Output;

    async fn get(&self, key: &Self::KeyType) -> Option<Self::ValueType> {
        let key_bytes = KS::serialize(key).expect("Failed to serialize key");

        let partition_count = self
            .engine_ref
            .get_table_partition_count(&self.table_name)
            .expect("Table doesn't exist");

        let partition = get_partition_for_key(&key_bytes, partition_count);

        self.engine_ref
            .get_state_store(self.table_name.clone(), partition)
            .get(key)
            .await
            .expect("Backend Failure")
    }
}
