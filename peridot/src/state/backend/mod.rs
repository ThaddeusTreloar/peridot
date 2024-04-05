use std::sync::Arc;

use dashmap::DashMap;
use futures::Future;

use crate::message::types::Message;

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
    type KeyType;
    type ValueType;

    fn get(&self, key: &Self::KeyType) -> impl Future<Output = Option<Self::ValueType>> + Send;
}

pub trait WriteableStateBackend<K, V> {
    fn commit_update(
        self: Arc<Self>,
        message: Message<K, V>,
    ) -> impl Future<Output = Option<Message<K, V>>> + Send;
    fn delete(&self, key: &K) -> impl Future<Output = Option<Message<K, V>>> + Send;
}

// User facing API interface for interacting with a state store
pub trait BackendView {
    type KeyType;
    type ValueType;

    fn get(&self, key: &Self::KeyType) -> impl Future<Output = Option<Self::ValueType>> + Send;
    // fn all
    // fn range
    // fn prefix
    // fn count
}
