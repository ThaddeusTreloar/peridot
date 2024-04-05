use std::{hash::Hash, sync::Arc};

use dashmap::DashMap;
use tracing::info;

use crate::message::types::Message;

use super::{CommitLog, ReadableStateBackend, StateBackend, WriteableStateBackend};

pub struct InMemoryStateBackend<K, V>
where
    K: Hash + Eq,
{
    store: DashMap<K, V>,
    offsets: Arc<CommitLog>,
}

impl<K, V> Default for InMemoryStateBackend<K, V>
where
    K: Hash + Eq,
{
    fn default() -> Self {
        InMemoryStateBackend {
            store: Default::default(),
            offsets: Default::default(),
        }
    }
}

impl<K, V> StateBackend for InMemoryStateBackend<K, V>
where
    K: Send + Sync + Hash + Eq,
    V: Send + Sync,
{
    async fn with_topic_name(_topic_name: &str) -> Self {
        Self::default()
    }

    async fn with_topic_name_and_commit_log(_topic_name: &str, commit_log: Arc<CommitLog>) -> Self {
        InMemoryStateBackend {
            store: Default::default(),
            offsets: commit_log,
        }
    }

    fn get_commit_log(&self) -> std::sync::Arc<CommitLog> {
        self.offsets.clone()
    }

    async fn commit_offset(&self, topic: &str, partition: i32, offset: i64) {
        self.offsets.commit_offset(topic, partition, offset);

        info!("Committed offset: {}-{}:{}", topic, partition, offset);
        info!("Current offsets: {:?}", self.offsets);
    }

    async fn get_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.offsets.get_offset(topic, partition)
    }
}

impl<K, V> ReadableStateBackend for InMemoryStateBackend<K, V>
where
    K: Send + Sync + Hash + Eq,
    V: Send + Sync + Clone,
{
    type KeyType = K;
    type ValueType = V;

    async fn get(&self, key: &Self::KeyType) -> Option<Self::ValueType> {
        Some(self.store.get(key)?.value().clone())
    }
}

impl<K, V> WriteableStateBackend<K, V> for InMemoryStateBackend<K, V>
where
    K: Send + Sync + Hash + Eq + Clone,
    V: Send + Sync + Clone,
{
    async fn commit_update(self: Arc<Self>, message: Message<K, V>) -> Option<Message<K, V>> {
        self.store
            .insert(message.key().clone(), message.value().clone());
        None
    }

    async fn delete(&self, key: &K) -> Option<Message<K, V>> {
        self.store.remove(key);
        None
    }
}
