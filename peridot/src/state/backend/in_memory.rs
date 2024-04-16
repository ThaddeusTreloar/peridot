use std::{hash::Hash, sync::Arc};

use dashmap::DashMap;
use tracing::info;

use crate::message::types::Message;

use super::{NonVersioned, ReadableStateBackend, StateBackend, WriteableStateBackend};

pub struct InMemoryStateBackend<K, V, VM = NonVersioned>
where
    K: Hash + Eq,
{
    store: DashMap<K, V>,
    _versioning_method: std::marker::PhantomData<VM>,
}

#[derive(Debug, thiserror::Error)]
pub enum InMemoryStateBackendError {}

impl<K, V> Default for InMemoryStateBackend<K, V>
where
    K: Hash + Eq,
{
    fn default() -> Self {
        InMemoryStateBackend {
            store: Default::default(),
            _versioning_method: Default::default(),
        }
    }
}

impl<K, V> StateBackend for InMemoryStateBackend<K, V>
where
    K: Send + Sync + Hash + Eq,
    V: Send + Sync,
{
    async fn with_topic_name_and_partition(_topic_name: &str, _partition: i32) -> Self {
        Self::default()
    }
}

impl<K, V> ReadableStateBackend for InMemoryStateBackend<K, V, NonVersioned>
where
    K: Send + Sync + Hash + Eq,
    V: Send + Sync + Clone,
{
    type Error = InMemoryStateBackendError;
    type KeyType = K;
    type ValueType = V;
    type VersioningMethod = NonVersioned;

    async fn get(&self, key: &Self::KeyType) -> Result<Option<Self::ValueType>, Self::Error> {
        match self.store.get(key) {
            None => return Ok(None),
            Some(value) => {
                return Ok(Some(value.value().clone()));
            }
        }
    }
}

impl<K, V> WriteableStateBackend<K, V> for InMemoryStateBackend<K, V, NonVersioned>
where
    K: Send + Sync + Hash + Eq + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
    type Error = InMemoryStateBackendError;
    type VersioningMethod = NonVersioned;

    async fn commit_update(
        self: Arc<Self>,
        message: Message<K, V>,
    ) -> Result<Option<Message<K, V>>, Self::Error> {
        self.store
            .insert(message.key().clone(), message.value().clone());
        Ok(None)
    }

    async fn delete(self: Arc<Self>, key: &K) -> Result<Option<Message<K, V>>, Self::Error> {
        self.store.remove(key);
        Ok(None)
    }
}
