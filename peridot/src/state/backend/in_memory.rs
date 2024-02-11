use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;
use tracing::info;

use super::{ReadableStateBackend, WriteableStateBackend, StateBackend, CommitLog};

pub struct InMemoryStateBackend<T> {
    store: RwLock<HashMap<String, T>>,
    offsets: Arc<CommitLog>,
}

impl <T> Default for InMemoryStateBackend<T> {
    fn default() -> Self {
        InMemoryStateBackend {
            store: Default::default(),
            offsets: Default::default(),
        }
    }
}

impl <T> StateBackend for InMemoryStateBackend<T> 
where T: Send + Sync + 'static
{
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

impl <T> ReadableStateBackend<T> for InMemoryStateBackend<T> 
where T: Clone + Send + Sync + 'static
{
    async fn get(&self, key: &str) -> Option<T> {
        self.store
            .read().await
            .get(key)
            .cloned()
    }
}

impl <T> WriteableStateBackend<T> for InMemoryStateBackend<T> 
where T: Send + Sync + 'static
{
    async fn set(&self, key: &str, value: T) -> Option<T> {
        self.store
        .write().await
        .insert(key.to_string(), value)
    }
    
    async fn delete(&self, key: &str) -> Option<T> {
        self.store
            .write().await
            .remove(key)
    }
}