use std::collections::HashMap;

use tokio::sync::RwLock;
use tracing::info;

use super::{ReadableStateBackend, WriteableStateBackend, StateBackend};

pub struct InMemoryStateBackend<T> {
    store: RwLock<HashMap<String, T>>,
    offsets: RwLock<HashMap<String, i64>>,
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
    async fn commit_offset(&self, topic: &str, partition: i32, offset: i64) {
        let mut offsets = self.offsets.write().await;
        offsets.insert(format!("{}-{}", topic, partition), offset);

        info!("Committed offset: {}-{}:{}", topic, partition, offset);
        info!("Current offsets: {:?}", offsets);
    }

    async fn get_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        let offsets = self.offsets.read().await;
        offsets.get(&format!("{}-{}", topic, partition)).cloned()
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