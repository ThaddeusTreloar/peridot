use std::collections::HashMap;

use super::{ReadableStateBackend, WriteableStateBackend};

pub struct InMemoryStateBackend<T> {
    store: tokio::sync::RwLock<HashMap<String, T>>
}

impl <T> Default for InMemoryStateBackend<T> {
    fn default() -> Self {
        InMemoryStateBackend {
            store: tokio::sync::RwLock::new(HashMap::new())
        }
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