use std::sync::Arc;

use dashmap::DashMap;
use serde::{de::DeserializeOwned, Serialize};

use super::{StateBackend, StateBackendContext};

#[derive(Default)]
pub struct InMemoryStateBackend {
    store: DashMap<String, DashMap<Vec<u8>, Vec<u8>>>,
}

impl InMemoryStateBackend {}

#[derive(Debug, thiserror::Error)]
pub enum InMemoryStateBackendError {}

impl StateBackendContext for InMemoryStateBackend {
    async fn with_topic_name_and_partition(_topic_name: &str, _partition: i32) -> Self {
        Self::default()
    }

    fn get_state_store_time(&self) -> crate::message::types::PeridotTimestamp {
        unimplemented!("Get state store time")
    }
}

impl StateBackend for InMemoryStateBackend {
    type Error = InMemoryStateBackendError;

    async fn get<K, V>(
        self: Arc<Self>,
        key: K,
        store: Arc<String>,
    ) -> Result<Option<V>, Self::Error>
    where
        K: Serialize + Send,
        V: DeserializeOwned,
    {
        let store = self.store.get(store.as_ref()).expect("Failed to get store");

        let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");

        match store.get(&key_bytes).map(|v| v.value().clone()) {
            None => Ok(None),
            Some(value_bytes) => {
                let value: V =
                    bincode::deserialize(&value_bytes).expect("Failed to deserialize value");

                Ok(Some(value))
            }
        }
    }

    async fn put<K, V>(
        self: Arc<Self>,
        key: K,
        value: V,
        store: Arc<String>,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
        V: Serialize + Send,
    {
        let store = self.store.get(store.as_ref()).expect("Failed to get store");

        let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");
        let value_byte = bincode::serialize(&value).expect("Failed to serialize value");

        store.insert(key_bytes, value_byte);

        Ok(())
    }

    async fn put_range<K, V>(
        self: Arc<Self>,
        range: Vec<(K, V)>,
        store: Arc<String>,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
        V: Serialize + Send,
    {
        for (key, value) in range {
            let store = self.store.get(store.as_ref()).expect("Failed to get store");

            let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");
            let value_byte = bincode::serialize(&value).expect("Failed to serialize value");

            store.insert(key_bytes, value_byte);
        }

        Ok(())
    }

    async fn delete<K>(
        self: Arc<Self>,
        key: K,
        store: Arc<String>,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
    {
        let store = self.store.get(store.as_ref()).expect("Failed to get store");

        let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");

        store.remove(&key_bytes);

        Ok(())
    }
}
