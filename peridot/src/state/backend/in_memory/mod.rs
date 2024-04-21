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
        &self,
        key: K,
        store: &str,
    ) -> Result<Option<V>, Self::Error>
    where
        K: Serialize + Send,
        V: DeserializeOwned,
    {
        let store = self.store.get(store).expect("Failed to get store");

        let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");

        let value = match store.get(&key_bytes) {
            None => return Ok(None),
            Some(value_bytes) => 
                    bincode::deserialize(value_bytes.as_ref()).expect("Failed to deserialize value")
        };

        Ok(Some(value))
    }

    async fn put<K, V>(
        &self,
        key: K,
        value: V,
        store: &str,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
        V: Serialize + Send,
    {
        let store = self.store.get(store).expect("Failed to get store");

        let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");
        let value_byte = bincode::serialize(&value).expect("Failed to serialize value");

        store.insert(key_bytes, value_byte);

        Ok(())
    }

    async fn put_range<K, V>(
        &self,
        range: Vec<(K, V)>,
        store: &str,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
        V: Serialize + Send,
    {
        for (key, value) in range {
            let store = self.store.get(store).expect("Failed to get store");

            let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");
            let value_byte = bincode::serialize(&value).expect("Failed to serialize value");

            store.insert(key_bytes, value_byte);
        }

        Ok(())
    }

    async fn delete<K>(
        &self,
        key: K,
        store: &str,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
    {
        let store = self.store.get(store).expect("Failed to get store");

        let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");

        store.remove(&key_bytes);

        Ok(())
    }
}
