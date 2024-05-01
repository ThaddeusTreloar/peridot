use std::borrow::BorrowMut;

use dashmap::DashMap;
use serde::{de::DeserializeOwned, Serialize};

use crate::message::types::PeridotTimestamp;

use super::{Checkpoint, StateBackend, StateBackendContext};

#[derive(Default)]
pub struct InMemoryStateBackend {
    store: DashMap<String, DashMap<Vec<u8>, Vec<u8>>>,
    checkpoint: DashMap<String, Checkpoint>,
    store_time: PeridotTimestamp
}

impl InMemoryStateBackend {}

#[derive(Debug, thiserror::Error)]
pub enum InMemoryStateBackendError {
}

impl StateBackendContext for InMemoryStateBackend {
    async fn with_source_topic_name_and_partition(_topic_name: &str, _partition: i32) -> Self {
        Default::default()
    }
}

impl StateBackend for InMemoryStateBackend {
    type Error = InMemoryStateBackendError;

    async fn get_state_store_time(&self) -> PeridotTimestamp {
        self.store_time.clone()
    }

    async fn get_state_store_checkpoint(&self, table_name: &str) -> Option<Checkpoint> {
        Some(self.checkpoint.get(table_name)?.clone())
    }

    async fn create_checkpoint(&self, table_name: &str, offset: i64) -> Result<(), Self::Error> {
        match self.checkpoint.get_mut(table_name) {
            None => {
                self.checkpoint.insert(table_name.to_owned(), Checkpoint { 
                    table_name: table_name.to_owned(),
                    offset,
                });

                Ok(())
            },
            Some(mut checkpoint_ref) => {
                checkpoint_ref.value_mut().set_offset_if_greater(offset);

                Ok(())
            }
        }
    }

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
