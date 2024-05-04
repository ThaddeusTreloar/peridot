use std::borrow::{Borrow, BorrowMut};

use dashmap::{mapref::one::Ref, DashMap};
use serde::{de::DeserializeOwned, Serialize};
use tracing::info;

use crate::message::types::PeridotTimestamp;

use super::{Checkpoint, StateBackend};

#[derive(Default)]
pub struct InMemoryStateBackend {
    store: DashMap<String, DashMap<Vec<u8>, Vec<u8>>>,
    checkpoint: DashMap<String, Checkpoint>,
    store_time: PeridotTimestamp
}

impl InMemoryStateBackend {
    fn derive_state_key(state_name: &str, partition: i32) -> String {
        format!("{}-{}", state_name, partition)
    }

    fn get_state_store(&self, state_key: &str) -> Ref<String, DashMap<Vec<u8>, Vec<u8>>> {
        match self.store.get(state_key) {
            None => {
                self.store.insert(state_key.to_owned(), Default::default());

                self.store.get(state_key).expect("THIS SHOULD BE IMPOSSIBLE.")
            },
            Some(state) => {
                state
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InMemoryStateBackendError {
}

impl StateBackend for InMemoryStateBackend {
    type Error = InMemoryStateBackendError;

    fn with_source_topic_name_and_partition(_topic_name: &str, _partition: i32) -> Result<Self, Self::Error> {
        Ok(Default::default())
    }

    fn get_state_store_time(&self) -> PeridotTimestamp {
        self.store_time.clone()
    }

    fn get_state_store_checkpoint(&self, state_name: &str, partition: i32) -> Option<Checkpoint> {
        let checkpoint_name = format!("{}-{}", state_name, partition);

        Some(self.checkpoint.get(&checkpoint_name)?.clone())
    }

    fn create_checkpoint(&self, state_name: &str, partition: i32, offset: i64) -> Result<(), Self::Error> {
        let checkpoint_name = format!("{}-{}", state_name, partition);

        match self.checkpoint.get_mut(&checkpoint_name) {
            None => {
                self.checkpoint.insert(checkpoint_name.clone(), Checkpoint { 
                    table_name: checkpoint_name,
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
        state_name: &str,
        partition: i32,
    ) -> Result<Option<V>, Self::Error>
    where
        K: Serialize + Send,
        V: DeserializeOwned,
    {
        let state_key = Self::derive_state_key(state_name, partition);

        let store = self.get_state_store(&state_key);

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
        state_name: &str,
        partition: i32,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
        V: Serialize + Send,
    {
        let state_key = Self::derive_state_key(state_name, partition);

        let store = self.get_state_store(&state_key);

        let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");
        let value_byte = bincode::serialize(&value).expect("Failed to serialize value");

        store.insert(key_bytes, value_byte);

        Ok(())
    }

    async fn put_range<K, V>(
        &self,
        range: Vec<(K, V)>,
        state_name: &str,
        partition: i32,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
        V: Serialize + Send,
    {
        let state_key = Self::derive_state_key(state_name, partition);

        self.store.iter().for_each(|e| {
            tracing::debug!("Key in stores: {}", e.key())
        });

        for (key, value) in range {
            let store = self.get_state_store(&state_key);

            let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");
            let value_byte = bincode::serialize(&value).expect("Failed to serialize value");

            store.insert(key_bytes, value_byte);
        }

        Ok(())
    }

    async fn delete<K>(
        &self,
        key: K,
        state_name: &str,
        partition: i32,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
    {
        let state_key = Self::derive_state_key(state_name, partition);

        let store = self.get_state_store(&state_key);

        let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");

        store.remove(&key_bytes);

        Ok(())
    }

    async fn clear<K>(
        &self,
        state_name: &str,
        partition: i32,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
    {
        let state_key = Self::derive_state_key(state_name, partition);

        if let Some(store) = self.store.get(&state_key) {
            store.clear()
        }

        Ok(())
    }
}
