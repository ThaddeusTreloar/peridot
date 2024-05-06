use std::{
    borrow::{Borrow, BorrowMut},
    collections::HashMap,
    sync::atomic::AtomicI64,
    task::Waker,
};

use dashmap::{mapref::one::Ref, DashMap};
use serde::{de::DeserializeOwned, Serialize};
use tracing::info;

use crate::message::types::PeridotTimestamp;

use super::{Checkpoint, StateBackend};

#[derive(Default)]
pub struct InMemoryStateBackend {
    store: DashMap<String, DashMap<Vec<u8>, Vec<u8>>>,
    checkpoint: DashMap<String, Checkpoint>,
    // See notes here about compare and swap operations:
    // https://doc.rust-lang.org/std/sync/atomic/
    // Certain architectures may not support this implementation
    store_times: DashMap<String, AtomicI64>,
    // TODO: must ensure that these waker are woken, when the parent
    // sink is polled and returns pending.
    wakers: DashMap<String, Vec<Waker>>,
}

impl InMemoryStateBackend {
    fn derive_state_key(store_name: &str, partition: i32) -> String {
        format!("{}-{}", store_name, partition)
    }

    fn get_state_store(&self, state_key: &str) -> Ref<String, DashMap<Vec<u8>, Vec<u8>>> {
        match self.store.get(state_key) {
            None => {
                self.store.insert(state_key.to_owned(), Default::default());

                self.store
                    .get(state_key)
                    .expect("THIS SHOULD BE IMPOSSIBLE.")
            }
            Some(state) => state,
        }
    }

    fn update_store_time(&self, store_name: &str, partition: i32, time: i64) {
        let key = Self::derive_state_key(store_name, partition);

        match self.store_times.get(&key) {
            Some(store_time) => {
                store_time.fetch_max(time, std::sync::atomic::Ordering::SeqCst);
            }
            None => {
                self.store_times.insert(key, AtomicI64::from(time));
            }
        };
    }

    fn get_store_time(&self, store_name: &str, partition: i32) -> i64 {
        let key = Self::derive_state_key(store_name, partition);

        self.store_times
            .get(&key)
            .unwrap()
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn insert_waker(&self, store_name: &str, partition: i32, waker: Waker) {
        let key = Self::derive_state_key(store_name, partition);

        match self.wakers.get_mut(&key) {
            Some(mut wakers) => wakers.push(waker),
            None => todo!(""),
        }
    }

    fn wake_all(&self, store_name: &str, partition: i32) {
        let key = Self::derive_state_key(store_name, partition);

        match self.wakers.get_mut(&key) {
            Some(mut wakers) => wakers.drain(..).for_each(|w| w.wake()),
            None => todo!(""),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InMemoryStateBackendError {}

impl StateBackend for InMemoryStateBackend {
    type Error = InMemoryStateBackendError;

    fn wake(&self, topic: &str, partition: i32) {
        self.wake_all(topic, partition);
    }

    fn with_source_topic_name_and_partition(
        _topic_name: &str,
        _partition: i32,
    ) -> Result<Self, Self::Error> {
        Ok(Default::default())
    }

    fn get_state_store_time(&self, store_name: &str, partition: i32) -> PeridotTimestamp {
        //self.store_time.clone()
        todo!("")
    }

    fn poll_lag(&self, store_name: &str, partition: i32, waker: Waker) -> bool {
        todo!("")
    }

    fn get_state_store_checkpoint(&self, store_name: &str, partition: i32) -> Option<Checkpoint> {
        let checkpoint_name = format!("{}-{}", store_name, partition);

        Some(self.checkpoint.get(&checkpoint_name)?.clone())
    }

    fn create_checkpoint(
        &self,
        store_name: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), Self::Error> {
        let checkpoint_name = format!("{}-{}", store_name, partition);

        match self.checkpoint.get_mut(&checkpoint_name) {
            None => {
                self.checkpoint.insert(
                    checkpoint_name.clone(),
                    Checkpoint {
                        store_name: checkpoint_name,
                        offset,
                    },
                );

                Ok(())
            }
            Some(mut checkpoint_ref) => {
                checkpoint_ref.value_mut().set_offset_if_greater(offset);

                Ok(())
            }
        }
    }

    async fn get<K, V>(
        &self,
        key: K,
        store_name: &str,
        partition: i32,
    ) -> Result<Option<V>, Self::Error>
    where
        K: Serialize + Send,
        V: DeserializeOwned,
    {
        let state_key = Self::derive_state_key(store_name, partition);

        let store = self.get_state_store(&state_key);

        let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");

        let value = match store.get(&key_bytes) {
            None => return Ok(None),
            Some(value_bytes) => {
                bincode::deserialize(value_bytes.as_ref()).expect("Failed to deserialize value")
            }
        };

        Ok(Some(value))
    }

    async fn put<K, V>(
        &self,
        key: K,
        value: V,
        store_name: &str,
        partition: i32,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
        V: Serialize + Send,
    {
        let state_key = Self::derive_state_key(store_name, partition);

        let store = self.get_state_store(&state_key);

        let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");
        let value_byte = bincode::serialize(&value).expect("Failed to serialize value");

        // compute lag
        // if lag is acceptable, drain and wake wakers.

        store.insert(key_bytes, value_byte);

        Ok(())
    }

    async fn put_range<K, V>(
        &self,
        range: Vec<(K, V)>,
        store_name: &str,
        partition: i32,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
        V: Serialize + Send,
    {
        let state_key = Self::derive_state_key(store_name, partition);

        self.store
            .iter()
            .for_each(|e| tracing::debug!("Key in stores: {}", e.key()));

        for (key, value) in range {
            let store = self.get_state_store(&state_key);

            let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");
            let value_byte = bincode::serialize(&value).expect("Failed to serialize value");

            store.insert(key_bytes, value_byte);
        }

        Ok(())
    }

    async fn delete<K>(&self, key: K, store_name: &str, partition: i32) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
    {
        let state_key = Self::derive_state_key(store_name, partition);

        let store = self.get_state_store(&state_key);

        let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");

        store.remove(&key_bytes);

        Ok(())
    }

    async fn clear<K>(&self, store_name: &str, partition: i32) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
    {
        let state_key = Self::derive_state_key(store_name, partition);

        if let Some(store) = self.store.get(&state_key) {
            store.clear()
        }

        Ok(())
    }
}
