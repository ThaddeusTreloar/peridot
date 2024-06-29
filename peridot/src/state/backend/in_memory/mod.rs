/*
 * Copyright 2024 Thaddeus Treloar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

use std::{
    borrow::{Borrow, BorrowMut},
    collections::HashMap,
    sync::atomic::AtomicI64,
    task::{Context, Poll, Waker},
};

use dashmap::{
    mapref::one::{Ref, RefMut},
    DashMap,
};
use serde::{de::DeserializeOwned, Serialize};
use tracing::{debug, info};

use crate::message::types::PeridotTimestamp;

use super::{Checkpoint, StateBackend};

struct TimestampedWaker {
    time: i64,
    waker: Waker,
}

impl TimestampedWaker {
    fn new(time: i64, waker: Waker) -> Self {
        Self { time, waker }
    }

    fn wake(self) {
        self.waker.wake()
    }

    fn wake_by_ref(&self) {
        self.waker.wake_by_ref()
    }
}

impl PartialEq<Self> for TimestampedWaker {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

impl Eq for TimestampedWaker {}

impl PartialOrd<Self> for TimestampedWaker {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimestampedWaker {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time.cmp(&other.time)
    }
}

impl PartialEq<i64> for TimestampedWaker {
    fn eq(&self, other: &i64) -> bool {
        &self.time == other
    }
}

impl PartialOrd<i64> for TimestampedWaker {
    fn partial_cmp(&self, other: &i64) -> Option<std::cmp::Ordering> {
        Some(self.time.cmp(other))
    }
}

#[derive(Default)]
pub struct InMemoryStateBackend {
    stores: DashMap<String, DashMap<Vec<u8>, Vec<u8>>>,
    checkpoint: DashMap<String, Checkpoint>,
    // See notes here about compare and swap operations:
    // https://doc.rust-lang.org/std/sync/atomic/
    // Certain architectures may not support this implementation
    store_times: DashMap<String, AtomicI64>,
}

impl InMemoryStateBackend {
    fn derive_state_key(store_name: &str, partition: i32) -> String {
        format!("{}-{}", store_name, partition)
    }

    fn create_state_store(&self, state_key: &str) {
        match self.stores.get(state_key) {
            None => {
                self.store_times
                    .insert(state_key.to_owned(), AtomicI64::from(0));

                self.stores.insert(state_key.to_owned(), Default::default());

                self.checkpoint
                    .insert(state_key.to_owned(), Default::default());
            }
            Some(_) => (),
        }
    }

    fn get_state_store(&self, state_key: &str) -> Ref<String, DashMap<Vec<u8>, Vec<u8>>> {
        match self.stores.get(state_key) {
            None => panic!("State store doesn't exists"),
            Some(state) => state,
        }
    }

    fn get_mut_store(
        &self,
        store_name: &str,
        partition: i32,
    ) -> RefMut<'_, String, DashMap<Vec<u8>, Vec<u8>>> {
        let key = Self::derive_state_key(store_name, partition);

        self.stores.get_mut(&key).unwrap()
    }

    fn get_mut_store_times(
        &self,
        store_name: &str,
        partition: i32,
    ) -> RefMut<'_, String, AtomicI64> {
        let key = Self::derive_state_key(store_name, partition);

        self.store_times.get_mut(&key).unwrap()
    }

    fn get_store(
        &self,
        store_name: &str,
        partition: i32,
    ) -> Ref<'_, String, DashMap<Vec<u8>, Vec<u8>>> {
        let key = Self::derive_state_key(store_name, partition);

        self.stores.get(&key).unwrap()
    }

    fn get_store_times(&self, store_name: &str, partition: i32) -> Ref<'_, String, AtomicI64> {
        let key = Self::derive_state_key(store_name, partition);

        self.store_times.get(&key).unwrap()
    }

    fn update_store_time(&self, store_name: &str, partition: i32, time: i64) {
        let key = Self::derive_state_key(store_name, partition);

        match self.store_times.get(&key) {
            Some(store_time) => {
                let old = store_time.fetch_max(time, std::sync::atomic::Ordering::SeqCst);
                tracing::debug!("Updating store time: old: {}, new: {}", old, time);
            }
            None => {
                self.store_times.insert(key, AtomicI64::from(time));
            }
        };
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InMemoryStateBackendError {}

impl StateBackend for InMemoryStateBackend {
    type Error = InMemoryStateBackendError;

    fn with_source_topic_name_and_partition(
        _topic_name: &str,
        _partition: i32,
    ) -> Result<Self, Self::Error> {
        Ok(Default::default())
    }

    fn init_state(
        &self,
        _topic_name: &str,
        store_name: &str,
        partition: i32,
    ) -> Result<(), Self::Error> {
        let state_key = Self::derive_state_key(store_name, partition);

        self.create_state_store(&state_key);

        Ok(())
    }

    fn get_state_store_time(&self, store_name: &str, partition: i32) -> i64 {
        let key = Self::derive_state_key(store_name, partition);

        match self.store_times.get(&key) {
            Some(time) => time.load(std::sync::atomic::Ordering::SeqCst),
            None => 0,
        }
    }

    fn get_state_store_checkpoint(&self, store_name: &str, partition: i32) -> Option<Checkpoint> {
        let checkpoint_name = Self::derive_state_key(store_name, partition);

        Some(self.checkpoint.get(&checkpoint_name)?.clone())
    }

    fn create_checkpoint(
        &self,
        store_name: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), Self::Error> {
        let checkpoint_name = Self::derive_state_key(store_name, partition);

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
            None => {
                return Ok(None);
            }
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
        offset: i64,
        timestamp: i64,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
        V: Serialize + Send,
    {
        let state_key = Self::derive_state_key(store_name, partition);

        let store = self.get_state_store(&state_key);

        let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");
        let value_byte = bincode::serialize(&value).expect("Failed to serialize value");

        store.insert(key_bytes, value_byte);

        self.update_store_time(store_name, partition, timestamp);

        Ok(())
    }

    async fn put_range<K, V>(
        &self,
        range: Vec<(K, V)>,
        store_name: &str,
        partition: i32,
        offset: i64,
        timestamp: i64,
    ) -> Result<(), Self::Error>
    where
        K: Serialize + Send,
        V: Serialize + Send,
    {
        let state_key = Self::derive_state_key(store_name, partition);

        self.stores
            .iter()
            .for_each(|e| tracing::debug!("Key in stores: {}", e.key()));

        for (key, value) in range {
            let store = self.get_state_store(&state_key);

            let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");
            let value_byte = bincode::serialize(&value).expect("Failed to serialize value");

            store.insert(key_bytes, value_byte);
        }

        self.update_store_time(store_name, partition, timestamp);

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

        if let Some(store) = self.stores.get(&state_key) {
            store.clear()
        }

        Ok(())
    }
}
