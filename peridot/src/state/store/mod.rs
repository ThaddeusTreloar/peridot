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
    sync::Arc,
    task::{Context, Poll, Waker},
};

use futures::Future;
use serde::{de::DeserializeOwned, Serialize};

use crate::message::types::PeridotTimestamp;

use super::checkpoint::Checkpoint;

pub mod error;
pub mod in_memory;
// TODO: find a better name for this module.
//pub mod state_connectors;
//pub mod persistent;

struct VersionedRecord<V> {
    pub value: V,
    pub timestamp: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum StateStoreError {
    #[error(transparent)]
    Fatal(Box<dyn std::error::Error + Send>),
    #[error(transparent)]
    Recoverable(Box<dyn std::error::Error + Send>),
}

#[trait_variant::make(Send + Sync)]
pub trait StateStore
where
    Self: Sized,
{
    fn with_source_topic_name_and_partition(
        topic_name: &str,
        partition: i32,
    ) -> Result<Self, StateStoreError>;

    fn init_state(
        &self,
        topic_name: &str,
        state_name: &str,
        partition: i32,
    ) -> Result<(), StateStoreError>;

    fn get_state_store_time(&self, store_name: &str, partition: i32) -> i64;

    fn get_state_store_checkpoint(&self, store_name: &str, partition: i32) -> Option<Checkpoint>;

    fn create_checkpoint(
        &self,
        store_name: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), StateStoreError>;

    async fn get<K, V>(
        &self,
        key: &K,
        store: &str,
        partition: i32,
    ) -> Result<Option<V>, StateStoreError>
    where
        K: Serialize + Send + Sync,
        V: DeserializeOwned;

    async fn put<K, V>(
        &self,
        key: &K,
        value: &V,
        store: &str,
        partition: i32,
        offset: i64,
        timestamp: i64,
    ) -> Result<(), StateStoreError>
    where
        K: Serialize + Send + Sync,
        V: Serialize + Send + Sync;

    /// offset and timestamp are the hghest for this batch
    async fn put_range<K, V>(
        &self,
        range: Vec<(K, V)>,
        store: &str,
        partition: i32,
        offset: i64,
        timestamp: i64,
    ) -> Result<(), StateStoreError>
    where
        K: Serialize + Send + Sync,
        V: Serialize + Send + Sync;

    async fn delete<K>(&self, key: &K, store: &str, partition: i32) -> Result<(), StateStoreError>
    where
        K: Serialize + Send + Sync;

    async fn clear<K>(&self, store: &str, partition: i32) -> Result<(), StateStoreError>
    where
        K: Serialize + Send;
}

pub trait VersionedStateStore {
    type Error: std::error::Error;

    fn get_version<K, V>(
        self: Arc<Self>,
        key: K,
        store: Arc<String>,
        at_timestamp: i64,
    ) -> impl Future<Output = Result<Option<VersionedRecord<V>>, StateStoreError>> + Send
    where
        K: Serialize,
        V: DeserializeOwned;
    fn get_latest_version<K, V>(
        self: Arc<Self>,
        key: K,
        store: Arc<String>,
    ) -> impl Future<Output = Result<Option<VersionedRecord<V>>, StateStoreError>> + Send
    where
        K: Serialize,
        V: DeserializeOwned;
    fn put_version<K, V>(
        self: Arc<Self>,
        key: K,
        value: V,
        store: Arc<String>,
        timestamp: i64,
    ) -> impl Future<Output = Result<(), StateStoreError>> + Send
    where
        K: Serialize,
        V: DeserializeOwned;
    fn put_range_version<K, V>(
        self: Arc<Self>,
        range: Vec<(K, V, i64)>,
        store: Arc<String>,
    ) -> impl Future<Output = Result<(), StateStoreError>> + Send
    where
        K: Serialize,
        V: DeserializeOwned;
    fn delete_version<K>(
        self: Arc<Self>,
        key: K,
        store: Arc<String>,
        timestamp: i64,
    ) -> impl Future<Output = Result<(), StateStoreError>> + Send
    where
        K: Serialize;
}
