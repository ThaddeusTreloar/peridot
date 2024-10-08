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
    task::{Context, Poll},
};

use serde::{de::DeserializeOwned, Serialize};
use tracing::info;

use crate::{
    engine::state_store_manager::StateStoreManager,
    message::{state_fork::StoreStateCell, types::Message},
    state::{checkpoint::Checkpoint, store::StateStore},
};

use super::{FacadeError, ReadableStateFacade, WriteableStateFacade};

pub struct StateStoreFacade<K, V, B> {
    backend_manager: Arc<StateStoreManager<B>>,
    backend: Arc<B>,
    store_name: String,
    partition: i32,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<K, V, B> StateStoreFacade<K, V, B> {
    pub(crate) fn new(
        backend: Arc<B>,
        backend_manager: Arc<StateStoreManager<B>>,
        store_name: String,
        partition: i32,
    ) -> Self {
        Self {
            backend,
            backend_manager,
            store_name,
            partition,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub fn backend(&self) -> Arc<B> {
        self.backend.clone()
    }

    pub fn store_name(&self) -> &str {
        &self.store_name
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }
}

impl<K, V, B> ReadableStateFacade for StateStoreFacade<K, V, B>
where
    B: StateStore + Send + Sync + 'static,
    K: Serialize + Send + Sync,
    V: DeserializeOwned + Send + Sync,
{
    type KeyType = K;
    type ValueType = V;

    async fn get(
        self: Arc<Self>,
        key: Self::KeyType,
    ) -> Result<Option<Self::ValueType>, FacadeError> {
        Ok(self.backend
            .get(&key, self.store_name(), self.partition())
            .await?)
    }

    fn poll_time(&self, time: i64, cx: &mut Context<'_>) -> Poll<i64> {
        let state_time = self
            .backend
            .get_state_store_time(self.store_name(), self.partition());

        if state_time >= time {
            Poll::Ready(state_time)
        } else {
            let waker = cx.waker().clone();

            self.backend_manager
                .store_waker(self.store_name(), self.partition(), time, waker);

            Poll::Pending
        }
    }

    fn get_checkpoint(&self) -> Result<Option<Checkpoint>, FacadeError> {
        let checkpoint = self
            .backend
            .get_state_store_checkpoint(self.store_name(), self.partition());

        Ok(checkpoint)
    }

    fn get_stream_state(&self) -> Option<Arc<StoreStateCell>> {
        self.backend_manager
            .get_stream_state(self.store_name(), self.partition())
    }
}

impl<K, V, B> WriteableStateFacade for StateStoreFacade<K, V, B>
where
    B: StateStore + Send + Sync + 'static,
    K: Serialize + Send + Sync,
    V: Serialize + Send + Sync,
    Self: Send,
{
    type KeyType = K;
    type ValueType = V;

    async fn put(
        self: Arc<Self>,
        message: Message<Self::KeyType, Self::ValueType>,
    ) -> Result<(), FacadeError> {
        Ok(self.backend
            .put(
                &message.key,
                &message.value,
                self.store_name(),
                self.partition(),
                message.offset,
                message.timestamp.into(),
            )
            .await?)
    }

    async fn put_range(
        self: Arc<Self>,
        range: Vec<Message<Self::KeyType, Self::ValueType>>,
    ) -> Result<(), FacadeError> {
        if range.is_empty() {
            return Ok(());
        }

        let offset = range
            .iter()
            .map(|m| m.offset())
            .reduce(std::cmp::max)
            .unwrap();

        let timestamp = range
            .iter()
            .map(|m| Into::<i64>::into(m.timestamp()))
            .reduce(std::cmp::max)
            .unwrap();

        let data = range.into_iter().map(|m| (m.key, m.value)).collect();

        Ok(self.backend
            .put_range(data, self.store_name(), self.partition(), offset, timestamp)
            .await?)
    }

    async fn delete(self: Arc<Self>, key: Self::KeyType) -> Result<(), FacadeError> {
        Ok(self.backend
            .delete(&key, self.store_name(), self.partition())
            .await?)
    }

    fn create_checkpoint(self: Arc<Self>, consumer_position: i64) -> Result<(), FacadeError> {
        Ok(self.backend
            .create_checkpoint(self.store_name(), self.partition(), consumer_position)?)
    }

    fn wake(&self) {
        let state_time = self
            .backend
            .get_state_store_time(self.store_name(), self.partition());

        self.backend_manager
            .wake_for_time(self.store_name(), self.partition(), state_time);
    }

    fn wake_all(&self) {
        self.backend_manager
            .wake_all(self.store_name(), self.partition());
    }
}
