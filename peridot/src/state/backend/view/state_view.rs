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
    state::backend::{Checkpoint, StateBackend},
};

use super::ReadableStateView;

pub struct StateView<K, V, B> {
    backend_manager: Arc<StateStoreManager<B>>,
    backend: Arc<B>,
    store_name: String,
    partition: i32,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<K, V, B> StateView<K, V, B> {
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

impl<K, V, B> ReadableStateView for StateView<K, V, B>
where
    B: StateBackend + Send + Sync + 'static,
    K: Serialize + Send + Sync,
    V: DeserializeOwned + Send + Sync,
{
    type Error = B::Error;
    type KeyType = K;
    type ValueType = V;

    async fn get(
        self: Arc<Self>,
        key: Self::KeyType,
    ) -> Result<Option<Self::ValueType>, Self::Error> {
        self.backend
            .get(&key, self.store_name(), self.partition())
            .await
    }
}
