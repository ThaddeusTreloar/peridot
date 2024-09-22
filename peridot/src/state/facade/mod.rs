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

use crate::message::{
    state_fork::StoreStateCell,
    types::{Message, PeridotTimestamp},
};

use super::{
    checkpoint::Checkpoint,
    facade::{FacadeDistributor, StateStoreFacade}, store::StateStoreError,
};

pub(crate) mod facade_distributor;
pub(crate) mod state_facade;

pub(crate) use facade_distributor::*;
pub use state_facade::*;

#[derive(Debug, thiserror::Error)]
pub enum FacadeError {
    #[error(transparent)]
    Fatal(Box<dyn std::error::Error + Send>),
    #[error(transparent)]
    Recoverable(Box<dyn std::error::Error + Send>),
}

impl From<StateStoreError> for FacadeError {
    fn from(value: StateStoreError) -> Self {
        match value {
            StateStoreError::Fatal(e) => FacadeError::Fatal(e),
            StateStoreError::Recoverable(e) => FacadeError::Recoverable(e),
        }
    }
}

pub trait GetFacadeDistributor {
    type KeyType;
    type ValueType;
    type Backend;

    fn get_facade_distributor(
        &self,
    ) -> Result<FacadeDistributor<Self::KeyType, Self::ValueType, Self::Backend>, FacadeError>;
}

pub trait GetFacade {
    type KeyType;
    type ValueType;
    type Backend;

    fn get_facade(
        &self,
        partition: i32,
    ) -> Result<StateStoreFacade<Self::KeyType, Self::ValueType, Self::Backend>, FacadeError>;
}

#[trait_variant::make(Send)]
pub trait ReadableStateFacade {
    type KeyType: Serialize + Send;
    type ValueType: DeserializeOwned + Send;

    async fn get(
        self: Arc<Self>,
        key: Self::KeyType,
    ) -> Result<Option<Self::ValueType>, FacadeError>;

    fn poll_time(&self, time: i64, cx: &mut Context<'_>) -> Poll<i64>;

    fn get_checkpoint(&self) -> Result<Option<Checkpoint>, FacadeError>;

    fn get_stream_state(&self) -> Option<Arc<StoreStateCell>>;
}

#[trait_variant::make(Send)]
pub trait WriteableStateFacade {
    // TODO: Considering how timestamps will be passed to the facade
    // Do we pass a whole message for the facade to extract?
    // or do we add a timestamp field.
    type KeyType: Serialize + Send;
    type ValueType: Serialize + Send;

    async fn put(
        self: Arc<Self>,
        message: Message<Self::KeyType, Self::ValueType>,
    ) -> Result<(), FacadeError>;

    async fn put_range(
        self: Arc<Self>,
        range: Vec<Message<Self::KeyType, Self::ValueType>>,
    ) -> Result<(), FacadeError>;

    async fn delete(self: Arc<Self>, key: Self::KeyType) -> Result<(), FacadeError>;

    fn create_checkpoint(self: Arc<Self>, consumer_position: i64) -> Result<(), FacadeError>;

    fn wake(&self);
    fn wake_all(&self);
}

pub trait ReadWriteStateFacade<K, V>:
    ReadableStateFacade<KeyType = K, ValueType = V> + WriteableStateFacade<KeyType = K, ValueType = V>
{
}
