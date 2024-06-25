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
    facade::{FacadeDistributor, StateFacade},
    VersionedRecord,
};

pub trait GetViewDistributor {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;
    type Backend;

    fn get_view_distributor(
        &self,
    ) -> FacadeDistributor<Self::KeyType, Self::ValueType, Self::Backend>;
}

pub trait GetView {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;
    type Backend;

    fn get_view(
        &self,
        partition: i32,
    ) -> StateFacade<Self::KeyType, Self::ValueType, Self::Backend>;
}

#[trait_variant::make(Send)]
pub trait ReadableStateView {
    type Error: std::error::Error;
    type KeyType: Serialize + Send;
    type ValueType: DeserializeOwned + Send;

    async fn get(
        self: Arc<Self>,
        key: Self::KeyType,
    ) -> Result<Option<Self::ValueType>, Self::Error>;

    fn poll_time(&self, time: i64, cx: &mut Context<'_>) -> Poll<i64>;

    fn get_checkpoint(&self) -> Result<Option<Checkpoint>, Self::Error>;

    fn get_stream_state(&self) -> Option<Arc<StoreStateCell>>;
}

#[trait_variant::make(Send)]
pub trait WriteableStateView {
    // TODO: Considering how timestamps will be passed to the facade
    // Do we pass a whole message for the facade to extract?
    // or do we add a timestamp field.
    type Error: std::error::Error;
    type KeyType: Serialize + Send;
    type ValueType: Serialize + Send;

    async fn put(
        self: Arc<Self>,
        message: Message<Self::KeyType, Self::ValueType>,
    ) -> Result<(), Self::Error>;

    async fn put_range(
        self: Arc<Self>,
        range: Vec<Message<Self::KeyType, Self::ValueType>>,
    ) -> Result<(), Self::Error>;

    async fn delete(self: Arc<Self>, key: Self::KeyType) -> Result<(), Self::Error>;

    fn create_checkpoint(self: Arc<Self>, consumer_position: i64) -> Result<(), Self::Error>;

    fn wake(&self);
    fn wake_all(&self);
}

#[trait_variant::make(Send)]
pub trait ReadableVersionedStateView {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;

    async fn get_version(
        self: Arc<Self>,
        key: Self::KeyType,
        at_timestamp: i64,
    ) -> Result<Option<VersionedRecord<Self::ValueType>>, Self::Error>;
}

pub trait WriteableVersionedStateView {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;

    fn put_version(
        self: Arc<Self>,
        key: Self::KeyType,
        value: Self::ValueType,
        timestamp: i64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn put_version_range(
        self: Arc<Self>,
        range: Vec<(Self::KeyType, Self::ValueType, i64)>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn delete_version(
        self: Arc<Self>,
        key: Self::KeyType,
        timestamp: i64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait ReadWriteStateView<K, V>:
    ReadableStateView<KeyType = K, ValueType = V> + WriteableStateView<KeyType = K, ValueType = V>
{
}

impl<K, V, T> ReadWriteStateView<K, V> for T where
    T: ReadableStateView<KeyType = K, ValueType = V>
        + WriteableStateView<KeyType = K, ValueType = V>
{
}
