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

use super::{checkpoint::Checkpoint, VersionedRecord};

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

impl<K, V, T> ReadWriteStateFacade<K, V> for T where
    T: ReadableStateFacade<KeyType = K, ValueType = V>
        + WriteableStateFacade<KeyType = K, ValueType = V>
{
}
