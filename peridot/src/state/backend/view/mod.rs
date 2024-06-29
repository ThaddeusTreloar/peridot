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

use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};
use state_view::StateView;
use view_distributor::ViewDistributor;

pub mod state_view;
pub mod view_distributor;

pub trait GetViewDistributor {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;
    type Backend;

    fn get_view_distributor(
        &self,
    ) -> ViewDistributor<Self::KeyType, Self::ValueType, Self::Backend>;
}

pub trait GetView {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;
    type Backend;

    fn get_view(&self, partition: i32) -> StateView<Self::KeyType, Self::ValueType, Self::Backend>;
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
}
