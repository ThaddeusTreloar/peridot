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

use crate::{
    engine::{
        context::EngineContext, metadata_manager::table_metadata,
        state_store_manager::StateStoreManager, util::DeliveryGuaranteeType, AppEngine,
    },
    state::backend::{StateBackend, VersionedStateBackend},
};

use super::{GetFacade, StateFacade};

pub struct FacadeDistributor<K, V, B> {
    engine_context: Arc<EngineContext>,
    state_store_manager: Arc<StateStoreManager<B>>,
    store_name: String,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<K, V, B> FacadeDistributor<K, V, B>
where
    B: StateBackend + Send + Sync + 'static,
{
    pub fn new<G>(engine: &AppEngine<B, G>, store_name: String) -> Self
    where
        G: DeliveryGuaranteeType + Send + Sync + 'static,
    {
        Self {
            engine_context: engine.engine_context(),
            state_store_manager: engine.state_store_context(),
            store_name,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub fn fetch_backend(&self, partition: i32) -> Arc<B> {
        let table_metadata = self.engine_context.store_metadata(&self.store_name);

        self.state_store_manager
            .get_state_store(table_metadata.source_topic(), partition)
            .expect("Failed to get state store for facade distributor.")
    }

    pub fn store_name(&self) -> &str {
        &self.store_name
    }
}

impl<K, V, B> GetFacade for FacadeDistributor<K, V, B>
where
    B: StateBackend + Send + Sync + 'static,
{
    type Error = B::Error;
    type KeyType = K;
    type ValueType = V;
    type Backend = B;

    fn get_facade(
        &self,
        partition: i32,
    ) -> StateFacade<Self::KeyType, Self::ValueType, Self::Backend> {
        let backend = self.fetch_backend(partition);

        StateFacade::new(
            backend,
            self.state_store_manager.clone(),
            self.store_name().to_owned(),
            partition,
        )
    }
}
