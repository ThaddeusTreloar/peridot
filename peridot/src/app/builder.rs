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

use rdkafka::ClientConfig;

use crate::{
    engine::util::{DeliveryGuaranteeType, ExactlyOnce},
    state::store::{in_memory::InMemoryStateStore, StateStore},
};

use super::{config::PeridotConfig, error::PeridotAppCreationError, PeridotApp};

pub struct AppBuilder<C, B, G> {
    config: C,
    _delivery_guarantee: std::marker::PhantomData<G>,
    _state_backend: std::marker::PhantomData<B>,
}

impl Default for AppBuilder<(), (), ()> {
    fn default() -> Self {
        Self {
            config: (),
            _delivery_guarantee: Default::default(),
            _state_backend: Default::default(),
        }
    }
}

impl AppBuilder<(), (), ()> {
    pub fn new() -> Self {
        Default::default()
    }
}

impl<C, B, G> AppBuilder<C, B, G> {
    pub fn with_delivery_guarantee<NG>(self) -> AppBuilder<C, B, NG>
    where
        NG: DeliveryGuaranteeType + Send + Sync + 'static,
    {
        AppBuilder {
            config: self.config,
            _delivery_guarantee: Default::default(),
            _state_backend: Default::default(),
        }
    }

    pub fn with_state_backend<NB>(self) -> AppBuilder<C, NB, G>
    where
        NB: StateStore,
    {
        AppBuilder {
            config: self.config,
            _delivery_guarantee: Default::default(),
            _state_backend: Default::default(),
        }
    }

    pub fn with_config(self, config: PeridotConfig) -> AppBuilder<PeridotConfig, B, G> {
        AppBuilder {
            config,
            _delivery_guarantee: Default::default(),
            _state_backend: Default::default(),
        }
    }
}

impl AppBuilder<PeridotConfig, (), ()> {
    pub fn build(
        self,
    ) -> Result<PeridotApp<InMemoryStateStore, ExactlyOnce>, PeridotAppCreationError> {
        let app = PeridotApp::from_config(self.config)?;
        Ok(app)
    }
}

impl<B> AppBuilder<PeridotConfig, B, ()>
where
    B: StateStore + Send + Sync + 'static,
{
    pub fn build(self) -> Result<PeridotApp<B, ExactlyOnce>, PeridotAppCreationError> {
        let app = PeridotApp::from_config(self.config)?;
        Ok(app)
    }
}

impl<G> AppBuilder<PeridotConfig, (), G>
where
    G: DeliveryGuaranteeType + Send + Sync + 'static,
{
    pub fn build(self) -> Result<PeridotApp<InMemoryStateStore, G>, PeridotAppCreationError> {
        let app = PeridotApp::from_config(self.config)?;
        Ok(app)
    }
}

impl<B, G> AppBuilder<PeridotConfig, B, G>
where
    G: DeliveryGuaranteeType + Send + Sync + 'static,
    B: StateStore + Send + Sync + 'static,
{
    pub fn build(self) -> Result<PeridotApp<B, G>, PeridotAppCreationError> {
        let app = PeridotApp::from_config(self.config)?;
        Ok(app)
    }
}
