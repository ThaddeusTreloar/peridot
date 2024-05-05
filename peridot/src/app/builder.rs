use rdkafka::ClientConfig;

use crate::{
    engine::util::{DeliveryGuaranteeType, ExactlyOnce},
    state::backend::{in_memory::InMemoryStateBackend, StateBackend},
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
        NG: DeliveryGuaranteeType,
    {
        AppBuilder {
            config: self.config,
            _delivery_guarantee: Default::default(),
            _state_backend: Default::default(),
        }
    }

    pub fn with_state_backend<NB>(self) -> AppBuilder<C, NB, G>
    where
        NB: StateBackend,
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
    ) -> Result<PeridotApp<InMemoryStateBackend, ExactlyOnce>, PeridotAppCreationError> {
        let app = PeridotApp::from_config(self.config)?;
        Ok(app)
    }
}

impl<B> AppBuilder<PeridotConfig, B, ()>
where
    B: StateBackend + Send + Sync + 'static,
{
    pub fn build(self) -> Result<PeridotApp<B, ExactlyOnce>, PeridotAppCreationError> {
        let app = PeridotApp::from_config(self.config)?;
        Ok(app)
    }
}

impl<G> AppBuilder<PeridotConfig, (), G>
where
    G: DeliveryGuaranteeType,
{
    pub fn build(self) -> Result<PeridotApp<InMemoryStateBackend, G>, PeridotAppCreationError> {
        let app = PeridotApp::from_config(self.config)?;
        Ok(app)
    }
}

impl<B, G> AppBuilder<PeridotConfig, B, G>
where
    G: DeliveryGuaranteeType,
    B: StateBackend + Send + Sync + 'static,
{
    pub fn build(self) -> Result<PeridotApp<B, G>, PeridotAppCreationError> {
        let app = PeridotApp::from_config(self.config)?;
        Ok(app)
    }
}
