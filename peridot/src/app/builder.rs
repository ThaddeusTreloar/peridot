use rdkafka::ClientConfig;

use crate::{engine::util::{DeliveryGuaranteeType, ExactlyOnce}, state::backend::{in_memory::InMemoryStateBackend, StateBackend}};

use super::{config::PeridotConfig, error::PeridotAppCreationError, PeridotApp};

pub struct AppBuilder<C, B, G> {
    client_config: C,
    _delivery_guarantee: std::marker::PhantomData<G>,
    _state_backend: std::marker::PhantomData<B>
}

impl AppBuilder<(), (), ()> {
    pub fn new() -> Self {
        Self {
            client_config: (),
            _delivery_guarantee: Default::default(),
            _state_backend: Default::default(),
        }
    }
}

impl<C, B, G> AppBuilder<C, B, G> {
    pub fn with_delivery_guarantee<NG>(self) -> AppBuilder<C, B, NG>
    where
        NG: DeliveryGuaranteeType,
    {
        AppBuilder {
            client_config: self.client_config,
            _delivery_guarantee: Default::default(),
            _state_backend: Default::default(),
        }
    }

    pub fn with_state_backend<NB>(self) -> AppBuilder<C, NB, G>
    where
        NB: StateBackend,
    {
        AppBuilder {
            client_config: self.client_config,
            _delivery_guarantee: Default::default(),
            _state_backend: Default::default(),
        }
    }

    pub fn with_client_config(self, client_config: ClientConfig) -> AppBuilder<ClientConfig, B, G> {
        AppBuilder {
            client_config,
            _delivery_guarantee: Default::default(),
            _state_backend: Default::default(),
        }
    }

    pub fn with_config(self, config: PeridotConfig) -> AppBuilder<PeridotConfig, B, G> {
        AppBuilder {
            client_config: config,
            _delivery_guarantee: Default::default(),
            _state_backend: Default::default(),
        }
    }
}

impl AppBuilder<ClientConfig, (), ()> 
{
    pub fn build(self) -> Result<PeridotApp<InMemoryStateBackend, ExactlyOnce>, PeridotAppCreationError> {
        let app = PeridotApp::from_client_config(&self.client_config)?;
        Ok(app)
    }
}

impl<B> AppBuilder<ClientConfig, B, ()> 
where
    B: StateBackend + Send + Sync + 'static,
{
    pub fn build(self) -> Result<PeridotApp<B, ExactlyOnce>, PeridotAppCreationError> {
        let app = PeridotApp::from_client_config(&self.client_config)?;
        Ok(app)
    }
}

impl<G> AppBuilder<ClientConfig, (), G> 
where
    G: DeliveryGuaranteeType,
{
    pub fn build(self) -> Result<PeridotApp<InMemoryStateBackend, G>, PeridotAppCreationError> {
        let app = PeridotApp::from_client_config(&self.client_config)?;
        Ok(app)
    }
}

impl<B, G> AppBuilder<ClientConfig, B, G> 
where
    G: DeliveryGuaranteeType,
    B: StateBackend + Send + Sync + 'static,
{
    pub fn build(self) -> Result<PeridotApp<B, G>, PeridotAppCreationError> {
        let app = PeridotApp::from_client_config(&self.client_config)?;
        Ok(app)
    }
}

impl<B, G> AppBuilder<PeridotConfig, B, G> 
where
    G: DeliveryGuaranteeType,
    B: StateBackend + Send + Sync + 'static,
{
    pub fn build(self) -> Result<PeridotApp<B, G>, PeridotAppCreationError> {
        let app = PeridotApp::from_config(self.client_config)?;
        Ok(app)
    }
}
