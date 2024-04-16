use rdkafka::ClientConfig;

use crate::engine::util::{DeliveryGuaranteeType, ExactlyOnce};

use super::{config::PeridotConfig, error::PeridotAppCreationError, PeridotApp};

pub struct AppBuilder<C, G> {
    client_config: C,
    _delivery_guarantee: std::marker::PhantomData<G>,
}

impl AppBuilder<(), ()> {
    pub fn new() -> Self {
        Self {
            client_config: (),
            _delivery_guarantee: std::marker::PhantomData,
        }
    }
}

impl<C, G> AppBuilder<C, G> {
    pub fn with_delivery_guarantee<NG>(self) -> AppBuilder<C, NG>
    where
        NG: DeliveryGuaranteeType,
    {
        AppBuilder {
            client_config: self.client_config,
            _delivery_guarantee: std::marker::PhantomData,
        }
    }

    pub fn with_client_config(self, client_config: ClientConfig) -> AppBuilder<ClientConfig, G> {
        AppBuilder {
            client_config,
            _delivery_guarantee: std::marker::PhantomData,
        }
    }

    pub fn with_config(self, config: PeridotConfig) -> AppBuilder<PeridotConfig, G> {
        AppBuilder {
            client_config: config,
            _delivery_guarantee: std::marker::PhantomData,
        }
    }
}

impl AppBuilder<ClientConfig, ExactlyOnce> {
    pub fn build(self) -> Result<PeridotApp<ExactlyOnce>, PeridotAppCreationError> {
        let app = PeridotApp::from_client_config(&self.client_config)?;
        Ok(app)
    }
}

impl AppBuilder<PeridotConfig, ExactlyOnce> {
    pub fn build(self) -> Result<PeridotApp<ExactlyOnce>, PeridotAppCreationError> {
        let app = PeridotApp::from_config(self.client_config)?;
        Ok(app)
    }
}
