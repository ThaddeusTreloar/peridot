use std::collections::{HashMap, HashSet};

use rdkafka::ClientConfig;
use tracing::warn;

use crate::help;

use self::builder::{PeridotConfigBuilder, PeridotConfigError};

pub mod builder;
mod persistent_config;

#[derive(Default, Debug, Clone)]
pub struct PeridotConfig {
    client_config: ClientConfig,
}

impl From<PeridotConfigBuilder> for PeridotConfig {
    fn from(value: PeridotConfigBuilder) -> Self {
        let PeridotConfigBuilder { client_config } = value;

        Self { client_config }
    }
}

impl PeridotConfig {
    pub fn new_client_config(&self) -> ClientConfig {
        self.client_config.clone()
    }

    pub fn client_config_ref(&self) -> &ClientConfig {
        &self.client_config
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.client_config_ref().get(key)
    }

    pub fn app_id(&self) -> &str {
        self.client_config.get("app.id")
            .expect("Failed to get 'app.id' from PeridotConfig. This should not be possible")
    }
}

impl TryFrom<&ClientConfig> for PeridotConfig {
    type Error = PeridotConfigError;

    fn try_from(clients_config: &ClientConfig) -> Result<Self, Self::Error> {
        PeridotConfigBuilder::from(clients_config).build()
    }
}
