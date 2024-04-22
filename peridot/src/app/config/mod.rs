use std::collections::{HashMap, HashSet};

use rdkafka::ClientConfig;
use tracing::warn;

use crate::help;

use self::builder::{PeridotConfigBuilder, PeridotConfigError};

pub mod builder;

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
    pub fn client_config(&self) -> &ClientConfig {
        &self.client_config
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.client_config().get(key)
    }
}

impl TryFrom<&ClientConfig> for PeridotConfig {
    type Error = PeridotConfigError;

    fn try_from(clients_config: &ClientConfig) -> Result<Self, Self::Error> {
        PeridotConfigBuilder::from(clients_config).build()
    }
}
