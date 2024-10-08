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

use std::collections::{HashMap, HashSet};

use rdkafka::ClientConfig;
use tracing::warn;

use crate::help;

use self::builder::{
    PeridotConfigBuilder, PeridotConfigError, PERIDOT_FIELDS, GROUP_ID, GROUP_INSTANCE_ID,
};

pub mod builder;
mod persistent_config;

#[derive(Default, Debug, Clone)]
pub struct PeridotConfig {
    client_config: ClientConfig,
    app_config: HashMap<String, String>,
}

impl From<PeridotConfigBuilder> for PeridotConfig {
    fn from(value: PeridotConfigBuilder) -> Self {
        let PeridotConfigBuilder {
            client_config,
            app_config,
        } = value;

        Self {
            client_config,
            app_config,
        }
    }
}

impl PeridotConfig {
    pub(crate) fn without_group_id(mut self) -> PeridotConfig {
        self.client_config.remove(GROUP_ID);
        self.client_config.remove(GROUP_INSTANCE_ID);

        self
    }

    pub(crate) fn with_earliest_offset_reset(mut self) -> PeridotConfig {
        self.client_config.set("auto.offset.reset", "earliest");

        self
    }

    pub(crate) fn without_client_statistics(mut self) -> PeridotConfig {
        self.client_config.set("statistics.interval.ms", "0");

        self
    }

    pub fn new_client_config(&self) -> ClientConfig {
        self.client_config.clone()
    }

    pub fn client_config(&self) -> &ClientConfig {
        &self.client_config
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        if PERIDOT_FIELDS.contains(&key) {
            self.app_config.get(key).map(|s| s.as_str())
        } else {
            self.client_config.get(key)
        }
    }

    pub fn app_id(&self) -> &str {
        self.get("application.id").expect(
            "Failed to get 'application.id' from PeridotConfig. This should not be possible",
        )
    }
}

impl TryFrom<&ClientConfig> for PeridotConfig {
    type Error = PeridotConfigError;

    fn try_from(clients_config: &ClientConfig) -> Result<Self, Self::Error> {
        PeridotConfigBuilder::from(clients_config).build()
    }
}
