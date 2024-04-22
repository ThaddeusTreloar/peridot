use std::collections::{HashMap, HashSet};

use rdkafka::ClientConfig;
use tracing::warn;

use crate::help;

use super::PeridotConfig;

const REQUIRED_FIELDS: [&str; 2] = [
    "bootstrap.servers",
    "application.id"
];

const DEFAULT_FIELDS: [(&str, &str); 0] = [

];

#[derive(Debug, Clone, Default, derive_more::From)]
pub struct PeridotConfigBuilder {
    pub(crate) client_config: ClientConfig,
}

#[derive(Debug, thiserror::Error)]
pub enum PeridotConfigError {
    #[error("PeridotConfigError::MissingConfig")]
    MissingConfig {
        configs: Vec<&'static str>
    }
}

impl PeridotConfigBuilder {
    pub fn new() -> Self {
        Self { 
            ..Default::default()
        }
    }

    pub fn get<'a, K: Into<&'a str>>(&mut self, key: K) -> Option<&str> {
        self.client_config.get(key.into())
    }
    
    pub fn set<K: Into<String>, V: Into<String>>(&mut self, key: K, value: V) -> &mut Self {
        unimplemented!("")
    }

    pub fn remove<K: Into<String>>(&mut self, key: K) -> &mut Self {
        unimplemented!("")
    }

    fn prep_config(mut self) -> Result<Self, PeridotConfigError> {
        let missing_fields: Vec<&str> = REQUIRED_FIELDS.into_iter()
            .filter(|field|self.client_config.get(field).is_none())
            .collect();

        if !missing_fields.is_empty() {
            return Err(PeridotConfigError::MissingConfig { configs: missing_fields })
        }

        let app_id = self.client_config.get("application.id").unwrap().to_owned();

        let _ = self.client_config.set("group.id", app_id);
        
        Ok(self)
    }

    fn clean_config(mut self) -> Self {
        if let Some(value) = self.client_config.get("group.id") {
            warn!("'group.id' set as '{}' in client config. Disabling as 'application.id' is used to derive groupid in streams applications", value);

            self.client_config.remove("group.id");
        }

        self
    }

    pub fn build(self) -> Result<PeridotConfig, PeridotConfigError>  {
        Ok(
            self.clean_config()
                .prep_config()?
                .into()
        )
    }
}


impl From<&ClientConfig> for PeridotConfigBuilder {
    fn from(client_config: &ClientConfig) -> Self {
        Self {
            client_config: client_config.clone(),
        }
    }
}

impl From<&HashMap<String, String>> for PeridotConfigBuilder {
    fn from(config_map: &HashMap<String, String>) -> Self {
        let mut client_config: ClientConfig = Default::default();

        config_map.iter().for_each(
            |(key, value)| {
                client_config.set(key, value);
            }
        );

        Self {
            client_config,
        }
    }
}