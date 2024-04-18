use std::collections::{HashMap, HashSet};

use rdkafka::ClientConfig;
use tracing::warn;

use crate::help;

#[derive(Default, Debug, Clone)]
pub struct PeridotConfig {
    clients_config: ClientConfig,
    app_config: HashMap<String, String>,
    fields: HashSet<&'static str>,
}

fn gen_config_fields() -> HashSet<&'static str> {
    let mut fields = HashSet::new();

    // Compulsory fields

    fields
}

impl PeridotConfig {
    pub fn new() -> Self {
        PeridotConfig {
            clients_config: rdkafka::ClientConfig::new(),
            app_config: HashMap::new(),
            fields: gen_config_fields(),
        }
    }

    pub fn clients_config(&self) -> &ClientConfig {
        &self.clients_config
    }

    pub fn clean_config(&mut self) {}

    pub fn disable_auto_commit(&mut self) {
        if let Some("true") = self.clients_config.get("enable.auto.commit") {
            warn!("Auto commit is enabled in config. Disabled while using exactly-once-semantics.");
            help("auto-commit");

            self.clients_config.set("enable.auto.commit", "false");
        }
    }

    pub fn set(&mut self, key: &str, value: &str) {
        if self.fields.contains(key) {
            self.app_config.insert(key.to_string(), value.to_string());
        } else {
            self.clients_config.set(key, value);
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        if self.fields.contains(key) {
            self.app_config.get(key).cloned()
        } else {
            self.clients_config.get(key).map(|s| s.to_string())
        }
    }
}

impl From<&ClientConfig> for PeridotConfig {
    fn from(clients_config: &ClientConfig) -> Self {
        let mut new_config = PeridotConfig {
            clients_config: clients_config.clone(),
            app_config: Default::default(),
            fields: gen_config_fields(),
        };

        new_config.clean_config();

        new_config
    }
}
