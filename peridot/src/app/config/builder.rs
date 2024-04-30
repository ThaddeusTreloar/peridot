use std::{collections::{HashMap, HashSet}, fs::File, io, path::Path};

use rdkafka::ClientConfig;
use tracing::warn;
use uuid::Uuid;

use crate::help;

use super::{persistent_config::{self, PersistentConfig, PersistentConfigConversionError, PersistentConfigParseError, PersistentConfigWriteError}, PeridotConfig};

pub (super) const APPLICATION_ID: &str = "application.id";
pub (super) const BOOTSTRAP_SERVERS: &str = "bootstrap.servers";
pub (super) const CLIENT_ID: &str = "client.id";
pub (super) const GROUP_ID: &str = "group.id";
pub (super) const GROUP_INSTANCE_ID: &str = "group.instance.id";
pub (super) const PERSISTENT_CONFIG_DIR: &str = "persistent.config.dir";
pub (super) const PERSISTENT_CONFIG_FILENAME: &str = "peridot.persistent.config";
pub (super) const STATE_DIR: &str = "state.dir";

const REQUIRED_FIELDS: [&str; 3] = [
    BOOTSTRAP_SERVERS,
    APPLICATION_ID,
    STATE_DIR,
];

const DEFAULT_FIELDS: [(&str, &str); 2] = [
    (STATE_DIR, "/var/lib/peridot"),
    (PERSISTENT_CONFIG_DIR, "./"),
];

const FORBID_USER_SET_FIELDS: [(&str, &str); 3] = [
    (GROUP_ID, "'application.id' is used to derive 'group.id' in streams applications"),
    (GROUP_INSTANCE_ID, "'application.id' is used to derive 'group.instance.id' in streams applications"),
    (CLIENT_ID, "'application.id' is used to derive 'client.id' in streams applications"),
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
    },
    #[error("PeridotConfigError::WriteConfigError: failed to create file '{}'", 0)]
    WriteConfigError(#[from] io::Error),
    #[error(transparent)]
    PersistConfigWriteError(#[from] PersistentConfigWriteError),
    #[error(transparent)]
    PersistConfigReadError(#[from] PersistentConfigParseError),
    #[error(transparent)]
    PersistConversionError(#[from] PersistentConfigConversionError),
}

impl PeridotConfigBuilder {
    pub fn new() -> Self {
        Self { 
            ..Default::default()
        }
    }

    pub fn contains<'a, K: Into<&'a str>>(&self, key: K) -> bool {
        self.client_config.config_map().contains_key(key.into())
    }

    pub fn get<'a, K: Into<&'a str>>(&self, key: K) -> Option<&str> {
        self.client_config.get(key.into())
    }
    
    pub fn set<K: Into<String>, V: Into<String>>(&mut self, key: K, value: V) -> &mut Self {
        self.client_config.set(key, value);

        self
    }

    pub fn remove<'a, K: Into<&'a str>>(&mut self, key: K) -> &mut Self {
        self.client_config.remove(key.into());

        self
    }

    fn overwrite_with_persisted_config(mut self) -> Result<Self, PeridotConfigError> {
        let mut persistent_config_stub = Path::new(PERSISTENT_CONFIG_FILENAME);

        let mut persistent_config_path = Path::new(
            self.client_config.get(PERSISTENT_CONFIG_DIR)
                .unwrap_or_else(|| {
                    tracing::error!("{} not set in config. This should not be possible.", PERSISTENT_CONFIG_DIR);
                    panic!("{} not set in config. This should not be possible.", PERSISTENT_CONFIG_DIR)
                })
        );

        let filepath = persistent_config_path.join(persistent_config_stub);

        if !filepath.exists() {
            let mut file = File::create(filepath)?;
            let persistent_config = PersistentConfig::try_from(&self)?;
            persistent_config.write_config(&mut file)?;

            Ok(self)
        } else {
            let mut file = File::open(filepath)?;
            let persistent_config = PersistentConfig::try_from(file)?;

            self.client_config.extend(Vec::from(persistent_config));

            Ok(self)
        }
    }

    fn check_missing_required(mut self) -> Result<Self, PeridotConfigError> {
        let missing_fields: Vec<_> = REQUIRED_FIELDS.into_iter()
            .filter(|field|self.client_config.get(field).is_none())
            .collect();

        if !missing_fields.is_empty() {
            return Err(PeridotConfigError::MissingConfig { configs: missing_fields })
        }

        Ok(self)
    }

    fn derive_internal_fields(mut self) -> Result<Self, PeridotConfigError> {
        let app_id = self.client_config.get("application.id").unwrap().to_owned();

        // Check if instance-id is written out to persistent storage.
        // If not generate and store.
        let instance_suffix = Uuid::new_v4();
        let instance_id = format!("{}-{}", app_id, instance_suffix);

        let _ = self.client_config.set("group.id", app_id);
        let _ = self.client_config.set("group.instance.id", instance_id.clone());
        let _ = self.client_config.set("client.id", instance_id);
        
        Ok(self)
    }

    fn resolve_config_conflict(&mut self, key: &str, reason: &str) {
        if let Some(value) = self.client_config.get(key) {
            warn!("'group.id' set as '{}' in client config. Disabling becaus: {}", key, reason);
    
            self.client_config.remove(key);
        }
    }

    fn set_missing_defaults(mut self) -> Self {
        let missing_defaults = DEFAULT_FIELDS.into_iter()
            .filter(|(name, _)| self.client_config.get(name).is_none())
            .map(|(k , v)|(k.to_owned(), v.to_owned()))
            .collect::<Vec<_>>();

        self.client_config.extend(missing_defaults);

        self
    }

    fn clean_config(mut self) -> Self {
        FORBID_USER_SET_FIELDS.into_iter()
            .for_each(|(f, r)|self.resolve_config_conflict(f, r));

        self
    }

    pub fn build(self) -> Result<PeridotConfig, PeridotConfigError>  {
        Ok(
            self.clean_config()
                .set_missing_defaults()
                .check_missing_required()?
                .derive_internal_fields()?
                .overwrite_with_persisted_config()?
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