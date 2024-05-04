use std::{collections::{HashMap, HashSet}, fs::File, io, path::Path};

use rdkafka::ClientConfig;
use tracing::warn;
use uuid::Uuid;

use crate::help;

use super::{persistent_config::{self, PersistentConfig, PersistentConfigConversionError, PersistentConfigParseError, PersistentConfigWriteError}, PeridotConfig};

// Config keys
pub (super) const APPLICATION_ID: &str = "application.id";
pub (super) const BOOTSTRAP_SERVERS: &str = "bootstrap.servers";
pub (super) const CLIENT_ID: &str = "client.id";
pub (super) const ENABLE_IDEMPOTENCE: &str = "enable.idempotence";
pub (super) const GROUP_ID: &str = "group.id";
pub (super) const GROUP_INSTANCE_ID: &str = "group.instance.id";
pub (super) const ISOLATION_LEVEL: &str = "isolation.level";
pub (super) const PARTITIONER: &str = "partitioner";
pub (super) const PERSISTENT_CONFIG_DIR: &str = "persistent.config.dir";
pub (super) const PERSISTENT_CONFIG_FILENAME: &str = "peridot.persistent.config";
pub (super) const STATE_DIR: &str = "state.dir";

// Config values
pub (super) const ENABLE_IDEMPOTENCE_TRUE: &str = "TRUE";
pub (super) const ISOLATION_LEVEL_READ_COMMITTED: &str = "read_committed";
pub (super) const PARTITIONER_MURMUR_2_RANDOM: &str = "murmur2_random";

pub(super) const REQUIRED_FIELDS: [&str; 6] = [
    APPLICATION_ID,
    BOOTSTRAP_SERVERS,
    ENABLE_IDEMPOTENCE,
    ISOLATION_LEVEL,
    PARTITIONER,
    STATE_DIR,
];

pub(super) const APP_FIELDS: [&str; 3] = [
    APPLICATION_ID,
    STATE_DIR,
    PERSISTENT_CONFIG_DIR,
];

pub(super) const DEFAULT_FIELDS: [(&str, &str); 5] = [
    (ENABLE_IDEMPOTENCE, ENABLE_IDEMPOTENCE_TRUE),
    (ISOLATION_LEVEL, ISOLATION_LEVEL_READ_COMMITTED),// TODO: Make setting this dependent on delivery semantics arg
    (PARTITIONER, PARTITIONER_MURMUR_2_RANDOM),// TODO: Make setting this dependent on delivery semantics arg
    (PERSISTENT_CONFIG_DIR, "./"),
    (STATE_DIR, "/var/lib/peridot"),
];

pub(super) const FORBID_USER_SET_FIELDS: [(&str, &str); 3] = [
    (GROUP_ID, "'application.id' is used to derive 'group.id' in streams applications"),
    (GROUP_INSTANCE_ID, "'application.id' is used to derive 'group.instance.id' in streams applications"),
    (CLIENT_ID, "'application.id' is used to derive 'client.id' in streams applications"),
];

#[derive(Debug, Clone, Default, derive_more::From)]
pub struct PeridotConfigBuilder {
    pub(crate) client_config: ClientConfig,
    pub(crate) app_config: HashMap<String, String>,
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

    fn get_app_var(&self, key: &str) -> Option<&str> {
        self.app_config.get(key).map(|s|s.as_str())
    }

    fn get_client_var(&self, key: &str) -> Option<&str> {
        self.client_config.get(key)
    }
    
    pub fn get<'a, K: Into<&'a str>>(&self, key: K) -> Option<&str> {
        let key: &str = key.into();
        
        if APP_FIELDS.contains(&key) {
            self.get_app_var(key)
        } else {
            self.get_client_var(key)
        }
    }

    fn set_app_var(&mut self, key: String, value: String) {
        let _ = self.app_config.insert(key, value);
    }

    fn set_client_var(&mut self, key: String, value: String) {
        self.client_config.set(key, value);
    }
    
    pub fn set<K: Into<String>, V: Into<String>>(&mut self, key: K, value: V) -> &mut Self {
        let key: String = key.into();
        let value: String = value.into();

        if APP_FIELDS.contains(&key.as_str()) {
            self.set_app_var(key, value)
        } else {
            self.set_client_var(key, value)
        }

        self
    }

    pub fn remove<'a, K: Into<&'a str>>(&mut self, key: K) -> &mut Self {
        let key: &str = key.into();
        
        if APP_FIELDS.contains(&key) {
            self.app_config.remove(key);
        } else {
            self.client_config.remove(key);
        }

        self
    }

    pub fn extend(&mut self, other: &[(String, String)]) {
        for (k, v) in other {
            let _ = self.set(k, v);
        }
    }

    fn overwrite_with_persisted_config(mut self) -> Result<Self, PeridotConfigError> {
        let mut persistent_config_stub = Path::new(PERSISTENT_CONFIG_FILENAME);

        let mut persistent_config_path = Path::new(
            self.get(PERSISTENT_CONFIG_DIR)
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

            self.extend(Vec::from(persistent_config).as_slice());

            Ok(self)
        }
    }

    fn check_missing_required(mut self) -> Result<Self, PeridotConfigError> {
        let missing_fields: Vec<_> = REQUIRED_FIELDS.into_iter()
            .filter(|field|self.get(*field).is_none())
            .collect();

        if !missing_fields.is_empty() {
            Err(PeridotConfigError::MissingConfig { configs: missing_fields })?
        }

        Ok(self)
    }

    fn derive_internal_fields(mut self) -> Result<Self, PeridotConfigError> {
        let app_id = self.get("application.id").unwrap().to_owned();

        // Check if instance-id is written out to persistent storage.
        // If not generate and store.
        let instance_suffix = Uuid::new_v4();
        let instance_id = format!("{}-{}", app_id, instance_suffix);

        self
            .set("group.id", app_id)
            .set("group.instance.id", instance_id.clone())
            .set("client.id", instance_id);
        
        Ok(self)
    }

    fn resolve_config_conflict(&mut self, key: &str, reason: &str) {
        if let Some(value) = self.get(key) {
            warn!("'{}' set as '{}' in client config. Disabling becaus: {}", key, value, reason);
    
            self.remove(key);
        }
    }

    fn set_missing_defaults(mut self) -> Self {
        let missing_defaults = DEFAULT_FIELDS.into_iter()
            .filter(|(name, _)| self.get(*name).is_none())
            .map(|(k , v)|(k.to_owned(), v.to_owned()))
            .collect::<Vec<_>>();

        self.extend(&missing_defaults);

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
        client_config.config_map().into()
    }
}

impl From<&HashMap<String, String>> for PeridotConfigBuilder {
    fn from(config_map: &HashMap<String, String>) -> Self {
        let mut pconfig = Self {
            client_config: Default::default(),
            app_config: Default::default(),
        };

        config_map.iter()
            .for_each(
                |(key, value)| {
                    pconfig.set(key, value);
                }
            );

        pconfig
    }
}