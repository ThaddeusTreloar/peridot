use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read, Write},
};

use serde::{Deserialize, Serialize};

use super::builder::{PeridotConfigBuilder, GROUP_INSTANCE_ID};

const PERSISTENT_FIELDS: [&str; 1] = [GROUP_INSTANCE_ID];

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct PersistentConfig {
    pub(super) group_instance_id: String,
}

impl PersistentConfig {
    pub(super) fn write_config<W: Write>(
        &self,
        writer: &mut W,
    ) -> Result<(), PersistentConfigWriteError> {
        let data = serde_json::to_string(self)?;

        writer.write_all(data.as_bytes())?;

        Ok(())
    }
}

impl TryFrom<&PeridotConfigBuilder> for PersistentConfig {
    type Error = PersistentConfigConversionError;

    fn try_from(config: &PeridotConfigBuilder) -> Result<Self, Self::Error> {
        if let Some(missing_field) = PERSISTENT_FIELDS
            .into_iter()
            .find(|value| !config.contains(*value))
        {
            Err(PersistentConfigConversionError::InvalidConfigError { missing_field })
        } else {
            let field_values: HashMap<_, _> = PERSISTENT_FIELDS
                .into_iter()
                .map(|field| (field.to_owned(), config.get(field).to_owned()))
                .collect();

            Ok(Self {
                group_instance_id: config
                    .get(GROUP_INSTANCE_ID)
                    .expect("Failed to get field that should exist for PersistentConfig conversion")
                    .to_owned(),
            })
        }
    }
}

impl From<PersistentConfig> for Vec<(String, String)> {
    fn from(value: PersistentConfig) -> Self {
        let fields = vec![("group.instance.id".to_owned(), value.group_instance_id)];

        fields
    }
}

impl TryFrom<File> for PersistentConfig {
    type Error = PersistentConfigParseError;

    fn try_from(mut value: File) -> Result<Self, Self::Error> {
        let mut out = String::new();
        value.read_to_string(&mut out)?;

        let config = serde_json::from_str::<Self>(&out)?;

        Ok(config)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PersistentConfigConversionError {
    #[error("Field missing from source PeridotConfigBuilder: {}", missing_field)]
    InvalidConfigError { missing_field: &'static str },
}

#[derive(Debug, thiserror::Error)]
pub enum PersistentConfigWriteError {
    #[error("Failed to write file: {}", 0)]
    IOError(#[from] std::io::Error),
    #[error("Failed to serialize file contents: {}", 0)]
    JsonParseError(#[from] serde_json::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum PersistentConfigParseError {
    #[error("Failed to read file: {}", 0)]
    IOError(#[from] std::io::Error),
    #[error("Failed to parse file contents: {}", 0)]
    JsonParseError(#[from] serde_json::Error),
}
