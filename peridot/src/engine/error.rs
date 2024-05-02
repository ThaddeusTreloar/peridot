use rdkafka::error::KafkaError;

use crate::state::error::StateStoreCreationError;

use super::{admin_manager::AdminManagerError, changelog_manager::ChangelogManagerError, client_manager::ClientManagerError, metadata_manager::MetadataManagerError};

#[derive(Debug, thiserror::Error)]
pub enum PeridotEngineCreationError {
    #[error("Failed to client: {0}")]
    ClientCreationError(String),
    #[error(transparent)]
    AdminManagerError(#[from] AdminManagerError),
    #[error(transparent)]
    ClientManagerError(#[from] ClientManagerError),
    #[error(transparent)]
    ChangelogManagerError(#[from] ChangelogManagerError),
}

impl From<rdkafka::error::KafkaError> for PeridotEngineCreationError {
    fn from(error: rdkafka::error::KafkaError) -> Self {
        PeridotEngineCreationError::ClientCreationError(error.to_string())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PeridotEngineRuntimeError {
    #[error(transparent)]
    TableCreationError(#[from] TableCreationError),
    #[error(transparent)]
    BackendInitialisationError(#[from] StateStoreCreationError),
    #[error("Cyclic dependency detected for topic: {0}, you cannot both subsribe and publish to the same topic.")]
    CyclicDependencyError(String),
    #[error("Failed to create producer: {0}")]
    ProducerCreationError(#[from] KafkaError),
    #[error("Broken rebalance receiver.")]
    BrokenRebalanceReceiver,
    #[error(transparent)]
    ClientManagerError(#[from] ClientManagerError),
    #[error(transparent)]
    MetadataManagerError(#[from] MetadataManagerError),
}

#[derive(Debug, thiserror::Error)]
pub enum TableCreationError {
    #[error("Topic not found.")]
    TopicNotFound,
    #[error("Table exists.")]
    TableAlreadyExists,
    #[error("Failed to create table: {0}")]
    TableCreationError(String),
}

#[derive(Debug, thiserror::Error)]
pub enum TableRegistrationError {
    #[error("Table already registered.")]
    TableAlreadyRegistered,
}

#[derive(Debug, thiserror::Error)]
pub enum EngineInitialisationError {
    #[error("Attempted to reregister existing topic: {0}")]
    ConflictingTopicRegistration(String),
}
