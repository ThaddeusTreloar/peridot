use rdkafka::error::KafkaError;

use crate::state::error::StateStoreCreationError;

#[derive(Debug, thiserror::Error)]
pub enum PeridotEngineCreationError {
    #[error("Failed to client: {0}")]
    ClientCreationError(String),
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