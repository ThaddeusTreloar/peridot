use rdkafka::error::KafkaError;

use crate::engine::error::{PeridotEngineCreationError, PeridotEngineRuntimeError};

#[derive(Debug, thiserror::Error)]
pub enum PeridotAppCreationError {
    #[error("Failed to initialise engine: {0}")]
    EngineCreationError(#[from] PeridotEngineCreationError),
}

#[derive(Debug, thiserror::Error)]
pub enum PeridotAppRuntimeError {
    #[error("Failed to run: {0}")]
    RunError(String),
    #[error("Created multiple source with the same topic: {0}")]
    SourceConflictError(String),
    #[error("Failed to initialise engine: {0}")]
    EngineRuntimeError(#[from] PeridotEngineRuntimeError),
    #[error("Sink error: {0}")]
    SinkError(#[from] KafkaError),
    #[error("Failed to join job: {0}")]
    JobJoinError(#[from] tokio::task::JoinError),
}
