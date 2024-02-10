
#[derive(Debug, thiserror::Error)]
pub enum PeridotAppCreationError {
    #[error("Failed to client: {0}")]
    ClientCreationError(String),
}

impl From<rdkafka::error::KafkaError> for PeridotAppCreationError {
    fn from(error: rdkafka::error::KafkaError) -> Self {
        PeridotAppCreationError::ClientCreationError(error.to_string())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PeridotAppRuntimeError {
    #[error("Failed to run: {0}")]
    RunError(String),
    #[error("Created multiple source with the same topic: {0}")]
    SourceConflictError(String),
}