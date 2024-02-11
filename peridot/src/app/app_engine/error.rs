


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