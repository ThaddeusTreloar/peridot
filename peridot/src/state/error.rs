use rdkafka::error::KafkaError;

#[derive(thiserror::Error, Debug)]
pub enum StateStoreCreationError {
    #[error("Failed to create consumer: {0}")]
    FromConfigError(String),
}

impl From<KafkaError> for StateStoreCreationError {
    fn from(err: KafkaError) -> Self {
        StateStoreCreationError::FromConfigError(err.to_string())
    }
}