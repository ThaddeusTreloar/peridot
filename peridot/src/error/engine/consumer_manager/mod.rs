
#[derive(Debug, thiserror::Error)]
pub enum ClientManagerError {
    #[error("ClientManagerError::CreateConsumerError: Failed to create consumer while initialising ClientManager -> {}", 0)]
    CreateConsumerError(rdkafka::error::KafkaError),
    #[error("ClientManagerError::FetchTopicMetadataError: Failed to fetch topic metadata for '{}' caused by: {}", topic, err)]
    FetchTopicMetadataError {
        topic: String,
        err: rdkafka::error::KafkaError,
    },
    #[error(
        "ClientManagerError::GetPartitionQueueError: Failed to get partition queue for {}:{}",
        topic,
        partition
    )]
    GetPartitionQueueError { topic: String, partition: i32 },
    #[error("ClientManagerError::ConsumerSubscribeError: Failed to subscribe consumer to topic '{}' caused by: {}", topic, err)]
    ConsumerSubscribeError {
        topic: String,
        err: rdkafka::error::KafkaError,
    },
    #[error(
        "ClientManagerError::ConsumerPollError: Failed to poll consumer caused by: {}",
        err
    )]
    ConsumerPollError { err: rdkafka::error::KafkaError },
    #[error("ClientManagerError::ConflictingTopicRegistration: {}", topic)]
    ConflictingTopicRegistration { topic: String },
    #[error("ClientManagerError::TopicNotFoundOnCluster: {}", topic)]
    TopicNotFoundOnCluster { topic: String },
    #[error("ClientManagerError::PartitionMetadataNotFoundForTopic: {topic}")]
    PartitionMetadataNotFoundForTopic { topic: String },
    #[error("ClientManagerError::UnknownError: {error}, while handling: {info}")]
    UnknownError { error: u8, info: String },
}