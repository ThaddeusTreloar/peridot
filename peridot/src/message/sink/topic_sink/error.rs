use rdkafka::error::KafkaError;

#[derive(Debug, thiserror::Error)]
pub enum TopicSinkError {
    #[error(
        "Failed to send message for topic: {}, partition: {}, offset: {}, caused by: {}",
        topic,
        partition,
        offset,
        err
    )]
    FailedToSendMessageError {
        topic: String,
        partition: i32,
        offset: i64,
        err: KafkaError,
    },
    #[error(
        "Unknown error while checking producer send: {}, partition: {}, offset: {}, caused by: {}",
        topic,
        partition,
        offset,
        err
    )]
    UnknownError {
        topic: String,
        partition: i32,
        offset: i64,
        err: KafkaError,
    },
}
