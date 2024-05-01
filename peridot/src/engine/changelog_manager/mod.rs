use std::sync::Arc;

use rdkafka::consumer::Consumer;

use crate::app::{config::PeridotConfig, extensions::PeridotConsumerContext};

use super::{queue_manager::partition_queue::StreamPeridotPartitionQueue, PeridotConsumer};

#[derive(Debug, thiserror::Error)]
pub enum ChangelogManagerError {
    #[error("ClientManagerError::CreateConsumerError: Failed to create consumer while initialising ClientManager -> {}", 0)]
    CreateConsumerError(rdkafka::error::KafkaError),
    #[error("ChangelogManagerError::RequestChangelogPartition, failed to get changelog partition for {}:{} caused by: {}", topic, partition, err)]
    RequestChangelogPartition{
        topic: String,
        partition: i32,
        err: rdkafka::error::KafkaError,
    },
    #[error("ChangelogManagerError::ChangelogPartitionAlreadyActive, changelog stream already exists for {}:{}", topic, partition)]
    ChangelogPartitionAlreadyActive{
        topic: String,
        partition: i32,
    },
    #[error("ChangelogManagerError::ConsumerPartitionQueue, failed to get split partition for {}:{}", topic, partition)]
    ConsumerPartitionQueue {
        topic: String,
        partition: i32,
    },
}

pub(crate) struct ChangelogManager {
    consumer: Arc<PeridotConsumer>,
}

impl ChangelogManager {
    pub(crate) fn from_config(config: &PeridotConfig) -> Result<Self, ChangelogManagerError> {
        let context = PeridotConsumerContext::from_config(config);

        let consumer = config
            .client_config()
            .create_with_context(context.clone())
            .map_err(|e|ChangelogManagerError::CreateConsumerError(e))?;

        Ok(Self {
            consumer: Arc::new(consumer),
        })
    }

    pub(crate) fn request_changelog_partition(&self, changelog_topic: &str, partition: i32) -> Result<StreamPeridotPartitionQueue, ChangelogManagerError> {
        let partition_registered = self.consumer
            .assignment()
            .map_err(
                |err| ChangelogManagerError
                    ::RequestChangelogPartition { topic: changelog_topic.to_owned(), partition, err }
            )?.elements_for_topic(changelog_topic)
            .iter().any(|tp|tp.partition() == partition);

        if partition_registered {
            Err(
                ChangelogManagerError::ChangelogPartitionAlreadyActive { 
                    topic: changelog_topic.to_owned(), 
                    partition
                }
            )
        } else {
            match self.consumer.split_partition_queue(changelog_topic, partition) {
                None => Err(ChangelogManagerError::ConsumerPartitionQueue { 
                    topic: changelog_topic.to_owned(), 
                    partition
                }),
                Some(queue) => Ok(StreamPeridotPartitionQueue::new(queue))
            }
        }
    }

    pub(crate) fn close_changelog_stream(&self) {

    }
}