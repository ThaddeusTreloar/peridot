use std::{ops::Deref, sync::Arc, time::Duration};

use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    message::OwnedMessage,
};
use tracing::info;

use crate::{
    app::{config::PeridotConfig, extensions::PeridotConsumerContext},
    engine::metadata_manager::topic_metadata,
};

use super::{
    metadata_manager::topic_metadata::TopicMetadata,
    queue_manager::partition_queue::StreamPeridotPartitionQueue, util::ConsumerUtils,
    PeridotConsumer,
};

#[derive(Clone)]
pub(crate) struct ClientManager {
    consumer: Arc<PeridotConsumer>,
}

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
    #[error("ClientManagerError::PartitionMetadataNotFoundForTopic: {}", topic)]
    PartitionMetadataNotFoundForTopic { topic: String },
}

impl ClientManager {
    pub(crate) fn from_config(config: &PeridotConfig) -> Result<Self, ClientManagerError> {
        let context = PeridotConsumerContext::default();

        let consumer = config
            .client_config()
            .create_with_context(context.clone())
            .map_err(ClientManagerError::CreateConsumerError)?;

        Ok(Self {
            consumer: Arc::new(consumer),
        })
    }

    pub(crate) fn create_topic_source(
        &self,
        topic: &str,
    ) -> Result<TopicMetadata, ClientManagerError> {
        let raw_metadata = self
            .consumer
            .client()
            .fetch_metadata(Some(topic), Duration::from_millis(1000))
            .map_err(|err| ClientManagerError::FetchTopicMetadataError {
                topic: topic.to_owned(),
                err,
            })?;

        let topic_metadata = match raw_metadata.topics().first() {
            None => Err(ClientManagerError::TopicNotFoundOnCluster {
                topic: topic.to_owned(),
            })?,
            Some(meta) => {
                let partition_count = meta.partitions().iter().count();

                TopicMetadata::new(partition_count as i32)
            }
        };

        let mut subscription = self.consumer.get_subscribed_topics();

        if subscription.contains(&topic.to_owned()) {
            // TODO: Get rid of this clone
            Err(ClientManagerError::ConflictingTopicRegistration {
                topic: topic.to_owned(),
            })?;
        }

        subscription.push(topic.to_owned());

        self.consumer
            .subscribe(
                subscription
                    .iter()
                    .map(Deref::deref)
                    .collect::<Vec<_>>()
                    .as_slice(),
            )
            .map_err(|err| ClientManagerError::FetchTopicMetadataError {
                topic: topic.to_owned(),
                err,
            })?;

        Ok(topic_metadata)
    }

    pub(crate) fn get_partition_queue(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<StreamPeridotPartitionQueue, ClientManagerError> {
        tracing::debug!(
            "Getting partition queue for topic: {} partition: {}",
            topic,
            partition
        );

        //let waker = self.consumer.context()
        //    .pre_rebalance_waker();

        match self.consumer.split_partition_queue(topic, partition) {
            None => Err(ClientManagerError::GetPartitionQueueError {
                topic: topic.to_owned(),
                partition,
            }),
            Some(queue) => {
                tracing::debug!(
                    "Successfully split partition queue for topic: {}, partition: {}",
                    topic,
                    partition
                );
                Ok(StreamPeridotPartitionQueue::new(
                    queue,
                    topic.to_owned(),
                    partition,
                ))
            }
        }
    }

    pub(crate) fn poll_consumer(&self) -> Result<Option<OwnedMessage>, ClientManagerError> {
        match self.consumer.poll(Duration::from_millis(100)) {
            None => Ok(None),
            Some(result) => result
                .map(|m| Some(m.detach()))
                .map_err(|err| ClientManagerError::ConsumerPollError { err }),
        }
    }

    pub(crate) fn consumer_arc(&self) -> Arc<PeridotConsumer> {
        self.consumer.clone()
    }

    pub(crate) fn consumer_ref(&self) -> &PeridotConsumer {
        &self.consumer
    }

    pub(crate) fn consumer_context(&self) -> Arc<PeridotConsumerContext> {
        self.consumer.context().clone()
    }
}
