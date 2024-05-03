use std::{sync::Arc, time::Duration};

use rdkafka::{consumer::Consumer, TopicPartitionList};

use crate::app::{config::PeridotConfig, extensions::PeridotConsumerContext};

use super::{queue_manager::partition_queue::StreamPeridotPartitionQueue, PeridotConsumer};

pub struct Watermarks(i64, i64);

impl Watermarks {
    pub(crate) fn low(&self) -> i64 {
        self.0
    }

    pub(crate) fn high(&self) -> i64 {
        self.1
    }
}

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
    #[error("ChangelogManagerError::FetchWaterMarkError, failed to fetch watermark for {}:{} caused by {}", topic, partition, err)]
    FetchWaterMarkError {
        topic: String,
        partition: i32,
        err: rdkafka::error::KafkaError,
    },
    #[error("ChangelogManagerError::CloseChangelogError, failed to close changelog partition for {}:{} caused by {}", topic, partition, err)]
    CloseChangelogError {
        topic: String,
        partition: i32,
        err: rdkafka::error::KafkaError,
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
            .map_err(ChangelogManagerError::CreateConsumerError)?;

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
            let waker = self.consumer.context().pre_rebalance_waker();

            match self.consumer.split_partition_queue(changelog_topic, partition) {
                None => Err(ChangelogManagerError::ConsumerPartitionQueue { 
                    topic: changelog_topic.to_owned(), 
                    partition
                }),
                Some(queue) => Ok(StreamPeridotPartitionQueue::new(queue, waker, changelog_topic.to_owned(), partition))
            }
        }
    }

    pub(crate) fn close_changelog_stream(&self, changelog_topic: &str, partition: i32) -> Result<(), ChangelogManagerError> {
        let mut tpl = TopicPartitionList::new();

        tpl.add_partition(changelog_topic, partition);

        self.consumer.incremental_unassign(&tpl)
            .map_err(|err| ChangelogManagerError::CloseChangelogError { topic: changelog_topic.to_owned(), partition, err })
    }

    pub(super) fn get_watermark_for_changelog(&self, changelog_topic: &str, partition: i32) -> Result<Watermarks, ChangelogManagerError> {
        let (low, high) = self.consumer.client()
            .fetch_watermarks(changelog_topic, partition, Duration::from_millis(1000))
            .map_err(|err| ChangelogManagerError::FetchWaterMarkError { topic: changelog_topic.to_owned(), partition, err })?;

        Ok(Watermarks(low, high))
    }
}