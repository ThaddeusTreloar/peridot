/*
 * Copyright 2024 Thaddeus Treloar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

use std::{ops::Deref, sync::Arc, time::Duration};

use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    message::OwnedMessage,
    message::Message,
    metadata::Metadata,
    Offset,
};
use rdkafka_sys::RDKafkaRespErr;
use tracing::info;

use crate::{
    app::{config::PeridotConfig, extensions::PeridotConsumerContext},
    engine::metadata_manager::topic_metadata, error::{engine::consumer_manager::ClientManagerError, ErrorType, Fatal},
};

use super::{
    context::EngineContext, metadata_manager::topic_metadata::TopicMetadata,
    queue_manager::partition_queue::{PeridotPartitionQueue, StreamPeridotPartitionQueue}, util::ConsumerUtils,
    PeridotConsumer,
};

#[derive(Clone)]
pub(crate) struct ConsumerManager {
    consumer: Arc<PeridotConsumer>,
}

impl ConsumerManager {
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

    pub(crate) fn get_topic_metadata(&self, topic: &str) -> Result<Metadata, ClientManagerError> {
        self.consumer
            .client()
            .fetch_metadata(Some(topic), Duration::from_millis(2500))
            .map_err(|err| ClientManagerError::FetchTopicMetadataError {
                topic: topic.to_owned(),
                err,
            })
    }

    pub(crate) fn create_topic_source(
        &self,
        topic: &str,
    ) -> Result<TopicMetadata, ClientManagerError> {
        let raw_metadata = self.get_topic_metadata(topic)?;

        let topic_metadata = match raw_metadata.topics().first() {
            None => unreachable!(""),
            Some(meta) => {
                if let Some(error) = meta.error() {
                    tracing::error!("Encountered error while fetching metadata for topic: {}, error: {:?}", meta.name(), error);

                    match error {
                        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART => {
                            return Err(ClientManagerError::TopicNotFoundOnCluster { topic: topic.to_owned() });
                            todo!("Some config to create topic if not found");
                        },
                        e => {
                            return Err(ClientManagerError::UnknownError { error: e as u8, info: format!("fetching metadata for topic: {}", topic) })
                        }
                    }
                }

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
        engine_context: Arc<EngineContext>,
    ) -> Result<PeridotPartitionQueue, ClientManagerError> {
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
                Ok(queue)
            }
        }
    }

    pub(crate) fn poll_consumer(&self) {
        if let Some(msg) = self.consumer.poll(Duration::from_millis(1000)) {
            match msg {
                Err(e) => panic!("Failed to poll consumer: {}", e),
                Ok(msg) => {
                    let message = msg.detach();

                    tracing::error!(
                        "Unexpected changelog consumer message: topic: {}, partition: {}, offset: {}",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                    );

                    panic!(
                        "Unexpected changelog consumer message: topic: {}, partition: {}, offset: {}",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                    );
                }
            }
        }
    }

    pub(crate) fn get_topic_partition_consumer_position(&self, topic: &str, partition: i32) -> i64 {
        tracing::debug!("Finding consumer position for topic: {}, partition: {}", topic, partition);

        match self
            .consumer
            .position()
            .expect("Failed to get assignment")
            .elements_for_topic(topic)
            .into_iter()
            .find(|elem| elem.partition() == partition)
            .map(|elem| elem.offset())
            .expect("Failed to find consumer offset")
        {
            Offset::Offset(offset) => offset,
            Offset::Beginning => {
                panic!("Offset::Beginning")
            }
            Offset::End => {
                panic!("Offset::End")
            }
            Offset::OffsetTail(o) => {
                panic!("Offset::OffsetTail({})", o)
            }
            Offset::Stored => {
                panic!("Offset::Stored")
            }
            Offset::Invalid => {
                tracing::warn!("Current offset invalid...");
                -1
            }
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
