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

use std::{
    fmt::{Display, Write},
    sync::{atomic::AtomicI64, Arc},
    time::Duration,
};

use dashmap::DashMap;
use rdkafka::{consumer::Consumer, Message, Offset, TopicPartitionList};
use tracing::{info, warn};

use crate::app::{config::PeridotConfig, extensions::PeridotConsumerContext};

use super::{
    context::EngineContext, queue_manager::partition_queue::StreamPeridotPartitionQueue,
    PeridotConsumer,
};

#[derive(Debug)]
pub struct Watermarks(i64, i64);

impl Display for Watermarks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "Watermarks: {{ low: {}, high: {}}}",
            self.low(),
            self.high()
        ))
    }
}

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
    RequestChangelogPartition {
        topic: String,
        partition: i32,
        err: rdkafka::error::KafkaError,
    },
    #[error("ChangelogManagerError::ChangelogPartitionAlreadyActive, changelog stream already exists for {}:{}", topic, partition)]
    ChangelogPartitionAlreadyActive { topic: String, partition: i32 },
    #[error(
        "ChangelogManagerError::ConsumerPartitionQueue, failed to get split partition for {}:{}",
        topic,
        partition
    )]
    ConsumerPartitionQueue { topic: String, partition: i32 },
    #[error("ChangelogManagerError::FetchWaterMarkError, failed to fetch watermark for {}:{} caused by {}", topic, partition, err)]
    FetchWaterMarkError {
        topic: String,
        partition: i32,
        err: rdkafka::error::KafkaError,
    },
    #[error("ChangelogManagerError::CloseChangelog, failed to close changelog partition for {}:{} caused by {}", topic, partition, err)]
    CloseChangelog {
        topic: String,
        partition: i32,
        err: rdkafka::error::KafkaError,
    },
    #[error(
        "ChangelogManagerError::FailedToSeekConsumer, failed to consumer {}:{} to {} caused by {}",
        topic,
        partition,
        offset,
        err
    )]
    FailedToSeekConsumer {
        topic: String,
        partition: i32,
        offset: i64,
        err: rdkafka::error::KafkaError,
    },
}

pub(crate) struct ChangelogManager {
    consumer: Arc<PeridotConsumer>,
    changelog_positions: Arc<DashMap<(String, i32), AtomicI64>>,
}

impl ChangelogManager {
    pub(crate) fn from_config(config: &PeridotConfig) -> Result<Self, ChangelogManagerError> {
        let config = config.clone().with_earliest_offset_reset();

        let context = PeridotConsumerContext::default();

        let consumer = config
            .client_config()
            .create_with_context(context)
            .map_err(ChangelogManagerError::CreateConsumerError)?;

        Ok(Self {
            consumer: Arc::new(consumer),
            changelog_positions: Default::default(),
        })
    }

    pub(crate) fn request_changelog_partition_for_state_store(
        &self,
        state_name: &str,
        changelog_topic: &str,
        partition: i32,
        engine_context: Arc<EngineContext>,
    ) -> Result<StreamPeridotPartitionQueue, ChangelogManagerError> {
        let partition_registered = self
            .consumer
            .assignment()
            .map_err(|err| ChangelogManagerError::RequestChangelogPartition {
                topic: changelog_topic.to_owned(),
                partition,
                err,
            })?
            .elements_for_topic(changelog_topic)
            .iter()
            .any(|tp| tp.partition() == partition);

        if partition_registered {
            Err(ChangelogManagerError::ChangelogPartitionAlreadyActive {
                topic: changelog_topic.to_owned(),
                partition,
            })
        } else {
            tracing::debug!(
                "Getting partition for topic: {}, partition: {}",
                changelog_topic,
                partition
            );

            let partition_queue = match self
                .consumer
                .split_partition_queue(changelog_topic, partition)
            {
                None => Err(ChangelogManagerError::ConsumerPartitionQueue {
                    topic: changelog_topic.to_owned(),
                    partition,
                })?,
                Some(queue) => StreamPeridotPartitionQueue::new_changelog(
                    queue,
                    state_name,
                    changelog_topic.to_owned(),
                    partition,
                    engine_context,
                ),
            };

            let mut tpl = TopicPartitionList::new();

            tpl.add_partition(changelog_topic, partition);

            self.consumer
                .incremental_assign(&tpl)
                .expect("Failed to assign partition.");

            self.consumer
                .assignment()
                .expect("Failed to get assignment")
                .elements()
                .iter()
                .for_each(|tp| {
                    info!(
                        "Incremental assign. Current assignment, topic: {}, partition: {}",
                        tp.topic(),
                        tp.partition()
                    )
                });

            tracing::debug!(
                "Partition assigned to consumer for topic: {}, partition: {}",
                changelog_topic,
                partition
            );

            Ok(partition_queue)
        }
    }

    pub(crate) fn close_changelog_stream(
        &self,
        changelog_topic: &str,
        partition: i32,
    ) -> Result<(), ChangelogManagerError> {
        let mut tpl = TopicPartitionList::new();

        self.consumer
            .assignment()
            .expect("Failed to get assignment")
            .elements()
            .iter()
            .for_each(|tp| {
                info!(
                    "Incremental unassign. Current assignment, topic: {}, partition: {}",
                    tp.topic(),
                    tp.partition()
                )
            });

        tpl.add_partition(changelog_topic, partition);

        self.consumer.incremental_unassign(&tpl).map_err(|err| {
            ChangelogManagerError::CloseChangelog {
                topic: changelog_topic.to_owned(),
                partition,
                err,
            }
        })
    }

    pub(super) fn get_watermark_for_changelog(
        &self,
        changelog_topic: &str,
        partition: i32,
    ) -> Result<Watermarks, ChangelogManagerError> {
        let (low, high) = self
            .consumer
            .client()
            .fetch_watermarks(changelog_topic, partition, Duration::from_millis(1000))
            .map_err(|err| ChangelogManagerError::FetchWaterMarkError {
                topic: changelog_topic.to_owned(),
                partition,
                err,
            })?;

        Ok(Watermarks(low, high))
    }

    pub(super) fn seek_consumer(
        &self,
        changelog_topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), ChangelogManagerError> {
        let mut topic_partition_list = TopicPartitionList::new();

        topic_partition_list.add_partition_offset(
            changelog_topic,
            partition,
            rdkafka::Offset::Offset(offset),
        );

        self.consumer
            .seek_partitions(topic_partition_list, Duration::from_millis(1000))
            .map(|tpl| {
                for ((topic, partition), offset) in tpl.to_topic_map() {
                    match offset {
                        Offset::Offset(offset) => {
                            tracing::debug!(
                                "Successfully seeked topic: {}, partition: {}, to offset: {}",
                                topic,
                                partition,
                                offset
                            );
                        }
                        _ => tracing::error!(
                            "Unrecognised seek for topic: {}, partition: {}",
                            topic,
                            partition
                        ),
                    }
                }
            })
            .map_err(|err| ChangelogManagerError::FailedToSeekConsumer {
                topic: changelog_topic.to_owned(),
                partition,
                offset,
                err,
            })
    }

    pub(super) fn poll_consumer(&self) {
        if let Some(msg) = self.consumer.poll(Duration::from_millis(100)) {
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

    pub(super) fn get_topic_offset(&self, changelog_topic: &str, partition: i32) -> i64 {
        match self
            .consumer
            .assignment()
            .expect("Failed to get assignment")
            .elements_for_topic(changelog_topic)
            .into_iter()
            .find(|elem| elem.partition() == partition)
            .map(|elem| elem.offset())
            .expect("Unable to find topic offset.")
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
                warn!("Current offset invalid...");
                -1
            }
        }
    }

    pub(super) fn get_topic_consumer_position(&self, changelog_topic: &str, partition: i32) -> i64 {
        match self
            .consumer
            .position()
            .expect("Failed to get assignment")
            .elements_for_topic(changelog_topic)
            .into_iter()
            .find(|elem| elem.partition() == partition)
            .map(|elem| elem.offset())
            .expect("Unable to find topic offset.")
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
                warn!("Current offset invalid...");
                -1
            }
        }
    }

    pub(super) fn set_changelog_write_position(
        &self,
        changelog_topic: &str,
        partition: i32,
        offset: i64,
    ) {
        match self
            .changelog_positions
            .get(&(changelog_topic.to_string(), partition))
        {
            Some(stored_offset) => {
                stored_offset
                    .value()
                    .fetch_max(offset, std::sync::atomic::Ordering::Relaxed);
            }
            None => {
                self.changelog_positions
                    .insert((changelog_topic.to_owned(), partition), offset.into());
            }
        };
    }

    pub(super) fn get_changelog_write_position(
        &self,
        changelog_topic: &str,
        partition: i32,
    ) -> Option<i64> {
        self.changelog_positions
            .get(&(changelog_topic.to_string(), partition))
            .map(|offset| offset.value().load(std::sync::atomic::Ordering::Relaxed))
    }

    pub(super) fn get_lowest_stable_offset(
        &self,
        changelog_topic: &str,
        partition: i32,
    ) -> Option<i64> {
        self.consumer.context().get_lso(changelog_topic, partition)
    }

    pub(super) fn get_next_consumer_offset(
        &self,
        changelog_topic: &str,
        partition: i32,
    ) -> Option<i64> {
        self.consumer
            .context()
            .get_next_offset(changelog_topic, partition)
    }
}
