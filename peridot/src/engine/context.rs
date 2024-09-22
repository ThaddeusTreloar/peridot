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

use std::{sync::Arc, time::Duration};

use rdkafka::consumer::{Consumer, ConsumerGroupMetadata};
use tokio::time::sleep;
use tracing::info;

use crate::{app::config::PeridotConfig, error::engine::consumer_manager::ClientManagerError, state};

use super::{
    admin_manager::{AdminManager, AdminManagerError},
    changelog_manager::{ChangelogManager, ChangelogManagerError, Watermarks},
    consumer_manager::{self,  ConsumerManager},
    metadata_manager::{table_metadata, MetadataManager},
    wrapper::{partitioner::PeridotPartitioner, timestamp::TimestampExtractor},
    AppEngine, TableMetadata,
};

#[derive(Debug, thiserror::Error)]
pub enum EngineContextError {
    #[error("EngineContextError::CloseChangelogError, failed to close changelog partition for {}:{} caused by: {}", topic, partition, err)]
    CloseChangelogError {
        topic: String,
        partition: i32,
        err: rdkafka::error::KafkaError,
    },
    #[error("EngineContextError::UnregisteredSourceTopic, source topic '{}' not registered for state store '{}'", source_topic, state_store)]
    UnregisteredSourceTopic {
        source_topic: String,
        state_store: String,
    },
    #[error(transparent)]
    ConsumerManagerError(#[from] ClientManagerError),
    #[error(
        "EngineContextError::CreateTopicError, topic '{}' due to {}",
        topic,
        error
    )]
    CreateTopicError {
        topic: String,
        error: AdminManagerError,
    },
}

pub struct EngineContext {
    pub(super) config: PeridotConfig,
    pub(super) admin_manager: AdminManager,
    pub(super) consumer_manager: ConsumerManager,
    pub(super) metadata_manager: MetadataManager,
    pub(super) changelog_manager: ChangelogManager,
}

impl EngineContext {
    pub(super) fn new(
        config: PeridotConfig,
        admin_manager: AdminManager,
        client_manager: ConsumerManager,
        metadata_manager: MetadataManager,
        changelog_manager: ChangelogManager,
    ) -> Self {
        Self {
            config,
            admin_manager,
            consumer_manager: client_manager,
            metadata_manager,
            changelog_manager,
        }
    }

    pub(crate) fn store_metadata(&self, store_name: &str) -> TableMetadata {
        self.metadata_manager
            .get_table_metadata(store_name)
            .expect("Unable to get store for facade distributor")
    }

    pub(crate) fn group_metadata(&self) -> ConsumerGroupMetadata {
        self.consumer_manager
            .consumer_ref()
            .group_metadata()
            .expect("Failed to get consumer group metadata.")
    }

    pub(crate) fn watermark_for_changelog(&self, store_name: &str, partition: i32) -> Watermarks {
        let changelog_topic = self
            .metadata_manager
            .get_changelog_topic_for_store(store_name);

        self.changelog_manager
            .get_watermark_for_changelog(&changelog_topic, partition)
            .expect("Failed to fetch watermarks")
    }

    pub(crate) fn close_changelog_stream(
        &self,
        store_name: &str,
        partition: i32,
    ) -> Result<(), EngineContextError> {
        let changelog_topic = self
            .metadata_manager
            .get_changelog_topic_for_store(store_name);

        if let Err(err) = self
            .changelog_manager
            .close_changelog_stream(&changelog_topic, partition)
        {
            match err {
                ChangelogManagerError::CloseChangelog {
                    topic,
                    partition,
                    err,
                } => Err(EngineContextError::CloseChangelogError {
                    topic,
                    partition,
                    err,
                })?,
                _ => panic!("Unknown error while closing changelog partition."),
            };
        }

        Ok(())
    }

    pub(crate) fn get_changelog_topic_name(&self, store_name: &str) -> String {
        self.metadata_manager
            .get_changelog_topic_for_store(store_name)
    }

    async fn topic_exists(&self, topic: &str) -> Result<bool, ClientManagerError> {
        match self
            .consumer_manager
            .get_topic_metadata(topic)?
            .topics()
            .first()
            .and_then(|topic| topic.error())
        {
            Some(rdkafka_sys::RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART)
            | Some(rdkafka_sys::RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC) => Ok(false),
            Some(e) => panic!(
                "Unknown error while fetching metadata for topic: {}, error: {:?}",
                &topic, e
            ),
            None => Ok(true),
        }
    }

    async fn topic_ready(&self, topic: &str, is_changelog: bool) -> bool {
        if is_changelog {
            self.changelog_manager
                .get_watermark_for_changelog(topic, 0)
                .is_ok()
        } else {
            todo!("Check non-changelog topic ready.")
        }
    }

    async fn create_topic(
        &self,
        topic: &str,
        partition_count: i32,
    ) -> Result<(), EngineContextError> {
        self.admin_manager
            .create_topic(&topic, partition_count)
            .await
            .map_err(|e| EngineContextError::CreateTopicError {
                topic: topic.to_owned(),
                error: e,
            })
    }

    pub(crate) async fn register_topic_store(
        &self,
        source_topic: &str,
        store_name: &str,
    ) -> Result<TableMetadata, EngineContextError> {
        let changelog_topic = self.metadata_manager.derive_changelog_topic(store_name);

        if self.topic_exists(&changelog_topic).await? {
            info!("Changelog topic already exists, topic: {}", changelog_topic);
        } else {
            info!(
                "Changelog topic not on broker, creating, topic: {}",
                changelog_topic
            );
            // TODO: set relication factor from config.

            match self.metadata_manager.get_topic_metadata(source_topic) {
                None => Err(EngineContextError::UnregisteredSourceTopic {
                    source_topic: source_topic.to_owned(),
                    state_store: store_name.to_owned(),
                })?,
                Some(metadata) => {
                    self.create_topic(&changelog_topic, metadata.partition_count())
                        .await?;

                    while !self.topic_ready(&changelog_topic, true).await {
                        info!("Changelog topic not ready, topic: {}", &changelog_topic);
                        info!("Waiting for changelog topic to be ready...");

                        sleep(Duration::from_millis(500)).await;
                    }

                    info!("Changelog topic ready, topic: {}", &changelog_topic);
                }
            }
        }

        Ok(self
            .metadata_manager
            .register_table_with_changelog(store_name, source_topic)
            .expect("Failed to register table."))
    }

    pub(crate) fn seek_changlog_consumer(
        &self,
        state_store: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), ChangelogManagerError> {
        let changelog_topic = self.get_changelog_topic_name(state_store);

        self.changelog_manager
            .seek_consumer(&changelog_topic, partition, offset)
    }

    pub(crate) fn get_changelog_partition_offset(&self, state_store: &str, partition: i32) -> i64 {
        let changelog_topic = self.get_changelog_topic_name(state_store);

        self.changelog_manager
            .get_topic_offset(&changelog_topic, partition)
    }

    pub(crate) fn get_consumer_position(&self, topic: &str, partition: i32) -> i64 {
        self.consumer_manager
            .get_topic_partition_consumer_position(topic, partition)
    }

    pub(crate) fn get_changelog_consumer_position(&self, state_store: &str, partition: i32) -> i64 {
        let changelog_topic = self.get_changelog_topic_name(state_store);

        self.changelog_manager
            .get_topic_consumer_position(&changelog_topic, partition)
    }

    pub(crate) fn set_changelog_write_position(
        &self,
        changelog_topic: &str,
        partition: i32,
        offset: i64,
    ) {
        self.changelog_manager
            .set_changelog_write_position(changelog_topic, partition, offset)
    }

    pub(crate) fn get_changelog_write_position(
        &self,
        state_store: &str,
        partition: i32,
    ) -> Option<i64> {
        let changelog_topic = self.get_changelog_topic_name(state_store);

        self.changelog_manager
            .get_changelog_write_position(&changelog_topic, partition)
    }

    pub(crate) fn get_changelog_lso(&self, state_store: &str, partition: i32) -> Option<i64> {
        let changelog_topic = self.get_changelog_topic_name(state_store);

        self.changelog_manager
            .get_lowest_stable_offset(&changelog_topic, partition)
    }

    pub(crate) fn get_changelog_next_offset(
        &self,
        state_store: &str,
        partition: i32,
    ) -> Option<i64> {
        let changelog_topic = self.get_changelog_topic_name(state_store);

        self.changelog_manager
            .get_next_consumer_offset(&changelog_topic, partition)
    }
}
