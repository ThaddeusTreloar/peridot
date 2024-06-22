use std::sync::Arc;

use rdkafka::consumer::{Consumer, ConsumerGroupMetadata};

use crate::app::config::PeridotConfig;

use super::{
    admin_manager::AdminManager,
    changelog_manager::{ChangelogManager, ChangelogManagerError, Watermarks},
    client_manager::{self, ClientManager},
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
}

pub struct EngineContext {
    pub(super) config: PeridotConfig,
    pub(super) admin_manager: AdminManager,
    pub(super) client_manager: ClientManager,
    pub(super) metadata_manager: MetadataManager,
    pub(super) changelog_manager: ChangelogManager,
}

impl EngineContext {
    pub(super) fn new(
        config: PeridotConfig,
        admin_manager: AdminManager,
        client_manager: ClientManager,
        metadata_manager: MetadataManager,
        changelog_manager: ChangelogManager,
    ) -> Self {
        Self {
            config,
            admin_manager,
            client_manager,
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
        self.client_manager
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

    pub(crate) fn register_topic_store(
        &self,
        source_topic: &str,
        store_name: &str,
    ) -> Result<TableMetadata, EngineContextError> {
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

    pub(crate) fn get_changelog_consumer_position(&self, state_store: &str, partition: i32) -> i64 {
        let changelog_topic = self.get_changelog_topic_name(state_store);

        self.changelog_manager
            .get_topic_consumer_position(&changelog_topic, partition)
    }

    pub(crate) fn get_changelog_lowest_stable_offset(
        &self,
        state_store: &str,
        partition: i32,
    ) -> Option<i64> {
        let changelog_topic = self.get_changelog_topic_name(state_store);

        self.changelog_manager
            .get_lowest_stable_offset(&changelog_topic, partition)
    }
}
