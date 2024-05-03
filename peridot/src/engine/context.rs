use std::sync::Arc;

use crate::app::config::PeridotConfig;

use super::{
    admin_manager::AdminManager, changelog_manager::{ChangelogManager, ChangelogManagerError, Watermarks}, client_manager::{self, ClientManager}, metadata_manager::MetadataManager, wrapper::{partitioner::PeridotPartitioner, timestamp::TimestampExtractor}, AppEngine, TableMetadata
};

#[derive(Debug, thiserror::Error)]
pub enum EngineContextError {
    #[error("EngineContextError::CloseChangelogError, failed to close changelog partition for {}:{} caused by: {}", topic, partition, err)]
    CloseChangelogError{
        topic: String,
        partition: i32,
        err: rdkafka::error::KafkaError,
    },
}

#[derive(Clone)]
pub struct EngineContext {
    pub(super) config: PeridotConfig,
    pub(super) admin_manager: Arc<AdminManager>,
    pub(super) client_manager: Arc<ClientManager>,
    pub(super) metadata_manager: Arc<MetadataManager>,
    pub(super) changelog_manager: Arc<ChangelogManager>,
}

impl EngineContext {
    pub(super) fn new(
        config: PeridotConfig,
        admin_manager: Arc<AdminManager>,
        client_manager: Arc<ClientManager>,
        metadata_manager: Arc<MetadataManager>,
        changelog_manager: Arc<ChangelogManager>,        
    ) -> Self {
        Self {
            config,
            admin_manager,
            client_manager,
            metadata_manager,
            changelog_manager,
        }
    }

    // TODO: change the visibilty of these so that components cannot
    // directly access engine component managers.
    pub(crate) fn client_manager(&self) -> Arc<ClientManager> {
        self.client_manager.clone()
    }

    pub(crate) fn metadata_manager(&self) -> Arc<MetadataManager> {
        self.metadata_manager.clone()
    }

    pub(crate) fn changelog_manager(&self) -> Arc<ChangelogManager> {
        self.changelog_manager.clone()
    }

    pub(crate) fn get_watermark_for_changelog(&self, table_name: &str, partition: i32) -> Watermarks {
        let changelog_topic = self.metadata_manager.derive_changelog_topic(table_name);

        self.changelog_manager
            .get_watermark_for_changelog(&changelog_topic, partition)
            .expect("Failed to fetch watermarks")
    }

    pub(crate) fn close_changelog_stream(&self, table_name: &str, partition: i32) -> Result<(), EngineContextError> {
        let changelog_topic = self.metadata_manager.derive_changelog_topic(table_name);

        if let Err(err) = self.changelog_manager.close_changelog_stream(&changelog_topic, partition) {
            match err {
                ChangelogManagerError::CloseChangelogError { 
                    topic, 
                    partition, 
                    err 
                } => Err(
                    EngineContextError::CloseChangelogError { topic, partition, err }
                )?,
                _ => panic!("Unknown error while closing changelog partition.")
            };
        }

        Ok(())
    }

    pub(crate) fn register_state_store(&self, source_topic: &str, state_name: &str) -> Result<(), EngineContextError> {
            
    }
}