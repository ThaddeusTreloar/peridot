use std::sync::Arc;

use super::{
    changelog_manager::ChangelogManager, client_manager::{self, ClientManager}, metadata_manager::MetadataManager, wrapper::{partitioner::PeridotPartitioner, timestamp::TimestampExtractor}, AppEngine, TableMetadata
};

#[derive(Clone)]
pub struct EngineContext {
    pub(super) client_manager: Arc<ClientManager>,
    pub(super) metadata_manager: Arc<MetadataManager>,
    pub(super) changelog_manager: Arc<ChangelogManager>,
}

impl EngineContext {
    pub(crate) fn client_manager(&self) -> Arc<ClientManager> {
        self.client_manager.clone()
    }
    pub(crate) fn metadata_manager(&self) -> Arc<MetadataManager> {
        self.metadata_manager.clone()
    }
    pub(crate) fn changelog_manager(&self) -> Arc<ChangelogManager> {
        self.changelog_manager.clone()
    }
}