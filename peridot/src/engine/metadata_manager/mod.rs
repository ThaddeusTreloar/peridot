use std::sync::Arc;

use dashmap::DashMap;

use self::{table_metadata::TableMetadata, topic_metadata::TopicMetadata};

pub mod table_metadata;
pub mod topic_metadata;

#[derive(Debug, thiserror::Error)]
pub enum MetadataManagerError {
    #[error("MetadataError::TopicAlreadyRegistered: {}", topic)]
    TopicAlreadyRegistered {
        topic: String,
    },
    #[error("MetadataError::TableAlreadyRegistered: {}", table)]
    TableAlreadyRegistered {
        table: String,
    },
}

#[derive(Debug, Default)]
pub(crate) struct MetadataManager {
    app_id: String,
    table_metadata: Arc<DashMap<String, TableMetadata>>,
    source_topic_metadata: Arc<DashMap<String, TopicMetadata>>,
}

impl MetadataManager {
    pub fn new(app_id: &str) -> Self {
        Self {
            app_id: app_id.to_owned(),
            ..Default::default()
        }
    }

    pub(crate) fn register_source_topic(&self, topic: &str, metadata: &TopicMetadata) -> Result<(), MetadataManagerError> {
        if !self.source_topic_metadata.contains_key(topic) {
            Err(MetadataManagerError::TopicAlreadyRegistered { topic: topic.to_owned() })
        } else {
            self.source_topic_metadata.insert(topic.to_owned(), metadata.clone());

            Ok(())
        }
    }

    pub(crate) fn register_table(&self, table_name: &str, source_topic: &str) -> Result<TableMetadata, MetadataManagerError> {
        if !self.table_metadata.contains_key(table_name) {
            Err(MetadataManagerError::TableAlreadyRegistered { table: table_name.to_owned() })
        } else {
            let new_metadata = TableMetadata::new(source_topic);

            self.table_metadata.insert(table_name.to_owned(), new_metadata.clone());

            Ok(new_metadata)
        }
    }

    pub(crate) fn register_table_with_changelog(&self, table_name: &str, source_topic: &str) -> Result<TableMetadata, MetadataManagerError> {
        if !self.table_metadata.contains_key(table_name) {
            Err(MetadataManagerError::TableAlreadyRegistered { table: table_name.to_owned() })
        } else {
            let changelog_topic = self.derive_changelog_topic(table_name);

            let mut new_metadata = TableMetadata::new_with_changelog(source_topic, &changelog_topic);

            self.table_metadata.insert(table_name.to_owned(), new_metadata.clone());

            Ok(new_metadata)
        }
    }

    pub(crate) fn get_table_metadata(&self, table: &str) -> Option<TableMetadata> {
        self.table_metadata.get(table).map(|m|m.clone())
    }

    pub(crate) fn get_topic_metadata(&self, topic: &str) -> Option<TopicMetadata> {
        self.source_topic_metadata.get(topic).map(|m|m.clone())
    }

    pub(crate) fn get_tables_for_topic(&self, topic: &str) -> Vec<String> {
        if !self.source_topic_metadata.contains_key(topic) {
            Vec::new()
        } else {
            self.table_metadata
                .iter()
                .filter(|entry| entry.value().source_topic().eq(topic))
                .map(|entry| entry.key().to_owned())
                .collect()
        }

    }

    pub(crate) fn derive_changelog_topic(&self, table_name: &str) -> String {
        format!("{}-{}-Changelog", &self.app_id, table_name)
    }
}
