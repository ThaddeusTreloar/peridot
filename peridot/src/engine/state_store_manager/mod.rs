use std::sync::Arc;

use dashmap::DashMap;

use crate::state::backend::StateBackend;

pub type StateStoreMap<B> = Arc<DashMap<(String, i32), Arc<B>>>;

#[derive(Debug, thiserror::Error)]
pub enum StateStoreManagerError {
    #[error("State store exists for {}:{}", topic, partition)]
    StateStoreExists {
        topic: String,
        partition: i32,
    },
    #[error("Failed to create state store for {}:{} caused by {}", topic, partition, err)]
    StateStoreCreation {
        topic: String,
        partition: i32,
        err: Box<dyn std::error::Error>
    },
}

#[derive()]
pub(crate) struct StateStoreManager<B> {
    state_stores: StateStoreMap<B>,
}

impl <B> Default for StateStoreManager<B> {
    fn default() -> Self {
        Self {
            state_stores: Default::default(),
        }
    }
}

impl <B> StateStoreManager<B> 
where
    B: StateBackend,
    B::Error: 'static,
{
    pub(super) fn new() -> Self {
        Default::default()
    }

    pub(crate) fn get_state_store(&self, source_topic: &str, partition: i32) -> Option<Arc<B>> {
        self.state_stores.get(&(source_topic.to_owned(), partition)).map(|s|s.clone())
    }

    pub(crate) fn create_state_store(&self, source_topic: &str, partition: i32) -> Result<(), StateStoreManagerError> {
        if self.state_stores.contains_key(&(source_topic.to_owned(), partition)) {
            Err(StateStoreManagerError::StateStoreExists { topic: source_topic.to_owned(), partition })?
        }

        let backend = B::with_source_topic_name_and_partition(source_topic, partition)
            .map_err(|err| StateStoreManagerError::StateStoreCreation { topic: source_topic.to_owned(), partition, err: Box::new(err) })?;

        self.state_stores.insert((source_topic.to_owned(), partition), Arc::new(backend));

        Ok(())
    }
}