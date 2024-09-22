use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum StateBackendCreationError {
    #[error("State store exists for {}:{}", store_name, partition)]
    StateStoreExists { store_name: String, partition: i32 },
    #[error(
        "Failed to create state store for {}:{} caused by {}",
        store_name,
        partition,
        err
    )]
    CreationFailed {
        store_name: String,
        partition: i32,
        is_recoverable: bool,
        err: Box<dyn std::error::Error>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum StateBackendFetchError {
    #[error("State store exists for {}:{}", store_name, partition)]
    StateStoreNotExists { store_name: String, partition: i32 },
    #[error(
        "Failed to get state store for {}:{} caused by {}",
        store_name,
        partition,
        err
    )]
    FetchFailed {
        store_name: String,
        partition: i32,
        is_recoverable: bool,
        err: Box<dyn std::error::Error>,
    },
}

pub trait StateBackend<B> {
    fn get_state_store(
        &self,
        store_name: &str,
        partition: i32,
    ) -> Result<Arc<B>, StateBackendFetchError>;

    fn create_state_store(&self, store_name: &str) -> Result<(), StateBackendCreationError>;
}
