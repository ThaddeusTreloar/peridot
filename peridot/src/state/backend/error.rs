

#[derive(Debug, thiserror::Error)]
pub enum PersistantStateBackendCreationError {
    #[error("Failed to create persistant state backend: {0}")]
    LoadFileError(String),
}

impl From<surrealdb::Error> for PersistantStateBackendCreationError {
    fn from(err: surrealdb::Error) -> Self {
        PersistantStateBackendCreationError::LoadFileError(err.to_string())
    }
}