

#[derive(Debug, thiserror::Error)]
pub enum BackendCreationError {
    #[error("Failed to create persistant state backend: {0}")]
    LoadFileError(String),
}

impl From<surrealdb::Error> for BackendCreationError {
    fn from(err: surrealdb::Error) -> Self {
        BackendCreationError::LoadFileError(err.to_string())
    }
}