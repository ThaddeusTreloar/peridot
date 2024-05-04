

#[derive(Debug, thiserror::Error)]
pub enum BackendCreationError {
    #[error("Failed to create persistant state backend: {0}")]
    LoadFileError(String),
}