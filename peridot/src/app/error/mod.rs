use super::app_engine::error::PeridotEngineCreationError;


#[derive(Debug, thiserror::Error)]
pub enum PeridotAppCreationError {
    #[error("Failed to initialise engine: {0}")]
    EngineCreationError(#[from] PeridotEngineCreationError),
}

#[derive(Debug, thiserror::Error)]
pub enum PeridotAppRuntimeError {
    #[error("Failed to run: {0}")]
    RunError(String),
    #[error("Created multiple source with the same topic: {0}")]
    SourceConflictError(String),
}