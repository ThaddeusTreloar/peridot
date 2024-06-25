/*
 * Copyright 2024 Thaddeus Treloar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

use rdkafka::error::KafkaError;

use crate::engine::error::{PeridotEngineCreationError, PeridotEngineRuntimeError};

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
    #[error("Failed to initialise engine: {0}")]
    EngineRuntimeError(#[from] PeridotEngineRuntimeError),
    #[error("Sink error: {0}")]
    SinkError(#[from] KafkaError),
    #[error("Failed to join job: {0}")]
    JobJoinError(#[from] tokio::task::JoinError),
}
