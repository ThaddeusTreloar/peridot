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

use crate::{error::engine::consumer_manager::ClientManagerError, state::error::StateStoreCreationError};

use super::{
    admin_manager::AdminManagerError, changelog_manager::ChangelogManagerError,
    metadata_manager::MetadataManagerError,
};

#[derive(Debug, thiserror::Error)]
pub enum PeridotEngineCreationError {
    #[error("Failed to client: {0}")]
    ClientCreationError(String),
    #[error(transparent)]
    AdminManagerError(#[from] AdminManagerError),
    #[error(transparent)]
    ClientManagerError(#[from] ClientManagerError),
    #[error(transparent)]
    ChangelogManagerError(#[from] ChangelogManagerError),
}

impl From<rdkafka::error::KafkaError> for PeridotEngineCreationError {
    fn from(error: rdkafka::error::KafkaError) -> Self {
        PeridotEngineCreationError::ClientCreationError(error.to_string())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PeridotEngineRuntimeError {
    #[error(transparent)]
    TableCreationError(#[from] TableCreationError),
    #[error(transparent)]
    BackendInitialisationError(#[from] StateStoreCreationError),
    #[error("Cyclic dependency detected for topic: {0}, you cannot both subsribe and publish to the same topic.")]
    CyclicDependencyError(String),
    #[error("Failed to create producer: {0}")]
    ProducerCreationError(#[from] KafkaError),
    #[error("Broken rebalance receiver.")]
    BrokenRebalanceReceiver,
    #[error(transparent)]
    ClientManagerError(#[from] ClientManagerError),
    #[error(transparent)]
    MetadataManagerError(#[from] MetadataManagerError),
}

#[derive(Debug, thiserror::Error)]
pub enum TableCreationError {
    #[error("Topic not found.")]
    TopicNotFound,
    #[error("Table exists.")]
    TableAlreadyExists,
    #[error("Failed to create table: {0}")]
    TableCreationError(String),
}

#[derive(Debug, thiserror::Error)]
pub enum TableRegistrationError {
    #[error("Table already registered.")]
    TableAlreadyRegistered,
}

#[derive(Debug, thiserror::Error)]
pub enum EngineInitialisationError {
    #[error("Attempted to reregister existing topic: {0}")]
    ConflictingTopicRegistration(String),
}
