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

#[derive(thiserror::Error, Debug)]
pub enum StateStoreCreationError {
    #[error("Failed to create consumer: {0}")]
    FromConfigError(String),
}

impl From<KafkaError> for StateStoreCreationError {
    fn from(err: KafkaError) -> Self {
        StateStoreCreationError::FromConfigError(err.to_string())
    }
}
