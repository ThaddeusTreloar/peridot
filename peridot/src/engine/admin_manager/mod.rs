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

use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic},
    client::DefaultClientContext,
    config::FromClientConfig,
};

use crate::app::config::PeridotConfig;

#[derive(Debug, thiserror::Error)]
pub enum AdminManagerError {
    #[error("ClientManagerError::CreateClientError: Failed to create client while initialising AdminManager -> {}", 0)]
    CreateClientError(rdkafka::error::KafkaError),
    #[error(
        "ClientManagerError::CreateTopicError: Failed to create topic '{}' due to {}",
        topic,
        error
    )]
    CreateTopicError {
        topic: String,
        error: rdkafka::error::KafkaError,
    },
}

pub struct AdminManager {
    client: AdminClient<DefaultClientContext>,
}

impl AdminManager {
    pub(super) fn new(config: &PeridotConfig) -> Result<Self, AdminManagerError> {
        let client = AdminClient::from_config(config.client_config())
            .map_err(AdminManagerError::CreateClientError)?;

        Ok(Self { client })
    }

    pub(super) async fn create_topic(
        &self,
        topic: &str,
        partitions: i32,
    ) -> Result<(), AdminManagerError> {
        let new_topic = vec![NewTopic::new(
            topic,
            partitions,
            rdkafka::admin::TopicReplication::Fixed(3),
        )];

        match self
            .client
            .create_topics(&new_topic, &AdminOptions::new())
            .await
        {
            Ok(v) => match v.first().expect("No topics successful") {
                Ok(_) => Ok(()),
                Err(e) => panic!("Failed to create topic"),
            },
            Err(e) => Err(AdminManagerError::CreateTopicError {
                topic: topic.to_owned(),
                error: e,
            }),
        }
    }
}
