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

use std::time::Duration;

use rdkafka::{
    config::FromClientConfig,
    producer::{FutureProducer, Producer},
};

use crate::app::config::PeridotConfig;

use super::{util::ExactlyOnce, DeliverySemantics};

const TRANSACTIONAL_ID: &str = "transactional.id";

#[derive(Debug, thiserror::Error)]
pub enum ProducerFactoryError {
    #[error(
        "ProducerFactoryError::ProducerCreationError, failed to create producer, caused by: {}",
        err
    )]
    ProducerCreationError { err: rdkafka::error::KafkaError },
    #[error("ProducerFactoryError::InitTransactionsError , failed to init transactions for producer, caused by: {}", err)]
    InitTransactionsError { err: rdkafka::error::KafkaError },
}

pub struct ProducerFactory {
    config: PeridotConfig,
    delivery_semantics: DeliverySemantics,
}

impl ProducerFactory {
    pub fn new(config: PeridotConfig, delivery_semantics: DeliverySemantics) -> Self {
        Self {
            config,
            delivery_semantics,
        }
    }

    pub fn create_producer(
        &self,
        source_topic: &str,
        partition: i32,
    ) -> Result<FutureProducer, ProducerFactoryError> {
        let transaction_id = format!("peridot-{}-{}", source_topic, partition);

        // TODO: we are cloning the config as it is uncertain whether the created producer references
        // the config or clones it internally. Currently the rdkafka library creates a new CString
        // for each entry, but we may not want to rely on this behaviour.
        //
        // At a later date we can review this approach.
        let mut config = self.config.new_client_config();

        config
            .set(TRANSACTIONAL_ID, transaction_id)
            .set("statistics.interval.ms", "0");

        let producer = FutureProducer::from_config(&config)
            .map_err(|err| ProducerFactoryError::ProducerCreationError { err })?;

        if let DeliverySemantics::ExactlyOnce = self.delivery_semantics {
            producer
                .init_transactions(Duration::from_millis(2500))
                .map_err(|err| ProducerFactoryError::InitTransactionsError { err })?;

            producer
                .begin_transaction()
                .expect("Failed to begin initial transaction.");
            //.map_err(|err| ProducerFactoryError::InitTransactionsError { err })?;
        }

        Ok(producer)
    }

    pub fn config(&self) -> &PeridotConfig {
        &self.config
    }
}
