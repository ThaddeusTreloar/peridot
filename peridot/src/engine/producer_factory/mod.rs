use std::time::Duration;

use rdkafka::{config::FromClientConfig, producer::{FutureProducer, Producer}};

use crate::app::config::PeridotConfig;

use super::{util::ExactlyOnce, DeliverySemantics};

#[derive(Debug, thiserror::Error)]
pub enum ProducerFactoryError {
    #[error("ProducerFactoryError::ProducerCreationError, failed to create producer, caused by: {}", err)]
    ProducerCreationError{
        err: rdkafka::error::KafkaError,
    },
    #[error("ProducerFactoryError::InitTransactionsError , failed to init transactions for producer, caused by: {}", err)]
    InitTransactionsError {
        err: rdkafka::error::KafkaError,
    },
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

    pub fn create_producer(&self) -> Result<FutureProducer, ProducerFactoryError> {
        todo!("Set transaction id.");

        let producer = FutureProducer::from_config(self.config.client_config())
            .map_err(|err| ProducerFactoryError::ProducerCreationError { err })?;

        todo!("Ensure init_transactions will fence previous generation producers.");

        if let DeliverySemantics::ExactlyOnce = self.delivery_semantics {
            producer
                .init_transactions(Duration::from_millis(2500))
                .map_err(|err| ProducerFactoryError::InitTransactionsError { err })?;
        }
    
        Ok(producer)
    }

    pub fn config(&self) -> &PeridotConfig {
        &self.config
    }
}
