use futures::Future;
use rdkafka::{ClientConfig, consumer::{StreamConsumer, Consumer, stream_consumer::StreamPartitionQueue, ConsumerContext}};
use tokio::sync::broadcast::Sender;
use tracing::info;

use crate::app::extensions::PeridotConsumerContext;

use self::error::{PeridotAppCreationError, PeridotAppRuntimeError};

pub mod error;
pub mod extensions;

pub trait PeridotTable {}

pub struct ConcreteTable {}

impl PeridotTable for ConcreteTable {}

pub trait PeridotStream {}

pub struct ConcreteStream {}

impl PeridotStream for ConcreteStream {}

pub trait App {
    type Error;

    fn run(self) -> impl Future<Output = Result<(), Self::Error>>;
    fn table<K, V>(&self, topic: &str) -> Result<impl PeridotTable, Self::Error>;
    fn stream<K, V>(&self, topic: &str) -> Result<impl PeridotStream, Self::Error>;
}

#[derive()]
pub struct PeridotApp {
    config: ClientConfig,
    source: StreamConsumer,
}

impl PeridotApp {
    pub fn new(config: &ClientConfig) -> Result<Self, PeridotAppCreationError> {
        Ok(PeridotApp {
            config: config.clone(),
            source: config.create()?,
        })
    }
}

impl App for PeridotApp {
    type Error = PeridotAppRuntimeError;

    async fn run(self) -> Result<(), Self::Error>{
        unimplemented!("Running PeridotApp");
        Ok(())
    }

    fn table<K, V>(&self, topic: &str) -> Result<impl PeridotTable, Self::Error> {
        let mut subscription = self.source
            .subscription()
            .expect("Failed to get subscription.")
            .elements()
            .into_iter()
            .map(|tp|tp.topic().to_string())
            .collect::<Vec<String>>();

        if subscription.contains(&topic.to_string()) {
            return Err(PeridotAppRuntimeError::SourceConflictError(topic.to_string()));
        }

        subscription.push(topic.to_string());

        self.source.subscribe(
            subscription
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>()
                .as_slice()
            )
            .expect("Failed to subscribe to topic.");

        let (queue_sender, queue_receiver) = tokio::sync::mpsc::channel::<StreamPartitionQueue<PeridotConsumerContext>>(1024);

        info!("Creating table for topic: {}", topic);
        Ok(ConcreteTable {})
    }

    fn stream<K, V>(&self, topic: &str) -> Result<impl PeridotStream, Self::Error> {
        info!("Creating stream for topic: {}", topic);
        Ok(ConcreteStream {})
    }
}