use std::sync::Arc;

use crossbeam::atomic::AtomicCell;
use futures::Future;
use rdkafka::{ClientConfig, consumer::{StreamConsumer, Consumer, stream_consumer::StreamPartitionQueue, ConsumerContext}};
use tokio::sync::broadcast::Sender;
use tracing::info;

use crate::app::extensions::PeridotConsumerContext;

use self::{error::{PeridotAppCreationError, PeridotAppRuntimeError}, app_engine::{AppEngine, StateType, TableBuilder}};

pub mod error;
pub mod extensions;
pub mod app_engine;

pub type PeridotConsumer = StreamConsumer<PeridotConsumerContext>;
pub type PeridotPartitionQueue = StreamPartitionQueue<PeridotConsumerContext>;

pub trait PeridotTable<K, V> {}

pub struct PTable<K, V> {
    _key: std::marker::PhantomData<K>,
    _value: std::marker::PhantomData<V>,
}

impl<K, V> Default for PTable<K, V> {
    fn default() -> Self {
        PTable {
            _key: std::marker::PhantomData,
            _value: std::marker::PhantomData,
        }
    }
}

impl <K, V> PeridotTable<K, V> for PTable<K, V> {}

pub trait PeridotStream<K, V> {}

pub struct PStream<K, V> {
    _key: std::marker::PhantomData<K>,
    _value: std::marker::PhantomData<V>,
}

impl <K, V> Default for PStream<K, V> {
    fn default() -> Self {
        PStream {
            _key: std::marker::PhantomData,
            _value: std::marker::PhantomData,
        }
    }
}

impl<K, V> PeridotStream<K, V> for PStream<K, V> {}

#[derive(Default)]
pub struct PeridotApp {}

pub trait App {
    type Error;

    fn run(&self) -> impl Future<Output = Result<(), Self::Error>>;
    fn table<K, V>(&self, topic: &str) -> Result<impl PeridotTable<K, V>, Self::Error>;
    fn stream<K, V>(&self, topic: &str) -> Result<impl PeridotStream<K, V>, Self::Error>;
}

#[derive()]
pub struct PeridotAppBuilder {
    config: ClientConfig,
    engine: Arc<AppEngine>,
}

impl PeridotAppBuilder {
    pub fn new(config: &ClientConfig) -> Result<Self, PeridotAppCreationError> {
        let engine = AppEngine::from_config(config)?;

        Ok(PeridotAppBuilder {
            config: config.clone(),
            engine: Arc::new(engine),
        })
    }
}

impl PeridotAppBuilder {
    //type Error = PeridotAppRuntimeError;

    async fn run(self) -> Result<PeridotApp, PeridotAppRuntimeError>{
        info!("Running PeridotApp");
        Ok(Default::default())
    }

    fn table<K, V>(&self, topic: &str) -> Result<TableBuilder<K, V>, PeridotAppRuntimeError> {
        let table_builder = TableBuilder::<K, V>::new(topic, self.engine.clone());

        Ok(table_builder)
    }

    fn stream<K, V>(&self, topic: &str) -> Result<PStream<K, V>, PeridotAppRuntimeError> {
        info!("Creating stream for topic: {}", topic);
        Ok(Default::default())
    }
}


async fn tester() -> Result<(), PeridotAppRuntimeError> {
    let app_builder = PeridotAppBuilder::new(&ClientConfig::new()).unwrap();

    let _table_builder = app_builder.table::<String, String>("test.topic").unwrap()
        .with_state_type(StateType::InMemory)
        .build()
        .unwrap();

    let _ = app_builder
        .run()
        .await?;

    Ok(())
}   