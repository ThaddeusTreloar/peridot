use std::sync::Arc;

use rdkafka::{
    consumer::{stream_consumer::StreamPartitionQueue, StreamConsumer},
    ClientConfig,
};
use tracing::info;

use crate::{
    app::extensions::PeridotConsumerContext,
    state::{
        backend::{
            persistent::PersistentStateBackend, ReadableStateBackend, StateBackend,
            WriteableStateBackend,
        },
        StateStore,
    },
};

use self::{
    app_engine::{AppEngine, TableBuilder},
    error::{PeridotAppCreationError, PeridotAppRuntimeError},
};

pub mod app_engine;
pub mod error;
pub mod extensions;

pub type PeridotConsumer = StreamConsumer<PeridotConsumerContext>;
pub type PeridotPartitionQueue = StreamPartitionQueue<PeridotConsumerContext>;

pub trait PeridotTable<K, V, B>
where
    B: StateBackend + ReadableStateBackend<V> + WriteableStateBackend<V> + Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static + for<'de> serde::Deserialize<'de>,
{
    fn get_store(&self) -> Result<Arc<StateStore<'_, B, V>>, PeridotAppRuntimeError>;
}

pub struct PTable<'a, K, V, B = PersistentStateBackend<V>>
where
    V: Send + Sync + 'static + for<'de> serde::Deserialize<'de>,
{
    store: Arc<StateStore<'a, B, V>>,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<'a, K, V, B> PeridotTable<K, V, B> for PTable<'a, K, V, B>
where
    B: StateBackend + ReadableStateBackend<V> + WriteableStateBackend<V> + Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static + for<'de> serde::Deserialize<'de>,
{
    fn get_store(&self) -> Result<Arc<StateStore<'_, B, V>>, PeridotAppRuntimeError> {
        Ok(self.store.clone())
    }
}

pub trait PeridotStream<K, V> {}

pub struct PStream<K, V> {
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<K, V> Default for PStream<K, V> {
    fn default() -> Self {
        PStream {
            _key_type: std::marker::PhantomData,
            _value_type: std::marker::PhantomData,
        }
    }
}

impl<K, V> PeridotStream<K, V> for PStream<K, V> {}

#[derive(Default)]
pub struct PeridotApp {}

#[derive()]
pub struct PeridotAppBuilder {
    _config: ClientConfig,
    engine: Arc<AppEngine>,
}

impl PeridotAppBuilder {
    pub fn from_config(config: &ClientConfig) -> Result<Self, PeridotAppCreationError> {
        let engine = AppEngine::from_config(config)?;

        Ok(PeridotAppBuilder {
            _config: config.clone(),
            engine: Arc::new(engine),
        })
    }
}

impl PeridotAppBuilder {
    //type Error = PeridotAppRuntimeError;

    pub async fn run(self) -> Result<(), PeridotAppRuntimeError> {
        info!("Running PeridotApp");
        self.engine.run().await?;
        Ok(())
    }
    pub fn table<K, V, B>(
        &self,
        topic: &str,
    ) -> Result<TableBuilder<K, V, B>, PeridotAppRuntimeError>
    where
        B: StateBackend
            + ReadableStateBackend<V>
            + WriteableStateBackend<V>
            + Send
            + Sync
            + 'static,
        K: Send + Sync + 'static,
        V: Send + Sync + 'static + for<'de> serde::Deserialize<'de>,
    {
        let table_builder = TableBuilder::new(topic, self.engine.clone());

        Ok(table_builder)
    }

    pub fn stream<K, V>(&self, topic: &str) -> Result<PStream<K, V>, PeridotAppRuntimeError> {
        info!("Creating stream for topic: {}", topic);
        Ok(Default::default())
    }
}
