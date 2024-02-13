use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::Future;
use rdkafka::{
    consumer::{stream_consumer::StreamPartitionQueue, StreamConsumer},
    ClientConfig,
};
use tracing::info;

use crate::{
    app::extensions::PeridotConsumerContext,
    state::backend::{ReadableStateBackend, StateBackend, WriteableStateBackend},
};

use self::{
    app_engine::{streams::StreamBuilder, AppEngine, util::{ExactlyOnce, DeliveryGuaranteeType, AtMostOnce, AtLeastOnce}},
    config::PeridotConfig,
    error::{PeridotAppCreationError, PeridotAppRuntimeError},
    psink::PSink,
    pstream::PStream,
    ptable::PTable,
};

pub mod app_engine;
pub mod config;
pub mod error;
pub mod extensions;
pub mod wrappers;
pub mod psink;
pub mod pstream;
pub mod ptable;

pub type PeridotConsumer = StreamConsumer<PeridotConsumerContext>;
pub type PeridotPartitionQueue = StreamPartitionQueue<PeridotConsumerContext>;

#[derive()]
pub struct PeridotApp<G = ExactlyOnce> 
where G: DeliveryGuaranteeType
{
    _config: PeridotConfig,
    engine: Arc<AppEngine<G>>,
}

impl <G> PeridotApp<G> 
where G: DeliveryGuaranteeType
{
    pub fn from_client_config(config: &ClientConfig) -> Result<Self, PeridotAppCreationError> {
        let config = PeridotConfig::from(config);

        let engine = AppEngine::from_config(&config)?;

        Ok(Self {
            _config: config,
            engine: Arc::new(engine),
        })
    }

    pub fn from_config(mut config: PeridotConfig) -> Result<Self, PeridotAppCreationError> {
        config.clean_config();

        let engine = AppEngine::from_config(&config)?;

        Ok(Self {
            _config: config.clone(),
            engine: Arc::new(engine),
        })
    }

    pub async fn run(&self) -> Result<(), PeridotAppRuntimeError> {
        info!("Running PeridotApp");
        self.engine.run().await?;
        Ok(())
    }

}

impl PeridotApp<AtMostOnce> {
    pub async fn table<K, V, B>(
        &self,
        topic: &str,
    ) -> Result<PTable<K, V, B>, PeridotAppRuntimeError>
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
        let engine = self.engine.clone();

        Ok(AppEngine::<AtMostOnce>::table(engine, topic.to_string()).await?)
    }

    pub async fn arc_table<K, V, B>(
        self: Arc<Self>,
        topic: &str,
    ) -> Result<PTable<K, V, B>, PeridotAppRuntimeError>
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
        let engine = self.engine.clone();

        Ok(AppEngine::<AtMostOnce>::table(engine, topic.to_string()).await?)
    }

    pub async fn stream(&self, topic: &str) -> Result<PStream<AtMostOnce>, PeridotAppRuntimeError> {
        info!("Creating stream for topic: {}", topic);
        Ok(AppEngine::<AtMostOnce>::stream(self.engine.clone(), topic.to_string()).await?)
    }

    pub async fn stream_builder(&self, topic: &str) -> StreamBuilder<AtMostOnce> {
        StreamBuilder::<AtMostOnce>::new(topic, self.engine.clone())
    }

    pub async fn sink<K, V>(&self, topic: &str) -> Result<PSink<K, V, AtMostOnce>, PeridotAppRuntimeError> {
        info!("Creating sink for topic: {}", topic);
        Ok(AppEngine::<AtMostOnce>::sink(self.engine.clone(), topic.to_string()).await?)
    }
}

impl PeridotApp<AtLeastOnce> {
    pub async fn table<K, V, B>(
        &self,
        topic: &str,
    ) -> Result<PTable<K, V, B>, PeridotAppRuntimeError>
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
        let engine = self.engine.clone();

        Ok(AppEngine::<AtLeastOnce>::table(engine, topic.to_string()).await?)
    }

    pub async fn arc_table<K, V, B>(
        self: Arc<Self>,
        topic: &str,
    ) -> Result<PTable<K, V, B>, PeridotAppRuntimeError>
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
        let engine = self.engine.clone();

        Ok(AppEngine::<AtLeastOnce>::table(engine, topic.to_string()).await?)
    }

    pub async fn stream(&self, topic: &str) -> Result<PStream<AtLeastOnce>, PeridotAppRuntimeError> {
        info!("Creating stream for topic: {}", topic);
        Ok(AppEngine::<AtLeastOnce>::stream(self.engine.clone(), topic.to_string()).await?)
    }

    pub async fn stream_builder(&self, topic: &str) -> StreamBuilder<AtLeastOnce> {
        StreamBuilder::<AtLeastOnce>::new(topic, self.engine.clone())
    }

    pub async fn sink<K, V>(&self, topic: &str) -> Result<PSink<K, V, AtLeastOnce>, PeridotAppRuntimeError> {
        info!("Creating sink for topic: {}", topic);
        Ok(AppEngine::<AtLeastOnce>::sink(self.engine.clone(), topic.to_string()).await?)
    }
}

impl PeridotApp<ExactlyOnce> {
    pub async fn table<K, V, B>(
        &self,
        topic: &str,
    ) -> Result<PTable<K, V, B>, PeridotAppRuntimeError>
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
        let engine = self.engine.clone();

        Ok(AppEngine::<ExactlyOnce>::table(engine, topic.to_string()).await?)
    }

    pub async fn arc_table<K, V, B>(
        self: Arc<Self>,
        topic: &str,
    ) -> Result<PTable<K, V, B>, PeridotAppRuntimeError>
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
        let engine = self.engine.clone();

        Ok(AppEngine::<ExactlyOnce>::table(engine, topic.to_string()).await?)
    }

    pub async fn stream(&self, topic: &str) -> Result<PStream<ExactlyOnce>, PeridotAppRuntimeError> {
        info!("Creating stream for topic: {}", topic);
        Ok(AppEngine::<ExactlyOnce>::stream(self.engine.clone(), topic.to_string()).await?)
    }

    pub async fn stream_builder(&self, topic: &str) -> StreamBuilder<ExactlyOnce> {
        StreamBuilder::<ExactlyOnce>::new(topic, self.engine.clone())
    }

    pub async fn sink<K, V>(&self, topic: &str) -> Result<PSink<K, V, ExactlyOnce>, PeridotAppRuntimeError> {
        info!("Creating sink for topic: {}", topic);
        Ok(AppEngine::<ExactlyOnce>::sink(self.engine.clone(), topic.to_string()).await?)
    }
}
