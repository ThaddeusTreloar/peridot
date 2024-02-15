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
    engine::{AppEngine, util::{ExactlyOnce, DeliveryGuaranteeType, AtMostOnce, AtLeastOnce}},
    state::backend::{ReadableStateBackend, StateBackend, WriteableStateBackend}, pipeline::{serde_ext::PDeserialize, pipeline::stream::stream::Pipeline},
};

use self::{
    config::PeridotConfig,
    error::{PeridotAppCreationError, PeridotAppRuntimeError},
    psink::{PSink, PSinkBuilder},
    pstream::PStream,
    ptable::PTable,
};

pub mod config;
pub mod error;
pub mod extensions;
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

    pub fn engine_ref(&self) -> Arc<AppEngine<G>> {
        self.engine.clone()
    }

    pub async fn run(&self) -> Result<(), PeridotAppRuntimeError> {
        info!("Running PeridotApp");
        self.engine.run().await?;
        Ok(())
    }

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
        Ok(AppEngine::<G>::table(self.engine.clone(), topic.to_string()).await?)
    }

    pub fn stream<KS, VS>(&self, topic: &str) -> Result<Pipeline<KS, VS, G>, PeridotAppRuntimeError> 
    where
        KS: PDeserialize,
        VS: PDeserialize
    {
        info!("Creating stream for topic: {}", topic);
        Ok(self.engine.clone().stream(topic.to_string())?)
    }

    pub async fn sink<K, V>(&self, topic: &str) -> Result<PSinkBuilder<G>, PeridotAppRuntimeError> {
        info!("Creating sink for topic: {}", topic);
        Ok(AppEngine::<G>::sink(self.engine.clone(), topic.to_string()).await?)
    }
}