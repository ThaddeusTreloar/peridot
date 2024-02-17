use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{Future, future::{join_all, BoxFuture}};
use rdkafka::{
    consumer::{stream_consumer::StreamPartitionQueue, StreamConsumer, BaseConsumer},
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

pub type PeridotConsumer = BaseConsumer<PeridotConsumerContext>;

type Job = Pin<Box<dyn Future<Output = Result<(), PeridotAppRuntimeError>> + Send>>;
type JobFactory<G> = Box<dyn FnOnce(&AppBuilder<G>) -> Job>;

#[derive()]
pub struct PeridotApp<G = ExactlyOnce> 
where G: DeliveryGuaranteeType
{
    _config: PeridotConfig,
    jobs: Vec<JobFactory<G>>,
    engine: Arc<AppEngine<G>>,
    app_builder: AppBuilder<G>,
}

pub struct AppBuilder<G>
where G: DeliveryGuaranteeType
{
    engine: Arc<AppEngine<G>>,
}

impl<G> AppBuilder<G> 
where G: DeliveryGuaranteeType
{
    pub fn from_engine(engine: Arc<AppEngine<G>>) -> Self {
        Self {
            engine,
        }
    }

    pub async fn table<KS, VS, B>(
        &self,
        topic: &str,
    ) -> Result<PTable<KS, VS, B>, PeridotAppRuntimeError>
    where
        B: StateBackend
            + ReadableStateBackend<KS::Output, VS::Output>
            + WriteableStateBackend<KS::Output, VS::Output>
            + Send
            + Sync
            + 'static,
        KS: PDeserialize + Send + Sync + 'static,
        VS: PDeserialize + Send + Sync + 'static,
        KS::Output: Send + Sync + 'static,
        VS::Output: Send + Sync + 'static,
    {
        Ok(AppEngine::<G>::table::<KS, VS, B>(self.engine.clone(), topic.to_string()).await?)
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

impl <G> PeridotApp<G> 
where G: DeliveryGuaranteeType + 'static
{
    pub fn from_client_config(config: &ClientConfig) -> Result<Self, PeridotAppCreationError> {
        let config = PeridotConfig::from(config);

        let engine = AppEngine::from_config(&config)?;

        let engine = Arc::new(engine);
        let engine_ref = engine.clone();

        Ok(Self {
            _config: config,
            jobs: Default::default(),
            engine,
            app_builder: AppBuilder::from_engine(engine_ref),
        })
    }

    pub fn from_config(mut config: PeridotConfig) -> Result<Self, PeridotAppCreationError> {
        config.clean_config();

        let engine = AppEngine::from_config(&config)?;

        let engine = Arc::new(engine);
        let engine_ref = engine.clone();

        Ok(Self {
            _config: config.clone(),
            jobs: Default::default(),
            engine: engine,
            app_builder: AppBuilder::from_engine(engine_ref),
        })
    }

    pub fn job(&mut self, job: impl FnOnce(&AppBuilder<G>) -> Job + 'static) {
        self.jobs.push(Box::new(job));
    }

    pub fn engine_ref(&self) -> Arc<AppEngine<G>> {
        self.engine.clone()
    }

    pub async fn run(self) -> Result<(), PeridotAppRuntimeError> {
        info!("Running PeridotApp");

        let job_results = self.jobs
            .into_iter()
            .map(|job_factory| job_factory(&self.app_builder))
            .map(|job| tokio::spawn(job))
            .collect::<Vec<_>>();
        
        self.engine.run().await?;

        for job_result in join_all(job_results).await {
            job_result??
        }

        Ok(())
    }
}