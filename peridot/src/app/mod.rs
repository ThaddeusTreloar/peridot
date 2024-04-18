use std::{pin::Pin, sync::Arc};

use futures::{future::join_all, Future};
use rdkafka::{consumer::BaseConsumer, ClientConfig};
use tracing::info;

use crate::{
    app::extensions::PeridotConsumerContext, engine::{
        util::{DeliveryGuaranteeType, ExactlyOnce},
        wrapper::serde::PDeserialize,
        AppEngine,
    }, pipeline::stream::serialiser::SerialiserPipeline, state::backend::{in_memory::InMemoryStateBackend, StateBackend}, task::transparent::TransparentTask
};

use self::{
    config::PeridotConfig,
    error::{PeridotAppCreationError, PeridotAppRuntimeError},
};

pub mod builder;
pub mod config;
pub mod error;
pub mod extensions;

pub type PeridotConsumer = BaseConsumer<PeridotConsumerContext>;

type Job = Pin<Box<dyn Future<Output = Result<(), PeridotAppRuntimeError>> + Send>>;
type JobList = Box<Job>;

#[derive()]
pub struct PeridotApp<B = InMemoryStateBackend, G = ExactlyOnce>
where
    G: DeliveryGuaranteeType,
{
    _config: PeridotConfig,
    jobs: Vec<JobList>,
    engine: Arc<AppEngine<B>>,
    app_builder: StreamBuilder<B, ExactlyOnce>,
    _phantom: std::marker::PhantomData<G>,
}

pub struct StreamBuilder<B, G>
where
    G: DeliveryGuaranteeType,
{
    engine: Arc<AppEngine<B, ExactlyOnce>>,
    _phantom: std::marker::PhantomData<G>,
}

impl<B, G> StreamBuilder<B, G>
where
    G: DeliveryGuaranteeType,
    B: Send + Sync + 'static,
{
    pub fn from_engine<NB>(engine: Arc<AppEngine<NB, ExactlyOnce>>) -> StreamBuilder<NB, G> {
        StreamBuilder::<NB, G> {
            engine,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn stream<KS, VS>(
        &self,
        topic: &str,
    ) -> Result<SerialiserPipeline<KS, VS, ExactlyOnce>, PeridotAppRuntimeError>
    where
        KS: PDeserialize,
        VS: PDeserialize,
    {
        info!("Creating stream for topic: {}", topic);
        Ok(self.engine.clone().stream(topic.to_string())?)
    }
}

impl<B, G> PeridotApp<B, G> 
where
    G: DeliveryGuaranteeType,
    B: StateBackend + Send + Sync + 'static,
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
            app_builder: StreamBuilder::<B, _>::from_engine(engine_ref),
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn task<'a, KS, VS>(
        &'a mut self,
        topic: &'a str,
    ) -> TransparentTask<'a, SerialiserPipeline<KS, VS, ExactlyOnce>, B, G>
    where
        KS: PDeserialize + Send + 'static,
        VS: PDeserialize + Send + 'static,
    {
        let input: SerialiserPipeline<KS, VS, ExactlyOnce> = self
            .app_builder
            .stream(topic)
            .expect("Failed to create topic");

        TransparentTask::new(self, input)
    }

    pub fn from_config(mut config: PeridotConfig) -> Result<Self, PeridotAppCreationError> {
        config.clean_config();

        let engine = AppEngine::from_config(&config)?;

        let engine = Arc::new(engine);
        let engine_ref = engine.clone();

        Ok(Self {
            _config: config.clone(),
            jobs: Default::default(),
            engine,
            app_builder: StreamBuilder::<B, _>::from_engine(engine_ref),
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn job(&mut self, job: Job) {
        self.jobs.push(Box::new(job));
    }

    pub fn stream_builder(&self) -> &StreamBuilder<B, ExactlyOnce> {
        &self.app_builder
    }

    pub fn engine_ref(&self) -> Arc<AppEngine<B, ExactlyOnce>> {
        self.engine.clone()
    }

    pub async fn run(self) -> Result<(), PeridotAppRuntimeError> {
        info!("Running PeridotApp");

        let job_results = self
            .jobs
            .into_iter()
            .map(tokio::spawn)
            .collect::<Vec<_>>();

        self.engine.run().await?;

        for job_result in join_all(job_results).await {
            job_result??
        }

        Ok(())
    }
}
