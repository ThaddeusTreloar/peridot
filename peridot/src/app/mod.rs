use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll}, marker::PhantomData,
};

use futures::{Future, future::{join_all, BoxFuture}};
use rdkafka::{
    consumer::{stream_consumer::StreamPartitionQueue, StreamConsumer, BaseConsumer},
    ClientConfig,
};
use tracing::info;

use crate::{
    app::extensions::PeridotConsumerContext,
    engine::{AppEngine, util::{ExactlyOnce, DeliveryGuaranteeType, AtMostOnce, AtLeastOnce}, tasks::{Builder, FromBuilder, Stream}},
    state::backend::{ReadableStateBackend, StateBackend, WriteableStateBackend}, pipeline::{serde_ext::{PDeserialize, PSerialize}, pipeline::stream::{stream::Pipeline, PipelineStream, PipelineStreamSinkExt}, message::{stream::{connector::QueueConnector, MessageStream}, sink::MessageSink}},
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
type JobList = Box<Job>;

#[derive()]
pub struct PeridotApp<G = ExactlyOnce> 
where G: DeliveryGuaranteeType
{
    _config: PeridotConfig,
    jobs: Vec<JobList>,
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

    pub fn task<'a, KS, VS, F, R>(&'a mut self, topic: &'a str, handler: F) -> Task<'a, F, Pipeline<KS, VS, G>, R, G>
    where 
        F: Fn(Pipeline<KS, VS, G>) -> R,
        R: PipelineStream + Send + 'static,
        R::MStream: Send + 'static,
        KS: PDeserialize + Send + 'static,
        VS: PDeserialize + Send + 'static
    {
        let input: Pipeline<KS, VS, G> = self.app_builder.stream(topic).expect("Failed to create topic");

        Task::new(self, handler, input)
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

    pub fn job(&mut self, job: Job) {
        self.jobs.push(Box::new(job));
    }

    pub fn job_from_pipeline<KS, VS, Si, R>(&mut self, topic: &str, pipeline: R) 
    where
        R: PipelineStream + Send + 'static,
        KS: PSerialize<R::KeyType> + Send + 'static,
        VS: PSerialize<R::ValueType> + Send + 'static,
        Si: MessageSink<R::KeyType, R::ValueType> + Send + 'static,
    {
        let job = pipeline.sink::<KS, VS, Si>(topic);

        self.job(Box::pin(job));
    }

    pub fn engine_ref(&self) -> Arc<AppEngine<G>> {
        self.engine.clone()
    }

    pub async fn run(self) -> Result<(), PeridotAppRuntimeError> {
        info!("Running PeridotApp");

        let job_results = self.jobs
            .into_iter()
            .map(|job| tokio::spawn(job))
            .collect::<Vec<_>>();
        
        self.engine.run().await?;

        for job_result in join_all(job_results).await {
            job_result??
        }

        Ok(())
    }
}

#[must_use = "pipelines do nothing unless patched to a topic"]
pub struct Task<'a, F, I, R, G>
where 
    F: Fn(I) -> R,
    I: PipelineStream,
    R: PipelineStream,
    G: DeliveryGuaranteeType
{
    app: &'a mut PeridotApp<G>,
    handler: F,
    input: I,
}

impl <'a, F, I, R, G> Task<'a, F, I, R, G>
where 
    F: Fn(I) -> R,
    I: PipelineStream,
    R: PipelineStream + Send + 'static,
    R::MStream: Send + 'static,
    G: DeliveryGuaranteeType + 'static
{
    pub fn new(app: &'a mut PeridotApp<G>, handler: F, input: I) -> Self {
        Self {
            app,
            handler,
            input,
        }
    }

    pub fn and_then<F1, R1>(self, next: F1) -> Task<'a, F1, R, R1, G>
    where 
        F1: Fn(R) -> R1,
        R1: PipelineStream + Send + 'static,
        R1::MStream: Send + 'static,
    {
        Task::<'a>::new(self.app, next, (self.handler)(self.input))
    }

    pub fn into_pipeline(self) -> R {
        (self.handler)(self.input)
    }

    pub fn into_topic<KS, VS, Si>(self, topic: &str) 
    where
        KS: PSerialize<R::KeyType> + Send + 'static,
        VS: PSerialize<R::ValueType> + Send + 'static,
        Si: MessageSink<R::KeyType,R::ValueType> + Send + 'static,
    {
        let job = (self.handler)(self.input).sink::<KS, VS, Si>(topic);

        self.app.job(Box::pin(job));
    }
}


