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
    engine::{AppEngine, util::{ExactlyOnce, DeliveryGuaranteeType, AtMostOnce, AtLeastOnce}},
    state::backend::{ReadableStateBackend, StateBackend, WriteableStateBackend}, pipeline::{serde_ext::{PDeserialize, PSerialize}, pipeline::stream::{stream::Pipeline, PipelineStream, PipelineStreamSinkExt, PipelineStreamExt, map::MapPipeline}, message::{stream::{connector::QueueConnector, MessageStream}, sink::MessageSink, types::{FromMessage, PatchMessage}}},
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
    app_builder: StreamBuilder<G>,
}

pub struct StreamBuilder<G>
where G: DeliveryGuaranteeType
{
    engine: Arc<AppEngine<G>>,
}

impl<G> StreamBuilder<G> 
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
            + ReadableStateBackend<KeyType = KS::Output, ValueType = VS::Output>
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
            app_builder: StreamBuilder::from_engine(engine_ref),
        })
    }

    pub fn task<'a, KS, VS>(&'a mut self, topic: &'a str) -> TransparentTask<'a, Pipeline<KS, VS, G>, G>
    where 
        KS: PDeserialize + Send + 'static,
        VS: PDeserialize + Send + 'static
    {
        let input: Pipeline<KS, VS, G> = self.app_builder.stream(topic).expect("Failed to create topic");

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
            engine: engine,
            app_builder: StreamBuilder::from_engine(engine_ref),
        })
    }

    pub fn job(&mut self, job: Job) {
        self.jobs.push(Box::new(job));
    }

    pub fn stream_builder(&self) -> &StreamBuilder<G> {
        &self.app_builder
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
pub struct TransparentTask<'a, R, G>
where 
    R: PipelineStream,
    G: DeliveryGuaranteeType
{
    app: &'a mut PeridotApp<G>,
    output: R,
}

impl <'a, R, G> TransparentTask<'a, R, G>
where
    R: PipelineStream + 'static,
    G: DeliveryGuaranteeType + 'static
{
    fn new(app: &'a mut PeridotApp<G>, handler: R) -> Self {
        Self {
            app,
            output: handler,
        }
    }
}

impl <'a, R, G> Task<'a, G> for TransparentTask<'a, R, G>
where 
    R: PipelineStream + Send + 'static,
    R::MStream: Send,
    G: DeliveryGuaranteeType + Send + 'static
{
    type R = R;

    fn and_then<F1, R1>(self, next: F1) -> MutTask<'a, F1, Self::R, R1, G>
    where 
        F1: Fn(Self::R) -> R1,
        R1: PipelineStream + Send + 'static,
        R1::MStream: Send + 'static,
    {
        MutTask::<'a>::new(self.app, next, self.output)
    }

    fn map<MF, ME, MR>(self, next: MF) -> MutTask<'a, 
        impl Fn(Self::R) -> MapPipeline<Self::R, MF, ME, MR>, 
        Self::R, MapPipeline<Self::R, MF, ME, MR>, 
        G
    >
    where 
        MF: Fn(ME) -> MR + Send + Sync + Clone + 'static,
        ME: FromMessage<<Self::R as PipelineStream>::KeyType, <Self::R as PipelineStream>::ValueType> + Send + 'static,
        MR: PatchMessage<<Self::R as PipelineStream>::KeyType, <Self::R as PipelineStream>::ValueType> + Send + 'static,
    {
        MutTask::<'a>::new(
            self.app, 
            move |input|input.map(next.clone()), 
            self.output
        )
    }

    fn into_pipeline(self) -> Self::R {
        self.output
    }

    fn into_topic<Si>(self, topic: &str) 
    where
        Si: MessageSink + Send + 'static,
        Si::KeySerType: PSerialize<Input = <Self::R as PipelineStream>::KeyType> + Send + 'static,
        Si::ValueSerType: PSerialize<Input = <Self::R as PipelineStream>::ValueType> + Send + 'static,
    {
        let job = self.output.sink::<Si>(topic);

        self.app.job(Box::pin(job));
    }
}

pub trait IntoTask 
{
    type R: PipelineStream;

    fn into_task<'a, G>(self, app: &'a mut PeridotApp<G>) -> impl Task<'a, G, R = Self::R>
        where G: DeliveryGuaranteeType + Send + 'static;
}

impl<P> IntoTask for P 
where
    P: PipelineStream + Send + 'static,
{
    type R = P;

    fn into_task<'a, G>(self, app: &'a mut PeridotApp<G>) -> impl Task<'a, G, R = Self::R>
    where G: DeliveryGuaranteeType + Send + 'static
    {
        TransparentTask::new(app, self)
    }
}

pub trait Task<'a, G> 
where 
    G: DeliveryGuaranteeType
{
    type R: PipelineStream;

    fn and_then<F1, R1>(self, next: F1) -> MutTask<'a, F1, Self::R, R1, G>
    where 
        F1: Fn(Self::R) -> R1,
        R1: PipelineStream + Send + 'static,
        R1::MStream: Send + 'static;

    fn map<MF, ME, MR>(self, next: MF) -> MutTask<'a, 
        impl Fn(Self::R) -> MapPipeline<Self::R, MF, ME, MR>, 
        Self::R, MapPipeline<Self::R, MF, ME, MR>, 
        G
    >
    where 
        MF: Fn(ME) -> MR + Send + Sync + Clone + 'static,
        ME: FromMessage<<Self::R as PipelineStream>::KeyType, <Self::R as PipelineStream>::ValueType> + Send + 'static,
        MR: PatchMessage<<Self::R as PipelineStream>::KeyType, <Self::R as PipelineStream>::ValueType> + Send + 'static;

    fn into_table<S>(self, table_name: &str) -> () 
    where 
        S: ReadableStateBackend<
            KeyType = <Self::R as PipelineStream>::KeyType, 
            ValueType = <Self::R as PipelineStream>::ValueType
        >,
        Self: Sized
    {
        unimplemented!()
    }

    fn into_pipeline(self) -> Self::R;

    fn into_topic<Si>(self, topic: &str) 
    where
        Si: MessageSink + Send + 'static,
        Si::KeySerType: PSerialize<Input = <Self::R as PipelineStream>::KeyType> + Send + 'static,
        Si::ValueSerType: PSerialize<Input = <Self::R as PipelineStream>::ValueType> + Send + 'static;
}

#[must_use = "pipelines do nothing unless patched to a topic"]
pub struct MutTask<'a, F, I, R, G>
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

impl <'a, F, I, R, G> MutTask<'a, F, I, R, G>
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
}

impl <'a, F, I, R, G> Task<'a, G> for MutTask<'a, F, I, R, G>
where 
    F: Fn(I) -> R,
    I: PipelineStream,
    R: PipelineStream + Send + 'static,
    G: DeliveryGuaranteeType + Send + 'static
{
    type R = R;

    fn and_then<F1, R1>(self, next: F1) -> MutTask<'a, F1, R, R1, G>
    where 
        F1: Fn(R) -> R1,
        R1: PipelineStream + Send + 'static,
        R1::MStream: Send + 'static,
    {
        MutTask::<'a>::new(self.app, next, (self.handler)(self.input))
    }

    fn map<MF, ME, MR>(self, next: MF) -> MutTask<'a, 
        impl Fn(Self::R) -> MapPipeline<Self::R, MF, ME, MR>, 
        Self::R, MapPipeline<Self::R, MF, ME, MR>, 
        G
    >
    where 
        MF: Fn(ME) -> MR + Send + Sync + Clone + 'static,
        ME: FromMessage<<Self::R as PipelineStream>::KeyType, <Self::R as PipelineStream>::ValueType> + Send + 'static,
        MR: PatchMessage<<Self::R as PipelineStream>::KeyType, <Self::R as PipelineStream>::ValueType> + Send + 'static,
    {
        MutTask::<'a>::new(
            self.app, 
            move |input|input.map(next.clone()), 
            (self.handler)(self.input)
        )
    }

    fn into_pipeline(self) -> R {
        (self.handler)(self.input)
    }

    fn into_topic<Si>(self, topic: &str) 
    where
        Si: MessageSink + Send + 'static,
        Si::KeySerType: PSerialize<Input = <Self::R as PipelineStream>::KeyType> + Send + 'static,
        Si::ValueSerType: PSerialize<Input = <Self::R as PipelineStream>::ValueType> + Send + 'static,
    {
        let job = (self.handler)(self.input).sink::<Si>(topic);

        self.app.job(Box::pin(job));
    }
}
