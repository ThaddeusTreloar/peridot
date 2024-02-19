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
    engine::{AppEngine, util::{ExactlyOnce, DeliveryGuaranteeType, AtMostOnce, AtLeastOnce}, tasks::{Builder, FromBuilder}},
    state::backend::{ReadableStateBackend, StateBackend, WriteableStateBackend}, pipeline::{serde_ext::PDeserialize, pipeline::stream::{stream::Pipeline, PipelineStream}, message::stream::connector::QueueConnector},
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

    pub fn task<KS, VS, F>(&self, topic: &str, handler: F) -> Head<F, KS, VS>
    where
        KS: PDeserialize,
        VS: PDeserialize,
    {
        Head::from_topic(topic.to_string(), handler)
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

pub struct Head<F, KS, VS>
where 
    KS: PDeserialize,
    VS: PDeserialize,
{
    topic: String,
    handler: F,
    _key_ser_type: PhantomData<KS>,
    _val_ser_type: PhantomData<VS>,
}

impl <F, KS, VS> Head<F, KS, VS>
where 
    KS: PDeserialize,
    VS: PDeserialize,
{
    pub fn from_topic(topic: String, handler: F) -> Self {
        Self {
            topic: topic,
            handler,
            _key_ser_type: Default::default(),
            _val_ser_type: Default::default(),
        }
    }
/*
    pub fn and_then<NRK, NRV, N, NS, NI>(self, next: N) -> Next<N, RK, RV, NRK, NRV, NI, Self, NS>
    where N: Fn(NI) -> NS,
        NI: FromBuilder<RK, RV, Self>,
        NS: PipelineStream<NRK, NRV>,
    {
        Next {
            upstream: self,
            handler: next,
            _handler_input_type: Default::default(),
            _builder_type: Default::default(),
            _stream_return: Default::default(),
            _key_type: Default::default(),
            _val_type: Default::default(),
            _return_key_type: Default::default(),
            _return_val_type: Default::default(),
        }
    } */
}

impl <F, KS, VS, RK, RV> Builder for Head<F, KS, VS>
where
    F: Fn(Pipeline<KS, VS, ExactlyOnce>) -> ,
    KS: PDeserialize,
    VS: PDeserialize,
{
    type Output = F::Output;

    fn generate_pipeline(&self) -> Self::Output {
        unimplemented!("")
    }
}
/*
pub struct Next<F, K, V, RK, RV, In, B, S>
where F: Fn(In) -> S,
    In: FromBuilder<K, V, B>,
    B: Builder<K, V>,
    S: PipelineStream<RK, RV>
{
    upstream: B,
    handler: F,
    _handler_input_type: PhantomData<In>,
    _builder_type: PhantomData<B>,
    _stream_return: PhantomData<S>,
    _key_type: PhantomData<K>,
    _val_type: PhantomData<V>,
    _return_key_type: PhantomData<RK>,
    _return_val_type: PhantomData<RV>,
}

impl <F, K, V, RK, RV, In, B, S> Builder<RK, RV> for Next<F, K, V, RK, RV, In, B, S>
where F: Fn(In) -> S,
    In: FromBuilder<K, V, B>,
    B: Builder<K, V>,
    S: PipelineStream<RK, RV>
{
    type Output = S;

    fn generate_pipeline(&self) -> Self::Output {
        unimplemented!("")
    }
} */