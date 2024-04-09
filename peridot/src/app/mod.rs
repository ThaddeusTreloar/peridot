use std::{pin::Pin, sync::Arc};

use futures::{future::join_all, Future};
use rdkafka::{consumer::BaseConsumer, ClientConfig};
use tracing::info;

use crate::{
    app::extensions::PeridotConsumerContext,
    engine::{
        util::{DeliveryGuaranteeType, ExactlyOnce},
        AppEngine,
    },
    pipeline::stream::serialiser::SerialiserPipeline,
    serde_ext::PDeserialize,
    state::backend::{ReadableStateBackend, StateBackend, WriteableStateBackend},
    task::transparent::TransparentTask,
};

use self::{
    config::PeridotConfig,
    error::{PeridotAppCreationError, PeridotAppRuntimeError},
    psink::PSinkBuilder,
    ptable::PTable,
};

pub mod config;
pub mod error;
pub mod extensions;
pub mod psink;
pub mod ptable;

pub type PeridotConsumer = BaseConsumer<PeridotConsumerContext>;

type Job = Pin<Box<dyn Future<Output = Result<(), PeridotAppRuntimeError>> + Send>>;
type JobList = Box<Job>;

#[derive()]
pub struct PeridotApp<G = ExactlyOnce>
where
    G: DeliveryGuaranteeType,
{
    _config: PeridotConfig,
    jobs: Vec<JobList>,
    engine: Arc<AppEngine<()>>,
    app_builder: StreamBuilder<ExactlyOnce>,
    _phantom: std::marker::PhantomData<G>,
}

pub struct StreamBuilder<G>
where
    G: DeliveryGuaranteeType,
{
    engine: Arc<AppEngine<(), ExactlyOnce>>,
    _phantom: std::marker::PhantomData<G>,
}

impl<G> StreamBuilder<G>
where
    G: DeliveryGuaranteeType,
{
    pub fn from_engine(engine: Arc<AppEngine<(), ExactlyOnce>>) -> Self {
        Self {
            engine,
            _phantom: std::marker::PhantomData,
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
        unimplemented!()
        //Ok(AppEngine::<B, ExactlyOnce>::table::<KS, VS, B>(self.engine.clone(), topic.to_string()).await?)
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

    pub async fn sink<K, V>(
        &self,
        topic: &str,
    ) -> Result<PSinkBuilder<ExactlyOnce>, PeridotAppRuntimeError> {
        info!("Creating sink for topic: {}", topic);
        Ok(AppEngine::<(), ExactlyOnce>::sink(self.engine.clone(), topic.to_string()).await?)
    }
}

impl PeridotApp<ExactlyOnce> {
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
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn task<'a, KS, VS>(
        &'a mut self,
        topic: &'a str,
    ) -> TransparentTask<'a, SerialiserPipeline<KS, VS, ExactlyOnce>>
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
            app_builder: StreamBuilder::from_engine(engine_ref),
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn job(&mut self, job: Job) {
        self.jobs.push(Box::new(job));
    }

    pub fn stream_builder(&self) -> &StreamBuilder<ExactlyOnce> {
        &self.app_builder
    }

    pub fn engine_ref(&self) -> Arc<AppEngine<(), ExactlyOnce>> {
        self.engine.clone()
    }

    pub async fn run(self) -> Result<(), PeridotAppRuntimeError> {
        info!("Running PeridotApp");

        let job_results = self
            .jobs
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
