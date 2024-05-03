use std::{pin::Pin, sync::{Arc, Mutex}};

use futures::{future::{join_all, select_all}, Future};
use rdkafka::{consumer::BaseConsumer, ClientConfig};
use serde::Serialize;
use tracing::info;

use crate::{
    app::extensions::PeridotConsumerContext, engine::{
        util::{DeliveryGuaranteeType, ExactlyOnce},
        wrapper::serde::PeridotDeserializer,
        AppEngine,
    }, pipeline::stream::serialiser::SerialiserPipeline, state::backend::{in_memory::InMemoryStateBackend, StateBackend}, task::{table::TableTask, transparent::TransparentTask, Task}
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
type DirectTableTask<'a, KS, VS, B, G> = TableTask<'a, SerialiserPipeline<KS, VS>, B, G>;

#[derive()]
pub struct PeridotApp<B = InMemoryStateBackend, G = ExactlyOnce>
where
    G: DeliveryGuaranteeType,
{
    _config: PeridotConfig,
    jobs: Mutex<Vec<JobList>>,
    engine: Arc<AppEngine<B>>,
    _phantom: std::marker::PhantomData<G>,
}

impl<B, G> PeridotApp<B, G> 
where
    G: DeliveryGuaranteeType,
    B: StateBackend + Send + Sync + 'static,
{
    pub fn stream<KS, VS>(
        &self,
        topic: &str,
    ) -> Result<SerialiserPipeline<KS, VS, ExactlyOnce>, PeridotAppRuntimeError>
    where
        KS: PeridotDeserializer,
        VS: PeridotDeserializer,
    {
        info!("Creating stream for topic: {}", topic);
        Ok(self.engine.clone().new_input_stream(topic.to_string())?)
    }

    pub fn table<'a, KS, VS>(
        &'a self,
        topic: &'a str,
        table_name: &'a str,
    ) -> DirectTableTask<KS, VS, B, G>
    where
        KS: PeridotDeserializer + Send + 'static,
        VS: PeridotDeserializer + Send + 'static,
        KS::Output: Clone + Serialize + Send,
        VS::Output: Clone + Serialize + Send,
    {
        let input: SerialiserPipeline<KS, VS, ExactlyOnce> = self
            .stream(topic)
            .expect("Failed to create topic");

        TransparentTask::new(self, topic,input).into_table(table_name)
    }

    pub fn task<'a, KS, VS>(
        &'a self,
        topic: &'a str,
    ) -> TransparentTask<'a, SerialiserPipeline<KS, VS, ExactlyOnce>, B, G>
    where
        KS: PeridotDeserializer + Send + 'static,
        VS: PeridotDeserializer + Send + 'static,
    {
        let input: SerialiserPipeline<KS, VS, ExactlyOnce> = self
            .stream(topic)
            .expect("Failed to create topic");

        TransparentTask::new(self, topic, input)
    }

    pub fn from_config(mut config: PeridotConfig) -> Result<Self, PeridotAppCreationError> {
        let engine = AppEngine::from_config(&config)?;

        let engine = Arc::new(engine);
        let engine_ref = engine.clone();

        Ok(Self {
            _config: config.clone(),
            jobs: Default::default(),
            engine,
            _phantom: std::marker::PhantomData,
        })
    }

    pub(crate) fn job(&self, job: Job) {
        let mut jobs = self.jobs.lock()
            .expect("Job lock poinsoned.");

        jobs.push(Box::new(job));
    }

    pub(crate) fn engine_ref(&self) -> Arc<AppEngine<B, ExactlyOnce>> {
        self.engine.clone()
    }

    pub async fn run(self) -> Result<(), PeridotAppRuntimeError> {
        info!("Running PeridotApp");

        let jobs = self.jobs.lock();

        let job_results = jobs
            .expect("Job lock poinsoned!")
            .drain(..)
            .map(tokio::spawn)
            .collect::<Vec<_>>();

        self.engine.run().await?;

        let mut select = select_all(job_results.into_iter());
        
        loop {
            let (job_result, _, remaining) = select.await;

            match job_result {
                Ok(_) => (),
                Err(e) => {
                    unimplemented!("Transition engine state on job fail: {}", e)
                },
            }

            select = select_all(remaining);
        }

        Ok(())
    }
}
