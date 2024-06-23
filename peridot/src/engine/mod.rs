pub type PeridotConsumer = BaseConsumer<PeridotConsumerContext>;
pub type StateStoreMap<B> = Arc<DashMap<(String, i32), Arc<B>>>;
pub(crate) type Job = Pin<Box<dyn Future<Output = Result<(), PeridotAppRuntimeError>> + Send>>;
type JobList = Box<Job>;

use crossbeam::atomic::AtomicCell;
use dashmap::{DashMap, DashSet};
use futures::future::select_all;
use futures::Future;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{BaseConsumer, ConsumerContext};
use rdkafka::producer::{FutureProducer, NoCustomPartitioner, Producer};
use rdkafka::util::Timeout;
use rdkafka::{
    consumer::Consumer, producer::PARTITION_UA, topic_partition_list::TopicPartitionListElem,
    ClientConfig, TopicPartitionList,
};
use std::default;
use std::ops::Deref;
use std::pin::Pin;
use std::process::exit;
use std::sync::Mutex;
use std::{fmt::Display, marker::PhantomData, sync::Arc, time::Duration};
use tokio::signal::unix::{signal, SignalKind};
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use tracing::{error, info, warn};
use tracing_subscriber::field::display;

use crate::app::error::PeridotAppRuntimeError;
use crate::engine::queue_manager::QueueManager;
use crate::state::backend::StateBackend;
use crate::{engine::wrapper::serde::PeridotDeserializer, pipeline::stream::head::HeadPipeline};

use self::admin_manager::AdminManager;
use self::changelog_manager::ChangelogManager;
use self::consumer_manager::ConsumerManager;
use self::context::EngineContext;
use self::engine_state::EngineState;
use self::error::{EngineInitialisationError, TableRegistrationError};
use self::metadata_manager::table_metadata::TableMetadata;
use self::metadata_manager::topic_metadata::{self, TopicMetadata};
use self::metadata_manager::MetadataManager;
use self::producer_factory::ProducerFactory;
use self::queue_manager::partition_queue::StreamPeridotPartitionQueue;
use self::queue_manager::QueueSender;
use self::state_store_manager::StateStoreManager;
use self::util::DeliveryGuaranteeType;
use self::{
    error::{PeridotEngineCreationError, PeridotEngineRuntimeError},
    util::{ConsumerUtils, ExactlyOnce},
};

use crate::app::{
    config::PeridotConfig,
    extensions::{Commit, OwnedRebalance, PeridotConsumerContext},
};

pub mod admin_manager;
pub mod changelog_manager;
pub mod circuit_breaker;
pub mod consumer_manager;
pub mod context;
pub mod engine_state;
pub mod error;
pub mod hook_manager;
pub mod metadata_manager;
pub mod producer_factory;
pub mod queue_manager;
pub mod state_store_manager;
pub mod util;
pub mod wrapper;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum DeliverySemantics {
    #[default]
    ExactlyOnce,
    AtLeastOnce,
    AtMostOnce,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StateType {
    #[default]
    Persistent,
    InMemory,
}

pub struct AppEngine<B, G = ExactlyOnce> {
    engine_context: Arc<EngineContext>,
    state_store_manager: Arc<StateStoreManager<B>>,
    producer_factory: Arc<ProducerFactory>,
    downstreams: Arc<DashMap<String, QueueSender>>,
    // hook_manager: HookManager,
    shutdown_signal: tokio::sync::broadcast::Sender<()>,
    engine_state: Arc<AtomicCell<EngineState>>,
    jobs: Mutex<Vec<JobList>>,
    _delivery_guarantee: PhantomData<G>,
}

impl<B, G> AppEngine<B, G>
where
    G: DeliveryGuaranteeType + Send + Sync + 'static,
    B: StateBackend + Send + Sync + 'static,
{
    pub(crate) fn state_store_context(&self) -> Arc<StateStoreManager<B>> {
        self.state_store_manager.clone()
    }

    pub fn engine_context(&self) -> Arc<EngineContext> {
        self.engine_context.clone()
    }

    pub async fn run(&self) -> Result<(), PeridotEngineRuntimeError> {
        let waker_context = self.engine_context.consumer_manager.consumer_context();

        let pre_rebalance_waker = waker_context.pre_rebalance_waker();
        let commit_waker = waker_context.commit_waker();
        let rebalance_waker = waker_context.pre_rebalance_waker();
        //let rebalance_waker = waker_context.post_rebalance_waker();

        let queue_distributor = QueueManager::<B>::new(
            self.engine_context(),
            self.state_store_manager.clone(),
            self.producer_factory.clone(),
            self.downstreams.clone(),
            self.engine_state.clone(),
            rebalance_waker,
        );

        tokio::spawn(queue_distributor);

        let shutdown_singnal_ref = self.shutdown_signal.clone();

        tokio::spawn(async move {
            let rx = shutdown_singnal_ref.subscribe();

            let mut sigterm = signal(SignalKind::terminate()).unwrap();
            let mut sigint = signal(SignalKind::interrupt()).unwrap();

            select! {
                _ = sigterm.recv() => info!("Engine recieved SIGTERM, notifying tasks."),
                _ = sigint.recv() => info!("Engine recieved SIGINT, notifying tasks."),
            };

            match shutdown_singnal_ref.send(()) {
                Ok(_) => info!("Shutdown signal broadcast to tasks."),
                Err(e) => warn!("Shutdown signal already sent."),
            };

            // TODO: Remove this when shutdown hooks have been integrated.
            exit(0)
        });

        let jobs = self.jobs.lock();

        let job_results = jobs
            .expect("Job lock poinsoned!")
            .drain(..)
            .map(tokio::spawn)
            .collect::<Vec<_>>();

        let mut select = select_all(job_results.into_iter());

        tracing::debug!("Engine running...");

        loop {
            let (job_result, _, remaining) = select.await;

            match job_result {
                Ok(_) => (),
                Err(e) => {
                    unimplemented!("Transition engine state on job fail: {}", e)
                }
            }

            select = select_all(remaining);
        }

        Ok(()) // Transition engine state.
    }

    pub fn from_config(config: &PeridotConfig) -> Result<Self, PeridotEngineCreationError> {
        let engine_context = EngineContext {
            config: config.clone(),
            admin_manager: AdminManager::new(config)?,
            consumer_manager: ConsumerManager::from_config(config)?,
            metadata_manager: MetadataManager::new(config.app_id()),
            changelog_manager: ChangelogManager::from_config(config)?,
        };

        let shutdown_signal = tokio::sync::broadcast::Sender::new(1);

        Ok(Self {
            engine_context: Arc::new(engine_context),
            producer_factory: Arc::new(ProducerFactory::new(
                config.clone(),
                DeliverySemantics::ExactlyOnce,
            )),
            downstreams: Default::default(),
            engine_state: Default::default(),
            shutdown_signal,
            state_store_manager: Default::default(),
            jobs: Default::default(),
            _delivery_guarantee: PhantomData,
        })
    }

    pub(crate) fn submit(&self, job: Job) {
        let mut jobs = self.jobs.lock().expect("Job lock poinsoned.");

        jobs.push(Box::new(job));
    }

    pub(crate) fn input_stream<KS, VS>(
        &self,
        topic: String,
    ) -> Result<HeadPipeline<KS, VS>, PeridotEngineRuntimeError>
    where
        KS: PeridotDeserializer,
        VS: PeridotDeserializer,
    {
        // TODO: there is some time that passes between subscribing to a topic and
        // registering with the queue manager. We suspect that this may lead to a situation
        // where if this function is called while the engine is running, then the QueueManager
        // may recieve a partition queue before it's internal record has updated, leading to
        // undefined behaviour.
        let metadata = self
            .engine_context
            .consumer_manager
            .create_topic_source(&topic)?;
        self.engine_context
            .metadata_manager
            .register_source_topic(&topic, &metadata)?;

        let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel();
        self.downstreams.insert(topic.clone(), queue_sender);

        Ok(HeadPipeline::new(queue_receiver))
    }
}
