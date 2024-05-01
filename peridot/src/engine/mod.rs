pub type PeridotConsumer = BaseConsumer<PeridotConsumerContext>;

pub type StateStoreMap<B> = Arc<DashMap<(String, i32), Arc<B>>>;

use crossbeam::atomic::AtomicCell;
use dashmap::{DashMap, DashSet};
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{BaseConsumer, ConsumerContext};
use rdkafka::producer::{FutureProducer, NoCustomPartitioner, Producer};
use rdkafka::util::Timeout;
use rdkafka::{
    consumer::Consumer, producer::PARTITION_UA, topic_partition_list::TopicPartitionListElem,
    ClientConfig, TopicPartitionList,
};
use tracing_subscriber::field::display;
use std::default;
use std::ops::Deref;
use std::{fmt::Display, marker::PhantomData, sync::Arc, time::Duration};
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use tracing::{error, info, warn};

use crate::engine::queue_manager::QueueManager;
use crate::state::backend::StateBackendContext;
use crate::{
    engine::wrapper::serde::PeridotDeserializer, pipeline::stream::serialiser::SerialiserPipeline,
};

use self::changelog_manager::ChangelogManager;
use self::client_manager::ClientManager;
use self::context::EngineContext;
use self::engine_state::EngineState;
use self::error::{EngineInitialisationError, TableRegistrationError};
use self::metadata_manager::table_metadata::TableMetadata;
use self::metadata_manager::topic_metadata::{self, TopicMetadata};
use self::metadata_manager::MetadataManager;
use self::producer_factory::ProducerFactory;
use self::queue_manager::partition_queue::StreamPeridotPartitionQueue;
use self::queue_manager::QueueSender;
use self::{
    error::{PeridotEngineCreationError, PeridotEngineRuntimeError},
    util::{ConsumerUtils, ExactlyOnce},
};

use crate::app::{
    config::PeridotConfig,
    extensions::{Commit, OwnedRebalance, PeridotConsumerContext},
};

pub mod changelog_manager;
pub mod circuit_breaker;
pub mod client_manager;
pub mod context;
pub mod queue_manager;
pub mod error;
pub mod engine_state;
pub mod metadata_manager;
pub mod producer_factory;
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
    client_manager: Arc<ClientManager>,
    metadata_manager: Arc<MetadataManager>,
    changelog_manager: Arc<ChangelogManager>,
    producer_factory: Arc<ProducerFactory>,
    downstreams: Arc<DashMap<String, QueueSender>>,
    engine_state: Arc<AtomicCell<EngineState>>,
    state_stores: StateStoreMap<B>,
    _delivery_guarantee: PhantomData<G>,
}

impl<B> AppEngine<B>
where
    B: StateBackendContext + Send + Sync + 'static,
{
    pub fn get_engine_context(&self) -> EngineContext {
        EngineContext { 
            client_manager: self.client_manager.clone(), 
            metadata_manager: self.metadata_manager.clone(), 
            changelog_manager: self.changelog_manager.clone(), 
        }
    }

    pub async fn run(&self) -> Result<(), PeridotEngineRuntimeError> {
        let waker_context = self.client_manager.consumer_context();

        let pre_rebalance_waker = waker_context.pre_rebalance_waker();
        let commit_waker = waker_context.commit_waker();
        let rebalance_waker = waker_context.post_rebalance_waker();

        let queue_distributor = QueueManager::new(
            self.get_engine_context(),
            self.producer_factory.clone(),
            self.downstreams.clone(),
            self.engine_state.clone(),
            pre_rebalance_waker,
        );

        tokio::spawn(queue_distributor);

        info!("Engine running...");

        Ok(()) // Transition engine state.
    }

    pub fn from_config(config: &PeridotConfig) -> Result<Self, PeridotEngineCreationError> {
        let context = PeridotConsumerContext::from_config(config);

        Ok(Self {
            client_manager: Arc::new(ClientManager::from_config(config)?),
            metadata_manager: Arc::new(MetadataManager::new(config.app_id())),
            changelog_manager: Arc::new(ChangelogManager::from_config(config)?),
            producer_factory: Arc::new(ProducerFactory::new(config.clone(), DeliverySemantics::ExactlyOnce)),
            downstreams: Default::default(),
            engine_state: Default::default(),
            state_stores: Default::default(),
            _delivery_guarantee: PhantomData,
        })
    }

    pub(crate) fn new_input_stream<KS, VS>(
        self: Arc<AppEngine<B>>,
        topic: String,
    ) -> Result<SerialiserPipeline<KS, VS>, PeridotEngineRuntimeError>
    where
        KS: PeridotDeserializer,
        VS: PeridotDeserializer,
    {
        // TODO: there is some time that passes between subscribing to a topic and
        // registering with the queue manager. We suspect that this may lead to a situation
        // where if this function is called while the engine is running, then the QueueManager
        // may recieve a partition queue before it's internal record has updated, leading to
        // undefined behaviour.
        let metadata = self.client_manager.create_topic_source(&topic)?;
        self.metadata_manager.register_source_topic(&topic, &metadata)?;

        let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel();
        self.downstreams.insert(topic.clone(), queue_sender);

        Ok(SerialiserPipeline::new(queue_receiver))
    }
}