use std::{
    fmt::Display,
    marker::PhantomData,
    sync::Arc,
    time::Duration,
};

use crossbeam::atomic::AtomicCell;
use dashmap::{DashMap, DashSet};
use rdkafka::{
    consumer::{stream_consumer::StreamPartitionQueue, Consumer},
    producer::{PARTITION_UA, BaseProducer},
    ClientConfig, TopicPartitionList, config::FromClientConfig, topic_partition_list::TopicPartitionListElem,
};
use tokio::{select, sync::mpsc::Sender};
use tracing::{debug, error, info, warn};

use crate::state::{
    backend::{
        persistent::PersistentStateBackend, ReadableStateBackend, StateBackend,
        WriteableStateBackend, CommitLog,
    },
    StateStore,
};

use self::error::{PeridotEngineCreationError, PeridotEngineRuntimeError};

use super::{
    extensions::{Commit, OwnedRebalance, PeridotConsumerContext}, PeridotConsumer, PeridotPartitionQueue, ptable::PTable, pstream::PStream, psink::PSink,
};

pub mod error;
pub mod tables;
pub mod sinks;
pub mod streams;

#[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
pub enum EngineState {
    Lagging,
    NotReady,
    Rebalancing,
    Running,
    #[default]
    Stopped,
}

impl Display for EngineState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StateType {
    #[default]
    Persistent,
    InMemory,
}

pub struct AppEngine {
    config: ClientConfig,
    consumer: Arc<PeridotConsumer>,
    waker_context: Arc<PeridotConsumerContext>,
    downstreams: Arc<DashMap<String, Sender<PeridotPartitionQueue>>>,
    downstream_commit_logs: Arc<CommitLog>,
    state_streams: Arc<DashSet<String>>,
    engine_state: Arc<AtomicCell<EngineState>>,
}

impl AppEngine {
    pub fn from_config(config: &ClientConfig) -> Result<Self, PeridotEngineCreationError> {
        let context = PeridotConsumerContext::default();
        let consumer = config.create_with_context(context.clone())?;

        Ok(AppEngine {
            config: config.clone(),
            consumer: Arc::new(consumer),
            waker_context: Arc::new(context),
            downstreams: Default::default(),
            downstream_commit_logs: Default::default(),
            state_streams: Default::default(),
            engine_state: Default::default(),
        })
    }

    pub async fn table<K, V, B>(
        app_engine: Arc<AppEngine>,
        topic: String,
    ) -> Result<PTable<'static, K, V, B>, PeridotEngineRuntimeError>
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
        let mut subscription = app_engine
            .consumer
            .subscription()
            .expect("Failed to get subscription.")
            .elements()
            .into_iter()
            .map(|tp| tp.topic().to_string())
            .collect::<Vec<String>>();

        if subscription.contains(&topic) {
            return Err(PeridotEngineRuntimeError::TableCreationError(
                error::TableCreationError::TableAlreadyExists,
            ));
        }

        subscription.push(topic.clone());

        app_engine
            .consumer
            .subscribe(
                subscription
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<&str>>()
                    .as_slice(),
            )
            .expect("Failed to subscribe to topic.");

        app_engine.state_streams.insert(topic.clone());

        let (queue_sender, queue_receiver) =
            tokio::sync::mpsc::channel::<StreamPartitionQueue<PeridotConsumerContext>>(1024);

        app_engine.downstreams.insert(topic.clone(), queue_sender);

        let commit_waker = app_engine.waker_context.commit_waker();

        let backend = B::with_topic_name_and_commit_log(
            topic.as_str(), 
            app_engine.downstream_commit_logs.clone()
        ).await;

        let state_store: StateStore<B, V> = StateStore::try_new(
            topic,
            backend,
            app_engine.engine_state.clone(),
            commit_waker,
            queue_receiver,
        )?;

        Ok(PTable::<K, V, B>::new(Arc::new(state_store)))
    }

    pub async fn stream(
        app_engine: Arc<AppEngine>,
        topic: String,
    ) -> Result<PStream, PeridotEngineRuntimeError> {

        let mut subscription = app_engine
            .consumer
            .subscription()
            .expect("Failed to get subscription.")
            .elements()
            .into_iter()
            .map(|tp| tp.topic().to_string())
            .collect::<Vec<String>>();

        if subscription.contains(&topic) {
            return Err(PeridotEngineRuntimeError::TableCreationError(
                error::TableCreationError::TableAlreadyExists,
            ));
        }

        subscription.push(topic.clone());

        app_engine
            .consumer
            .subscribe(
                subscription
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<&str>>()
                    .as_slice(),
            )
            .expect("Failed to subscribe to topic.");

        //app_engine.state_streams.insert(topic.clone());

        let (queue_sender, queue_receiver) =
            tokio::sync::mpsc::channel::<StreamPartitionQueue<PeridotConsumerContext>>(1024);

        app_engine.downstreams.insert(topic.clone(), queue_sender);

        let commit_waker = app_engine.waker_context.commit_waker();

        Ok(PStream::new(
            topic,
            app_engine.downstream_commit_logs.clone(),
            app_engine.engine_state.clone(),
            commit_waker,
            queue_receiver,
        ))
    }

    pub async fn sink(
        app_engine: Arc<AppEngine>,
        topic: String,
    ) -> Result<PSink, PeridotEngineRuntimeError> {
        let mut subscription = app_engine
            .consumer
            .subscription()
            .expect("Failed to get subscription.")
            .elements()
            .into_iter()
            .map(|tp| tp.topic().to_string())
            .collect::<Vec<String>>();

        if subscription.contains(&topic) {
            return Err(PeridotEngineRuntimeError::CyclicDependencyError(topic));
        } 

        let producer = BaseProducer::from_config(&app_engine.config)?;

        Ok(PSink::new(producer, topic))
    }

    pub async fn run(&self) -> Result<(), PeridotEngineRuntimeError> {
        let pre_rebalance_waker = self.waker_context.pre_rebalance_waker();
        let commit_waker = self.waker_context.commit_waker();
        let rebalance_waker = self.waker_context.post_rebalance_waker();

        self.start_update_thread(pre_rebalance_waker);
        self.start_commit_listener(commit_waker);
        self.start_rebalance_listener(rebalance_waker);
        self.start_lag_listener();

        info!("Engine running...");

        Ok(())
    }

    fn start_update_thread(&self, mut waker: tokio::sync::broadcast::Receiver<OwnedRebalance>) {
        let commit_logs = self.downstream_commit_logs.clone();
        let consumer = self.consumer.clone();
        let downstreams = self.downstreams.clone();
        let state = self.engine_state.clone();
        let state_streams = self.state_streams.clone();

        tokio::spawn(async move {
            loop {
                debug!("Starting consumer threads...");
                let topic_partitions = consumer.assignment().expect("No subscription");

                // If there is a problem during testing, this may be it.
                // consumer.resume(&topic_partitions).expect("msg");

                let topic_partitions: Vec<_> = topic_partitions
                    .elements()
                    .iter()
                    .map(|tp| (tp.topic().to_string(), tp.partition()))
                    .collect();

                let mut count = 0;

                for (topic, partition) in topic_partitions.into_iter() {
                    count += 1;

                    debug!("Topic: {} Partition: {}", topic, partition);

                    if partition != PARTITION_UA {
                        let partition_queue = consumer
                            .split_partition_queue(topic.as_str(), partition)
                            .expect("No partition queue");

                        if state_streams.contains(&topic) {
                            let local_committed_offset = commit_logs
                                .get_offset(topic.as_str(), partition)
                                .unwrap_or(0);

                            let mut topic_partition_list = TopicPartitionList::new();
                            topic_partition_list.add_partition(topic.as_str(), partition);

                            let group_offset = consumer
                                .committed_offsets(topic_partition_list, Duration::from_secs(1))
                                .expect("No group offset")
                                .find_partition(topic.as_str(), partition)
                                .unwrap()
                                .offset()
                                .to_raw()
                                .unwrap_or(0);

                            info!(
                                "Local committed offset: {} Group offset: {}",
                                local_committed_offset, group_offset
                            );

                            if local_committed_offset < group_offset {
                                let (low_watermark, _) = consumer
                                    .client()
                                    .fetch_watermarks(topic.as_str(), partition, Duration::from_secs(1))
                                    .expect("Failed to fetch watermarks");

                                info!("Locally Committed Offset: {}, Low watermark: {}", local_committed_offset, low_watermark);

                                let max_offset = std::cmp::max(local_committed_offset, low_watermark);

                                // TODO: Seek to max(local_committed_offset, low watermark)
                                info!(
                                    "Seeking to max(lco, lwm) offset: {}",
                                    max_offset
                                );
                                consumer
                                    .seek(
                                        topic.as_str(),
                                        partition,
                                        rdkafka::Offset::Offset(max_offset),
                                        Duration::from_secs(1),
                                    )
                                    .expect("Failed to seek to group offset");
                            }
                        }

                        let queue_sender = downstreams.get(topic.as_str());

                        if let Some(queue_sender) = queue_sender {
                            let queue_sender = queue_sender.value();

                            info!("Sending partition queue to downstream: {}", topic);

                            queue_sender
                                .send(partition_queue)
                                .await
                                .expect("Failed to send partition queue");
                        } else {
                            error!("No downstream found for topic: {}", topic);
                        }
                    }
                }

                if count == 0 {
                    info!("No assigned partitions for Engine, stopping...");
                    state.store(EngineState::Stopped);
                } else {
                    state.store(EngineState::NotReady);
                }

                select! {
                    message = consumer.recv() => {
                        error!("Unexpected consumer message: {:?}", message);
                        // TODO: decide on the behaviour here because this should be unreachable
                        // panic!("Unexpected consumer message: {:?}", message);
                        // break 'outer;
                    },
                    _rebalance = waker.recv() => {
                        debug!("Rebalance waker received: {:?}", _rebalance);
                        state.store(EngineState::Stopped);
                    },
                };

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
    }

    fn start_commit_listener(&self, mut _waker: tokio::sync::broadcast::Receiver<Commit>) {
        warn!("start_commit_listener: Currently unused");
    }

    // Currently unused
    fn start_rebalance_listener(
        &self,
        mut waker: tokio::sync::broadcast::Receiver<OwnedRebalance>,
    ) {
        warn!("start_rebalance_listener: Currently unused");

        tokio::spawn(async move {
            while let Ok(rebalance) = waker.recv().await {
                match rebalance {
                    OwnedRebalance::Error(_) => {
                        error!("{}", rebalance);
                    }
                    _ => {
                        info!("{}", rebalance);
                    }
                }
            }
        });
    }

    fn start_lag_listener(&self) {
        let lag_max = 100;
        let interval = Duration::from_millis(1000);
        let consumer = self.consumer.clone();
        let consumer_state = self.engine_state.clone();
        let state_streams = self.state_streams.clone();

        tokio::spawn(async move {
            'outer: loop {
                tokio::time::sleep(interval).await;

                if consumer_state.load() == EngineState::Stopped {
                    debug!("Consumer stopped...");
                    continue;
                }

                let subscription = consumer.position().expect("Failed to get subscription");

                for consumer_tp in subscription.elements() {
                    let topic = consumer_tp.topic();
                    let partition = consumer_tp.partition();

                    let (_, high_watermark) = consumer.client()
                        .fetch_watermarks(topic, partition, Duration::from_secs(1))
                        .expect("Failed to fetch watermarks");

                    // TODO: currently this will only return a valid offset
                    // if the consumer has consumed a message from the partition.
                    // otherwise returns -1001.
                    // This is a problem because if the saved offset is 1234
                    // and the high watermark is 1234, the consumer will
                    // appear as though it is lagging, when in fact it is not.
                    let consumer_offset = consumer_tp
                        .offset()
                        .to_raw()
                        .expect("Failed to convert consumer offset to i64");

                    if high_watermark < 0 {
                        debug!(
                            "Broker offset not found for topic partition: {}->{}",
                            topic, partition
                        );
                    } else if consumer_offset < 0 {
                        debug!(
                            "Consumer offset not found for topic partition: {}->{}",
                            topic, partition
                        );
                    } else if consumer_offset + lag_max < high_watermark {
                        match consumer_state.load() {
                            EngineState::Running | EngineState::NotReady => {
                                consumer_state.store(EngineState::Lagging);

                                info!(
                                    "Consumer topic partition {}->{} lagging: {} < {}",
                                    topic, partition, consumer_offset, high_watermark
                                );

                                let assignment = consumer
                                    .assignment()
                                    .expect("Failed to get assignment");

                                let assignment_elements = assignment
                                    .elements();

                                let stream_topics = assignment_elements
                                    .iter()
                                    .filter(|tp| !state_streams.contains(tp.topic()))
                                    .collect::<Vec<&TopicPartitionListElem>>();

                                let mut tp_list = TopicPartitionList::new();

                                for tp in stream_topics {
                                    tp_list.add_partition(tp.topic(), tp.partition());
                                }

                                consumer.pause(&tp_list).expect("Failed to pause consumer");
                                
                                continue 'outer;
                            }
                            state => {
                                info!("Consumer {}...", state);
                                continue 'outer;
                            }
                        }
                    }
                }

                if consumer_state.load() == EngineState::Lagging {
                    consumer_state.store(EngineState::Running);
                    let assignment = consumer
                        .assignment()
                        .expect("Failed to get assignment");

                    let assignment_elements = assignment
                        .elements();

                    let stream_topics = assignment_elements
                        .iter()
                        .filter(|tp| !state_streams.contains(tp.topic()))
                        .collect::<Vec<&TopicPartitionListElem>>();

                    let mut tp_list = TopicPartitionList::new();

                    for tp in stream_topics {
                        tp_list.add_partition(tp.topic(), tp.partition());
                    }

                    consumer.resume(&tp_list).expect("Failed to pause consumer");
                    info!("Consumer caught up...");
                } else if consumer_state.load() == EngineState::NotReady {
                    consumer_state.store(EngineState::Running);
                    info!("Consumer running...");
                }

                tokio::time::sleep(interval).await;
            }
        });
    }
}
