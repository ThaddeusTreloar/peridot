use std::{collections::{hash_map::DefaultHasher, HashMap}, fmt::Display, sync::Arc, time::{Duration, SystemTime}};

use crossbeam::atomic::AtomicCell;
use dashmap::DashMap;
use rdkafka::{consumer::{StreamConsumer, stream_consumer::StreamPartitionQueue, Consumer}, ClientConfig, producer::PARTITION_UA, TopicPartitionList};
use tokio::{sync::mpsc::Sender, select};
use tracing::{debug, info, error, warn};

use crate::state::backend::{StateBackend, CommitLogs};

use self::error::{PeridotEngineCreationError, PeridotEngineRuntimeError};

use super::{extensions::{PeridotConsumerContext, OwnedRebalance, Commit}, PeridotConsumer, PeridotPartitionQueue, PTable};

pub mod error;

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

#[derive()]
pub struct TableBuilder<K, V> {
    engine: Arc<AppEngine>,
    state_type: AtomicCell<StateType>,
    topic: String,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl <K, V> TableBuilder<K, V> {
    pub fn new(topic: &str, engine: Arc<AppEngine>) -> Self {
        TableBuilder {
            engine,
            state_type: Default::default(),
            topic: topic.to_string(),
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub fn with_state_type(self, state_type: StateType) -> Self {
        self.state_type.store(state_type);
        self
    }

    pub fn build(self) -> Result<PTable<K, V>, PeridotEngineRuntimeError> {
        let new_table = NewTable::from(self);

        self.engine.table(new_table)
    }
}

pub struct NewTable<K, V> {
    state_type: AtomicCell<StateType>,
    topic: String,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl <K, V> From<TableBuilder<K, V>> for NewTable<K, V> {
    fn from(builder: TableBuilder<K, V>) -> Self {
        NewTable {
            state_type: builder.state_type,
            topic: builder.topic,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

pub struct AppEngine {
    consumer: Arc<PeridotConsumer>,
    waker_context: Arc<PeridotConsumerContext>,
    downstreams: Arc<DashMap<String, Sender<PeridotPartitionQueue>>>,
    downstream_commit_logs: Arc<CommitLogs>,
    state_streams: Arc<Vec<String>>,
    engine_state: Arc<AtomicCell<EngineState>>,
}

impl AppEngine {
    pub fn table<K, V>(&self, NewTable {
        state_type,
        topic,
        ..
    }: NewTable<K, V>) -> Result<PTable<K, V>, PeridotEngineRuntimeError> {
        let mut subscription = self.consumer
            .subscription()
            .expect("Failed to get subscription.")
            .elements()
            .into_iter()
            .map(|tp|tp.topic().to_string())
            .collect::<Vec<String>>();

        if subscription.contains(&topic.to_string()) {
            return Err(
                PeridotEngineRuntimeError::TableCreationError(
                    error::TableCreationError::TableAlreadyExists
                )
            );
        }

        subscription.push(topic.to_string());

        self.consumer.subscribe(
            subscription
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>()
                .as_slice()
            )
            .expect("Failed to subscribe to topic.");

        self.state_streams.push(topic.to_string());

        //let (queue_sender, queue_receiver) = tokio::sync::mpsc::channel::<StreamPartitionQueue<PeridotConsumerContext>>(1024);

        Ok(PTable::<K, V>{  })
    }

    pub fn from_config(config: &ClientConfig) -> Result<Self, PeridotEngineCreationError> {
        let context = PeridotConsumerContext::new();
        let consumer = config.create_with_context(context.clone())?;

        Ok(AppEngine {
            consumer: Arc::new(consumer),
            waker_context: Arc::new(context),
            downstreams: Default::default(),
            downstream_commit_logs: Default::default(),
            state_streams: Default::default(),
            engine_state: Default::default(),
        })
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
                    .map(
                        |tp| (
                            tp.topic().to_string(), 
                            tp.partition(), 
                        ),
                    ).collect();

                let mut count = 0;

                for (topic, partition) in topic_partitions.into_iter() {
                    count += 1;

                    debug!(
                        "Topic: {} Partition: {}",
                        topic,
                        partition
                    );

                    if partition != PARTITION_UA {
                        let partition_queue = consumer
                            .split_partition_queue(
                                topic.as_str(),
                                partition,
                            )
                            .expect("No partition queue");

                        if state_streams.contains(&topic) {
                            let local_committed_offset = commit_logs.get_offset(topic.as_str(), partition).unwrap_or(0);
    
                            let mut topic_partition_list = TopicPartitionList::new();
                            topic_partition_list.add_partition(topic.as_str(), partition);
    
                            let group_offset = consumer
                                .committed_offsets(
                                    topic_partition_list,
                                    Duration::from_secs(1),
                                )
                                .expect("No group offset")
                                .find_partition(topic.as_str(), partition)
                                .unwrap()
                                .offset()
                                .to_raw()
                                .unwrap_or(0);
    
                            info!(
                                "Local committed offset: {} Group offset: {}",
                                local_committed_offset,
                                group_offset
                            );
    
                            if local_committed_offset < group_offset {
                                info!("Seeking to locally committed offset: {}", local_committed_offset);
                                consumer.seek(topic.as_str(), partition, rdkafka::Offset::Offset(local_committed_offset), Duration::from_secs(1))
                                    .expect("Failed to seek to group offset");
                            }
                        }

                        let queue_sender = downstreams
                            .get(topic.as_str());

                        if let Some(queue_sender) = queue_sender {
                            let queue_sender = queue_sender.value();

                            queue_sender.send(partition_queue).await.expect("Failed to send partition queue");
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

    fn start_commit_listener(&self, mut waker: tokio::sync::broadcast::Receiver<Commit>) {
        warn!("start_commit_listener: Currently unused");
    }

    // Currently unused
    fn start_rebalance_listener(&self, mut waker: tokio::sync::broadcast::Receiver<OwnedRebalance>) {
        warn!("start_rebalance_listener: Currently unused")

        /*tokio::spawn(async move {
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
        });*/
    }

    fn start_lag_listener(&self) {
        let lag_max = 100;
        let interval = Duration::from_millis(1000);
        let consumer = self.consumer.clone();
        let consumer_state = self.engine_state.clone();

        tokio::spawn(async move {
            'outer: loop {
                tokio::time::sleep(interval).await;

                if consumer_state.load() == EngineState::Stopped {
                    info!("Consumer stopped...");
                    continue;
                }

                let subscription = consumer.position().expect("Failed to get subscription");

                let current_time: i64 = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Failed to get time")
                    .as_millis() as i64;

                let offsets = match consumer
                    .offsets_for_timestamp(current_time, Duration::from_millis(1000))
                {
                    Ok(offsets) => offsets,
                    Err(e) => {
                        info!(
                            "Failed to get offsets for timestamp: {}, due to: {}",
                            current_time, e
                        );
                        continue;
                    }
                };

                for consumer_tp in subscription.elements() {
                    let topic = consumer_tp.topic();
                    let partition = consumer_tp.partition();

                    let broker_offset_tp = offsets
                        .find_partition(topic, partition)
                        .expect("Failed to get topic partition");

                    let consumer_offset = consumer_tp
                        .offset()
                        .to_raw()
                        .expect("Failed to convert consumer offset to i64");

                    let broker_offset = broker_offset_tp
                        .offset()
                        .to_raw()
                        .expect("Failed to convert broker offset to i64");

                    if broker_offset == -1 {
                        info!("Broker offset not found for topic partition: {}->{}", topic, partition);
                    } else if consumer_offset + lag_max < broker_offset {
                        match consumer_state.load() {
                            EngineState::Running | EngineState::NotReady => {
                                consumer_state.store(EngineState::Lagging);

                                info!("Consumer topic partition {}->{} lagging: {} < {}", topic, partition, consumer_offset, broker_offset);
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
                    info!("Consumer no longer lagging...");
                } else if consumer_state.load() == EngineState::NotReady {
                    consumer_state.store(EngineState::Running);
                    info!("Consumer running...");
                }

                tokio::time::sleep(interval).await;
            }
        });
    }
}