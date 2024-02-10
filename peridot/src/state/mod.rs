use std::{
    fmt::Display,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime}, collections::HashMap,
};

use crossbeam::atomic::AtomicCell;
use futures::StreamExt;
use rdkafka::{
    consumer::{stream_consumer::StreamPartitionQueue, Consumer, ConsumerContext, StreamConsumer},
    producer::PARTITION_UA,
    ClientConfig, Message, topic_partition_list::TopicPartitionListElem, TopicPartitionList,
};
use serde::de::DeserializeOwned;
use tokio::{select, sync::mpsc::Receiver};
use tracing::{debug, error, info, warn, trace};

use crate::types::Topic;

use self::{
    backend::{ReadableStateBackend, WriteableStateBackend, StateBackend, StateStoreCommit},
    error::StateStoreCreationError,
    extensions::{OwnedRebalance, StateStoreContext},
};

pub mod backend;
pub mod error;
mod extensions;

#[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
pub enum ConsumerState {
    Lagging,
    NotReady,
    Rebalancing,
    Running,
    #[default]
    Stopped,
}

impl Display for ConsumerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub trait ReadableStateStore<T> {
    async fn get(&self, key: &str) -> Option<T>;
}

pub trait WriteableStateStore<T> {
    async fn set(&self, key: &str, value: T) -> Option<T>;
    async fn delete(&self, key: &str) -> Option<T>;
}

pub struct StateStore<'a, T, U>
where
    U: DeserializeOwned + Send + Sync + 'static,
{
    consumer: Arc<StreamConsumer<StateStoreContext>>,
    backend: Arc<T>,
    state: Arc<AtomicCell<ConsumerState>>,
    _lifetime: &'a PhantomData<()>,
    _type: PhantomData<U>,
}

async fn start_partition_update_thread<C, R, T>(
    parition_queue: StreamPartitionQueue<C, R>,
    store: Arc<impl WriteableStateBackend<T>>,
) where
    C: ConsumerContext,
    T: serde::de::DeserializeOwned + Send + Sync + 'static,
{
    parition_queue
        .stream()
        .filter_map(|item| async {
            match item {
                Ok(i) => Some(i),
                Err(_) => None,
            }
        })
        .for_each(|msg| {
            let store_ref = store.clone();

            async move {
                let raw_key = msg.key().expect("No key");
                trace!("Key: {:?}", raw_key);
                let key = String::from_utf8_lossy(raw_key).to_string();
                let raw_value = msg.payload().expect("No value");
                trace!("Value: {:?}", raw_value);
                let value =
                    serde_json::from_slice::<T>(raw_value).expect("Failed to deserialize value");

                store_ref.set(&key, value).await;
            }
        })
        .await;
}

impl<'a, T, U> StateStore<'a, T, U>
where
    T: Default + StateBackend + ReadableStateBackend<U> + WriteableStateBackend<U> + Send + Sync + 'static,
    U: DeserializeOwned + Send + Sync + 'static + Default,
{
    pub fn from_consumer_config(
        topic: &str,
        config: &ClientConfig,
    ) -> Result<Self, StateStoreCreationError> {
        let (context, pre_rebalance_waker, post_rebalance_waker, commit_waker) = StateStoreContext::new();

        let client = config.create_with_context(context)?;
        let backend = Default::default();

        StateStore::try_new(topic, client, backend, pre_rebalance_waker, post_rebalance_waker, commit_waker)
    }
}

impl<'a, T, U> StateStore<'a, T, U>
where
    T: StateBackend + ReadableStateBackend<U> + WriteableStateBackend<U> + Send + Sync + 'static,
    U: DeserializeOwned + Send + Sync + 'static + Default,
{
    pub fn from_consumer_config_and_backend(
        topic: &str,
        config: &ClientConfig,
        backend: T,
    ) -> Result<Self, StateStoreCreationError> {
        let (context, pre_rebalance_waker, post_rebalance_waker, commit_waker) = StateStoreContext::new();

        let client = config.create_with_context(context)?;

        StateStore::try_new(topic, client, backend, pre_rebalance_waker, post_rebalance_waker, commit_waker)
    }

    fn try_new(
        topic: &str,
        client: StreamConsumer<StateStoreContext>,
        backend: T,
        pre_rebalance_waker: Receiver<OwnedRebalance>,
        post_rebalance_waker: Receiver<OwnedRebalance>,
        commit_waker: Receiver<StateStoreCommit>,
    ) -> Result<Self, StateStoreCreationError> {
        let state_store = StateStore {
            consumer: Arc::new(client),
            backend: Arc::new(backend),
            state: Default::default(),
            _lifetime: &PhantomData,
            _type: PhantomData,
        };

        state_store.consumer.subscribe(&[topic])?;

        state_store.start_update_thread(pre_rebalance_waker);
        state_store.start_lag_listener();

        state_store.start_rebalance_listener(post_rebalance_waker);
        state_store.start_commit_listener(commit_waker);

        Ok(state_store)
    }

    fn start_update_thread(&self, mut waker: Receiver<OwnedRebalance>) {
        let consumer = self.consumer.clone();
        let store = self.backend.clone();
        let state = self.state.clone();

        self.backend.set("jon", Default::default());

        tokio::spawn(async move {
            '_outer: loop {
                debug!("Starting consumer threads...");
                let topic_partitions = consumer.assignment().expect("No subscription");

                consumer.resume(&topic_partitions).expect("msg");

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

                        let local_committed_offset = store.get_offset(topic.as_str(), partition).await.unwrap_or(0);
                        
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

                        tokio::spawn(start_partition_update_thread(
                            partition_queue,
                            store.clone(),
                        ));
                    }
                }

                if count == 0 {
                    info!("No partitions found for topic, stopping state store...");
                    state.store(ConsumerState::Stopped);
                } else {
                    state.store(ConsumerState::NotReady);
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
                        state.store(ConsumerState::Stopped);
                    },
                };

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
    }

    fn start_commit_listener(&self, mut waker: Receiver<StateStoreCommit>) {
        let store = self.backend.clone();

        tokio::spawn(async move {
            while let Some(message) = waker.recv().await {
                store.commit_offset(
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
                .await;
            }
        });
    }

    // Currently unused
    fn start_rebalance_listener(&self, mut waker: Receiver<OwnedRebalance>) {
        tokio::spawn(async move {
            while let Some(rebalance) = waker.recv().await {
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
        let consumer_state = self.state.clone();

        tokio::spawn(async move {
            'outer: loop {
                tokio::time::sleep(interval).await;

                if consumer_state.load() == ConsumerState::Stopped {
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
                            ConsumerState::Running | ConsumerState::NotReady => {
                                consumer_state.store(ConsumerState::Lagging);

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

                if consumer_state.load() == ConsumerState::Lagging {
                    consumer_state.store(ConsumerState::Running);
                    info!("Consumer no longer lagging...");
                } else if consumer_state.load() == ConsumerState::NotReady {
                    consumer_state.store(ConsumerState::Running);
                    info!("Consumer running...");
                }

                tokio::time::sleep(interval).await;
            }
        });
    }
}

impl<'a, T, U> ReadableStateStore<U> for StateStore<'a, T, U>
where
    T: ReadableStateBackend<U> + WriteableStateBackend<U>,
    U: DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn get(&self, key: &str) -> Option<U> {
        while let ConsumerState::Lagging
        | ConsumerState::Stopped
        | ConsumerState::Rebalancing
        | ConsumerState::NotReady = self.state.load()
        {
            info!("State store not ready: {}", self.state.load());
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        self.backend.get(key).await
    }
}
