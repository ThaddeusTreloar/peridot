use std::{collections::HashMap, sync::Arc, time::{Duration, Instant, SystemTime}, marker::PhantomData, default, fmt::{Display, Write}};

use crossbeam::atomic::AtomicCell;
use rdkafka::{consumer::{StreamConsumer, Consumer, stream_consumer::StreamPartitionQueue, ConsumerContext}, ClientConfig, Message, message::OwnedMessage, topic_partition_list, ClientContext, error::KafkaError, util::DefaultRuntime, producer::PARTITION_UA};
use futures::{StreamExt, Stream, Future};
use serde::de::DeserializeOwned;
use tokio::{sync::{RwLock, mpsc::Receiver}, select};
use tracing::{info, debug, error};

use self::{error::StateStoreCreationError, extensions::{StateStoreContext, OwnedRebalance}, backend::{WriteableStateBackend, ReadableStateBackend}};

pub mod error;
pub mod backend;
mod extensions;

#[derive(Debug, PartialEq, Eq, Default, Clone, Copy, PartialOrd, Ord)]
pub enum ConsumerState {
    #[default]
    Stopped,
    Running,
    Initialised,
    Rebalancing,
    Lagging,
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
where U: DeserializeOwned + Send + Sync + 'static
{
    consumer: Arc<StreamConsumer<StateStoreContext>>,
    backend: Arc<T>,
    state: Arc<AtomicCell<ConsumerState>>,
    _lifetime: &'a PhantomData<()>,
    _type: PhantomData<U>,
}

async fn start_partition_update_thread<C, R, T>(parition_queue: StreamPartitionQueue<C, R>, store: Arc<impl WriteableStateBackend<T>>) 
where C: ConsumerContext, T: serde::de::DeserializeOwned + Send + Sync + 'static
{
    parition_queue
        .stream()
        .filter_map(|item| async {
                match item {
                    Ok(i) => Some(i),
                    Err(_) => None
            }
        }).for_each(|msg| {
            let store_ref = store.clone();

            async move {
                let raw_key = msg.key().expect("No key");
                debug!("Key: {:?}", raw_key);
                let key = String::from_utf8_lossy(raw_key).to_string();
                let raw_value = msg.payload().expect("No value");
                debug!("Value: {:?}", raw_value);
                let value = serde_json::from_slice::<T>(raw_value).expect("Failed to deserialize value");

                store_ref.set(&key, value).await;
        }}).await;
}

impl <'a, T, U> StateStore<'a, T, U> 
where T: Default + ReadableStateBackend<U> + WriteableStateBackend<U> + Send + Sync + 'static,
        U: DeserializeOwned + Send + Sync + 'static + Default
{
    pub fn from_consumer_config(topic: &str, config: &ClientConfig) -> Result<Arc<Self>, StateStoreCreationError> {
        let (pre_sender, pre_reciever) = tokio::sync::mpsc::channel(100);
        let (post_sender, post_reciever) = tokio::sync::mpsc::channel(100);

        let context = StateStoreContext::new(pre_sender, post_sender);

        let client = config.create_with_context(context)?;
        let backend = Default::default();

        StateStore::try_new(topic, client, backend, pre_reciever, post_reciever)
    }
}

impl <'a, T, U> StateStore<'a, T, U> 
where T: ReadableStateBackend<U> + WriteableStateBackend<U> + Send + Sync + 'static,
        U: DeserializeOwned + Send + Sync + 'static + Default
{
    pub fn from_consumer_config_and_backend(topic: &str, config: &ClientConfig, backend: T) -> Result<Arc<Self>, StateStoreCreationError> {
        let (pre_sender, pre_reciever) = tokio::sync::mpsc::channel(100);
        let (post_sender, post_reciever) = tokio::sync::mpsc::channel(100);

        let context = StateStoreContext::new(pre_sender, post_sender);

        let client = config.create_with_context(context)?;

        StateStore::try_new(topic, client, backend, pre_reciever, post_reciever)
    }

    fn try_new(topic: &str, client: StreamConsumer<StateStoreContext>, backend: T, pre_rebalance_waker: Receiver<OwnedRebalance>, post_rebalance_waker: Receiver<OwnedRebalance>) -> Result<Arc<Self>, StateStoreCreationError> {
        let state_store = Arc::new(StateStore {
            consumer: Arc::new(client),
            backend: Arc::new(backend),
            state: Default::default(),
            _lifetime: &PhantomData,
            _type: PhantomData,
        });

        state_store.consumer.subscribe(&[topic])?;

        state_store.start_update_thread(pre_rebalance_waker);
        state_store.start_lag_listener();
        
        state_store.clone().start_rebalance_listener(post_rebalance_waker);
        
        Ok(state_store)
    }

    fn start_update_thread(&self, mut waker: Receiver<OwnedRebalance>) {
        let consumer = self.consumer.clone();
        let store = self.backend.clone();
        let state = self.state.clone();

        self.backend.set("jon", Default::default());

        tokio::spawn(
            async move {
                loop {
                    info!("Starting consumer threads...");
                    let topic_partitions = consumer.assignment().expect("No subscription");

                    consumer.resume(&topic_partitions).expect("msg");

                    let topic_partition_vec = topic_partitions.elements();

                    let mut count = 0;

                    for topic_partition in topic_partition_vec {
                        let partition = topic_partition.partition();
                        let topic = topic_partition.topic();
                        
                        count+=1;
                        debug!("Topic: {} Partition: {}", topic, partition);

                        if partition != PARTITION_UA {
                            let partition_queue = consumer.split_partition_queue(topic, partition).expect("No partition queue");
                            
                            tokio::spawn(start_partition_update_thread(partition_queue, store.clone()));
                        } 
                        /*else {
                            let topic_md = consumer
                                .fetch_metadata(Some(topic), Duration::from_millis(1000))
                                .expect("Failed to get topic metadata");

                            let partitions = topic_md.topics().iter().find(
                                    |t| t.name() == topic 
                                ).expect("Failed to find topic metadata")
                                .partitions()
                                .iter()
                                .map(
                                    |tp|tp.id()    
                                );

                            for partition in partitions {
                                debug!("Starting update thread for topic: {} partition: {}", topic, partition);

                                let partition_queue = consumer.split_partition_queue(topic, partition).expect("No partition queue");

                                tokio::spawn(start_partition_update_thread(partition_queue, store.clone()));
                            }
                        }*/
                    }

                    if count == 0 {
                        info!("No partitions found for topic, stopping...");
                        state.store(ConsumerState::Stopped);
                    } else {
                        state.store(ConsumerState::Initialised);
                    }

                    select! {
                        message = consumer.recv() => {
                            error!("Unexpected consumer message: {:?}", message);
                        },
                        rebalance = waker.recv() => {
                            info!("Rebalance waker received.");
                        },
                    };

                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            }
        );
    }

    fn start_rebalance_listener(self: Arc<Self>, mut waker: Receiver<OwnedRebalance>) {
        tokio::spawn(
            async move {
                while let Some(rebalance) = waker.recv().await {
                    match rebalance {
                        OwnedRebalance::Assign(tp_list) => {
                            info!("Rebalance: Assign {:?}", tp_list);
                        },
                        OwnedRebalance::Revoke(tp_list) => {
                            info!("Rebalance: Revoke {:?}", tp_list);
                        },
                        OwnedRebalance::Error(e) => {
                            info!("Rebalance: Error {:?}", e);
                        }
                    }
                }
            }
        );
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
    
                let offsets = match consumer.offsets_for_timestamp(current_time, Duration::from_millis(1000)) {
                    Ok(offsets) => offsets,
                    Err(e) => {
                        info!("Failed to get offsets for timestamp: {}, due to: {}", current_time, e);
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
    
                    if consumer_offset+lag_max < broker_offset {
                        match consumer_state.load() {
                            ConsumerState::Running | 
                            ConsumerState::Initialised => {

                                consumer_state.store(ConsumerState::Lagging);
    
                                info!("Consumer lagging...");
                                continue 'outer;
                            },
                            state => {
                                info!("Consumer {}...", state);
                                continue 'outer;
                            },
                        }
                    }
                }
    
                if consumer_state.load() == ConsumerState::Lagging {
                    consumer_state.store(ConsumerState::Running);
                    info!("Consumer no longer lagging...");
                } else if consumer_state.load() == ConsumerState::Initialised {
                    consumer_state.store(ConsumerState::Running);
                    info!("Consumer running...");
                }
    
                tokio::time::sleep(interval).await;
            }
        });
    }

    
}

impl <'a, T, U> ReadableStateStore<U> for StateStore<'a, T, U> 
where T: ReadableStateBackend<U> + WriteableStateBackend<U>,
        U: DeserializeOwned + Clone + Send + Sync + 'static
{
    async fn get(&self, key: &str) -> Option<U> {
        while let ConsumerState::Lagging | 
            ConsumerState::Stopped |
            ConsumerState::Rebalancing |
            ConsumerState::Initialised = self.state.load() 
        {
            info!(
                "State store not ready: {}",
                self.state.load()
            );
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        self.backend
            .get(key)
            .await
    }
}