use std::{collections::HashMap, sync::Arc, time::{Duration, Instant, SystemTime}, marker::PhantomData, default};

use crossbeam::atomic::AtomicCell;
use rdkafka::{consumer::{StreamConsumer, Consumer, stream_consumer::StreamPartitionQueue, ConsumerContext}, ClientConfig, Message, message::OwnedMessage, topic_partition_list, ClientContext, error::KafkaError, util::DefaultRuntime};
use futures::{StreamExt, Stream};
use tokio::sync::RwLock;
use tracing::{info, debug};
use tracing_subscriber::field::debug;

use self::error::StateStoreCreationError;

pub mod error;

#[derive(Debug)]
pub struct StateStoreContext {
    channel: tokio::sync::mpsc::Sender<OwnedRebalance>,
}

impl ClientContext for StateStoreContext {

}

enum OwnedRebalance {
    Assign(topic_partition_list::TopicPartitionList),
    Revoke(topic_partition_list::TopicPartitionList),
    Error(KafkaError)
}

impl From<&rdkafka::consumer::Rebalance<'_>> for OwnedRebalance {
    fn from(rebalance: &rdkafka::consumer::Rebalance<'_>) -> Self {
        match rebalance.clone() {
            rdkafka::consumer::Rebalance::Assign(tp_list) => OwnedRebalance::Assign(tp_list.clone()),
            rdkafka::consumer::Rebalance::Revoke(tp_list) => OwnedRebalance::Revoke(tp_list.clone()),
            rdkafka::consumer::Rebalance::Error(e) => OwnedRebalance::Error(e),
        }
    }
}

impl ConsumerContext for StateStoreContext {
    fn post_rebalance<'a>(&self, rebalance: &rdkafka::consumer::Rebalance<'_>) {
        let owned_rebalance: OwnedRebalance = rebalance.into();

        self.channel.try_send(owned_rebalance).expect("Failed to send rebalance");
    }
}

#[derive(Debug, PartialEq, Eq, Default, Clone, Copy, PartialOrd, Ord)]
pub enum ConsumerState {
    #[default]
    Stopped,
    Running,
    Rebalancing,
    Lagging,
}

pub trait ReadableStateStore<T> {
    async fn get(&self, key: &str) -> Option<T>;
}

pub trait WriteableStateStore<T> {
    async fn set(&self, key: &str, value: T) -> Option<T>;
    async fn delete(&self, key: &str) -> Option<T>;
}

pub struct InMemoryStateStore<'a, T> 
where T: serde::Deserialize<'a>
{
    consumer: Arc<StreamConsumer<StateStoreContext>>,
    store: Arc<RwLock<HashMap<String, T>>>,
    state: Arc<AtomicCell<ConsumerState>>,
    _lifetime: &'a PhantomData<()>
}

async fn start_partition_update_thread<C, R, T>(parition_queue: StreamPartitionQueue<C, R>, store: Arc<RwLock<HashMap<String, T>>>) 
where C: ConsumerContext, T: serde::de::DeserializeOwned,
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

                store_ref.write().await.insert(key, value);
        }}).await;
}

impl <'a, T> InMemoryStateStore<'a, T> 
where T: serde::de::DeserializeOwned + Send + Sync + 'static
{
    pub fn from_consumer_config(config: &ClientConfig, topic: &str) -> Result<Arc<Self>, StateStoreCreationError> {
        let (sender, reciever) = tokio::sync::mpsc::channel(100);

        let context = StateStoreContext {
            channel: sender
        };

        let client = config.create_with_context(context)?;

        let store = InMemoryStateStore {
            consumer: Arc::new(client),
            store: Default::default(),
            state: Default::default(),
            _lifetime: &PhantomData
        };

        store.consumer.subscribe(&[topic])?;

        store.start_update_thread();
        store.start_lag_listener();
        
        store.state.store(ConsumerState::Lagging);
        
        while let ConsumerState::Lagging = store.state.load() {
            info!(
                "Consumer state: {:?} waiting for consumer to start...",
                store.state.load()
            );
            std::thread::sleep(Duration::from_millis(1000));
        }

        let self_ref = Arc::new(store);

        self_ref.clone().start_rebalance_listener(reciever);
        
        Ok(self_ref)
    }

    fn start_update_thread(&self) {
        let consumer = self.consumer.clone();
        let store = self.store.clone();

        tokio::spawn(
            async move {
                let topic_partitions = consumer.subscription().expect("No subscription");

                consumer.resume(&topic_partitions).expect("msg");

                for topic_partition in topic_partitions.elements() {
                    let partition = topic_partition.partition();
                    let topic = topic_partition.topic();

                    if partition == -1 {
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
                    } else {
                        let partition_queue = consumer.split_partition_queue(topic, partition).expect("No partition queue");
                        
                        tokio::spawn(start_partition_update_thread(partition_queue, store.clone()));
                    }
                }

                let _ = consumer.recv().await;
                println!("Consumer unexpecdly stopped: ");
            }
        );
    }

    fn start_rebalance_listener(self: Arc<Self>, mut reciever: tokio::sync::mpsc::Receiver<OwnedRebalance>) {
        tokio::spawn(
            async move {
                while let Some(rebalance) = reciever.recv().await {
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
            loop {
                tokio::time::sleep(interval).await;
    
                let subscription = consumer.position().expect("Failed to get subscription");
    
                let current_time: i64 = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Failed to get time")
                    .as_millis() as i64;
    
                let offsets = match consumer.offsets_for_timestamp(current_time, Duration::from_millis(1000)) {
                    Ok(offsets) => offsets,
                    Err(_) => {
                        info!("Failed to get offsets for timestamp: {:?}", consumer_state.load());
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
                            ConsumerState::Rebalancing |
                            ConsumerState::Stopped |
                            ConsumerState::Lagging => {
                                info!("Consumer still lagging...");
                                continue;
                            },
                            ConsumerState::Running => {
                                consumer_state.store(ConsumerState::Lagging);
    
                                info!("Consumer lagging...");
                                continue;
                            },
                        }
                    }
                }
    
                if consumer_state.load() == ConsumerState::Lagging {
                    consumer_state.store(ConsumerState::Running);
                    info!("Consumer no longer lagging...");
                }
    
                tokio::time::sleep(interval).await;
            }
        });
    }

    
}

impl <'a, T> ReadableStateStore<T> for InMemoryStateStore<'a, T> 
where T: serde::de::DeserializeOwned + Clone
{
    async fn get(&self, key: &str) -> Option<T> {
        while let ConsumerState::Lagging | ConsumerState::Rebalancing = self.state.load() {
            info!(
                "Consumer state: {:?} waiting for consumer to catch up...",
                self.state.load()
            );
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        while let ConsumerState::Stopped = self.state.load() {
            info!(
                "Consumer state: {:?} waiting for consumer to start...",
                self.state.load()
            );
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        Some(
            self.store
                .read().await
                .get(key)?
                .clone()
        )
    }
}