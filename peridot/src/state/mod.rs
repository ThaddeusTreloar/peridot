use std::{marker::PhantomData, sync::Arc, time::Duration};

use crossbeam::atomic::AtomicCell;
use futures::{Future, StreamExt};
use rdkafka::{consumer::Consumer, Message, TopicPartitionList};
use tracing::info;

use crate::{
    app::PeridotConsumer,
    engine::{partition_queue::StreamPeridotPartitionQueue, EngineState, RawQueueReceiver},
    serde_ext::PDeserialize,
};

use self::{
    backend::{ReadableStateBackend, StateBackend, WriteableStateBackend},
    error::StateStoreCreationError,
};

pub mod backend;
pub mod error;
pub mod table;

pub trait ReadableStateStore<K, V> {
    fn get(&self, key: &K) -> impl Future<Output = Option<V>>;
}

pub trait WriteableStateStore<K, V> {
    fn set(&self, key: K, value: V) -> impl Future<Output = Option<V>>;
    fn delete(&self, key: K) -> impl Future<Output = Option<V>>;
}

pub struct StateStore<KS, VS, T>
where
    KS: PDeserialize,
    VS: PDeserialize,
    //U: DeserializeOwned + Send + Sync + 'static,
{
    topic: String,
    backend: Arc<T>,
    state: Arc<AtomicCell<EngineState>>,
    consumer_ref: Arc<PeridotConsumer>,
    _key_serialiser: PhantomData<KS>,
    _value_serialiser: PhantomData<VS>,
}

async fn start_partition_update_thread<KS, VS>(
    topic: String,
    partition: i32,
    parition_queue: StreamPeridotPartitionQueue,
    consumer_ref: Arc<PeridotConsumer>,
    store: Arc<impl WriteableStateBackend<KS::Output, VS::Output>>,
) where
    KS: PDeserialize + Send + Sync,
    VS: PDeserialize + Send + Sync,
    KS::Output: Send + Sync,
    VS::Output: Send + Sync,
{
    parition_queue
        .for_each(|msg| {
            let _store_ref = store.clone();
            let consumer_ref = consumer_ref.clone();
            let topic_ref = topic.as_str();

            async move {
                let raw_key = msg.key().unwrap();

                let _key = KS::deserialize(raw_key).expect("Failed to deserialise key");

                let raw_value = msg.payload().unwrap();

                let _ = VS::deserialize(raw_value).expect("Failed to deserialise value");

                //store_ref.commit_update(&key, value).await;

                let mut topic_partition_list = TopicPartitionList::default();
                topic_partition_list
                    .add_partition_offset(
                        topic_ref,
                        partition,
                        rdkafka::Offset::Offset(msg.offset() + 1),
                    )
                    .expect("Failed to add partition offset");
                consumer_ref
                    .commit(&topic_partition_list, rdkafka::consumer::CommitMode::Async)
                    .expect("Failed to make async commit in state store");
            }
        })
        .await;
}

impl<KS, VS, T> StateStore<KS, VS, T>
where
    KS: PDeserialize + Send + Sync + 'static,
    VS: PDeserialize + Send + Sync + 'static,
    KS::Output: Send + Sync + 'static,
    VS::Output: Send + Sync + 'static,
    T: StateBackend
        + ReadableStateBackend<KeyType = KS::Output, ValueType = VS::Output>
        + WriteableStateBackend<KS::Output, VS::Output>
        + Send
        + Sync
        + 'static,
{
    pub fn try_new(
        topic: String,
        backend: T,
        consumer_ref: Arc<PeridotConsumer>,
        state: Arc<AtomicCell<EngineState>>,
        stream_queue: RawQueueReceiver,
    ) -> Result<Self, StateStoreCreationError> {
        let state_store = StateStore {
            topic: topic,
            backend: Arc::new(backend),
            state,
            consumer_ref,
            _key_serialiser: PhantomData,
            _value_serialiser: PhantomData,
        };

        state_store.start_update_thread(stream_queue);

        Ok(state_store)
    }

    pub fn new_new() -> () {

        //
    }

    pub fn new_update_thread() {
        // wait for new queue
        // build producer
        // start update thread
        // {
        //      waits for message
        //      queues item for production
        //      store in state store
        //      commit transaction, on failure, revert state store changes
        // }
    }

    fn start_update_thread(&self, mut stream_queue: RawQueueReceiver) {
        let store = self.backend.clone();
        let consumer_ref = self.consumer_ref.clone();
        let topic = self.topic.clone();

        tokio::spawn(async move {
            while let Some((partition, queue)) = stream_queue.recv().await {
                tokio::spawn(start_partition_update_thread::<KS, VS>(
                    topic.clone(),
                    partition,
                    queue,
                    consumer_ref.clone(),
                    store.clone(),
                ));
            }
        });
    }
}

impl<KS, VS, T> ReadableStateStore<KS::Output, VS::Output> for StateStore<KS, VS, T>
where
    KS: PDeserialize,
    VS: PDeserialize,
    T: ReadableStateBackend<KeyType = KS::Output, ValueType = VS::Output>
        + WriteableStateBackend<KS::Output, VS::Output>,
{
    async fn get(&self, _: &KS::Output) -> Option<VS::Output> {
        while let EngineState::Lagging
        | EngineState::Stopped
        | EngineState::Rebalancing
        | EngineState::NotReady = self.state.load()
        {
            info!("State store not ready: {}", self.state.load());
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        None
    }
}
