use std::{marker::PhantomData, sync::Arc, time::Duration};

use crossbeam::atomic::AtomicCell;
use futures::{Future, StreamExt};
use rdkafka::{
    consumer::{stream_consumer::StreamPartitionQueue, ConsumerContext},
    Message,
};
use serde::de::DeserializeOwned;
use tokio::sync::broadcast::Receiver;
use tracing::{info, trace};

use crate::{app::{extensions::Commit, PeridotPartitionQueue}, engine::EngineState};

use self::{
    backend::{ReadableStateBackend, StateBackend, WriteableStateBackend},
    error::StateStoreCreationError,
};

pub mod backend;
pub mod error;

pub trait ReadableStateStore<T> {
    fn get(&self, key: &str) -> impl Future<Output = Option<T>>;
}

pub trait WriteableStateStore<T> {
    fn set(&self, key: &str, value: T) -> impl Future<Output = Option<T>>;
    fn delete(&self, key: &str) -> impl Future<Output = Option<T>>;
}

pub struct StateStore<'a, T, U>
where
    U: DeserializeOwned + Send + Sync + 'static,
{
    topic: Arc<String>,
    backend: Arc<T>,
    state: Arc<AtomicCell<EngineState>>,
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
    T: StateBackend + ReadableStateBackend<U> + WriteableStateBackend<U> + Send + Sync + 'static,
    U: DeserializeOwned + Send + Sync + 'static,
{
    pub fn try_new(
        topic: String,
        backend: T,
        state: Arc<AtomicCell<EngineState>>,
        commit_waker: Receiver<Commit>,
        stream_queue: tokio::sync::mpsc::Receiver<PeridotPartitionQueue>,
    ) -> Result<Self, StateStoreCreationError> {
        let state_store = StateStore {
            topic: Arc::new(topic),
            backend: Arc::new(backend),
            state,
            _lifetime: &PhantomData,
            _type: PhantomData,
        };

        state_store.start_update_thread(stream_queue);
        state_store.start_commit_listener(commit_waker);

        Ok(state_store)
    }

    fn start_update_thread(
        &self,
        mut stream_queue: tokio::sync::mpsc::Receiver<PeridotPartitionQueue>,
    ) {
        let store = self.backend.clone();

        tokio::spawn(async move {
            while let Some(queue) = stream_queue.recv().await {
                tokio::spawn(start_partition_update_thread(queue, store.clone()));
            }
        });
    }

    fn start_commit_listener(&self, mut waker: Receiver<Commit>) {
        let store = self.backend.clone();
        let table_topic = self.topic.clone();

        tokio::spawn(async move {
            while let Ok(message) = waker.recv().await {
                let Commit {topic, partition, offset} = message;

                if table_topic.as_str() == topic.as_str() && offset > 0 {
                    info!("Committing offset: {}-{}:{}", topic, partition, offset);
                    store.commit_offset(topic.as_str(), partition, offset).await;
                } else {
                    info!("Table: {}, ignoring commit: {}-{}:{}", table_topic, topic, partition, offset);
                }
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
        while let EngineState::Lagging
        | EngineState::Stopped
        | EngineState::Rebalancing
        | EngineState::NotReady = self.state.load()
        {
            info!("State store not ready: {}", self.state.load());
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        self.backend.get(key).await
    }
}
