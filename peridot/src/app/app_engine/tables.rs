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
    producer::PARTITION_UA,
    ClientConfig, TopicPartitionList,
};
use tokio::{select, sync::mpsc::Sender};
use tracing::{debug, error, info, warn};

use crate::{state::{
    backend::{
        persistent::PersistentStateBackend, ReadableStateBackend, StateBackend,
        WriteableStateBackend, CommitLog,
    },
    StateStore,
}, app::ptable::PTable};

use super::{error::{PeridotEngineCreationError, PeridotEngineRuntimeError}, util::{ExactlyOnce, DeliveryGuaranteeType, AtMostOnce, AtLeastOnce}};

use super::super::{
    extensions::{Commit, OwnedRebalance, PeridotConsumerContext},
    PeridotConsumer, PeridotPartitionQueue, AppEngine,
};

#[derive()]
pub struct TableBuilder<K, V, B = PersistentStateBackend<V>, G = ExactlyOnce> 
where G: DeliveryGuaranteeType
{
    engine: Arc<AppEngine<G>>,
    topic: String,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
    _backend_type: std::marker::PhantomData<B>,
}

impl<K, V, B, G> TableBuilder<K, V, B, G>
where
    B: StateBackend + ReadableStateBackend<V> + WriteableStateBackend<V> + Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static + for<'de> serde::Deserialize<'de>,
    G: DeliveryGuaranteeType
{
    pub fn new(topic: &str, engine: Arc<AppEngine<G>>) -> Self 
    {
        TableBuilder {
            engine,
            topic: topic.to_string(),
            _key_type: PhantomData,
            _value_type: PhantomData,
            _backend_type: PhantomData,
        }
    }
}

impl<K, V, B> TableBuilder<K, V, B, AtMostOnce>
where
    B: StateBackend + ReadableStateBackend<V> + WriteableStateBackend<V> + Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static + for<'de> serde::Deserialize<'de>,
{
    pub async fn build<'a>(self) -> Result<PTable<'a, K, V, B>, PeridotEngineRuntimeError> {
        let Self { engine, topic, .. } = self;

        AppEngine::<AtMostOnce>::table(engine, topic).await
    }
}

impl<K, V, B> TableBuilder<K, V, B, AtLeastOnce>
where
    B: StateBackend + ReadableStateBackend<V> + WriteableStateBackend<V> + Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static + for<'de> serde::Deserialize<'de>,
{
    pub async fn build<'a>(self) -> Result<PTable<'a, K, V, B>, PeridotEngineRuntimeError> {
        let Self { engine, topic, .. } = self;

        AppEngine::<AtLeastOnce>::table(engine, topic).await
    }
}

impl<K, V, B> TableBuilder<K, V, B, ExactlyOnce>
where
    B: StateBackend + ReadableStateBackend<V> + WriteableStateBackend<V> + Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static + for<'de> serde::Deserialize<'de>,
{
    pub async fn build<'a>(self) -> Result<PTable<'a, K, V, B>, PeridotEngineRuntimeError> {
        let Self { engine, topic, .. } = self;

        AppEngine::<ExactlyOnce>::table(engine, topic).await
    }
}

pub struct NewTable<K, V, B = PersistentStateBackend<V>> {
    _topic: String,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
    _backend_type: std::marker::PhantomData<B>,
}

impl<K, V, B> From<TableBuilder<K, V, B>> for NewTable<K, V, B> {
    fn from(builder: TableBuilder<K, V, B>) -> Self {
        NewTable {
            _topic: builder.topic,
            _key_type: Default::default(),
            _value_type: Default::default(),
            _backend_type: Default::default(),
        }
    }
}