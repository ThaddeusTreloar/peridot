use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    engine::{
        queue_manager::queue_metadata::QueueMetadata,
        state_store_manager::{self, StateStoreManager},
        AppEngine,
    },
    message::sink::state_sink::StateSink,
    state::backend::{facade::StateFacade, StateBackend},
};

use super::MessageSinkFactory;

pub struct StateSinkFactory<B, K, V> {
    state_store_manager: Arc<StateStoreManager<B>>,
    store_name: String,
    source_topic: String,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<B, K, V> StateSinkFactory<B, K, V> {
    pub(crate) fn from_state_store_manager(
        state_store_manager: Arc<StateStoreManager<B>>,
        store_name: &str,
        source_topic: &str,
    ) -> Self {
        Self {
            state_store_manager,
            store_name: store_name.to_owned(),
            source_topic: source_topic.to_owned(),
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

impl<B, K, V> MessageSinkFactory<K, V> for StateSinkFactory<B, K, V>
where
    K: Serialize + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    B: StateBackend + Send + Sync + 'static,
{
    type SinkType = StateSink<B, K, V>;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType {
        let partition = queue_metadata.partition();

        let state_store = self
            .state_store_manager
            .get_state_store(&self.source_topic, partition)
            .expect("No state store for partition.");

        let facade = StateFacade::new(
            state_store,
            self.store_name.clone(),
            queue_metadata.partition(),
        );

        StateSink::<B, K, V>::new(queue_metadata, facade)
    }
}
