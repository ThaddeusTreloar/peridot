use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    engine::{queue_manager::queue_metadata::QueueMetadata, AppEngine},
    message::sink::state_sink::StateSink,
    state::backend::{facade::StateFacade, StateBackend, StateBackendContext},
};

use super::MessageSinkFactory;

pub struct StateSinkFactory<B, K, V> {
    engine_ref: Arc<AppEngine<B>>,
    store_name: String,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<B, K, V> StateSinkFactory<B, K, V> {
    pub fn from_backend_ref(engine_ref: Arc<AppEngine<B>>, store_name: String) -> Self {
        Self {
            engine_ref,
            store_name,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

impl<B, K, V> MessageSinkFactory<K, V> for StateSinkFactory<B, K, V>
where
    K: Serialize + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    B: StateBackendContext + StateBackend + Send + Sync + 'static,
{
    type SinkType = StateSink<B, K, V>;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType {
        let partition = queue_metadata.partition();

        todo!("Get state store for table")
        /*let state_store = self
            .engine_ref
            .get_state_store_for_table(&self.store_name, partition)
            .expect("Failed to get state store");
        let facade = StateFacade::new(state_store, self.store_name.clone());

        StateSink::<B, K, V>::new(queue_metadata, facade)*/
    }
}
