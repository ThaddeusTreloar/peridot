use std::sync::Arc;

use crate::{
    engine::{
        context::EngineContext, metadata_manager::table_metadata,
        state_store_manager::StateStoreManager, util::DeliveryGuaranteeType, AppEngine,
    },
    state::backend::{view::GetView, StateBackend, VersionedStateBackend},
};

use super::StateFacade;

pub struct FacadeDistributor<K, V, B> {
    engine_context: Arc<EngineContext>,
    state_store_manager: Arc<StateStoreManager<B>>,
    store_name: String,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<K, V, B> FacadeDistributor<K, V, B>
where
    B: StateBackend + Send + Sync + 'static,
{
    pub fn new<G>(engine: &AppEngine<B, G>, store_name: String) -> Self
    where
        G: DeliveryGuaranteeType + Send + Sync + 'static,
    {
        Self {
            engine_context: engine.engine_context(),
            state_store_manager: engine.state_store_context(),
            store_name,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub fn fetch_backend(&self, partition: i32) -> Arc<B> {
        let table_metadata = self.engine_context.store_metadata(&self.store_name);

        self.state_store_manager
            .get_state_store(table_metadata.source_topic(), partition)
            .expect("Failed to get state store for facade distributor.")
    }

    pub fn store_name(&self) -> &str {
        &self.store_name
    }
}

impl<K, V, B> GetView for FacadeDistributor<K, V, B>
where
    B: StateBackend + Send + Sync + 'static,
{
    type Error = B::Error;
    type KeyType = K;
    type ValueType = V;
    type Backend = B;

    fn get_view(
        &self,
        partition: i32,
    ) -> StateFacade<Self::KeyType, Self::ValueType, Self::Backend> {
        let backend = self.fetch_backend(partition);

        StateFacade::new(
            backend,
            self.state_store_manager.clone(),
            self.store_name().to_owned(),
            partition,
        )
    }
}
