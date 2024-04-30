use std::sync::Arc;

use crate::{engine::AppEngine, state::backend::{GetView, StateBackend, StateBackendContext, VersionedStateBackend}};

use super::StateFacade;

pub struct FacadeDistributor<K, V, B> 
{
    app_engine: Arc<AppEngine<B>>,
    store_name: String,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<K, V, B> FacadeDistributor<K, V, B> 
where
    B: StateBackendContext + Send + Sync + 'static,
{
    pub fn new(backend: Arc<AppEngine<B>>, store_name: String) -> Self {
        Self {
            app_engine: backend,
            store_name,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub fn fetch_backend(&self, partition: i32) -> Arc<B> {
        self.app_engine.get_state_store_for_table(self.store_name(), partition)
            .expect("Table not found while fetching store.")
    }

    pub fn store_name(&self) -> &str {
        &self.store_name
    }
}

impl<K, V, B> GetView for FacadeDistributor<K, V, B> 
where
    B: StateBackendContext + StateBackend + Send + Sync + 'static,
{
    type Error = B::Error;
    type KeyType = K;
    type ValueType = V;
    type Backend = B;

    fn get_view (
            &self,
            partition: i32,
        ) -> StateFacade<Self::KeyType, Self::ValueType, Self::Backend> {
        let backend = self.fetch_backend(partition);

        StateFacade::new(backend, self.store_name().to_owned())
    }
}
