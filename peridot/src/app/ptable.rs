use std::sync::Arc;

use crate::state::{
    backend::{
        persistent::PersistentStateBackend, ReadableStateBackend, StateBackend,
        WriteableStateBackend,
    },
    StateStore,
};

use super::error::PeridotAppRuntimeError;

pub trait PeridotTable<K, V, B>
where
    B: StateBackend + ReadableStateBackend<V> + WriteableStateBackend<V> + Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static + for<'de> serde::Deserialize<'de>,
{
    fn get_store(&self) -> Result<Arc<StateStore<'_, B, V>>, PeridotAppRuntimeError>;
}

pub struct PTable<'a, K, V, B = PersistentStateBackend<V>>
where
    V: Send + Sync + 'static + for<'de> serde::Deserialize<'de>,
{
    store: Arc<StateStore<'a, B, V>>,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<'a, K, V, B> PTable<'a, K, V, B>
where
    B: StateBackend + ReadableStateBackend<V> + WriteableStateBackend<V> + Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static + for<'de> serde::Deserialize<'de>,
{
    pub fn new(store: Arc<StateStore<'a, B, V>>) -> Self {
        PTable {
            store,
            _key_type: std::marker::PhantomData,
            _value_type: std::marker::PhantomData,
        }
    }
}

impl<'a, K, V, B> PeridotTable<K, V, B> for PTable<'a, K, V, B>
where
    B: StateBackend + ReadableStateBackend<V> + WriteableStateBackend<V> + Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static + for<'de> serde::Deserialize<'de>,
{
    fn get_store(&self) -> Result<Arc<StateStore<'_, B, V>>, PeridotAppRuntimeError> {
        Ok(self.store.clone())
    }
}
