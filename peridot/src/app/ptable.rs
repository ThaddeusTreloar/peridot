use std::sync::Arc;

use crate::{state::{
    backend::{
        persistent::PersistentStateBackend, ReadableStateBackend, StateBackend,
        WriteableStateBackend,
    },
    StateStore,
}, pipeline::serde_ext::PDeserialize};

use super::error::PeridotAppRuntimeError;

pub trait PeridotTable<KS, VS, B>
where
    B: StateBackend,
    KS: PDeserialize + Send + Sync,
    VS: PDeserialize + Send + Sync,
    KS::Output: Send + Sync,
    VS::Output: Send + Sync,
{
    fn get_store(&self) -> Result<Arc<StateStore<KS, VS, B>>, PeridotAppRuntimeError>;
}

pub struct PTable<KS, VS, B>
where
    KS: PDeserialize + Send + Sync,
    VS: PDeserialize + Send + Sync,
    KS::Output: Send + Sync,
    VS::Output: Send + Sync,
    B: StateBackend,
{
    store: Arc<StateStore<KS, VS, B>>,
    _key_serialiser_type: std::marker::PhantomData<KS>,
    _value_serialiser_type: std::marker::PhantomData<VS>,
}

impl<KS, VS, B> PTable<KS, VS, B>
where
    B: StateBackend + ReadableStateBackend<KS::Output, VS::Output> + WriteableStateBackend<KS::Output, VS::Output>,
    KS: PDeserialize + Send + Sync,
    VS: PDeserialize + Send + Sync,
    KS::Output: Send + Sync,
    VS::Output: Send + Sync,
{
    pub fn new(store: Arc<StateStore<KS, VS, B>>) -> Self {
        PTable {
            store,
            _key_serialiser_type: std::marker::PhantomData,
            _value_serialiser_type: std::marker::PhantomData,
        }
    }
}

impl<KS, VS, B> PeridotTable<KS, VS, B> for PTable<KS, VS, B>
where
    B: StateBackend + ReadableStateBackend<KS::Output, VS::Output> + WriteableStateBackend<KS::Output, VS::Output>,
    KS: PDeserialize + Send + Sync,
    VS: PDeserialize + Send + Sync,
    KS::Output: Send + Sync,
    VS::Output: Send + Sync,
{
    fn get_store(&self) -> Result<Arc<StateStore<KS, VS, B>>, PeridotAppRuntimeError> {
        Ok(self.store.clone())
    }
}
