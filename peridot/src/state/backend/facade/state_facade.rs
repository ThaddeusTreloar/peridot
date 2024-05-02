use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};

use crate::state::backend::{
    Checkpoint, ReadableStateView, StateBackend, WriteableStateView
};

pub struct StateFacade<K, V, B> 
{
    backend: Arc<B>,
    store_name: String,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<K, V, B> StateFacade<K, V, B> 
{
    pub fn new(backend: Arc<B>, store_name: String) -> Self {
        Self {
            backend,
            store_name,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub fn backend(&self) -> Arc<B> {
        self.backend.clone()
    }

    pub fn store_name(&self) -> &str {
        &self.store_name
    }
}

impl<K, V, B> ReadableStateView for StateFacade<K, V, B>
where
    B: StateBackend + Send + Sync,
    K: Serialize + Send + Sync,
    V: DeserializeOwned + Send + Sync,
{
    type Error = B::Error;
    type KeyType = K;
    type ValueType = V;

    async fn get(
        self: Arc<Self>,
        key: Self::KeyType,
    ) -> Result<Option<Self::ValueType>, Self::Error>
    {
        self.backend.get(key, self.store_name()).await
    }

    fn get_checkpoint(&self) -> Result<Option<Checkpoint> ,Self::Error> {
        let checkpoint = self.backend.get_state_store_checkpoint(self.store_name());

        Ok(checkpoint)
    }    
}

impl<K, V, B> WriteableStateView for StateFacade<K, V, B>
where
    B: StateBackend + Send + Sync,
    K: Serialize + Send + Sync,
    V: Serialize + Send + Sync,
    Self: Send
{
    type Error = B::Error;
    type KeyType = K;
    type ValueType = V;

    fn create_checkpoint(&self, offset:i64,) -> Result<() ,Self::Error> {
        self.backend.create_checkpoint(self.store_name(), offset);

        Ok(())
    }

    async fn put(
        self: Arc<Self>,
        key: Self::KeyType,
        value: Self::ValueType,
    ) -> Result<(), Self::Error> {
        self.backend.put(key, value, self.store_name()).await
    }

    async fn put_range(
        self: Arc<Self>,
        range: Vec<(Self::KeyType, Self::ValueType)>,
    ) -> Result<(), Self::Error> {
        self.backend.put_range(range, self.store_name()).await
    }

    async fn delete(
        self: Arc<Self>,
        key: Self::KeyType,
    ) -> Result<(), Self::Error> {
        self.backend.delete(key, self.store_name()).await
    }
}