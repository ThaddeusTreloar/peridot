use std::sync::Arc;

use futures::Future;
use serde::{de::DeserializeOwned, Serialize};

use super::{
    ReadableStateView, ReadableVersionedStateView, StateBackend, VersionedRecord,
    VersionedStateBackend, WriteableStateView, WriteableVersionedStateView,
};

//pub mod global_facade;

pub struct StateFacade<K, V, B> {
    backend: Arc<B>,
    store_name: Arc<String>,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<K, V, B> StateFacade<K, V, B> {
    pub fn new(backend: Arc<B>, store_name: String) -> Self {
        Self {
            backend,
            store_name: Arc::new(store_name),
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
    B: StateBackend,
    K: Serialize + Send,
    V: DeserializeOwned + Send,
    B::Error: Send,
{
    type Error = B::Error;
    type KeyType = K;
    type ValueType = V;

    fn get(
        self: Arc<Self>,
        key: Self::KeyType,
    ) -> impl Future<Output = Result<Option<Self::ValueType>, Self::Error>> + Send
    {
        let backend_ref = self.backend.clone();
        let store_ref = self.store_name.clone();

        backend_ref.get(key, store_ref.clone())
    }
}

impl<K, V, B> WriteableStateView for StateFacade<K, V, B>
where
    B: StateBackend,
    K: Serialize + Send,
    V: Serialize + Send,
{
    type Error = B::Error;
    type KeyType = K;
    type ValueType = V;

    fn put(
        self: Arc<Self>,
        key: Self::KeyType,
        value: Self::ValueType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let backend_ref = self.backend.clone();
        let store_ref = self.store_name.clone();

        backend_ref.put(key, value, store_ref)
    }

    fn put_range(
        self: Arc<Self>,
        range: Vec<(Self::KeyType, Self::ValueType)>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let backend_ref = self.backend.clone();
        let store_ref = self.store_name.clone();

        backend_ref.put_range(range, store_ref)
    }

    fn delete(
        self: Arc<Self>,
        key: Self::KeyType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let backend_ref = self.backend.clone();
        let store_ref = self.store_name.clone();

        backend_ref.delete(key, store_ref)
    }
}