use std::sync::Arc;

use futures::Future;

use super::{
    ReadableStateView, ReadableVersionedStateView, StateBackend, VersionedRecord,
    VersionedStateBackend, WriteableStateView, WriteableVersionedStateView,
};

//pub mod global_facade;

pub struct StateFacade<K, V, B> {
    backend: Arc<B>,
    store_name: String,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<K, V, B> StateFacade<K, V, B> {
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
    B: StateBackend,
    K: Send,
    V: Send,
    B::Error: Send,
{
    type Error = B::Error;
    type KeyType = K;
    type ValueType = V;

    fn get<'a>(
        &'a self,
        key: &'a Self::KeyType,
    ) -> impl Future<Output = Result<Option<Self::ValueType>, Self::Error>> + Send + 'a {
        self.backend.get(key, &self.store_name)
    }
}

impl<K, V, B> WriteableStateView for StateFacade<K, V, B>
where
    B: StateBackend,
{
    type Error = B::Error;
    type KeyType = K;
    type ValueType = V;

    fn put<'a>(
        &'a self,
        key: &'a Self::KeyType,
        value: &'a Self::ValueType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.backend.put(key, value, self.store_name())
    }

    fn put_range<'a>(
        &'a self,
        range: Vec<(&'a Self::KeyType, &'a Self::ValueType)>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.backend.put_range(range, self.store_name())
    }

    fn delete<'a>(
        &'a self,
        key: &'a Self::KeyType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.backend.delete(key, self.store_name())
    }
}

impl<K, V, B> ReadableVersionedStateView for StateFacade<K, V, B>
where
    B: VersionedStateBackend,
{
    type Error = B::Error;
    type KeyType = K;
    type ValueType = V;

    fn get_version<'a>(
        &'a self,
        key: &'a Self::KeyType,
        at_timestamp: i64,
    ) -> impl Future<Output = Result<Option<VersionedRecord<Self::ValueType>>, Self::Error>> + Send + 'a
    {
        self.backend
            .get_version(key, self.store_name(), at_timestamp)
    }
}

impl<K, V, B> WriteableVersionedStateView for StateFacade<K, V, B>
where
    B: VersionedStateBackend,
{
    type Error = B::Error;
    type KeyType = K;
    type ValueType = V;

    fn put_version<'a>(
        &'a self,
        key: &'a Self::KeyType,
        value: &'a Self::ValueType,
        timestamp: i64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.backend
            .put_version(key, value, self.store_name(), timestamp)
    }

    fn put_version_range<'a>(
        &'a self,
        range: Vec<(&'a Self::KeyType, &'a Self::ValueType, i64)>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.backend.put_range_version(range, self.store_name())
    }

    fn delete_version<'a>(
        &'a self,
        key: &'a Self::KeyType,
        timestamp: i64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.backend
            .delete_version(key, self.store_name(), timestamp)
    }
}
