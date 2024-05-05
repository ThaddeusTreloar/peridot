use std::sync::Arc;

use futures::Future;

use crate::{engine::context::EngineContext, message::types::Message};

use super::{
    ReadableStateView, ReadableVersionedStateView, StateBackend, VersionedRecord,
    VersionedStateBackend, WriteableStateView, WriteableVersionedStateView,
};

// This will become the user facing facade for interacting with the state backend
// outside of a streams application, mostly when using interactive queries.
#[derive(Clone)]
pub struct UnpartitionedStateFacade<K, V, EC> {
    engine_context: EC,
    store_name: String,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<K, V, EC> UnpartitionedStateFacade<K, V, EC>
where
    EC: EngineContext,
{
    pub fn new(store_name: String, engine_context: EC) -> Self {
        Self {
            engine_context,
            store_name: store_name,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

/*
impl<KS, VD, EC> GlobalView for EngineBackendView<KS, VD, EC>
where
    KS: PSerialize,
    EC: EngineContext,
    EC::Backend: ReadableStateBackend<KeyType = KS::Input, ValueType = VD::Output>,
    //B: ReadableStateBackend<KeyType = KS::Input, ValueType = VD::Output> + Send + Sync + 'static,
{
    type KeyType = KS::Input;
    type ValueType = VD::Output;

    async fn get(&self, key: &Self::KeyType) -> Option<Self::ValueType> {
        let key_bytes = KS::serialize(key).expect("Failed to serialize key");

        let partition_count = self
            .engine_context
            .get_table_partition_count(&self.store_name)
            .expect("Table doesn't exist");

        let partition = get_partition_for_key(&key_bytes, partition_count);

        self.engine_context
            .state_backend(self.store_name.clone(), partition)
            .get(key)
            .await
            .expect("Backend Failure")
    }
} */

impl<K, V, B> ReadableStateView for UnpartitionedStateFacade<K, V, B>
where
    B: StateBackend,
{
    type Error = B::Error;
    type KeyType = K;
    type ValueType = V;

    fn get(
        &self,
        key: &Self::KeyType,
    ) -> impl Future<Output = Result<Option<Self::ValueType>, Self::Error>> + Send {
        self.backend.get(key, &self.store_name)
    }
}

impl<K, V, B> WriteableStateView for UnpartitionedStateFacade<K, V, B>
where
    B: StateBackend,
{
    type Error = B::Error;
    type KeyType = K;
    type ValueType = V;

    fn put(
        self: Arc<Self>,
        message: &Message<Self::KeyType, Self::ValueType>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.backend
            .put(message.key(), message.value(), self.store_name())
    }

    fn put_range(
        self: Arc<Self>,
        range: &[(&Self::KeyType, &Self::ValueType)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.backend.put_range(range, self.store_name())
    }

    fn delete(
        self: Arc<Self>,
        message: &Self::KeyType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.backend.delete(message.key(), self.store_name())
    }
}

impl<K, V, B> ReadableVersionedStateView for UnpartitionedStateFacade<K, V, B>
where
    B: VersionedStateBackend,
{
    type Error = B::Error;
    type KeyType = K;
    type ValueType = V;

    fn get_version(
        &self,
        key: &Self::KeyType,
        at_timestamp: i64,
    ) -> impl Future<Output = Result<Option<VersionedRecord<Self::ValueType>>, Self::Error>> + Send
    {
        self.backend
            .get_version(key, self.store_name(), at_timestamp)
    }
}

impl<K, V, B> WriteableVersionedStateView for UnpartitionedStateFacade<K, V, B>
where
    B: VersionedStateBackend,
{
    type Error = B::Error;
    type KeyType = K;
    type ValueType = V;

    fn put_version(
        self: Arc<Self>,
        message: Message<Self::KeyType, Self::ValueType>,
        timestamp: i64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.backend
            .put_version(message.key(), message.value(), self.store_name(), timestamp)
    }

    fn put_version_range(
        self: Arc<Self>,
        range: &[(&Self::KeyType, &Self::ValueType, i64)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.backend.put_version_range(range, self.store_name())
    }

    fn delete_version(
        self: Arc<Self>,
        key: &Self::KeyType,
        timestamp: i64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.backend
            .delete_version(key, self.store_name(), timestamp)
    }
}
