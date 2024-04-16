use std::sync::Arc;

use futures::Future;

use crate::{
    engine::{
        context::EngineContext,
        wrapper::serde::{PDeserialize, PSerialize},
    },
    message::types::Message,
    util::hash::get_partition_for_key,
};

pub mod error;
pub mod in_memory;
pub mod persistent;

pub trait StateBackend {
    fn with_topic_name_and_partition(
        topic_name: &str,
        partition: i32,
    ) -> impl Future<Output = Self>;
}

pub trait VersioningMethod {}
pub struct TimestampVersioned {}
impl VersioningMethod for TimestampVersioned {}
pub struct NonVersioned {}
impl VersioningMethod for NonVersioned {}

pub trait ReadableStateBackend {
    type Error: std::error::Error;
    type KeyType;
    type ValueType;
    type VersioningMethod: VersioningMethod;

    fn get(
        &self,
        key: &Self::KeyType,
    ) -> impl Future<Output = Result<Option<Self::ValueType>, Self::Error>> + Send;
}

pub trait WriteableStateBackend<K, V> {
    type Error: std::error::Error;
    type VersioningMethod: VersioningMethod;

    fn commit_update(
        self: Arc<Self>,
        message: Message<K, V>,
    ) -> impl Future<Output = Result<Option<Message<K, V>>, Self::Error>> + Send + 'static;

    fn delete(
        self: Arc<Self>,
        key: &K,
    ) -> impl Future<Output = Result<Option<Message<K, V>>, Self::Error>> + Send;
}

// User facing API interface for interacting with a state store
// Should have global access to all partitions for the given table
pub trait BackendView {
    type KeyType;
    type ValueType;

    async fn get(&self, key: &Self::KeyType) -> Option<Self::ValueType>;
    // fn all
    // fn range
    // fn prefix
    // fn count
}

#[derive(Clone)]
pub struct EngineBackendView<KS, VD, EC> {
    engine_context: EC,
    table_name: String,
    _key_type: std::marker::PhantomData<KS>,
    _value_type: std::marker::PhantomData<VD>,
}

impl<KS, VD, EC> EngineBackendView<KS, VD, EC>
where
    EC: EngineContext,
{
    pub fn new(table_name: String, engine_context: EC) -> Self {
        Self {
            engine_context,
            table_name,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

impl<KS, VD, EC> BackendView for EngineBackendView<KS, VD, EC>
where
    KS: PSerialize,
    VD: PDeserialize,
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
            .get_table_partition_count(&self.table_name)
            .expect("Table doesn't exist");

        let partition = get_partition_for_key(&key_bytes, partition_count);

        self.engine_context
            .state_backend(self.table_name.clone(), partition)
            .get(key)
            .await
            .expect("Backend Failure")
    }
}
