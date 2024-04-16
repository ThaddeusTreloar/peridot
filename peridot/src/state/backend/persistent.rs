use std::{path::Path, sync::Arc};

use serde::de::DeserializeOwned;
use surrealdb::{
    engine::local::{Db, File},
    sql::Id,
    Surreal,
};
use tracing::info;

use crate::message::types::Message;

use super::{
    error::BackendCreationError, NonVersioned, ReadableStateBackend, StateBackend,
    WriteableStateBackend,
};

pub struct PersistentStateBackend<K, V, VM = NonVersioned> {
    store: Surreal<Db>,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
    _versioning_method: std::marker::PhantomData<VM>,
}

#[derive(Debug, thiserror::Error)]
pub enum PersistentStateBackendError {
    #[error("Error creating backend: {0}")]
    BackendCreationError(#[from] BackendCreationError),
    #[error("Error deserialising commit log: {0}")]
    DeserialisationError(#[from] serde_json::Error),
    #[error(transparent)]
    GenericSurrealError(#[from] surrealdb::Error),
}

impl<K, V, VM> PersistentStateBackend<K, V, VM>
where
    K: Send + Sync,
    V: Send + Sync,
{
    pub async fn try_from_file(path: &Path) -> Result<Self, BackendCreationError> {
        let store = Surreal::new::<File>(path).await?;

        // TODO: Derive some db name from some app_id
        store.use_ns("stream_app_state_backend").await?;
        store.use_db("stream_app_state_backend").await?;

        let backend = Self {
            store,
            _key_type: Default::default(),
            _value_type: Default::default(),
            _versioning_method: Default::default(),
        };

        Ok(backend)
    }
}

impl<K, V, VM> StateBackend for PersistentStateBackend<K, V, VM>
where
    K: Send + Sync,
    V: Send + Sync,
{
    async fn with_topic_name_and_partition(topic_name: &str, partition: i32) -> Self {
        let state_db_filename = format!("peridot.{}.{}.db", topic_name, partition);

        let state_dir = Path::new("/tmp").join(state_db_filename);

        Self::try_from_file(state_dir.as_path())
            .await
            .expect("Failed to create state backend")
    }
}

impl<K, V> ReadableStateBackend for PersistentStateBackend<K, V, NonVersioned>
where
    K: Send + Sync + Into<Id> + Clone,
    V: Send + Sync + Clone + DeserializeOwned,
{
    type Error = PersistentStateBackendError;
    type KeyType = K;
    type ValueType = V;
    type VersioningMethod = NonVersioned;

    async fn get(&self, key: &Self::KeyType) -> Result<Option<Self::ValueType>, Self::Error> {
        Ok(self.store.select(("state", key.clone())).await?)
    }
}

impl<K, V> WriteableStateBackend<K, V> for PersistentStateBackend<K, V, NonVersioned>
where
    K: Send + Sync + Into<Id> + Clone + 'static,
    V: Send + Sync + Clone + serde::Serialize + DeserializeOwned + 'static,
{
    /*async fn set(&self, key: &K, value: V) -> Option<V> {
        if let Some(old_value) = self.store
            .select(("state", key.clone())).await
            .expect("Failed to get value from db") {

            self.store
                .update::<Option<V>>(("state", key.clone()))
                .content::<V>(value)
                .await
                .expect("Failed to set value in db");

            Some(old_value)
        } else {
            self.store
                .create::<Option<V>>(("state", key.clone()))
                .content::<V>(value)
                .await
                .expect("Failed to set value in db");

            None
        }
    }

    async fn delete(&self, _key: &K) -> Option<V> {
        unimplemented!("Delete not implemented")
    }*/
    type Error = PersistentStateBackendError;
    type VersioningMethod = NonVersioned;

    async fn commit_update(
        self: Arc<Self>,
        _message: Message<K, V>,
    ) -> Result<Option<Message<K, V>>, Self::Error> {
        Ok(None)
    }

    async fn delete(self: Arc<Self>, _key: &K) -> Result<Option<Message<K, V>>, Self::Error> {
        Ok(None)
    }
}
