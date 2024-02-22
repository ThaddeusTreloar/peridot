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
    error::BackendCreationError, CommitLog, ReadableStateBackend, StateBackend,
    WriteableStateBackend,
};

pub struct PersistentStateBackend<K, V> {
    store: Surreal<Db>,
    commit_log: Arc<CommitLog>,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct OffsetStruct {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

impl OffsetStruct {
    fn _new(topic: String, partition: i32, offset: i64) -> Self {
        OffsetStruct {
            topic,
            partition,
            offset,
        }
    }
}

impl From<Vec<OffsetStruct>> for CommitLog {
    fn from(val: Vec<OffsetStruct>) -> Self {
        let commit_log = CommitLog::default();

        for offset_struct in val {
            commit_log.commit_offset(
                offset_struct.topic.as_str(),
                offset_struct.partition,
                offset_struct.offset,
            );
        }

        commit_log
    }
}

impl<K, V> PersistentStateBackend<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    pub async fn try_from_file(path: &Path) -> Result<Self, BackendCreationError> {
        let store = Surreal::new::<File>(path).await?;

        // TODO: Derive some db name from some app_id
        store.use_ns("stream_app_state_backend").await?;
        store.use_db("stream_app_state_backend").await?;

        let stored_offsets = store
            .select::<Vec<OffsetStruct>>("offsets")
            .await
            .expect("Failed to get value from db");

        let commit_log: Arc<CommitLog> = Arc::new(stored_offsets.into());

        info!("Commit log: {:?}", commit_log);

        let backend = Self {
            store,
            commit_log,
            _key_type: std::marker::PhantomData,
            _value_type: std::marker::PhantomData,
        };

        Ok(backend)
    }

    pub async fn try_from_file_and_commit_log(
        path: &Path,
        commit_log: Arc<CommitLog>,
    ) -> Result<Self, BackendCreationError> {
        let store = Surreal::new::<File>(path).await?;

        // TODO: Derive some db name from some app_id
        store.use_ns("stream_app_state_backend").await?;
        store.use_db("stream_app_state_backend").await?;

        let stored_offsets = store
            .select::<Vec<OffsetStruct>>("offsets")
            .await
            .expect("Failed to get value from db");

        for stored_offset in stored_offsets {
            commit_log.commit_offset(
                stored_offset.topic.as_str(),
                stored_offset.partition,
                stored_offset.offset,
            );
        }

        info!("Commit log: {:?}", commit_log);

        let backend = Self {
            store,
            commit_log,
            _key_type: std::marker::PhantomData,
            _value_type: std::marker::PhantomData,
        };

        Ok(backend)
    }
}

impl<K, V> StateBackend for PersistentStateBackend<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    async fn with_topic_name(topic_name: &str) -> Self {
        let state_db_filename = format!("peridot.{}.db", topic_name);

        let state_dir = Path::new("/tmp").join(state_db_filename);

        Self::try_from_file(state_dir.as_path())
            .await
            .expect("Failed to create state backend")
    }

    async fn with_topic_name_and_commit_log(
        topic_name: &str,
        commit_log: std::sync::Arc<CommitLog>,
    ) -> Self {
        let state_db_filename = format!("peridot.{}.db", topic_name);

        let state_dir = Path::new("/tmp").join(state_db_filename);

        Self::try_from_file_and_commit_log(state_dir.as_path(), commit_log)
            .await
            .expect("Failed to create state backend")
    }

    fn get_commit_log(&self) -> std::sync::Arc<CommitLog> {
        self.commit_log.clone()
    }

    async fn commit_offset(&self, topic: &str, partition: i32, offset: i64) {
        let key = format!("{}-{}", topic, partition);

        let content = OffsetStruct {
            topic: topic.to_string(),
            partition,
            offset,
        };

        if self
            .store
            .select::<Option<OffsetStruct>>(("offsets", key.as_str()))
            .await
            .expect("Failed to get value from db")
            .is_some()
        {
            self.store
                .update::<Option<OffsetStruct>>(("offsets", key))
                .content::<OffsetStruct>(content)
                .await
                .expect("Failed to set value in db");
        } else {
            self.store
                .create::<Option<OffsetStruct>>(("offsets", key))
                .content::<OffsetStruct>(content)
                .await
                .expect("Failed to set value in db");
        }

        self.commit_log.commit_offset(topic, partition, offset);
    }

    async fn get_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        let key = format!("{}-{}", topic, partition);

        self.store
            .select::<Option<OffsetStruct>>(("offsets", key.as_str()))
            .await
            .expect("Failed to get value from db")
            .map(|offset_struct| offset_struct.offset)
    }
}

impl<K, V> ReadableStateBackend for PersistentStateBackend<K, V>
where
    K: Send + Sync + Into<Id> + Clone,
    V: Send + Sync + Clone + DeserializeOwned,
{
    type KeyType = K;
    type ValueType = V;

    async fn get(&self, key: &Self::KeyType) -> Option<Self::ValueType> {
        self.store
            .select(("state", key.clone()))
            .await
            .expect("Failed to get value from db")
    }
}

impl<K, V> WriteableStateBackend<K, V> for PersistentStateBackend<K, V>
where
    K: Send + Sync + Into<Id> + Clone,
    V: Send + Sync + Clone + serde::Serialize + DeserializeOwned,
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

    async fn commit_update(&self, message: &Message<K, V>) -> Option<Message<K, V>> {
        None
    }

    async fn delete(&self, key: &K) -> Option<Message<K, V>> {
        None
    }
}
