
use std::{path::Path, sync::Arc};

use dashmap::DashMap;
use surrealdb::{Surreal, engine::local::{File, Db}};
use tokio::fs::try_exists;

use super::{ReadableStateBackend, WriteableStateBackend, error::BackendCreationError, StateBackend, CommitLog};

pub struct PersistentStateBackend<T> {
    store: Surreal<Db>,
    commit_log: Arc<CommitLog>,
    _type: std::marker::PhantomData<T>
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct OffsetStruct{
    pub topic: String,
    pub partition: i32,
    pub offset: i64
}

impl OffsetStruct {
    fn new(topic: String, partition: i32, offset: i64) -> Self {
        OffsetStruct {
            topic,
            partition,
            offset
        }
    }
}

impl Into<CommitLog> for Vec<OffsetStruct> {
    fn into(self) -> CommitLog {
        let mut commit_log = CommitLog::default();

        for offset_struct in self {
            commit_log.commit_offset(offset_struct.topic.as_str(), offset_struct.partition, offset_struct.offset);
        }

        commit_log
    }
}

impl <T> PersistentStateBackend<T> 
where T: Send + Sync + 'static
{
    pub async fn try_from_file(path: &Path) -> Result<Self, BackendCreationError> {
        let store = Surreal::new::<File>(path).await?;
        
        // TODO: Derive some db name from some app_id
        store.use_ns("stream_app_state_backend").await?;
        store.use_db("stream_app_state_backend").await?;

        let stored_offsets = store.select::
            <Vec<OffsetStruct>>("offsets")
            .await
            .expect("Failed to get value from db");

        let commit_log: Arc<CommitLog> = Arc::new(stored_offsets.into());

        let backend: PersistentStateBackend<T> = PersistentStateBackend {
            store,
            commit_log,
            _type: Default::default()
        };

        Ok(backend)
    }
}

impl <T> StateBackend for PersistentStateBackend<T> 
where T: Send + Sync + 'static
{
    async fn with_topic_name(topic_name: &str) -> Self {
        let state_db_filename = format!("peridot.{}.db", topic_name);

        let state_dir = Path::new("/tmp").join(state_db_filename);
        
        Self::try_from_file(state_dir.as_path()).await.expect("Failed to create state backend")
    }

    fn get_commit_log(&self) -> std::sync::Arc<CommitLog> {
        self.commit_log.clone()
    }

    async fn commit_offset(&self, topic: &str, partition: i32, offset: i64) {
        let key = format!("{}-{}", topic, partition);

        let content = OffsetStruct {
            topic: topic.to_string(),
            partition,
            offset
        };

        if self.store
            .select::<Option<OffsetStruct>>(("offsets", key.as_str())).await
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
            .select::<Option<OffsetStruct>>(("offsets", key.as_str())).await
            .expect("Failed to get value from db")
            .map(|offset_struct| offset_struct.offset)
    }
}

impl <T> ReadableStateBackend<T> for PersistentStateBackend<T> 
where T: Clone + Send + Sync + 'static + for<'de> serde::Deserialize<'de>
{
    async fn get(&self, key: &str) -> Option<T> {
        self.store
            .select(("state", key)).await
            .expect("Failed to get value from db")
    }
}

impl <T> WriteableStateBackend<T> for PersistentStateBackend<T> 
where T: Send + Sync + 'static + for<'de> serde::Deserialize<'de> + serde::Serialize
{
    async fn set(&self, key: &str, value: T) -> Option<T> {
        if let Some(old_value) = self.store
            .select(("state", key)).await
            .expect("Failed to get value from db") {

            self.store
                .update::<Option<T>>(("state", key))
                .content::<T>(value)
                .await
                .expect("Failed to set value in db");

            Some(old_value)
        } else {
            self.store
                .create::<Option<T>>(("state", key))
                .content::<T>(value)
                .await
                .expect("Failed to set value in db");

            None
        }
    }
    
    async fn delete(&self, _key: &str) -> Option<T> {
        unimplemented!("Delete not implemented")
    }
}