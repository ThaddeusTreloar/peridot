
use std::path::Path;

use surrealdb::{Surreal, engine::local::{File, Db}};

use super::{ReadableStateBackend, WriteableStateBackend, error::PersistantStateBackendCreationError, StateBackend};

pub struct PersistantStateBackend<T> {
    store: Surreal<Db>,
    _type: std::marker::PhantomData<T>
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct OffsetStruct{
    offset: i64
}

impl From<i64> for OffsetStruct {
    fn from(offset: i64) -> Self {
        OffsetStruct {
            offset
        }
    }
}

impl <T> PersistantStateBackend<T> {
    pub async fn try_from_file(path: &Path) -> Result<Self, PersistantStateBackendCreationError> {
        let store = Surreal::new::<File>(path).await?;
        
        // TODO: Derive some db name from some app_id
        store.use_ns("stream_app_state_backend").await?;
        store.use_db("stream_app_state_backend").await?;

        let backend = PersistantStateBackend {
            store,
            _type: std::marker::PhantomData
        };

        Ok(backend)
    }
}

impl <T> StateBackend for PersistantStateBackend<T> 
where T: Send + Sync + 'static
{
    async fn commit_offset(&self, topic: &str, partition: i32, offset: i64) {
        let key = format!("{}-{}", topic, partition);

        if self.store
            .select::<Option<OffsetStruct>>(("offsets", key.as_str())).await
            .expect("Failed to get value from db") 
            .is_some()
        {

            self.store
                .update::<Option<OffsetStruct>>(("offsets", key))
                .content::<OffsetStruct>(offset.into())
                .await
                .expect("Failed to set value in db");
        } else {
            self.store
                .create::<Option<OffsetStruct>>(("offsets", key))
                .content::<OffsetStruct>(offset.into())
                .await
                .expect("Failed to set value in db");
        }
    }

    async fn get_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        let key = format!("{}-{}", topic, partition);

        self.store
            .select::<Option<OffsetStruct>>(("offsets", key.as_str())).await
            .expect("Failed to get value from db")
            .map(|offset_struct| offset_struct.offset)
    }
}

impl <T> ReadableStateBackend<T> for PersistantStateBackend<T> 
where T: Clone + Send + Sync + 'static + for<'de> serde::Deserialize<'de>
{
    async fn get(&self, key: &str) -> Option<T> {
        self.store
            .select(("state", key)).await
            .expect("Failed to get value from db")
    }
}

impl <T> WriteableStateBackend<T> for PersistantStateBackend<T> 
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