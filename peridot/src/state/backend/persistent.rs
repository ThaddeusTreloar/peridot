
use std::path::Path;

use surrealdb::{Surreal, engine::local::{File, Db}};

use super::{ReadableStateBackend, WriteableStateBackend, error::PersistantStateBackendCreationError};

pub struct PersistantStateBackend<T> {
    store: Surreal<Db>,
    _type: std::marker::PhantomData<T>
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