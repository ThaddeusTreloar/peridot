/*
 * Copyright 2024 Thaddeus Treloar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

use std::{path::Path, sync::Arc};

use futures::Future;

use super::{error::BackendCreationError, StateBackend, StateBackendContext};

pub struct PersistentStateBackend {
    store: Surreal<Db>,
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

impl PersistentStateBackend {
    pub async fn try_from_file(path: &Path) -> Result<Self, BackendCreationError> {
        let store = Surreal::new::<File>(path).await?;

        store.use_db("peridot").await?;
        store.use_ns("internal").await?;

        Ok(Self { store })
    }
}

impl StateBackendContext for PersistentStateBackend {
    async fn with_topic_name_and_partition(topic_name: &str, partition: i32) -> Self {
        let state_db_filename = format!("peridot.{}.{}.db", topic_name, partition);

        let state_dir = Path::new("/tmp").join(state_db_filename);

        Self::try_from_file(state_dir.as_path())
            .await
            .expect("Failed to create state backend")
    }

    fn get_state_store_time(&self) -> crate::message::types::PeridotTimestamp {
        unimplemented!("Get state store time")
    }
}

impl StateBackend for PersistentStateBackend {
    type Error = PersistentStateBackendError;

    fn get<K, V>(
        self: Arc<Self>,
        _key: K,
        _store: Arc<String>,
    ) -> impl Future<Output = Result<Option<V>, Self::Error>> + Send {
        async { unimplemented!("Get") }
    }

    fn put<K, V>(
        self: Arc<Self>,
        _key: K,
        _value: V,
        _store: Arc<String>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { unimplemented!("Put") }
    }

    fn put_range<K, V>(
        self: Arc<Self>,
        _range: Vec<(K, V)>,
        _store: Arc<String>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { unimplemented!("PutRange") }
    }

    fn delete<K>(
        self: Arc<Self>,
        _key: K,
        _store: Arc<String>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { unimplemented!("Delete") }
    }
}
