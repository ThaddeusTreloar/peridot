use dashmap::DashMap;
use futures::Future;

use super::{StateBackend, StateBackendContext};

pub struct InMemoryStateBackend {
    store: DashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug, thiserror::Error)]
pub enum InMemoryStateBackendError {}

impl Default for InMemoryStateBackend {
    fn default() -> Self {
        InMemoryStateBackend {
            store: Default::default(),
        }
    }
}

impl StateBackendContext for InMemoryStateBackend {
    async fn with_topic_name_and_partition(_topic_name: &str, _partition: i32) -> Self {
        Self::default()
    }

    fn get_state_store_time(&self) -> crate::message::types::PeridotTimestamp {
        unimplemented!("Get state store time")
    }
}

impl StateBackend for InMemoryStateBackend {
    type Error = InMemoryStateBackendError;

    fn get<K, V>(
        &self,
        _key: &K,
        _store: &str,
    ) -> impl Future<Output = Result<Option<V>, Self::Error>> + Send {
        //Ok(self.store.get(key).map(|v| v.value()))
        async { unimplemented!("Get") }
    }

    fn put<K, V>(
        &self,
        _key: &K,
        _value: &V,
        _store: &str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        //self.store.insert(key.clone(), value.clone());
        async { unimplemented!("Put") }
    }

    fn put_range<K, V>(
        &self,
        _range: Vec<(&K, &V)>,
        _store: &str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        //for (key, value) in range {
        //    self.store.insert(key.clone(), value.clone());
        //}

        async { unimplemented!("Put range") }
    }

    fn delete<K>(
        &self,
        _key: &K,
        _store: &str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        //self.store.remove(key);

        async { unimplemented!("Delete") }
    }
}
