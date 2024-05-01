use std::sync::{Arc, Mutex};

use dashmap::DashMap;

use super::partition_queue::StreamPeridotPartitionQueue;

pub struct ChangelogQueues {
    inner: Arc<DashMap<String, StreamPeridotPartitionQueue>>,
}

impl Clone for ChangelogQueues {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }   
    }
}

impl ChangelogQueues {
    pub fn new(queues: Vec<(String, StreamPeridotPartitionQueue)>) -> Self {
        let inner = Arc::new(queues.into_iter().collect());

        Self {
            inner
        }
    }

    fn get_queue(&self, store: &str) -> Option<StreamPeridotPartitionQueue> {
        self.inner.remove(store).map(|(k, v)|v)
    }
}
