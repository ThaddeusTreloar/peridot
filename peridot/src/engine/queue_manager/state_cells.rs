use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crossbeam::atomic::AtomicCell;
use dashmap::DashMap;

use crate::message::{state_fork::StoreStateCell, StreamState};

use super::partition_queue::StreamPeridotPartitionQueue;

pub struct StateCells {
    inner: Arc<HashMap<String, Arc<StoreStateCell>>>,
}

impl Clone for StateCells {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl StateCells {
    pub fn new(queues: Vec<(String, Arc<StoreStateCell>)>) -> Self {
        let inner = Arc::new(queues.into_iter().collect());

        Self { inner }
    }

    pub(crate) fn take(&self, store: &str) -> Option<Arc<StoreStateCell>> {
        self.inner.get(store).map(|arc| arc.clone())
    }
}
