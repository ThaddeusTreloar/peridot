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
