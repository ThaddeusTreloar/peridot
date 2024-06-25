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

use std::sync::{Arc, Mutex};

use dashmap::DashMap;

use super::partition_queue::StreamPeridotPartitionQueue;

pub struct ChangelogQueues {
    inner: Arc<DashMap<String, StreamPeridotPartitionQueue>>,
}

impl Clone for ChangelogQueues {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl ChangelogQueues {
    pub fn new(queues: Vec<(String, StreamPeridotPartitionQueue)>) -> Self {
        let inner = Arc::new(queues.into_iter().collect());

        Self { inner }
    }

    pub(crate) fn take(&self, store: &str) -> Option<StreamPeridotPartitionQueue> {
        self.inner.remove(store).map(|(k, v)| v)
    }
}
