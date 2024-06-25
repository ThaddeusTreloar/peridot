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
use rdkafka::producer::FutureProducer;

use crate::{
    app::{config::PeridotConfig, PeridotConsumer},
    engine::{context::EngineContext, AppEngine},
    message::{state_fork::StoreStateCell, StreamState},
};

use super::{
    changelog_queues::ChangelogQueues, partition_queue::StreamPeridotPartitionQueue,
    state_cells::StateCells,
};

#[derive(Clone)]
pub struct QueueMetadata {
    pub(super) engine_context: Arc<EngineContext>,
    pub(super) producer_ref: Arc<FutureProducer>,
    pub(super) changelog_queues: ChangelogQueues,
    pub(super) state_cells: StateCells,
    pub(super) partition: i32,
    pub(super) source_topic: String,
}

impl QueueMetadata {
    pub fn partition(&self) -> i32 {
        self.partition
    }

    pub fn source_topic(&self) -> &str {
        self.source_topic.as_str()
    }

    pub fn engine_context(&self) -> &EngineContext {
        &self.engine_context
    }

    pub fn engine_context_arc(&self) -> Arc<EngineContext> {
        self.engine_context.clone()
    }

    pub fn producer(&self) -> &FutureProducer {
        &self.producer_ref
    }

    pub fn producer_arc(&self) -> Arc<FutureProducer> {
        self.producer_ref.clone()
    }

    pub fn take_changelog_queue(&self, store_name: &str) -> Option<StreamPeridotPartitionQueue> {
        self.changelog_queues.take(store_name)
    }

    pub fn clone_stream_state(&self, store_name: &str) -> Option<Arc<StoreStateCell>> {
        self.state_cells.take(store_name)
    }
}
