use std::sync::{Arc, Mutex};

use rdkafka::producer::FutureProducer;

use crate::{app::{config::PeridotConfig, PeridotConsumer}, engine::{context::EngineContext, AppEngine}, state::backend::StateBackendContext};

use super::{changelog_queues::ChangelogQueues, partition_queue::StreamPeridotPartitionQueue};

#[derive(Clone)]
pub struct QueueMetadata {
    pub(super) engine_context: EngineContext,
    pub(super) producer_ref: Arc<FutureProducer>,
    pub(super) changelog_queues: ChangelogQueues,
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

    pub fn producer(&self) -> Arc<FutureProducer> {
        self.producer_ref.clone()
    }
}