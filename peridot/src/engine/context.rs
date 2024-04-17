use std::sync::Arc;

use super::{
    wrapper::{partitioner::PeridotPartitioner, timestamp::TimestampExtractor},
    AppEngine,
};

pub trait EngineContext {
    type Partitioner: PeridotPartitioner;
    type TimestampExtractor: TimestampExtractor;
    type Backend;

    fn partitioner(&self) -> &Self::Partitioner;
    fn timestamp_extractor(&self) -> &Self::TimestampExtractor;
    fn state_backend(&self, source_topic: String, partition: i32) -> Arc<Self::Backend>;
    fn get_source_topic_for_table(&self, table_name: &str) -> Option<String>;
    fn get_table_partition_count(&self, table_name: &str) -> Option<i32>;
}

#[derive(Clone)]
pub struct PeridotEngineContext<P, T, B> {
    engine_ref: Arc<AppEngine<B>>,
    partitioner: Arc<P>,
    timestamp_extractor: Arc<T>,
}

impl<P, T, B> PeridotEngineContext<P, T, B> {
    pub fn new(
        engine_ref: Arc<AppEngine<B>>,
        partitioner: Arc<P>,
        timestamp_extractor: Arc<T>,
    ) -> Self {
        PeridotEngineContext {
            engine_ref,
            partitioner,
            timestamp_extractor,
        }
    }
}

impl<P, T, B> EngineContext for PeridotEngineContext<P, T, B>
where
    P: PeridotPartitioner,
    T: TimestampExtractor,
    B: Send + Sync + 'static,
{
    type Partitioner = P;
    type TimestampExtractor = T;
    type Backend = B;

    fn partitioner(&self) -> &Self::Partitioner {
        &self.partitioner
    }

    fn timestamp_extractor(&self) -> &Self::TimestampExtractor {
        &self.timestamp_extractor
    }

    fn state_backend(&self, _source_topic: String, _partition: i32) -> Arc<Self::Backend> {
        unimplemented!("state_backend")
    }

    fn get_source_topic_for_table(&self, _table_name: &str) -> Option<String> {
        unimplemented!("get_source_topic_for_table")
    }

    fn get_table_partition_count(&self, _table_name: &str) -> Option<i32> {
        unimplemented!("get_table_partition_count")
    }
}
