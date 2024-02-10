use futures::Future;
use rdkafka::topic_partition_list::TopicPartitionListElem;

pub mod in_memory;
pub mod persistent;
pub mod error;


pub trait StateBackend {
    fn commit_offset(&self, topic: &str, partition: i32, offset: i64) -> impl Future<Output = ()> + Send;
    fn get_offset(&self, topic: &str, partition: i32) -> impl Future<Output = Option<i64>> + Send;
}

pub trait ReadableStateBackend<T> {
    fn get(&self, key: &str) -> impl Future<Output = Option<T>> + Send;
}

pub trait WriteableStateBackend<T> 
{
    fn set(&self, key: &str, value: T) -> impl Future<Output = Option<T>> + Send;
    fn delete(&self, key: &str)-> impl Future<Output = Option<T>> + Send;
}

