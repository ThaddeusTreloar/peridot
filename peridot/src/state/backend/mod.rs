use futures::Future;
use rdkafka::topic_partition_list::TopicPartitionListElem;

pub mod in_memory;
pub mod persistent;
pub mod error;

#[derive(Debug, Clone)]
pub struct StateStoreCommit {
    topic: String,
    partition: i32,
    offset: i64,
}

impl StateStoreCommit {
    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }
}

impl <'a> From<TopicPartitionListElem<'a>> for StateStoreCommit {
    fn from(elem: TopicPartitionListElem<'a>) -> Self {
        StateStoreCommit {
            topic: elem.topic().to_string(),
            partition: elem.partition(),
            offset: match elem.offset() {
                rdkafka::Offset::Offset(offset) => offset,
                _ => 0
            }
        }
    }
}

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

