use std::sync::Arc;

use dashmap::DashMap;
use futures::Future;

pub mod in_memory;
pub mod persistent;
pub mod error;

// Commit logs are a temporary solution to the problem of tracking offsets
// This will be deprecated in favour of a upstreaming persitent storage
// to the AppEngine
#[derive(Debug, Default)]
pub struct CommitLogs {
    logs: DashMap<String, CommitLog>
}

impl CommitLogs {
    pub fn get_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        let key = format!("{}-{}", topic, partition);
        self.logs
            .get(&key)
            .and_then(|log| log.get_offset(topic, partition))
    }
}

#[derive(Debug, Default)]
pub struct CommitLog {
    store: DashMap<String, i64>
}

impl CommitLog {
    fn commit_offset(&self, topic: &str, partition: i32, offset: i64) {
        let key = format!("{}-{}", topic, partition);
        self.store.insert(key, offset);
    }

    pub fn get_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        let key = format!("{}-{}", topic, partition);
        self.store.get(&key).map(|offset| *offset)
    }
}

pub trait StateBackend
{
    fn get_commit_log(&self) -> Arc<CommitLog>;
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

