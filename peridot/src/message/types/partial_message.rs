use super::{MessageHeaders, PeridotTimestamp};

pub struct PartialMessage<K, V> {
    pub(crate) topic: Option<String>,
    pub(crate) timestamp: Option<PeridotTimestamp>,
    pub(crate) partition: Option<i32>,
    pub(crate) offset: Option<i64>,
    pub(crate) headers: Option<MessageHeaders>,
    pub(crate) key: Option<K>,
    pub(crate) value: Option<V>,
}