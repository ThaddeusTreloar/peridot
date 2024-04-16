use crate::message::types::{Message, PeridotTimestamp};

pub trait TimestampExtractor {
    fn extract_timestamp<'a, K, V>(&self, message: &'a Message<K, V>) -> &'a PeridotTimestamp;
}

pub struct DefaultTimeStampExtractor;

impl TimestampExtractor for DefaultTimeStampExtractor {
    fn extract_timestamp<'a, K, V>(&self, message: &'a Message<K, V>) -> &'a PeridotTimestamp {
        message.timestamp()
    }
}
