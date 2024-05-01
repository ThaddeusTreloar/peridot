use serde::Serialize;

use crate::{engine::queue_manager::queue_metadata::QueueMetadata, message::sink::changelog_sink::ChangelogSink};

use super::MessageSinkFactory;

pub struct ChangelogSinkFactory<K, V> {
    changelog_topic: String,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<K, V> ChangelogSinkFactory<K, V> {
    pub fn new(changelog_topic: String) -> Self {
        Self {
            changelog_topic,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

impl<K, V> MessageSinkFactory<K, V> for ChangelogSinkFactory<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    type SinkType = ChangelogSink<K, V>;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType {
        ChangelogSink::from_queue_metadata(queue_metadata, self.changelog_topic.clone())
    }
}
