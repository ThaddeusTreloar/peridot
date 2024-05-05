use std::sync::Arc;

use serde::Serialize;

use crate::{
    engine::{
        context::EngineContext, queue_manager::queue_metadata::QueueMetadata,
        wrapper::serde::PeridotSerializer,
    },
    message::sink::{changelog_sink::ChangelogSink, topic_sink::TopicSink},
};

use super::MessageSinkFactory;

pub struct TopicSinkFactory<KS, VS> {
    topic: String,
    engine_context: Arc<EngineContext>,
    _key_ser_type: std::marker::PhantomData<KS>,
    _value_ser_type: std::marker::PhantomData<VS>,
}

impl<KS, VS> TopicSinkFactory<KS, VS> {
    pub fn new(topic: &str, engine_context: Arc<EngineContext>) -> Self {
        Self {
            topic: topic.to_owned(),
            engine_context,
            _key_ser_type: Default::default(),
            _value_ser_type: Default::default(),
        }
    }
}

impl<KS, VS> MessageSinkFactory<KS::Input, VS::Input> for TopicSinkFactory<KS, VS>
where
    KS: PeridotSerializer,
    VS: PeridotSerializer,
    KS::Input: Serialize,
    VS::Input: Serialize,
{
    type SinkType = TopicSink<KS, VS>;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType {
        TopicSink::from_queue_metadata(queue_metadata, &self.topic)
    }
}
