use crate::{
    engine::QueueMetadata,
    message::sink::MessageSink,
};

pub mod changelog_sink;
pub mod print_sink;

pub trait MessageSinkFactory {
    type SinkType: MessageSink;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType;
}
