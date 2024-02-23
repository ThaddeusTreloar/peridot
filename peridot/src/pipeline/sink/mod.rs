use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
    pin::Pin,
    task::{ready, Context, Poll},
};

use futures::Future;
use pin_project_lite::pin_project;
use tracing::info;

use crate::{
    app::error::PeridotAppRuntimeError,
    engine::{util::ExactlyOnce, QueueMetadata},
    message::{
        sink::{MessageSink, PrintSink},
        stream::{MessageStream, PipelineStage},
    },
    serde_ext::PSerialize,
};

use super::stream::PipelineStream;

pub mod sink;

pub trait MessageSinkFactory {
    type SinkType: MessageSink;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType;
}

pub trait PipelineSink<M>
where
    M: MessageStream,
{
    type Error: Display + Debug;
    type SinkType: MessageSink;

    fn start_send(self: Pin<&mut Self>, message: PipelineStage<M>) -> Result<(), Self::Error>;
}

pub struct GenericPipelineSink<SF> {
    sink_factory: SF,
}

impl<SF> GenericPipelineSink<SF> {
    pub fn new(sink_factory: SF) -> Self {
        Self { sink_factory }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GenericPipelineSinkError {}

pub struct PrintSinkFactory<KS, VS> {
    _key_ser_type: PhantomData<KS>,
    _value_ser_type: PhantomData<VS>,
}

impl<KS, VS> PrintSinkFactory<KS, VS> {
    pub fn new() -> Self {
        Self {
            _key_ser_type: PhantomData,
            _value_ser_type: PhantomData,
        }
    }
}

impl<KS, VS> MessageSinkFactory for PrintSinkFactory<KS, VS>
where
    KS: PSerialize,
    KS::Input: Display,
    VS: PSerialize,
    VS::Input: Display,
{
    type SinkType = PrintSink<KS, VS>;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType {
        PrintSink::from_queue_metadata(queue_metadata)
    }
}

impl<SF, M> PipelineSink<M> for GenericPipelineSink<SF>
where
    M: MessageStream + Send + 'static,
    SF: MessageSinkFactory,
    SF::SinkType: Send + 'static,
    <SF::SinkType as MessageSink>::KeySerType: PSerialize<Input = M::KeyType>,
    <SF::SinkType as MessageSink>::ValueSerType: PSerialize<Input = M::ValueType>,
{
    type Error = GenericPipelineSinkError;
    type SinkType = SF::SinkType;

    fn start_send(self: Pin<&mut Self>, queue_item: PipelineStage<M>) -> Result<(), Self::Error> {
        let PipelineStage(metadata, message_stream) = queue_item;

        let message_sink = self.sink_factory.new_sink(metadata);

        let forward = Forward::new(message_stream, message_sink);

        tokio::spawn(forward);

        Ok(())
    }
}