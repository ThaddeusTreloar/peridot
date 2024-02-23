use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
    pin::Pin,
};

use crate::{
    engine::QueueMetadata,
    message::{
        forward::Forward,
        sink::{MessageSink, PrintSink},
        stream::{MessageStream, MessageStreamExt, PipelineStage},
    },
    serde_ext::PSerialize,
};

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
