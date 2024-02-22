use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll, ready}, fmt::{Display, Debug},
};

use futures::Future;
use pin_project_lite::pin_project;
use tracing::info;

use crate::{
    engine::{util::ExactlyOnce, QueueMetadata},
    pipeline::{message::{stream::{MessageStream, PipelineStage}, sink::{MessageSink, PrintSink}}, serde_ext::PSerialize}, app::error::PeridotAppRuntimeError,
};

use super::stream::PipelineStream;

pub mod sink;

pub trait MessageSinkFactory {
    type SinkType: MessageSink;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType;
}

pub trait PipelineSink<M>
where
    M: MessageStream
{
    type Error: Display + Debug;
    type SinkType: MessageSink;

    fn start_send(
        self: Pin<&mut Self>,
        message: PipelineStage<M>,
    ) -> Result<(), Self::Error>;
}

pub struct GenericPipelineSink<SF>
{
    sink_factory: SF
}

impl <SF> GenericPipelineSink<SF>
{
    pub fn new(sink_factory: SF) -> Self {
        Self {
            sink_factory
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GenericPipelineSinkError {

}

pub struct PrintSinkFactory<KS, VS> {
    _key_ser_type: PhantomData<KS>,
    _value_ser_type: PhantomData<VS>,
}

impl <KS, VS> PrintSinkFactory<KS, VS> {
    pub fn new() ->  Self {
        Self {
            _key_ser_type: PhantomData,
            _value_ser_type: PhantomData,
        }
    }
}

impl <KS, VS> MessageSinkFactory for PrintSinkFactory<KS, VS>
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

impl <SF, M> PipelineSink<M> for GenericPipelineSink<SF>
where
    M: MessageStream + Send + 'static,
    SF: MessageSinkFactory,
    SF::SinkType: Send + 'static,
    <SF::SinkType as MessageSink>::KeySerType: PSerialize<Input = M::KeyType>,
    <SF::SinkType as MessageSink>::ValueSerType: PSerialize<Input = M::ValueType>,

{
    type Error = GenericPipelineSinkError;
    type SinkType = SF::SinkType;

    fn start_send(
            self: Pin<&mut Self>,
            queue_item: PipelineStage<M>,
        ) -> Result<(), Self::Error>
    {

        let PipelineStage(metadata, message_stream) = queue_item;

        let message_sink = self.sink_factory.new_sink(metadata);

        let forward = Forward::new(message_stream, message_sink);

        tokio::spawn(forward);

        Ok(())
    }
}

pin_project! {
    #[project = SinkProjection]
    pub struct PipelineForward<S, Si, G = ExactlyOnce>
    where
        S: PipelineStream,
        Si: PipelineSink<S::MStream>
    {
        #[pin]
        queue_stream: S,
        #[pin]
        sink: Si,
        _delivery_guarantee: PhantomData<G>
    }
}

impl<S, Si, G> PipelineForward<S, Si, G>
where
    S: PipelineStream,
    Si: PipelineSink<S::MStream>
{
    pub fn new(queue_stream: S, sink: Si) -> Self {
        Self {
            queue_stream,
            sink,
            _delivery_guarantee: PhantomData,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("Queue receiver error: {0}")]
    QueueReceiverError(String),
}

impl<S, Si, G> Future for PipelineForward<S, Si, G> 
where
    S: PipelineStream + Send + 'static,
    S::MStream: Send + 'static,
    Si: PipelineSink<S::MStream> + Send + 'static,
    <Si::SinkType as MessageSink>::KeySerType: PSerialize<Input = <S::MStream as MessageStream>::KeyType> + Send + 'static,
    <Si::SinkType as MessageSink>::ValueSerType: PSerialize<Input = <S::MStream as MessageStream>::ValueType> + Send + 'static,
    {
    type Output = Result<(), PeridotAppRuntimeError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> 
    {
        let SinkProjection { mut queue_stream, mut sink, ..} = self.project();
        
        loop {
            let pipeline_stage = match queue_stream.as_mut().poll_next(cx) {
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(q)) => q,
            };

            sink.as_mut().start_send(pipeline_stage).expect("Failed to send pipeline stage");
        }
    }
}

pin_project! {
    #[project = ForwardProjection]
    pub struct Forward<M, Si>
    where
        M: MessageStream,
        Si: MessageSink,
        Si::KeySerType: PSerialize<Input = <M as MessageStream>::KeyType>,
        Si::ValueSerType: PSerialize<Input = <M as MessageStream>::ValueType>,
    {
        #[pin]
        message_stream: M,
        #[pin]
        message_sink: Si,
    }
}

impl<M, Si> Forward<M, Si>
where
    M: MessageStream,
    Si: MessageSink,
    Si::KeySerType: PSerialize<Input = <M as MessageStream>::KeyType>,
    Si::ValueSerType: PSerialize<Input = <M as MessageStream>::ValueType>,
{
    pub fn new(message_stream: M, message_sink: Si) -> Self {
        Self {
            message_stream,
            message_sink,
        }
    }
}

const BATCH_SIZE: usize = 1024;

impl <M, Si> Future for Forward<M, Si>
where
    M: MessageStream,
    Si: MessageSink,
    Si::KeySerType: PSerialize<Input = <M as MessageStream>::KeyType>,
    Si::ValueSerType: PSerialize<Input = <M as MessageStream>::ValueType>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ForwardProjection { mut message_stream, mut message_sink, .. } = self.project();

        info!("Forwarding messages from stream to sink...");

        let mut poll_count = 0;

        loop {
            match message_stream.as_mut().poll_next(cx) {
                Poll::Ready(None) => {
                    info!("No Messages left for stream, finishing...");
                    ready!(message_sink.as_mut().poll_close(cx));
                    return Poll::Ready(())
                },
                Poll::Pending => {
                    info!("No messages available, waiting...");
                    ready!(message_sink.as_mut().poll_commit(cx));
                    return Poll::Pending;
                },
                Poll::Ready(Some(message)) => {
                    message_sink.as_mut().start_send(message).expect("Failed to send message to sink.");
                    poll_count += 1;

                    if poll_count >= BATCH_SIZE {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                },
            };
        }
    }
}