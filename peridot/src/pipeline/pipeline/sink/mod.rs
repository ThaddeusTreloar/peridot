use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll, ready},
};

use futures::Future;
use pin_project_lite::pin_project;
use tracing::info;

use crate::{
    engine::util::ExactlyOnce,
    pipeline::{message::{stream::{MessageStream, PipelineStage}, sink::MessageSink}, serde_ext::PSerialize}, app::error::PeridotAppRuntimeError,
};

use super::stream::PipelineStream;

pub mod sink;

pub trait PipelineSink<Si, K, V>
where
    Si: MessageSink,
    Si::KeySerType: PSerialize<Input = K>,
    Si::ValueSerType: PSerialize<Input = V>
{
    type Error;

    fn poll_next_queue(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), Self::Error>>>;
}

/*pub trait PipelineSinkExt<KS, VS, Si>: PipelineSink<KS, VS, Si>
where
    KS: PSerialize,
    VS: PSerialize,
    Si: MessageSink<KS, VS>,
{
    fn sink(self) -> ()
    where
        Self: Sized,
    {
        ()
    }
}

impl<P, KS, VS, Si> PipelineSinkExt<KS, VS, Si> for P
where
    KS: PSerialize,
    VS: PSerialize,
    P: PipelineSink<KS, VS, Si>,
    Si: MessageSink<KS, VS>,
{
}*/

pin_project! {
    #[project = SinkProjection]
    pub struct Sink<S, Si, G = ExactlyOnce>
    where
        S: PipelineStream,
        Si: MessageSink,
        Si::KeySerType: PSerialize<Input = <S::MStream as MessageStream>::KeyType>,
        Si::ValueSerType: PSerialize<Input = <S::MStream as MessageStream>::ValueType>,
    {
        #[pin]
        queue_stream: S,
        sink_topic: String,
        _sink_type: PhantomData<Si>,
        _delivery_guarantee: PhantomData<G>
    }
}

impl<S, Si, G> Sink<S, Si, G>
where
    S: PipelineStream,
    Si: MessageSink,
    Si::KeySerType: PSerialize<Input = <S::MStream as MessageStream>::KeyType>,
    Si::ValueSerType: PSerialize<Input = <S::MStream as MessageStream>::ValueType>,
{
    pub fn new(queue_stream: S, topic: String) -> Self {
        Self {
            queue_stream,
            sink_topic: topic,
            _sink_type: PhantomData,
            _delivery_guarantee: PhantomData,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("Queue receiver error: {0}")]
    QueueReceiverError(String),
}

impl<S, Si, G> Future for Sink<S, Si, G> 
where
    S: PipelineStream + Send + 'static,
    S::MStream: MessageStream + Send + 'static,
    Si: MessageSink + Send + 'static,
    Si::KeySerType: PSerialize<Input = <S::MStream as MessageStream>::KeyType> + Send + 'static,
    Si::ValueSerType: PSerialize<Input = <S::MStream as MessageStream>::ValueType> + Send + 'static,
    {
    type Output = Result<(), PeridotAppRuntimeError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> 
    {
        let SinkProjection { mut queue_stream, sink_topic, ..} = self.project();
        
        loop {
            let PipelineStage (queue_metadata, message_stream) = match queue_stream.as_mut().poll_next(cx) {
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(q)) => q,
            };

            info!("Building sink for topic: {}, parition: {}", sink_topic.clone(), queue_metadata.partition());
    
            let message_sink = Si::from_queue_metadata(queue_metadata.clone());
    
            let join_handle = tokio::spawn(Forward::<_, _>::new(message_stream, message_sink));

            info!("Sink thread spawned: {:?}", join_handle)
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
                },
            };
        }
    }
}