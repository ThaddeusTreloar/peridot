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

pub trait PipelineSink<KS, VS, Si>
where
    KS: PSerialize,
    VS: PSerialize,
    Si: MessageSink<KS, VS>,
{
    type Error;

    fn poll_next_queue(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), Self::Error>>>;
}

pub trait PipelineSinkExt<KS, VS, Si>: PipelineSink<KS, VS, Si>
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
}

pin_project! {
    #[project = SinkProjection]
    pub struct Sink<KS, VS, S, M, Si, G = ExactlyOnce>
    where
        KS: PSerialize,
        VS: PSerialize,
        S: PipelineStream<KS::Input, VS::Input>,
        Si: MessageSink<KS, VS>,
        M: MessageStream<KS::Input, VS::Input>,
    {
        #[pin]
        queue_stream: S,
        sink_topic: String,
        _key_serialiser: PhantomData<KS>,
        _value_serialiser: PhantomData<VS>,
        _queue_type: PhantomData<M>,
        _sink_type: PhantomData<Si>,
        _delivery_guarantee: PhantomData<G>
    }
}

impl<KS, VS, S, M, Si, G> Sink<KS, VS, S, M, Si, G>
where
    KS: PSerialize,
    VS: PSerialize,
    S: PipelineStream<KS::Input, VS::Input>,
    Si: MessageSink<KS, VS>,
    M: MessageStream<KS::Input, VS::Input>,
{
    pub fn new(queue_stream: S, topic: String) -> Self {
        Self {
            queue_stream,
            sink_topic: topic,
            _key_serialiser: PhantomData,
            _value_serialiser: PhantomData,
            _queue_type: PhantomData,
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

impl<KS, VS, S, M, G, Si> Future for Sink<KS, VS, S, M, Si, G> 
where
    KS: PSerialize + Send + 'static,
    VS: PSerialize + Send + 'static,
    S: PipelineStream<KS::Input, VS::Input> + 'static,
    S::M: MessageStream<KS::Input, VS::Input> + Send + 'static,
    M: MessageStream<KS::Input, VS::Input> + Send + 'static,
    Si: MessageSink<KS, VS> + Send + 'static,
{
    type Output = Result<(), PeridotAppRuntimeError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> 
    {
        let SinkProjection { mut queue_stream, sink_topic, ..} = self.project();
        
        loop {
            let PipelineStage { queue_metadata, message_stream , ..} = match queue_stream.as_mut().poll_next(cx) {
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(q)) => q,
            };

            info!("Building sink for topic: {}, parition: {}", sink_topic.clone(), queue_metadata.partition());
    
            let message_sink = Si::from_queue_metadata(queue_metadata.clone());
    
            let join_handle = tokio::spawn(Forward::new(message_stream, message_sink));

            info!("Sink thread spawned: {:?}", join_handle)
        }
    }
}

pin_project! {
    #[project = ForwardProjection]
    pub struct Forward<KS, VS, M, S>
    where
        KS: PSerialize,
        VS: PSerialize,
        M: MessageStream<KS::Input, VS::Input>,
        S: MessageSink<KS, VS>,
    {
        #[pin]
        message_stream: M,
        #[pin]
        message_sink: S,
        _key_serialiser: PhantomData<KS>,
        _value_serialiser: PhantomData<VS>,
    }
}

impl<KS, VS, M, S> Forward<KS, VS, M, S>
where
    KS: PSerialize,
    VS: PSerialize,
    M: MessageStream<KS::Input, VS::Input>,
    S: MessageSink<KS, VS>,
{
    pub fn new(message_stream: M, message_sink: S) -> Self {
        Self {
            message_stream,
            message_sink,
            _key_serialiser: PhantomData,
            _value_serialiser: PhantomData,
        }
    }
}

impl <KS, VS, M, S> Future for Forward<KS, VS, M, S>
where
    KS: PSerialize,
    VS: PSerialize,
    M: MessageStream<KS::Input, VS::Input>,
    S: MessageSink<KS, VS>,
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