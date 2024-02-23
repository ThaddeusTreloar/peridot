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
    Si: PipelineSink<S::MStream>,
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
    <Si::SinkType as MessageSink>::KeySerType:
        PSerialize<Input = <S::MStream as MessageStream>::KeyType> + Send + 'static,
    <Si::SinkType as MessageSink>::ValueSerType:
        PSerialize<Input = <S::MStream as MessageStream>::ValueType> + Send + 'static,
{
    type Output = Result<(), PeridotAppRuntimeError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let SinkProjection {
            mut queue_stream,
            mut sink,
            ..
        } = self.project();

        loop {
            let pipeline_stage = match queue_stream.as_mut().poll_next(cx) {
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(q)) => q,
            };

            sink.as_mut()
                .start_send(pipeline_stage)
                .expect("Failed to send pipeline stage");
        }
    }
}