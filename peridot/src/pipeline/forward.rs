/*
 * Copyright 2024 Thaddeus Treloar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Future;
use pin_project_lite::pin_project;

use crate::{
    app::error::PeridotAppRuntimeError,
    engine::util::ExactlyOnce,
    message::{
        forward::Forward,
        sink::MessageSink,
        stream::{MessageStream, PipelineStage},
    },
};

use super::{sink::MessageSinkFactory, stream::PipelineStream};

pin_project! {
    #[project = SinkProjection]
    pub struct PipelineForward<S, SF, G = ExactlyOnce>
    where
        S: PipelineStream,
    {
        #[pin]
        queue_stream: S,
        sink_factory: SF,
        _delivery_guarantee: PhantomData<G>
    }
}

impl<S, SF, G> PipelineForward<S, SF, G>
where
    S: PipelineStream,
{
    pub fn new(queue_stream: S, sink_factory: SF) -> Self {
        Self {
            queue_stream,
            sink_factory,
            _delivery_guarantee: PhantomData,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("Queue receiver error: {0}")]
    QueueReceiverError(String),
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineForwardError {}

impl<S, SF, G> Future for PipelineForward<S, SF, G>
where
    S: PipelineStream + Send + 'static,
    S::MStream: MessageStream + Send + 'static,
    SF: MessageSinkFactory<S::KeyType, S::ValueType> + Send + 'static,
    SF::SinkType: Send,
{
    type Output = Result<(), PeridotAppRuntimeError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let SinkProjection {
            mut queue_stream,
            sink_factory,
            ..
        } = self.project();

        loop {
            let PipelineStage(metadata, message_stream) = match queue_stream.as_mut().poll_next(cx)
            {
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(q)) => q,
            };

            let message_sink = sink_factory.new_sink(metadata.clone());

            let forward = Forward::new(message_stream, message_sink, metadata.clone());

            // TODO: maybe store this somewhere to be checked.
            tokio::spawn(forward);
        }
    }
}
