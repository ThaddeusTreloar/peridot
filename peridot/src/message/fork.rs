use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{ready, Future};
use pin_project_lite::pin_project;
use tracing::info;

use crate::serde_ext::PSerialize;

use super::{
    sink::MessageSink,
    stream::{ChannelSink, MessageStream},
};

pin_project! {
    #[project = ForkProjection]
    pub struct Fork<M, Si>
    where
        M: MessageStream,
        Si: MessageSink,
        Si::KeySerType: PSerialize<Input = M::KeyType>,
        Si::ValueSerType: PSerialize<Input = M::ValueType>,
    {
        #[pin]
        message_stream: M,
        #[pin]
        message_sink: Si,
    }
}

impl<M, Si> Fork<M, Si>
where
    M: MessageStream,
    Si: MessageSink,
    Si::KeySerType: PSerialize<Input = <M as MessageStream>::KeyType>,
    Si::ValueSerType: PSerialize<Input = <M as MessageStream>::ValueType>,
{
    pub fn new(
        message_stream: M,
        message_sink: Si,
    ) -> Self {
        Self {
            message_stream,
            message_sink,
        }
    }
}

impl<M, Si> MessageStream for Fork<M, Si>
where
    M: MessageStream,
    Si: MessageSink,
    Si::KeySerType: PSerialize<Input = <M as MessageStream>::KeyType>,
    Si::ValueSerType: PSerialize<Input = <M as MessageStream>::ValueType>,
{
    type KeyType = M::KeyType;
    type ValueType = M::ValueType;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<super::types::Message<Self::KeyType, Self::ValueType>>> {
        let ForkProjection {
            mut message_stream,
            mut message_sink,
            ..
        } = self.project();

        info!("ForkedSinking messages from stream to sink...");

        match message_stream.as_mut().poll_next(cx) {
            Poll::Ready(None) => {
                info!("No Messages left for stream, finishing...");
                ready!(message_sink.as_mut().poll_close(cx));
                Poll::Ready(None)
            }
            Poll::Pending => {
                info!("No messages available, waiting...");
                ready!(message_sink.as_mut().poll_commit(cx));


                Poll::Pending
            }
            Poll::Ready(Some(message)) => {
                message_sink
                    .as_mut()
                    .start_send(&message)
                    .expect("Failed to send message to sink.");

                Poll::Ready(Some(message))
            }
        }
    }
}
