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
    #[project = ForkedSinkProjection]
    pub struct ForkedForward<M, Si>
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
        #[pin]
        downstream: ChannelSink<M::KeyType, M::ValueType>
    }
}

impl<M, Si> ForkedForward<M, Si>
where
    M: MessageStream,
    Si: MessageSink,
    Si::KeySerType: PSerialize<Input = <M as MessageStream>::KeyType>,
    Si::ValueSerType: PSerialize<Input = <M as MessageStream>::ValueType>,
{
    pub fn new(
        message_stream: M,
        message_sink: Si,
        downstream: ChannelSink<M::KeyType, M::ValueType>,
    ) -> Self {
        Self {
            message_stream,
            message_sink,
            downstream,
        }
    }
}

const BATCH_SIZE: usize = 1024;

impl<M, Si> Future for ForkedForward<M, Si>
where
    M: MessageStream,
    Si: MessageSink,
    Si::KeySerType: PSerialize<Input = <M as MessageStream>::KeyType>,
    Si::ValueSerType: PSerialize<Input = <M as MessageStream>::ValueType>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ForkedSinkProjection {
            mut message_stream,
            mut message_sink,
            mut downstream,
            ..
        } = self.project();

        info!("ForkedSinking messages from stream to sink...");

        for _ in 0..BATCH_SIZE {
            match message_stream.as_mut().poll_next(cx) {
                Poll::Ready(None) => {
                    info!("No Messages left for stream, finishing...");
                    ready!(message_sink.as_mut().poll_close(cx));
                    return Poll::Ready(());
                }
                Poll::Pending => {
                    info!("No messages available, waiting...");
                    ready!(message_sink.as_mut().poll_commit(cx));
                    return Poll::Pending;
                }
                Poll::Ready(Some(message)) => {
                    message_sink
                        .as_mut()
                        .start_send(&message)
                        .expect("Failed to send message to sink.");

                    if let Err(e) = downstream.send(message) {
                        panic!("Failed to send message to downstream: {:?}", e);
                        // Handle the error
                    }
                }
            }
        }

        info!("No messages available, waiting...");
        ready!(message_sink.as_mut().poll_commit(cx));

        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
