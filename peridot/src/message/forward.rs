use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{ready, Future};
use pin_project_lite::pin_project;
use tracing::info;

use crate::serde_ext::PSerialize;

use super::{sink::MessageSink, stream::MessageStream};

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

impl<M, Si> Future for Forward<M, Si>
where
    M: MessageStream,
    Si: MessageSink,
    Si::KeySerType: PSerialize<Input = <M as MessageStream>::KeyType>,
    Si::ValueSerType: PSerialize<Input = <M as MessageStream>::ValueType>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ForwardProjection {
            mut message_stream,
            mut message_sink,
            ..
        } = self.project();

        info!("Forwarding messages from stream to sink...");

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
                }
            }
        }

        info!("No messages available, waiting...");
        ready!(message_sink.as_mut().poll_commit(cx));

        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
