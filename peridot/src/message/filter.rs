use std::{
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use crate::message::types::{FromMessage, Message, PatchMessage};

use pin_project_lite::pin_project;

use super::{
    stream::{MessageStream, MessageStreamPoll},
    BATCH_SIZE,
};

pin_project! {
    pub struct FilterMessage<M, F> {
        #[pin]
        stream: M,
        callback: Arc<F>,
    }
}

impl<M, F> FilterMessage<M, F> {
    pub fn new(stream: M, callback: Arc<F>) -> Self {
        Self { stream, callback }
    }
}

impl<M, F> MessageStream for FilterMessage<M, F>
where
    M: MessageStream,
    F: Fn(&M::KeyType, &M::ValueType) -> bool,
    Self: Sized,
{
    type KeyType = M::KeyType;
    type ValueType = M::ValueType;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<MessageStreamPoll<Self::KeyType, Self::ValueType>> {
        let mut this = self.project();

        for _ in 0..BATCH_SIZE {
            let message = match ready!(this.stream.as_mut().poll_next(cx)) {
                MessageStreamPoll::Message(message) => message,
                other => return Poll::Ready(other),
            };

            if (this.callback)(message.key(), message.value()) {
                return Poll::Ready(MessageStreamPoll::Message(message));
            }
        }

        // Reschedule the task so that it doesn't hold a worker indefinitely
        cx.waker().wake_by_ref();

        Poll::Pending
    }
}
