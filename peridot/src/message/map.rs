use std::{
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use crate::message::types::{FromMessage, Message, PatchMessage};

use pin_project_lite::pin_project;

use super::stream::{MessageStream, MessageStreamPoll};

pin_project! {
    pub struct MapMessage<M, F, E, R> {
        #[pin]
        stream: M,
        callback: Arc<F>,
        _extractor: PhantomData<E>,
        _patcher: PhantomData<R>,
    }
}

impl<M, F, E, R> MapMessage<M, F, E, R> {
    pub fn new(stream: M, callback: Arc<F>) -> Self {
        Self {
            stream,
            callback,
            _extractor: PhantomData,
            _patcher: PhantomData,
        }
    }
}

impl<M, F, E, R> MessageStream for MapMessage<M, F, E, R>
where
    M: MessageStream,
    F: Fn(E) -> R,
    E: FromMessage<M::KeyType, M::ValueType>,
    R: PatchMessage<M::KeyType, M::ValueType>,
    Self: Sized,
{
    type KeyType = R::RK;
    type ValueType = R::RV;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<MessageStreamPoll<Self::KeyType, Self::ValueType>> {
        let this = self.project();

        match ready!(this.stream.poll_next(cx)) {
            MessageStreamPoll::Closed => Poll::Ready(MessageStreamPoll::Closed),
            MessageStreamPoll::Commit(val) => Poll::Ready(MessageStreamPoll::Commit(val)),
            MessageStreamPoll::Message(message) => {
                let (extractor, partial_message) = E::from_message(message);

                let map_result = (this.callback)(extractor);

                let patched_message = map_result.patch(partial_message);

                Poll::Ready(MessageStreamPoll::Message(patched_message))
            }
        }
    }
}
