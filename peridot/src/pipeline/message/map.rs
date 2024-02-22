use std::{
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::pipeline::message::types::{FromMessage, Message, PatchMessage};

use pin_project_lite::pin_project;

use super::stream::MessageStream;

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
    ) -> Poll<Option<Message<Self::KeyType, Self::ValueType>>> {
        let this = self.project();
        let next = this.stream.poll_next(cx);

        let msg = match next {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(msg)) => msg,
        };

        let extractor = E::from_message(&msg);

        let map_result = (this.callback)(extractor);

        let patched_message = map_result.patch(msg);

        Poll::Ready(Some(patched_message))
    }
}
