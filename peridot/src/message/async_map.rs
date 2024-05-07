use std::{
    marker::PhantomData,
    pin::Pin,
    process::Output,
    sync::Arc,
    task::{Context, Poll},
};

use crate::message::types::{FromMessage, Message, PatchMessage};

use futures::Future;
use pin_project_lite::pin_project;

use super::stream::MessageStream;

pin_project! {
    pub struct AsyncMapMessage<M, F, E, R> {
        #[pin]
        stream: M,
        callback: Arc<F>,
        futures: Vec<Pin<Box<dyn Future<Output = R>>>>,
        _extractor: PhantomData<E>,
        _patcher: PhantomData<R>,
    }
}

impl<M, F, E, R> AsyncMapMessage<M, F, E, R> {
    pub fn new(stream: M, callback: Arc<F>) -> Self {
        Self {
            stream,
            callback,
            futures: Default::default(),
            _extractor: Default::default(),
            _patcher: Default::default(),
        }
    }
}

impl<M, F, E, R> MessageStream for AsyncMapMessage<M, F, E, R>
where
    M: MessageStream,
    F: Fn(E) -> dyn Future<Output = R>,
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

        let (extractor, partial_message) = E::from_message(msg);

        todo!("");

        //let mut map_future = Box::pin((this.callback)(extractor));

        //match map_future.as_mut().poll(cx) {
        //    Poll::Pending => this.futures.push(map_future),
        //    Poll::Ready(result) => {
        //        todo!("")
        //    }
        //};

        //let patched_message = map_result.patch(partial_message);

        //Poll::Ready(Some(patched_message))
        todo!("")
    }
}
