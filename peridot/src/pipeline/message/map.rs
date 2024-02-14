use std::{pin::Pin, task::{Context, Poll}, marker::PhantomData, sync::Arc};

use crate::pipeline::message::{FromMessage, PatchMessage, Message};

use pin_project_lite::pin_project;

use super::MessageStream;

pin_project! {
    pub struct MapMessage<S, F, E, R, K, V> {
        #[pin]
        stream: S,
        callback: Arc<F>,
        _extractor_type: PhantomData<E>,
        _reassembler_type: PhantomData<R>,
        _source_key_type: PhantomData<K>,
        _source_value_type: PhantomData<V>,
    }
}

impl <S, F, E, R, K, V> MapMessage<S, F, E, R, K, V> {
    pub fn new(stream: S, callback: Arc<F>) -> Self {
        Self {
            stream,
            callback,
            _extractor_type: PhantomData,
            _reassembler_type: PhantomData,
            _source_key_type: PhantomData,
            _source_value_type: PhantomData,
        }
    }
}

impl <S, F, E, R, K, V, RK, RV> MessageStream<RK, RV> for MapMessage<S, F, E, R, K, V> 
where S: MessageStream<K, V>,
    F: Fn(E) -> R,
    E: FromMessage<K, V>, 
    R: PatchMessage<K, V, RK, RV>,
    Self: Sized 
{
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Message<RK, RV>>> {
        let this = self.project();
        let next = this.stream.poll_next(cx);

        let msg = match next {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(msg)) => msg
        };

        let extractor = E::from_message(&msg);

        let map_result = (this.callback)(extractor);

        let patched_message = map_result.patch(msg);

        Poll::Ready(Some(patched_message))
    }
}