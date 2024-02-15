use std::{
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::pipeline::message::types::{FromMessage, Message, PatchMessage};

use pin_project_lite::pin_project;
use tracing::info;

use super::stream::MessageStream;

pin_project! {
    pub struct MapMessage<K, V, M, F, E, R> {
        #[pin]
        stream: M,
        callback: Arc<F>,
        _extractor_type: PhantomData<E>,
        _reassembler_type: PhantomData<R>,
        _source_key_type: PhantomData<K>,
        _source_value_type: PhantomData<V>,
    }
}

impl<K, V, M, F, E, R> MapMessage<K, V, M, F, E, R> {
    pub fn new(stream: M, callback: Arc<F>) -> Self {
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

impl<K, V, M, F, E, R, RK, RV> MessageStream<RK, RV> for MapMessage<K, V, M, F, E, R>
where
    M: MessageStream<K, V>,
    F: Fn(E) -> R,
    E: FromMessage<K, V>,
    R: PatchMessage<K, V, RK, RV>,
    Self: Sized,
{
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Message<RK, RV>>> {
        let this = self.project();
        let next = this.stream.poll_next(cx);

        info!("Polling next message for mapping...");

        let msg = match next {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(msg)) => msg,
        };

        info!("Found message, mapping...");

        let extractor = E::from_message(&msg);

        let map_result = (this.callback)(extractor);

        let patched_message = map_result.patch(msg);

        Poll::Ready(Some(patched_message))
    }
}
