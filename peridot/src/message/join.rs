use std::{
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use futures::{Future, FutureExt};
use pin_project_lite::pin_project;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    message::types::PatchMessage,
    state::backend::{facade::StateFacade, ReadableStateView, StateBackend},
};

use super::{
    stream::MessageStream,
    types::{FromMessage, KeyValue, Message, PartialMessage, Value},
};

pin_project! {
    pub struct JoinMessage<M, C, B, R>
    where
        M: MessageStream,
        B: StateBackend,
        C: Combiner<M::ValueType, R>,
    {
        #[pin]
        stream: M,
        state: Arc<StateFacade<M::KeyType, R, B>>,
        combiner: Arc<C>,
        state_future: Option<Pin<Box<JoinMessageFuture<R, B::Error, M::KeyType, M::ValueType>>>>
    }
}

impl<M, C, B, R> JoinMessage<M, C, B, R>
where
    M: MessageStream,
    B: StateBackend,
    C: Combiner<M::ValueType, R>,
{
    pub(crate) fn new(stream: M, state: StateFacade<M::KeyType, R, B>, combiner: Arc<C>) -> Self {
        Self {
            stream,
            state: Arc::new(state),
            combiner,
            state_future: None,
        }
    }
}

impl<M, C, B, R> MessageStream for JoinMessage<M, C, B, R>
where
    M: MessageStream,
    M::KeyType: Clone + Serialize + Send + Sync + 'static,
    R: DeserializeOwned + Send + Sync + 'static,
    B: StateBackend + Sync + 'static,
    C: Combiner<M::ValueType, R>,
{
    type KeyType = M::KeyType;
    type ValueType = C::Output;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<super::types::Message<Self::KeyType, Self::ValueType>>> {
        let this = self.project();

        if this.state_future.is_none() {
            let message = match ready!(this.stream.poll_next(cx)) {
                None => return Poll::Ready(None),
                Some(msg) => msg,
            };

            let join_future = Box::pin(JoinMessageFuture::new(this.state.clone(), message));

            let _ = this.state_future.replace(join_future);
        }

        if let Some(future) = this.state_future {
            let (message, right) = ready!(future.as_mut().poll(cx));

            if let Some(right) = right {
                let (Value(left), partial_message) = Value::from_message(message);

                let output_value = this.combiner.combine(left, right);

                let output_message = Value(output_value).patch(partial_message);

                let _ = this.state_future.take();

                return Poll::Ready(Some(output_message));
            }

            let _ = this.state_future.take();
        }

        cx.waker().wake_by_ref();

        Poll::Pending
    }
}

type inner_fetch<R, E> = Pin<Box<dyn Future<Output = Result<Option<R>, E>> + Send>>;

pin_project! {
    struct JoinMessageFuture<R, E, K, V> {
        state_fetch: inner_fetch<R, E>,
        message: Option<Message<K, V>>,
    }
}

impl<R, K, V> JoinMessageFuture<R, (), K, V>
where
    K: Clone + Serialize + Send + Sync + 'static,
    R: DeserializeOwned + Send + Sync + 'static,
{
    fn new<B>(
        state: Arc<StateFacade<K, R, B>>,
        message: Message<K, V>,
    ) -> JoinMessageFuture<R, B::Error, K, V>
    where
        B: StateBackend + Send + Sync + 'static,
    {
        let key = message.key().clone();

        let state_fetch = Box::pin(state.clone().get(key));

        JoinMessageFuture {
            state_fetch,
            message: Some(message),
        }
    }
}

impl<R, E, K, V> Future for JoinMessageFuture<R, E, K, V>
where
    K: Clone + Serialize + Send + Sync,
    R: DeserializeOwned + Send + Sync,
{
    type Output = (Message<K, V>, Option<R>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match ready!(this.state_fetch.poll_unpin(cx)) {
            Ok(ret) => {
                let msg = this.message.take().unwrap();

                Poll::Ready((msg, ret))
            }
            Err(e) => {
                panic!("Failed")
            }
        }
    }
}

pub trait Combiner<L, R>: Send + Sync {
    type Output;

    fn combine(&self, left: L, right: R) -> Self::Output;
}

impl<L, R, F, RV> Combiner<L, R> for F
where
    F: Send + Sync + Fn(L, R) -> RV,
{
    type Output = RV;

    fn combine(&self, left: L, right: R) -> Self::Output {
        (self)(left, right)
    }
}
