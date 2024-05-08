use std::{
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use futures::{Future, FutureExt};
use pin_project_lite::pin_project;
use rdkafka::message;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::{info, Level};

use crate::{
    message::types::PatchMessage,
    state::backend::{facade::StateFacade, view::ReadableStateView, StateBackend},
};

use super::{
    state_fork::{StoreState, StoreStateCell},
    stream::{MessageStream, MessageStreamPoll},
    types::{FromMessage, KeyValue, Message, PartialMessage, Value},
    BATCH_SIZE,
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
        cached_state_time: i64,
        store_state: Arc<StoreStateCell>,
        waiting_message: Option<Message<M::KeyType, M::ValueType>>,
        state_future: Option<Pin<Box<JoinMessageFuture<R, B::Error, M::KeyType, M::ValueType>>>>
    }
}

impl<M, C, B, R> JoinMessage<M, C, B, R>
where
    M: MessageStream,
    M::KeyType: Clone + Serialize + Send + Sync + 'static,
    R: DeserializeOwned + Send + Sync + 'static,
    B: StateBackend + Sync + 'static,
    C: Combiner<M::ValueType, R>,
{
    pub(crate) fn new(stream: M, state: StateFacade<M::KeyType, R, B>, combiner: Arc<C>) -> Self {
        let store_state = state.get_stream_state().unwrap();

        Self {
            stream,
            state: Arc::new(state),
            combiner,
            cached_state_time: 0,
            store_state,
            waiting_message: None,
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
    ) -> std::task::Poll<MessageStreamPoll<Self::KeyType, Self::ValueType>> {
        let span = tracing::span!(Level::TRACE, "->InnerJoin::poll_next",).entered();

        let mut this = self.project();

        for _ in 0..BATCH_SIZE {
            match this.state_future {
                None => {
                    let message = match this.waiting_message.take() {
                        Some(message) => message,
                        None => match ready!(this.stream.poll_next(cx)) {
                            MessageStreamPoll::Message(msg) => msg,
                            MessageStreamPoll::Closed => {
                                return Poll::Ready(MessageStreamPoll::Closed)
                            }
                            MessageStreamPoll::Commit(val) => {
                                return Poll::Ready(MessageStreamPoll::Commit(val))
                            }
                        },
                    };

                    // TODO: Maybe optimise this by having the message interface provide
                    // timestamp directly.
                    if *this.cached_state_time < message.timestamp().into()
                        && this.store_state.load() != StoreState::Sleeping
                    {
                        tracing::debug!(
                            "chached time outdated: cached_state_time: {}, message_time: {:?}",
                            this.cached_state_time,
                            message.timestamp()
                        );

                        match this.state.poll_time(message.timestamp().into(), cx) {
                            Poll::Pending => {
                                info!("state_time not caught up, sleeping...");
                                let _ = this.waiting_message.replace(message);
                                return Poll::Pending;
                            }
                            Poll::Ready(time) => {
                                info!(
                                    "state_time caught up, new_state_time: {}, message_time: {:?}",
                                    time,
                                    message.timestamp()
                                );
                                *this.cached_state_time = time;
                            }
                        };
                    }

                    let join_future = Box::pin(JoinMessageFuture::new(this.state.clone(), message));

                    let _ = this.state_future.replace(join_future);
                }
                Some(future) => {
                    let (message, right) = ready!(future.as_mut().poll(cx));

                    if let Some(right) = right {
                        let (Value(left), partial_message) = Value::from_message(message);

                        let output_value = this.combiner.combine(left, right);

                        let output_message = Value(output_value).patch(partial_message);

                        let _ = this.state_future.take();

                        return Poll::Ready(MessageStreamPoll::Message(output_message));
                    } else {
                        tracing::debug!(
                            "Dropped message, no RHS in table, offset: {}",
                            message.offset()
                        );
                    }

                    let _ = this.state_future.take();
                }
            }
        }

        cx.waker().wake_by_ref();

        Poll::Pending
    }
}

type InnerFetch<R, E> = Pin<Box<dyn Future<Output = Result<Option<R>, E>> + Send>>;

pin_project! {
    struct JoinMessageFuture<R, E, K, V> {
        state_fetch: InnerFetch<R, E>,
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
