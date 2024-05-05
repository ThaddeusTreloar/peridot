use std::{marker::PhantomData, sync::Arc};

use crate::{
    message::join::{Combiner, JoinMessage},
    state::backend::{GetView, GetViewDistributor, StateBackend},
};

use super::stream::PipelineStream;

pub struct JoinPipeline<S, T, C>
where
    S: PipelineStream,
{
    inner: S,
    table: Arc<T>,
    combiner: Arc<C>,
}

impl<S, T, C> JoinPipeline<S, T, C>
where
    S: PipelineStream,
    T: GetView,
    S::KeyType: PartialEq<T::KeyType>,
{
    pub fn new(inner: S, table: T, combiner: C) -> Self {
        Self {
            inner,
            table: Arc::new(table),
            combiner: Arc::new(combiner),
        }
    }
}

impl<S, T, C> PipelineStream for JoinPipeline<S, T, C>
where
    S: PipelineStream,
    S::KeyType: Send,
    T: GetView + Send,
    T::KeyType: Send,
    T::ValueType: Send,
    T::Backend: StateBackend + Send + Sync,
    S::KeyType: PartialEq<T::KeyType>,
    C: Combiner<S::ValueType, T::ValueType> + Send + Sync,
    C::Output: Send,
{
    type MStream = JoinMessage<S::MStream, C, T::Backend, T::ValueType>;
    type KeyType = S::KeyType;
    type ValueType = C::Output;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<crate::message::stream::PipelineStage<Self::MStream>>> {
        unimplemented!("")
    }
}
