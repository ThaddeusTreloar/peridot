use std::{marker::PhantomData, sync::Arc};

use crate::{message::join::{Combiner, JoinMessage}, state::backend::{GetView, GetViewDistributor, StateBackend}};

use super::stream::PipelineStream;

pub struct JoinPipeline<S, T, C, RV> 
where
    S: PipelineStream,
{
    inner: S,
    table: Arc<T>,
    combiner: Arc<C>,
    _return_value: PhantomData<RV>
}

impl<S, T, C, RV> JoinPipeline<S, T, C, RV>
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
            _return_value: Default::default(),
        }
    }
}

impl<S, T, C, RV> PipelineStream for JoinPipeline<S, T, C, RV> 
where
    S: PipelineStream,
    S::KeyType: Send,
    T: GetView + Send,
    T::KeyType: Send,
    T::ValueType: Send,
    T::Backend: StateBackend + Send + Sync,
    S::KeyType: PartialEq<T::KeyType>,
    C: Combiner<S::ValueType, T::ValueType, RV> + Send + Sync,
    RV: Send,
{
    type MStream = JoinMessage<S::MStream, C, T::Backend, T::ValueType, RV>;
    type KeyType = S::KeyType;
    type ValueType = RV;

    fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<crate::message::stream::PipelineStage<Self::MStream>>> {
        unimplemented!("")
    }
}
