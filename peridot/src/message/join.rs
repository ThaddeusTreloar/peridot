use std::{marker::PhantomData, sync::Arc};

use pin_project_lite::pin_project;

use crate::state::backend::{facade::StateFacade, StateBackend};

use super::stream::MessageStream;

pin_project! {
    pub struct JoinMessage<M, C, B, R, RV>
    where
        M: MessageStream,
        C: Combiner<M::ValueType, R, RV>,
    {
        stream: M,
        state: StateFacade<M::KeyType, R, B>,
        combiner: Arc<C>,
        _return_value: PhantomData<RV>,
    }
}

impl <M, C, B, R, RV> MessageStream for JoinMessage<M, C, B, R, RV>
where
    M: MessageStream,
    B: StateBackend,
    C: Combiner<M::ValueType, R, RV>,
{
    type KeyType = M::KeyType;
    type ValueType = RV;

    fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<super::types::Message<Self::KeyType, Self::ValueType>>> {
        unimplemented!("")
    }
}

pub trait Combiner<L, R, RV>: Send + Sync {
    fn combine(&self, left: L, right: R) -> RV;
}

impl <L, R, RV, F> Combiner<L, R, RV> for F
where
    F: Send + Sync + Fn(L, R) -> RV
{
    fn combine(&self, left: L, right: R) -> RV {
        (self)(left, right)
    }
}