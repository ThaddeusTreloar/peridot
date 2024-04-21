use std::{marker::PhantomData, sync::Arc};

use pin_project_lite::pin_project;

use crate::state::backend::{facade::StateFacade, StateBackend};

use super::stream::MessageStream;

pin_project! {
    pub struct JoinMessage<M, C, B, R>
    where
        M: MessageStream,
        C: Combiner<M::ValueType, R>,
    {
        stream: M,
        state: StateFacade<M::KeyType, R, B>,
        combiner: Arc<C>,
    }
}

impl <M, C, B, R> MessageStream for JoinMessage<M, C, B, R>
where
    M: MessageStream,
    B: StateBackend,
    C: Combiner<M::ValueType, R>,
{
    type KeyType = M::KeyType;
    type ValueType = C::Output;

    fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<super::types::Message<Self::KeyType, Self::ValueType>>> {
        unimplemented!("")
    }
}

pub trait Combiner<L, R>: Send + Sync {
    type Output;

    fn combine(&self, left: L, right: R) -> Self::Output;
}

impl <L, R, F, RV> Combiner<L, R> for F
where
    F: Send + Sync + Fn(L, R) -> RV
{
    type Output = RV;
    
    fn combine(&self, left: L, right: R) -> Self::Output {
        (self)(left, right)
    }
}