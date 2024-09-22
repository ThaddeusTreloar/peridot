use futures::Future;

use crate::{
    message::types::Message,
    pipeline::stream::PipelineStream,
    state::facade::GetFacadeDistributor,
    state::view::{state_view::StateView, GetViewDistributor, ReadableStateView},
    state::store::{
    },
};

use super::Task;

pub trait Process<F, FR, S> {
    fn process(self, f: F, state: S);
}

impl<'a, F, FR, T, S> Process<F, FR, (&S)> for T
where
    T: Task<'a>,
    S: GetViewDistributor,
    //F: Fn(
    //    Message<<T::R as PipelineStream>::ValueType, <T::R as PipelineStream>::ValueType>,
    //    (StateView<S::KeyType, S::ValueType, S::Backend>),
    //) -> FR,
    FR: Future,
{
    fn process(self, f: F, state: (&S)) {
        unimplemented!("")
    }
}

impl<'a, F, FR, T, S1, S2> Process<F, FR, (&S1, &S2)> for T
where
    T: Task<'a>,
    S1: GetViewDistributor,
    S2: GetViewDistributor,
    F: Fn(
        Message<<T::R as PipelineStream>::ValueType, <T::R as PipelineStream>::ValueType>,
        (
            StateView<S1::KeyType, S1::ValueType, S1::Backend>,
            StateView<S2::KeyType, S2::ValueType, S2::Backend>,
        ),
    ) -> FR,
    FR: Future,
{
    fn process(self, f: F, state: (&S1, &S2)) {
        unimplemented!("")
    }
}
