use std::fmt::Display;

use serde::Serialize;

use crate::{
    app::PeridotApp,
    engine::{util::DeliveryGuaranteeType, wrapper::serde::PeridotSerializer},
    message::{join::Combiner, types::{FromMessage, PatchMessage}},
    pipeline::{
        join::JoinPipeline, map::MapPipeline, sink::print_sink::PrintSinkFactory, stream::{PipelineStream, PipelineStreamExt}
    }, state::backend::{facade::{FacadeDistributor, StateFacade}, GetViewDistributor, StateBackend},
};

use self::{table::TableTask, transform::TransformTask};

pub mod table;
pub mod transform;
pub mod transparent;

/*
pub trait IntoTask {
    type G: DeliveryGuaranteeType;
    type B: StateBackend + Send + 'static;
    type R: PipelineStream + Send;

    fn into_task<'a>(self, app: &'a mut PeridotApp<ExactlyOnce>) -> impl Task<'a, R = Self::R>;
}

impl<P> IntoTask for P
where
    P: PipelineStream + Send + 'static,
{
    type R = P;

    fn into_task<'a>(self, app: &'a mut PeridotApp<ExactlyOnce>) -> impl Task<'a, R = Self::R> {
        TransparentTask::new(app, self)
    }
} */

pub trait Task<'a> {
    type G: DeliveryGuaranteeType;
    type B: StateBackend + Send + Sync + 'static;
    type R: PipelineStream + Send + 'static;

    fn and_then<F1, R1>(self, next: F1) -> TransformTask<'a, F1, Self::R, R1, Self::B, Self::G>
    where
        F1: Fn(Self::R) -> R1,
        R1: PipelineStream + Send + 'static,
        R1::MStream: Send + 'static,
        Self: Sized,
    {
        let (app, output) = self.into_parts();

        TransformTask::<'a>::new(app, next, output)
    }

    fn join<T, C>(self, table: T, combiner: C) -> TransformTask<
        'a,
        impl FnOnce(Self::R) -> JoinPipeline<Self::R, FacadeDistributor<T::KeyType, T::ValueType, T::Backend>, C>,
        Self::R,
        JoinPipeline<Self::R, FacadeDistributor<T::KeyType, T::ValueType, T::Backend>, C>,
        Self::B, 
        Self::G
    >
    where
        T: GetViewDistributor + Send + 'a,
        T::KeyType: Send + Sync + 'static,
        T::ValueType: Send + Sync + 'static,
        T::Backend: StateBackend + Sync + 'static,
        C: Combiner<
            <Self::R as PipelineStream>::ValueType,
            T::ValueType,
        > + 'static,
        C::Output: Send + 'static,
        <Self::R as PipelineStream>::KeyType: PartialEq<T::KeyType> + Send,
        Self: Sized,
    {
        let (app, output) = self.into_parts();

        let view = table.get_view_distributor();

        let transform = move |input: Self::R| input.join(view, combiner);

        TransformTask::<'a>::new(app, transform, output)
    }

    fn map<MF, ME, MR>(
        self,
        next: MF,
    ) -> TransformTask<
        'a,
        impl FnOnce(Self::R) -> MapPipeline<Self::R, MF, ME, MR>,
        Self::R,
        MapPipeline<Self::R, MF, ME, MR>,
        Self::B, 
        Self::G
    >
    where
        MF: Fn(ME) -> MR + Send + Sync + Clone + 'static,
        ME: FromMessage<
                <Self::R as PipelineStream>::KeyType,
                <Self::R as PipelineStream>::ValueType,
            > + Send
            + 'static,
        MR: PatchMessage<
                <Self::R as PipelineStream>::KeyType,
                <Self::R as PipelineStream>::ValueType,
            > + Send
            + 'static,
        Self: Sized,
        Self::R: PipelineStreamExt,
    {
        let (app, output) = self.into_parts();

        TransformTask::<'a>::new(app, move |input| input.map(next), output)
    }

    fn into_table(self, table_name: &str) -> TableTask<'a, Self::R, Self::B, Self::G>
    where
        <Self::R as PipelineStream>::KeyType: Clone + Serialize + Send + 'static,
        <Self::R as PipelineStream>::ValueType: Clone + Serialize + Send + 'static,
        Self: Sized + 'a,
    {
        let (app, output) = self.into_parts();

        unimplemented!("");

        app.engine_ref()
            .register_table(table_name.to_owned(), String::new())
            .expect("Table already registered.");

        TableTask::new(app, table_name.to_owned(), output)
    }

    fn into_pipeline(self) -> Self::R;

    fn into_parts(self) -> (&'a PeridotApp<Self::B, Self::G>, Self::R);

    fn into_topic<KS, VS>(self, _topic: &str)
    where
        KS: PeridotSerializer<Input = <Self::R as PipelineStream>::KeyType> + Send + 'static,
        VS: PeridotSerializer<Input = <Self::R as PipelineStream>::ValueType> + Send + 'static,
        KS::Input: Send + Display + 'static,
        VS::Input: Send + Display + 'static,
        Self: Sized + 'a,
        Self::R: PipelineStreamExt,
    {
        let sink_factory = PrintSinkFactory::<KS, VS>::new();
        let (app, output) = self.into_parts();
        let job = output.forward(sink_factory);
        app.job(Box::pin(job));
    }
}
