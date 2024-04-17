use std::fmt::Display;

use crate::{
    app::PeridotApp,
    engine::util::ExactlyOnce,
    engine::wrapper::serde::PSerialize,
    message::types::{FromMessage, PatchMessage},
    pipeline::{
        map::MapPipeline,
        sink::print_sink::PrintSinkFactory,
        stream::{PipelineStream, PipelineStreamExt},
    },
};

use self::{transform::TransformTask, transparent::TransparentTask};

pub mod transform;
pub mod transparent;

pub trait IntoTask {
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
}

pub trait Task<'a> {
    type R: PipelineStream + Send + 'static;

    fn and_then<F1, R1>(self, next: F1) -> TransformTask<'a, F1, Self::R, R1>
    where
        F1: Fn(Self::R) -> R1,
        R1: PipelineStream + Send + 'static,
        R1::MStream: Send + 'static,
        Self: Sized,
    {
        let (app, output) = self.into_parts();

        TransformTask::<'a>::new(app, next, output)
    }

    fn map<MF, ME, MR>(
        self,
        next: MF,
    ) -> TransformTask<
        'a,
        impl Fn(Self::R) -> MapPipeline<Self::R, MF, ME, MR>,
        Self::R,
        MapPipeline<Self::R, MF, ME, MR>,
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

        TransformTask::<'a>::new(app, move |input| input.map(next.clone()), output)
    }

    fn into_table(self, _table_name: &str)
    where
        Self: Sized,
    {
        let (_app, _output) = self.into_parts();

        unimplemented!("into_table")
    }

    fn into_pipeline(self) -> Self::R;

    fn into_parts(self) -> (&'a mut PeridotApp<ExactlyOnce>, Self::R);

    fn into_topic<KS, VS>(self, _topic: &str)
    where
        KS: PSerialize<Input = <Self::R as PipelineStream>::KeyType> + Send + 'static,
        <Self::R as PipelineStream>::KeyType: Display,
        VS: PSerialize<Input = <Self::R as PipelineStream>::ValueType> + Send + 'static,
        <Self::R as PipelineStream>::ValueType: Display,
        KS::Input: Send + 'static,
        VS::Input: Send + 'static,
        Self: Sized,
        Self::R: PipelineStreamExt,
    {
        let sink_factory = PrintSinkFactory::<KS, VS>::new();

        let (app, output) = self.into_parts();

        let job = output.forward(sink_factory);

        app.job(Box::pin(job));
    }
}
