use std::fmt::Display;

use crate::{
    app::PeridotApp,
    engine::util::DeliveryGuaranteeType,
    message::types::{FromMessage, PatchMessage},
    pipeline::{
        map::MapPipeline,
        sink::PrintSinkFactory,
        stream::{PipelineStream, PipelineStreamExt},
    },
    serde_ext::PSerialize,
    state::backend::ReadableStateBackend,
};

use self::{transform::TransformTask, transparent::TransparentTask};

pub mod transform;
pub mod transparent;

pub trait IntoTask {
    type R: PipelineStream;

    fn into_task<'a, G>(self, app: &'a mut PeridotApp<G>) -> impl Task<'a, G, R = Self::R>
    where
        G: DeliveryGuaranteeType + Send + 'static;
}

impl<P> IntoTask for P
where
    P: PipelineStream + Send + 'static,
{
    type R = P;

    fn into_task<'a, G>(self, app: &'a mut PeridotApp<G>) -> impl Task<'a, G, R = Self::R>
    where
        G: DeliveryGuaranteeType + Send + 'static,
    {
        TransparentTask::new(app, self)
    }
}

pub trait Task<'a, G>
where
    G: DeliveryGuaranteeType + 'static,
{
    type R: PipelineStream + Send + 'static;

    fn and_then<F1, R1>(self, next: F1) -> TransformTask<'a, F1, Self::R, R1, G>
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
        G,
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

    fn into_table<S>(self, table_name: &str) -> ()
    where
        S: ReadableStateBackend<
            KeyType = <Self::R as PipelineStream>::KeyType,
            ValueType = <Self::R as PipelineStream>::ValueType,
        >,
        Self: Sized,
    {
        unimplemented!()
    }

    fn into_pipeline(self) -> Self::R;

    fn into_parts(self) -> (&'a mut PeridotApp<G>, Self::R);

    fn into_topic<KS, VS>(self, topic: &str)
    where
        KS: PSerialize<Input = <Self::R as PipelineStream>::KeyType> + Send + 'static,
        <Self::R as PipelineStream>::KeyType: Display,
        VS: PSerialize<Input = <Self::R as PipelineStream>::ValueType> + Send + 'static,
        <Self::R as PipelineStream>::ValueType: Display,
        Self: Sized,
        Self::R: PipelineStreamExt,
    {
        let sink_factory = PrintSinkFactory::<KS, VS>::new();

        let (app, output) = self.into_parts();

        let job = output.forward(sink_factory);

        app.job(Box::pin(job));
    }
}
