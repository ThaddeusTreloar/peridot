use std::fmt::Display;

use crate::{
    app::PeridotApp,
    engine::util::DeliveryGuaranteeType,
    message::types::{FromMessage, PatchMessage},
    pipeline::{
        sink::{GenericPipelineSink, PrintSinkFactory},
        stream::{map::MapPipeline, PipelineStream, PipelineStreamExt, PipelineStreamSinkExt},
    },
    serde_ext::PSerialize,
};

use super::Task;

#[must_use = "pipelines do nothing unless patched to a topic"]
pub struct TransformTask<'a, F, I, R, G>
where
    F: Fn(I) -> R,
    I: PipelineStream,
    R: PipelineStream,
    G: DeliveryGuaranteeType,
{
    app: &'a mut PeridotApp<G>,
    handler: F,
    input: I,
}

impl<'a, F, I, R, G> TransformTask<'a, F, I, R, G>
where
    F: Fn(I) -> R,
    I: PipelineStream,
    R: PipelineStream + Send + 'static,
    R::MStream: Send + 'static,
    G: DeliveryGuaranteeType + 'static,
{
    pub fn new(app: &'a mut PeridotApp<G>, handler: F, input: I) -> Self {
        Self {
            app,
            handler,
            input,
        }
    }
}

impl<'a, F, I, R, G> Task<'a, G> for TransformTask<'a, F, I, R, G>
where
    F: Fn(I) -> R,
    I: PipelineStream,
    R: PipelineStream + Send + 'static,
    G: DeliveryGuaranteeType + Send + 'static,
{
    type R = R;

    fn into_pipeline(self) -> R {
        (self.handler)(self.input)
    }

    fn into_parts(self) -> (&'a mut PeridotApp<G>, Self::R) {
        let Self {
            app,
            handler,
            input,
            ..
        } = self;

        (app, handler(input))
    }
}
