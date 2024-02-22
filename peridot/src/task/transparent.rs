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

use super::{transform::TransformTask, Task};

#[must_use = "pipelines do nothing unless patched to a topic"]
pub struct TransparentTask<'a, R, G>
where
    R: PipelineStream,
    G: DeliveryGuaranteeType,
{
    app: &'a mut PeridotApp<G>,
    output: R,
}

impl<'a, R, G> TransparentTask<'a, R, G>
where
    R: PipelineStream + 'static,
    G: DeliveryGuaranteeType + 'static,
{
    pub fn new(app: &'a mut PeridotApp<G>, handler: R) -> Self {
        Self {
            app,
            output: handler,
        }
    }
}

impl<'a, R, G> Task<'a, G> for TransparentTask<'a, R, G>
where
    R: PipelineStream + Send + 'static,
    R::MStream: Send,
    G: DeliveryGuaranteeType + Send + 'static,
{
    type R = R;

    fn into_pipeline(self) -> Self::R {
        self.output
    }

    fn into_parts(self) -> (&'a mut PeridotApp<G>, Self::R) {
        let Self { app, output, .. } = self;

        (app, output)
    }
}
