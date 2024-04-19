use crate::{app::PeridotApp, engine::util::DeliveryGuaranteeType, pipeline::stream::PipelineStream, state::backend::StateBackend};

use super::Task;

#[must_use = "pipelines do nothing unless patched to a topic"]
pub struct TransformTask<'a, F, I, R, B, G>
where
    F: Fn(I) -> R,
    I: PipelineStream,
    R: PipelineStream,
    G: DeliveryGuaranteeType,
{
    app: &'a PeridotApp<B, G>,
    handler: F,
    input: I,
}

impl<'a, F, I, R, B, G> TransformTask<'a, F, I, R, B, G>
where
    F: Fn(I) -> R,
    I: PipelineStream,
    R: PipelineStream + Send + 'static,
    R::MStream: Send + 'static,
    G: DeliveryGuaranteeType,
{
    pub fn new(app: &'a PeridotApp<B, G>, handler: F, input: I) -> Self {
        Self {
            app,
            handler,
            input,
        }
    }
}

impl<'a, F, I, R, B, G> Task<'a> for TransformTask<'a, F, I, R, B, G>
where
    F: Fn(I) -> R,
    I: PipelineStream,
    R: PipelineStream + Send + 'static,
    G: DeliveryGuaranteeType,
    B: StateBackend + Send + Sync + 'static,
{
    type G = G;
    type R = R;
    type B = B;

    fn into_pipeline(self) -> R {
        (self.handler)(self.input)
    }

    fn into_parts(self) -> (&'a PeridotApp<Self::B, Self::G>, Self::R) {
        let Self {
            app,
            handler,
            input,
            ..
        } = self;

        (app, handler(input))
    }
}
