use crate::{
    app::PeridotApp,
    engine::util::{DeliveryGuaranteeType, ExactlyOnce},
    pipeline::stream::PipelineStream,
};

use super::Task;

#[must_use = "pipelines do nothing unless patched to a topic"]
pub struct TransformTask<'a, F, I, R>
where
    F: Fn(I) -> R,
    I: PipelineStream,
    R: PipelineStream,
{
    app: &'a mut PeridotApp<ExactlyOnce>,
    handler: F,
    input: I,
}

impl<'a, F, I, R> TransformTask<'a, F, I, R>
where
    F: Fn(I) -> R,
    I: PipelineStream,
    R: PipelineStream + Send + 'static,
    R::MStream: Send + 'static,
{
    pub fn new(app: &'a mut PeridotApp<ExactlyOnce>, handler: F, input: I) -> Self {
        Self {
            app,
            handler,
            input,
        }
    }
}

impl<'a, F, I, R> Task<'a> for TransformTask<'a, F, I, R>
where
    F: Fn(I) -> R,
    I: PipelineStream,
    R: PipelineStream + Send + 'static,
{
    type R = R;

    fn into_pipeline(self) -> R {
        (self.handler)(self.input)
    }

    fn into_parts(self) -> (&'a mut PeridotApp<ExactlyOnce>, Self::R) {
        let Self {
            app,
            handler,
            input,
            ..
        } = self;

        (app, handler(input))
    }
}
