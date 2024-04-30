use crate::{app::PeridotApp, engine::util::DeliveryGuaranteeType, pipeline::stream::PipelineStream, state::backend::{StateBackend, StateBackendContext}};

use super::Task;

#[must_use = "pipelines do nothing unless patched to a topic"]
pub struct TransparentTask<'a, R, B, G>
where
    R: PipelineStream,
    B: StateBackend,
    G: DeliveryGuaranteeType,
{
    app: &'a PeridotApp<B, G>,
    output: R,
}

impl<'a, R, B, G> TransparentTask<'a, R, B, G>
where
    R: PipelineStream + 'static,
    B: StateBackend,
    G: DeliveryGuaranteeType,
{
    pub fn new(app: &'a PeridotApp<B, G>, handler: R) -> Self {
        Self {
            app,
            output: handler,
        }
    }
}

impl<'a, R, B, G> Task<'a> for TransparentTask<'a, R, B, G>
where
    R: PipelineStream + Send + 'static,
    R::MStream: Send,
    B: StateBackendContext + StateBackend + Send + Sync + 'static,
    G: DeliveryGuaranteeType,
{
    type G = G;
    type R = R;
    type B = B;

    fn into_pipeline(self) -> Self::R {
        self.output
    }

    fn into_parts(self) -> (&'a PeridotApp<Self::B, Self::G>, Self::R) {
        let Self { app, output, .. } = self;

        (app, output)
    }
}
