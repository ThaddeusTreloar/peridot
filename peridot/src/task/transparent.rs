use crate::{
    app::PeridotApp, engine::util::DeliveryGuaranteeType, pipeline::stream::PipelineStream,
    state::backend::StateBackend,
};

use super::{PipelineParts, Task};

#[must_use = "pipelines do nothing unless patched to a topic"]
pub struct TransparentTask<'a, R, B, G>
where
    R: PipelineStream,
    B: StateBackend,
    G: DeliveryGuaranteeType,
{
    app: &'a PeridotApp<B, G>,
    source_topic: String,
    output: R,
}

impl<'a, R, B, G> TransparentTask<'a, R, B, G>
where
    R: PipelineStream + 'static,
    B: StateBackend,
    G: DeliveryGuaranteeType,
{
    pub fn new(app: &'a PeridotApp<B, G>, source_topic: &str, handler: R) -> Self {
        Self {
            app,
            source_topic: source_topic.to_owned(),
            output: handler,
        }
    }
}

impl<'a, R, B, G> Task<'a> for TransparentTask<'a, R, B, G>
where
    R: PipelineStream + Send + 'static,
    R::MStream: Send,
    B: StateBackend + Send + Sync + 'static,
    G: DeliveryGuaranteeType,
{
    type G = G;
    type R = R;
    type B = B;

    fn into_pipeline(self) -> Self::R {
        self.output
    }

    fn into_parts(self) -> PipelineParts<'a, Self::B, Self::G, Self::R> {
        let Self {
            app,
            source_topic,
            output,
        } = self;

        PipelineParts(app, source_topic, output)
    }
}
