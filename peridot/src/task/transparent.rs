use crate::{app::PeridotApp, engine::util::ExactlyOnce, pipeline::stream::PipelineStream};

use super::Task;

#[must_use = "pipelines do nothing unless patched to a topic"]
pub struct TransparentTask<'a, R>
where
    R: PipelineStream,
{
    app: &'a mut PeridotApp<ExactlyOnce>,
    output: R,
}

impl<'a, R> TransparentTask<'a, R>
where
    R: PipelineStream + 'static,
{
    pub fn new(app: &'a mut PeridotApp<ExactlyOnce>, handler: R) -> Self {
        Self {
            app,
            output: handler,
        }
    }
}

impl<'a, R> Task<'a> for TransparentTask<'a, R>
where
    R: PipelineStream + Send + 'static,
    R::MStream: Send,
{
    type R = R;

    fn into_pipeline(self) -> Self::R {
        self.output
    }

    fn into_parts(self) -> (&'a mut PeridotApp<ExactlyOnce>, Self::R) {
        let Self { app, output, .. } = self;

        (app, output)
    }
}
