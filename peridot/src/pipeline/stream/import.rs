use std::{
    marker::PhantomData, pin::Pin, sync::Arc, task::{Context, Poll}
};

use pin_project_lite::pin_project;
use tracing::info;

use crate::{
    engine::{util::ExactlyOnce, wrapper::serde::PeridotDeserializer, QueueReceiver},
    message::{stream::{import::{ImportQueue, IntegrationWrapper}, serialiser::QueueSerialiser, PipelineStage}, types::PatchMessage},
};

use super::PipelineStream;

pin_project! {
    pub struct ImportPipeline<S, F>
    {
        imports: Vec<S>,
        callback: Arc<F>
    }
}

impl<S, F> ImportPipeline<S, F>
{
    pub fn new(imports: Vec<S>, callback: F) -> Self {
        Self {
            imports,
            callback: Arc::new(callback),
        }
    }
}

impl<S, F, RM> PipelineStream for ImportPipeline<S, F>
where
    S: futures::Stream,
    F: Fn(S::Item) -> RM,
    RM: PatchMessage<(), ()>
{
    type KeyType = RM::RK;
    type ValueType = RM::RV;
    type MStream = IntegrationWrapper<S, F>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<PipelineStage<Self::MStream>>> {
        match self.as_mut().imports.pop() {
            None => Poll::Pending,
            Some(next) => Poll::Ready(Some(IntegrationWrapper::new(next, self.callback.clone()))),
        }
    }
}
