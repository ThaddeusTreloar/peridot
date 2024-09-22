/*
 * Copyright 2024 Thaddeus Treloar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

use std::{
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;

use crate::message::{
    map::MapMessage,
    stream::{MessageStream, PipelineStage},
    types::{FromMessageOwned, PatchMessage},
};

use super::stream::PipelineStream;

pin_project! {
    pub struct MapPipeline<S, F, E, R>
    where
        S: PipelineStream,
    {
        #[pin]
        inner: S,
        callback: Arc<F>,
        _extractor: PhantomData<E>,
        _patcher: PhantomData<R>,
    }
}

impl<S, F, E, R> MapPipeline<S, F, E, R>
where
    S: PipelineStream,
{
    pub fn new(inner: S, callback: F) -> Self {
        Self {
            inner,
            callback: Arc::new(callback),
            _extractor: PhantomData,
            _patcher: PhantomData,
        }
    }
}

impl<S, F, E, R> PipelineStream for MapPipeline<S, F, E, R>
where
    S: PipelineStream,
    F: Fn(E) -> R + Send + Sync,
    E: FromMessageOwned<
            <<S as PipelineStream>::MStream as MessageStream>::KeyType,
            <<S as PipelineStream>::MStream as MessageStream>::ValueType,
        > + Send,
    R: PatchMessage<
            <<S as PipelineStream>::MStream as MessageStream>::KeyType,
            <<S as PipelineStream>::MStream as MessageStream>::ValueType,
        > + Send,
{
    type KeyType = R::RK;
    type ValueType = R::RV;
    type MStream = MapMessage<S::MStream, F, E, R>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<PipelineStage<Self::MStream>>> {
        let this = self.project();

        match this.inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(queue)) => Poll::Ready(Option::Some(queue.map(this.callback.clone()))),
        }
    }
}
