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

use std::{marker::PhantomData, sync::Arc, task::Poll};

use pin_project_lite::pin_project;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    engine::queue_manager::queue_metadata,
    message::{
        join::{Combiner, JoinMessage},
        stream::PipelineStage,
    },
    state::backend::{view::GetView, view::GetViewDistributor, StateBackend},
};

use super::stream::PipelineStream;

pin_project! {
    pub struct JoinPipeline<S, T, C>
    where
        S: PipelineStream,
    {
        #[pin]
        inner: S,
        table: Arc<T>,
        combiner: Arc<C>,
    }
}

impl<S, T, C> JoinPipeline<S, T, C>
where
    S: PipelineStream,
    T: GetView,
    S::KeyType: PartialEq<T::KeyType>,
{
    pub fn new(inner: S, table: T, combiner: C) -> Self {
        Self {
            inner,
            table: Arc::new(table),
            combiner: Arc::new(combiner),
        }
    }
}

impl<S, T, C> PipelineStream for JoinPipeline<S, T, C>
where
    S: PipelineStream,
    S::KeyType: Clone + Serialize + Send + Sync + 'static,
    S::ValueType: Send + Sync,
    T: GetView<KeyType = S::KeyType> + Send,
    T::KeyType: Send + Sync,
    T::ValueType: DeserializeOwned + Send + Sync + 'static,
    T::Backend: StateBackend + Send + Sync + 'static,
    S::KeyType: PartialEq<T::KeyType>,
    C: Combiner<S::ValueType, T::ValueType> + Send + Sync,
    C::Output: Send + Sync,
{
    type MStream = JoinMessage<S::MStream, C, T::Backend, T::ValueType>;
    type KeyType = S::KeyType;
    type ValueType = C::Output;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<crate::message::stream::PipelineStage<Self::MStream>>> {
        let this = self.project();

        let PipelineStage(queue_metadata, upstream) = match this.inner.poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(queue)) => queue,
        };

        let facade = this.table.get_view(queue_metadata.partition());

        let join = JoinMessage::new(upstream, facade, this.combiner.clone());

        Poll::Ready(Some(PipelineStage(queue_metadata, join)))
    }
}
