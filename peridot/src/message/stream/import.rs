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
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use pin_project_lite::pin_project;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::message::types::{Message, PatchMessage};

use super::MessageStream;

pin_project! {
    pub struct ImportQueue<S> {
        #[pin]
        input: S,
    }
}

impl<S> ImportQueue<S> {
    pub fn new(input: S) -> Self {
        ImportQueue { input }
    }
}

impl<S> MessageStream for ImportQueue<S>
where
    S: futures::Stream,
    S::Item: PatchMessage<(), ()>,
{
    type KeyType = <S::Item as PatchMessage<(), ()>>::RK;
    type ValueType = <S::Item as PatchMessage<(), ()>>::RV;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Message<Self::KeyType, Self::ValueType>>> {
        let mut this = self.project();

        match this.input.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(patch)) => {
                let message = Message::default();

                Poll::Ready(patch.patch(message.into()))
            }
        }
    }
}

pin_project! {
    #[project = WrapperProjection]
    pub struct IntegrationWrapper<S, F> {
        #[pin]
        upstream: S,
        integration_callback: Arc<F>
    }
}

impl<S, F> IntegrationWrapper<S, F> {
    pub fn new(upstream: S, callback: F) -> Self {
        Self {
            upstream,
            integration_callback: Arc::new(callback),
        }
    }
}

impl<S, F, RM> futures::Stream for IntegrationWrapper<S, F>
where
    S: futures::Stream,
    F: Fn(S::Item) -> RM,
    RM: PatchMessage<(), ()>,
{
    type Item = RM;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let WrapperProjection {
            upstream,
            integration_callback,
        } = self.project();

        let item = ready!(upstream.poll_next(cx))?;

        (integration_callback)(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        None
    }
}
