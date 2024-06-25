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
    task::{ready, Context, Poll},
};

use crate::message::types::{FromMessage, Message, PatchMessage};

use pin_project_lite::pin_project;

use super::{
    stream::{MessageStream, MessageStreamPoll},
    BATCH_SIZE,
};

pin_project! {
    pub struct FilterMessage<M, F> {
        #[pin]
        stream: M,
        callback: Arc<F>,
    }
}

impl<M, F> FilterMessage<M, F> {
    pub fn new(stream: M, callback: Arc<F>) -> Self {
        Self { stream, callback }
    }
}

impl<M, F> MessageStream for FilterMessage<M, F>
where
    M: MessageStream,
    F: Fn(&M::KeyType, &M::ValueType) -> bool,
    Self: Sized,
{
    type KeyType = M::KeyType;
    type ValueType = M::ValueType;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<MessageStreamPoll<Self::KeyType, Self::ValueType>> {
        let mut this = self.project();

        for _ in 0..BATCH_SIZE {
            let message = match ready!(this.stream.as_mut().poll_next(cx)) {
                MessageStreamPoll::Message(message) => message,
                other => return Poll::Ready(other),
            };

            if (this.callback)(message.key(), message.value()) {
                return Poll::Ready(MessageStreamPoll::Message(message));
            }
        }

        // Reschedule the task so that it doesn't hold a worker indefinitely
        cx.waker().wake_by_ref();

        Poll::Pending
    }
}
