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

use crate::message::types::{FromMessageOwned, Message, PatchMessage};

use pin_project_lite::pin_project;

use super::{stream::{MessageStream, MessageStreamPoll}, types::{FromMessage, FromMessageMut}};

pin_project! {
    pub struct MapMessage<M, F, E, R> {
        #[pin]
        stream: M,
        callback: Arc<F>,
        _extractor: PhantomData<E>,
        _patcher: PhantomData<R>,
    }
}

impl<M, F, E, R> MapMessage<M, F, E, R> {
    pub fn new(stream: M, callback: Arc<F>) -> Self {
        Self {
            stream,
            callback,
            _extractor: PhantomData,
            _patcher: PhantomData,
        }
    }
}

impl<M, F, E, R> MessageStream for MapMessage<M, F, E, R>
where
    M: MessageStream,
    F: Fn(E) -> R,
    E: FromMessageOwned<M::KeyType, M::ValueType>,
    R: PatchMessage<M::KeyType, M::ValueType>,
    Self: Sized,
{
    type KeyType = R::RK;
    type ValueType = R::RV;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<MessageStreamPoll<Self::KeyType, Self::ValueType>> {
        let this = self.project();

        match ready!(this.stream.poll_next(cx)) {
            MessageStreamPoll::Closed => Poll::Ready(MessageStreamPoll::Closed),
            MessageStreamPoll::Commit => Poll::Ready(MessageStreamPoll::Commit),
            MessageStreamPoll::Revert => Poll::Ready(MessageStreamPoll::Revert),
            MessageStreamPoll::Error(e) => Poll::Ready(MessageStreamPoll::Error(e)),
            MessageStreamPoll::Message(message) => {
                let (extractor, partial_message) = E::from_message_owned(message);

                let map_result = (this.callback)(extractor);

                let patched_message = map_result.patch_message(partial_message);

                Poll::Ready(MessageStreamPoll::Message(patched_message))
            }
        }
    }
}

/*
impl<'a, M, F, E, R> MessageStream for MapMessage<M, F, E, R>
where
    M: MessageStream,
    M::KeyType: 'a,
    M::ValueType: 'a,
    F: Fn(E),
    E: FromMessageMut<'a, M::KeyType, M::ValueType>,
    Self: Sized,
{
    type KeyType = M::KeyType;
    type ValueType = M::ValueType;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<MessageStreamPoll<Self::KeyType, Self::ValueType>> {
        let this = self.project();

        match ready!(this.stream.poll_next(cx)) {
            MessageStreamPoll::Closed => Poll::Ready(MessageStreamPoll::Closed),
            MessageStreamPoll::Commit(val) => Poll::Ready(MessageStreamPoll::Commit(val)),
            MessageStreamPoll::Message(mut message) => {
                let partial_message = E::from_message_mut(&mut message);

                (this.callback)(partial_message);

                Poll::Ready(MessageStreamPoll::Message(message))
            }
        }
    }
}
 */