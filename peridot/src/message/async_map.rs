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
    process::Output,
    sync::Arc,
    task::{ready, Context, Poll},
};

use crate::message::types::{FromMessageOwned, Message, PatchMessage};

use futures::Future;
use pin_project_lite::pin_project;

use super::stream::{MessageStream, MessageStreamPoll};

pin_project! {
    pub struct AsyncMapMessage<M, F, E, R> {
        #[pin]
        stream: M,
        callback: Arc<F>,
        futures: Vec<Pin<Box<dyn Future<Output = R>>>>,
        _extractor: PhantomData<E>,
        _patcher: PhantomData<R>,
    }
}

impl<M, F, E, R> AsyncMapMessage<M, F, E, R> {
    pub fn new(stream: M, callback: Arc<F>) -> Self {
        Self {
            stream,
            callback,
            futures: Default::default(),
            _extractor: Default::default(),
            _patcher: Default::default(),
        }
    }
}

impl<M, F, E, R> MessageStream for AsyncMapMessage<M, F, E, R>
where
    M: MessageStream,
    F: Fn(E) -> dyn Future<Output = R>,
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
        let next = this.stream.poll_next(cx);

        let message = match ready!(next) {
            MessageStreamPoll::Message(message) => message,
            other => return other.translate(),
        };

        let (extractor, partial_message) = E::from_message_owned(message);

        todo!("");

        //let mut map_future = Box::pin((this.callback)(extractor));

        //match map_future.as_mut().poll(cx) {
        //    Poll::Pending => this.futures.push(map_future),
        //    Poll::Ready(result) => {
        //        todo!("")
        //    }
        //};

        //let patched_message = map_result.patch(partial_message);

        //Poll::Ready(Some(patched_message))
        todo!("")
    }
}
