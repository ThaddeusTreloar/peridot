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
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::message::types::Message;

use super::{MessageStream, MessageStreamPoll};

pin_project! {
    pub struct TransparentQueue<K, V> {
        #[pin]
        input: UnboundedReceiver<Message<K, V>>,
    }
}

impl<K, V> TransparentQueue<K, V> {
    pub fn new(input: UnboundedReceiver<Message<K, V>>) -> Self {
        TransparentQueue { input }
    }
}

impl<K, V> From<UnboundedReceiver<Message<K, V>>> for TransparentQueue<K, V> {
    fn from(input: UnboundedReceiver<Message<K, V>>) -> Self {
        TransparentQueue::new(input)
    }
}

impl<K, V> MessageStream for TransparentQueue<K, V> {
    type KeyType = K;
    type ValueType = V;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<MessageStreamPoll<Self::KeyType, Self::ValueType>> {
        let mut this = self.project();

        match this.input.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(msg)) => Poll::Ready(Some(msg)),
        }
    }
}
