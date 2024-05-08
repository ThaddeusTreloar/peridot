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
