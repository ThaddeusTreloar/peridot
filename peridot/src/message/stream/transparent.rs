use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use pin_project_lite::pin_project;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::error;

use crate::{
    message::types::{Message, TryFromOwnedMessage},
    serde_ext::PDeserialize,
};

use super::MessageStream;

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

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Message<K, V>>> {
        let mut this = self.project();

        match this.input.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(msg)) => Poll::Ready(Some(msg)),
        }
    }
}
