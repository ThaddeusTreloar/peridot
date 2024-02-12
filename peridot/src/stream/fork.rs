use std::{pin::Pin, task::{Context, Poll}, sync::Arc};

use futures::{Stream, Future, ready, StreamExt};
use pin_project_lite::pin_project;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;



pin_project! {
    /// Future for the [`fork`](super::PStreamExt::fork) method.
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Forked<St> 
    where St: Stream 
    {
        broadcast: Arc<tokio::sync::broadcast::Sender<St::Item>>,
    }
}

impl<St> Forked<St>
where
    St: Stream + std::marker::Send + 'static,
    St::Item: Clone + Send + 'static,
{
    pub fn new(stream: St) -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(1024);

        let sender_ref = Arc::new(sender);

        let sender_clone = sender_ref.clone();

        tokio::spawn(async move{
            tokio::pin!(stream);

            while let Some(item) = stream.next().await {
                let _ = sender_clone.send(item.clone());
            }
        });

        Self { broadcast: sender_ref }
    }

    pub fn fork(&self) -> Fork<St> {
        Fork {
            receiver: self.broadcast.subscribe().into(),
            _stream_type: std::marker::PhantomData,
        }
    }
}

pin_project! {
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Fork<St: Stream> {
        #[pin]
        receiver: tokio_stream::wrappers::BroadcastStream<St::Item>,
        _stream_type: std::marker::PhantomData<St>,
    }
}

impl <St> Stream for Fork<St>
where
    St: Stream + std::marker::Send + 'static,
    St::Item: Clone + Send + 'static,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project().receiver.poll_next(cx) {
            Poll::Ready(result) => match result {
                Some(Ok(item)) => Poll::Ready(Some(item)),
                Some(Err(BroadcastStreamRecvError::Lagged(lag_len))) => panic!("BroadcastStream lagged by {}", lag_len),
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}