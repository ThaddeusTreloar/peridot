use std::{
    sync::Arc,
    task::{Poll, Waker},
    time::Duration,
};

use futures::Stream;
use parking_lot::Mutex;
use rdkafka::{consumer::base_consumer::PartitionQueue, message::OwnedMessage};

use crate::app::extensions::PeridotConsumerContext;

type PeridotPartitionQueue = PartitionQueue<PeridotConsumerContext>;

pub struct StreamPeridotPartitionQueue {
    waker: Arc<Mutex<Option<Waker>>>,
    partition_queue: PeridotPartitionQueue,
}

impl StreamPeridotPartitionQueue {
    pub fn new(mut partition_queue: PeridotPartitionQueue) -> Self {
        let waker: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(Default::default()));
        let waker_ref = waker.clone();
        let cb = move || {
            let mut maybe_waker = waker_ref.lock();

            if let Some(waker) = maybe_waker.take() {
                waker.wake()
            }
        };

        partition_queue.set_nonempty_callback(cb);

        Self {
            waker,
            partition_queue,
        }
    }
}

impl Stream for StreamPeridotPartitionQueue {
    type Item = OwnedMessage;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // TODO: handle stream consumption time option.
        // let stream_time: i64 = 0;
        // let _timestamp = PeridotTimestamp::ConsumptionTime(stream_time);

        match self.partition_queue.poll(Duration::from_millis(0)) {
            Some(Ok(message)) => Poll::Ready(Option::Some(message.detach())),
            Some(Err(e)) => panic!("Failed to get message from upstream: {}", e),
            None => {
                self.waker.lock().replace(cx.waker().clone());

                Poll::Pending
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}
