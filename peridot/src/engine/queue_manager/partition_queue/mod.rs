use std::{
    pin::Pin,
    sync::Arc,
    task::{Poll, Waker},
    time::Duration,
};

use futures::{Future, Stream};
use parking_lot::Mutex;
use pin_project_lite::pin_project;
use rdkafka::{consumer::base_consumer::PartitionQueue, message::OwnedMessage};
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tracing::info;

use crate::app::extensions::{OwnedRebalance, PeridotConsumerContext, RebalanceReceiver};

pub mod queue_service;

pub(super) type PeridotPartitionQueue = PartitionQueue<PeridotConsumerContext>;

pin_project! {
    #[project = QueueProjection]
    pub struct StreamPeridotPartitionQueue {
        waker: Arc<Mutex<Option<Waker>>>,
        partition_queue: PeridotPartitionQueue,
        source_topic: String,
        partition: i32,
        // TODO: maybe remove rebalance waker as due to consumer behaviour when splitting
        // partition queues, we have to split partition queues before our first call to Consumer::poll.
        // Therefore, if we drop a partition queue, there is a possibility that the consumer may buffer
        // messages for this partition before a replacement partition queue.
        // Maybe there is a way to return this queue to the queue manager, but considering there
        // that would make a circular reference, it might be more hassle than it is worth.
        // Another approach would be to have the queue manager create and store a new PeridotPartitionQueue on partition
        // revoke, then this stream would destroy it's own, before closing the pipeline. However, we still have the possibility
        // of some time span where the queue manager has not yet created a new split partition, and this struct
        // has been dropped, in which the BaseConsumer may buffer a message from this partition.
        //pre_rebalance_waker: RebalanceReceiver,
    }
}

impl StreamPeridotPartitionQueue {
    pub fn new(
        mut partition_queue: PeridotPartitionQueue,
        source_topic: String,
        partition: i32,
    ) -> Self {
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
            source_topic,
            partition,
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

        // TODO: Implement some broadcast::Recevier that recieves commands from the
        // app engine. One example would be SIGTERM so that partition queue can propogate
        // the signal to downstream pipelines to commit and close their dependencies.

        let QueueProjection {
            waker,
            partition_queue,
            source_topic,
            partition,
        } = self.project();

        //tracing::debug!("Checking rebalances for topic: {} partition: {}", source_topic, partition);
        //match pre_rebalance_waker.try_recv() {
        //    Err(TryRecvError::Empty) => tracing::debug!("No rebalance for topic: {} partition: {}", source_topic, partition),
        //    Err(e) => panic!("{}", e),
        //    Ok(OwnedRebalance::Revoke(tpl)) => {
        //        if tpl.find_partition(source_topic, *partition).is_some() {
        //            tracing::debug!("Partition revoked for topic: {} partition: {}", source_topic, partition);
        //            return Poll::Ready(None)
        //        }
        //    }
        //    Ok(_) => tracing::debug!("No rebalance for topic: {} partition: {}", source_topic, partition),
        //}

        tracing::debug!(
            "Checking consumer messages for topic: {} partition: {}",
            source_topic,
            partition
        );

        match partition_queue.poll(Duration::from_millis(0)) {
            Some(Ok(message)) => Poll::Ready(Option::Some(message.detach())),
            Some(Err(e)) => panic!("Failed to get message from upstream: {}", e),
            None => {
                waker.lock().replace(cx.waker().clone());

                Poll::Pending
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}
