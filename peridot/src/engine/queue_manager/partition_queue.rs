use std::{
    pin::Pin, sync::Arc, task::{Poll, Waker}, time::Duration
};

use futures::{Future, Stream};
use parking_lot::Mutex;
use pin_project_lite::pin_project;
use rdkafka::{consumer::base_consumer::PartitionQueue, message::OwnedMessage};
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tracing::info;

use crate::app::extensions::{OwnedRebalance, PeridotConsumerContext, RebalanceReceiver};

type PeridotPartitionQueue = PartitionQueue<PeridotConsumerContext>;

pin_project! {
    #[project = QueueProjection]
    pub struct StreamPeridotPartitionQueue {
        waker: Arc<Mutex<Option<Waker>>>,
        partition_queue: PeridotPartitionQueue,
        source_topic: String, 
        partition: i32,
        pre_rebalance_waker: RebalanceReceiver,
    }
}

impl StreamPeridotPartitionQueue {
    pub fn new(
        mut partition_queue: PeridotPartitionQueue, 
        pre_rebalance_waker: RebalanceReceiver,
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
            pre_rebalance_waker,
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
        
        let QueueProjection { 
            waker, 
            partition_queue, 
            mut pre_rebalance_waker, 
            source_topic,
            partition, 
        } = self.project();

        info!("Checking rebalances for topic: {} partition: {}", source_topic, partition);
        match pre_rebalance_waker.try_recv() {
            Err(TryRecvError::Empty) => info!("No rebalance for topic: {} partition: {}", source_topic, partition),
            Err(e) => panic!("{}", e),
            Ok(OwnedRebalance::Revoke(tpl)) => {
                if tpl.find_partition(source_topic, *partition).is_some() {
                    info!("Partition revoked for topic: {} partition: {}", source_topic, partition);
                    return Poll::Ready(None)
                }
            }
            Ok(_) => info!("No rebalance for topic: {} partition: {}", source_topic, partition),
        }

        info!("Checking consumer messages for topic: {} partition: {}", source_topic, partition);
        
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
