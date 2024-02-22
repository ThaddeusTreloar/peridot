use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use crossbeam::atomic::AtomicCell;
use dashmap::{DashMap, DashSet};
use futures::{ready, Future};
use pin_project_lite::pin_project;
use rdkafka::{consumer::Consumer, producer::PARTITION_UA, Message, Offset, TopicPartitionList};
use tokio::sync::broadcast::Receiver;
use tracing::{debug, error, info};

use crate::{
    app::{extensions::OwnedRebalance, PeridotConsumer},
    state::backend::CommitLog,
};

use super::{
    error::PeridotEngineRuntimeError, partition_queue::StreamPeridotPartitionQueue, EngineState,
    RawQueueForwarder,
};

pin_project! {
    #[derive()]
    pub struct QueueDistributor {
        consumer: Arc<PeridotConsumer>,
        downstreams: Arc<DashMap<String, RawQueueForwarder>>,
        downstream_commit_logs: Arc<CommitLog>,
        state_streams: Arc<DashSet<String>>,
        engine_state: Arc<AtomicCell<EngineState>>,
        #[pin]
        rebalance_waker: Receiver<OwnedRebalance>,
        sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    }
}

impl QueueDistributor {
    pub fn new(
        consumer: Arc<PeridotConsumer>,
        downstreams: Arc<DashMap<String, RawQueueForwarder>>,
        downstream_commit_logs: Arc<CommitLog>,
        state_streams: Arc<DashSet<String>>,
        engine_state: Arc<AtomicCell<EngineState>>,
        rebalance_waker: Receiver<OwnedRebalance>,
    ) -> Self {
        Self {
            consumer,
            downstreams,
            downstream_commit_logs,
            state_streams,
            engine_state,
            rebalance_waker,
            sleep: None,
        }
    }
}

fn forward_partitions(
    partitions: TopicPartitionList,
    state_streams: &Arc<DashSet<String>>,
    downstream_commit_logs: &Arc<CommitLog>,
    consumer: &Arc<PeridotConsumer>,
    downstreams: &Arc<DashMap<String, RawQueueForwarder>>,
    engine_state: &Arc<AtomicCell<EngineState>>,
) {
    info!("Handling rebalance");
    let (state, mut for_downstream): (Vec<_>, Vec<_>) = partitions
        .elements()
        .into_iter()
        .map(|tp| (tp.topic().to_string(), tp.partition()))
        .filter(|(_, p)| *p != PARTITION_UA)
        .partition(|(t, _)| state_streams.contains(t));

    for_downstream.extend(
        state
            .into_iter()
            .map(|(t, p)| {
                let local_offset = downstream_commit_logs
                    .get_offset(t.as_str(), p)
                    .unwrap_or(0);
                (t, p, local_offset)
            })
            .map(|(t, p, o)| {
                let (lwm, _) = consumer
                    .fetch_watermarks(&t, p, Duration::from_millis(0))
                    .expect("Failed to fetch watermarks");

                (t, p, std::cmp::max(o, lwm))
            })
            .map(|(t, p, o)| {
                consumer
                    .seek(&t, p, Offset::Offset(o), Duration::from_millis(0))
                    .expect("Failed to seek consumer.");
                (t, p)
            }),
    );

    let count = for_downstream.iter().count();

    if count == 0 {
        info!("No new partitions assigned, skipping...");
        //info!("No assigned partitions for Engine, stopping...");
        //engine_state.store(EngineState::Stopped);
    } else {
        for (topic, partition) in for_downstream.into_iter() {
            let queue_sender = downstreams.get(&topic);

            info!(
                "Attempting to send partition queue to downstream: {}, for partition: {}",
                topic, partition
            );

            match queue_sender {
                None => error!("No downstream found for topic: {}", topic),
                Some(queue_sender) => {
                    let partition_queue = StreamPeridotPartitionQueue::new(
                        consumer
                            .split_partition_queue(&topic, partition)
                            .expect("Failed to get partition queue"),
                    );

                    info!(
                        "Sending partition queue to downstream: {}, for partition: {}",
                        topic, partition
                    );

                    queue_sender
                        .send((partition, partition_queue))
                        .expect("Failed to send partition queue");
                }
            }
        }

        engine_state.store(EngineState::NotReady);
    }
}

impl Future for QueueDistributor {
    type Output = Result<(), PeridotEngineRuntimeError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.sleep.as_mut() {
                Some(sleep) => {
                    ready!(sleep.as_mut().poll(cx));
                    let _ = this.sleep.take();
                }
                _ => (),
            }

            debug!("Polling distributor...");

            if let Some(message) = this.consumer.poll(Duration::from_millis(0)) {
                let message = message.expect("Recieved bad message in distributor");

                error!(
                    "Unexpected consumer message: topic: {}, partition: {}, offset: {}",
                    message.topic(),
                    message.partition(),
                    message.offset()
                );

                panic!(
                    "Unexpected consumer message: topic: {}, partition: {}, offset: {}",
                    message.topic(),
                    message.partition(),
                    message.offset()
                );
            }

            debug!("Checking for rebalance");

            let maybe_rebalance = this.rebalance_waker.recv();

            debug!("Recieved rebalance");

            tokio::pin!(maybe_rebalance);

            match maybe_rebalance.poll(cx) {
                Poll::Ready(Err(e)) => {
                    error!("Failed to recieve rebalance: {}", e);
                    Err(PeridotEngineRuntimeError::BrokenRebalanceReceiver)?
                }
                Poll::Ready(Ok(OwnedRebalance::Error(e))) => Err(e)?,
                Poll::Ready(Ok(OwnedRebalance::Assign(partitions))) => forward_partitions(
                    partitions,
                    this.state_streams,
                    this.downstream_commit_logs,
                    this.consumer,
                    this.downstreams,
                    this.engine_state,
                ),
                _ => (),
            };
            debug!("Sleeping until next poll...");

            let sleep = tokio::time::sleep(Duration::from_millis(1000));

            this.sleep.replace(Box::pin(sleep));
        }
    }
}
