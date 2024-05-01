pub type Queue = (QueueMetadata, StreamPeridotPartitionQueue);

pub type QueueSender = UnboundedSender<Queue>;
pub type QueueReceiver = UnboundedReceiver<Queue>;

use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use crossbeam::atomic::AtomicCell;
use dashmap::{DashMap, DashSet};
use futures::{ready, Future, FutureExt};
use pin_project_lite::pin_project;
use rdkafka::{producer::PARTITION_UA, Message, TopicPartitionList};
use tokio::sync::{broadcast::{error::TryRecvError, Receiver}, mpsc::{UnboundedReceiver, UnboundedSender}};
use tracing::{debug, error, info};

use crate::{app::{extensions::OwnedRebalance, PeridotConsumer}, engine::{context::EngineContext, queue_manager::changelog_queues::ChangelogQueues}};

use self::{
    queue_metadata::QueueMetadata,
    partition_queue::StreamPeridotPartitionQueue
};

use super::{
    changelog_manager::{self, ChangelogManager}, client_manager::ClientManager, engine_state::EngineState, error::PeridotEngineRuntimeError, metadata_manager::{self, MetadataManager}, producer_factory::ProducerFactory, TableMetadata
};

pub mod changelog_queues;
pub mod partition_queue;
pub mod queue_metadata;

pin_project! {
    #[derive()]
    pub struct QueueManager {
        engine_context: EngineContext,
        producer_factory: Arc<ProducerFactory>,
        downstreams: Arc<DashMap<String, QueueSender>>,
        engine_state: Arc<AtomicCell<EngineState>>,
        #[pin]
        rebalance_waker: Receiver<OwnedRebalance>,
        sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    }
}

impl QueueManager {
    pub(crate) fn new(
        engine_context: EngineContext,
        producer_factory: Arc<ProducerFactory>,
        downstreams: Arc<DashMap<String, QueueSender>>,
        engine_state: Arc<AtomicCell<EngineState>>,
        rebalance_waker: Receiver<OwnedRebalance>,
    ) -> Self {
        Self {
            engine_context,
            producer_factory,
            downstreams,
            engine_state,
            rebalance_waker,
            sleep: None,
        }
    }

    pub fn register_downstream(&self, queue_sender: QueueSender) {

    }
}

impl Future for QueueManager {
    type Output = Result<(), PeridotEngineRuntimeError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        if let Some(sleep) = this.sleep.as_mut() {
            ready!(sleep.as_mut().poll(cx));
            let _ = this.sleep.take();
        }

        // TODO: Check for failed queues and reassign them before the next poll.

        debug!("Polling distributor...");

        if let Some(message) = this.engine_context.client_manager.poll_consumer()? {
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

        debug!("Checking for rebalances");

        let rebalance = match this.rebalance_waker.try_recv() {
            Ok(rebalance) => rebalance,
            Err(TryRecvError::Closed) => return Poll::Ready(Ok(())),
            Err(TryRecvError::Empty) => {
                let sleep = tokio::time::sleep(Duration::from_millis(100));

                this.sleep.replace(Box::pin(sleep));

                if let Some(sleep) = this.sleep.as_mut() {
                    sleep.as_mut().poll(cx);
                }

                return Poll::Pending;
            },
            Err(TryRecvError::Lagged(_)) => panic!("Rebalance receiver lagged."),
        };

        debug!("Recieved rebalance");

        let partitions = match rebalance {
            OwnedRebalance::Error(e) => Err(e)?,
            OwnedRebalance::Assign(partitions) => partitions,
            OwnedRebalance::Revoke(_) => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            },
        };

        info!("Handling rebalance");

        let for_downstream: Vec<_> = partitions
            .elements()
            .into_iter()
            .map(|tp| (tp.topic().to_string(), tp.partition()))
            .filter(|(_, p)| *p != PARTITION_UA)
            .collect();

        for (topic, partition) in for_downstream.into_iter() {
            let queue_sender = this.downstreams.get(&topic)
                .expect("Failed to get queue sender for topic.");

            info!(
                "Attempting to send partition queue to downstream: {}, for partition: {}",
                topic, partition
            );

            let changelog_queues: Vec<(String, StreamPeridotPartitionQueue)> = this
                .engine_context
                .metadata_manager
                .get_tables_for_topic(&topic)
                .into_iter()
                .map(
                    |table| {
                        let changelog_topic = this
                            .engine_context
                            .metadata_manager
                            .derive_changelog_topic(&table);

                        let partition = this
                            .engine_context
                            .changelog_manager
                            .request_changelog_partition(&changelog_topic, partition)
                            .expect("Failed to get changelog partition");

                        (table, partition)
                    }
                ).collect();

            let partition_queue = this
                .engine_context
                .client_manager
                .get_partition_queue(&topic, partition)
                .expect("Failed to get partition queue.");

            let queue_metadata =  QueueMetadata {
                engine_context: this.engine_context.clone(),
                producer_ref: Arc::new(this.producer_factory.create_producer().expect("Fail")),
                changelog_queues: ChangelogQueues::new(changelog_queues),
                partition,
                source_topic: topic.clone(),
            };

            info!(
                "Sending partition queue to downstream: {}, for partition: {}",
                topic, partition
            );

            queue_sender
                .send((queue_metadata, partition_queue))
                .expect("Failed to send partition queue");
        }

        cx.waker().wake_by_ref();

        Poll::Pending
    }
}