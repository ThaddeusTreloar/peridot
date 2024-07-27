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

pub type Queue = (QueueMetadata, StreamPeridotPartitionQueue);

pub type QueueSender = UnboundedSender<Queue>;
pub type QueueReceiver = UnboundedReceiver<Queue>;

use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use crossbeam::atomic::AtomicCell;
use dashmap::{DashMap, DashSet};
use futures::{ready, Future, FutureExt, SinkExt};
use pin_project_lite::pin_project;
use rdkafka::{producer::PARTITION_UA, Message, TopicPartitionList};
use serde::Deserialize;
use tokio::{
    sync::{
        broadcast::{error::TryRecvError, Receiver},
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
    time::Sleep,
};
use tracing::{debug, error, info};

use crate::{
    app::{
        extensions::{OwnedRebalance, RebalanceReceiver},
        PeridotConsumer,
    },
    engine::{
        context::EngineContext,
        queue_manager::{changelog_queues::ChangelogQueues, state_cells::StateCells},
        wrapper::serde::{PeridotDeserializer, PeridotSerializer},
    },
    message::StreamState,
    state::backend::StateBackend,
};

use self::{
    partition_queue::{PeridotPartitionQueue, StreamPeridotPartitionQueue},
    queue_metadata::QueueMetadata,
};

use super::{
    changelog_manager::{self, ChangelogManager},
    consumer_manager::ConsumerManager,
    engine_state::EngineState,
    error::PeridotEngineRuntimeError,
    metadata_manager::{self, MetadataManager},
    producer_factory::ProducerFactory,
    state_store_manager::StateStoreManager,
    TableMetadata,
};

pub mod changelog_queues;
pub mod partition_queue;
pub mod queue_metadata;
pub mod state_cells;

pin_project! {
    #[derive()]
    pub struct QueueManager<B> {
        engine_context: Arc<EngineContext>,
        state_store_manager: Arc<StateStoreManager<B>>,
        producer_factory: Arc<ProducerFactory>,
        partition_queues: HashMap<(String, i32), StreamPeridotPartitionQueue>,
        //changelog_queues: HashMap<(String, i32), StreamPeridotPartitionQueue>,
        downstreams: Arc<DashMap<String, QueueSender>>,
        engine_state: Arc<AtomicCell<EngineState>>,
        #[pin]
        rebalance_waker: RebalanceReceiver,
        sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    }
}

impl<B> QueueManager<B> {
    pub(crate) fn new(
        engine_context: Arc<EngineContext>,
        state_store_manager: Arc<StateStoreManager<B>>,
        producer_factory: Arc<ProducerFactory>,
        downstreams: Arc<DashMap<String, QueueSender>>,
        engine_state: Arc<AtomicCell<EngineState>>,
        rebalance_waker: RebalanceReceiver,
    ) -> Self {
        let partition_queues = engine_context
            .metadata_manager
            .list_source_topics()
            .into_iter()
            .flat_map(|(topic, md)| (0..md.partition_count()).map(move |p: i32| (topic.clone(), p)))
            .map(|(topic, partition)| {
                let partition_queue = engine_context
                    .consumer_manager
                    .get_partition_queue(&topic, partition, engine_context.clone())
                    .expect("Failed to get partition queue.");

                ((topic, partition), partition_queue)
            })
            .collect();

        Self {
            engine_context,
            state_store_manager,
            producer_factory,
            partition_queues,
            downstreams,
            engine_state,
            rebalance_waker,
            sleep: None,
        }
    }

    pub fn register_downstream(&self, queue_sender: QueueSender) {}
}

impl<B> Future for QueueManager<B>
where
    B: StateBackend,
    B::Error: 'static,
{
    type Output = Result<(), PeridotEngineRuntimeError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        if let Some(sleep) = this.sleep.as_mut() {
            ready!(sleep.as_mut().poll(cx));
            let _ = this.sleep.take();
        }

        //todo!("Enqueue downstream PipelineStage instances and only release on post rebalance waker.");
        // TODO: Check for failed queues and reassign them before the next poll.

        tracing::trace!("Polling distributor...");

        if let Some(message) = this.engine_context.consumer_manager.poll_consumer()? {
            let key = <String as PeridotDeserializer>::deserialize(message.key().unwrap()).unwrap();
            let value =
                <String as PeridotDeserializer>::deserialize(message.payload().unwrap()).unwrap();

            error!(
                "Unexpected consumer message: topic: {}, partition: {}, offset: {}, key: {}, value: {}",
                message.topic(),
                message.partition(),
                message.offset(),
                key,
                value,
            );

            panic!(
                "Unexpected consumer message: topic: {}, partition: {}, offset: {}, key: {}, value: {}",
                message.topic(),
                message.partition(),
                message.offset(),
                key,
                value,
            );
        }

        tracing::trace!("Checking for rebalances");

        let rebalance = match this.rebalance_waker.try_recv() {
            Ok(rebalance) => rebalance,
            Err(TryRecvError::Closed) => {
                tracing::debug!("Wakers closed...");
                return Poll::Ready(Ok(()));
            }
            Err(TryRecvError::Empty) => {
                let mut s = Box::pin(tokio::time::sleep(Duration::from_millis(100)));

                s.poll_unpin(cx);

                let _ = this.sleep.replace(s);

                cx.waker().wake_by_ref();

                return Poll::Pending;
            }
            Err(TryRecvError::Lagged(_)) => panic!("Rebalance receiver lagged."),
        };

        debug!("Recieved rebalance");

        let partitions = match rebalance {
            OwnedRebalance::Error(e) => Err(e)?,
            OwnedRebalance::Assign(partitions) => partitions,
            OwnedRebalance::Revoke(_) => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        };

        tracing::debug!("Handling rebalance");

        let for_downstream: Vec<_> = partitions
            .elements()
            .into_iter()
            .map(|tp| (tp.topic().to_string(), tp.partition()))
            .filter(|(_, p)| *p != PARTITION_UA)
            .filter(|(t, p)| this.partition_queues.contains_key(&(t.clone(), *p)))
            .rev()
            .collect();

        for (topic, partition) in for_downstream.into_iter() {
            let queue_sender = this
                .downstreams
                .get(&topic)
                .expect("Failed to get queue sender for topic.");

            tracing::debug!(
                "Attempting to send partition queue to downstream: {}, for partition: {}",
                topic,
                partition
            );

            this.state_store_manager
                .create_state_store_if_not_exists(&topic, partition)
                .expect("Failed to create state store.");

            let changelog_queues = ChangelogQueues::new(
                this.engine_context
                    .metadata_manager
                    .get_tables_for_topic(&topic)
                    .into_iter()
                    .map(|table| {
                        let changelog_topic = this
                            .engine_context
                            .metadata_manager
                            .get_changelog_topic_for_store(&table);

                        let partition = this
                            .engine_context
                            .changelog_manager
                            .request_changelog_partition_for_state_store(
                                &table,
                                &changelog_topic,
                                partition,
                                this.engine_context.clone(),
                            )
                            .expect("Failed to get changelog partition");

                        (table, partition)
                    })
                    .collect(),
            );

            let state_cells: StateCells = StateCells::new(
                this.engine_context
                    .metadata_manager
                    .get_tables_for_topic(&topic)
                    .into_iter()
                    .map(|state_name| {
                        (
                            state_name.clone(),
                            this.state_store_manager
                                .create_state_store_for_topic(&topic, &state_name, partition)
                                .expect("Failed to build state store for topic."),
                        )
                    })
                    .collect(),
            );

            let partition_queue = this
                .partition_queues
                .remove(&(topic.clone(), partition))
                .expect("Failed to get partition queue.");

            let queue_metadata = QueueMetadata {
                engine_context: this.engine_context.clone(),
                producer_ref: Arc::new(
                    this.producer_factory
                        .create_producer(&topic, partition)
                        .expect("Fail"),
                ),
                changelog_queues,
                state_cells,
                partition,
                source_topic: topic.clone(),
            };

            tracing::debug!(
                "Sending partition queue to downstream: {}, for partition: {}",
                topic,
                partition
            );

            queue_sender
                .send((queue_metadata, partition_queue))
                .expect("Failed to send partition queue");
        }

        cx.waker().wake_by_ref();

        Poll::Pending
    }
}
