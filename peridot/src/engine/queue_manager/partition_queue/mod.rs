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
    sync::Arc,
    task::{Poll, Waker},
    time::Duration,
};

use futures::{Future, Stream};
use parking_lot::Mutex;
use pin_project_lite::pin_project;
use rdkafka::{consumer::base_consumer::PartitionQueue, message::OwnedMessage, Message};
use tokio::{
    sync::broadcast::error::{RecvError, TryRecvError},
    time::Instant,
};
use tracing::info;

use crate::{
    app::extensions::{OwnedRebalance, PeridotConsumerContext, RebalanceReceiver},
    engine::context::EngineContext,
    message::stream::MessageStreamPoll,
};

pub mod queue_service;

pub type PeridotPartitionQueue = PartitionQueue<PeridotConsumerContext>;

enum QueueType {
    Core,
    Changelog(String),
}

pin_project! {
    #[project = QueueProjection]
    pub struct StreamPeridotPartitionQueue {
        waker: Arc<Mutex<Option<Waker>>>,
        partition_queue: PeridotPartitionQueue,
        source_topic: String,
        partition: i32,
        queue_type: QueueType,
        highest_offset: i64,
        engine_context: Arc<EngineContext>,
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
        engine_context: Arc<EngineContext>,
    ) -> Self {
        let waker: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(Default::default()));
        let waker_ref = waker.clone();
        let cb = move || {
            let mut maybe_waker = waker_ref.lock();

            if let Some(waker) = maybe_waker.take() {
                info!("Wake non empty successfully");
                waker.wake()
            } else {
                info!("Woke with no waker...");
                //todo!("Queue became non empty while lock not present.")
            }
        };

        partition_queue.set_nonempty_callback(cb);

        Self {
            waker,
            partition_queue,
            source_topic,
            partition,
            queue_type: QueueType::Core,
            highest_offset: -1,
            engine_context,
        }
    }

    pub fn new_changelog(
        mut partition_queue: PeridotPartitionQueue,
        state_store: &str,
        source_topic: String,
        partition: i32,
        engine_context: Arc<EngineContext>,
    ) -> Self {
        let waker: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(Default::default()));
        let waker_ref = waker.clone();
        let cb = move || {
            let mut maybe_waker = waker_ref.lock();

            if let Some(waker) = maybe_waker.take() {
                info!("Wake non empty successfully");
                waker.wake()
            } else {
                info!("Woke with no waker...");
                //todo!("Queue became non empty while lock not present.")
            }
        };

        partition_queue.set_nonempty_callback(cb);

        Self {
            waker,
            partition_queue,
            source_topic,
            partition,
            queue_type: QueueType::Changelog(state_store.to_owned()),
            highest_offset: -1,
            engine_context,
        }
    }

    pub fn source_topic(&self) -> &str {
        &self.source_topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }

    pub fn consumer_position(&self) -> i64 {
        match &self.queue_type {
            QueueType::Core => {
                //self
                //    .engine_context
                //    .get_consumer_position(self.source_topic(), self.partition())
                self.highest_offset
            }
            QueueType::Changelog(state_name) => self
                .engine_context
                .get_changelog_next_offset(state_name, self.partition())
                .unwrap_or(0),
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
            highest_offset,
            engine_context,
            ..
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
            Some(Ok(message)) => {
                *highest_offset = message.offset();

                Poll::Ready(Some(message.detach()))
            }
            Some(Err(e)) => {
                tracing::error!("Failed to get message from upstream: {}", e);
                todo!("Propogate error from head downstream")
            }
            None => {
                // PartitionQueues will return None if no messages are buffered
                // We set the waker so that when the queue becomes non empty,
                // the task will resume.
                // TODO: potential race condition if the queue becomes non empty
                // before the lock is set.
                waker.lock().replace(cx.waker().clone());

                info!("Queue empty, Pending.");
                let consumer_position =
                    engine_context.get_consumer_position(source_topic, *partition);

                info!("Consumer position: {}", consumer_position);

                Poll::Pending
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}
