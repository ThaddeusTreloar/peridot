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

use core::panic;
use std::{
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll, Waker},
    time::Duration,
};

use futures::{FutureExt, Stream};
use parking_lot::Mutex;
use pin_project_lite::pin_project;
use rdkafka::{consumer::base_consumer::PartitionQueue, Message as rdMessage};
use tokio::time::{sleep, Instant, Sleep};
use tracing::error;

use crate::{
    app::extensions::PeridotConsumerContext, engine::{context::EngineContext, queue_manager::partition_queue::{self, PeridotPartitionQueue}, wrapper::serde::PeridotDeserializer}, message::{
        types::{Message, TryFromBorrowedMessage, TryFromOwnedMessage},
        StreamState,
    }
};

use super::{MessageStream, MessageStreamPoll};

pub const COMMIT_INTERVAL: u64 = 250;

enum HeadType {
    Core,
    Changelog(String),
}

pin_project! {
    #[project=QueueHeadProjection]
    pub struct QueueHead<KS, VS> {
        #[pin]
        partition_queue: PeridotPartitionQueue,
        interval: Option<Instant>,
        is_committed: bool,
        source_topic: String,
        partition: i32,
        commit_waker: Option<Pin<Box<Sleep>>>,
        non_empty_waker: Arc<Mutex<Option<Waker>>>,
        head_type: HeadType,
        highest_offset: i64,
        engine_context: Arc<EngineContext>,
        _key_serialiser: PhantomData<KS>,
        _value_serialiser: PhantomData<VS>,
    }
}

impl<KS, VS> QueueHead<KS, VS> {
    pub fn new(
        mut partition_queue: PeridotPartitionQueue,
        engine_context: Arc<EngineContext>,
        source_topic: &str,
        partition: i32
    ) -> Self {
        let non_empty_waker: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(Default::default()));
        let waker_ref = non_empty_waker.clone();
        let cb = move || {
            let mut maybe_waker = waker_ref.lock();

            if let Some(waker) = maybe_waker.take() {
                tracing::info!("Wake non empty successfully");
                waker.wake()
            } else {
                tracing::info!("Woke with no waker...");
                //todo!("Queue became non empty while lock not present.")
            }
        };

        partition_queue.set_nonempty_callback(cb);

        QueueHead {
            partition_queue,
            interval: None,
            is_committed: false,
            commit_waker: None,
            non_empty_waker,
            source_topic: source_topic.to_owned(),
            partition,
            head_type: HeadType::Core,
            highest_offset: -1,
            engine_context,
            _key_serialiser: PhantomData,
            _value_serialiser: PhantomData,
        }
    }

    pub fn new_changelog(
        mut partition_queue: PeridotPartitionQueue,
        engine_context: Arc<EngineContext>,
        state_store: &str,
        source_topic: &str,
        partition: i32
    ) -> Self {
        let non_empty_waker: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(Default::default()));
        let waker_ref = non_empty_waker.clone();
        let cb = move || {
            let mut maybe_waker = waker_ref.lock();

            if let Some(waker) = maybe_waker.take() {
                tracing::info!("Wake non empty successfully");
                waker.wake()
            } else {
                tracing::info!("Woke with no waker...");
                //todo!("Queue became non empty while lock not present.")
            }
        };

        partition_queue.set_nonempty_callback(cb);

        QueueHead {
            partition_queue,
            interval: None,
            is_committed: false,
            commit_waker: None,
            non_empty_waker,
            source_topic: source_topic.to_owned(),
            partition,
            head_type: HeadType::Changelog(state_store.to_owned()),
            highest_offset: -1,
            engine_context,
            _key_serialiser: PhantomData,
            _value_serialiser: PhantomData,
        }
    }
}

impl<KS, VS> MessageStream for QueueHead<KS, VS>
where
    KS: PeridotDeserializer,
    VS: PeridotDeserializer,
{
    type KeyType = KS::Output;
    type ValueType = VS::Output;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<MessageStreamPoll<KS::Output, VS::Output>> {
        let QueueHeadProjection {
            mut partition_queue,
            interval,
            is_committed,
            commit_waker,
            non_empty_waker,
            source_topic,
            partition,
            highest_offset,
            engine_context,
            head_type,
            ..
        } = self.project();

        match interval.take() {
            None => {
                let _ = interval.replace(Instant::now());
            }
            Some(instant) => {
                if instant.elapsed().as_millis() > COMMIT_INTERVAL as u128 {
                    *is_committed = true;

                    let next_offset = match head_type {
                        HeadType::Core => engine_context.get_consumer_position(&source_topic, *partition),
                        HeadType::Changelog(state_name) => engine_context.get_changelog_consumer_position(&state_name, *partition),
                    };

                    tracing::debug!(
                        "Commit interval reached. Sending commit request upstream for offset: {}",
                        next_offset
                    );

                    return Poll::Ready(MessageStreamPoll::Commit(Ok(next_offset)));
                } else {
                    let _ = interval.replace(instant);
                }
            }
        }

        /*
        if is_closed {
            return Poll::Ready(MessageStreamPoll::Closed)
        }

        if sigint.poll() {
            *is_closed = true;

            return Poll::Ready(MessageStreamPoll::Commit(Ok(next_offset)))
        }
         */

        tracing::debug!(
            "Checking consumer messages for topic: {} partition: {}",
            source_topic,
            partition
        );

        match partition_queue.poll(Duration::from_millis(0)) {
            Some(Ok(message)) => {
                *highest_offset = message.offset();

                *is_committed = false;


                match <Message<KS::Output, VS::Output> as TryFromBorrowedMessage<
                    KS,
                    VS,
                >>::try_from_borrowed_message(message)
                {
                    Err(e) => {
                        tracing::error!("Failed to deser msg: {}", e);
                        todo!("Propograte deserialisation error, drop, or route to dead letter queue.");
                    }
                    Ok(m) => Poll::Ready(MessageStreamPoll::Message(m)),
                }
            }
            Some(Err(e)) => {
                tracing::error!("Failed to get message from upstream: {}", e);
                todo!("Propogate error from head downstream")
            }
            None => {
                non_empty_waker.lock().replace(cx.waker().clone());

                tracing::info!("Queue empty, Pending.");

                
                let consumer_position = match head_type {
                    HeadType::Core => *highest_offset + 1,
                    HeadType::Changelog(store_name) => engine_context.get_changelog_consumer_position(
                        store_name, 
                        *partition
                    )
                };

                tracing::info!("Consumer position: {}", consumer_position);

                match interval {
                    Some(i) => {
                        let elapsed = i.elapsed().as_millis() as u64;

                        if elapsed > COMMIT_INTERVAL {
                            cx.waker().wake_by_ref();
                        } else {
                            let remaining = COMMIT_INTERVAL - elapsed;
    
                            let mut w = Box::pin(sleep(Duration::from_millis(remaining)));
    
                            w.poll_unpin(cx);
    
                            let _ = commit_waker.replace(w);
                        }
                    },
                    None => panic!("No timer set!?")
                }

                tracing::info!(
                    "No buffered messages, Pending..."
                );

                Poll::Pending
            }
        }
/*
        match input.as_mut().poll_next(cx) {
            Poll::Pending => {
                

                // Eager commit code. Tends to increase scheduler contention.
                /*if *is_committed {
                    tracing::info!(
                        "No buffered messages, already committed. Pending..."
                    );

                    Poll::Pending
                } else {
                    *is_committed = true;

                    let next_offset = input.consumer_position();

                    tracing::info!(
                        "No buffered messages. Sending commit request upstream for offset: {}",
                        next_offset
                    );

                    Poll::Ready(MessageStreamPoll::Commit(Ok(next_offset)))
                }*/
            }
            Poll::Ready(None) => panic!("PartitionQueueStreams should never return none. If this panic is triggered, there must be an upstream change."),
            Poll::Ready(Some(raw_msg)) => {
                
            },
        } */
    }
}
