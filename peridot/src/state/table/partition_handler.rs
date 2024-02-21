use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll}, ops::Deref, time::Duration,
};

use futures::{ready, Future, FutureExt};
use pin_project_lite::pin_project;
use rdkafka::{producer::{BaseProducer, Producer, BaseRecord}, config::{FromClientConfigAndContext, FromClientConfig}, consumer::Consumer, TopicPartitionList};
use surrealdb::sql::Base;
use tokio::{task::JoinHandle, sync::mpsc::{Sender, UnboundedSender}};

use crate::{
    engine::QueueMetadata,
    pipeline::message::{stream::MessageStream, types::{Message, PeridotTimestamp}, sink::MessageSink},
    state::backend::{ReadableStateBackend, WriteableStateBackend}, app::extensions::PeridotConsumerContext,
};

const BUFFER_LIMIT: i32 = 100;

pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub (super) struct TablePartitionHandler<M>
    where
        M: MessageStream,
    {
        #[pin]
        queue: M,
        queue_metadata: QueueMetadata,
        target_topic: String,
        producer: BaseProducer,
        storage_sink: UnboundedSender<Message<<M as MessageStream>::KeyType, <M as MessageStream>::ValueType>>,
    }
}

impl<M> TablePartitionHandler<M>
where
    M: MessageStream,
{
    pub(super) fn new(queue: M, queue_metadata: QueueMetadata, target_topic: String, storage_sink: UnboundedSender<Message<<M as MessageStream>::KeyType, <M as MessageStream>::ValueType>>) -> Self {
        let producer = BaseProducer::from_config(queue_metadata.client_config()).expect("Failed to create consumer for partition queue");

        producer.init_transactions(Duration::from_millis(1000)).expect("Failed to init transactions");

        Self {
            queue,
            queue_metadata,
            target_topic,
            producer,
            storage_sink
        }
    }
}

impl<M> Future for TablePartitionHandler<M>
where
    M: MessageStream,
    M::KeyType: serde::Serialize,
    M::ValueType: serde::Serialize,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        this.producer.begin_transaction().expect("Failed to begin transaction");
        
        let offset = -1;
        
        loop {
            match this.queue.as_mut().poll_next(cx) {
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => {
                    if offset > 0 {
                        let mut tpl = TopicPartitionList::default();

                        tpl.add_partition_offset(
                            &this.queue_metadata.source_topic(), 
                            this.queue_metadata.partition(), 
                            rdkafka::Offset::Offset(offset)
                        ).expect("Failed to add partition offset");

                        this.producer.send_offsets_to_transaction(
                            &tpl,
                            &this.queue_metadata.consumer().group_metadata().expect("No Consumer group metadata"), 
                            Duration::from_millis(1000)
                        ).expect("Failed to send offsets to transaction");

                        match this.producer.commit_transaction(Duration::from_millis(1000)) {
                            Ok(_) => {
                                return Poll::Pending;
                            },
                            Err(e) => {
                                // Handle error
                                panic!("Failed to commit transaction: {}", e);
                            }
                        }
                    }
                },
                Poll::Ready(Some(message)) => {
                    let timestamp = match message.timestamp() {
                        PeridotTimestamp::NotAvailable => 0,
                        PeridotTimestamp::CreateTime(ts) => *ts,
                        PeridotTimestamp::LogAppendTime(ts) => *ts
                    };
        
                    let key = serde_json::to_vec(message.key()).expect("Failed to serialize key");
                    let value = serde_json::to_vec(message.value()).expect("Failed to serialize value");
        
                    let record = BaseRecord::to(&this.target_topic)
                        .headers(message.headers().into_owned_headers())
                        .timestamp(timestamp)
                        .payload(&value)
                        .key(&key);
        
                    this.producer.send(record).expect("Failed to send message");

                    this.storage_sink.send(message).expect("Downstream closed");

                    if this.producer.in_flight_count() > BUFFER_LIMIT {
                        if offset > 0 {
                            let mut tpl = TopicPartitionList::default();
    
                            tpl.add_partition_offset(
                                &this.queue_metadata.source_topic(), 
                                this.queue_metadata.partition(), 
                                rdkafka::Offset::Offset(offset)
                            ).expect("Failed to add partition offset");
    
                            this.producer.send_offsets_to_transaction(
                                &tpl,
                                &this.queue_metadata.consumer().group_metadata().expect("No Consumer group metadata"), 
                                Duration::from_millis(1000)
                            ).expect("Failed to send offsets to transaction");
    
                            match this.producer.commit_transaction(Duration::from_millis(1000)) {
                                Ok(_) => {
                                    cx.waker().wake_by_ref();
                                    return Poll::Pending;
                                },
                                Err(e) => {
                                    // Handle error
                                    panic!("Failed to commit transaction: {}", e);
                                }
                            }
                        }
                    }
                },
            }
        };
    }
}