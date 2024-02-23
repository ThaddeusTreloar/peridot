use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use pin_project_lite::pin_project;
use rdkafka::{
    config::FromClientConfig,
    consumer::Consumer,
    producer::{BaseProducer, BaseRecord, Producer},
    TopicPartitionList,
};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    engine::QueueMetadata,
    message::{
        sink::MessageSink,
        types::{Message, PeridotTimestamp},
    },
    serde_ext::{Json, PSerialize},
};

const BUFFER_LIMIT: i32 = 100;

// Sink Factory

pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub (super) struct TablePartitionHandler<K, V>
    {
        queue_metadata: QueueMetadata,
        target_topic: String,
        producer: BaseProducer,
        next_offset: i64,
        _key_type: PhantomData<K>,
        _value_type: PhantomData<V>,
    }
}

impl<K, V> TablePartitionHandler<K, V> {
    pub(super) fn new(queue_metadata: QueueMetadata, target_topic: String) -> Self {
        let producer = BaseProducer::from_config(queue_metadata.client_config())
            .expect("Failed to create consumer for partition queue");

        producer
            .init_transactions(Duration::from_millis(1000))
            .expect("Failed to init transactions");

        Self {
            queue_metadata,
            target_topic,
            next_offset: Default::default(),
            producer,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TablePartitionHandlerError {}

impl<K, V> MessageSink for TablePartitionHandler<K, V>
where
    K: serde::Serialize,
    V: serde::Serialize,
{
    type KeySerType = Json<K>;
    type ValueSerType = Json<V>;
    type Error = TablePartitionHandlerError;

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        unimplemented!("")
    }

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        unimplemented!("")
    }

    fn start_send(
        self: Pin<&mut Self>,
        message: &Message<
            <Self::KeySerType as PSerialize>::Input,
            <Self::ValueSerType as PSerialize>::Input,
        >,
    ) -> Result<(), Self::Error> {
        let this = self.project();

        let timestamp = match message.timestamp() {
            PeridotTimestamp::NotAvailable => 0,
            PeridotTimestamp::CreateTime(ts) => *ts,
            PeridotTimestamp::LogAppendTime(ts) => *ts,
        };

        let key = serde_json::to_vec(message.key()).expect("Failed to serialize key");
        let value = serde_json::to_vec(message.value()).expect("Failed to serialize value");

        let record = BaseRecord::to(this.target_topic)
            .headers(message.headers().into_owned_headers())
            .timestamp(timestamp)
            .payload(&value)
            .key(&key);

        this.producer.send(record).expect("Failed to send message");

        *this.next_offset = message.offset() + 1;

        Ok(())
    }

    fn poll_commit(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut tpl = TopicPartitionList::default();

        tpl.add_partition_offset(
            &self.queue_metadata.source_topic(),
            self.queue_metadata.partition(),
            rdkafka::Offset::Offset(self.next_offset),
        )
        .expect("Failed to add partition offset");

        self.producer
            .send_offsets_to_transaction(
                &tpl,
                &self
                    .queue_metadata
                    .consumer()
                    .group_metadata()
                    .expect("No Consumer group metadata"),
                Duration::from_millis(1000),
            )
            .expect("Failed to send offsets to transaction");

        match self
            .producer
            .commit_transaction(Duration::from_millis(1000))
        {
            Ok(_) => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Err(e) => {
                // TODO: Handle error
                panic!("Failed to commit transaction: {}", e);
            }
        }
    }

    fn from_queue_metadata(queue_metadata: QueueMetadata) -> Self
    where
        Self: Sized,
    {
        unimplemented!("")
    }
}
