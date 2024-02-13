use std::{sync::Arc, pin::Pin, task::{Context, Poll}, time::Duration, marker::PhantomData};

use futures::Sink;
use rdkafka::{
    consumer::{StreamConsumer, Consumer},
    ClientConfig, message::BorrowedMessage, producer::{FutureProducer, BaseProducer, Producer}, TopicPartitionList, error::KafkaError,
};
use tracing::info;

use crate::{
    app::extensions::PeridotConsumerContext,
    stream::types::{IntoRecordParts, RecordPartsError, TryIntoRecordParts}, serde_ext::PSerialize
};

use super::{
    app_engine::util::{ExactlyOnce, AtLeastOnce},
    error::PeridotAppRuntimeError, wrappers::MessageContext,
};


pub struct PSink<K, V, G = ExactlyOnce> {
    // Maybe swap to threaded producer
    producer: Arc<BaseProducer>,
    consumer_ref: Arc<StreamConsumer<PeridotConsumerContext>>,
    topic: String,
    _key_ser_type: PhantomData<K>,
    _value_ser_type: PhantomData<V>,
    _delivery_guarantee: PhantomData<G>,
}

impl<K, V, G> PSink<K, V, G> {
    pub fn new(
        producer: BaseProducer,
        consumer_ref: Arc<StreamConsumer<PeridotConsumerContext>>,
        topic: String,
    ) -> Self {
        Self {
            producer: Arc::new(producer),
            consumer_ref,
            topic,
            _key_ser_type: PhantomData,
            _value_ser_type: PhantomData,
            _delivery_guarantee: PhantomData,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PSinkError {
    #[error("Failed to send message: {0}")]
    SendError(String),
    #[error("Failed to commit transaction: {0}")]
    CommitError(String),
    #[error("Failed to create record: {0}")]
    RecordCreationError(#[from] RecordPartsError),
    #[error("Kafka runtime error: {0}")]
    KafkaRuntimeError(#[from] rdkafka::error::KafkaError),
    #[error("Add partition offset error: {0}")]
    AddPartitionOffsetError(String),
}

impl <K, V, I> Sink<I> for PSink<K, V, ExactlyOnce>
where I: TryIntoRecordParts<K, V> + MessageContext,
    K: PSerialize,
    V: PSerialize
{
    type Error = PSinkError;

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        info!("Starting transaction");

        self.producer.begin_transaction().expect("Failed to begin transaction");

        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: I,
    ) -> Result<(), Self::Error> {
    
        let topic = item.consumer_topic();
        let partition = item.consumer_partition();
        let offset = item.consumer_offset();

        let parts = item
            .try_into_record_parts()?;

        let record = parts
            .into_record(&self.topic);

        let group_meta = self.consumer_ref.group_metadata().expect("No group metadata");

        let mut tpl = TopicPartitionList::new();

        tpl.add_partition_offset(&topic, partition, rdkafka::Offset::Offset(offset))
            .map_err(|e| PSinkError::AddPartitionOffsetError(e.to_string()))?;

        match self.producer.send(record) {
            Ok(_) => {
                info!("Sending message offsets for transaction: topic: {}, partition: {}, offset: {}", topic, partition, offset);

                self.producer
                    .send_offsets_to_transaction(
                        &tpl, 
                        &group_meta, 
                        Duration::from_millis(1000)
                    ).expect("Failed to commit offsets to transaction");
                Ok(())
            },
            Err(e) => {
                // Todo: Dead letter queue
                Err(PSinkError::from(e.0))
            },
        }
    }

    // TODO: guarentee transaction.timeout.ms
    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        info!("Commiting transaction");

        while let Err(e) = self.producer.commit_transaction(Duration::from_millis(1000)) {
            match e {
                KafkaError::Transaction(e) => {
                    if e.is_retriable() {
                        info!("Retrying transaction commit: {}", e);
                    } else {
                        info!("Non-retriable error: {}", e);
                        return Poll::Ready(Ok(()));
                    }
                },
                e => {
                    info!("Non-retriable error: {}", e);
                    return Poll::Ready(Ok(()))
                },
            }
        }
        
        self.producer.begin_transaction().expect("Failed to begin transaction");

        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.producer.commit_transaction(Duration::from_secs(1)).expect("Failed to commit transaction");

        Poll::Ready(Ok(()))
    }
}

impl <K, V, I> Sink<I> for PSink<K, V, AtLeastOnce>
where I: IntoRecordParts
{
    type Error = PeridotAppRuntimeError;

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: I,
    ) -> Result<(), Self::Error> {
    
        let parts = item
            .into_record_parts();

        let record = parts
            .into_record(&self.topic);
        // Add delivery callback

        match self.producer.send(record) {
            Ok(_) => Ok(()),
            Err(e) => Err(PeridotAppRuntimeError::from(e.0)),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.producer.flush(Duration::from_secs(1)).expect("Failed to flush producer");

        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.producer.flush(Duration::from_secs(1)).expect("Failed to flush producer");

        Poll::Ready(Ok(()))
    }
}
