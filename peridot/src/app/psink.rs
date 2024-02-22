use std::marker::PhantomData;

use crate::{
    engine::util::{AtLeastOnce, ExactlyOnce},
    pipeline::{
        message::stream::{MessageStream, PipelineStage},
        serde_ext::PSerialize,
    },
};

use super::config::PeridotConfig;

#[derive(Debug, Clone, Default)]
pub enum SinkMode {
    #[default]
    Attached,
    Detached,
}

pub struct PSinkBuilder<G = AtLeastOnce> {
    clients_config: PeridotConfig,
    dest_topic: String,
    _delivery_guarantee: PhantomData<G>,
}

impl<G> PSinkBuilder<G> {
    pub fn new(dest_topic: String, clients_config: PeridotConfig) -> Self {
        Self {
            clients_config,
            dest_topic,
            _delivery_guarantee: PhantomData,
        }
    }
}

pub struct PSink<KS, VS, M, G = ExactlyOnce>
where
    KS: PSerialize,
    VS: PSerialize,
    M: MessageStream,
{
    pipeline: PipelineStage<M>,
    _key_ser_type: PhantomData<KS>,
    _value_ser_type: PhantomData<VS>,
    _delivery_guarantee: PhantomData<G>,
}

impl<KS, VS, M, G> PSink<KS, VS, M, G>
where
    KS: PSerialize,
    VS: PSerialize,
    M: MessageStream,
    G: 'static,
{
    pub fn new(pipeline: PipelineStage<M>) -> Self {
        Self {
            pipeline,
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
    RecordCreationError(String),
    #[error("Kafka runtime error: {0}")]
    KafkaRuntimeError(#[from] rdkafka::error::KafkaError),
    #[error("Add partition offset error: {0}")]
    AddPartitionOffsetError(String),
}
/*
impl<K, V, I> Sink<I> for PSink<K, V, ExactlyOnce>
where
    I: TryIntoRecordParts<K, V> + MessageContext,
    K: PSerialize,
    V: PSerialize,
{
    type Error = PSinkError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        info!("Starting transaction");
        match self.producer.begin_transaction() {
            Ok(_) => (),
            Err(KafkaError::Transaction(e)) => {
                info!("Failed to start transaction: {}, {}", e.code(), e);
                return Poll::Ready(Ok(()));
            }
            Err(e) => {
                info!("Failed to start transaction: {}", e);
                return Poll::Ready(Ok(()));
            }
        };

        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let topic = item.consumer_topic();
        let partition = item.consumer_partition();
        let offset = item.consumer_offset();

        let parts = item.try_into_record_parts()?;

        let record = parts.into_record(&self.topic);

        info!("Sending message: {:?}", record);

        let group_meta = self
            .consumer_ref
            .group_metadata()
            .expect("No group metadata");

        let mut tpl = TopicPartitionList::new();

        tpl.add_partition_offset(&topic, partition, rdkafka::Offset::Offset(offset))
            .map_err(|e| PSinkError::AddPartitionOffsetError(e.to_string()))?;

        match self.producer.send(record) {
            Ok(_) => {
                info!(
                    "Sending message offsets for transaction: topic: {}, partition: {}, offset: {}",
                    topic, partition, offset
                );

                self.producer
                    .send_offsets_to_transaction(&tpl, &group_meta, Duration::from_millis(1000))
                    .expect("Failed to commit offsets to transaction");
                Ok(())
            }
            Err(e) => {
                // Todo: Dead letter queue
                Err(PSinkError::from(e.0))
            }
        }
    }

    // TODO: guarentee transaction.timeout.ms
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        info!("Commiting transaction");

        if self.producer.in_flight_count() < 1 {
            info!("No in-flight messages, skipping transaction commit");
            return Poll::Ready(Ok(()));
        }

        while let Err(e) = self
            .producer
            .commit_transaction(Duration::from_millis(1000))
        {
            match e {
                KafkaError::Transaction(e) => {
                    if e.is_retriable() {
                        info!("Retrying transaction commit: {}", e);
                    } else {
                        info!("Non-retriable error: {}", e);
                        return Poll::Ready(Ok(()));
                    }
                }
                e => {
                    info!("Non-retriable error: {}", e);
                    return Poll::Ready(Ok(()));
                }
            }
        }

        info!("Transaction committed!");

        self.producer
            .begin_transaction()
            .expect("Failed to begin transaction");

        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.producer
            .commit_transaction(Duration::from_secs(1))
            .expect("Failed to commit transaction");

        Poll::Ready(Ok(()))
    }
}

impl<K, V, I> Sink<I> for PSink<K, V, AtLeastOnce>
where
    I: IntoRecordParts,
{
    type Error = PeridotAppRuntimeError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let parts = item.into_record_parts();

        let record = parts.into_record(&self.topic);
        // Add delivery callback

        match self.producer.send(record) {
            Ok(_) => Ok(()),
            Err(e) => Err(PeridotAppRuntimeError::from(e.0)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.producer
            .flush(Duration::from_secs(1))
            .expect("Failed to flush producer");

        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.producer
            .flush(Duration::from_secs(1))
            .expect("Failed to flush producer");

        Poll::Ready(Ok(()))
    }
}
 */
