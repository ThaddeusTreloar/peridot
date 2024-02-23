use std::{pin::Pin, task::{Context, Poll}};

use rdkafka::{config::FromClientConfig, producer::{BaseProducer, Producer}};

use crate::{engine::QueueMetadata, message::sink::MessageSink, serde_ext::PSerialize};

use super::MessageSinkFactory;

pub struct ChangelogSinkFactory<KS, KV> {
    changelog_topic: String,
    _key_ser_type: std::marker::PhantomData<KS>,
    _value_ser_type: std::marker::PhantomData<KV>,
}

impl <KS, VS> ChangelogSinkFactory<KS, VS> {
    pub fn new(changelog_topic: String) -> Self {
        Self { 
            changelog_topic,
            _key_ser_type: std::marker::PhantomData,
            _value_ser_type: std::marker::PhantomData, 
        }
    }
}

impl<KS, VS> MessageSinkFactory for ChangelogSinkFactory<KS, VS> 
where
    KS: PSerialize,
    VS: PSerialize,
{
    type SinkType = ChangelogSink<KS, VS>;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType {
        ChangelogSink::<KS, VS>::from_queue_metadata(queue_metadata, self.changelog_topic.clone())
    }
}

pub struct ChangelogSink<KS, VS> {
    changelog_topic: String,
    consumer_metadata: QueueMetadata,
    producer: BaseProducer,
    _key_ser_type: std::marker::PhantomData<KS>,
    _value_ser_type: std::marker::PhantomData<VS>,
}

impl<KS, VS> ChangelogSink<KS, VS> {
    pub fn from_queue_metadata(queue_metadata: QueueMetadata, changelog_topic: String) -> Self {
        let producer = BaseProducer::from_config(queue_metadata.client_config()).expect("Failed to build producer for changelog sink.");
        Self {
            changelog_topic,
            consumer_metadata: queue_metadata,
            producer,
            _key_ser_type: std::marker::PhantomData,
            _value_ser_type: std::marker::PhantomData,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChangelogSinkError {}

impl<KS, VS> MessageSink for ChangelogSink<KS, VS> 
where
    KS: PSerialize,
    VS: PSerialize,
{
    type Error = ChangelogSinkError;
    type KeySerType = KS;
    type ValueSerType = VS;

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))   
    }

    fn poll_commit(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))   
    }

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))   
    }

    fn start_send(
            self: Pin<&mut Self>,
            message: &crate::message::types::Message<
                <Self::KeySerType as crate::serde_ext::PSerialize>::Input,
                <Self::ValueSerType as crate::serde_ext::PSerialize>::Input,
            >,
        ) -> Result<(), Self::Error> {
            Ok(())
    }
}