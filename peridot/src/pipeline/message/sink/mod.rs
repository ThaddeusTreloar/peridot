use std::{
    fmt::Display,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll}, error::Error,
};

use pin_project_lite::pin_project;
use tracing::info;

use crate::{engine::QueueMetadata, pipeline::serde_ext::PSerialize};

use super::types::Message;

pub trait MessageSink<KS, VS>
where
    KS: PSerialize,
    VS: PSerialize,
{
    type Error: Error;

    fn from_queue_metadata(queue_metadata: QueueMetadata) -> Self
    where
        Self: Sized;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), Self::Error>>>;
    fn start_send(
        self: Pin<&mut Self>,
        message: Message<KS::Input, VS::Input>,
    ) -> Result<(), Self::Error>;
    fn poll_commit(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), Self::Error>>>;
    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), Self::Error>>>;
}

pub trait MessageSinkExt<KS, VS>: MessageSink<KS, VS>
where
    KS: PSerialize,
    VS: PSerialize,
{
    fn sink(self) -> ()
    where
        Self: Sized,
    {
        ()
    }
}

impl<P, KS, VS> MessageSinkExt<KS, VS> for P
where
    P: MessageSink<KS, VS>,
    KS: PSerialize,
    VS: PSerialize,
{
}

pin_project! {
    pub struct PrintSink<KS, VS>
    where
        KS: PSerialize,
        VS: PSerialize,
    {
        queue_metadata: Arc<QueueMetadata>,
        _key_serialiser_type: PhantomData<KS>,
        _value_serialiser_type: PhantomData<VS>,
    }
}

impl<KS, VS> PrintSink<KS, VS>
where
    KS: PSerialize,
    VS: PSerialize,
{
    pub fn new(queue_metadata: QueueMetadata) -> Self {
        Self {
            queue_metadata: Arc::new(queue_metadata),
            _key_serialiser_type: PhantomData,
            _value_serialiser_type: PhantomData,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PrintSinkError {}

impl<KS, VS> MessageSink<KS, VS> for PrintSink<KS, VS>
where
    KS: PSerialize,
    VS: PSerialize,
    KS::Input: Display,
    VS::Input: Display,
{
    type Error = PrintSinkError;

    fn from_queue_metadata(queue_metadata: QueueMetadata) -> Self {
        Self::new(queue_metadata)
    }

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), Self::Error>>> {
        Poll::Ready(Some(Ok(())))
    }

    fn start_send(
        self: Pin<&mut Self>,
        message: Message<KS::Input, VS::Input>,
    ) -> Result<(), Self::Error> {
        
        let ser_key = KS::serialize(message.key()).expect("Failed to serialise key.");
        let ser_value = VS::serialize(message.value()).expect("Failed to serialise value.");
    
        info!("Debug Sink: Sending message: {}", message);
        info!("Debug Sink: Serialised message: {{ key: {:?}, value: {:?} }}", ser_key, ser_value);
        info!(
            "Debug Sink: Queue metadata: {{ partition: {}, source_topic: {} }}",
            self.queue_metadata.partition(),
            self.queue_metadata.source_topic()
        );

        Ok(())
    }

    fn poll_commit(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), Self::Error>>> {
        Poll::Ready(Some(Ok(())))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), Self::Error>>> {
        Poll::Ready(Some(Ok(())))
    }
}
