use std::{collections::VecDeque, default, future::IntoFuture, pin::Pin, sync::Arc, task::{ready, Context, Poll}, time::Duration};

use futures::{Future, FutureExt};
use pin_project_lite::pin_project;
use rdkafka::{config::FromClientConfig, error::KafkaError, producer::{future_producer::OwnedDeliveryResult, BaseProducer, DeliveryFuture, FutureProducer, FutureRecord, Producer}, util::Timeout};

use crate::{engine::QueueMetadata, message::{sink::MessageSink, types::Message}, serde_ext::PSerialize, state::backend::{self, ReadableStateBackend, WriteableStateBackend}};

use super::MessageSinkFactory;

pub struct StateSinkFactory<KS, KV, B> {
    backend: Arc<B>,
    _key_ser_type: std::marker::PhantomData<KS>,
    _value_ser_type: std::marker::PhantomData<KV>,
}

impl <KS, VS, B> StateSinkFactory<KS, VS, B> {
    pub fn from_backend_ref(backend: Arc<B>) -> Self {
        Self { 
            backend,
            _key_ser_type: std::marker::PhantomData,
            _value_ser_type: std::marker::PhantomData, 
        }
    }
}

impl<KS, VS, B> MessageSinkFactory for StateSinkFactory<KS, VS, B> 
where
    KS: PSerialize + 'static,
    VS: PSerialize + 'static,
    KS::Input: Clone,
    VS::Input: Clone,
    B: ReadableStateBackend + WriteableStateBackend<KS::Input, VS::Input> + Send + 'static

{
    type SinkType = StateSink<KS, VS, B>;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType {
        StateSink::<KS, VS, B>::from_queue_metadata_and_backend_ref(queue_metadata, self.backend.clone())
    }
}

pin_project! {
    pub struct StateSink<KS, VS, B> 
    where
        KS: PSerialize,
        VS: PSerialize,
    {
        queue_metadata: QueueMetadata,
        backend_ref: Arc<B>,
        pending_commit: Option<Pin<Box<dyn Future<Output=Option<Message<KS::Input, VS::Input>>>>>>,
        _key_ser_type: std::marker::PhantomData<KS>,
        _value_ser_type: std::marker::PhantomData<VS>,
    }
}


impl<KS, VS, B> StateSink<KS, VS, B> 
where
    KS: PSerialize,
    VS: PSerialize,
{
    pub fn from_queue_metadata_and_backend_ref(queue_metadata: QueueMetadata, backend_ref: Arc<B>) -> Self {
        Self {
            queue_metadata,
            backend_ref,
            pending_commit: None,
            _key_ser_type: Default::default(),
            _value_ser_type: Default::default(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StateSinkError {
    #[error("Failed to run: {0}")]
    EnqueueFailed(#[from] KafkaError)
}

impl<KS, VS, B> MessageSink for StateSink<KS, VS, B> 
where
    KS: PSerialize + 'static,
    VS: PSerialize + 'static,
    KS::Input: Clone,
    VS::Input: Clone,
    B: WriteableStateBackend<KS::Input, VS::Input> + 'static + Send
{
    type Error = StateSinkError;
    type KeySerType = KS;
    type ValueSerType = VS;

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_commit(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();

        Poll::Ready(Ok(()))
    }

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();

        match this.pending_commit {
            Some(task) => {
                match task.poll_unpin(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(_) => {
                        let _ = this.pending_commit.take();

                        Poll::Ready(Ok(()))
                    }
                }
            },
            None => Poll::Ready(Ok(()))
        }
    }

    fn start_send(
        self: Pin<&mut Self>,
        message: &Message<
            <Self::KeySerType as PSerialize>::Input,
            <Self::ValueSerType as PSerialize>::Input,
        >,
    ) -> Result<(), Self::Error> 
    {
        let this = self.project();

        let backend_ref_clone = this.backend_ref.clone();

        let commit = Box::pin(
            backend_ref_clone.commit_update(
                message.clone()
            )
        );

        // TODO: check there isn't a val waiting
        this.pending_commit.replace(commit);

        Ok(())
    }
}