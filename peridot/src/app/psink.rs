use std::{sync::Arc, pin::Pin, task::{Context, Poll}, time::Duration};

use futures::{Stream, stream::empty, StreamExt, Sink};
use rdkafka::{
    consumer::{stream_consumer::StreamPartitionQueue, StreamConsumer},
    ClientConfig, message::BorrowedMessage, producer::{FutureProducer, BaseProducer},
};
use tracing::info;

use crate::{
    app::extensions::PeridotConsumerContext,
    state::{
        backend::{
            persistent::PersistentStateBackend, ReadableStateBackend, StateBackend,
            WriteableStateBackend,
        },
        StateStore,
    }, stream::{types::{KeyValue, IntoRecordParts}, self},
};

use super::{
    app_engine::{AppEngine},
    error::{PeridotAppCreationError, PeridotAppRuntimeError},
};


pub struct PSink {
    // Maybe swap to threaded producer
    producer: Arc<BaseProducer>,
    topic: String,
}

impl PSink {
    pub fn new(
        producer: BaseProducer,
        topic: String,
    ) -> Self {
        PSink {
            producer: Arc::new(producer),
            topic,
        }
    }
}

impl <I> Sink<I> for PSink 
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
        self.producer.poll(Duration::from_secs(0));
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.producer.poll(Duration::from_secs(1));

        Poll::Ready(Ok(()))
    }
}
