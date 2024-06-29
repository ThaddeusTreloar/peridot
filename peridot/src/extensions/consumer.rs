use std::{sync::Arc, time::Duration};

use rdkafka::{
    consumer::{base_consumer::PartitionQueue, ConsumerContext},
    error::KafkaResult,
    message::{BorrowedMessage, OwnedMessage},
};
use tokio::task::spawn_blocking;

pub trait AsyncConsumer {
    async fn async_poll(self: Arc<Self>) -> Option<KafkaResult<OwnedMessage>>;
}

impl<C> AsyncConsumer for PartitionQueue<C>
where
    C: ConsumerContext + 'static,
{
    async fn async_poll(self: Arc<Self>) -> Option<KafkaResult<OwnedMessage>> {
        let self_ref = self.clone();

        spawn_blocking(move || match self_ref.poll(Duration::from_millis(2500))? {
            Ok(msg) => Some(Ok(msg.detach())),
            Err(e) => Some(Err(e)),
        })
        .await
        .expect("Failed to spawn blocking")
    }
}
