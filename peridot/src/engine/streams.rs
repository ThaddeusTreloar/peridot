use std::sync::Arc;

use tokio::sync::broadcast::Receiver;
use tracing::info;

use crate::{
    engine::{QueueForwarder, QueueMetadataProtoype, RawQueueReceiver},
    pipeline::stream::serialiser::SerialiserPipeline,
    serde_ext::PDeserialize,
};

async fn forwarding_thread<B>(
    prototype_metadata: Arc<QueueMetadataProtoype<B>>,
    mut reciever: RawQueueReceiver,
    forwarder: QueueForwarder,
) where
    B: Send + Sync + 'static,
{
    while let Some((partition, queue)) = reciever.recv().await {
        let queue_metadata = prototype_metadata.create_queue_metadata(partition);

        info!(
            "Recieved new queue item for topic: {}, partition: {}",
            queue_metadata.source_topic(),
            queue_metadata.partition()
        );

        forwarder
            .send((queue_metadata, queue))
            .expect("Failed to send queue item");
    }
}

pub fn new_stream<KS, VS, B, G>(
    queue_metadata_prototype: QueueMetadataProtoype<B>,
    raw_queue_receiver: RawQueueReceiver,
) -> SerialiserPipeline<KS, VS, G>
where
    KS: PDeserialize,
    VS: PDeserialize,
    B: Send + Sync + 'static,
{
    let qmp_ref = Arc::new(queue_metadata_prototype);
    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(forwarding_thread(qmp_ref, raw_queue_receiver, queue_sender));

    SerialiserPipeline::new(queue_receiver)
}
