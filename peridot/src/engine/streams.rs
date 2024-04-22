use std::sync::Arc;

use tracing::info;

use crate::{
    engine::wrapper::serde::PeridotDeserializer,
    engine::{QueueForwarder, QueueMetadataFactory, RawQueueReceiver},
    pipeline::stream::serialiser::SerialiserPipeline,
};

async fn forwarding_thread<B>(
    prototype_metadata: Arc<QueueMetadataFactory<B>>,
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
    queue_metadata_prototype: QueueMetadataFactory<B>,
    raw_queue_receiver: RawQueueReceiver,
) -> SerialiserPipeline<KS, VS, G>
where
    KS: PeridotDeserializer,
    VS: PeridotDeserializer,
    B: Send + Sync + 'static,
{
    let qmp_ref = Arc::new(queue_metadata_prototype);
    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(forwarding_thread(qmp_ref, raw_queue_receiver, queue_sender));

    SerialiserPipeline::new(queue_receiver)
}
