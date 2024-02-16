use std::{sync::Arc, marker::PhantomData};

use tracing::{error, info};
use crossbeam::atomic::AtomicCell;
use futures::{Stream, StreamExt, stream::empty};
use rdkafka::{message::{BorrowedMessage, OwnedMessage}, consumer::{stream_consumer::StreamPartitionQueue, ConsumerContext}, error::KafkaError};
use tokio::sync::broadcast::Receiver;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::{
    engine::{EngineState, util::{AtMostOnce, AtLeastOnce, ExactlyOnce}, QueueForwarder, QueueMetadataProtoype, RawQueueReceiver, Queue},
    state::backend::CommitLog, pipeline::{serde_ext::PDeserialize, pipeline::stream::stream::Pipeline},
};

use super::extensions::Commit;

pub struct PStream {
    // output_stream: UnboundedReceiverStream<OwnedMessage>,
    // engine_state: Arc<AtomicCell<EngineState>>,
    // commit_logs: Arc<CommitLog>,
    consumer_metadata: Arc<QueueMetadataProtoype>,
    commit_waker: Arc<Receiver<Commit>>,
}

async fn forwarding_thread(
    prototype_metadata: Arc<QueueMetadataProtoype>,
    mut reciever: RawQueueReceiver, 
    forwarder: QueueForwarder
) {
    while let Some((partition, queue)) = reciever.recv().await {
        let queue_metadata = prototype_metadata.map(partition);

        info!("Recieved new queue item for topic: {}, partition: {}", queue_metadata.source_topic(), queue_metadata.partition());

        forwarder.send((queue_metadata, queue))
            .expect("Failed to send queue item");
    }
}

impl PStream {
    pub fn new<KS, VS, G>(
        queue_metadata_prototype: QueueMetadataProtoype,
        raw_queue_receiver: RawQueueReceiver,
    ) -> Pipeline<KS, VS, G>
    where
        KS: PDeserialize,
        VS: PDeserialize,
    {
        let qmp_ref = Arc::new(queue_metadata_prototype);
        let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(
            forwarding_thread(
                qmp_ref,
                raw_queue_receiver,
                queue_sender,
        ));

        Pipeline::new(queue_receiver)
    }
}