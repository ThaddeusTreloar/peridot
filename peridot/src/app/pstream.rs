use std::sync::Arc;

use tracing::error;
use crossbeam::atomic::AtomicCell;
use futures::{Stream, StreamExt, stream::empty};
use rdkafka::{message::{BorrowedMessage, OwnedMessage}, consumer::{stream_consumer::StreamPartitionQueue, ConsumerContext}, error::KafkaError};
use tokio::sync::broadcast::Receiver;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::state::backend::{CommitLogs, CommitLog};

use super::{app_engine::EngineState, extensions::Commit, PeridotPartitionQueue};

pub trait PeridotStream<K, V> {}

pub struct PStream {
    output_stream: UnboundedReceiverStream<OwnedMessage>,
    engine_state: Arc<AtomicCell<EngineState>>,
    commit_logs: Arc<CommitLog>,
    commit_waker: Arc<Receiver<Commit>>,
}

impl PStream {
    pub fn new(topic: String,
        commit_logs: Arc<CommitLog>,
        engine_state: Arc<AtomicCell<EngineState>>,
        commit_waker: Receiver<Commit>,
        queue_receiver: tokio::sync::mpsc::Receiver<PeridotPartitionQueue>,
    ) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

        Self::start_forwarding_thread(sender, queue_receiver);

        PStream {
            output_stream: UnboundedReceiverStream::new(receiver),
            engine_state,
            commit_logs,
            commit_waker: Arc::new(commit_waker),
        }
    }

    pub fn start_forwarding_thread(
        sender: tokio::sync::mpsc::UnboundedSender<OwnedMessage>,
        mut queue_receiver: tokio::sync::mpsc::Receiver<PeridotPartitionQueue>,
    ) {
        tokio::spawn(async move {
            let sender = sender;

            while let Some(queue) = queue_receiver.recv().await {
                let new_sender = sender.clone();

                tokio::spawn(async move {
                    while let msg = queue.recv().await {
                        match msg {
                            Ok(msg) => {
                                match new_sender.send(msg.detach()) {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("Error sending message: {}", e);
                                        break;
                                    }
                                };
                            }
                            Err(e) => {
                                error!("Error receiving message: {}", e);
                                break;
                            }
                        }
                    }
                });
            }
        });
    }

    pub fn stream<M>(self) -> impl Stream<Item = M>
    where M: From<OwnedMessage>
    {
        self.output_stream.map(From::<OwnedMessage>::from)
    }
}

impl<K, V> PeridotStream<K, V> for PStream {}