use std::{sync::Arc, marker::PhantomData};

use tracing::error;
use crossbeam::atomic::AtomicCell;
use futures::{Stream, StreamExt, stream::empty};
use rdkafka::{message::{BorrowedMessage, OwnedMessage}, consumer::{stream_consumer::StreamPartitionQueue, ConsumerContext}, error::KafkaError};
use tokio::sync::broadcast::Receiver;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::state::backend::CommitLog;

use super::{app_engine::{EngineState, util::{AtMostOnce, AtLeastOnce, ExactlyOnce}}, extensions::Commit, PeridotPartitionQueue, wrappers::{MessageContext, FromOwnedMessage}};

pub struct PStream<G = ExactlyOnce> {
    output_stream: UnboundedReceiverStream<OwnedMessage>,
    engine_state: Arc<AtomicCell<EngineState>>,
    commit_logs: Arc<CommitLog>,
    commit_waker: Arc<Receiver<Commit>>,
    phantom_data: PhantomData<G>,
}

impl <G> PStream<G> {
    pub fn new(topic: String,
        commit_logs: Arc<CommitLog>,
        engine_state: Arc<AtomicCell<EngineState>>,
        commit_waker: Receiver<Commit>,
        queue_receiver: tokio::sync::mpsc::Receiver<PeridotPartitionQueue>,
    ) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

        Self::start_forwarding_thread(sender, queue_receiver);

        Self {
            output_stream: UnboundedReceiverStream::new(receiver),
            engine_state,
            commit_logs,
            commit_waker: Arc::new(commit_waker),
            phantom_data: PhantomData,
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
}

impl PStream<AtMostOnce> {
    pub fn stream<M>(self) -> impl Stream<Item = M>
    where M: From<OwnedMessage>
    {
        self.output_stream.map(From::<OwnedMessage>::from)
    }
}

impl PStream<AtLeastOnce> {
    pub fn stream<M>(self) -> impl Stream<Item = M>
    where M: From<OwnedMessage>
    {
        self.output_stream.map(From::<OwnedMessage>::from)
    }
}

impl PStream<ExactlyOnce> {
    pub fn stream<M, S>(self) -> impl Stream<Item = M>
    where M: FromOwnedMessage<S> + MessageContext
    {
        self.output_stream.map(FromOwnedMessage::from_owned_message)
    }
}