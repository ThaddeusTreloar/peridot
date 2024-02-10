use std::fmt::Display;

use rdkafka::{ClientContext, topic_partition_list, error::KafkaError, consumer::ConsumerContext};
use tokio::sync::mpsc::{
    Sender,
    Receiver
};
use tracing::error;

use super::backend::StateStoreCommit;

#[derive(Debug)]
pub struct StateStoreContext {
    pre_rebalance_waker: Sender<OwnedRebalance>,
    post_rebalance_waker: Sender<OwnedRebalance>,
    commit_waker: Sender<StateStoreCommit>,
}

impl ClientContext for StateStoreContext {}

impl StateStoreContext {
    pub fn new() -> (Self, Receiver<OwnedRebalance>, Receiver<OwnedRebalance>, Receiver<StateStoreCommit>) {
        let (pre_rebalance_waker, pre_rebalance_receiver) = tokio::sync::mpsc::channel(100);
        let (post_rebalance_waker, post_rebalance_receiver) = tokio::sync::mpsc::channel(100);
        let (commit_waker, commit_receiver) = tokio::sync::mpsc::channel(100);

        (
            StateStoreContext {
                pre_rebalance_waker,
                post_rebalance_waker,
                commit_waker,
            },
            pre_rebalance_receiver,
            post_rebalance_receiver,
            commit_receiver
        )
    }
}

#[derive(Debug)]
pub enum OwnedRebalance {
    Assign(topic_partition_list::TopicPartitionList),
    Revoke(topic_partition_list::TopicPartitionList),
    Error(KafkaError)
}

impl Display for OwnedRebalance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OwnedRebalance::Assign(tp_list) => write!(f, "Assign: {:?}", tp_list),
            OwnedRebalance::Revoke(tp_list) => write!(f, "Revoke: {:?}", tp_list),
            OwnedRebalance::Error(e) => write!(f, "Error: {:?}", e),
        }
    }
}

impl From<&rdkafka::consumer::Rebalance<'_>> for OwnedRebalance {
    fn from(rebalance: &rdkafka::consumer::Rebalance<'_>) -> Self {
        match rebalance.clone() {
            rdkafka::consumer::Rebalance::Assign(tp_list) => OwnedRebalance::Assign(tp_list.clone()),
            rdkafka::consumer::Rebalance::Revoke(tp_list) => OwnedRebalance::Revoke(tp_list.clone()),
            rdkafka::consumer::Rebalance::Error(e) => OwnedRebalance::Error(e),
        }
    }
}

impl ConsumerContext for StateStoreContext {
    fn pre_rebalance<'a>(&self, rebalance: &rdkafka::consumer::Rebalance<'_>) {
        let owned_rebalance: OwnedRebalance = rebalance.into();

        self.pre_rebalance_waker.try_send(owned_rebalance).expect("Failed to send rebalance");
    }

    fn post_rebalance<'a>(&self, rebalance: &rdkafka::consumer::Rebalance<'_>) {
        let owned_rebalance: OwnedRebalance = rebalance.into();

        self.post_rebalance_waker.try_send(owned_rebalance).expect("Failed to send rebalance");
    }

    fn commit_callback(&self, result: rdkafka::error::KafkaResult<()>, offsets: &rdkafka::TopicPartitionList) {
        match result {
            Ok(_) => {
                for offset in offsets.elements() {
                    self.commit_waker
                        .try_send(StateStoreCommit::from(offset))
                        .expect("Failed to send commit");
                }
            },
            Err(e) => {
                error!("Failed to commit offsets: {}", e);
            }
        }
    }
}