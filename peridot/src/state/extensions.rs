use rdkafka::{ClientContext, topic_partition_list, error::KafkaError, consumer::ConsumerContext};
use tokio::sync::mpsc::Sender;

#[derive(Debug)]
pub struct StateStoreContext {
    pre_rebalance_waker: tokio::sync::mpsc::Sender<OwnedRebalance>,
    post_rebalance_waker: tokio::sync::mpsc::Sender<OwnedRebalance>,
}

impl ClientContext for StateStoreContext {}

impl StateStoreContext {
    pub fn new(pre: Sender<OwnedRebalance>, post: Sender<OwnedRebalance>) -> Self {
        StateStoreContext {
            pre_rebalance_waker: pre,
            post_rebalance_waker: post,
        }
    }
}

pub enum OwnedRebalance {
    Assign(topic_partition_list::TopicPartitionList),
    Revoke(topic_partition_list::TopicPartitionList),
    Error(KafkaError)
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
}