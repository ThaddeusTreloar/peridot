use std::{fmt::Display, sync::Arc};

use rdkafka::{ClientContext, topic_partition_list::{self, TopicPartitionListElem}, error::KafkaError, consumer::ConsumerContext};
use tokio::sync::broadcast::{
    channel,
    Sender,
    Receiver
};
use tracing::error;


#[derive(Debug, Clone)]
pub struct Commit {
    topic: String,
    partition: i32,
    offset: i64,
}

impl Commit {
    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }
}

impl <'a> From<TopicPartitionListElem<'a>> for Commit {
    fn from(elem: TopicPartitionListElem<'a>) -> Self {
        Commit {
            topic: elem.topic().to_string(),
            partition: elem.partition(),
            offset: match elem.offset() {
                rdkafka::Offset::Offset(offset) => offset,
                _ => 0
            }
        }
    }
}

#[derive(Debug)]
pub struct ContextWakers {
    pub pre_rebalance_waker: Receiver<OwnedRebalance>,
    pub post_rebalance_waker: Receiver<OwnedRebalance>,
    pub commit_waker: Receiver<Commit>,
}

impl ContextWakers {
    pub fn new(
        pre_rebalance_waker: Receiver<OwnedRebalance>,
        post_rebalance_waker: Receiver<OwnedRebalance>,
        commit_waker: Receiver<Commit>,
    ) -> Self {
        ContextWakers {
            pre_rebalance_waker,
            post_rebalance_waker,
            commit_waker,
        }
    }
}

#[derive(Debug)]
pub struct PeridotConsumerContext {
    pre_rebalance_waker: Arc<Sender<OwnedRebalance>>,
    post_rebalance_waker: Arc<Sender<OwnedRebalance>>,
    commit_waker: Arc<Sender<Commit>>,
}

impl Clone for PeridotConsumerContext {
    fn clone(&self) -> Self {
        PeridotConsumerContext {
            pre_rebalance_waker: self.pre_rebalance_waker.clone(),
            post_rebalance_waker: self.post_rebalance_waker.clone(),
            commit_waker: self.commit_waker.clone(),
        }
    }
}

impl ClientContext for PeridotConsumerContext {}

impl PeridotConsumerContext {
    pub fn new() -> Self {
        let (pre_rebalance_waker, pre_rebalance_receiver) = channel(100);
        let (post_rebalance_waker, post_rebalance_receiver) = channel(100);
        let (commit_waker, commit_receiver) = channel(100);

        PeridotConsumerContext {
            pre_rebalance_waker: Arc::new(pre_rebalance_waker),
            post_rebalance_waker: Arc::new(post_rebalance_waker),
            commit_waker: Arc::new(commit_waker),
        }
    }

    pub fn wakers(&self) -> ContextWakers {
        ContextWakers {
            pre_rebalance_waker: self.pre_rebalance_waker.subscribe(),
            post_rebalance_waker: self.post_rebalance_waker.subscribe(),
            commit_waker: self.commit_waker.subscribe(),
        }
    }
}

#[derive(Debug, Clone)]
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

impl ConsumerContext for PeridotConsumerContext {
    fn pre_rebalance<'a>(&self, rebalance: &rdkafka::consumer::Rebalance<'_>) {
        let owned_rebalance: OwnedRebalance = rebalance.into();

        self.pre_rebalance_waker.send(owned_rebalance).expect("Failed to send rebalance");
    }

    fn post_rebalance<'a>(&self, rebalance: &rdkafka::consumer::Rebalance<'_>) {
        let owned_rebalance: OwnedRebalance = rebalance.into();

        self.post_rebalance_waker.send(owned_rebalance).expect("Failed to send rebalance");
    }

    fn commit_callback(&self, result: rdkafka::error::KafkaResult<()>, offsets: &rdkafka::TopicPartitionList) {
        match result {
            Ok(_) => {
                for offset in offsets.elements() {
                    self.commit_waker
                        .send(Commit::from(offset))
                        .expect("Failed to send commit");
                }
            },
            Err(e) => {
                error!("Failed to commit offsets: {}", e);
            }
        }
    }
}