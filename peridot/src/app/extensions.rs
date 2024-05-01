use std::{fmt::Display, time::Duration};

use rdkafka::{
    consumer::ConsumerContext,
    error::KafkaError,
    topic_partition_list::{self, TopicPartitionListElem},
    util::Timeout,
    ClientContext,
};
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tracing::error;

use super::config::PeridotConfig;

#[derive(Debug, Clone)]
pub struct Commit {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

impl Display for Commit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Commit: {}-{}:{}",
            self.topic, self.partition, self.offset
        )
    }
}

impl Commit {
    pub fn new(topic: String, partition: i32, offset: i64) -> Self {
        Self {
            topic,
            partition,
            offset,
        }
    }

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

impl<'a> From<TopicPartitionListElem<'a>> for Commit {
    fn from(elem: TopicPartitionListElem<'a>) -> Self {
        Commit {
            topic: elem.topic().to_string(),
            partition: elem.partition(),
            offset: match elem.offset() {
                rdkafka::Offset::Offset(offset) => offset,
                _ => 0,
            },
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
    pre_rebalance_waker: Sender<OwnedRebalance>,
    post_rebalance_waker: Sender<OwnedRebalance>,
    commit_waker: Sender<Commit>,
    max_poll_interval: Timeout,
}

impl Clone for PeridotConsumerContext {
    fn clone(&self) -> Self {
        PeridotConsumerContext {
            pre_rebalance_waker: self.pre_rebalance_waker.clone(),
            post_rebalance_waker: self.post_rebalance_waker.clone(),
            commit_waker: self.commit_waker.clone(),
            max_poll_interval: self.max_poll_interval,
        }
    }
}

impl ClientContext for PeridotConsumerContext {}

impl Default for PeridotConsumerContext {
    fn default() -> Self {
        let (pre_rebalance_waker, _) = channel(1024);
        let (post_rebalance_waker, _) = channel(1024);
        let (commit_waker, _) = channel(1024);

        PeridotConsumerContext {
            pre_rebalance_waker,
            post_rebalance_waker,
            commit_waker, // The default max_poll_interval is 5 minutes
            max_poll_interval: Timeout::After(Duration::from_millis(300000)),
        }
    }
}

impl PeridotConsumerContext {
    pub fn from_config(config: &PeridotConfig) -> Self {
        let mut this = Self::default();

        if let Some(interval) = config.get("max.poll.interval.ms") {
            this.max_poll_interval = Timeout::After(Duration::from_millis(
                interval.parse().expect("Invalid max.poll.interval.ms"),
            ));
        }

        this
    }

    pub fn wakers(&self) -> ContextWakers {
        ContextWakers {
            pre_rebalance_waker: self.pre_rebalance_waker.subscribe(),
            post_rebalance_waker: self.post_rebalance_waker.subscribe(),
            commit_waker: self.commit_waker.subscribe(),
        }
    }

    pub fn pre_rebalance_waker(&self) -> Receiver<OwnedRebalance> {
        self.pre_rebalance_waker.subscribe()
    }

    pub fn post_rebalance_waker(&self) -> Receiver<OwnedRebalance> {
        self.post_rebalance_waker.subscribe()
    }

    pub fn commit_waker(&self) -> Receiver<Commit> {
        self.commit_waker.subscribe()
    }

    pub fn pre_rebalance_sender(&self) -> Sender<OwnedRebalance> {
        self.pre_rebalance_waker.clone()
    }

    pub fn post_rebalance_sender(&self) -> Sender<OwnedRebalance> {
        self.post_rebalance_waker.clone()
    }

    pub fn commit_sender(&self) -> Sender<Commit> {
        self.commit_waker.clone()
    }
}

#[derive(Debug, Clone)]
pub enum OwnedRebalance {
    Assign(topic_partition_list::TopicPartitionList),
    Revoke(topic_partition_list::TopicPartitionList),
    Error(KafkaError),
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
            rdkafka::consumer::Rebalance::Assign(tp_list) => {
                OwnedRebalance::Assign(tp_list.clone())
            }
            rdkafka::consumer::Rebalance::Revoke(tp_list) => {
                OwnedRebalance::Revoke(tp_list.clone())
            }
            rdkafka::consumer::Rebalance::Error(e) => OwnedRebalance::Error(e),
        }
    }
}

impl ConsumerContext for PeridotConsumerContext {
    fn pre_rebalance<'a>(&self, rebalance: &rdkafka::consumer::Rebalance<'_>) {
        let owned_rebalance: OwnedRebalance = rebalance.into();

        self.pre_rebalance_waker
            .send(owned_rebalance)
            .expect("Failed to send rebalance");
    }

    fn post_rebalance<'a>(&self, rebalance: &rdkafka::consumer::Rebalance<'_>) {
        let owned_rebalance: OwnedRebalance = rebalance.into();

        self.post_rebalance_waker
            .send(owned_rebalance)
            .expect("Failed to send rebalance");
    }

    fn commit_callback(
        &self,
        result: rdkafka::error::KafkaResult<()>,
        offsets: &rdkafka::TopicPartitionList,
    ) {
        match result {
            Ok(_) => {
                for offset in offsets.elements() {
                    self.commit_waker
                        .send(Commit::from(offset))
                        .expect("Failed to send commit");
                }
            }
            Err(e) => {
                error!("Failed to commit offsets: {}", e);
            }
        }
    }

    fn main_queue_min_poll_interval(&self) -> Timeout {
        self.max_poll_interval
    }
}
