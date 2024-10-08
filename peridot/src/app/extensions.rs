/*
 * Copyright 2024 Thaddeus Treloar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

pub(crate) type RebalanceSender = Sender<OwnedRebalance>;
pub(crate) type RebalanceReceiver = Receiver<OwnedRebalance>;

use std::{collections::HashMap, fmt::Display, time::Duration};

use dashmap::DashMap;
use rdkafka::{
    consumer::ConsumerContext, error::KafkaError, statistics::Partition, topic_partition_list::{self, TopicPartitionListElem}, util::Timeout, ClientContext
};
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tracing::{debug, error, info};

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
    pub pre_rebalance_waker: RebalanceReceiver,
    pub post_rebalance_waker: RebalanceReceiver,
    pub commit_waker: Receiver<Commit>,
}

impl ContextWakers {
    pub fn new(
        pre_rebalance_waker: RebalanceReceiver,
        post_rebalance_waker: RebalanceReceiver,
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
    pre_rebalance_waker: RebalanceSender,
    post_rebalance_waker: RebalanceSender,
    commit_waker: Sender<Commit>,
    metadata: DashMap<String, DashMap<i32, Partition>>,
}

impl Clone for PeridotConsumerContext {
    fn clone(&self) -> Self {
        PeridotConsumerContext {
            pre_rebalance_waker: self.pre_rebalance_waker.clone(),
            post_rebalance_waker: self.post_rebalance_waker.clone(),
            commit_waker: self.commit_waker.clone(),
            metadata: Default::default(),
        }
    }
}

impl ClientContext for PeridotConsumerContext {
    fn stats(&self, statistics: rdkafka::Statistics) {
        statistics.topics.into_iter().for_each(|(topic, t_stats)| {
            t_stats
                .partitions
                .into_iter()
                .filter(|(p, _)| p >= &&0)
                .filter(|(_, stat)| stat.ls_offset >= 0)
                .for_each(|(p, p_stat)| {
                    self.set_partition_stats(&topic, p, p_stat);
                })
        });
    }

    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        let _ = log_message;
    }
}

impl Default for PeridotConsumerContext {
    fn default() -> Self {
        let (pre_rebalance_waker, _) = channel(1024);
        let (post_rebalance_waker, _) = channel(1024);
        let (commit_waker, _) = channel(1024);

        PeridotConsumerContext {
            pre_rebalance_waker,
            post_rebalance_waker,
            commit_waker,
            metadata: Default::default(),
        }
    }
}

impl PeridotConsumerContext {
    pub fn wakers(&self) -> ContextWakers {
        ContextWakers {
            pre_rebalance_waker: self.pre_rebalance_waker.subscribe(),
            post_rebalance_waker: self.post_rebalance_waker.subscribe(),
            commit_waker: self.commit_waker.subscribe(),
        }
    }

    pub fn pre_rebalance_waker(&self) -> RebalanceReceiver {
        self.pre_rebalance_waker.subscribe()
    }

    pub fn post_rebalance_waker(&self) -> RebalanceReceiver {
        self.post_rebalance_waker.subscribe()
    }

    pub fn commit_waker(&self) -> Receiver<Commit> {
        self.commit_waker.subscribe()
    }

    pub fn pre_rebalance_sender(&self) -> RebalanceSender {
        self.pre_rebalance_waker.clone()
    }

    pub fn post_rebalance_sender(&self) -> RebalanceSender {
        self.post_rebalance_waker.clone()
    }

    pub fn commit_sender(&self) -> Sender<Commit> {
        self.commit_waker.clone()
    }

    pub fn set_partition_stats(&self, topic: &str, partition: i32, stats: Partition) {
        match self.metadata.get_mut(topic) {
            None => {
                let inner = DashMap::new();
                inner.insert(partition, stats);

                self.metadata.insert(topic.to_owned(), inner);
            }
            Some(topic_map) => match topic_map.get_mut(&partition) {
                None => {
                    topic_map.insert(partition, stats);
                }
                Some(mut partition) => {
                    *partition.value_mut() = stats;
                }
            },
        }
    }

    pub fn get_next_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.metadata.get(topic)?.get(&partition).map(|stats| stats.next_offset)
    }

    pub fn get_lso(&self, topic: &str, partition: i32) -> Option<i64> {
        self.metadata.get(topic)?.get(&partition).map(|stats| stats.ls_offset)
    }

    pub fn get_eof(&self, topic: &str, partition: i32) -> Option<i64> {
        self.metadata.get(topic)?.get(&partition).map(|stats| stats.eof_offset)
    }

    pub fn get_fetch_count(&self, topic: &str, partition: i32) -> Option<i64> {
        self.metadata.get(topic)?.get(&partition).map(|stats| stats.fetchq_cnt)
    }

    pub fn get_app_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.metadata.get(topic)?.get(&partition).map(|stats| stats.app_offset)
    }

    pub fn get_query_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.metadata.get(topic)?.get(&partition).map(|stats| stats.query_offset)
    }

    pub fn get_rx(&self, topic: &str, partition: i32) -> Option<u64> {
        self.metadata.get(topic)?.get(&partition).map(|stats| stats.rxmsgs)
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

        debug!("Context: pre rebalance, {}", &owned_rebalance);

        let _ = self.pre_rebalance_waker.send(owned_rebalance);
    }

    fn post_rebalance<'a>(&self, rebalance: &rdkafka::consumer::Rebalance<'_>) {
        let owned_rebalance: OwnedRebalance = rebalance.into();

        debug!("Context: post rebalance, {}", &owned_rebalance);

        let _ = self.post_rebalance_waker.send(owned_rebalance);
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
}
