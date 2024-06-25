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

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub enum PeridotTimestamp {
    #[default]
    NotAvailable,
    CreateTime(i64),
    IngestionTime(i64),
    ConsumptionTime(i64),
}

impl From<&PeridotTimestamp> for i64 {
    fn from(value: &PeridotTimestamp) -> Self {
        match value {
            PeridotTimestamp::NotAvailable => 0,
            PeridotTimestamp::CreateTime(ts) => *ts,
            PeridotTimestamp::IngestionTime(ts) => *ts,
            PeridotTimestamp::ConsumptionTime(ts) => *ts,
        }
    }
}

impl From<PeridotTimestamp> for i64 {
    fn from(value: PeridotTimestamp) -> Self {
        match value {
            PeridotTimestamp::NotAvailable => 0,
            PeridotTimestamp::CreateTime(ts) => ts,
            PeridotTimestamp::IngestionTime(ts) => ts,
            PeridotTimestamp::ConsumptionTime(ts) => ts,
        }
    }
}

impl From<i64> for PeridotTimestamp {
    fn from(ts: i64) -> Self {
        Self::ConsumptionTime(ts)
    }
}

impl From<rdkafka::message::Timestamp> for PeridotTimestamp {
    fn from(ts: rdkafka::message::Timestamp) -> Self {
        match ts {
            rdkafka::message::Timestamp::CreateTime(ts) => Self::CreateTime(ts),
            rdkafka::message::Timestamp::LogAppendTime(ts) => Self::IngestionTime(ts),
            rdkafka::message::Timestamp::NotAvailable => Self::NotAvailable,
        }
    }
}

impl From<PeridotTimestamp> for Option<i64> {
    fn from(value: PeridotTimestamp) -> Self {
        match value {
            PeridotTimestamp::NotAvailable => None,
            PeridotTimestamp::CreateTime(ts) => Some(ts),
            PeridotTimestamp::IngestionTime(ts) => Some(ts),
            PeridotTimestamp::ConsumptionTime(ts) => Some(ts),
        }
    }
}

impl From<&PeridotTimestamp> for Option<i64> {
    fn from(value: &PeridotTimestamp) -> Self {
        match value {
            PeridotTimestamp::NotAvailable => None,
            PeridotTimestamp::CreateTime(ts) => Some(*ts),
            PeridotTimestamp::IngestionTime(ts) => Some(*ts),
            PeridotTimestamp::ConsumptionTime(ts) => Some(*ts),
        }
    }
}
