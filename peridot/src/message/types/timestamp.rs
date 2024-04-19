
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum PeridotTimestamp {
    NotAvailable,
    CreateTime(i64),
    IngestionTime(i64),
    ConsumptionTime(i64),
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

impl Into<Option<i64>> for PeridotTimestamp {
    fn into(self) -> Option<i64> {
        match self {
            PeridotTimestamp::NotAvailable => None,
            PeridotTimestamp::CreateTime(ts) => Some(ts),
            PeridotTimestamp::IngestionTime(ts) => Some(ts),
            PeridotTimestamp::ConsumptionTime(ts) => Some(ts),
        }
    }
}

impl Into<Option<i64>> for &PeridotTimestamp {
    fn into(self) -> Option<i64> {
        match self {
            PeridotTimestamp::NotAvailable => None,
            PeridotTimestamp::CreateTime(ts) => Some(*ts),
            PeridotTimestamp::IngestionTime(ts) => Some(*ts),
            PeridotTimestamp::ConsumptionTime(ts) => Some(*ts),
        }
    }
}