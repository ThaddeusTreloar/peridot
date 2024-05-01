
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub enum PeridotTimestamp {
    #[default]
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