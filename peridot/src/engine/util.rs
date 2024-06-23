use rdkafka::consumer::{Consumer, ConsumerContext};
use tracing::warn;

pub(crate) trait ConsumerUtils<C>: Consumer<C>
where
    C: ConsumerContext,
{
    fn get_subscribed_topics(&self) -> Vec<String> {
        let subscription = self.subscription().expect("Failed to get subscription.");

        if subscription.count() == 0 {
            Vec::new()
        } else {
            subscription
                .elements()
                .iter()
                .map(|t| t.topic().to_string())
                .collect::<Vec<String>>()
        }
    }

    fn is_subscribed_to(&self, topic: &str) -> bool {
        let subscription = self.subscription().expect("Failed to get subscription.");

        if subscription.count() == 0 {
            false
        } else {
            subscription
                .elements()
                .iter()
                .map(|t| t.topic())
                .any(|t| t == topic)
        }
    }
}

impl<T, C> ConsumerUtils<C> for T
where
    T: Consumer<C>,
    C: ConsumerContext,
{
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryGuarantee {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

impl<T> From<T> for DeliveryGuarantee
where
    T: Into<String>,
{
    fn from(s: T) -> Self {
        let s: String = s.into();

        match s.as_str() {
            "at-most-once" => DeliveryGuarantee::AtMostOnce,
            "at-least-once" => DeliveryGuarantee::AtLeastOnce,
            "exactly-once" => DeliveryGuarantee::ExactlyOnce,
            _ => {
                warn!(
                    "Unknown delivery guarentee: {}. Defaulting to exactly-once",
                    s
                );
                DeliveryGuarantee::ExactlyOnce
            }
        }
    }
}

pub trait DeliveryGuaranteeType {}

#[derive(Default)]
pub struct AtMostOnce {}
impl DeliveryGuaranteeType for AtMostOnce {}
#[derive(Default)]
pub struct AtLeastOnce {}
impl DeliveryGuaranteeType for AtLeastOnce {}
#[derive(Default)]
pub struct ExactlyOnce {}
impl DeliveryGuaranteeType for ExactlyOnce {}
