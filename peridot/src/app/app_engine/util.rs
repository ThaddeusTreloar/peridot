use tracing::warn;



#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryGuarantee {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

impl <T> From<T> for DeliveryGuarantee
where
    T: Into<String>
{
    fn from(s: T) -> Self {
        let s: String = s.into();

        match s.as_str() {
            "at-most-once" => DeliveryGuarantee::AtMostOnce,
            "at-least-once" => DeliveryGuarantee::AtLeastOnce,
            "exactly-once" => DeliveryGuarantee::ExactlyOnce,
            _ => {
                warn!("Unknown delivery guarentee: {}. Defaulting to exactly-once", s);
                DeliveryGuarantee::ExactlyOnce
            }
        }
    }
}

pub trait DeliveryGuaranteeType {}

pub struct AtMostOnce {}
impl DeliveryGuaranteeType for AtMostOnce {}
pub struct AtLeastOnce {}
impl DeliveryGuaranteeType for AtLeastOnce {}
pub struct ExactlyOnce {}
impl DeliveryGuaranteeType for ExactlyOnce {}