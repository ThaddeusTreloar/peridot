use crate::{engine::wrapper::serde::PeridotSerializer, util::hash::get_partition_for_key};

pub trait PeridotPartitioner {
    fn partition_key<KS>(&self, key: KS::Input, partition_count: i32) -> i32
    where
        KS: PeridotSerializer;
}

pub struct DefaultPartitioner;

impl PeridotPartitioner for DefaultPartitioner {
    fn partition_key<KS>(&self, key: KS::Input, partition_count: i32) -> i32
    where
        KS: PeridotSerializer,
    {
        let key_bytes = KS::serialize(&key).expect("Failed to serialise key while paritioning.");

        get_partition_for_key(&key_bytes, partition_count)
    }
}
