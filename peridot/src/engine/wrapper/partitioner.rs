use crate::{engine::wrapper::serde::PSerialize, util::hash::get_partition_for_key};

pub trait PeridotPartitioner {
    fn partition_key<KS>(&self, key: KS::Input, partition_count: i32) -> i32
    where
        KS: PSerialize;
}

pub struct DefaultPartitioner;

impl PeridotPartitioner for DefaultPartitioner {
    fn partition_key<KS>(&self, key: KS::Input, partition_count: i32) -> i32
    where
        KS: PSerialize,
    {
        let key_bytes = KS::serialize(&key).expect("Failed to serialise key while paritioning.");

        get_partition_for_key(&key_bytes, partition_count)
    }
}
