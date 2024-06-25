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
