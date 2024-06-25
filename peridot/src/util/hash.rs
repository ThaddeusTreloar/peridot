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

use std::{
    ffi::c_void,
    ptr::{null, null_mut},
};

use rdkafka::bindings::rd_kafka_msg_partitioner_murmur2;

pub(crate) fn get_partition_for_key(key_bytes: &[u8], partition_count: i32) -> i32 {
    unsafe {
        // Note: rkt, rkt_opaque, and key_opaque are null pointers
        // to simplify this interface. Looking at the rdkafka source
        // from librdkafka/src/rdkafka_message.c, it seems that these
        // parameters are not used in the murmur2 partitioner.
        // They are used in the rd_kafka_msg_partitioner_murmur2_random
        // partitioner, however.
        //
        // At some point we should consider creating a robust way to rely
        // on the upstream library as this is fairly brittle.
        // We could alternatively use the `fasthash` crate, which is maintained
        // by the original author of murmur2, but the implementation may
        // not be exactly the same as the one used in librdkafka.
        rd_kafka_msg_partitioner_murmur2(
            null(),
            key_bytes.as_ptr() as *const c_void,
            key_bytes.len(),
            partition_count,
            null_mut(),
            null_mut(),
        )
    }
}
