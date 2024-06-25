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

pub(crate) fn topic_partition_offset(topic: &str, partition: i32, offset: i64) -> String {
    format!(
        "topic: {}, partition: {}, offset: {}",
        topic, partition, offset
    )
}

pub(crate) fn topic_partition_offset_err(
    topic: &str,
    partition: i32,
    offset: i64,
    err: &impl std::error::Error,
) -> String {
    format!(
        "topic: {}, partition: {}, offset: {}, caused by: {}",
        topic, partition, offset, err
    )
}
