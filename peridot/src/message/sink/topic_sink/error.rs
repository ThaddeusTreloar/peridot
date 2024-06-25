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

use rdkafka::error::KafkaError;

#[derive(Debug, thiserror::Error)]
pub enum TopicSinkError {
    #[error(
        "Failed to send message for topic: {}, partition: {}, offset: {}, caused by: {}",
        topic,
        partition,
        offset,
        err
    )]
    FailedToSendMessageError {
        topic: String,
        partition: i32,
        offset: i64,
        err: KafkaError,
    },
    #[error(
        "Unknown error while checking producer send: {}, partition: {}, offset: {}, caused by: {}",
        topic,
        partition,
        offset,
        err
    )]
    UnknownError {
        topic: String,
        partition: i32,
        offset: i64,
        err: KafkaError,
    },
}
