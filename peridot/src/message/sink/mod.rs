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
    error::Error,
    pin::Pin,
    task::{Context, Poll},
};

use super::types::Message;

use crate::error::ErrorType;

pub mod debug_sink;
pub mod export;
pub(crate) mod noop_sink;
pub(crate) mod state_sink;
pub mod topic_sink;

pub trait MessageSink<K, V> {
    type Error: Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ErrorType<Self::Error>>>;
    fn start_send(
        self: Pin<&mut Self>,
        message: Message<K, V>,
    ) -> Result<Message<K, V>, ErrorType<Self::Error>>;
    fn poll_commit(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<i64, ErrorType<Self::Error>>>;
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ErrorType<Self::Error>>>;
}

pub trait MessageSinkExt<K, V>: MessageSink<K, V> {
    fn sink(self)
    where
        Self: Sized,
    {
    }
}

pub trait CommitingSink {}

pub trait NonCommittingSink {}
