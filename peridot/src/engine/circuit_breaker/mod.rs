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
    pin::Pin,
    task::{Context, Poll},
};

use futures::Future;

// TODO: internal circuit braker
#[derive(Debug)]
pub(crate) struct CircuitBreaker {
    state: CircuitState,
    listener: tokio::sync::mpsc::Receiver<()>,
    broadcast: tokio::sync::broadcast::Sender<CircuitState>,
}

impl Future for CircuitBreaker {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // listen

        // evaulate

        // broadcast

        Poll::Pending
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, derive_more::Display)]
pub(crate) enum CircuitState {
    #[default]
    Closed,
    Open,
    PartialOpen(usize),
}

impl CircuitState {
    fn new() -> Self {
        Default::default()
    }

    fn threshold_exceeded(&mut self) {
        match *self {
            Self::PartialOpen(_) | Self::Closed => *self = Self::Open,
            Self::Open => (),
        }
    }

    fn ok(&mut self, level: usize) {
        match *self {
            Self::PartialOpen(_) | Self::Open => *self = Self::PartialOpen(level),
            Self::Closed => (),
        }
    }
}
