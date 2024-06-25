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

use std::fmt::Display;

use tokio::sync::broadcast::{Receiver, Sender};

/// Transitions when starting are:
///     Stopped -> Rebalancing -> Rebuilding -> Running
#[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
pub enum EngineState {
    Failed,
    Lagging,
    NotReady,
    Rebalancing,
    Rebuilding,
    Running,
    #[default]
    Stopped,
}

impl Display for EngineState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, thiserror::Error, derive_more::Display)]
pub enum StateTransitionError {
    #[display(
        fmt = "StateTransitionError::InvalidStateTransition: from {} to {}",
        from,
        to
    )]
    InvalidStateTransition { to: String, from: String },
}

impl EngineState {
    fn transition_state(self, state: EngineState) -> Result<Self, StateTransitionError> {
        Ok(self)
    }
}

type StateListener = Receiver<EngineState>;
type StateBroadcaster = Sender<EngineState>;
