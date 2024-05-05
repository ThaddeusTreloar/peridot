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
