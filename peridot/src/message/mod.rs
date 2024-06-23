pub mod async_map;
pub mod filter;
pub mod fork;
pub mod forward;
pub mod join;
pub mod map;
pub mod par_map;
pub mod sink;
pub mod state_fork;
pub mod stream;
pub mod types;

// Derive from partition_nodes/max_producer_msg
const BATCH_SIZE: usize = 65535;

#[derive(Debug, Default, PartialEq, Eq, derive_more::Display)]
pub(crate) enum StreamState {
    #[default]
    Uncommitted,
    Committing,
    Committed,
    Sleeping,
    Closing,
}

impl StreamState {
    fn is_committing(&self) -> bool {
        *self == StreamState::Committing
    }
}
