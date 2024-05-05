pub mod fork;
pub mod forward;
pub mod join;
pub mod map;
pub mod sink;
pub mod state_fork;
pub mod stream;
pub mod types;

#[derive(Debug, Default, PartialEq, Eq)]
enum CommitState {
    #[default]
    Uncommitted,
    Committing,
    Committed,
}

impl CommitState {
    fn is_committing(&self) -> bool {
        *self == CommitState::Committing
    }
}
