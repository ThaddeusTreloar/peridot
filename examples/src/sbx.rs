use uuid::Uuid;
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TableState {
    #[default]
    Uninitialised,
    Rebuilding,
    Ready,
}


fn main() {
    let uuid = vec![TableState::Ready, TableState::Uninitialised, TableState::Rebuilding, TableState::Rebuilding];

    println!("{:?}", uuid.iter().max());
}