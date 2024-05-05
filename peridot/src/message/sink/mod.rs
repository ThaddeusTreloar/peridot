use std::{
    error::Error,
    pin::Pin,
    task::{Context, Poll},
};

use super::types::Message;

pub mod bench_sink;
pub(crate) mod changelog_sink;
pub(crate) mod noop_sink;
pub mod print_sink;
pub(crate) mod state_sink;
pub mod topic_sink;

pub trait MessageSink<K, V> {
    type Error: Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
    fn start_send(self: Pin<&mut Self>, message: Message<K, V>) -> Result<(), Self::Error>;
    fn poll_commit(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
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
