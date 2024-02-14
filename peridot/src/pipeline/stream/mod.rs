use std::{pin::Pin, task::{Context, Poll}};

use crate::pipeline::message::Message;

pub mod map;
pub mod pipeline;

pub trait MessageStream<K, V> {
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Message<K, V>>>;
}

/*
pub trait MessageTrait<K, V> {}

pub trait MessageStream<K, V>: Stream 
where <Self as Stream>::Item: MessageTrait<K, V>
{
    
}
*/