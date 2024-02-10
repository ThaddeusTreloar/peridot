use std::collections::HashMap;

use futures::Future;

pub mod in_memory;
pub mod persistent;
pub mod error;

pub trait ReadableStateBackend<T> {
    fn get(&self, key: &str) -> impl Future<Output = Option<T>> + Send;
}

pub trait WriteableStateBackend<T> 
{
    fn set(&self, key: &str, value: T) -> impl Future<Output = Option<T>> + Send;
    fn delete(&self, key: &str)-> impl Future<Output = Option<T>> + Send;
}

