use crate::engine::tasks::Builder;


pub struct App {

}

pub trait IntoReference {
}

pub struct Task(&str);
pub struct Topic(&str);

impl IntoReference for Task {
}

impl IntoReference for Topic {
}

impl IntoReference for (Task, Task) {
}

impl IntoReference for (Task, Topic) {
}

impl IntoReference for (Topic, Task) {
}

impl IntoReference for (Topic, Topic) {
}

trait StreamHandler {
}

type HandlerBuilder<B> = Box<dyn Fn(&B)>;

impl <K, V, B> StreamHandler for HandlerBuilder<B> 
where B: Builder<K, V>
{}

pub trait AppControl<T> 
where T: IntoReference
{
    fn task(reference: T, cb: ());
    fn job(reference: T, cb: ());
}