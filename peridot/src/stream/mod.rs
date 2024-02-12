use futures::{Stream, StreamExt, stream::{empty, Forward, Map}, Sink, TryStream};
use rdkafka::message::BorrowedMessage;

use crate::app::PeridotApp;

use self::{types::{IntoRecordParts, KeyValue}, fork::{Forked, Fork}, checkpoint::Checkpointed};

pub mod checkpoint;
pub mod fork;
pub mod types;

pub trait PStream<'a, M>
where M: From<&'a BorrowedMessage<'a>>
{
    fn pstream(&self) -> impl Stream<Item = M>;
}

pub trait PStreamExt: Stream + Send + Sized
{
    async fn fork(self) -> Forked<Self> 
    where Self: 'static,
        Self::Item: Clone + Send + 'static
    {
        Forked::new(self)
    }

    
    async fn checkpoint<S>(self, sink: S) -> (Forward<Map<Fork<Self>, impl FnMut(Self::Item) -> Result<Self::Item, <S as Sink<<Self as Stream>::Item>>::Error>>, S>, Checkpointed<Fork<Self>>)
    where S: Sink<Self::Ok, Error = Self::Error> + Sink<<Self as Stream>::Item>,
        Self: TryStream + Sized + 'static,
        Self::Item: Clone + Send + 'static
    {
        let forked = self.fork().await;

        let checkpointed = Checkpointed::new(forked.fork());

        let forward = forked.fork().map(Ok).forward(sink);

        (forward, checkpointed)
    }
}

impl <St> PStreamExt for St
where St: Stream + Send + Sized
{}

mod tests {
    use futures::{Stream, StreamExt, stream::empty, SinkExt, TryFutureExt};
    use rdkafka::ClientConfig;
    use tokio::select;

    use crate::app::PeridotApp;

    use super::{PStream, PStreamExt, types::KeyValue};

    async fn tester() {
        let config = ClientConfig::new();

        let app = PeridotApp::from_config(&config).unwrap();

        let stream = app
            .stream_builder("changeOfAddress").await
            .build().await.unwrap();

        let s: Vec<String> = stream
            .stream::<KeyValue<String, String>>()
            .map(
                |message| message.key().to_owned()
            ).collect()
            .await;

        //let mut sink = app.sink("sink.topic").unwrap();

        //let checkpoint = stream_fork.fork().map(Ok).forward(&mut sink);
    }
}