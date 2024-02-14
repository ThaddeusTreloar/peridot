use std::sync::Arc;

use futures::Stream;
use pin_project_lite::pin_project;

pin_project! {
    /// Future for the [`fork`](super::PStreamExt::fork) method.
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Checkpointed<St: Stream> {
        inner: St,
    }
}

impl <St> Checkpointed<St>
where
    St: Stream
{
    pub fn new(stream: St) -> Self {
        Self { inner: stream }
    }
}