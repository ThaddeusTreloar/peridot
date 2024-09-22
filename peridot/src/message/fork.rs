/*
 * Copyright 2024 Thaddeus Treloar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

use std::{
    pin::Pin,
    task::{ready, Context, Poll},
};

use futures::Future;
use pin_project_lite::pin_project;
use tracing::{info, Level};

use crate::{
    engine::queue_manager::queue_metadata::{self, QueueMetadata}, error::ErrorType, message::stream::MessageCommitError
};

use super::{
    sink::MessageSink,
    stream::{MessageStream, MessageStreamPoll},
    StreamState,
};

pin_project! {
    #[project = ForkProjection]
    pub struct Fork<M, Si>
    where
        M: MessageStream,
        Si: MessageSink<M::KeyType, M::ValueType>,
    {
        queue_metadata: QueueMetadata,
        #[pin]
        message_stream: M,
        #[pin]
        message_sink: Si,
        commit_state: StreamState,
        next_offset: i64,
    }
}

impl<M, Si> Fork<M, Si>
where
    M: MessageStream,
    Si: MessageSink<M::KeyType, M::ValueType>,
{
    pub fn new(message_stream: M, message_sink: Si, queue_metadata: QueueMetadata) -> Self {
        Self {
            queue_metadata,
            message_stream,
            message_sink,
            commit_state: Default::default(),
            next_offset: 0,
        }
    }
}

impl<M, Si> Fork<M, Si>
where
    M: MessageStream,
    Si: MessageSink<M::KeyType, M::ValueType>,
    Si::Error: 'static,
    M::KeyType: Clone,
    M::ValueType: Clone,
{
    fn commit(
        queue_metadata: &mut QueueMetadata,
        mut message_sink: Pin<&mut Si>,
        mut commit_state: &mut StreamState,
        next_offset: &mut i64,
        cx: &mut Context<'_>,
    ) -> Poll<MessageStreamPoll<M::KeyType, M::ValueType>> {
        tracing::debug!(
            "Committing fork for sink: {}, at offset: {}",
            std::any::type_name::<Si>(),
            next_offset,
        );

        match ready!(message_sink.as_mut().poll_commit(*next_offset, cx)) {
            Ok(_) => {
                tracing::debug!("Commit completed for fork, at offset: {}", *next_offset,);

                *commit_state = StreamState::Committed;

                Poll::Ready(MessageStreamPoll::Commit(Ok(*next_offset)))
            }
            Err(ErrorType::Fatal(e)) => Poll::Ready(MessageStreamPoll::Commit(Err(
                MessageCommitError::SinkCommitError(Box::new(e)),
            ))),
            Err(ErrorType::Retryable(e)) => Poll::Ready(MessageStreamPoll::Commit(Err(
                MessageCommitError::SinkCommitError(Box::new(e)),
            ))),
        }
    }

    fn close(
        queue_metadata: &mut QueueMetadata,
        mut message_sink: Pin<&mut Si>,
        cx: &mut Context<'_>,
    ) -> Poll<MessageStreamPoll<M::KeyType, M::ValueType>> {
        tracing::debug!(
            "Upstream queue for topic: {}, partition: {}, has completed. Cleaning up...",
            queue_metadata.source_topic(),
            queue_metadata.partition()
        );

        ready!(message_sink.as_mut().poll_close(cx)).expect("Failed to close");

        Poll::Ready(MessageStreamPoll::Closed)
    }

    fn handle_poll(
        queue_metadata: &mut QueueMetadata,
        mut message_stream: Pin<&mut M>,
        mut message_sink: Pin<&mut Si>,
        commit_state: &mut StreamState,
        next_offset: &mut i64,
        cx: &mut Context<'_>,
    ) -> Poll<MessageStreamPoll<M::KeyType, M::ValueType>> {
        match ready!(message_stream.as_mut().poll_next(cx)) {
            MessageStreamPoll::Closed => {
                *commit_state = StreamState::Closing;

                Self::close(queue_metadata, message_sink, cx)
            }
            MessageStreamPoll::Commit(Ok(offset)) => {
                tracing::debug!("Recieved commit message for offset: {}", offset);

                tracing::debug!(
                    "Consumer position increased, committing fork at offset: {}.",
                    offset
                );
                *next_offset = offset;

                *commit_state = StreamState::Committing;

                if offset > *next_offset {
                    Self::commit(queue_metadata, message_sink, commit_state, next_offset, cx)
                } else {
                    tracing::debug!(
                        "No change in consumer offset, skipping commit at offset: {}.",
                        offset
                    );
                    Poll::Ready(MessageStreamPoll::Commit(Ok(*next_offset)))
                }
            }
            MessageStreamPoll::Commit(Err(e)) => Poll::Ready(MessageStreamPoll::Commit(Err(e))),
            MessageStreamPoll::Message(message) => {
                tracing::debug!("Message received in Fork, sending to sink.");

                *commit_state = StreamState::Uncommitted;

                match message_sink
                    .as_mut()
                    .start_send(message.clone()){
                        Ok(_) => (),
                        Err(ErrorType::Fatal(e)) => panic!("Fatal error"),
                        Err(ErrorType::Retryable(e)) => todo!("Handle retryable error"),
                };

                Poll::Ready(MessageStreamPoll::Message(message))
            }
        }
    }
}

impl<M, Si> MessageStream for Fork<M, Si>
where
    M: MessageStream,
    Si: MessageSink<M::KeyType, M::ValueType>,
    Si::Error: 'static,
    M::KeyType: Clone,
    M::ValueType: Clone,
{
    type KeyType = M::KeyType;
    type ValueType = M::ValueType;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<MessageStreamPoll<Self::KeyType, Self::ValueType>> {
        let span = tracing::span!(Level::DEBUG, "->Fork::poll",).entered();

        let ForkProjection {
            queue_metadata,
            mut message_stream,
            mut message_sink,
            commit_state,
            next_offset,
            ..
        } = self.project();

        match *commit_state {
            StreamState::Committed | StreamState::Uncommitted | StreamState::Sleeping => {
                Self::handle_poll(
                    queue_metadata,
                    message_stream,
                    message_sink,
                    commit_state,
                    next_offset,
                    cx,
                )
            }
            StreamState::Committing => {
                Self::commit(queue_metadata, message_sink, commit_state, next_offset, cx)
            }
            StreamState::Closing => Self::close(queue_metadata, message_sink, cx),
        }
        // Poll the stream for the next message. If self.commit_state is
        // Committed, we know that we have committed our producer transaction,
        // and we will begin polling upstream nodes until they yield Poll::Ready
        // at which point we know that they have either committed or aborted.
        /*
         */
    }
}
