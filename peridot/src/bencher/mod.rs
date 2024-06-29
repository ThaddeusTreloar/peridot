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
    task::{Context, Poll},
};

use futures::Future;
use pin_project_lite::pin_project;
use tracing::info;

pin_project! {
    pub struct Bencher {
        message_count: i64,
        timer: tokio::time::Instant,
        offsets: Vec<i64>,
        waker: tokio::sync::mpsc::Receiver<(i32, i64)>,
    }
}

impl Bencher {
    pub fn new(message_count: i64, waker: tokio::sync::mpsc::Receiver<(i32, i64)>) -> Self {
        Self {
            waker,
            timer: tokio::time::Instant::now(),
            message_count,
            offsets: vec![0; 128],
        }
    }
}

impl Future for Bencher {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if let Some((part, offset)) = std::task::ready!(this.waker.poll_recv(cx)) {
            std::mem::replace(&mut this.offsets[part as usize], offset);
        }

        info!("Offsets total: {}", this.offsets.iter().sum::<i64>());

        if this.offsets.iter().sum::<i64>() > *this.message_count {
            let time_taken = this.timer.elapsed().as_millis();
            let mps = this.offsets.iter().sum::<i64>() as f64 / (time_taken as f64 / 1000_f64);

            info!("Time taken: {}ms", time_taken);
            info!("Messages per sec: {:.2}m/s", mps);

            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();

            Poll::Pending
        }
    }
}
