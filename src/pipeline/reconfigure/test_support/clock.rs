/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Keeps retry time explicit while real child exit and UDS I/O still progress.

use crate::pipeline::plugin::test_support::TEST_DEADLINE;
use std::error::Error;
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::mpsc::{RecvTimeoutError, channel};

type TestResult = Result<(), Box<dyn Error>>;

pub(in crate::pipeline) async fn with_frozen_clock(
    check: impl Future<Output = TestResult>,
) -> TestResult {
    let (release, hold) = channel::<()>();
    // Tokio inhibits auto-advance while a blocking task runs. A real deadline
    // releases that inhibition if the child stalls, so the fixture can time out.
    let blocker = tokio::task::spawn_blocking(move || hold.recv_timeout(TEST_DEADLINE));
    tokio::time::pause();
    let mut checking = std::pin::pin!(check);
    let outcome = poll_fn(|context| {
        match catch_unwind(AssertUnwindSafe(|| checking.as_mut().poll(context))) {
            Ok(result) => result.map(Ok),
            Err(panic) => std::task::Poll::Ready(Err(panic)),
        }
    })
    .await;
    tokio::time::resume();
    drop(release);
    let released = blocker.await?;
    match outcome {
        Err(panic) => resume_unwind(panic),
        Ok(result) => {
            assert_eq!(
                released,
                Err(RecvTimeoutError::Disconnected),
                "Frozen-clock test exceeded its real deadline"
            );
            result
        }
    }
}
