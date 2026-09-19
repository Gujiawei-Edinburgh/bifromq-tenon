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

//! Shutdown-aware work used by every reconfiguration phase.

use std::future::Future;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::error::PipelineReconfigureError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ReconfigureShutdown {
    Planned,
    Force,
}

pub(super) enum ReconfigureProgress<T> {
    Completed(T),
    Stopped,
}

pub(super) struct BlockingReconfigureJob<T> {
    task: JoinHandle<Result<T, PipelineReconfigureError>>,
    cancellation: Option<BlockingJobCancellation>,
}

pub(super) type BlockingJobCancellation = Box<dyn FnOnce() + Send + 'static>;

impl<T> BlockingReconfigureJob<T> {
    pub(super) async fn wait(&mut self) -> Result<T, PipelineReconfigureError> {
        (&mut self.task)
            .await
            .map_err(PipelineReconfigureError::BlockingTask)?
    }

    /// Requests cancellation and waits for both work and disposal to finish.
    /// The caller must keep awaiting this operation, even after shutdown wins.
    pub(super) async fn cancel_and_discard(&mut self) -> Result<(), PipelineReconfigureError>
    where
        T: Send + 'static,
    {
        if let Some(cancel) = self.cancellation.take() {
            cancel();
        }
        self.task.abort();
        let value = match self.wait().await {
            Ok(value) => value,
            Err(error) if error.is_cancellation_exit() => return Ok(()),
            Err(error) => return Err(error),
        };
        // A completed preparation may own workers and files. Their Drop
        // must not synchronously join or remove files on the control loop.
        tokio::task::spawn_blocking(move || drop(value))
            .await
            .map_err(PipelineReconfigureError::BlockingTask)?;
        Ok(())
    }
}

pub(super) fn spawn_blocking_reconfigure_work<T>(
    cancellation: Option<BlockingJobCancellation>,
    work: impl FnOnce() -> Result<T, PipelineReconfigureError> + Send + 'static,
) -> BlockingReconfigureJob<T>
where
    T: Send + 'static,
{
    BlockingReconfigureJob {
        task: tokio::task::spawn_blocking(work),
        cancellation,
    }
}

pub(super) async fn wait_for_shutdown(
    receiver: &mut watch::Receiver<Option<ReconfigureShutdown>>,
) -> ReconfigureShutdown {
    loop {
        if let Some(shutdown) = *receiver.borrow_and_update() {
            return shutdown;
        }
        if receiver.changed().await.is_err() {
            std::process::abort();
        }
    }
}

pub(in crate::pipeline) enum DataPlaneOperation<T> {
    Completed(T),
    WorkerExited,
    Shutdown(ReconfigureShutdown),
}

pub(super) async fn select_data_plane_operation<T>(
    worker_exit: impl Future<Output = ()>,
    operation: impl Future<Output = T>,
    shutdown: &mut watch::Receiver<Option<ReconfigureShutdown>>,
) -> DataPlaneOperation<T> {
    tokio::select! {
        biased;
        () = worker_exit => DataPlaneOperation::WorkerExited,
        shutdown = wait_for_shutdown(shutdown) => DataPlaneOperation::Shutdown(shutdown),
        result = operation => DataPlaneOperation::Completed(result),
    }
}

#[cfg(all(test, not(feature = "loom-model")))]
mod tests;
