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

//! Owns serialized Pipeline reconfiguration independently from control I/O.
//!
//! One long-lived Tokio task owns the sole [`crate::pipeline::reconfigure::PipelineReconfigurer`], its active
//! `apply` call, and one latest-pending Revision slot. The task runs on the
//! Pipeline main thread's current-thread runtime; it does not add an OS thread.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Notify, oneshot, watch};
use tokio::task::{JoinError, JoinHandle};

use crate::pipeline::terminate_pipeline;
use crate::time::Deadline;

use crate::contracts::core::{PipelineRevisionPlan, PipelineStatusSnapshot};
use crate::pipeline::reconfigure::revision::PipelineRevision;
use crate::pipeline::reconfigure::{
    PipelineApplyOutcome, PipelineReconfigureError, PipelineReconfigureShutdownHandle,
    PipelineReconfigurer, ReconfigureShutdown, RuntimeObservation,
};

/// The Control Loop handle for one long-lived Pipeline Controller actor.
pub(crate) struct PipelineController {
    pending: Arc<LatestRevisionSlot<PipelineRevisionPlan>>,
    statuses: watch::Receiver<Option<PipelineStatusSnapshot>>,
    shutdown: Option<oneshot::Sender<ReconfigureShutdown>>,
    task: Option<JoinHandle<Result<(), PipelineControllerError>>>,
}

impl PipelineController {
    pub(in crate::pipeline) fn start(
        mut reconfigurer: PipelineReconfigurer,
        first_target: PipelineRevision,
        reconfigure_timeout: Duration,
    ) -> Self {
        let pending = Arc::new(LatestRevisionSlot::new());
        let (status_sender, statuses) = watch::channel(None);
        let (shutdown, shutdown_receiver) = oneshot::channel();
        let task_pending = Arc::clone(&pending);
        let task = tokio::spawn(async move {
            run_controller(
                &mut reconfigurer,
                ControllerPhase::Applying {
                    target: Box::new(first_target),
                    deadline: None,
                },
                reconfigure_timeout,
                task_pending,
                status_sender,
                shutdown_receiver,
            )
            .await
        });
        Self {
            pending,
            statuses,
            shutdown: Some(shutdown),
            task: Some(task),
        }
    }

    /// Replaces the single pending Revision without waiting for reconstruction or apply.
    pub(crate) fn submit_latest(&self, revision: PipelineRevisionPlan) {
        self.pending.replace(revision);
    }

    /// Waits for the next complete status or the Controller's terminal failure.
    pub(crate) async fn next_status(
        &mut self,
    ) -> Result<PipelineStatusSnapshot, PipelineControllerError> {
        enum Observation {
            Status(Result<(), watch::error::RecvError>),
            Task(Result<Result<(), PipelineControllerError>, JoinError>),
        }

        let observation = {
            let task = self
                .task
                .as_mut()
                .ok_or(PipelineControllerError::TaskResultAlreadyConsumed)?;
            tokio::select! {
                biased;
                changed = self.statuses.changed() => Observation::Status(changed),
                result = task => Observation::Task(result),
            }
        };
        match observation {
            Observation::Status(Ok(())) => self
                .statuses
                .borrow_and_update()
                .clone()
                .ok_or(PipelineControllerError::StatusMissing),
            Observation::Status(Err(_)) => {
                let task = self
                    .task
                    .take()
                    .ok_or(PipelineControllerError::TaskResultAlreadyConsumed)?;
                Err(controller_task_error(task.await))
            }
            Observation::Task(result) => {
                let _completed_task = self
                    .task
                    .take()
                    .ok_or(PipelineControllerError::TaskResultAlreadyConsumed)?;
                Err(controller_task_error(result))
            }
        }
    }

    /// Quiesces and drains the runtime before final child shutdown.
    pub(crate) async fn shutdown_and_wait(mut self) -> Result<(), PipelineControllerError> {
        self.finish(ReconfigureShutdown::Planned).await
    }

    /// Force-stops every owned resource after a terminal control or runtime failure.
    pub(crate) async fn force_shutdown_and_wait(mut self) -> Result<(), PipelineControllerError> {
        self.finish(ReconfigureShutdown::Force).await
    }

    async fn finish(
        &mut self,
        shutdown: ReconfigureShutdown,
    ) -> Result<(), PipelineControllerError> {
        if let Some(sender) = self.shutdown.take() {
            let _ = sender.send(shutdown);
        }
        let task = self
            .task
            .take()
            .ok_or(PipelineControllerError::TaskResultAlreadyConsumed)?;
        task.await
            .unwrap_or_else(|source| Err(PipelineControllerError::Task(source)))
    }
}

impl fmt::Debug for PipelineController {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PipelineController")
            .field(
                "task_finished",
                &self.task.as_ref().is_none_or(JoinHandle::is_finished),
            )
            .finish_non_exhaustive()
    }
}

impl Drop for PipelineController {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn run_controller(
    reconfigurer: &mut PipelineReconfigurer,
    mut phase: ControllerPhase,
    reconfigure_timeout: Duration,
    pending: Arc<LatestRevisionSlot<PipelineRevisionPlan>>,
    statuses: watch::Sender<Option<PipelineStatusSnapshot>>,
    mut shutdown: oneshot::Receiver<ReconfigureShutdown>,
) -> Result<(), PipelineControllerError> {
    loop {
        let deadline = phase.deadline();
        // Terminal cleanup belongs to the same operation budget. The timer
        // kills the group while this future still owns all blocked resources.
        let completing = async {
            let operation = phase
                .advance(
                    reconfigurer,
                    reconfigure_timeout,
                    &pending,
                    &statuses,
                    &mut shutdown,
                )
                .await;
            match operation {
                Ok(ControlFlow::Continue(next)) => Ok(ControlFlow::Continue(next)),
                Ok(ControlFlow::Break(shutdown)) => {
                    shutdown_reconfigurer(reconfigurer, shutdown).await?;
                    Ok(ControlFlow::Break(()))
                }
                Err(error) => {
                    let _cleanup =
                        shutdown_reconfigurer(reconfigurer, ReconfigureShutdown::Force).await;
                    Err(error)
                }
            }
        };
        let result = match deadline {
            Some(deadline) => enforce(deadline, completing).await,
            None => completing.await,
        }?;
        match result {
            ControlFlow::Continue(next) => phase = next,
            ControlFlow::Break(()) => return Ok(()),
        }
    }
}

/// Keeps the operation alive until the process group is terminated.
async fn enforce<T>(deadline: Deadline, operation: impl Future<Output = T>) -> T {
    // An expired Tokio timer need not be ready on its first poll.
    if deadline.has_elapsed() {
        terminate_pipeline();
    }
    tokio::pin!(operation);
    tokio::select! {
        biased;
        () = deadline.wait() => terminate_pipeline(),
        result = &mut operation => result,
    }
}

struct RevisionReconstructionTask<T> {
    task: JoinHandle<T>,
}

impl<T> RevisionReconstructionTask<T> {
    fn request_cancel(&self) {
        self.task.abort();
    }

    async fn join(&mut self) {
        let _result = (&mut self.task).await;
    }

    async fn cancel_and_join_with_cleanup<F, R>(&mut self, cleanup: F) -> R
    where
        F: Future<Output = R>,
    {
        self.request_cancel();
        let (_reconstruction_result, cleanup) = tokio::join!(self.join(), cleanup);
        cleanup
    }
}

impl<T> Drop for RevisionReconstructionTask<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

enum ControllerPhase {
    Applying {
        target: Box<PipelineRevision>,
        deadline: Option<Deadline>,
    },
    WaitingForRevision,
    Reconstructing {
        reconstruction: RevisionReconstructionTask<PipelineRevision>,
        deadline: Deadline,
    },
}

impl ControllerPhase {
    fn deadline(&self) -> Option<Deadline> {
        match self {
            Self::Applying { deadline, .. } => *deadline,
            Self::Reconstructing { deadline, .. } => Some(*deadline),
            Self::WaitingForRevision => None,
        }
    }

    async fn advance(
        self,
        reconfigurer: &mut PipelineReconfigurer,
        reconfigure_timeout: Duration,
        pending: &LatestRevisionSlot<PipelineRevisionPlan>,
        statuses: &watch::Sender<Option<PipelineStatusSnapshot>>,
        shutdown: &mut oneshot::Receiver<ReconfigureShutdown>,
    ) -> Result<ControlFlow<ReconfigureShutdown, Self>, PipelineControllerError> {
        match self {
            Self::Applying { target, deadline } => {
                Self::advance_applying(target, deadline, reconfigurer, statuses, shutdown).await
            }
            Self::WaitingForRevision => {
                Self::advance_waiting(
                    reconfigurer,
                    reconfigure_timeout,
                    pending,
                    statuses,
                    shutdown,
                )
                .await
            }
            Self::Reconstructing {
                reconstruction,
                deadline,
            } => {
                Self::advance_reconstructing(
                    reconstruction,
                    deadline,
                    reconfigurer,
                    statuses,
                    shutdown,
                )
                .await
            }
        }
    }

    async fn advance_applying(
        target: Box<PipelineRevision>,
        deadline: Option<Deadline>,
        reconfigurer: &mut PipelineReconfigurer,
        statuses: &watch::Sender<Option<PipelineStatusSnapshot>>,
        shutdown: &mut oneshot::Receiver<ReconfigureShutdown>,
    ) -> Result<ControlFlow<ReconfigureShutdown, Self>, PipelineControllerError> {
        let shutdown_handle = reconfigurer.shutdown_handle();
        match select_operation_or_shutdown(
            shutdown_handle,
            reconfigurer.apply(*target, deadline),
            shutdown,
        )
        .await?
        {
            ControlFlow::Break(shutdown) => Ok(ControlFlow::Break(shutdown)),
            ControlFlow::Continue(PipelineApplyOutcome::Applied(status)) => {
                statuses.send_replace(Some(status));
                Ok(ControlFlow::Continue(Self::WaitingForRevision))
            }
            ControlFlow::Continue(PipelineApplyOutcome::Stopped) => {
                Err(PipelineControllerError::StoppedUnexpectedly)
            }
        }
    }

    async fn advance_waiting(
        reconfigurer: &mut PipelineReconfigurer,
        reconfigure_timeout: Duration,
        pending: &LatestRevisionSlot<PipelineRevisionPlan>,
        statuses: &watch::Sender<Option<PipelineStatusSnapshot>>,
        shutdown: &mut oneshot::Receiver<ReconfigureShutdown>,
    ) -> Result<ControlFlow<ReconfigureShutdown, Self>, PipelineControllerError> {
        tokio::select! {
            biased;
            shutdown = &mut *shutdown => Ok(ControlFlow::Break(received_shutdown(shutdown))),
            revision = pending.take() => {
                // Start before handing any received material to a blocking worker.
                let deadline = Deadline::start(reconfigure_timeout);
                Ok(ControlFlow::Continue(Self::Reconstructing {
                    reconstruction: spawn_revision_reconstruction(revision),
                    deadline,
                }))
            }
            event = reconfigurer.observe_runtime() => {
                match handle_observed_event(reconfigurer, event, shutdown).await? {
                    ControlFlow::Continue(status) => {
                        statuses.send_replace(Some(status));
                        Ok(ControlFlow::Continue(Self::WaitingForRevision))
                    }
                    ControlFlow::Break(shutdown) => Ok(ControlFlow::Break(shutdown)),
                }
            }
        }
    }

    async fn advance_reconstructing(
        mut reconstruction: RevisionReconstructionTask<PipelineRevision>,
        deadline: Deadline,
        reconfigurer: &mut PipelineReconfigurer,
        statuses: &watch::Sender<Option<PipelineStatusSnapshot>>,
        shutdown: &mut oneshot::Receiver<ReconfigureShutdown>,
    ) -> Result<ControlFlow<ReconfigureShutdown, Self>, PipelineControllerError> {
        tokio::select! {
            biased;
            shutdown = &mut *shutdown => {
                let shutdown = received_shutdown(shutdown);
                finish_reconstructing(reconfigurer, &mut reconstruction, shutdown).await?;
                Ok(ControlFlow::Break(shutdown))
            }
            result = &mut reconstruction.task => {
                let target = result.map_err(PipelineControllerError::RevisionReconstructionTask)?;
                Ok(ControlFlow::Continue(Self::Applying {
                    target: Box::new(target),
                    deadline: Some(deadline),
                }))
            }
            event = reconfigurer.observe_runtime() => match handle_observed_event(reconfigurer, event, shutdown).await {
                Ok(ControlFlow::Continue(status)) => {
                    statuses.send_replace(Some(status));
                    Ok(ControlFlow::Continue(Self::Reconstructing { reconstruction, deadline }))
                }
                Ok(ControlFlow::Break(shutdown)) => {
                    finish_reconstructing(reconfigurer, &mut reconstruction, shutdown).await?;
                    Ok(ControlFlow::Break(shutdown))
                }
                Err(source) => {
                    let _cleanup = finish_reconstructing(
                        reconfigurer,
                        &mut reconstruction,
                        ReconfigureShutdown::Force,
                    )
                    .await;
                    Err(source)
                }
            }
        }
    }
}

async fn finish_reconstructing(
    reconfigurer: &mut PipelineReconfigurer,
    reconstruction: &mut RevisionReconstructionTask<PipelineRevision>,
    shutdown: ReconfigureShutdown,
) -> Result<(), PipelineControllerError> {
    reconstruction
        .cancel_and_join_with_cleanup(shutdown_reconfigurer(reconfigurer, shutdown))
        .await
}

async fn handle_observed_event(
    reconfigurer: &mut PipelineReconfigurer,
    event: RuntimeObservation,
    shutdown: &mut oneshot::Receiver<ReconfigureShutdown>,
) -> Result<ControlFlow<ReconfigureShutdown, PipelineStatusSnapshot>, PipelineControllerError> {
    select_operation_or_shutdown(
        reconfigurer.shutdown_handle(),
        reconfigurer.handle_runtime_observation(event),
        shutdown,
    )
    .await
}

async fn select_operation_or_shutdown<T, Operation>(
    shutdown_handle: PipelineReconfigureShutdownHandle,
    operation: Operation,
    shutdown: &mut oneshot::Receiver<ReconfigureShutdown>,
) -> Result<ControlFlow<ReconfigureShutdown, T>, PipelineControllerError>
where
    Operation: Future<Output = Result<T, PipelineReconfigureError>>,
{
    tokio::pin!(operation);
    tokio::select! {
        biased;
        shutdown = &mut *shutdown => {
            let shutdown = received_shutdown(shutdown);
            shutdown_handle.request(shutdown);
            operation.await.map_err(PipelineControllerError::Reconfigure)?;
            Ok(ControlFlow::Break(shutdown))
        }
        result = &mut operation => result
            .map(ControlFlow::Continue)
            .map_err(PipelineControllerError::Reconfigure),
    }
}

fn spawn_revision_reconstruction(
    revision: PipelineRevisionPlan,
) -> RevisionReconstructionTask<PipelineRevision> {
    RevisionReconstructionTask {
        task: tokio::task::spawn_blocking(move || PipelineRevision::from_runner(revision)),
    }
}

async fn shutdown_reconfigurer(
    reconfigurer: &mut PipelineReconfigurer,
    shutdown: ReconfigureShutdown,
) -> Result<(), PipelineControllerError> {
    reconfigurer
        .shutdown(shutdown)
        .await
        .map_err(PipelineControllerError::Reconfigure)
}

fn received_shutdown(
    shutdown: Result<ReconfigureShutdown, oneshot::error::RecvError>,
) -> ReconfigureShutdown {
    shutdown.unwrap_or(ReconfigureShutdown::Force)
}

fn controller_task_error(
    result: Result<Result<(), PipelineControllerError>, JoinError>,
) -> PipelineControllerError {
    match result {
        Ok(Err(error)) => error,
        Ok(Ok(())) => PipelineControllerError::StoppedUnexpectedly,
        Err(error) => PipelineControllerError::Task(error),
    }
}

struct LatestRevisionSlot<T> {
    value: Mutex<Option<T>>,
    changed: Notify,
}

impl<T> LatestRevisionSlot<T> {
    const fn new() -> Self {
        Self {
            value: Mutex::new(None),
            changed: Notify::const_new(),
        }
    }

    fn replace(&self, value: T) {
        let replaced = {
            let mut pending = self.lock_value();
            pending.replace(value)
        };
        drop(replaced);
        self.changed.notify_one();
    }

    async fn take(&self) -> T {
        loop {
            if let Some(value) = self.lock_value().take() {
                return value;
            }
            self.changed.notified().await;
        }
    }

    fn lock_value(&self) -> std::sync::MutexGuard<'_, Option<T>> {
        self.value
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// A fatal failure in the sole Pipeline Controller state owner.
#[derive(Debug)]
pub(crate) enum PipelineControllerError {
    RevisionReconstructionTask(JoinError),
    Reconfigure(PipelineReconfigureError),
    StatusMissing,
    StoppedUnexpectedly,
    Task(JoinError),
    TaskResultAlreadyConsumed,
}

impl PipelineControllerError {
    /// Returns the stable local diagnostic code for this terminal failure.
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::RevisionReconstructionTask(_) => "pipeline.revision_reconstruction_task_failed",
            Self::Reconfigure(PipelineReconfigureError::DataPlaneFailure(_)) => {
                "pipeline.data_plane_failed"
            }
            Self::Reconfigure(PipelineReconfigureError::DataPlaneStopped) => {
                "pipeline.data_plane_stopped"
            }
            Self::Reconfigure(_) => "pipeline.reconfigure_failed",
            Self::StatusMissing => "pipeline.controller_status_missing",
            Self::StoppedUnexpectedly => "pipeline.controller_stopped_unexpectedly",
            Self::Task(_) => "pipeline.controller_failed",
            Self::TaskResultAlreadyConsumed => "pipeline.controller_result_already_consumed",
        }
    }
}

impl fmt::Display for PipelineControllerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RevisionReconstructionTask(_) => {
                formatter.write_str("Pipeline revision reconstruction task failed")
            }
            Self::Reconfigure(_) => formatter.write_str("Pipeline reconfiguration failed"),
            Self::StatusMissing => formatter.write_str("Pipeline Controller status is missing"),
            Self::StoppedUnexpectedly => {
                formatter.write_str("Pipeline Controller stopped unexpectedly")
            }
            Self::Task(_) => formatter.write_str("Pipeline Controller task failed"),
            Self::TaskResultAlreadyConsumed => {
                formatter.write_str("Pipeline Controller task result was already consumed")
            }
        }
    }
}

impl Error for PipelineControllerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RevisionReconstructionTask(error) | Self::Task(error) => Some(error),
            Self::Reconfigure(error) => Some(error),
            Self::StatusMissing | Self::StoppedUnexpectedly | Self::TaskResultAlreadyConsumed => {
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(feature = "loom-model"))]
    mod deadline;
    #[cfg(not(feature = "loom-model"))]
    mod reconfiguration;

    use super::{LatestRevisionSlot, RevisionReconstructionTask};
    use std::error::Error;
    use std::future::Future as _;
    use std::io;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};
    use tokio::sync::oneshot;

    #[tokio::test(flavor = "current_thread")]
    async fn latest_revision_slot_keeps_only_d_while_b_is_active() -> Result<(), Box<dyn Error>> {
        let slot = Arc::new(LatestRevisionSlot::new());
        let active_slot = Arc::clone(&slot);
        let (active_sender, active_receiver) = oneshot::channel();
        let (release_sender, release_receiver) = oneshot::channel();
        let active = tokio::spawn(async move {
            active_sender
                .send(())
                .map_err(|()| io::Error::other("Active operation observer was dropped"))?;
            release_receiver
                .await
                .map_err(|_| io::Error::other("Active operation was not released"))?;
            Ok::<String, io::Error>(active_slot.take().await)
        });
        active_receiver
            .await
            .map_err(|_| io::Error::other("Active operation did not start"))?;

        slot.replace(String::from("revision-c"));
        slot.replace(String::from("revision-d"));
        release_sender
            .send(())
            .map_err(|()| io::Error::other("Active operation already stopped"))?;

        assert_eq!(active.await??, "revision-d");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn latest_revision_slot_wakes_a_registered_waiter() -> Result<(), Box<dyn Error>> {
        let slot = LatestRevisionSlot::new();
        let mut waiter = Box::pin(slot.take());
        let mut context = Context::from_waker(Waker::noop());
        assert_eq!(waiter.as_mut().poll(&mut context), Poll::Pending);

        slot.replace(String::from("revision-b"));

        assert_eq!(waiter.await, "revision-b");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cleanup_starts_before_a_running_reconstruction_task_can_finish()
    -> Result<(), Box<dyn Error>> {
        let (reconstruction_started_sender, reconstruction_started_receiver) = oneshot::channel();
        let (release_reconstruction_sender, release_reconstruction_receiver) =
            std::sync::mpsc::sync_channel(1);
        let task = tokio::task::spawn_blocking(move || {
            let _ = reconstruction_started_sender.send(());
            let _ = release_reconstruction_receiver.recv();
        });
        let mut reconstruction: RevisionReconstructionTask<()> =
            RevisionReconstructionTask { task };
        reconstruction_started_receiver
            .await
            .map_err(|_| io::Error::other("Reconstruction task did not start"))?;

        let (cleanup_started_sender, cleanup_started_receiver) = oneshot::channel();
        let cleanup = async move {
            cleanup_started_sender
                .send(())
                .map_err(|()| io::Error::other("Cleanup observer was dropped"))
        };
        let operation = reconstruction.cancel_and_join_with_cleanup(cleanup);
        tokio::pin!(operation);

        tokio::select! {
            result = &mut operation => {
                return Err(format!(
                    "Reconstruction finished before its release while cleanup returned {result:?}"
                ).into());
            }
            observed = cleanup_started_receiver => {
                observed.map_err(|_| io::Error::other("Cleanup did not start"))?;
            }
        }
        release_reconstruction_sender
            .send(())
            .map_err(|_| io::Error::other("Reconstruction task stopped before release"))?;
        operation.await?;
        Ok(())
    }
}
