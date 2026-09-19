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

//! Shared worker-thread ownership and terminal-event accounting.
//!
//! A [`WorkerTask`] converts return, failure, and panic into one bounded event.
//! Its [`WorkerSet`] keeps the receiver, every observed event, and every
//! [`JoinHandle`] in the same owner. Waiting is therefore cancellation-safe:
//! dropping a losing `select!` branch drops only the borrow, never the event or
//! thread owner. Final completion joins every thread before selecting the one
//! failure that best explains the generation's termination.

use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::thread::JoinHandle;

use tokio::sync::mpsc::{Receiver, Sender, error::TryRecvError};

use super::error::{PipelineRuntimeError, PipelineRuntimeShutdownError, PipelineWorker};
use super::retirement::PipelineDrainObservation;
use crate::identifiers::FlowId;
use crate::pipeline::channel::FlowChannelError;

pub(super) enum WorkerRunResult<E> {
    /// The worker body returned through its ordinary result boundary.
    Returned(Result<(), E>),
    /// Panic propagation was caught at the thread entry boundary.
    Panicked,
}

/// Failure precedence used only after all owned worker threads are joined.
///
/// Declaration order is intentional because derived ordering chooses the most
/// explanatory exit when several workers terminate during the same cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum WorkerExitDisposition {
    /// A worker returned after a planned stop or completed drain.
    CleanExit,
    /// A worker reported its own independent runtime failure.
    IndependentFailure,
    /// The worker crossed the panic boundary.
    Panic,
}

impl WorkerExitDisposition {
    fn is_primary_failure(self) -> bool {
        matches!(self, Self::IndependentFailure | Self::Panic)
    }

    pub(super) fn is_expected_after_planned_stop(self) -> bool {
        matches!(self, Self::CleanExit)
    }
}

/// One terminal event that preserves the worker-specific source error.
pub(super) struct WorkerExit {
    pub(super) flow_id: FlowId,
    pub(super) channel_index: u32,
    pub(super) result: WorkerRunResult<FlowChannelError>,
}

impl WorkerExit {
    fn panicked(worker: PipelineWorker) -> Self {
        Self {
            flow_id: worker.flow_id,
            channel_index: worker.channel_index,
            result: WorkerRunResult::Panicked,
        }
    }

    fn worker(&self) -> PipelineWorker {
        PipelineWorker {
            flow_id: self.flow_id.clone(),
            channel_index: self.channel_index,
        }
    }

    pub(super) fn disposition(&self) -> WorkerExitDisposition {
        match self.result {
            WorkerRunResult::Panicked => WorkerExitDisposition::Panic,
            WorkerRunResult::Returned(Err(_)) => WorkerExitDisposition::IndependentFailure,
            WorkerRunResult::Returned(Ok(())) => WorkerExitDisposition::CleanExit,
        }
    }

    fn into_error(self) -> PipelineRuntimeError {
        let worker = self.worker();
        match self.result {
            WorkerRunResult::Returned(Err(source)) => PipelineRuntimeError::FlowChannelFailed {
                flow_id: self.flow_id,
                channel_index: self.channel_index,
                source,
            },
            WorkerRunResult::Returned(Ok(())) => {
                PipelineRuntimeError::WorkerExitedUnexpectedly { worker }
            }
            WorkerRunResult::Panicked => PipelineRuntimeError::WorkerPanicked { worker },
        }
    }
}

/// One thread entry that reports a panic through the same exit path as errors.
pub(super) struct WorkerTask {
    /// Stable identity retained outside the worker body for panic attribution.
    pub(super) worker: PipelineWorker,
    /// Returns `None` only when startup was aborted before runtime publication.
    pub(super) work: Box<dyn FnOnce() -> Option<WorkerExit> + Send + 'static>,
    /// Bounded terminal-event sender paired with the owning [`WorkerSet`].
    pub(super) exit_sender: Sender<WorkerExit>,
}

impl WorkerTask {
    pub(super) fn run(self) {
        let Self {
            worker,
            work,
            exit_sender,
        } = self;
        let exit = match panic::catch_unwind(AssertUnwindSafe(work)) {
            Ok(Some(exit)) => exit,
            Ok(None) => return,
            Err(_) => WorkerExit::panicked(worker),
        };
        let _ = exit_sender.try_send(exit);
    }
}

/// Join ownership paired with the identity needed if joining observes a panic.
pub(super) struct WorkerThread {
    worker: PipelineWorker,
    handle: JoinHandle<()>,
}

impl WorkerThread {
    pub(super) fn new(worker: PipelineWorker, handle: JoinHandle<()>) -> Self {
        Self { worker, handle }
    }
}

/// Cancellation-safe event history and JoinHandles for one logical worker set.
pub(super) struct WorkerSet {
    /// Receiver position lives with the owner rather than any temporary future.
    exits: Receiver<WorkerExit>,
    /// Events already consumed by wait or non-blocking observation.
    observed_exits: Vec<WorkerExit>,
    /// A closed event channel is retained as an invariant failure.
    event_channel_closed: bool,
    /// Handles are removed only by the final join operation.
    workers: Vec<WorkerThread>,
}

impl WorkerSet {
    pub(super) fn new(exits: Receiver<WorkerExit>, workers: Vec<WorkerThread>) -> Self {
        Self {
            exits,
            observed_exits: Vec::new(),
            event_channel_closed: false,
            workers,
        }
    }

    pub(super) fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub(super) fn observed_exit_count(&self) -> usize {
        self.observed_exits.len()
    }

    pub(super) fn has_observed_event(&self) -> bool {
        !self.observed_exits.is_empty() || self.event_channel_closed
    }

    /// Freeze this set's exit interpretation before publishing the final Stop.
    pub(super) fn completion_context(&self, stop_was_requested: bool) -> WorkerCompletionContext {
        if stop_was_requested || !self.has_observed_event() {
            WorkerCompletionContext::PlannedStop
        } else {
            WorkerCompletionContext::FailureAlreadyObserved
        }
    }

    pub(super) fn poll_next_exit(&mut self, context: &mut Context<'_>) -> Poll<WorkerObservation> {
        if self.event_channel_closed {
            return Poll::Ready(WorkerObservation::EventChannelClosed);
        }
        match Pin::new(&mut self.exits).poll_recv(context) {
            Poll::Ready(Some(exit)) => {
                let disposition = exit.disposition();
                self.observed_exits.push(exit);
                Poll::Ready(WorkerObservation::Exit(disposition))
            }
            Poll::Ready(None) => {
                self.event_channel_closed = true;
                Poll::Ready(WorkerObservation::EventChannelClosed)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    pub(super) fn observe_exit_now(&mut self) {
        if self.has_observed_event() {
            return;
        }
        match self.exits.try_recv() {
            Ok(exit) => self.observed_exits.push(exit),
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => self.event_channel_closed = true,
        }
    }

    // The caller keeps the owning Runtime around this await. If the future is
    // cancelled, the WorkerSet and every JoinHandle remain in that owner so its
    // Drop path can stop and join the same workers.
    async fn complete(&mut self) -> WorkerSetCompletion {
        let prior_observation_count = self.observed_exits.len();
        while !self.event_channel_closed && self.observed_exits.len() < self.workers.len() {
            match self.exits.recv().await {
                Some(exit) => self.observed_exits.push(exit),
                None => self.event_channel_closed = true,
            }
        }
        let joined_panic = self.join();
        WorkerSetCompletion {
            exits: std::mem::take(&mut self.observed_exits),
            prior_observation_count,
            event_channel_closed: self.event_channel_closed,
            joined_panic,
        }
    }

    pub(super) fn join(&mut self) -> Option<PipelineWorker> {
        join_worker_threads(&mut self.workers)
    }

    /// Retains all events while allowing only clean exits during a planned stop.
    /// Failure leaves the exact source here for the terminal completion pass.
    /// Callers must not resume planned observation after a reported failure.
    fn poll_planned_completion(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<PipelineDrainObservation> {
        loop {
            if self
                .observed_exits
                .last()
                .is_some_and(|exit| exit.disposition() != WorkerExitDisposition::CleanExit)
            {
                return Poll::Ready(PipelineDrainObservation::WorkerExited);
            }
            if self.event_channel_closed {
                return Poll::Ready(PipelineDrainObservation::WorkerExited);
            }
            if self.observed_exit_count() == self.worker_count() {
                return Poll::Ready(PipelineDrainObservation::Drained);
            }
            if self.poll_next_exit(context).is_pending() {
                return Poll::Pending;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WorkerObservation {
    Exit(WorkerExitDisposition),
    EventChannelClosed,
}

struct WorkerSetCompletion {
    exits: Vec<WorkerExit>,
    prior_observation_count: usize,
    event_channel_closed: bool,
    joined_panic: Option<PipelineWorker>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WorkerCompletionContext {
    PlannedStop,
    FailureAlreadyObserved,
}

/// Polls every set so a blocked sibling cannot hide another worker's failure.
pub(super) fn poll_planned_completion<'a>(
    worker_sets: impl IntoIterator<Item = &'a mut WorkerSet>,
    context: &mut Context<'_>,
) -> Poll<PipelineDrainObservation> {
    let mut complete = true;
    for workers in worker_sets {
        match workers.poll_planned_completion(context) {
            Poll::Ready(PipelineDrainObservation::Drained) => {}
            Poll::Ready(PipelineDrainObservation::WorkerExited) => {
                return Poll::Ready(PipelineDrainObservation::WorkerExited);
            }
            Poll::Pending => complete = false,
        }
    }
    if complete {
        Poll::Ready(PipelineDrainObservation::Drained)
    } else {
        Poll::Pending
    }
}

pub(super) async fn finish_worker_sets<'a, Sets>(
    worker_sets: Sets,
) -> Result<(), PipelineRuntimeError>
where
    Sets: IntoIterator<Item = (WorkerCompletionContext, &'a mut WorkerSet)>,
    Sets::IntoIter: ExactSizeIterator,
{
    // Complete every set before reporting an error. Returning early would drop
    // still-owned JoinHandles and violate the finite-cleanup guarantee.
    let worker_sets = worker_sets.into_iter();
    let mut completions = Vec::new();
    completions
        .try_reserve_exact(worker_sets.len())
        .map_err(|_| PipelineRuntimeError::ResourceLimitExceeded)?;
    for (context, worker_set) in worker_sets {
        completions.push((context, worker_set.complete().await));
    }

    if let Some(worker) = completions
        .iter_mut()
        .find_map(|(_, completion)| completion.joined_panic.take())
    {
        return Err(PipelineRuntimeError::WorkerPanicked { worker });
    }
    if completions
        .iter()
        .any(|(_, completion)| completion.event_channel_closed)
    {
        return Err(PipelineRuntimeError::InternalEventChannelClosed);
    }

    let mut selected = None;
    let mut primary_cause_already_selected = false;
    for (context, completion) in completions {
        for (index, exit) in completion.exits.into_iter().enumerate() {
            if context == WorkerCompletionContext::PlannedStop
                && exit.disposition().is_expected_after_planned_stop()
            {
                continue;
            }
            let was_prior_observation = index < completion.prior_observation_count;
            if context == WorkerCompletionContext::FailureAlreadyObserved
                && was_prior_observation
                && exit.disposition().is_primary_failure()
            {
                if !primary_cause_already_selected {
                    selected = Some(exit);
                    primary_cause_already_selected = true;
                }
                continue;
            }
            match &mut selected {
                Some(current)
                    if !primary_cause_already_selected
                        && exit.disposition() > current.disposition() =>
                {
                    *current = exit;
                }
                Some(_) => {}
                None => selected = Some(exit),
            }
        }
    }

    match selected {
        Some(selected) => Err(selected.into_error()),
        None => Ok(()),
    }
}

pub(super) fn abort_process_if_stop_failed(result: Result<(), PipelineRuntimeShutdownError>) {
    // Without a successful wake, a blocking worker may never reach its join.
    // Process abort is the only finite cleanup boundary left to this owner.
    if result.is_err() {
        std::process::abort();
    }
}

pub(super) fn join_worker_threads(workers: &mut Vec<WorkerThread>) -> Option<PipelineWorker> {
    let mut first_panic = None;
    for worker in workers.drain(..) {
        if worker.handle.join().is_err() && first_panic.is_none() {
            first_panic = Some(worker.worker);
        }
    }
    first_panic
}

#[cfg(all(test, not(feature = "loom-model")))]
pub(super) mod test_support {
    use super::FlowId;
    use crate::pipeline::runtime::PipelineRuntime;
    use std::future::Future;
    use std::io;
    use tokio::sync::{mpsc, oneshot};

    pub(in crate::pipeline::runtime) fn has_finished_thread(workers: &super::WorkerSet) -> bool {
        workers
            .workers
            .iter()
            .any(|worker| worker.handle.is_finished())
    }

    /// Delays the original terminal report, modeling scheduling before its
    /// publication. The caller polls the relay alongside the real Controller.
    pub(in crate::pipeline) fn hold_worker_exit(
        runtime: &mut PipelineRuntime,
        flow: &FlowId,
    ) -> io::Result<(HeldWorkerExit, impl Future<Output = ()> + Send + use<>)> {
        let workers = runtime
            .flows
            .get_mut(flow)
            .ok_or_else(|| io::Error::other("Fixture Flow is missing"))?
            .worker_set_mut();
        let (sender, receiver) = mpsc::channel(workers.exits.max_capacity());
        let mut original = std::mem::replace(&mut workers.exits, receiver);
        let (arrived, observed) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let forwarding = async move {
            if let Some(exit) = original.recv().await {
                let _ = arrived.send(());
                let _ = released.await;
                if sender.send(exit).await.is_err() {
                    return;
                }
            }
            while let Some(exit) = original.recv().await {
                if sender.send(exit).await.is_err() {
                    return;
                }
            }
        };
        Ok((
            HeldWorkerExit {
                observed,
                release: Some(release),
            },
            forwarding,
        ))
    }

    pub(in crate::pipeline) struct HeldWorkerExit {
        observed: oneshot::Receiver<()>,
        release: Option<oneshot::Sender<()>>,
    }

    impl HeldWorkerExit {
        pub(in crate::pipeline) async fn wait(&mut self) -> Result<(), oneshot::error::RecvError> {
            (&mut self.observed).await
        }

        pub(in crate::pipeline) fn release(&mut self) {
            self.release.take();
        }
    }

    impl Drop for HeldWorkerExit {
        fn drop(&mut self) {
            self.release();
        }
    }
}
