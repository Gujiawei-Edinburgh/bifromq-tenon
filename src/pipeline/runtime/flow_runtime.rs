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

//! Ownership and startup protocol for one fixed Flow Channel generation.
//!
//! One [`FlowRuntime`] owns every FlowChannel control handle and worker
//! thread created from the same FlowChannel specification. FlowChannel threads
//! open their own thread-affine Lua VM, report startup, and then wait behind one
//! shared control.
//! This module does not decide when a revision may cut over; it only exposes
//! cloned replacement controls after the complete generation is running.

use std::collections::BTreeSet;
use std::io;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::mpsc::{self as sync_mpsc, SyncSender};
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use std::time::Instant;

use tokio::sync::mpsc::{self as async_mpsc, Sender};

use super::error::{PipelineRuntimeError, PipelineRuntimeShutdownError, PipelineWorker};
use super::runtime_spec::FlowRuntimeSpec;
use super::startup::{StartupControl, StartupDecision};
use super::worker::{
    WorkerCompletionContext, WorkerExit, WorkerObservation, WorkerRunResult, WorkerSet, WorkerTask,
    WorkerThread, abort_process_if_stop_failed, join_worker_threads,
};
use crate::identifiers::{FlowId, PluginInstanceId};
use crate::pipeline::channel::PreparedEgressRoutes;
use crate::pipeline::channel::metrics::{ChannelMetrics, FlowObservation};
use crate::pipeline::channel::{
    ChannelWake, ChannelWakeError, FlowChannel, FlowChannelBells, FlowChannelCommandControl,
    FlowChannelControl, FlowChannelError, FlowChannelQueuePaths, FlowChannelSpec,
};
use crate::pipeline::diagnostics::{ChannelDiagnosticPublisher, PipelineDiagnosticsPublisher};

/// Complete control and thread ownership for one Flow's FlowChannel generation.
pub(super) struct FlowRuntime {
    control: ChannelRuntimeControl,
    workers: WorkerSet,
    // This registration survives a worker's temporary absence during recovery.
    _metrics: Option<Arc<FlowObservation>>,
}

impl FlowRuntime {
    pub(super) fn prepare_with_spawner(
        flow_id: &FlowId,
        spec: FlowRuntimeSpec,
        diagnostics: &PipelineDiagnosticsPublisher,
        pipeline_started_at: Instant,
        startup: Arc<StartupControl>,
        spawner: &mut impl FnMut(String, WorkerTask) -> io::Result<JoinHandle<()>>,
    ) -> Result<(Self, Vec<ChannelBindingCompletion>), PipelineRuntimeError> {
        let ChannelLaunchMaterials {
            flow_control,
            metrics,
            launches,
        } = build_channel_launches(flow_id, spec, diagnostics)?;
        let spawned = spawn_channel_workers(launches, pipeline_started_at, &startup, spawner)?;
        let CollectedChannelStartup {
            channels,
            workers,
            bindings,
        } = collect_channel_startup(flow_id, spawned, &startup)?;
        Ok((
            Self {
                control: ChannelRuntimeControl {
                    flow_control,
                    channels,
                },
                workers,
                _metrics: metrics,
            },
            bindings,
        ))
    }

    pub(super) fn command_controls(
        &self,
    ) -> Result<Box<[FlowChannelCommandControl]>, PipelineRuntimeError> {
        self.control.command_controls()
    }

    pub(super) fn drain(&self) -> Result<(), PipelineRuntimeShutdownError> {
        self.control.drain()
    }

    pub(super) fn drain_egress(&self) -> Result<(), PipelineRuntimeShutdownError> {
        self.control.drain_egress()
    }

    /// Records Sink Instances that left the Pipeline in every Channel that still
    /// waits on them, then wakes those waits.
    pub(super) fn depart_egress_targets(
        &self,
        instances: &BTreeSet<PluginInstanceId>,
    ) -> Result<(), PipelineRuntimeShutdownError> {
        self.control.depart_egress_targets(instances)
    }

    /// Wakes every Channel after a peer process failure without changing the
    /// departure facts. The peer may be restarted with the same identity.
    pub(super) fn force_wake(&self) -> Result<(), PipelineRuntimeShutdownError> {
        self.control.force_wake()
    }

    pub(super) fn stop(&self) -> Result<(), PipelineRuntimeShutdownError> {
        self.control.stop()
    }

    fn has_stop_request(&self) -> bool {
        self.control.flow_control.has_stop_request()
    }

    /// Freezes this Flow's exit interpretation, then publishes its final Stop.
    ///
    /// The interpretation must be frozen before Stop is published, because Stop
    /// itself causes clean consequence exits that would otherwise hide an
    /// already observed failure. This owner holds both the WorkerSet that
    /// interprets exits and the control that publishes Stop, so the order is
    /// enforced here. The caller passes the returned pair to `finish_worker_sets`.
    pub(super) fn begin_final_stop(&mut self) -> (WorkerCompletionContext, &mut WorkerSet) {
        let context = self.workers.completion_context(self.has_stop_request());
        abort_process_if_stop_failed(self.control.stop());
        (context, &mut self.workers)
    }

    pub(super) fn poll_next_exit(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Option<WorkerObservation>> {
        if self.workers.observed_exit_count() == self.workers.worker_count() {
            return Poll::Ready(None);
        }
        self.workers.poll_next_exit(context).map(Some)
    }

    pub(super) fn observe_exit_now(&mut self) {
        self.workers.observe_exit_now();
    }

    pub(super) fn has_observed_event(&self) -> bool {
        self.workers.has_observed_event()
    }

    pub(super) fn worker_count(&self) -> usize {
        self.workers.worker_count()
    }

    pub(super) fn worker_set_mut(&mut self) -> &mut WorkerSet {
        &mut self.workers
    }
}

impl Drop for FlowRuntime {
    fn drop(&mut self) {
        if self.workers.worker_count() == 0 {
            return;
        }
        abort_process_if_stop_failed(self.control.stop());
        self.workers.join();
    }
}

/// A one-shot binding observation, consumed before target activation.
pub(super) struct ChannelBindingCompletion {
    flow_id: FlowId,
    channel_index: u32,
    completion: tokio::sync::oneshot::Receiver<Result<(), FlowChannelError>>,
}

impl ChannelBindingCompletion {
    pub(super) async fn wait(self) -> Result<(), PipelineRuntimeError> {
        self.completion
            .await
            .map_err(|_| PipelineRuntimeError::FlowChannelOpenPanicked {
                flow_id: self.flow_id.clone(),
                channel_index: self.channel_index,
            })?
            .map_err(|source| PipelineRuntimeError::FlowChannelOpen {
                flow_id: self.flow_id,
                channel_index: self.channel_index,
                source,
            })
    }
}

/// Builds the one-generation launch inputs before any worker thread owns them.
struct ChannelLaunchMaterials {
    flow_control: Arc<FlowChannelControl>,
    metrics: Option<Arc<FlowObservation>>,
    launches: Vec<ChannelLaunch>,
}

fn build_channel_launches(
    flow_id: &FlowId,
    spec: FlowRuntimeSpec,
    diagnostics: &PipelineDiagnosticsPublisher,
) -> Result<ChannelLaunchMaterials, PipelineRuntimeError> {
    let FlowRuntimeSpec {
        metrics,
        channel_spec,
        channels,
    } = spec;
    assert!(
        !channels.is_empty(),
        "validated Flow parallelism is positive"
    );
    let channel_count = channels.len();
    let flow_control = Arc::new(FlowChannelControl::new());
    let mut launches = Vec::new();
    launches
        .try_reserve(channel_count)
        .map_err(|_| PipelineRuntimeError::ResourceLimitExceeded)?;
    for (index, channel) in channels.into_vec().into_iter().enumerate() {
        let channel_index =
            u32::try_from(index).map_err(|_| PipelineRuntimeError::ResourceLimitExceeded)?;
        assert!(
            channel_spec.matches_routes(&channel.routes),
            "the same revision creates the registry and Queue bindings"
        );
        launches.push(ChannelLaunch {
            flow_id: flow_id.clone(),
            channel_index,
            queues: channel.queues,
            bells: channel.bells,
            routes: channel.routes,
            spec: channel_spec.clone(),
            diagnostics: diagnostics.flow_channel(flow_id, channel_index),
            metrics: metrics
                .as_ref()
                .map_or_else(ChannelMetrics::default, |flow| flow.channel(channel_index)),
            flow_control: Arc::clone(&flow_control),
        });
    }
    Ok(ChannelLaunchMaterials {
        flow_control,
        metrics,
        launches,
    })
}

/// Spawns every Channel while the local worker vector owns each successful handle.
fn spawn_channel_workers(
    launches: Vec<ChannelLaunch>,
    pipeline_started_at: Instant,
    startup: &Arc<StartupControl>,
    spawner: &mut impl FnMut(String, WorkerTask) -> io::Result<JoinHandle<()>>,
) -> Result<SpawnedChannels, PipelineRuntimeError> {
    let channel_count = launches.len();
    let mut workers = Vec::new();
    workers
        .try_reserve(channel_count)
        .map_err(|_| PipelineRuntimeError::ResourceLimitExceeded)?;
    let mut channel_controls = Vec::new();
    channel_controls
        .try_reserve(channel_count)
        .map_err(|_| PipelineRuntimeError::ResourceLimitExceeded)?;
    let (startup_sender, startup_receiver) = sync_mpsc::sync_channel(channel_count);
    // Every worker publishes at most one terminal event, so this exact
    // per-generation capacity makes WorkerTask::run's non-blocking send
    // infallible while this Channel owner is alive.
    let (exit_sender, exits) = async_mpsc::channel(channel_count);
    for launch in launches {
        let channel_index = launch.channel_index;
        let worker = PipelineWorker {
            flow_id: launch.flow_id.clone(),
            channel_index,
        };
        let thread_name = format!(
            "tenon-flow-{}-channel-{channel_index}",
            launch.flow_id.as_str()
        );
        let task = channel_task(
            launch,
            pipeline_started_at,
            Arc::clone(startup),
            startup_sender.clone(),
            exit_sender.clone(),
        );
        let handle = match spawner(thread_name, task) {
            Ok(handle) => handle,
            Err(source) => {
                abort_startup(startup, &mut workers);
                return Err(PipelineRuntimeError::WorkerSpawn { worker, source });
            }
        };
        workers.push(WorkerThread::new(worker, handle));
    }
    drop(startup_sender);
    drop(exit_sender);
    Ok(SpawnedChannels {
        startup_receiver,
        exits,
        channel_controls,
        workers,
    })
}

/// Collects a complete control set or aborts and joins every started worker.
struct CollectedChannelStartup {
    channels: Box<[ChannelControl]>,
    workers: WorkerSet,
    bindings: Vec<ChannelBindingCompletion>,
}

fn collect_channel_startup(
    flow_id: &FlowId,
    mut spawned: SpawnedChannels,
    startup: &StartupControl,
) -> Result<CollectedChannelStartup, PipelineRuntimeError> {
    let channel_count = spawned.workers.len();
    let mut bindings = Vec::new();
    let mut channel_controls = spawned.channel_controls;

    // Every FlowChannel must publish a usable control set before the caller can
    // start Egress and release the shared gate. A failure therefore leaves
    // no partially published FlowChannel generation.
    while channel_controls.len() < channel_count {
        match spawned.startup_receiver.recv() {
            Ok(ChannelStartup::Ready {
                channel_index,
                wake,
                command_control,
                binding,
            }) => {
                bindings.push(ChannelBindingCompletion {
                    flow_id: flow_id.clone(),
                    channel_index,
                    completion: binding,
                });
                channel_controls.push(ChannelControl {
                    flow_id: flow_id.clone(),
                    channel_index,
                    wake,
                    command_control,
                });
            }
            Ok(ChannelStartup::Failed {
                channel_index,
                source,
            }) => {
                abort_startup(startup, &mut spawned.workers);
                return Err(PipelineRuntimeError::FlowChannelOpen {
                    flow_id: flow_id.clone(),
                    channel_index,
                    source,
                });
            }
            Ok(ChannelStartup::Panicked { channel_index }) => {
                abort_startup(startup, &mut spawned.workers);
                return Err(PipelineRuntimeError::FlowChannelOpenPanicked {
                    flow_id: flow_id.clone(),
                    channel_index,
                });
            }
            Err(_) => {
                abort_startup(startup, &mut spawned.workers);
                return Err(PipelineRuntimeError::InternalEventChannelClosed);
            }
        }
    }
    channel_controls.sort_unstable_by_key(|channel| channel.channel_index);
    Ok(CollectedChannelStartup {
        channels: channel_controls.into_boxed_slice(),
        workers: WorkerSet::new(spawned.exits, spawned.workers),
        bindings,
    })
}

/// Startup receiver, Channel controls, terminal events and thread handles for one generation.
struct SpawnedChannels {
    startup_receiver: sync_mpsc::Receiver<ChannelStartup>,
    exits: async_mpsc::Receiver<WorkerExit>,
    channel_controls: Vec<ChannelControl>,
    workers: Vec<WorkerThread>,
}

/// Per-Channel Queue wake and command controls.
struct ChannelControl {
    // This identity stays with independently stopped worker handles when a
    // Channel generation leaves the live Flow map during replacement.
    flow_id: FlowId,
    channel_index: u32,
    wake: ChannelWake,
    command_control: FlowChannelCommandControl,
}

impl ChannelControl {
    fn wake_for_drain(&self) -> Result<(), ChannelWakeError> {
        // One doorbell carries every fact the Channel waits on, so a directive
        // is published first and then rung once, whatever it asks for.
        let queue_wake = self.wake.wake();
        self.command_control.abort_active();
        queue_wake
    }

    fn wake_for_stop(&self) -> Result<(), ChannelWakeError> {
        let queue_wakes = self.wake.wake();
        self.command_control.abort_active();
        queue_wakes
    }
}

/// One Flow directive plus its complete, index-ordered Channel wake set.
struct ChannelRuntimeControl {
    flow_control: Arc<FlowChannelControl>,
    channels: Box<[ChannelControl]>,
}

impl ChannelRuntimeControl {
    fn command_controls(&self) -> Result<Box<[FlowChannelCommandControl]>, PipelineRuntimeError> {
        // A resource operation needs only cloneable command handles. The
        // Channel generation keeps exclusive ownership of Queue wake controls
        // and JoinHandles throughout the operation.
        let mut controls = Vec::new();
        controls
            .try_reserve_exact(self.channels.len())
            .map_err(|_| PipelineRuntimeError::ResourceLimitExceeded)?;
        controls.extend(
            self.channels
                .iter()
                .map(|channel| channel.command_control.clone()),
        );
        Ok(controls.into_boxed_slice())
    }

    fn drain(&self) -> Result<(), PipelineRuntimeShutdownError> {
        self.flow_control.request_drain();
        self.for_each(ChannelControl::wake_for_drain)
    }

    fn drain_egress(&self) -> Result<(), PipelineRuntimeShutdownError> {
        self.flow_control.request_egress_drain();
        self.for_each(ChannelControl::wake_for_stop)
    }

    fn stop(&self) -> Result<(), PipelineRuntimeShutdownError> {
        self.flow_control.request_stop();
        self.for_each(ChannelControl::wake_for_stop)
    }

    /// Records departed Sink Instances in every Channel of this generation.
    fn depart_egress_targets(
        &self,
        instances: &BTreeSet<PluginInstanceId>,
    ) -> Result<(), PipelineRuntimeShutdownError> {
        self.for_each(|channel| channel.wake.depart_egress_targets(instances))
    }

    fn force_wake(&self) -> Result<(), PipelineRuntimeShutdownError> {
        self.for_each(|channel| channel.wake.force_wake())
    }

    fn for_each(
        &self,
        request: impl Fn(&ChannelControl) -> Result<(), ChannelWakeError>,
    ) -> Result<(), PipelineRuntimeShutdownError> {
        let mut first_error = None;
        for channel in &self.channels {
            if let Err(source) = request(channel)
                && first_error.is_none()
            {
                first_error = Some(PipelineRuntimeShutdownError {
                    flow_id: channel.flow_id.clone(),
                    channel_index: channel.channel_index,
                    source,
                });
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// Exactly one startup report sent by each spawned Channel thread.
enum ChannelStartup {
    Ready {
        channel_index: u32,
        wake: ChannelWake,
        command_control: FlowChannelCommandControl,
        binding: tokio::sync::oneshot::Receiver<Result<(), FlowChannelError>>,
    },
    Failed {
        channel_index: u32,
        source: FlowChannelError,
    },
    Panicked {
        channel_index: u32,
    },
}

/// Owned inputs moved into one Channel thread before it opens its Lua VM.
struct ChannelLaunch {
    flow_id: FlowId,
    channel_index: u32,
    queues: FlowChannelQueuePaths,
    bells: FlowChannelBells,
    spec: FlowChannelSpec,
    diagnostics: ChannelDiagnosticPublisher,
    metrics: ChannelMetrics,
    routes: PreparedEgressRoutes,
    flow_control: Arc<FlowChannelControl>,
}

fn channel_task(
    launch: ChannelLaunch,
    pipeline_started_at: Instant,
    startup: Arc<StartupControl>,
    startup_sender: SyncSender<ChannelStartup>,
    exit_sender: Sender<WorkerExit>,
) -> WorkerTask {
    let worker = PipelineWorker {
        flow_id: launch.flow_id.clone(),
        channel_index: launch.channel_index,
    };
    let work = Box::new(move || {
        let ChannelLaunch {
            flow_id,
            channel_index,
            queues,
            bells,
            spec,
            diagnostics,
            metrics,
            routes,
            flow_control,
        } = launch;
        let startup_for_lua = Arc::clone(&startup);
        let opened = panic::catch_unwind(AssertUnwindSafe(|| {
            FlowChannel::prepare(
                queues,
                bells,
                pipeline_started_at,
                spec,
                diagnostics,
                routes,
                flow_control,
                move || startup_for_lua.is_aborted(),
                metrics,
            )
        }));
        let (channel, bound) = match opened {
            Ok(Ok((channel, wake, command_control))) => {
                let (bound, binding) = tokio::sync::oneshot::channel();
                if startup_sender
                    .send(ChannelStartup::Ready {
                        channel_index,
                        wake,
                        command_control,
                        binding,
                    })
                    .is_err()
                {
                    return None;
                }
                (channel, bound)
            }
            Ok(Err(source)) => {
                let _ = startup_sender.send(ChannelStartup::Failed {
                    channel_index,
                    source,
                });
                return None;
            }
            Err(_) => {
                let _ = startup_sender.send(ChannelStartup::Panicked { channel_index });
                return None;
            }
        };

        if startup.wait_for_binding() == StartupDecision::Abort {
            return None;
        }
        let channel = match channel.bind() {
            Ok(channel) => {
                let _ = bound.send(Ok(()));
                channel
            }
            Err(error) => {
                let _ = bound.send(Err(error));
                return None;
            }
        };

        if startup.wait() == StartupDecision::Abort {
            return None;
        }
        Some(WorkerExit {
            flow_id,
            channel_index,
            result: WorkerRunResult::Returned(channel.run()),
        })
    });
    WorkerTask {
        worker,
        work,
        exit_sender,
    }
}

fn abort_startup(startup: &StartupControl, workers: &mut Vec<WorkerThread>) {
    // Abort releases ready siblings and interrupts siblings still loading Lua.
    startup.abort();
    join_worker_threads(workers);
}
