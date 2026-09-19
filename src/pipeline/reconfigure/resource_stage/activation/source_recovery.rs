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

//! Rebuilds a failed Source's complete Flow before that Source may restart.
//!
//! Reconfigurer owns this operation outside cancelable observation. Old workers
//! remain in current until joined; preparation owns new workers until adoption.
//! Source files are replaced only after both old endpoint owners have exited.
//! Egress files and Sink readers remain untouched throughout the handoff.

use super::super::data_plane::channel_spec;
use super::super::queue_layout::{create_source_channels, flow_egress_bindings, open_bell_region};
use super::runtime_status::handle_plugin_event;
use crate::identifiers::{FlowId, PluginInstanceId, SinkContractId};
use crate::pipeline::channel::{PreparedEgressQueue, PreparedEgressRoutes};
use crate::pipeline::plugin::{PluginInstanceError, PluginInstanceEvent};
use crate::pipeline::reconfigure::operation::{
    DataPlaneOperation, ReconfigureProgress, select_data_plane_operation,
    spawn_blocking_reconfigure_work,
};
use crate::pipeline::reconfigure::runtime_files::{
    INSTANCES_DIRECTORY_NAME, SINK_DIRECTORY_NAME, SOURCE_DIRECTORY_NAME, egress_queue_path,
    flow_channel_bell_path, instance_working_directory, loops_bell_path,
};
use crate::pipeline::reconfigure::{PipelineReconfigureError, PipelineReconfigurer};
use crate::pipeline::runtime::{FlowRuntimeSpec, PipelineRuntime, StartupControl};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use tenon_ipc::bell::BellRegion;

/// Progress of consumed failure events in this finite recovery batch.
#[derive(PartialEq, Eq)]
enum RecoveryStep {
    Pending,
    Rebuilt,
}

impl PipelineReconfigurer {
    /// The selected event proves the old child was actually created and reaped.
    /// The caller must keep awaiting after forwarding a shutdown request.
    #[expect(
        clippy::expect_used,
        reason = "recovery is entered only for Sources bound by the retained current revision"
    )]
    pub(super) async fn rebuild_source_flows(
        &mut self,
        first: PluginInstanceId,
    ) -> Result<ReconfigureProgress<()>, PipelineReconfigureError> {
        // These are consumed failure events, not copied lifecycle states.
        // Keep completed events excluded until this batch returns to Controller,
        // so repeated failures cannot prevent it from taking a newer revision.
        let mut pending = BTreeMap::from([(first, RecoveryStep::Pending)]);
        // debt: Selection scans the bounded failure batch. If large simultaneous
        // failures make selection measurable, add a measured pending-work index.
        while let Some(id) = pending
            .iter()
            .find_map(|(id, step)| (*step == RecoveryStep::Pending).then(|| id.clone()))
        {
            let current = self.current.as_mut().expect("Recovery retains current");
            let (flow_id, flow) = current
                .target
                .document()
                .flows()
                .iter()
                .find(|(_, flow)| flow.source() == &id)
                .expect("Source recovery selects a bound Source");
            let flow_id = flow_id.clone();
            let max_pending_records = flow.max_pending_records();
            let max_record_bytes = flow.max_record_bytes();
            let pipeline_root = current.working_directory.path().to_owned();
            let instances_directory = pipeline_root.join(INSTANCES_DIRECTORY_NAME);
            let source_directory =
                instance_working_directory(&instances_directory, &id).join(SOURCE_DIRECTORY_NAME);
            let spec = channel_spec(&current.target, &flow_id, self.environment.lua_limits());
            let bindings = flow_egress_bindings(&current.target, &flow_id);
            let channel_count = current
                .target
                .document()
                .channel_count(&flow_id, self.environment.available_cpu_count())
                .get();
            // The Flow's Channels keep running across this repair, so its Channel
            // region is reopened, never replaced. Every Egress Queue and Sink
            // region below is a live file this repair only maps.
            let channel_region =
                open_bell_region(&flow_channel_bell_path(&pipeline_root, flow_id.as_str()))?;
            // Hold the existing Flow metric registration across the worker gap.
            let metrics = self
                .metrics
                .as_ref()
                .map(|metrics| metrics.flow(flow_id.as_str()));
            let origin = current.runtime.started_at();
            let selected = BTreeSet::from([flow_id.clone()]);
            current.runtime.stop_flows(&selected);
            current
                .runtime
                .finish_resource_retirement(&selected)
                .await
                .map_err(|source| PipelineReconfigureError::RuntimeTransition(Box::new(source)))?;
            if self.shutdown_requested().is_some() {
                return Ok(ReconfigureProgress::Stopped);
            }

            let startup = Arc::new(StartupControl::new());
            let cancellation = Arc::clone(&startup);
            let diagnostics = self.diagnostics.clone();
            let mut work = spawn_blocking_reconfigure_work(
                Some(Box::new(move || cancellation.abort())),
                move || {
                    std::fs::remove_dir_all(&source_directory).map_err(|source| {
                        PipelineReconfigureError::DirectoryRemove {
                            path: source_directory.clone(),
                            source,
                        }
                    })?;
                    let sink_regions = open_sink_bell_regions(&instances_directory, &bindings)?;
                    let routes = retained_egress_routes(
                        &bindings,
                        channel_count,
                        &instances_directory,
                        &flow_id,
                        &sink_regions,
                    );
                    let channels = create_source_channels(
                        &source_directory,
                        max_pending_records,
                        max_record_bytes,
                        routes,
                        &channel_region,
                    )?;
                    PipelineRuntime::prepare_resources(
                        diagnostics,
                        BTreeMap::from([(flow_id, FlowRuntimeSpec::new(spec, channels, metrics))]),
                        Some(origin),
                        startup,
                    )
                    .map_err(PipelineReconfigureError::RuntimeStart)
                },
            );
            let outcome = self.wait_source_recovery(work.wait(), &mut pending).await;
            let mut prepared = match outcome {
                Ok(DataPlaneOperation::Completed(result)) => result?,
                interrupted => {
                    work.cancel_and_discard().await?;
                    return self.finish_recovery_interruption(interrupted).await;
                }
            };
            let outcome = self
                .wait_source_recovery(prepared.bind(), &mut pending)
                .await;
            match outcome {
                Ok(DataPlaneOperation::Completed(Ok(()))) => {
                    prepared.activate_into(
                        &mut self
                            .current
                            .as_mut()
                            .expect("Recovery retains current")
                            .runtime,
                    );
                }
                other => {
                    tokio::task::spawn_blocking(move || drop(prepared))
                        .await
                        .map_err(PipelineReconfigureError::BlockingTask)?;
                    match other {
                        Ok(DataPlaneOperation::Completed(Err(source))) => {
                            return Err(PipelineReconfigureError::RuntimeTransition(Box::new(
                                source,
                            )));
                        }
                        interrupted => return self.finish_recovery_interruption(interrupted).await,
                    }
                }
            }
            pending.insert(id, RecoveryStep::Rebuilt);
        }
        Ok(ReconfigureProgress::Completed(()))
    }

    #[expect(
        clippy::expect_used,
        reason = "the recovery wait retains current until its result is selected"
    )]
    async fn wait_source_recovery<T>(
        &mut self,
        operation: impl Future<Output = T>,
        pending: &mut BTreeMap<PluginInstanceId, RecoveryStep>,
    ) -> Result<DataPlaneOperation<T>, PluginInstanceError> {
        tokio::pin!(operation);
        loop {
            let current = self.current.as_mut().expect("Recovery retains current");
            tokio::select! {
                biased;
                event = select_data_plane_operation(current.runtime.wait_for_worker_exit(),
                    current.plugins.instances.next_event_where(|id| !pending.contains_key(id)), &mut self.shutdown_receiver) => match event {
                    DataPlaneOperation::WorkerExited => return Ok(DataPlaneOperation::WorkerExited),
                    DataPlaneOperation::Shutdown(shutdown) => return Ok(DataPlaneOperation::Shutdown(shutdown)),
                    DataPlaneOperation::Completed(event) => {
                        let event = event?;
                        if matches!(&event, PluginInstanceEvent::ProcessFailed(_)) {
                            current.runtime.force_wake();
                        }
                        if let PluginInstanceEvent::ProcessFailed(id) = &event
                            && current.target.document().flows().values().any(|flow| flow.source() == id) {
                            pending.insert(id.clone(), RecoveryStep::Pending);
                        } else {
                            handle_plugin_event(event, &mut current.plugins.instances, &current.target,
                                current.working_directory.path(), &self.control, &self.diagnostics,
                                &self.shutdown_receiver, self.environment.available_cpu_count())?;
                        }
                    }
                },
                result = &mut operation => return Ok(DataPlaneOperation::Completed(result)),
            }
        }
    }

    async fn finish_recovery_interruption<T>(
        &mut self,
        interruption: Result<DataPlaneOperation<T>, PluginInstanceError>,
    ) -> Result<ReconfigureProgress<()>, PipelineReconfigureError> {
        match interruption {
            Ok(DataPlaneOperation::Shutdown(_)) => Ok(ReconfigureProgress::Stopped),
            Ok(DataPlaneOperation::WorkerExited) => Err(self.finish_worker_failure().await),
            Err(source) => Err(self.finish_instance_failure(source).await),
            Ok(DataPlaneOperation::Completed(_)) => {
                unreachable!("Completed recovery work is handled before cleanup")
            }
        }
    }
}

/// Opens the own-loop doorbell region of every Sink Instance these routes bind.
///
/// A Source repair leaves every Sink process running, so each region is an
/// existing file that this repair only maps. The repair needs the mapping to
/// ring a Sink loop when it commits an Egress record or reclaims one.
fn open_sink_bell_regions(
    instances_directory: &Path,
    bindings: &HashMap<SinkContractId, Vec<PluginInstanceId>>,
) -> Result<BTreeMap<PluginInstanceId, Arc<BellRegion>>, PipelineReconfigureError> {
    let mut regions = BTreeMap::new();
    for id in bindings.values().flatten() {
        if regions.contains_key(id) {
            continue;
        }
        let path = loops_bell_path(
            &instance_working_directory(instances_directory, id).join(SINK_DIRECTORY_NAME),
        );
        regions.insert(id.clone(), open_bell_region(&path)?);
    }
    Ok(regions)
}

/// Rebuilds one Flow's retained Egress routes from regions this repair opened.
///
/// Recovery restarts only the Source, so every Egress Queue file and Sink region
/// already exists: each target is bound to its exact Path and doorbell, never
/// reopened as a second writer or replaced.
fn retained_egress_routes(
    bindings: &HashMap<SinkContractId, Vec<PluginInstanceId>>,
    channel_count: u32,
    instances_directory: &Path,
    flow_id: &FlowId,
    sink_regions: &BTreeMap<PluginInstanceId, Arc<BellRegion>>,
) -> Vec<PreparedEgressRoutes> {
    (0..channel_count)
        .map(|index| {
            bindings
                .iter()
                .map(|(contract, sinks)| {
                    (
                        contract.clone(),
                        sinks
                            .iter()
                            .map(|sink| {
                                (
                                    sink.clone(),
                                    PreparedEgressQueue::Retained {
                                        path: egress_queue_path(
                                            &instance_working_directory(instances_directory, sink),
                                            flow_id.as_str(),
                                            index,
                                        ),
                                        peer_region: Arc::clone(&sink_regions[sink]),
                                    },
                                )
                            })
                            .collect(),
                    )
                })
                .collect()
        })
        .collect()
}

#[cfg(all(test, not(feature = "loom-model")))]
mod tests;
