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

//! Creates candidate Queue files and Bell Regions and binds their endpoints.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::{NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::contracts::core::PipelineEnvironment;
use crate::identifiers::{FlowId, PluginInstanceId, SinkContractId};
use crate::pipeline::channel::{
    FlowChannelBells, FlowChannelQueuePaths, PreparedEgressQueue, PreparedEgressRoutes,
};
use crate::pipeline::ingress_queue::{
    COMPLETION_MAX_PAYLOAD_SIZE, completion_capacity, submission_capacity,
};
use crate::pipeline::reconfigure::PipelineReconfigureError;
use crate::pipeline::reconfigure::plan::{CompiledResourceChanges, FlowChange, ResourceMutation};
use crate::pipeline::reconfigure::revision::PipelineRevision;
use crate::pipeline::reconfigure::runtime_files::{
    INSTANCES_DIRECTORY_NAME, SINK_DIRECTORY_NAME, create_private_directory, create_queue,
    egress_queue_path, ensure_bell_region_file, ensure_private_directory, flow_channel_bell_path,
    flow_directory, instance_working_directory, loops_bell_path,
};
use crate::pipeline::runtime::ChannelRuntimeSpec;
use tenon_ipc::bell::BellRegion;
use tenon_ipc::queue::QueueWriter;

use super::directories::{CANDIDATE_DIRECTORY_NAME, ResourceDirectories};

/// The candidate Queue files and Bell Regions of one resource batch.
///
/// The Bell Regions never travel past this module: binding a Queue endpoint
/// consumes the region, and every later waiter or ringer reaches the same
/// mapping through the endpoint it already holds.
pub(super) struct PreparedQueueLayout {
    pub(super) channel_routes: BTreeMap<FlowId, Vec<PreparedEgressRoutes>>,
    /// The Channel doorbell region of every Flow whose Queues this batch creates.
    pub(super) flow_bells: BTreeMap<FlowId, Arc<BellRegion>>,
}

/// Creates candidate directories and Queue files, then returns every target route.
pub(super) fn prepare_routes(
    changes: &CompiledResourceChanges,
    environment: &PipelineEnvironment,
    working_directory: &mut ResourceDirectories,
) -> Result<PreparedQueueLayout, PipelineReconfigureError> {
    let root = working_directory.path().to_owned();
    stage_instance_directories(changes, working_directory, &root)?;
    let flow_bells = channel_bell_regions(changes, environment, &root)?;
    let sink_bells = create_sink_bell_regions(changes, &root)?;
    let channel_routes =
        prepare_egress_routes(changes, environment, &root, &flow_bells, &sink_bells)?;
    Ok(PreparedQueueLayout {
        channel_routes,
        flow_bells,
    })
}

pub(super) fn open_bell_region(path: &Path) -> Result<Arc<BellRegion>, PipelineReconfigureError> {
    BellRegion::open(path).map_err(|source| PipelineReconfigureError::BellRegionOpen {
        path: path.to_owned(),
        source,
    })
}

/// Resolves the Channel doorbell region of every Flow this batch binds routes to.
///
/// The Flow's Channels park in this region and the Flow's Source and Sinks ring
/// it, so a batch that rebuilds the Flow's Queue layout passes the new Channel
/// count and lets the Region file decide: the same count reuses the mapping the
/// running Channels park in, and only a different count replaces it. A Queue
/// header names the slot its waiting loop published, and that ordinal only means
/// anything inside the mapping the loop published it into.
///
/// A Flow this batch re-routes without resizing keeps the region its running
/// Channels already park in: only that Flow's Egress files moved, and the
/// Channel count those slots are numbered by did not change.
fn channel_bell_regions(
    changes: &CompiledResourceChanges,
    environment: &PipelineEnvironment,
    root: &Path,
) -> Result<BTreeMap<FlowId, Arc<BellRegion>>, PipelineReconfigureError> {
    let mut regions = BTreeMap::new();
    for flow_id in changes.new_queue_flows() {
        let slot_count = changes
            .target
            .document()
            .channel_count(flow_id, environment.available_cpu_count());
        let replacing_live_flow = changes.flows[flow_id] == FlowChange::ReplaceQueues;
        // Retained Sink readers may still hold the live Bell Region mapping even
        // when this Flow's Queue files are being replaced. Reuse that inode when
        // the slot numbering remains compatible; renaming a candidate over it
        // would leave the new Channel and retained Sink ringing different maps.
        let reuse_live_region =
            replacing_live_flow && !should_replace_channel_bell(changes, root, flow_id);
        let region_root = if replacing_live_flow && !reuse_live_region {
            root.join(CANDIDATE_DIRECTORY_NAME)
        } else {
            root.to_owned()
        };
        if replacing_live_flow && !reuse_live_region {
            ensure_private_directory(
                &root
                    .join(CANDIDATE_DIRECTORY_NAME)
                    .join(super::super::runtime_files::FLOWS_DIRECTORY_NAME),
            )?;
        }
        ensure_private_directory(&flow_directory(&region_root, flow_id.as_str()))?;
        let path = flow_channel_bell_path(&region_root, flow_id.as_str());
        ensure_bell_region_file(&path, slot_count)?;
        regions.insert(flow_id.clone(), open_bell_region(&path)?);
    }
    for queue in &changes.egress_queues {
        if queue.mutation == ResourceMutation::Remove || regions.contains_key(&queue.flow_id) {
            continue;
        }
        let path = flow_channel_bell_path(root, queue.flow_id.as_str());
        regions.insert(queue.flow_id.clone(), open_bell_region(&path)?);
    }
    Ok(regions)
}

/// Returns whether a Flow replacement needs a new Channel Bell Region inode.
///
/// A retained Sink process may still hold the current live mapping. Equal slot
/// counts keep every Queue header ordinal meaningful in that mapping, so the
/// inode must be reused. A changed or malformed region is replaced only after
/// all retained users that depend on its ordinals have been replaced.
pub(super) fn should_replace_channel_bell(
    changes: &CompiledResourceChanges,
    root: &Path,
    flow_id: &FlowId,
) -> bool {
    if changes.flows.get(flow_id) != Some(&FlowChange::ReplaceQueues) {
        return false;
    }
    let target_slots = changes
        .target
        .document()
        .channel_count(flow_id, changes.available_cpu_count);
    let live_path = flow_channel_bell_path(root, flow_id.as_str());
    BellRegion::open(&live_path)
        .map(|region| region.slot_count() != target_slots)
        .unwrap_or(true)
}

/// Returns every Sink Instance this batch binds Egress to.
///
/// Routes exist for every Flow this batch does not remove, so this is exactly
/// the set whose own-loop region a retained Channel may have to ring.
pub(super) fn batch_sink_instances(
    changes: &CompiledResourceChanges,
) -> BTreeSet<PluginInstanceId> {
    changes
        .flows
        .iter()
        .filter(|(_, change)| **change != FlowChange::Remove)
        .flat_map(|(flow_id, _)| changes.target.document().flows()[flow_id].sinks())
        .cloned()
        .collect()
}

/// Creates the own-loop region of every Sink Instance these routes bind.
///
/// A Sink Instance runs one Egress loop over every Egress Queue it reads, so its
/// region holds the single doorbell that loop parks on and each of those Queues
/// publishes that same ordinal. A retained process keeps the region it already
/// published into: its Queues did not move, so neither did the mapping those
/// publications live in.
fn create_sink_bell_regions(
    changes: &CompiledResourceChanges,
    root: &Path,
) -> Result<BTreeMap<PluginInstanceId, Arc<BellRegion>>, PipelineReconfigureError> {
    let instances = root.join(INSTANCES_DIRECTORY_NAME);
    let candidates = root.join(CANDIDATE_DIRECTORY_NAME);
    let mut regions = BTreeMap::new();
    for instance_id in batch_sink_instances(changes) {
        // A retained process keeps the region it already published slots into;
        // a launched process gets a fresh one, because those publications died
        // with the mapping that held them.
        let (path, launched) = match changes.process_mutation(&instance_id) {
            // `begin_install` moves the candidate region onto the live name with
            // the Egress Queues this Instance replaced.
            Some(ResourceMutation::Replace) => {
                let directory =
                    instance_working_directory(&candidates, &instance_id).join(SINK_DIRECTORY_NAME);
                (
                    ensure_private_directory(&directory).map(|()| loops_bell_path(&directory))?,
                    true,
                )
            }
            Some(ResourceMutation::Add) => {
                let directory =
                    instance_working_directory(&instances, &instance_id).join(SINK_DIRECTORY_NAME);
                (
                    create_private_directory(&directory).map(|()| loops_bell_path(&directory))?,
                    true,
                )
            }
            _ => (
                loops_bell_path(
                    &instance_working_directory(&instances, &instance_id).join(SINK_DIRECTORY_NAME),
                ),
                false,
            ),
        };
        if launched {
            ensure_bell_region_file(&path, SINK_SLOT_COUNT)?;
        }
        regions.insert(instance_id, open_bell_region(&path)?);
    }
    Ok(regions)
}

/// One Sink Instance serves every Egress Queue it reads from one loop, so its
/// own-loop region holds exactly the one doorbell that loop parks on.
const SINK_SLOT_COUNT: NonZeroU32 = NonZeroU32::MIN;

/// Creates the live instance directories, and per-instance candidate directories
/// for existing instances that gain new files. Cleanup authority stays in
/// `working_directory`.
fn stage_instance_directories(
    changes: &CompiledResourceChanges,
    working_directory: &mut ResourceDirectories,
    root: &Path,
) -> Result<(), PipelineReconfigureError> {
    let target = &changes.target;
    let instances_directory = root.join(INSTANCES_DIRECTORY_NAME);
    for instance_id in changes.added_instances() {
        working_directory.create_instance(&instance_working_directory(
            &instances_directory,
            instance_id,
        ))?;
    }
    // Exactly the Instances whose Source or Sink directory this batch writes
    // into and whose process is not already launching into its final directory.
    let new_file_instances: BTreeSet<_> = changes
        .new_queue_flows()
        .map(|id| target.document().flows()[id].source().clone())
        .chain(
            batch_sink_instances(changes)
                .into_iter()
                .filter(|id| changes.process_mutation(id) == Some(ResourceMutation::Replace)),
        )
        .filter(|id| changes.process_mutation(id) != Some(ResourceMutation::Add))
        .collect();
    if !new_file_instances.is_empty() {
        working_directory.create_candidate()?;
        for instance_id in new_file_instances {
            create_private_directory(&instance_working_directory(
                &root.join(CANDIDATE_DIRECTORY_NAME),
                &instance_id,
            ))?;
        }
    }
    Ok(())
}

/// Creates every new Egress Queue and assembles the per-Flow routes. New writers
/// move into the result; retained Queues remain unopened until their old writer exits.
#[allow(
    clippy::expect_used,
    reason = "the compiled plan and Queue layout establish these internal identities"
)]
fn prepare_egress_routes(
    changes: &CompiledResourceChanges,
    environment: &PipelineEnvironment,
    root: &Path,
    flow_bells: &BTreeMap<FlowId, Arc<BellRegion>>,
    sink_bells: &BTreeMap<PluginInstanceId, Arc<BellRegion>>,
) -> Result<BTreeMap<FlowId, Vec<PreparedEgressRoutes>>, PipelineReconfigureError> {
    let target = &changes.target;
    let instances_directory = root.join(INSTANCES_DIRECTORY_NAME);

    let mut egress_targets = BTreeMap::new();
    let mut created_directories = BTreeSet::new();
    for queue in &changes.egress_queues {
        if queue.mutation == ResourceMutation::Remove {
            continue;
        }
        let flow = &target.document().flows()[&queue.flow_id];
        let record_queue_capacity =
            submission_capacity(flow.max_pending_records(), flow.max_record_bytes())
                .expect("Runner verifies every Flow's Queue capacity");
        let instance_directory = staged_instance_directory(changes, root, &queue.instance_id);
        let queue_path = egress_queue_path(
            &instance_directory,
            queue.flow_id.as_str(),
            queue.channel_index,
        );
        let flow_directory = queue_path
            .parent()
            .expect("Queue layout has a Flow directory");
        for directory in [
            instance_directory.join(SINK_DIRECTORY_NAME),
            flow_directory.to_owned(),
        ] {
            if created_directories.insert(directory.clone()) {
                ensure_private_directory(&directory)?;
            }
        }
        create_queue(&queue_path, record_queue_capacity, flow.max_record_bytes())?;
        // The Channel's own slot carries no cause: every fact the Channel waits
        // on reaches this one address, and the retained Source that releases the
        // Completion of this Channel reads the same slot from its Queue header.
        let bell = flow_bells[&queue.flow_id]
            .loop_bell(queue.channel_index)
            .expect("one Channel slot exists per Channel of its Flow");
        let writer = QueueWriter::open(
            &queue_path,
            bell,
            Arc::clone(&sink_bells[&queue.instance_id]),
        )
        .map_err(|source| PipelineReconfigureError::QueueOpen {
            path: queue_path,
            source,
        })?;
        egress_targets.insert(
            (
                queue.instance_id.clone(),
                queue.flow_id.clone(),
                queue.channel_index,
            ),
            writer,
        );
    }

    let mut channel_routes = BTreeMap::new();
    for (flow_id, change) in &changes.flows {
        if *change == FlowChange::Remove {
            continue;
        }
        let bindings = flow_egress_bindings(target, flow_id);
        let mut channels = Vec::new();
        for channel_index in 0..target
            .document()
            .channel_count(flow_id, environment.available_cpu_count())
            .get()
        {
            let mut routes = HashMap::new();
            for (contract, instances) in &bindings {
                let mut targets = BTreeMap::new();
                for id in instances {
                    let material = match egress_targets.remove(&(
                        id.clone(),
                        flow_id.clone(),
                        channel_index,
                    )) {
                        Some(writer) => PreparedEgressQueue::New(writer),
                        // A retained Queue is opened only after the writer of the
                        // retired generation has actually exited. Its peer region
                        // is this batch's, so that a Sink launched by this batch
                        // is woken in the mapping it publishes into.
                        None => PreparedEgressQueue::Retained {
                            path: egress_queue_path(
                                &instance_working_directory(&instances_directory, id),
                                flow_id.as_str(),
                                channel_index,
                            ),
                            peer_region: Arc::clone(&sink_bells[id]),
                        },
                    };
                    targets.insert(id.clone(), material);
                }
                routes.insert(contract.clone(), targets);
            }
            channels.push(routes);
        }
        channel_routes.insert(flow_id.clone(), channels);
    }
    assert!(
        egress_targets.is_empty(),
        "every new Queue is assigned to its exact Channel"
    );
    Ok(channel_routes)
}

/// Creates one Source side's Queue pair per Channel and the doorbell region its
/// own loop parks on.
///
/// The Source's own region holds one slot per waiting loop, not per Channel:
/// every Submission Queue publishes the Submission loop's slot and every
/// Completion Queue publishes the Completion loop's, whatever the Channel count
/// is. `channel_region` is the Flow's Channel region; each Channel waits on its
/// own slot there, and this side only rings it.
#[expect(
    clippy::expect_used,
    reason = "the fixed Completion payload size is nonzero"
)]
pub(super) fn create_source_channels(
    source_directory: &Path,
    max_pending_records: NonZeroU64,
    max_record_bytes: NonZeroU64,
    routes: Vec<PreparedEgressRoutes>,
    channel_region: &Arc<BellRegion>,
) -> Result<Vec<ChannelRuntimeSpec>, PipelineReconfigureError> {
    ensure_private_directory(source_directory)?;
    let mut channels = Vec::new();
    channels
        .try_reserve_exact(routes.len())
        .map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?;
    let source_slot_count =
        NonZeroU32::new(SOURCE_LOOP_SLOT_COUNT).expect("the Source owns one slot per waiting loop");
    let source_bell_path = loops_bell_path(source_directory);
    // The Source that publishes into this region is being launched into a
    // directory this batch staged, so a region already at its final slot count
    // is one no live Source loop can be parked on and is reused as is.
    ensure_bell_region_file(&source_bell_path, source_slot_count)?;
    let source_region = open_bell_region(&source_bell_path)?;
    let submission_capacity = submission_capacity(max_pending_records, max_record_bytes)
        .expect("Runner verifies every Flow's Queue capacity");
    let completion_capacity = completion_capacity(max_pending_records)
        .expect("Completion frames are smaller than the verified record frames");
    let completion_max_payload = NonZeroU64::new(COMPLETION_MAX_PAYLOAD_SIZE as u64)
        .expect("The fixed Completion payload size is nonzero");
    for (index, routes) in routes.into_iter().enumerate() {
        let submission = source_directory.join(format!("submission-{index}.queue"));
        let completion = source_directory.join(format!("completion-{index}.queue"));
        create_queue(&submission, submission_capacity, max_record_bytes)?;
        create_queue(&completion, completion_capacity, completion_max_payload)?;
        let channel_index =
            u32::try_from(index).map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?;
        let channel_bell = channel_region
            .loop_bell(channel_index)
            .expect("the Channel region holds one slot per Channel");
        channels.push(ChannelRuntimeSpec::new(
            FlowChannelQueuePaths::new(submission, completion),
            FlowChannelBells::new(
                // The Channel waits on its own slot of its Flow's Channel
                // region; both of its Ingress endpoints publish that slot into
                // their headers, so the Source rings this one address.
                channel_bell,
                // Committing a Completion or releasing a Submission frame rings
                // the slot the Source published in its own loop region. One slot
                // belongs to the Submission loop and one to the Completion loop,
                // so a ring always reaches the loop that waits on that role.
                Arc::clone(&source_region),
            ),
            routes,
        ));
    }
    Ok(channels)
}

/// The Source owns one slot per waiting loop, independent of the Channel count.
const SOURCE_LOOP_SLOT_COUNT: u32 = 2;

pub(super) fn flow_egress_bindings(
    target: &PipelineRevision,
    flow_id: &FlowId,
) -> HashMap<SinkContractId, Vec<PluginInstanceId>> {
    let mut bindings: HashMap<_, Vec<_>> = HashMap::new();
    for id in target.document().flows()[flow_id].sinks() {
        let instance = &target.document().plugin_instances()[id];
        let contract = SinkContractId::from_parts(
            instance.program_name().clone(),
            instance.exact_version().clone(),
        );
        bindings.entry(contract).or_default().push(id.clone());
    }
    for ids in bindings.values_mut() {
        ids.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
    }
    bindings
}

pub(super) fn staged_instance_directory(
    changes: &CompiledResourceChanges,
    root: &Path,
    instance_id: &PluginInstanceId,
) -> PathBuf {
    let parent = if changes.process_mutation(instance_id) == Some(ResourceMutation::Add) {
        root.join(INSTANCES_DIRECTORY_NAME)
    } else {
        root.join(CANDIDATE_DIRECTORY_NAME)
    };
    instance_working_directory(&parent, instance_id)
}
