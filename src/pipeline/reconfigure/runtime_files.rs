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

//! Owns the private working-directory and Queue-file layout.

use std::fs::DirBuilder;
use std::io;
use std::num::{NonZeroU32, NonZeroU64};
use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest as _, Sha256};

use super::error::PipelineReconfigureError;
use crate::identifiers::PluginInstanceId;
use tenon_ipc::bell::{BellRegion, create_bell_region, read_bell_region_epoch};
use tenon_ipc::queue::{DataCapacity, create_queue_file};

pub(crate) const SOURCE_DIRECTORY_NAME: &str = "source";
pub(super) const INSTANCES_DIRECTORY_NAME: &str = "instances";
pub(crate) const SINK_DIRECTORY_NAME: &str = "sink";
pub(super) const FLOWS_DIRECTORY_NAME: &str = "flows";
const CHANNELS_BELL_FILE_NAME: &str = "channels.bells";
pub(super) const LOOPS_BELL_FILE_NAME: &str = "loops.bells";

pub(super) fn create_private_directory(path: &Path) -> Result<(), PipelineReconfigureError> {
    create_directory_entry(path)?;
    enforce_private_directory_permissions(path)
}

/// Creates one private directory, accepting a directory this same batch already made.
///
/// One resource batch creates a directory once but binds several files into it:
/// the Channel Bell Region, the Queue files of that same Flow or side, and the
/// per-Flow egress directory. Requiring each writer to know whether another has
/// already created the shared parent would duplicate ownership facts.
pub(super) fn ensure_private_directory(path: &Path) -> Result<(), PipelineReconfigureError> {
    if path.is_dir() {
        return enforce_private_directory_permissions(path);
    }
    create_private_directory(path)
}

pub(super) fn create_directory_entry(path: &Path) -> Result<(), PipelineReconfigureError> {
    let mut builder = DirBuilder::new();
    builder.mode(0o700);
    builder
        .create(path)
        .map_err(|source| PipelineReconfigureError::DirectoryCreate {
            path: path.to_owned(),
            source,
        })
}

pub(super) fn enforce_private_directory_permissions(
    path: &Path,
) -> Result<(), PipelineReconfigureError> {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(|source| {
        PipelineReconfigureError::DirectoryPermission {
            path: path.to_owned(),
            source,
        }
    })
}

pub(super) fn create_queue(
    path: &Path,
    capacity: DataCapacity,
    max_payload_size: NonZeroU64,
) -> Result<(), PipelineReconfigureError> {
    create_queue_file(path, capacity, max_payload_size).map_err(|source| {
        PipelineReconfigureError::QueueCreate {
            path: path.to_owned(),
            source,
        }
    })
}

fn identity_directory_name(id: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(id.as_bytes()))
}

/// Returns the shared directory of one Flow's cross-process Queue endpoints.
pub(super) fn flow_directory(pipeline_root: &Path, flow_id: &str) -> PathBuf {
    pipeline_root
        .join(FLOWS_DIRECTORY_NAME)
        .join(identity_directory_name(flow_id))
}

/// Returns the Bell Region that holds one Flow's Channel doorbells.
///
/// Every Channel of the Flow and every process that commits to or releases one
/// of its Queues reaches this one file: the Source Instance, all Sink Instances,
/// and the Runner's own Channel threads.
pub(crate) fn flow_channel_bell_path(pipeline_root: &Path, flow_id: &str) -> PathBuf {
    flow_directory(pipeline_root, flow_id).join(CHANNELS_BELL_FILE_NAME)
}

/// Returns the Bell Region that holds one plugin side's own loop doorbells.
///
/// The name is fixed inside the side directory, so a plugin derives it exactly
/// as it derives its Queue paths: the Source from `source/loops.bells` under the
/// working directory it was launched with, the Sink from `sink/loops.bells`.
/// Only the Runner's Channels ring these slots.
pub(crate) fn loops_bell_path(side_directory: &Path) -> PathBuf {
    side_directory.join(LOOPS_BELL_FILE_NAME)
}

/// Ensures the Bell Region at one final path holds exactly `slot_count` slots.
///
/// A Region is named by the loop layout that numbers its slots, so an existing
/// Region already holding this many slots is reused unchanged. Reuse is what
/// keeps every peer that mapped the Region earlier ringing the slots the loops
/// it was bound to still park in; replacing it would leave those peers writing
/// a mapping no loop waits on. Only a different slot count — a different Channel
/// count — replaces it, and so does a path this process cannot open as a Region,
/// because a mapping that cannot be opened holds no live loop.
///
/// A replaced region keeps a nondecreasing diagnostic epoch, saturating at the
/// maximum value so diagnostic metadata can never prevent rebuilding a Region.
///
/// # Errors
///
/// Returns [`PipelineReconfigureError::BellRegionRemove`] when an earlier region
/// cannot be removed, or [`PipelineReconfigureError::BellRegionCreate`] when the
/// new region cannot be created or initialized.
pub(super) fn ensure_bell_region_file(
    path: &Path,
    slot_count: NonZeroU32,
) -> Result<(), PipelineReconfigureError> {
    if let Ok(existing) = BellRegion::open(path)
        && existing.slot_count() == slot_count
    {
        return Ok(());
    }
    let epoch = read_bell_region_epoch(path).unwrap_or(0).saturating_add(1);
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(source) if source.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(PipelineReconfigureError::BellRegionRemove {
                path: path.to_owned(),
                source,
            });
        }
    }
    create_bell_region(path, slot_count, epoch).map_err(|source| {
        PipelineReconfigureError::BellRegionCreate {
            path: path.to_owned(),
            source,
        }
    })
}

pub(super) fn instance_working_directory(
    instances_directory: &Path,
    id: &PluginInstanceId,
) -> PathBuf {
    instances_directory.join(identity_directory_name(id.as_str()))
}

/// Returns the sole on-disk layout for one Sink input Channel.
pub(crate) fn egress_queue_path(
    instance_directory: &Path,
    flow_id: &str,
    channel_id: u32,
) -> PathBuf {
    instance_directory
        .join(SINK_DIRECTORY_NAME)
        .join(identity_directory_name(flow_id))
        .join(format!("egress-{channel_id}.queue"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_maximum_diagnostic_epoch_does_not_block_region_reuse_or_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("loops.bells");
        let one = NonZeroU32::new(1).ok_or("one slot is nonzero")?;
        let two = NonZeroU32::new(2).ok_or("two slots are nonzero")?;
        create_bell_region(&path, one, u64::MAX)?;
        let original = std::fs::read(&path)?;

        ensure_bell_region_file(&path, one)?;
        assert_eq!(std::fs::read(&path)?, original);
        ensure_bell_region_file(&path, two)?;
        assert_eq!(BellRegion::open(&path)?.slot_count(), two);
        assert_eq!(read_bell_region_epoch(&path), Some(u64::MAX));
        Ok(())
    }
}
