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

//! The existing Queue files and Bell Regions bound to one FlowChannel.

use std::path::PathBuf;
use std::sync::Arc;

use tenon_ipc::bell::{BellRegion, LoopBell};

#[derive(Debug)]
pub(crate) struct FlowChannelQueuePaths {
    submission: PathBuf,
    completion: PathBuf,
}

impl FlowChannelQueuePaths {
    /// Binds one exact Queue pair to the FlowChannel identified by slice order.
    #[must_use]
    pub(crate) fn new(submission: PathBuf, completion: PathBuf) -> Self {
        Self {
            submission,
            completion,
        }
    }

    pub(super) fn into_parts(self) -> (PathBuf, PathBuf) {
        (self.submission, self.completion)
    }
}

/// The Bell Regions one Channel's own Queue endpoints bind.
///
/// `bell` is the Channel loop's own doorbell. Every wait of the Channel parks on
/// it, and both of the Channel's endpoints publish its slot so a committing
/// Source or a releasing Sink reaches this loop. `source_region` is the Bell
/// Region of the Source Instance that owns the other end of the Channel's
/// Submission and Completion Queues; a wait here that admits input or reclaims
/// completion space rings the slot the Source published there.
#[derive(Debug)]
pub(crate) struct FlowChannelBells {
    bell: Arc<LoopBell>,
    source_region: Arc<BellRegion>,
}

impl FlowChannelBells {
    #[must_use]
    pub(crate) fn new(bell: Arc<LoopBell>, source_region: Arc<BellRegion>) -> Self {
        Self {
            bell,
            source_region,
        }
    }

    pub(super) fn into_parts(self) -> (Arc<LoopBell>, Arc<BellRegion>) {
        (self.bell, self.source_region)
    }
}
