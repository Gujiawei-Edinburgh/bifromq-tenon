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

//! Wake handles for the single wait of one concrete Flow Channel.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::identifiers::PluginInstanceId;
use tenon_ipc::bell::{BellError, BellInterrupter};

/// The process-local departure facts one Channel's blocked waits re-read.
///
/// `departed` records Sink Instances of the current route generation that left
/// the Pipeline, so a release they still owe can never arrive. Only the Pipeline
/// owner that retires those Instances may add a departure, and the fact dies with
/// the generation that observed it: an identity this Pipeline removes and later
/// re-adds is a live Sink of the next generation, which still owes every release
/// it accepts.
#[derive(Debug, Default)]
pub(crate) struct EgressWaitRegistration {
    departed: BTreeSet<PluginInstanceId>,
}

impl EgressWaitRegistration {
    /// Starts the route generation that now owns Egress.
    ///
    /// Every earlier generation settled before this call, so its departures
    /// describe Identities that no longer bind these routes. Keeping them would
    /// read a re-added Identity as permanently gone and retry output a live Sink
    /// is about to release.
    pub(super) fn start_generation(&mut self) {
        self.departed.clear();
    }

    /// Reports whether this Sink Instance left the Pipeline.
    pub(super) fn has_departed(&self, instance: &PluginInstanceId) -> bool {
        self.departed.contains(instance)
    }

    fn depart(&mut self, instances: &BTreeSet<PluginInstanceId>) {
        self.departed.extend(instances.iter().cloned());
    }
}

/// Every wake source of one Channel's single Queue wait.
///
/// A Channel parks on exactly one doorbell address, so a Flow directive, a
/// Channel command, a retired Sink target, and a cross-process Queue event all
/// reach the same handle. The doorbell carries no cause: every wake only tells
/// the Channel to re-read its whole condition set.
#[derive(Debug)]
pub(crate) struct ChannelWake {
    bell: BellInterrupter,
    // This owns the departure facts across writer replacement. A new generation
    // starts before the Channel can enter its next Queue wait.
    egress_waits: Arc<Mutex<EgressWaitRegistration>>,
}

impl ChannelWake {
    pub(super) fn new(bell: BellInterrupter) -> Self {
        Self {
            bell,
            egress_waits: Arc::new(Mutex::new(EgressWaitRegistration::default())),
        }
    }

    pub(super) fn egress_registration(&self) -> Arc<Mutex<EgressWaitRegistration>> {
        Arc::clone(&self.egress_waits)
    }

    /// Wakes this Channel after its Flow directive changed or a command arrived.
    pub(crate) fn wake(&self) -> Result<(), ChannelWakeError> {
        self.bell
            .interrupt()
            .map_err(|source| ChannelWakeError::Wake { source })
    }

    /// Wakes this Channel unconditionally without recording a departure fact.
    ///
    /// A failed or restarting peer may still be replaced and therefore must
    /// not be marked as permanently absent. The unconditional platform wake
    /// closes the window where the peer published a notification and exited
    /// before the conditional wake reached the kernel.
    pub(crate) fn force_wake(&self) -> Result<(), ChannelWakeError> {
        self.bell
            .force_wake()
            .map_err(|source| ChannelWakeError::ForceWake { source })
    }

    /// Records that these Sink Instances left the Pipeline, then wakes this
    /// Channel so it re-reads every condition it may be blocked on.
    pub(crate) fn depart_egress_targets(
        &self,
        instances: &BTreeSet<PluginInstanceId>,
    ) -> Result<(), ChannelWakeError> {
        self.egress_waits
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .depart(instances);
        self.bell
            .force_wake()
            .map_err(|source| ChannelWakeError::Departure { source })
    }
}

/// A failure while waking one Channel Queue wait.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum ChannelWakeError {
    /// The Channel's doorbell could not be rung.
    Wake {
        /// Original Bell Region wake failure.
        source: BellError,
    },
    /// A failed peer required an unconditional platform wake.
    ForceWake {
        /// Original Bell Region wake failure.
        source: BellError,
    },
    /// A retired Sink target could not wake the Channel that still waits on it.
    Departure {
        /// Original Bell Region wake failure.
        source: BellError,
    },
}

impl fmt::Display for ChannelWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Wake { .. } => "Flow Channel wait could not be woken",
            Self::ForceWake { .. } => "Flow Channel wait could not be force-woken",
            Self::Departure { .. } => "Flow Channel wait could not be woken after a Sink left",
        })
    }
}

impl Error for ChannelWakeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Wake { source } | Self::ForceWake { source } | Self::Departure { source } => {
                Some(source)
            }
        }
    }
}
