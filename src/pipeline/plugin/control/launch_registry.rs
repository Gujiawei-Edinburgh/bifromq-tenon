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

//! One-time launch identity registration and attachment handoff.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::oneshot;

use super::session::{
    AttachedPluginControl, AttachedPluginControlTransport, PluginControlTransport,
};
use crate::payload_contract::PluginInterface;

const PROCESS_NONCE_LENGTH: usize = 8;
pub(in crate::pipeline::plugin) const PLUGIN_LAUNCH_ID_LENGTH: usize = 16;

/// Shared authority for every Plugin launch created by one Pipeline process.
#[derive(Clone)]
pub(super) struct PluginControlLaunchRegistry {
    state: Arc<Mutex<PluginControlLaunchRegistryState>>,
}

impl PluginControlLaunchRegistry {
    /// Creates one process-local launch namespace.
    pub(super) fn try_new() -> Result<Self, getrandom::Error> {
        let mut process_nonce = [0; PROCESS_NONCE_LENGTH];
        getrandom::fill(&mut process_nonce)?;
        Ok(Self {
            state: Arc::new(Mutex::new(PluginControlLaunchRegistryState {
                process_nonce,
                next_launch_sequence: 0,
                launches: HashMap::new(),
            })),
        })
    }

    /// Registers the exact interface capability of one new Plugin process.
    pub(super) fn register(&self, interface: PluginInterface) -> PendingPluginControl {
        let mut state = self.lock();
        let sequence = state.next_launch_sequence;
        state.next_launch_sequence += 1;
        let launch_id = PluginLaunchId::new(state.process_nonce, sequence);
        let (attachment, attached) = oneshot::channel();
        let replaced = state.launches.insert(
            launch_id,
            PendingPluginAttachment {
                interface,
                attachment,
            },
        );
        assert!(
            replaced.is_none(),
            "A process-local Plugin launch identity must be unique"
        );
        PendingPluginControl {
            registration: PluginControlRegistration {
                launch_id,
                launches: self.clone(),
            },
            attached,
        }
    }

    /// Atomically claims one still-pending launch.
    pub(super) fn claim(&self, launch_id: &[u8]) -> Option<PendingPluginAttachment> {
        self.lock().launches.remove(launch_id)
    }

    #[allow(
        clippy::expect_used,
        reason = "short registry critical sections execute no panicking external code"
    )]
    fn lock(&self) -> MutexGuard<'_, PluginControlLaunchRegistryState> {
        self.state
            .lock()
            .expect("Plugin launch registry lock must not be poisoned")
    }

    fn remove(&self, launch_id: &PluginLaunchId) {
        self.lock().launches.remove(launch_id);
    }
}

impl fmt::Debug for PluginControlLaunchRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginControlLaunchRegistry")
            .field("launch_count", &self.lock().launches.len())
            .finish()
    }
}

struct PluginControlLaunchRegistryState {
    process_nonce: [u8; PROCESS_NONCE_LENGTH],
    next_launch_sequence: u64,
    launches: HashMap<PluginLaunchId, PendingPluginAttachment>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct PluginLaunchId([u8; PLUGIN_LAUNCH_ID_LENGTH]);

impl PluginLaunchId {
    fn new(process_nonce: [u8; PROCESS_NONCE_LENGTH], sequence: u64) -> Self {
        let mut bytes = [0; PLUGIN_LAUNCH_ID_LENGTH];
        bytes[..PROCESS_NONCE_LENGTH].copy_from_slice(&process_nonce);
        bytes[PROCESS_NONCE_LENGTH..].copy_from_slice(&sequence.to_be_bytes());
        Self(bytes)
    }

    fn as_bytes(&self) -> &[u8; PLUGIN_LAUNCH_ID_LENGTH] {
        &self.0
    }
}

impl Borrow<[u8]> for PluginLaunchId {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl fmt::Debug for PluginLaunchId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginLaunchId")
            .field("length", &PLUGIN_LAUNCH_ID_LENGTH)
            .finish_non_exhaustive()
    }
}

pub(super) struct PendingPluginAttachment {
    interface: PluginInterface,
    attachment: oneshot::Sender<AttachedPluginControlTransport>,
}

impl PendingPluginAttachment {
    pub(super) fn hand_off(
        self,
        transport: PluginControlTransport,
    ) -> Result<(), PluginControlAttachmentError> {
        let transport = AttachedPluginControlTransport {
            interface: self.interface,
            inbound: transport.inbound,
            outbound: transport.outbound,
        };
        self.attachment
            .send(transport)
            .map_err(|_| PluginControlAttachmentError)
    }
}

/// Exclusive owner waiting for one registered Plugin process to attach.
#[must_use = "dropping this owner retires the unclaimed Plugin launch"]
pub(in crate::pipeline::plugin) struct PendingPluginControl {
    registration: PluginControlRegistration,
    attached: oneshot::Receiver<AttachedPluginControlTransport>,
}

impl PendingPluginControl {
    /// Returns the exact 16-byte identity passed to the Plugin process.
    pub(in crate::pipeline::plugin) fn launch_id(&self) -> &[u8; PLUGIN_LAUNCH_ID_LENGTH] {
        self.registration.launch_id.as_bytes()
    }

    /// Keeps the pending registration alive while waiting for the one valid Attach.
    pub(in crate::pipeline::plugin) async fn attach(
        self,
    ) -> Result<AttachedPluginControl, PluginControlAttachmentError> {
        let Self {
            registration,
            attached,
        } = self;
        let transport = attached.await.map_err(|_| PluginControlAttachmentError)?;
        drop(registration);
        Ok(AttachedPluginControl::new(transport))
    }
}

impl fmt::Debug for PendingPluginControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingPluginControl")
            .field("launch_id", &self.registration.launch_id)
            .finish_non_exhaustive()
    }
}

pub(super) struct PluginControlRegistration {
    launch_id: PluginLaunchId,
    launches: PluginControlLaunchRegistry,
}

impl Drop for PluginControlRegistration {
    fn drop(&mut self) {
        self.launches.remove(&self.launch_id);
    }
}

impl fmt::Debug for PluginControlRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginControlRegistration")
            .field("launch_id", &self.launch_id)
            .finish_non_exhaustive()
    }
}

/// An attachment could not reach the exact launch owner that registered it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline::plugin) struct PluginControlAttachmentError;

impl fmt::Display for PluginControlAttachmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Plugin control attachment owner is unavailable")
    }
}

impl Error for PluginControlAttachmentError {}
