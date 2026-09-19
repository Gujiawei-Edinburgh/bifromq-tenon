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

//! Hands off selected Instances without suspending unrelated child lifecycles.
//!
//! Owners remain in the sole Instance map until every selected child is reaped.
//! Each borrowed reap future stores progress in its owner, including completed
//! process waits and control-stream closure. Canceling a wait loses no progress.
//! After requesting retirement, callers must use this wait or terminal cleanup,
//! never normal event/status polling of the partially stopped collection.

use super::super::controlled_lifecycle::HandoffReadiness;
use std::collections::{BTreeMap, BTreeSet};
use std::future::{Future as _, poll_fn};
use std::pin::pin;
use std::task::{Context, Poll};

use super::{PluginInstanceError, PluginInstanceEvent, PluginInstanceOwner, PluginInstances};
use crate::identifiers::PluginInstanceId;

/// Either the complete old batch is gone, or one retained owner needs attention.
pub(in crate::pipeline) enum PluginRetirementEvent {
    Reaped,
    Retained(PluginInstanceEvent),
}

impl PluginInstances {
    /// Signals every selected old session before waiting for any child.
    /// An OS/control failure retains every owner and requires terminal cleanup.
    pub(in crate::pipeline) fn request_retirement(
        &mut self,
        selected: &BTreeSet<PluginInstanceId>,
    ) -> Result<(), PluginInstanceError> {
        self.request_stop_where(
            |id| selected.contains(id),
            PluginInstanceOwner::request_planned_stop,
        )
    }

    /// Waits for batch retirement or a retained event, never retrying selected owners.
    /// Reaped owners are removed together; only then may the caller launch replacements.
    pub(in crate::pipeline) fn poll_handoff_event(
        &mut self,
        context: &mut Context<'_>,
        healthy: &BTreeSet<PluginInstanceId>,
        retiring: &BTreeSet<PluginInstanceId>,
    ) -> Poll<Result<PluginRetirementEvent, PluginInstanceError>> {
        self.poll_handoff_health(context, healthy)?;
        let mut reaped = true;
        for id in retiring {
            match pin!(self.owner_mut(id).reap_after_stop()).poll(context) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(source)) => {
                    return Poll::Ready(Err(PluginInstanceError::new(id.clone(), source)));
                }
                Poll::Pending => reaped = false,
            }
        }
        if reaped && !retiring.is_empty() {
            for id in retiring {
                self.owners.remove(id);
            }
            return Poll::Ready(Ok(PluginRetirementEvent::Reaped));
        }
        self.poll_event_where(context, |id| {
            !retiring.contains(id) && !healthy.contains(id)
        })
        .map(|event| event.map(PluginRetirementEvent::Retained))
    }

    /// Freezes session readiness without losing failures observed at Cutover entry.
    pub(in crate::pipeline) async fn handoff_readiness(
        &mut self,
        selected: &BTreeSet<PluginInstanceId>,
    ) -> Result<BTreeMap<PluginInstanceId, HandoffReadiness>, PluginInstanceError> {
        poll_fn(|context| {
            Poll::Ready(
                selected
                    .iter()
                    .map(|id| {
                        self.owner_mut(id)
                            .handoff_readiness(context)
                            .map(|readiness| (id.clone(), readiness))
                            .map_err(|source| PluginInstanceError::new(id.clone(), source))
                    })
                    .collect::<
                        Result<BTreeMap<PluginInstanceId, HandoffReadiness>, PluginInstanceError>,
                    >(),
            )
        })
        .await
    }

    pub(in crate::pipeline) fn request_source_quiesce(
        &mut self,
        selected: &BTreeSet<PluginInstanceId>,
    ) -> Result<(), PluginInstanceError> {
        self.request_stop_where(
            |id| selected.contains(id),
            PluginInstanceOwner::begin_source_quiesce,
        )
    }

    /// Reports whether every selected old child has finished starting.
    ///
    /// The ordered handoff waits for this before it asks a session to quiesce,
    /// because quiescing a pending launch force stops the peer that still owes
    /// the releases a Channel already committed to it. A child that never
    /// becomes Ready keeps the handoff waiting until the reconfigure deadline
    /// ends it, which is deliberate: a silent early teardown of a peer that
    /// still owes a release is what this wait exists to prevent.
    pub(in crate::pipeline) fn handoff_peers_are_started(
        &self,
        selected: &BTreeSet<PluginInstanceId>,
    ) -> bool {
        selected
            .iter()
            .all(|id| !self.owners[id].has_pending_handoff_launch())
    }

    pub(in crate::pipeline) fn sources_are_quiesced(
        &self,
        selected: &BTreeSet<PluginInstanceId>,
    ) -> bool {
        selected
            .iter()
            .all(|id| self.owners[id].is_source_quiesced())
    }

    /// Observes frozen healthy sessions without starting retries or consuming other owners.
    pub(in crate::pipeline) async fn wait_for_handoff_failure(
        &mut self,
        healthy: &BTreeSet<PluginInstanceId>,
    ) -> PluginInstanceError {
        poll_fn(|context| match self.poll_handoff_health(context, healthy) {
            Ok(()) => Poll::Pending,
            Err(error) => Poll::Ready(error),
        })
        .await
    }

    fn poll_handoff_health(
        &mut self,
        context: &mut Context<'_>,
        healthy: &BTreeSet<PluginInstanceId>,
    ) -> Result<(), PluginInstanceError> {
        for id in healthy {
            self.owner_mut(id)
                .poll_handoff(context)
                .map_err(|source| PluginInstanceError::new(id.clone(), source))?;
        }
        Ok(())
    }
}
