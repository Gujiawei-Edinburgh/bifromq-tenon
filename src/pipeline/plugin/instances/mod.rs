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

//! Owns the unique Instance-to-child collection for one applied Pipeline.
//!
//! Validated revisions supply unique identities. The map owns those identities;
//! each value owns its child, startup progress, retry sequence, and cleanup.
//! Launch material is borrowed
//! from the exact revision chosen by the caller, never cached here.
//!
//! Polling follows identity order and stores progress in the child owners, so
//! dropping an event wait loses no progress. Removal retains ownership until
//! reap succeeds. Bulk shutdown signals every selected child before awaiting
//! any child; its caller must not resume event polling after terminal cleanup.
//! Failures retain owned children for the Pipeline's force-cleanup path.

use std::collections::BTreeMap;
use std::future::poll_fn;
use std::task::{Context, Poll};

use crate::contracts::core::{PluginInstanceState, PluginInstanceStatus};
use crate::identifiers::PluginInstanceId;
use crate::pipeline::diagnostics::PluginDiagnosticPublisher;

use super::ControlledPluginLaunch;
use super::lifecycle::{
    PluginInstanceOwner, PluginLifecycleError, PluginRetryBackoff, PluginStateEvent,
    PluginStatusState,
};
use super::{PluginInstanceError, PluginInstanceEvent};

mod retirement;
pub(in crate::pipeline) use retirement::PluginRetirementEvent;

#[derive(Debug)]
pub(in crate::pipeline) struct PluginInstances {
    owners: BTreeMap<PluginInstanceId, PluginInstanceOwner>,
}

impl PluginInstances {
    /// Starts one complete UDS-controlled Instance set.
    pub(in crate::pipeline) fn launch_controlled<'a>(
        launches: impl IntoIterator<
            Item = (
                PluginInstanceId,
                ControlledPluginLaunch<'a>,
                PluginDiagnosticPublisher,
            ),
        >,
        retry_backoff: PluginRetryBackoff,
    ) -> Self {
        Self {
            owners: launches
                .into_iter()
                .map(|(identity, launch, diagnostics)| {
                    (
                        identity,
                        PluginInstanceOwner::launch_controlled(launch, retry_backoff, diagnostics),
                    )
                })
                .collect(),
        }
    }

    /// Moves disjoint, compiled retained owners here and leaves their old map empty.
    /// Child state and retry deadlines move unchanged; both maps use the frozen backoff.
    pub(in crate::pipeline) fn append(&mut self, retained: &mut Self) {
        self.owners.append(&mut retained.owners);
    }

    /// Reports whether the collection owns no Instance lifecycle responsibilities.
    pub(in crate::pipeline) fn is_empty(&self) -> bool {
        self.owners.is_empty()
    }

    /// Waits for one child transition while preserving all canceled-wait progress.
    pub(in crate::pipeline) async fn next_event(
        &mut self,
    ) -> Result<PluginInstanceEvent, PluginInstanceError> {
        poll_fn(|context| self.poll_event_where(context, |_| true)).await
    }

    /// Keeps other lifecycle owners progressing while selected Sources are rebuilt.
    pub(in crate::pipeline) async fn next_event_where(
        &mut self,
        selected: impl Fn(&PluginInstanceId) -> bool,
    ) -> Result<PluginInstanceEvent, PluginInstanceError> {
        poll_fn(|context| self.poll_event_where(context, &selected)).await
    }

    /// Restarts one UDS-controlled Instance using the caller's applied revision.
    pub(in crate::pipeline) fn restart_controlled(
        &mut self,
        identity: &PluginInstanceId,
        launch: ControlledPluginLaunch<'_>,
        diagnostics: PluginDiagnosticPublisher,
    ) -> Result<(), PluginInstanceError> {
        let control = launch.control;
        self.owner_mut(identity)
            .restart_controlled(launch, diagnostics, || control.record_restart(identity))
            .map_err(|source| PluginInstanceError::new(identity.clone(), source))
    }

    /// Quiesces every Source-capable Instance before the caller drains the data plane.
    pub(in crate::pipeline) async fn quiesce_sources(&mut self) -> Result<(), PluginInstanceError> {
        let mut first_error = None;
        for (identity, owner) in &mut self.owners {
            if let Err(source) = owner.begin_source_quiesce()
                && first_error.is_none()
            {
                first_error = Some(PluginInstanceError::new(identity.clone(), source));
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }

        poll_fn(|context| {
            for (identity, owner) in &mut self.owners {
                match owner.poll_source_quiesced(context) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(source)) => {
                        return Poll::Ready(Err(PluginInstanceError::new(
                            identity.clone(),
                            source,
                        )));
                    }
                    Poll::Pending => {}
                }
            }
            if self
                .owners
                .values()
                .all(PluginInstanceOwner::is_source_quiesced)
            {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        })
        .await
    }

    /// Sends final Shutdown to every remaining Instance, then reaps every direct child.
    pub(in crate::pipeline) async fn shutdown_all(&mut self) -> Result<(), PluginInstanceError> {
        assert!(
            self.owners
                .values()
                .all(PluginInstanceOwner::is_source_quiesced),
            "Final Plugin shutdown requires every Source-capable Instance to be quiesced"
        );
        self.stop_where(|_| true, PluginInstanceOwner::request_planned_stop)
            .await
    }

    /// Force-signals the complete collection before reaping any child.
    pub(super) async fn force_stop_all(&mut self) -> Result<(), PluginInstanceError> {
        self.stop_where(|_| true, PluginInstanceOwner::request_force_stop)
            .await
    }

    /// Ends collection ownership only after every child has been reaped.
    /// Cancellation or an OS failure retains all owners for continued terminal cleanup.
    pub(in crate::pipeline) async fn force_remove_all(
        &mut self,
    ) -> Result<(), PluginInstanceError> {
        self.force_stop_all().await?;
        self.owners.clear();
        Ok(())
    }

    fn poll_event_where(
        &mut self,
        context: &mut Context<'_>,
        selected: impl Fn(&PluginInstanceId) -> bool,
    ) -> Poll<Result<PluginInstanceEvent, PluginInstanceError>> {
        for (identity, owner) in self.owners.iter_mut().filter(|(id, _)| selected(id)) {
            match owner.poll_event(context) {
                Poll::Ready(Ok(PluginStateEvent::StatusChanged)) => {
                    return Poll::Ready(Ok(PluginInstanceEvent::StatusChanged));
                }
                Poll::Ready(Ok(PluginStateEvent::ProcessFailed)) => {
                    return Poll::Ready(Ok(PluginInstanceEvent::ProcessFailed(identity.clone())));
                }
                Poll::Ready(Ok(PluginStateEvent::RestartDue)) => {
                    return Poll::Ready(Ok(PluginInstanceEvent::RestartDue(identity.clone())));
                }
                Poll::Ready(Err(source)) => {
                    return Poll::Ready(Err(PluginInstanceError::new(identity.clone(), source)));
                }
                Poll::Pending => {}
            }
        }
        Poll::Pending
    }

    #[allow(
        clippy::expect_used,
        reason = "compiled runtime operations and observed retry events select owned identities"
    )]
    fn owner_mut(&mut self, identity: &PluginInstanceId) -> &mut PluginInstanceOwner {
        self.owners
            .get_mut(identity)
            .expect("Plugin operation must select an owned Instance identity")
    }

    async fn stop_where(
        &mut self,
        selected: impl Fn(&PluginInstanceId) -> bool,
        request_stop: fn(&mut PluginInstanceOwner) -> Result<(), PluginLifecycleError>,
    ) -> Result<(), PluginInstanceError> {
        let mut first_error = self.request_stop_where(&selected, request_stop).err();
        for (identity, owner) in &mut self.owners {
            if selected(identity)
                && let Err(source) = owner.reap_after_stop().await
                && first_error.is_none()
            {
                first_error = Some(PluginInstanceError::new(identity.clone(), source));
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn request_stop_where(
        &mut self,
        selected: impl Fn(&PluginInstanceId) -> bool,
        request_stop: fn(&mut PluginInstanceOwner) -> Result<(), PluginLifecycleError>,
    ) -> Result<(), PluginInstanceError> {
        let mut first_error = None;
        for (identity, owner) in &mut self.owners {
            if selected(identity)
                && let Err(source) = request_stop(owner)
                && first_error.is_none()
            {
                first_error = Some(PluginInstanceError::new(identity.clone(), source));
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl PluginInstances {
    /// Materializes the current wire view without exposing or copying child owners.
    pub(in crate::pipeline) fn statuses(&self) -> Vec<PluginInstanceStatus> {
        self.owners
            .iter()
            .map(|(id, owner)| {
                let (state, last_error) = owner.status();
                let state = match state {
                    PluginStatusState::Starting => PluginInstanceState::Starting,
                    PluginStatusState::Running => PluginInstanceState::Running,
                    PluginStatusState::StartFailed => PluginInstanceState::StartFailed,
                    PluginStatusState::RestartBackoff => PluginInstanceState::RestartBackoff,
                };
                PluginInstanceStatus {
                    id: id.as_str().to_owned(),
                    state: state.into(),
                    last_error,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod revision_tests;

#[cfg(test)]
mod controlled_tests;
