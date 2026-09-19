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

//! Coordinated definition replacement across one Flow's fixed FlowChannel set.
//!
//! Each existing FlowChannel thread creates its own replacement VM, then keeps
//! processing old Source and timer events until the common cutover decision.
//! Only after every FlowChannel reports `Prepared` may the reconfiguration
//! executor cross the cutover boundary. Cut-over FlowChannels remain paused
//! until every sibling has also cut over, so no FlowChannel consumes new Source
//! input while another FlowChannel still runs the old definition.

use std::sync::mpsc::{Receiver, sync_channel};

use super::error::PipelineRuntimeError;
use crate::identifiers::FlowId;
use crate::pipeline::channel::{
    ChannelDefinitionChange, FlowChannelCommandControl, FlowChannelReplacementEvent,
    FlowChannelReplacementTicket, PreparedEgressRoutes,
};

/// A transient, clone-handle-only view over one Flow's fixed FlowChannel set.
pub(crate) struct FlowDefinitionReplacement {
    // The coordinator can outlive its borrow of the live Flow map and needs
    // this immutable identity to attribute later protocol failures.
    flow_id: FlowId,
    channels: Box<[FlowChannelCommandControl]>,
}

impl FlowDefinitionReplacement {
    /// Takes clone-only controls; Channel threads and their JoinHandles remain
    /// owned by the live [`super::flow_runtime::FlowRuntime`]
    /// for the whole replacement.
    pub(super) fn new(flow_id: FlowId, channels: Box<[FlowChannelCommandControl]>) -> Self {
        Self { flow_id, channels }
    }

    /// Transfers every candidate Queue to its Channel without waiting for replies.
    pub(crate) fn begin(
        self,
        definition: ChannelDefinitionChange,
        routes: Vec<PreparedEgressRoutes>,
    ) -> Result<PendingFlowDefinitionReplacement, PipelineRuntimeError> {
        let Self { flow_id, channels } = self;
        assert_eq!(
            channels.len(),
            routes.len(),
            "every existing Channel has one target route set"
        );
        let channel_count = channels.len();
        let (event_sender, events) = sync_channel(channel_count);
        let mut tickets = Vec::new();
        tickets
            .try_reserve_exact(channel_count)
            .map_err(|_| PipelineRuntimeError::ResourceLimitExceeded)?;
        for (index, (channel, routes)) in channels.iter().zip(routes).enumerate() {
            let channel_index =
                u32::try_from(index).map_err(|_| PipelineRuntimeError::ResourceLimitExceeded)?;
            let ticket = channel
                .begin_replacement(
                    channel_index,
                    definition.clone(),
                    routes,
                    event_sender.clone(),
                )
                .map_err(|source| PipelineRuntimeError::FlowChannelCommandControl {
                    flow_id: flow_id.clone(),
                    channel_index,
                    source,
                })?;
            tickets.push(ticket);
        }
        drop(event_sender);

        Ok(PendingFlowDefinitionReplacement(
            PreparedFlowDefinitionReplacement {
                flow_id,
                tickets,
                events,
            },
        ))
    }
}

/// Published commands own candidate Queues; this observation owns only tickets.
pub(crate) struct PendingFlowDefinitionReplacement(PreparedFlowDefinitionReplacement);

impl PendingFlowDefinitionReplacement {
    /// Waits off the control thread until all candidates prepare or finish aborting.
    pub(crate) fn wait(self) -> Result<PreparedFlowDefinitionReplacement, PipelineRuntimeError> {
        let Self(candidate) = self;
        if let Err(failure) = wait_until_prepared(
            &candidate.flow_id,
            &candidate.events,
            candidate.tickets.len(),
        ) {
            candidate.abort()?;
            return Err(failure);
        }
        Ok(candidate)
    }
}

/// Every Channel has prepared a candidate VM while its old VM remains active.
pub(crate) struct PreparedFlowDefinitionReplacement {
    flow_id: FlowId,
    tickets: Vec<FlowChannelReplacementTicket>,
    events: Receiver<FlowChannelReplacementEvent>,
}

impl PreparedFlowDefinitionReplacement {
    /// Abandons a prepared VM and waits for every Channel to accept the abort.
    /// The caller must run this wait off the Pipeline control thread.
    pub(crate) fn abort(self) -> Result<(), PipelineRuntimeError> {
        let count = self.tickets.len();
        abort_and_wait(self.tickets, &self.events, count)
    }

    /// Retains every ticket on failure so terminal cleanup can Stop before Drop.
    pub(crate) fn cutover_in_place(&mut self) -> Result<(), PipelineRuntimeError> {
        let channel_count = self.tickets.len();
        for (index, ticket) in self.tickets.iter().enumerate() {
            let channel_index =
                u32::try_from(index).map_err(|_| PipelineRuntimeError::ResourceLimitExceeded)?;
            ticket
                .cutover()
                .map_err(|source| PipelineRuntimeError::FlowChannelCommandControl {
                    flow_id: self.flow_id.clone(),
                    channel_index,
                    source,
                })?;
        }
        wait_until_cutover_complete(&self.events, channel_count)
    }

    /// Requires successful cutover observation by the sole batch coordinator.
    pub(crate) fn into_paused(self) -> PausedFlowDefinitionReplacement {
        let Self {
            flow_id,
            tickets,
            events: _,
        } = self;
        PausedFlowDefinitionReplacement { flow_id, tickets }
    }
}

/// Every Channel owns its new VM but remains paused before target activation.
pub(crate) struct PausedFlowDefinitionReplacement {
    flow_id: FlowId,
    tickets: Vec<FlowChannelReplacementTicket>,
}

impl PausedFlowDefinitionReplacement {
    /// Releases the complete new FlowChannel set after all other target actions finish.
    pub(crate) fn activate(self) -> Result<(), PipelineRuntimeError> {
        let Self { flow_id, tickets } = self;
        for (index, ticket) in tickets.into_iter().enumerate() {
            let channel_index =
                u32::try_from(index).map_err(|_| PipelineRuntimeError::ResourceLimitExceeded)?;
            ticket.activate().map_err(|source| {
                PipelineRuntimeError::FlowChannelCommandControl {
                    flow_id: flow_id.clone(),
                    channel_index,
                    source,
                }
            })?;
        }
        Ok(())
    }
}

fn wait_until_prepared(
    flow_id: &FlowId,
    events: &Receiver<FlowChannelReplacementEvent>,
    channel_count: usize,
) -> Result<(), PipelineRuntimeError> {
    let mut seen = empty_seen_set(channel_count)?;
    for _ in 0..channel_count {
        match events.recv() {
            Ok(FlowChannelReplacementEvent::Prepared { index }) => {
                mark_once(&mut seen, index)?;
            }
            Ok(FlowChannelReplacementEvent::PreparationFailed { index, kind }) => {
                return Err(PipelineRuntimeError::FlowChannelReplacementPreparation {
                    flow_id: flow_id.clone(),
                    channel_index: index,
                    kind,
                });
            }
            Ok(FlowChannelReplacementEvent::CutoverComplete { .. })
            | Ok(FlowChannelReplacementEvent::Aborted { .. })
            | Err(_) => {
                return Err(PipelineRuntimeError::InternalEventChannelClosed);
            }
        }
    }
    Ok(())
}

fn wait_until_cutover_complete(
    events: &Receiver<FlowChannelReplacementEvent>,
    channel_count: usize,
) -> Result<(), PipelineRuntimeError> {
    let mut seen = empty_seen_set(channel_count)?;
    for _ in 0..channel_count {
        match events.recv() {
            Ok(FlowChannelReplacementEvent::CutoverComplete { index }) => {
                mark_once(&mut seen, index)?;
            }
            Ok(FlowChannelReplacementEvent::Prepared { .. })
            | Ok(FlowChannelReplacementEvent::PreparationFailed { .. })
            | Ok(FlowChannelReplacementEvent::Aborted { .. })
            | Err(_) => return Err(PipelineRuntimeError::InternalEventChannelClosed),
        }
    }
    Ok(())
}

fn abort_and_wait(
    tickets: Vec<FlowChannelReplacementTicket>,
    events: &Receiver<FlowChannelReplacementEvent>,
    channel_count: usize,
) -> Result<(), PipelineRuntimeError> {
    drop(tickets);
    let mut aborted = empty_seen_set(channel_count)?;
    let mut aborted_count = 0;
    while aborted_count < channel_count {
        match events.recv() {
            Ok(FlowChannelReplacementEvent::Aborted { index }) => {
                mark_once(&mut aborted, index)?;
                aborted_count += 1;
            }
            Ok(FlowChannelReplacementEvent::Prepared { .. })
            | Ok(FlowChannelReplacementEvent::PreparationFailed { .. }) => {}
            Ok(FlowChannelReplacementEvent::CutoverComplete { .. }) | Err(_) => {
                return Err(PipelineRuntimeError::InternalEventChannelClosed);
            }
        }
    }
    Ok(())
}

fn empty_seen_set(channel_count: usize) -> Result<Vec<bool>, PipelineRuntimeError> {
    let mut seen = Vec::new();
    seen.try_reserve_exact(channel_count)
        .map_err(|_| PipelineRuntimeError::ResourceLimitExceeded)?;
    seen.resize(channel_count, false);
    Ok(seen)
}

fn mark_once(seen: &mut [bool], index: u32) -> Result<(), PipelineRuntimeError> {
    let index =
        usize::try_from(index).map_err(|_| PipelineRuntimeError::InternalEventChannelClosed)?;
    let slot = seen
        .get_mut(index)
        .ok_or(PipelineRuntimeError::InternalEventChannelClosed)?;
    if *slot {
        return Err(PipelineRuntimeError::InternalEventChannelClosed);
    }
    *slot = true;
    Ok(())
}
