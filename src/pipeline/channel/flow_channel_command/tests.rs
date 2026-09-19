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

use std::io;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use super::*;

type TestResult = Result<(), Box<dyn Error>>;

const CHANNEL_INDEX: u32 = 7;

#[test]
fn prepared_candidate_requires_cutover_then_activation() -> TestResult {
    let control = running_control();
    let (session, decisions, events) = session(Arc::clone(&control));
    send_decision(&decisions, FlowChannelReplacementDecision::Cutover)?;

    let permit = cutover_permit(session)?;
    assert_prepared(receive_event(&events)?)?;
    send_decision(&decisions, FlowChannelReplacementDecision::Activate)?;

    permit.complete_and_wait_for_activation()?;
    assert_cutover_complete(receive_event(&events)?)?;
    Ok(())
}

#[test]
fn prepared_candidate_abort_reports_that_the_old_vm_remains_active() -> TestResult {
    let (session, decisions, events) = session(running_control());
    send_decision(&decisions, FlowChannelReplacementDecision::Abort)?;
    session.candidate_prepared()?;

    let CandidatePreparationOutcome::Aborted(session) = session.try_cutover()? else {
        return Err("candidate did not abort".into());
    };
    assert_prepared(receive_event(&events)?)?;
    assert!(
        events.try_recv().is_err(),
        "cleanup has not been acknowledged yet"
    );
    session.report_aborted();
    assert_aborted(receive_event(&events)?)?;
    Ok(())
}

#[test]
fn activation_before_cutover_is_rejected() -> TestResult {
    let (session, decisions, _events) = session(running_control());
    send_decision(&decisions, FlowChannelReplacementDecision::Activate)?;

    assert!(matches!(
        session.try_cutover(),
        Err(FlowChannelError::InternalInvariantViolation)
    ));
    Ok(())
}

#[test]
fn failed_candidate_reports_cleanup_without_waiting_for_abort() -> TestResult {
    let (session, _decisions, events) = session(running_control());

    session.candidate_failed(LuaVmErrorKind::SyntaxInvalid)?;
    assert_preparation_failed(receive_event(&events)?)?;
    assert_aborted(receive_event(&events)?)?;
    Ok(())
}

#[test]
fn repeated_cutover_instead_of_activation_is_rejected() -> TestResult {
    let (session, decisions, _events) = session(running_control());
    send_decision(&decisions, FlowChannelReplacementDecision::Cutover)?;
    let permit = cutover_permit(session)?;
    send_decision(&decisions, FlowChannelReplacementDecision::Cutover)?;

    assert!(matches!(
        permit.complete_and_wait_for_activation(),
        Err(FlowChannelError::InternalInvariantViolation)
    ));
    Ok(())
}

#[test]
fn required_event_receiver_loss_is_fatal_only_while_running() -> TestResult {
    let running = running_control();
    let (running_session, _decisions, running_events) = session(running);
    drop(running_events);
    assert!(matches!(
        running_session.candidate_failed(LuaVmErrorKind::SyntaxInvalid),
        Err(FlowChannelError::InternalInvariantViolation)
    ));

    let stopping = stopping_control();
    let (stopping_session, decisions, stopping_events) = session(stopping);
    drop(stopping_events);
    send_decision(&decisions, FlowChannelReplacementDecision::Abort)?;
    stopping_session.candidate_failed(LuaVmErrorKind::SyntaxInvalid)?;
    Ok(())
}

#[test]
fn aborted_event_is_best_effort_even_while_running() {
    let (session, _decisions, events) = session(running_control());
    drop(events);

    session.send_aborted_best_effort();
}

#[test]
fn abort_after_cutover_is_valid_only_for_planned_stop() -> TestResult {
    let running = running_control();
    let (running_session, running_decisions, _running_events) = session(running);
    send_decision(&running_decisions, FlowChannelReplacementDecision::Cutover)?;
    let running_permit = cutover_permit(running_session)?;
    send_decision(&running_decisions, FlowChannelReplacementDecision::Abort)?;
    assert!(matches!(
        running_permit.complete_and_wait_for_activation(),
        Err(FlowChannelError::InternalInvariantViolation)
    ));

    let stopping = running_control();
    let (stopping_session, stopping_decisions, _stopping_events) = session(Arc::clone(&stopping));
    send_decision(&stopping_decisions, FlowChannelReplacementDecision::Cutover)?;
    let stopping_permit = cutover_permit(stopping_session)?;
    stopping.request_stop();
    stopping_permit.complete_and_wait_for_activation()?;
    Ok(())
}

#[test]
fn dropping_ticket_sends_abort_and_releases_the_active_slot() -> TestResult {
    let directory = tempfile::tempdir()?;
    let region_path = directory.path().join("loops.bells");
    tenon_ipc::bell::create_bell_region(&region_path, std::num::NonZeroU32::MIN, 0)?;
    let region = tenon_ipc::bell::BellRegion::open(&region_path)?;
    let bell = region.loop_bell(0)?.interrupter();
    let (decision_sender, decisions) = sync_channel(CHANNEL_REPLACEMENT_DECISION_CAPACITY);
    let active_decision = Arc::new(Mutex::new(Some(decision_sender.clone())));
    let ticket = FlowChannelReplacementTicket {
        decision_sender: Some(decision_sender),
        active_decision: Arc::clone(&active_decision),
        bell,
    };

    drop(ticket);

    assert!(matches!(
        decisions.recv(),
        Ok(FlowChannelReplacementDecision::Abort)
    ));
    assert!(lock_active_decision(&active_decision).is_none());
    Ok(())
}

#[test]
fn command_slot_answers_pending_without_consuming_and_reports_a_dead_worker() -> TestResult {
    let directory = tempfile::tempdir()?;
    let region_path = directory.path().join("loops.bells");
    tenon_ipc::bell::create_bell_region(&region_path, std::num::NonZeroU32::MIN, 0)?;
    let region = tenon_ipc::bell::BellRegion::open(&region_path)?;
    let (inbox, control) = control_pair(region.loop_bell(0)?.interrupter());
    assert!(!inbox.has_pending());

    let (finished, _completion) = tokio::sync::oneshot::channel();
    control.send(ChannelCommand::FinishSourceSession(finished))?;
    // The park rechecks the pending fact without consuming it, and the single
    // slot still rejects a second command.
    assert!(inbox.has_pending());
    let (second, _completion) = tokio::sync::oneshot::channel();
    assert!(matches!(
        control.send(ChannelCommand::FinishSourceSession(second)),
        Err(FlowChannelCommandControlError::CommandAlreadyPending)
    ));
    assert!(matches!(
        inbox.try_take(),
        Some(ChannelCommand::FinishSourceSession(_))
    ));
    assert!(!inbox.has_pending());

    drop(inbox);
    let (third, _completion) = tokio::sync::oneshot::channel();
    assert!(matches!(
        control.send(ChannelCommand::FinishSourceSession(third)),
        Err(FlowChannelCommandControlError::WorkerDisconnected)
    ));
    Ok(())
}

fn session(
    control: Arc<FlowChannelControl>,
) -> (
    ChannelReplacementSession,
    SyncSender<FlowChannelReplacementDecision>,
    Receiver<FlowChannelReplacementEvent>,
) {
    let (decision_sender, decisions) = sync_channel(CHANNEL_REPLACEMENT_DECISION_CAPACITY);
    let (event_sender, events) = sync_channel(4);
    (
        ChannelReplacementSession {
            index: CHANNEL_INDEX,
            decisions,
            events: event_sender,
            control,
        },
        decision_sender,
        events,
    )
}

fn running_control() -> Arc<FlowChannelControl> {
    Arc::new(FlowChannelControl::new())
}

fn stopping_control() -> Arc<FlowChannelControl> {
    let control = running_control();
    control.request_stop();
    control
}

fn cutover_permit(
    session: ChannelReplacementSession,
) -> Result<ChannelReplacementCutoverPermit, Box<dyn Error>> {
    session.candidate_prepared()?;
    match session.try_cutover()? {
        CandidatePreparationOutcome::Cutover(permit) => Ok(permit),
        CandidatePreparationOutcome::Pending(_) => {
            Err("Prepared candidate has no cutover decision".into())
        }
        CandidatePreparationOutcome::Aborted(_) => {
            Err("Prepared candidate was unexpectedly aborted".into())
        }
    }
}

fn send_decision(
    sender: &SyncSender<FlowChannelReplacementDecision>,
    decision: FlowChannelReplacementDecision,
) -> io::Result<()> {
    sender
        .send(decision)
        .map_err(|_| io::Error::other("Lua replacement decision receiver disappeared"))
}

fn receive_event(
    events: &Receiver<FlowChannelReplacementEvent>,
) -> io::Result<FlowChannelReplacementEvent> {
    events
        .recv()
        .map_err(|_| io::Error::other("Lua replacement event sender disappeared"))
}

fn assert_prepared(event: FlowChannelReplacementEvent) -> io::Result<()> {
    match event {
        FlowChannelReplacementEvent::Prepared {
            index: CHANNEL_INDEX,
        } => Ok(()),
        _ => Err(io::Error::other("Expected Prepared Lua replacement event")),
    }
}

fn assert_preparation_failed(event: FlowChannelReplacementEvent) -> io::Result<()> {
    match event {
        FlowChannelReplacementEvent::PreparationFailed {
            index: CHANNEL_INDEX,
            kind: LuaVmErrorKind::SyntaxInvalid,
        } => Ok(()),
        _ => Err(io::Error::other(
            "Expected PreparationFailed Lua replacement event",
        )),
    }
}

fn assert_cutover_complete(event: FlowChannelReplacementEvent) -> io::Result<()> {
    match event {
        FlowChannelReplacementEvent::CutoverComplete {
            index: CHANNEL_INDEX,
        } => Ok(()),
        _ => Err(io::Error::other(
            "Expected CutoverComplete Lua replacement event",
        )),
    }
}

fn assert_aborted(event: FlowChannelReplacementEvent) -> io::Result<()> {
    match event {
        FlowChannelReplacementEvent::Aborted {
            index: CHANNEL_INDEX,
        } => Ok(()),
        _ => Err(io::Error::other("Expected Aborted Lua replacement event")),
    }
}
