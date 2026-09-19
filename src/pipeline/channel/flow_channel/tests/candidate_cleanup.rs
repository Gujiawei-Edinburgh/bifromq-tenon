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

//! Candidate cleanup must precede the event that permits deleting its Queue.

use super::*;
use crate::pipeline::channel::flow_channel_command::{
    ChannelCommand, ChannelDefinitionChange, FlowChannelReplacementEvent,
};
use tenon_ipc::bell::BellInterrupter;
use tenon_ipc::queue::{QueueWaiter, arm_bell_slot, queue_waiter_is_armed};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn failed_candidate_releases_new_writer_before_aborted_acknowledgement() -> TestResult {
    assert_cleanup_before_ack("error('candidate failed')")
}

#[test]
fn abandoned_candidate_releases_new_writer_before_aborted_acknowledgement() -> TestResult {
    assert_cleanup_before_ack("function main(event) end")
}

fn assert_cleanup_before_ack(candidate_lua: &'static str) -> TestResult {
    let source = SourceQueueFixture::new()?;
    let queues = source.queue_paths();
    let bells = source.bells()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("candidate.queue");
    let capacity = DataCapacity::try_from(EGRESS_CAPACITY)?;
    let max_payload = NonZeroU64::new(capacity.get() - tenon_ipc::queue::FRAME_HEADER_LEN as u64)
        .ok_or("empty capacity")?;
    create_queue_file(&path, capacity, max_payload)?;
    // The candidate writer owns the only strong handle to this doorbell, so a
    // ring that still lands proves the writer is still alive.
    let region_path = directory.path().join("candidate.bells");
    let region = create_test_bell_region(&region_path, 1)?;
    let writer = QueueWriter::open(&path, loop_bell(&region, 0)?, Arc::clone(&region))?;
    let interrupter = writer.wait_interrupter();
    let (event_sender, events) = mpsc::sync_channel(0);
    let (ticket_sender, tickets) = mpsc::sync_channel(0);
    let (advance, advance_requested) = mpsc::sync_channel(1);
    let worker = thread::spawn(move || -> Result<(), String> {
        let run = || -> TestResult {
            let (mut channel, _wake, commands) = open_channel(
                queues,
                bells,
                Instant::now(),
                channel_spec("function main(event) end", SourceDelivery::AtMostOnce, [])?,
                test_channel_publisher(0),
                HashMap::new(),
                Arc::new(FlowChannelControl::new()),
                || false,
            )?;
            let contract = sink_contract_id("com.example.kafka@1.0.0")?;
            let spec = channel_spec(candidate_lua, SourceDelivery::AtMostOnce, [&contract])?;
            let routes = HashMap::from([(
                contract,
                BTreeMap::from([(
                    PluginInstanceId::try_from("candidate")?,
                    PreparedEgressQueue::New(writer),
                )]),
            )]);
            let ticket = commands.begin_replacement(
                0,
                ChannelDefinitionChange::Replace(spec),
                routes,
                event_sender,
            )?;
            ticket_sender.send(ticket)?;
            let Some(ChannelCommand::Replace(request)) = channel.commands.try_take() else {
                return Err("replacement command is missing".into());
            };
            channel.prepare_replacement(request)?;
            advance_requested.recv_timeout(WAIT_LIMIT)?;
            channel.advance_replacement()?;
            Ok(())
        };
        run().map_err(|error| error.to_string())
    });
    let ticket = tickets.recv_timeout(WAIT_LIMIT)?;
    let prepared = events.recv_timeout(WAIT_LIMIT)?;
    drop(ticket);
    advance.send(())?;
    // The rendezvous channel prevents Aborted from completing until after the
    // probe. A live candidate writer would therefore remain mapped throughout.
    let deadline = Instant::now() + WAIT_LIMIT;
    let released = loop {
        match candidate_writer_released(&path, &region_path, &interrupter) {
            Ok(true) => break Ok(true),
            Ok(false) if Instant::now() < deadline => thread::yield_now(),
            result => break result,
        }
    };
    let aborted = events.recv_timeout(WAIT_LIMIT);
    let joined = worker.join().map_err(|_| "candidate worker panicked")?;
    joined.map_err(io::Error::other)?;
    assert!(matches!(
        prepared,
        FlowChannelReplacementEvent::Prepared { .. }
            | FlowChannelReplacementEvent::PreparationFailed { .. }
    ));
    assert!(matches!(
        aborted?,
        FlowChannelReplacementEvent::Aborted { .. }
    ));
    assert!(
        released?,
        "candidate mapping must be released before its cleanup acknowledgement"
    );
    Ok(())
}

/// Reports whether the candidate writer this test handed to the Channel is gone.
///
/// Arming the candidate's own doorbell makes a later ring visible: the writer
/// still alive rings it, and the writer dropped — which is what releases the
/// candidate Queue mapping — leaves it armed. The Channel signals the mapping
/// release by dropping the candidate routes before it reports the candidate
/// abandoned, so this is that same fact seen from the Queue side.
fn candidate_writer_released(
    queue_path: &Path,
    region_path: &Path,
    interrupter: &BellInterrupter,
) -> Result<bool, Box<dyn std::error::Error>> {
    arm_bell_slot(region_path, 0)?;
    interrupter.interrupt()?;
    Ok(queue_waiter_is_armed(
        queue_path,
        region_path,
        QueueWaiter::Writer,
    )?)
}
