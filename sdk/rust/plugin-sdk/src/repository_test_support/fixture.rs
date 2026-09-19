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

//! A real Source session over a temporary Queue layout.
//!
//! A Plugin's Connection owns the deliveries it admits, so a test of that
//! Connection needs the real [`PayloadSender`] the SDK hands a Program, not a
//! stand-in. This fixture creates what a Pipeline launch creates: one
//! Submission/Completion Queue pair per Channel, the Source's own two-slot
//! Region, and the Flow Region the Queue headers publish into. The test then
//! plays the side a Flow Channel loop plays — read a Submission, run the work,
//! commit a Completion.

use crate::Error;
use crate::LOOPS_BELL_FILE_NAME;
use crate::process::FailureBoundary;
use crate::source::session::{PayloadSender, Session};
use std::io;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tenon_ipc::bell::BellRegion;
use tenon_ipc::queue::{self, QueueReader as Reader, QueueWriter as Writer};

/// Encoded size of the fixed `IngressCompletion` schema.
///
/// The Pipeline pairs every Submission Queue with a Completion Queue of this
/// payload size, and the session verifies the pairing on open.
const COMPLETION_MAX_PAYLOAD_SIZE: usize = 13;

/// The Source's own Region holds one slot per loop, not per Channel.
const LOOPS: u32 = 2;

/// One running Source session with the Queues and Regions around it.
#[derive(Debug)]
pub struct SourceFixture {
    queues: tempfile::TempDir,
    /// The Flow Region belongs to the Pipeline, so it lives in a directory of
    /// its own that the fixture holds only for as long as the session runs.
    _flow: tempfile::TempDir,
    loops: Arc<BellRegion>,
    channels: Arc<BellRegion>,
    failure: Arc<Mutex<Option<String>>>,
    session: Session,
}

#[expect(
    clippy::expect_used,
    reason = "Poisoning the repository fixture diagnostic latch is an internal test bug"
)]
impl SourceFixture {
    /// Creates one Channel's Queue pair per index and opens the session.
    ///
    /// `pending` is how many records one Channel may hold unacknowledged, so
    /// the fixture offers exactly the admission window a launch of that
    /// `maxPendingRecords` offers.
    ///
    /// # Errors
    ///
    /// Returns the error that ended the launch: a Region or Queue that could
    /// not be created, or a layout the session rejects.
    pub fn create(
        channels: u32,
        pending: usize,
        maximum_record_bytes: usize,
    ) -> Result<Self, Error> {
        let queues = tempfile::tempdir()?;
        let slots =
            NonZeroU32::new(channels).ok_or("a Source fixture needs at least one Channel")?;
        let loops = region(&queues.path().join(LOOPS_BELL_FILE_NAME), LOOPS)?;
        let flow = tempfile::tempdir()?;
        let channel_bell_path = flow.path().join("channels.bells");
        let channels_region = region(&channel_bell_path, channels)?;
        let maximum_record_bytes =
            NonZeroU64::new(maximum_record_bytes as u64).ok_or("a payload limit is nonzero")?;
        let pending = NonZeroU64::new(pending as u64).ok_or("a pending limit is nonzero")?;
        let completion_maximum =
            NonZeroU64::new(COMPLETION_MAX_PAYLOAD_SIZE as u64).ok_or("completion is nonempty")?;
        let submission_frame = queue::maximum_frame_len(maximum_record_bytes)?;
        let completion_frame = queue::maximum_frame_len(completion_maximum)?;
        // One record limit is one frame, plus the frame a physical wrap needs.
        for index in 0..slots.get() {
            queue::create_queue_file(
                queues.path().join(format!("submission-{index}.queue")),
                queue::capacity_for_record_limit(pending, submission_frame)?,
                maximum_record_bytes,
            )?;
            queue::create_queue_file(
                queues.path().join(format!("completion-{index}.queue")),
                queue::capacity_for_record_limit(pending, completion_frame)?,
                completion_maximum,
            )?;
        }
        let failure = Arc::new(Mutex::new(None));
        let failed: FailureBoundary = {
            let failure = Arc::clone(&failure);
            Arc::new(move |error| {
                *failure.lock().expect("Source fixture failure lock") = Some(error.to_string());
            })
        };
        let session = Session::open(queues.path(), &channel_bell_path, failed)?;
        Ok(Self {
            queues,
            _flow: flow,
            loops,
            channels: channels_region,
            failure,
            session,
        })
    }

    /// Returns a sender for one Plugin build's Source payload type.
    pub fn sender<P>(&self) -> PayloadSender<P> {
        self.session.sender()
    }

    /// Opens the Submission Queue the way its Flow Channel loop does.
    ///
    /// # Errors
    ///
    /// Returns an [`io::Error`] when the Channel has no Queue pair or the file
    /// cannot be mapped.
    pub fn submission(&self, channel: u32) -> io::Result<Reader> {
        Reader::open(
            self.queues
                .path()
                .join(format!("submission-{channel}.queue")),
            self.channels.loop_bell(channel)?,
            Arc::clone(&self.loops),
        )
        .map_err(io::Error::from)
    }

    /// Opens the Completion Queue the way its Flow Channel loop does.
    ///
    /// # Errors
    ///
    /// Returns an [`io::Error`] when the Channel has no Queue pair or the file
    /// cannot be mapped.
    pub fn completion(&self, channel: u32) -> io::Result<Writer> {
        Writer::open(
            self.queues
                .path()
                .join(format!("completion-{channel}.queue")),
            self.channels.loop_bell(channel)?,
            Arc::clone(&self.loops),
        )
        .map_err(io::Error::from)
    }

    /// Reports the failure that ended the session, if one did.
    pub fn failure(&self) -> Option<String> {
        self.failure
            .lock()
            .expect("Source fixture failure lock")
            .clone()
    }
}

impl Drop for SourceFixture {
    fn drop(&mut self) {
        // Closing resolves every request still in flight, so a test that leaves
        // a delivery uncompleted never leaves a future whose sender vanished.
        let _ = self.session.close();
    }
}

fn region(path: &Path, slots: u32) -> io::Result<Arc<BellRegion>> {
    let slots = NonZeroU32::new(slots)
        .ok_or_else(|| io::Error::other("a Region needs at least one slot"))?;
    tenon_ipc::bell::create_bell_region(path, slots, 0)?;
    BellRegion::open(path).map_err(io::Error::from)
}
