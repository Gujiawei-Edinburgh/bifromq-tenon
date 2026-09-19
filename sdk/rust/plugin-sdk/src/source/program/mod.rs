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

//! One Source business object and its SDK-owned process lifetime.

#![expect(
    clippy::expect_used,
    reason = "Program owns its business object and sessions"
)]

use super::session::Session;
use super::{PayloadSender, TenonSource};
use crate::process::{
    ControlConnection, Event, FailureBoundary, Lifecycle, Publish, fatal, install_panic_hook,
    resolve, startup,
};
use crate::{Error, Value};
use prost::Message;
use std::fmt;
use std::sync::{Arc, mpsc};

/// Owns one Source business object, its queues, and its process lifetime.
pub struct SourceProgram<S: TenonSource> {
    business: S,
    session: Session,
    control: ControlConnection,
    received: mpsc::Receiver<Event>,
    config: Value,
    finished: bool,
}

impl<S: TenonSource> SourceProgram<S> {
    /// Creates and starts one Source. Startup failures terminate the plugin.
    pub fn run<P: Message>(
        factory: impl FnOnce(Value, usize, PayloadSender<P>) -> Result<S, Error>,
    ) -> Self {
        install_panic_hook();
        let startup = {
            let mut input = std::io::stdin().lock();
            resolve(startup::read(std::env::args_os().skip(1), &mut input))
        };
        let channel_bell_path = resolve(startup::require_channel_bell_path(&startup));
        let (events, received) = mpsc::channel();
        let failed: FailureBoundary = Arc::new(|error| fatal(error.as_ref()));
        let control = resolve(ControlConnection::start(
            startup.control_socket,
            startup.launch_id,
            Lifecycle::SourceCapable,
            events,
            failed.clone(),
        ));
        let session = resolve(Session::open(
            &startup.working_directory.join("source"),
            &channel_bell_path,
            failed,
        ));
        let mut business = resolve(factory(
            startup.config.clone(),
            session.parallelism(),
            session.sender(),
        ));
        business.start();
        Self {
            business,
            session,
            control,
            received,
            config: startup.config,
            finished: false,
        }
    }

    /// Returns the validated business configuration.
    pub fn config(&self) -> &Value {
        &self.config
    }

    /// Publishes Ready and owns quiesce and final shutdown.
    pub fn await_shutdown(mut self) {
        resolve(self.session.check_running());
        self.control.publish(Publish::Ready);
        while let Event::Quiesce = self.received.recv().expect("control owns lifecycle events") {
            self.session.stop_accepting();
            self.business.quiesce();
            resolve(self.session.quiesce());
            self.control.publish(Publish::Quiesced);
        }
        resolve(self.session.close());
        self.business.close();
        self.control.finish();
        self.finished = true;
    }
}

impl<S: TenonSource> fmt::Debug for SourceProgram<S> {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.debug_struct("SourceProgram").finish_non_exhaustive()
    }
}

impl<S: TenonSource> Drop for SourceProgram<S> {
    fn drop(&mut self) {
        if !self.finished {
            fatal(&"Source Program was dropped before shutdown completed");
        }
    }
}
