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

//! One shared Source-and-sink business object and its SDK-owned process lifetime.

#![expect(
    clippy::expect_used,
    reason = "Program owns its business object and joined sessions"
)]

use super::TenonSourceAndSink;
use crate::process::{
    ControlConnection, Event, FailureBoundary, Lifecycle, Publish, fatal, install_panic_hook,
    resolve, startup,
};
use crate::sink::session::{BatchWriter, Queues, Session as SinkSession};
use crate::source::session::Session as SourceSession;
use crate::{Error, FlowChannel, PayloadSender, Value};
use prost::Message;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::{Arc, mpsc};

/// Owns one shared business object and both queue sessions.
pub struct SourceAndSinkProgram<P, B: TenonSourceAndSink<P>> {
    business: Arc<SharedWriter<B>>,
    source: SourceSession,
    sink: SinkSession,
    control: ControlConnection,
    received: mpsc::Receiver<Event>,
    config: Value,
    finished: bool,
    payload: PhantomData<fn(P)>,
}

impl<P: Message + Default, B: TenonSourceAndSink<P>> SourceAndSinkProgram<P, B> {
    /// Starts one owner. Business configuration decides which work to enable.
    pub fn run<S: Message>(
        factory: impl FnOnce(Value, usize, PayloadSender<S>) -> Result<B, Error>,
    ) -> Self {
        install_panic_hook();
        let startup = {
            let mut input = std::io::stdin().lock();
            resolve(startup::read(std::env::args_os().skip(1), &mut input))
        };
        let channel_bell_path = resolve(startup::require_channel_bell_path(&startup));
        let channels = resolve(startup::require_sink_inputs(&startup));
        let (events, received) = mpsc::channel();
        let failed: FailureBoundary = Arc::new(|error| fatal(error.as_ref()));
        let control = resolve(ControlConnection::start(
            startup.control_socket,
            startup.launch_id,
            Lifecycle::SourceCapable,
            events,
            failed.clone(),
        ));
        let source = resolve(SourceSession::open(
            &startup.working_directory.join("source"),
            &channel_bell_path,
            failed,
        ));
        let queues = resolve(Queues::open(&startup.working_directory, channels));
        let mut owner = resolve(factory(
            startup.config.clone(),
            source.parallelism(),
            source.sender(),
        ));
        owner.start();
        let business = Arc::new(SharedWriter(owner));
        let sink = resolve(SinkSession::start(
            queues,
            business.clone(),
            source.failure_handler(),
        ));
        Self {
            business,
            source,
            sink,
            control,
            received,
            config: startup.config,
            finished: false,
            payload: PhantomData,
        }
    }

    /// Returns the validated business configuration.
    pub fn config(&self) -> &Value {
        &self.config
    }

    /// Publishes Ready and owns Source quiesce and final shutdown.
    pub fn await_shutdown(mut self) {
        resolve(self.source.check_running());
        self.control.publish(Publish::Ready);
        self.sink.activate();
        while let Event::Quiesce = self.received.recv().expect("control owns lifecycle events") {
            self.source.stop_accepting();
            self.business.0.quiesce();
            resolve(self.source.quiesce());
            self.control.publish(Publish::Quiesced);
        }
        resolve(self.source.close());
        resolve(self.sink.close());
        Arc::get_mut(&mut self.business)
            .expect("all Sink workers joined")
            .0
            .close();
        self.control.finish();
        self.finished = true;
    }
}

impl<P, B: TenonSourceAndSink<P>> fmt::Debug for SourceAndSinkProgram<P, B> {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("SourceAndSinkProgram")
            .finish_non_exhaustive()
    }
}

impl<P, B: TenonSourceAndSink<P>> Drop for SourceAndSinkProgram<P, B> {
    fn drop(&mut self) {
        if !self.finished {
            fatal(&"Source-and-sink Program was dropped before shutdown completed");
        }
    }
}

struct SharedWriter<B>(B);
impl<P, B: TenonSourceAndSink<P>> BatchWriter<P> for SharedWriter<B> {
    fn write(
        &self,
        channel: FlowChannel,
        records: Box<[P]>,
    ) -> impl Future<Output = Result<(), Error>> {
        self.0.write(channel, records)
    }
}
