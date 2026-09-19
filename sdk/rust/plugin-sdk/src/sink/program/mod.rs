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

//! One Sink business object and its SDK-owned process lifetime.

#![expect(
    clippy::expect_used,
    reason = "Program owns its business object and joined sessions"
)]

use super::TenonSink;
use super::session::{BatchWriter, Queues, Session};
use crate::process::{
    ControlConnection, Event, FailureBoundary, Lifecycle, Publish, fatal, install_panic_hook,
    resolve, startup,
};
use crate::{Error, FlowChannel, Value};
use prost::Message;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::{Arc, mpsc};

/// Owns one Sink business object and its paused Egress loop until readiness.
pub struct SinkProgram<P, S: TenonSink<P>> {
    business: Arc<SinkWriter<S>>,
    session: Session,
    control: ControlConnection,
    received: mpsc::Receiver<Event>,
    config: Value,
    finished: bool,
    payload: PhantomData<fn(P)>,
}

impl<P: Message + Default, S: TenonSink<P>> SinkProgram<P, S> {
    /// Creates and starts the business owner and prepares a paused Egress loop.
    pub fn run(factory: impl FnOnce(Value) -> Result<S, Error>) -> Self {
        install_panic_hook();
        let startup = {
            let mut input = std::io::stdin().lock();
            resolve(startup::read(std::env::args_os().skip(1), &mut input))
        };
        let channels = resolve(startup::require_sink_inputs(&startup));
        let (events, received) = mpsc::channel();
        let failed: FailureBoundary = Arc::new(|error| fatal(error.as_ref()));
        let control = resolve(ControlConnection::start(
            startup.control_socket,
            startup.launch_id,
            Lifecycle::SinkOnly,
            events,
            failed.clone(),
        ));
        let queues = resolve(Queues::open(&startup.working_directory, channels));
        let mut owner = resolve(factory(startup.config.clone()));
        owner.start();
        let business = Arc::new(SinkWriter(owner));
        let session = resolve(Session::start(queues, business.clone(), move |error| {
            failed(error)
        }));
        Self {
            business,
            session,
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

    /// Publishes Ready, activates the Egress loop, and waits for final shutdown.
    pub fn await_shutdown(mut self) {
        self.control.publish(Publish::Ready);
        self.session.activate();
        match self.received.recv().expect("control owns lifecycle events") {
            Event::Shutdown => {}
            Event::Quiesce => unreachable!("Sink-only program has no Source"),
        }
        resolve(self.session.close());
        Arc::get_mut(&mut self.business)
            .expect("the Sink Egress loop joined")
            .0
            .close();
        self.control.finish();
        self.finished = true;
    }
}

impl<P, S: TenonSink<P>> fmt::Debug for SinkProgram<P, S> {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.debug_struct("SinkProgram").finish_non_exhaustive()
    }
}

impl<P, S: TenonSink<P>> Drop for SinkProgram<P, S> {
    fn drop(&mut self) {
        if !self.finished {
            fatal(&"Sink Program was dropped before shutdown completed");
        }
    }
}

struct SinkWriter<S>(S);
impl<P, S: TenonSink<P>> BatchWriter<P> for SinkWriter<S> {
    fn write(
        &self,
        channel: FlowChannel,
        records: Box<[P]>,
    ) -> impl Future<Output = Result<(), Error>> {
        self.0.write(channel, records)
    }
}
