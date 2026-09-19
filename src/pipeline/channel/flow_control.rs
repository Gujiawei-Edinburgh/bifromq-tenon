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

//! Monotonic runtime directive shared by every Channel in one Flow.
//!
//! Directives only advance: Continue, drain Source, drain committed Egress, Stop.
//! Callers may skip stages; no drain request can overwrite Stop.
//! Production and Loom builds compile this exact transition code with their
//! respective atomic primitive.

#[cfg(all(test, feature = "loom-model"))]
use loom::sync::atomic::{AtomicU8, Ordering};
#[cfg(not(all(test, feature = "loom-model")))]
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum ChannelDirective {
    Continue = 0,
    Drain = 1,
    DrainEgress = 2,
    Stop = 3,
}

#[derive(Debug)]
pub(crate) struct FlowChannelControl {
    directive: AtomicU8,
}

impl FlowChannelControl {
    pub(crate) fn new() -> Self {
        Self {
            directive: AtomicU8::new(ChannelDirective::Continue as u8),
        }
    }

    pub(crate) fn request_drain(&self) {
        let _previous = self
            .directive
            .fetch_max(ChannelDirective::Drain as u8, Ordering::AcqRel);
    }

    pub(crate) fn request_egress_drain(&self) {
        let _previous = self
            .directive
            .fetch_max(ChannelDirective::DrainEgress as u8, Ordering::AcqRel);
    }

    pub(crate) fn request_stop(&self) {
        let _previous = self
            .directive
            .fetch_max(ChannelDirective::Stop as u8, Ordering::AcqRel);
    }

    pub(super) fn directive(&self) -> ChannelDirective {
        match self.directive.load(Ordering::Acquire) {
            value if value == ChannelDirective::Continue as u8 => ChannelDirective::Continue,
            value if value == ChannelDirective::Drain as u8 => ChannelDirective::Drain,
            value if value == ChannelDirective::DrainEgress as u8 => ChannelDirective::DrainEgress,
            value if value == ChannelDirective::Stop as u8 => ChannelDirective::Stop,
            _ => std::process::abort(),
        }
    }

    pub(super) fn is_stopping(&self) -> bool {
        matches!(
            self.directive(),
            ChannelDirective::DrainEgress | ChannelDirective::Stop
        )
    }

    pub(super) fn is_force_stopping(&self) -> bool {
        self.directive() == ChannelDirective::Stop
    }

    pub(crate) fn has_stop_request(&self) -> bool {
        self.directive() != ChannelDirective::Continue
    }
}

#[cfg(all(test, not(feature = "loom-model")))]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::{ChannelDirective, FlowChannelControl};

    #[test]
    fn concurrent_stop_always_dominates_drain_under_real_threads() {
        for _ in 0..1_000 {
            let control = Arc::new(FlowChannelControl::new());
            let start = Arc::new(Barrier::new(3));

            let drain_control = Arc::clone(&control);
            let drain_start = Arc::clone(&start);
            let drain = thread::spawn(move || {
                drain_start.wait();
                drain_control.request_drain();
            });

            let stop_control = Arc::clone(&control);
            let stop_start = Arc::clone(&start);
            let stop = thread::spawn(move || {
                stop_start.wait();
                stop_control.request_stop();
            });

            start.wait();
            assert!(drain.join().is_ok(), "Drain requester panicked");
            assert!(stop.join().is_ok(), "Stop requester panicked");
            assert_eq!(control.directive(), ChannelDirective::Stop);
        }
    }

    #[test]
    fn draining_one_flow_does_not_change_another_flow() {
        let first = FlowChannelControl::new();
        let second = FlowChannelControl::new();

        first.request_drain();

        assert_eq!(first.directive(), ChannelDirective::Drain);
        assert_eq!(second.directive(), ChannelDirective::Continue);
    }
}

#[cfg(all(test, feature = "loom-model"))]
mod loom_tests {
    use loom::sync::Arc;
    use loom::thread;

    use super::{ChannelDirective, FlowChannelControl};

    #[test]
    fn loom_stop_dominates_a_concurrent_drain_request() {
        loom::model(|| {
            let control = Arc::new(FlowChannelControl::new());
            let drain_control = Arc::clone(&control);
            let drain = thread::spawn(move || {
                drain_control.request_drain();
                drain_control.request_egress_drain();
            });
            let stop_control = Arc::clone(&control);
            let stop = thread::spawn(move || stop_control.request_stop());

            assert!(drain.join().is_ok(), "Drain requester panicked");
            assert!(stop.join().is_ok(), "Stop requester panicked");
            assert_eq!(control.directive(), ChannelDirective::Stop);
        });
    }

    #[test]
    fn loom_egress_drain_stops_input_without_forcing_committed_output_to_stop() {
        loom::model(|| {
            let control = Arc::new(FlowChannelControl::new());
            let source = Arc::clone(&control);
            let source_drain = thread::spawn(move || source.request_drain());
            control.request_egress_drain();
            assert!(source_drain.join().is_ok());
            assert!(control.is_stopping());
            assert!(!control.is_force_stopping());
            assert_eq!(control.directive(), ChannelDirective::DrainEgress);
        });
    }

    #[test]
    fn loom_drain_never_downgrades_an_observed_stop() {
        loom::model(|| {
            let control = Arc::new(FlowChannelControl::new());
            control.request_stop();
            let drain_control = Arc::clone(&control);
            let drain = thread::spawn(move || {
                drain_control.request_drain();
                drain_control.request_egress_drain();
            });

            assert!(drain.join().is_ok(), "Drain requester panicked");
            assert_eq!(control.directive(), ChannelDirective::Stop);
        });
    }
}
