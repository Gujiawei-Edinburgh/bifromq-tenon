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

//! The sole startup decision for one candidate Pipeline data plane.
//!
//! The atomic decision is the only state. The mutex and condition variable
//! only prevent a lost wake while ready workers sleep. Production and Loom
//! builds compile this exact protocol with their respective synchronization
//! primitives.

#[cfg(all(test, feature = "loom-model"))]
use loom::sync::atomic::{AtomicU8, Ordering};
#[cfg(all(test, feature = "loom-model"))]
use loom::sync::{Condvar, Mutex, MutexGuard};
#[cfg(not(all(test, feature = "loom-model")))]
use std::sync::atomic::{AtomicU8, Ordering};
#[cfg(not(all(test, feature = "loom-model")))]
use std::sync::{Condvar, Mutex, MutexGuard};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum StartupDecision {
    Pending = 0,
    Bind = 1,
    Run = 2,
    Abort = 3,
}

pub(crate) struct StartupControl {
    decision: AtomicU8,
    wait_lock: Mutex<()>,
    changed: Condvar,
}

impl StartupControl {
    pub(crate) fn new() -> Self {
        Self {
            decision: AtomicU8::new(StartupDecision::Pending as u8),
            wait_lock: Mutex::new(()),
            changed: Condvar::new(),
        }
    }

    pub(super) fn bind(&self) {
        self.decide(StartupDecision::Bind);
    }

    pub(super) fn activate(&self) {
        self.decide(StartupDecision::Run);
    }

    pub(crate) fn abort(&self) {
        self.decide(StartupDecision::Abort);
    }

    pub(crate) fn is_aborted(&self) -> bool {
        self.decision() == StartupDecision::Abort
    }

    pub(super) fn wait_for_binding(&self) -> StartupDecision {
        let mut guard = self.lock_wait();
        while self.decision() == StartupDecision::Pending {
            guard = self
                .changed
                .wait(guard)
                .unwrap_or_else(|error| error.into_inner());
        }
        self.decision()
    }

    pub(super) fn wait(&self) -> StartupDecision {
        let mut guard = self.lock_wait();
        loop {
            match self.decision() {
                StartupDecision::Pending | StartupDecision::Bind => {
                    guard = self
                        .changed
                        .wait(guard)
                        .unwrap_or_else(|error| error.into_inner());
                }
                decision => return decision,
            }
        }
    }

    fn decide(&self, decision: StartupDecision) {
        debug_assert_ne!(decision, StartupDecision::Pending);
        let _guard = self.lock_wait();
        match self.decision() {
            StartupDecision::Pending => {
                assert_ne!(
                    decision,
                    StartupDecision::Run,
                    "writers bind before activation"
                );
            }
            StartupDecision::Bind if decision == StartupDecision::Bind => return,
            StartupDecision::Bind => {}
            StartupDecision::Run | StartupDecision::Abort => return,
        }
        self.decision.store(decision as u8, Ordering::Release);
        self.changed.notify_all();
    }

    fn decision(&self) -> StartupDecision {
        match self.decision.load(Ordering::Acquire) {
            value if value == StartupDecision::Pending as u8 => StartupDecision::Pending,
            value if value == StartupDecision::Bind as u8 => StartupDecision::Bind,
            value if value == StartupDecision::Run as u8 => StartupDecision::Run,
            value if value == StartupDecision::Abort as u8 => StartupDecision::Abort,
            _ => std::process::abort(),
        }
    }

    fn lock_wait(&self) -> MutexGuard<'_, ()> {
        self.wait_lock
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
}

#[cfg(test)]
pub(in crate::pipeline) mod test_support {
    #[cfg(not(feature = "loom-model"))]
    use super::MutexGuard;
    use super::{StartupControl, StartupDecision};

    pub(in crate::pipeline) fn is_pending(control: &StartupControl) -> bool {
        control.decision() == StartupDecision::Pending
    }

    #[cfg(not(feature = "loom-model"))]
    pub(in crate::pipeline) fn hold_decision_lock(control: &StartupControl) -> MutexGuard<'_, ()> {
        control.lock_wait()
    }
}
