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

//! Models the coordinator's admission check followed by work outside the lock.
//! A completed Future is acknowledged only if its observation wins against stop.
//! Native Queue wait and mapping behavior is covered by the real process tests.

use loom::sync::{Arc, Mutex};
use loom::thread;

#[test]
fn stopping_and_completion_observation_have_one_ordered_acceptance_point() {
    loom::model(|| {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        enum Action {
            Admit,
            WriteReturned,
            AcceptCompletion,
            Release,
            Stop,
            Close,
        }
        let state = Arc::new(Mutex::new((true, Vec::new())));
        let worker_state = state.clone();
        let worker = thread::spawn(move || {
            {
                let mut state = worker_state.lock().expect("model admission lock");
                if !state.0 {
                    return;
                }
                state.1.push(Action::Admit);
            }
            // Business work does not hold admission. Stop is allowed between
            // accepting a batch and the return of its write method body.
            thread::yield_now();
            worker_state
                .lock()
                .expect("model return log")
                .1
                .push(Action::WriteReturned);
            {
                let mut state = worker_state.lock().expect("model completion lock");
                if !state.0 {
                    return;
                }
                state.1.push(Action::AcceptCompletion);
            }
            thread::yield_now();
            worker_state
                .lock()
                .expect("model release log")
                .1
                .push(Action::Release);
        });
        {
            let mut state = state.lock().expect("model stop lock");
            state.0 = false;
            state.1.push(Action::Stop);
        }
        worker.join().expect("model coordinator join");
        let mut state = state.lock().expect("model close log");
        state.1.push(Action::Close);
        let position = |action| state.1.iter().position(|entry| *entry == action);
        let stop = position(Action::Stop).expect("stop was recorded");
        let close = position(Action::Close).expect("close was recorded");
        if let Some(admit) = position(Action::Admit) {
            assert!(admit < stop);
            let returned = position(Action::WriteReturned).expect("every admitted body returns");
            assert!(admit < returned && returned < close);
        }
        if let Some(release) = position(Action::Release) {
            let accepted =
                position(Action::AcceptCompletion).expect("release requires an observed success");
            assert!(accepted < stop && accepted < release && release < close);
        }
    });
}
