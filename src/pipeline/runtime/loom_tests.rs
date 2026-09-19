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

use loom::sync::{Arc, mpsc};
use loom::thread;

use super::startup::{StartupControl, StartupDecision};

#[test]
fn loom_binding_precedes_activation_for_every_ready_worker() {
    loom::model(|| {
        let startup = Arc::new(StartupControl::new());
        let (bound, bindings) = mpsc::channel();
        let first_startup = Arc::clone(&startup);
        let first_bound = bound.clone();
        let first = thread::spawn(move || {
            assert_eq!(first_startup.wait_for_binding(), StartupDecision::Bind);
            assert!(first_bound.send(()).is_ok());
            first_startup.wait()
        });
        let second_startup = Arc::clone(&startup);
        let second = thread::spawn(move || {
            assert_eq!(second_startup.wait_for_binding(), StartupDecision::Bind);
            assert!(bound.send(()).is_ok());
            second_startup.wait()
        });
        startup.bind();
        assert!(bindings.recv().is_ok());
        assert!(bindings.recv().is_ok());
        startup.activate();
        assert!(matches!(first.join(), Ok(StartupDecision::Run)));
        assert!(matches!(second.join(), Ok(StartupDecision::Run)));
        startup.abort();
        assert!(
            !startup.is_aborted(),
            "Published activation is irreversible"
        );
    });
}

#[test]
fn loom_abort_reaches_workers_before_and_after_binding() {
    for initial in [StartupDecision::Pending, StartupDecision::Bind] {
        loom::model(move || {
            let startup = Arc::new(StartupControl::new());
            if initial == StartupDecision::Bind {
                startup.bind();
            }
            let worker_startup = Arc::clone(&startup);
            let worker = thread::spawn(move || {
                let binding = worker_startup.wait_for_binding();
                assert!(matches!(
                    binding,
                    StartupDecision::Bind | StartupDecision::Abort
                ));
                worker_startup.wait()
            });
            startup.abort();
            startup.bind();
            startup.activate();
            assert!(matches!(worker.join(), Ok(StartupDecision::Abort)));
            assert!(startup.is_aborted());
        });
    }
}
